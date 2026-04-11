package seol

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nireo/seol/skiplist"
	"github.com/nireo/seol/sstable"
	"github.com/nireo/seol/vlog"
)

const (
	defaultMemtableMaxBytes int64 = 4 << 20
	minMemtableArenaSize    int64 = 1 << 20
	defaultValueThreshold         = 128
	defaultWriteBatchWindow       = 100 * time.Microsecond
)

type immutableMemtable struct {
	table    *skiplist.Skiplist
	walPaths []string
}

type writeRequest struct {
	key   []byte
	value []byte
	err   error
	wg    sync.WaitGroup
}

var writeRequestPool = sync.Pool{
	New: func() any {
		return &writeRequest{}
	},
}

func acquireWriteRequest(key, value []byte) *writeRequest {
	req := writeRequestPool.Get().(*writeRequest)
	req.key = key
	req.value = value
	req.err = nil
	req.wg.Add(1)
	return req
}

func (req *writeRequest) finish(err error) {
	req.err = err
	req.wg.Done()
}

func (req *writeRequest) wait() error {
	req.wg.Wait()
	err := req.err
	req.key = nil
	req.value = nil
	req.err = nil
	writeRequestPool.Put(req)
	return err
}

type Options struct {
	MemtableMaxBytes        int64
	WALSyncInterval         time.Duration
	ValueLogSegmentMaxBytes int64
	ValueThreshold          int
	AsyncWrites             bool
	WriteBatchWindow        time.Duration
}

type dbReadState struct {
	closed    bool
	memtable  *skiplist.Skiplist
	immutable []*immutableMemtable
	sstables  []*sstable.Table
	valueLog  *vlog.Log
}

type DB struct {
	dir              string
	memtableMaxBytes int64
	walSyncInterval  time.Duration
	valueThreshold   int
	asyncWrites      bool
	writeBatchWindow time.Duration
	memtable         *skiplist.Skiplist
	memtableBytes    int64
	activeWal        *wal
	currentWalPaths  []string
	immutable        []*immutableMemtable
	sstables         []*sstable.Table // newest first
	valueLog         *vlog.Log
	flushFn          func(baseDir string, sk *skiplist.Skiplist) (*sstable.Table, error)
	writeCh          chan *writeRequest
	flushCh          chan *immutableMemtable
	readState        atomic.Pointer[dbReadState]

	mu       sync.RWMutex
	submitMu sync.Mutex
	flushErr error
	closed   bool
	writeWg  sync.WaitGroup
	flushWg  sync.WaitGroup
}

func Open(dir string) (*DB, error) {
	return OpenWithOptions(dir, Options{})
}

func OpenWithOptions(dir string, opts Options) (*DB, error) {
	return openDB(dir, opts, sstable.Flush)
}

func openDB(dir string, opts Options, flushFn func(baseDir string, sk *skiplist.Skiplist) (*sstable.Table, error)) (*DB, error) {
	if opts.MemtableMaxBytes <= 0 {
		opts.MemtableMaxBytes = defaultMemtableMaxBytes
	}
	if opts.ValueThreshold <= 0 {
		opts.ValueThreshold = defaultValueThreshold
	}
	if opts.WriteBatchWindow < 0 {
		opts.WriteBatchWindow = 0
	} else if opts.WriteBatchWindow == 0 {
		opts.WriteBatchWindow = defaultWriteBatchWindow
	}
	if flushFn == nil {
		flushFn = sstable.Flush
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var sstPaths []string
	var walPaths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		switch {
		case strings.HasSuffix(entry.Name(), ".sst"):
			sstPaths = append(sstPaths, path)
		case strings.HasSuffix(entry.Name(), ".wal"):
			walPaths = append(walPaths, path)
		}
	}
	sort.Slice(sstPaths, func(i, j int) bool {
		return filepath.Base(sstPaths[i]) > filepath.Base(sstPaths[j])
	})
	sort.Slice(walPaths, func(i, j int) bool {
		return filepath.Base(walPaths[i]) < filepath.Base(walPaths[j])
	})

	sstables := make([]*sstable.Table, 0, len(sstPaths))
	for _, path := range sstPaths {
		sst, err := sstable.Open(path)
		if err != nil {
			for _, opened := range sstables {
				_ = opened.Close()
			}
			return nil, err
		}
		sstables = append(sstables, sst)
	}

	valueLog, err := vlog.Open(dir, vlog.Options{SegmentMaxBytes: opts.ValueLogSegmentMaxBytes})
	if err != nil {
		for _, opened := range sstables {
			_ = opened.Close()
		}
		return nil, err
	}

	memtable := skiplist.New(memtableArenaSize(opts.MemtableMaxBytes))
	var memtableBytes int64
	for _, path := range walPaths {
		if err := replayWAL(path, func(key, value []byte) error {
			stored, err := storeValueForLSM(valueLog, opts.ValueThreshold, key, value)
			if err != nil {
				return err
			}
			memtable.Put(key, stored)
			memtableBytes += memtableEntrySize(key, value)
			return nil
		}); err != nil {
			_ = valueLog.Close()
			for _, opened := range sstables {
				_ = opened.Close()
			}
			return nil, err
		}
	}

	activeWal, err := createWAL(dir, opts.WALSyncInterval)
	if err != nil {
		_ = valueLog.Close()
		for _, opened := range sstables {
			_ = opened.Close()
		}
		return nil, err
	}

	currentWalPaths := append([]string(nil), walPaths...)
	currentWalPaths = append(currentWalPaths, activeWal.path)
	db := &DB{
		dir:              dir,
		memtableMaxBytes: opts.MemtableMaxBytes,
		walSyncInterval:  opts.WALSyncInterval,
		valueThreshold:   opts.ValueThreshold,
		asyncWrites:      opts.AsyncWrites,
		writeBatchWindow: opts.WriteBatchWindow,
		memtable:         memtable,
		memtableBytes:    memtableBytes,
		activeWal:        activeWal,
		currentWalPaths:  currentWalPaths,
		sstables:         sstables,
		valueLog:         valueLog,
		flushFn:          flushFn,
		writeCh:          make(chan *writeRequest, 256),
		flushCh:          make(chan *immutableMemtable, 1),
	}
	db.publishReadState()
	db.writeWg.Add(1)
	go db.writeLoop()
	db.flushWg.Add(1)
	go db.flushLoop()

	return db, nil
}

func memtableArenaSize(maxBytes int64) int64 {
	sz := maxBytes * 2
	if sz < minMemtableArenaSize {
		return minMemtableArenaSize
	}
	return sz
}

func (db *DB) Put(key, value []byte) error {
	req := acquireWriteRequest(key, value)

	db.submitMu.Lock()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		db.submitMu.Unlock()
		req.finish(errors.New("seol: db is closed"))
		return req.wait()
	}
	if db.flushErr != nil {
		err := db.flushErr
		db.mu.RUnlock()
		db.submitMu.Unlock()
		req.finish(err)
		return req.wait()
	}
	db.mu.RUnlock()

	db.writeCh <- req
	db.submitMu.Unlock()
	return req.wait()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	state := db.currentReadState()
	if state.closed {
		return nil, errors.New("seol: db is closed")
	}

	if state.memtable != nil {
		if stored := state.memtable.Get(key); stored != nil {
			return resolveStoredValue(state.valueLog, stored)
		}
	}
	for i := len(state.immutable) - 1; i >= 0; i-- {
		if stored := state.immutable[i].table.Get(key); stored != nil {
			return resolveStoredValue(state.valueLog, stored)
		}
	}
	for _, sst := range state.sstables {
		stored, err := sst.Get(key)
		if err != nil {
			return nil, err
		}
		if stored != nil {
			return resolveStoredValue(state.valueLog, stored)
		}
	}

	return nil, nil
}

func (db *DB) Close() error {
	db.submitMu.Lock()
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		db.submitMu.Unlock()
		return nil
	}
	db.closed = true
	db.publishReadStateLocked()
	db.mu.Unlock()
	db.writeCh <- nil
	db.submitMu.Unlock()

	db.writeWg.Wait()

	db.mu.Lock()

	var toFlush *immutableMemtable
	if db.memtable != nil && !db.memtable.Empty() {
		if err := db.activeWal.close(); err != nil {
			db.mu.Unlock()
			return fmt.Errorf("seol: close wal: %w", err)
		}
		toFlush = &immutableMemtable{
			table:    db.memtable,
			walPaths: append([]string(nil), db.currentWalPaths...),
		}
		db.immutable = append(db.immutable, toFlush)
	} else if db.activeWal != nil {
		activeWal := db.activeWal
		db.activeWal = nil
		db.currentWalPaths = nil
		db.memtable = nil
		db.mu.Unlock()
		err := activeWal.close()
		if removeErr := os.Remove(activeWal.path); err == nil && removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = removeErr
		}
		close(db.flushCh)
		db.flushWg.Wait()

		db.mu.Lock()
		defer db.mu.Unlock()
		var closeErr error
		if db.flushErr != nil {
			closeErr = db.flushErr
		}
		if err != nil && closeErr == nil {
			closeErr = err
		}
		if db.valueLog != nil {
			if closeVLogErr := db.valueLog.Close(); closeVLogErr != nil && closeErr == nil {
				closeErr = closeVLogErr
			}
		}
		for _, sst := range db.sstables {
			if closeSSTErr := sst.Close(); closeSSTErr != nil && closeErr == nil {
				closeErr = closeSSTErr
			}
		}
		return closeErr
	}

	db.memtable = nil
	db.activeWal = nil
	db.currentWalPaths = nil
	db.mu.Unlock()

	if toFlush != nil {
		db.flushCh <- toFlush
	}
	close(db.flushCh)
	db.flushWg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()

	var closeErr error
	if db.flushErr != nil {
		closeErr = db.flushErr
	}
	if db.valueLog != nil {
		if err := db.valueLog.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	for _, sst := range db.sstables {
		if err := sst.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}

	return closeErr
}

func (db *DB) writeLoop() {
	defer db.writeWg.Done()

	batch := make([]*writeRequest, 0, 64)
	walBatch := make([]walBatchEntry, 0, 64)
	storedBatch := make([][]byte, 0, 64)

	for {
		req := <-db.writeCh
		if req == nil {
			return
		}

		batch = append(batch[:0], req)
		closing := false
		for len(batch) < cap(batch) {
			select {
			case req = <-db.writeCh:
				if req == nil {
					closing = true
					goto processBatch
				}
				batch = append(batch, req)
			default:
				goto processBatch
			}
		}

		if len(batch) == 1 && db.writeBatchWindow > 0 {
			timer := time.NewTimer(db.writeBatchWindow)
			for len(batch) < cap(batch) {
				select {
				case req = <-db.writeCh:
					if req == nil {
						closing = true
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}
						goto processBatch
					}
					batch = append(batch, req)
				case <-timer.C:
					goto processBatch
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

	processBatch:
		db.processWriteBatch(batch, &walBatch, &storedBatch)
		if closing {
			return
		}
	}
}

func (db *DB) processWriteBatch(batch []*writeRequest, walBatchBuf *[]walBatchEntry, storedBatchBuf *[][]byte) {
	for len(batch) > 0 {
		db.mu.RLock()
		flushErr := db.flushErr
		valueLog := db.valueLog
		valueThreshold := db.valueThreshold
		activeWal := db.activeWal
		memtable := db.memtable
		db.mu.RUnlock()

		if flushErr != nil {
			failWriteRequests(batch, flushErr)
			return
		}
		if activeWal == nil || memtable == nil {
			failWriteRequests(batch, errors.New("seol: db is closed"))
			return
		}

		if cap(*walBatchBuf) < len(batch) {
			*walBatchBuf = make([]walBatchEntry, 0, len(batch))
		}
		if cap(*storedBatchBuf) < len(batch) {
			*storedBatchBuf = make([][]byte, 0, len(batch))
		}
		walBatch := (*walBatchBuf)[:0]
		storedBatch := (*storedBatchBuf)[:0]
		projectedBytes := db.memtableBytes
		prepared := 0
		crossing := false
		var prepErr error

		for prepared < len(batch) {
			req := batch[prepared]
			stored, err := storeValueForLSM(valueLog, valueThreshold, req.key, req.value)
			if err != nil {
				prepErr = db.setFlushErrIfNil(err)
				break
			}

			walBatch = append(walBatch, walBatchEntry{key: req.key, value: req.value})
			storedBatch = append(storedBatch, stored)
			projectedBytes += memtableEntrySize(req.key, req.value)
			prepared++
			if projectedBytes >= db.memtableMaxBytes {
				crossing = true
				break
			}
		}

		if prepared == 0 {
			failWriteRequests(batch, prepErr)
			return
		}

		waitForSync := !db.asyncWrites || db.walSyncInterval <= 0
		if err := activeWal.appendBatch(walBatch[:prepared], waitForSync); err != nil {
			err = db.setFlushErrIfNil(fmt.Errorf("seol: append wal: %w", err))
			failWriteRequests(batch, err)
			return
		}

		for i := 0; i < prepared; i++ {
			req := batch[i]
			memtable.Put(req.key, storedBatch[i])
			db.memtableBytes += memtableEntrySize(req.key, req.value)
			if crossing && i == prepared-1 {
				continue
			}
			req.finish(nil)
		}

		if crossing {
			if err := db.rotateMemtable(); err != nil {
				err = db.setFlushErrIfNil(err)
				batch[prepared-1].finish(err)
				failWriteRequests(batch[prepared:], err)
				return
			}
			batch[prepared-1].finish(nil)
		}

		batch = batch[prepared:]
		if prepErr != nil {
			failWriteRequests(batch, prepErr)
			return
		}
	}
}

func (db *DB) rotateMemtable() error {
	nextWal, err := createWAL(db.dir, db.walSyncInterval)
	if err != nil {
		return fmt.Errorf("seol: rotate wal: %w", err)
	}
	if err := db.activeWal.close(); err != nil {
		_ = nextWal.close()
		_ = os.Remove(nextWal.path)
		return fmt.Errorf("seol: close wal: %w", err)
	}

	toFlush := &immutableMemtable{
		table:    db.memtable,
		walPaths: append([]string(nil), db.currentWalPaths...),
	}
	newMemtable := skiplist.New(memtableArenaSize(db.memtableMaxBytes))

	db.mu.Lock()
	db.immutable = append(db.immutable, toFlush)
	db.memtable = newMemtable
	db.activeWal = nextWal
	db.currentWalPaths = []string{nextWal.path}
	db.publishReadStateLocked()
	db.mu.Unlock()

	db.memtableBytes = 0
	db.flushCh <- toFlush
	return nil
}

func (db *DB) setFlushErrIfNil(err error) error {
	db.mu.Lock()
	if db.flushErr == nil {
		db.flushErr = err
	}
	err = db.flushErr
	db.mu.Unlock()
	return err
}

func failWriteRequests(batch []*writeRequest, err error) {
	for _, req := range batch {
		req.finish(err)
	}
}

func (db *DB) flushLoop() {
	defer db.flushWg.Done()

	for imm := range db.flushCh {
		if err := db.valueLog.Sync(); err != nil {
			db.mu.Lock()
			if db.flushErr == nil {
				db.flushErr = fmt.Errorf("seol: sync value log: %w", err)
			}
			db.mu.Unlock()
			continue
		}
		sst, err := db.flushFn(db.dir, imm.table)

		db.mu.Lock()
		if err != nil {
			if db.flushErr == nil {
				db.flushErr = fmt.Errorf("seol: flush memtable: %w", err)
			}
			db.mu.Unlock()
			continue
		}

		idx := -1
		for i := range db.immutable {
			if db.immutable[i] == imm {
				idx = i
				break
			}
		}
		if idx >= 0 {
			db.immutable = append(db.immutable[:idx], db.immutable[idx+1:]...)
		}
		db.sstables = append([]*sstable.Table{sst}, db.sstables...)
		db.publishReadStateLocked()
		db.mu.Unlock()

		for _, path := range imm.walPaths {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				db.mu.Lock()
				if db.flushErr == nil {
					db.flushErr = fmt.Errorf("seol: remove wal %s: %w", filepath.Base(path), err)
				}
				db.mu.Unlock()
				break
			}
		}
	}
}

func (db *DB) currentReadState() *dbReadState {
	if state := db.readState.Load(); state != nil {
		return state
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	state := db.snapshotReadStateLocked()
	db.readState.Store(state)
	return state
}

func (db *DB) publishReadState() {
	db.mu.Lock()
	db.publishReadStateLocked()
	db.mu.Unlock()
}

func (db *DB) publishReadStateLocked() {
	db.readState.Store(db.snapshotReadStateLocked())
}

func (db *DB) snapshotReadStateLocked() *dbReadState {
	state := &dbReadState{
		closed:   db.closed,
		memtable: db.memtable,
		valueLog: db.valueLog,
	}
	if len(db.immutable) > 0 {
		state.immutable = append([]*immutableMemtable(nil), db.immutable...)
	}
	if len(db.sstables) > 0 {
		state.sstables = append([]*sstable.Table(nil), db.sstables...)
	}
	return state
}
