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
	defaultValueThreshold         = 2 << 10
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
	dir                     string
	memtableMaxBytes        int64
	walSyncInterval         time.Duration
	valueLogSegmentMaxBytes int64
	valueThreshold          int
	asyncWrites             bool
	writeBatchWindow        time.Duration
	memtable                *skiplist.Skiplist
	memtableBytes           int64
	activeWal               *wal
	currentWalPaths         []string
	immutable               []*immutableMemtable
	sstables                []*sstable.Table // newest first
	tableMeta               []TableMeta
	manifest                *manifestStore
	valueLog                *vlog.Log
	flushFn                 func(baseDir string, sk *skiplist.Skiplist) (*sstable.Table, error)
	writeCh                 chan *writeRequest
	flushCh                 chan *immutableMemtable
	readState               atomic.Pointer[dbReadState]
	submitCond              *sync.Cond
	submitters              atomic.Int64
	commitWg                sync.WaitGroup

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

func normalizeOptions(opts Options) Options {
	if opts.MemtableMaxBytes <= 0 {
		opts.MemtableMaxBytes = defaultMemtableMaxBytes
	}
	if opts.ValueThreshold <= 0 {
		opts.ValueThreshold = defaultValueThreshold
	}
	switch {
	case opts.WriteBatchWindow < 0:
		opts.WriteBatchWindow = 0
	case opts.WriteBatchWindow == 0:
		opts.WriteBatchWindow = defaultWriteBatchWindow
	}
	return opts
}

func scanDataFiles(dir string) ([]string, []string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
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
	return sstPaths, walPaths, nil
}

func openSSTables(paths []string) ([]*sstable.Table, error) {
	sstables := make([]*sstable.Table, 0, len(paths))
	for _, path := range paths {
		sst, err := sstable.Open(path)
		if err != nil {
			closeSSTables(sstables)
			return nil, err
		}
		sstables = append(sstables, sst)
	}
	return sstables, nil
}

func openSSTablesFromMeta(dir string, tables []TableMeta) ([]*sstable.Table, error) {
	paths := make([]string, 0, len(tables))
	for _, table := range tables {
		paths = append(paths, filepath.Join(dir, table.Filename))
	}
	return openSSTables(paths)
}

func closeSSTables(sstables []*sstable.Table) {
	for _, sst := range sstables {
		_ = sst.Close()
	}
}

func closeOpenResources(valueLog *vlog.Log, sstables []*sstable.Table) {
	if valueLog != nil {
		_ = valueLog.Close()
	}
	closeSSTables(sstables)
}

func replayWALs(paths []string, valueLog *vlog.Log, valueThreshold int, memtable *skiplist.Skiplist) (int64, error) {
	var memtableBytes int64
	for _, path := range paths {
		if err := replayWAL(path, func(key, value []byte) error {
			stored, err := storeValueForLSM(valueLog, valueThreshold, key, value)
			if err != nil {
				return err
			}
			memtable.Put(key, stored)
			memtableBytes += memtableEntrySize(key, value)
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return memtableBytes, nil
}

func openDB(dir string, opts Options, flushFn func(baseDir string, sk *skiplist.Skiplist) (*sstable.Table, error)) (*DB, error) {
	opts = normalizeOptions(opts)
	if flushFn == nil {
		flushFn = sstable.Flush
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	_, walPaths, err := scanDataFiles(dir)
	if err != nil {
		return nil, err
	}
	manifest, err := openManifest(dir)
	if err != nil {
		return nil, err
	}
	tableMeta := manifest.Tables()

	sstables, err := openSSTablesFromMeta(dir, tableMeta)
	if err != nil {
		return nil, err
	}

	valueLog, err := vlog.Open(dir, vlog.Options{SegmentMaxBytes: opts.ValueLogSegmentMaxBytes})
	if err != nil {
		closeSSTables(sstables)
		return nil, err
	}

	memtable := skiplist.New(memtableArenaSize(opts.MemtableMaxBytes))
	memtableBytes, err := replayWALs(walPaths, valueLog, opts.ValueThreshold, memtable)
	if err != nil {
		closeOpenResources(valueLog, sstables)
		return nil, err
	}

	activeWal, err := createWAL(dir, opts.WALSyncInterval)
	if err != nil {
		closeOpenResources(valueLog, sstables)
		return nil, err
	}

	currentWalPaths := append([]string(nil), walPaths...)
	currentWalPaths = append(currentWalPaths, activeWal.path)
	db := &DB{
		dir:                     dir,
		memtableMaxBytes:        opts.MemtableMaxBytes,
		walSyncInterval:         opts.WALSyncInterval,
		valueLogSegmentMaxBytes: opts.ValueLogSegmentMaxBytes,
		valueThreshold:          opts.ValueThreshold,
		asyncWrites:             opts.AsyncWrites,
		writeBatchWindow:        opts.WriteBatchWindow,
		memtable:                memtable,
		memtableBytes:           memtableBytes,
		activeWal:               activeWal,
		currentWalPaths:         currentWalPaths,
		sstables:                sstables,
		tableMeta:               tableMeta,
		manifest:                manifest,
		valueLog:                valueLog,
		flushFn:                 flushFn,
		writeCh:                 make(chan *writeRequest, 256),
		flushCh:                 make(chan *immutableMemtable, 1),
	}
	db.submitCond = sync.NewCond(&db.submitMu)
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
	return db.submitWrite(key, value)
}

func (db *DB) Delete(key []byte) error {
	return db.submitWrite(key, tombstoneValue)
}

func (db *DB) submitWrite(key, value []byte) error {
	req := acquireWriteRequest(key, value)

	if !db.beginSubmit() {
		req.finish(errors.New("seol: db is closed"))
		return req.wait()
	}

	db.mu.RLock()
	if db.flushErr != nil {
		err := db.flushErr
		db.mu.RUnlock()
		db.finishSubmit()
		req.finish(err)
		return req.wait()
	}
	db.mu.RUnlock()

	db.writeCh <- req
	db.finishSubmit()
	return req.wait()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	state := db.currentReadState()
	if state.closed {
		return nil, errors.New("seol: db is closed")
	}

	if state.memtable != nil {
		if stored := state.memtable.Get(key); stored != nil {
			return readStoredValue(state.valueLog, stored)
		}
	}
	for i := len(state.immutable) - 1; i >= 0; i-- {
		if stored := state.immutable[i].table.Get(key); stored != nil {
			return readStoredValue(state.valueLog, stored)
		}
	}
	for _, sst := range state.sstables {
		stored, err := sst.Get(key)
		if err != nil {
			return nil, err
		}
		if stored != nil {
			return readStoredValue(state.valueLog, stored)
		}
	}

	return nil, nil
}

func readStoredValue(valueLog *vlog.Log, stored []byte) ([]byte, error) {
	value, deleted, err := resolveStoredValue(valueLog, stored)
	if err != nil {
		return nil, err
	}
	if deleted {
		return nil, nil
	}
	return value, nil
}

func (db *DB) Close() error {
	if !db.markClosed() {
		return nil
	}

	db.stopWrites()

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
	}

	activeWal := db.activeWal
	db.memtable = nil
	db.activeWal = nil
	db.currentWalPaths = nil
	db.mu.Unlock()

	if toFlush == nil {
		return db.closeWithoutPendingFlush(activeWal)
	}

	return db.flushPendingMemtableAndClose(toFlush)
}

func (db *DB) markClosed() bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return false
	}
	db.closed = true
	db.publishReadStateLocked()
	return true
}

func (db *DB) stopWrites() {
	db.waitForSubmitters()
	db.writeCh <- nil
	db.writeWg.Wait()
}

func (db *DB) closeWithoutPendingFlush(activeWal *wal) error {
	walCloseErr := closeAndRemoveWAL(activeWal)
	close(db.flushCh)
	db.flushWg.Wait()
	db.commitWg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.closeStorageLocked(walCloseErr)
}

func (db *DB) flushPendingMemtableAndClose(toFlush *immutableMemtable) error {
	db.flushCh <- toFlush
	close(db.flushCh)
	db.flushWg.Wait()
	db.commitWg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()
	return db.closeStorageLocked(nil)
}

func closeAndRemoveWAL(activeWal *wal) error {
	if activeWal == nil {
		return nil
	}

	err := activeWal.close()
	if removeErr := os.Remove(activeWal.path); err == nil && removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		err = removeErr
	}
	return err
}

func (db *DB) closeStorageLocked(extraErr error) error {
	closeErr := db.flushErr
	closeErr = firstError(closeErr, extraErr)
	if db.valueLog != nil {
		closeErr = firstError(closeErr, db.valueLog.Close())
	}
	for _, sst := range db.sstables {
		closeErr = firstError(closeErr, sst.Close())
	}
	return closeErr
}

func firstError(err, next error) error {
	if err != nil {
		return err
	}
	return next
}

func (db *DB) writeLoop() {
	defer db.writeWg.Done()

	batch := make([]*writeRequest, 0, 256)
	walBatch := make([]walBatchEntry, 0, 256)
	storedBatch := make([][]byte, 0, 256)

	for {
		req := <-db.writeCh
		if req == nil {
			return
		}

		batch, closing := db.collectWriteBatch(req, batch)
		db.processWriteBatch(batch, &walBatch, &storedBatch)
		if closing {
			return
		}
	}
}

func (db *DB) collectWriteBatch(first *writeRequest, batch []*writeRequest) ([]*writeRequest, bool) {
	batch = append(batch[:0], first)
	for len(batch) < cap(batch) {
		select {
		case req := <-db.writeCh:
			if req == nil {
				return batch, true
			}
			batch = append(batch, req)
		default:
			return batch, false
		}
	}
	return batch, false
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
		if err := db.syncValueLogForFlush(); err != nil {
			continue
		}

		sst, err := db.flushFn(db.dir, imm.table)
		if err != nil {
			db.setFlushErrIfNil(fmt.Errorf("seol: flush memtable: %w", err))
			continue
		}

		if err := db.installFlushedTable(imm, sst); err != nil {
			db.setFlushErrIfNil(err)
			continue
		}
		if err := removeWALFiles(imm.walPaths); err != nil {
			db.setFlushErrIfNil(err)
		}
	}
}

func (db *DB) syncValueLogForFlush() error {
	if err := db.valueLog.Sync(); err != nil {
		return db.setFlushErrIfNil(fmt.Errorf("seol: sync value log: %w", err))
	}
	return nil
}

func (db *DB) installFlushedTable(imm *immutableMemtable, sst *sstable.Table) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for i := range db.immutable {
		if db.immutable[i] != imm {
			continue
		}
		db.immutable = append(db.immutable[:i], db.immutable[i+1:]...)
		break
	}

	updatedMeta := append([]TableMeta{tableMetaFromSST(sst.Metadata(), 0)}, db.tableMeta...)
	if err := db.manifest.ReplaceTables(updatedMeta); err != nil {
		return fmt.Errorf("seol: update manifest after flush: %w", err)
	}

	db.sstables = append([]*sstable.Table{sst}, db.sstables...)
	db.tableMeta = updatedMeta
	db.publishReadStateLocked()
	return nil
}

func removeWALFiles(paths []string) error {
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("seol: remove wal %s: %w", filepath.Base(path), err)
		}
	}
	return nil
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

func (db *DB) beginSubmit() bool {
	db.submitters.Add(1)
	if db.currentReadState().closed {
		db.finishSubmit()
		return false
	}
	return true
}

func (db *DB) finishSubmit() {
	if db.submitters.Add(-1) != 0 {
		return
	}
	db.submitMu.Lock()
	db.submitCond.Broadcast()
	db.submitMu.Unlock()
}

func (db *DB) waitForSubmitters() {
	db.submitMu.Lock()
	for db.submitters.Load() != 0 {
		db.submitCond.Wait()
	}
	db.submitMu.Unlock()
}
