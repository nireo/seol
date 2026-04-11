package seol

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nireo/seol/skiplist"
	"github.com/nireo/seol/sstable"
	"github.com/nireo/seol/vlog"
)

const (
	defaultMemtableMaxBytes int64 = 4 << 20
	minMemtableArenaSize    int64 = 1 << 20
	defaultValueThreshold         = 32
)

type immutableMemtable struct {
	table    *skiplist.Skiplist
	walPaths []string
}

type Options struct {
	MemtableMaxBytes        int64
	WALSyncInterval         time.Duration
	ValueLogSegmentMaxBytes int64
	ValueThreshold          int
}

type DB struct {
	dir              string
	memtableMaxBytes int64
	walSyncInterval  time.Duration
	valueThreshold   int
	memtable         *skiplist.Skiplist
	memtableBytes    int64
	activeWal        *wal
	currentWalPaths  []string
	immutable        []*immutableMemtable
	sstables         []*sstable.Table // newest first
	valueLog         *vlog.Log
	flushFn          func(baseDir string, sk *skiplist.Skiplist) (*sstable.Table, error)
	flushCh          chan *immutableMemtable

	mu       sync.RWMutex
	flushErr error
	closed   bool
	wg       sync.WaitGroup
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
		memtable:         memtable,
		memtableBytes:    memtableBytes,
		activeWal:        activeWal,
		currentWalPaths:  currentWalPaths,
		sstables:         sstables,
		valueLog:         valueLog,
		flushFn:          flushFn,
		flushCh:          make(chan *immutableMemtable, 1),
	}
	db.wg.Add(1)
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
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return errors.New("seol: db is closed")
	}
	if db.flushErr != nil {
		err := db.flushErr
		db.mu.Unlock()
		return err
	}
	stored, err := storeValueForLSM(db.valueLog, db.valueThreshold, key, value)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if err := db.activeWal.appendPut(key, value); err != nil {
		db.mu.Unlock()
		return fmt.Errorf("seol: append wal: %w", err)
	}

	db.memtable.Put(key, stored)
	db.memtableBytes += memtableEntrySize(key, value)
	if db.memtableBytes < db.memtableMaxBytes {
		db.mu.Unlock()
		return nil
	}

	nextWal, err := createWAL(db.dir, db.walSyncInterval)
	if err != nil {
		db.mu.Unlock()
		return fmt.Errorf("seol: rotate wal: %w", err)
	}
	if err := db.activeWal.close(); err != nil {
		_ = nextWal.close()
		_ = os.Remove(nextWal.path)
		db.mu.Unlock()
		return fmt.Errorf("seol: close wal: %w", err)
	}

	toFlush := &immutableMemtable{
		table:    db.memtable,
		walPaths: append([]string(nil), db.currentWalPaths...),
	}
	db.immutable = append(db.immutable, toFlush)
	db.memtable = skiplist.New(memtableArenaSize(db.memtableMaxBytes))
	db.memtableBytes = 0
	db.activeWal = nextWal
	db.currentWalPaths = []string{nextWal.path}
	db.mu.Unlock()

	db.flushCh <- toFlush
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errors.New("seol: db is closed")
	}
	memtable := db.memtable
	immutable := append([]*immutableMemtable(nil), db.immutable...)
	sstables := append([]*sstable.Table(nil), db.sstables...)
	db.mu.RUnlock()

	if stored := memtable.Get(key); stored != nil {
		return resolveStoredValue(db.valueLog, stored)
	}
	for i := len(immutable) - 1; i >= 0; i-- {
		if stored := immutable[i].table.Get(key); stored != nil {
			return resolveStoredValue(db.valueLog, stored)
		}
	}
	for _, sst := range sstables {
		stored, err := sst.Get(key)
		if err != nil {
			return nil, err
		}
		if stored != nil {
			return resolveStoredValue(db.valueLog, stored)
		}
	}

	return nil, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true

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
		db.mu.Unlock()
		err := activeWal.close()
		if removeErr := os.Remove(activeWal.path); err == nil && removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = removeErr
		}
		close(db.flushCh)
		db.wg.Wait()

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
	db.wg.Wait()

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

func (db *DB) flushLoop() {
	defer db.wg.Done()

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
