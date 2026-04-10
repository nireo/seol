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
)

const (
	defaultMemtableMaxBytes int64 = 4 << 20
	minMemtableArenaSize    int64 = 1 << 20
)

type immutableMemtable struct {
	table    *skiplist
	walPaths []string
}

type Options struct {
	MemtableMaxBytes int64
	WALSyncInterval  time.Duration
}

type DB struct {
	dir              string
	memtableMaxBytes int64
	walSyncInterval  time.Duration
	memtable         *skiplist
	activeWal        *wal
	currentWalPaths  []string
	immutable        []*immutableMemtable
	sstables         []*sstable // newest first
	flushFn          func(baseDir string, sk *skiplist) (*sstable, error)
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
	return openDB(dir, opts, flushSkiplist)
}

func openDB(dir string, opts Options, flushFn func(baseDir string, sk *skiplist) (*sstable, error)) (*DB, error) {
	if opts.MemtableMaxBytes <= 0 {
		opts.MemtableMaxBytes = defaultMemtableMaxBytes
	}
	if flushFn == nil {
		flushFn = flushSkiplist
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

	sstables := make([]*sstable, 0, len(sstPaths))
	for _, path := range sstPaths {
		sst, err := openSSTable(path)
		if err != nil {
			for _, opened := range sstables {
				_ = opened.close()
			}
			return nil, err
		}
		sstables = append(sstables, sst)
	}

	memtable := newSkiplist(memtableArenaSize(opts.MemtableMaxBytes))
	for _, path := range walPaths {
		if err := replayWAL(path, func(key, value []byte) error {
			memtable.put(key, value)
			return nil
		}); err != nil {
			for _, opened := range sstables {
				_ = opened.close()
			}
			return nil, err
		}
	}

	activeWal, err := createWAL(dir, opts.WALSyncInterval)
	if err != nil {
		for _, opened := range sstables {
			_ = opened.close()
		}
		return nil, err
	}

	currentWalPaths := append([]string(nil), walPaths...)
	currentWalPaths = append(currentWalPaths, activeWal.path)
	db := &DB{
		dir:              dir,
		memtableMaxBytes: opts.MemtableMaxBytes,
		walSyncInterval:  opts.WALSyncInterval,
		memtable:         memtable,
		activeWal:        activeWal,
		currentWalPaths:  currentWalPaths,
		sstables:         sstables,
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
	if err := db.activeWal.appendPut(key, value); err != nil {
		db.mu.Unlock()
		return fmt.Errorf("seol: append wal: %w", err)
	}

	db.memtable.put(key, value)
	if db.memtable.memSize() < db.memtableMaxBytes {
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
	db.memtable = newSkiplist(memtableArenaSize(db.memtableMaxBytes))
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
	sstables := append([]*sstable(nil), db.sstables...)
	db.mu.RUnlock()

	if value := memtable.get(key); value != nil {
		return value, nil
	}
	for i := len(immutable) - 1; i >= 0; i-- {
		if value := immutable[i].table.get(key); value != nil {
			return value, nil
		}
	}
	for _, sst := range sstables {
		value, err := sst.get(key)
		if err != nil {
			return nil, err
		}
		if value != nil {
			return value, nil
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
	if db.memtable != nil && !db.memtable.empty() {
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
		for _, sst := range db.sstables {
			if closeSSTErr := sst.close(); closeSSTErr != nil && closeErr == nil {
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
	for _, sst := range db.sstables {
		if err := sst.close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}

	return closeErr
}

func (db *DB) flushLoop() {
	defer db.wg.Done()

	for imm := range db.flushCh {
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
		db.sstables = append([]*sstable{sst}, db.sstables...)
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
