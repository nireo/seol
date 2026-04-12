package seol

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nireo/seol/skiplist"
	"github.com/nireo/seol/sstable"
)

func TestDBOpenCloseReload(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 10}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	if err := db.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put alpha: %v", err)
	}
	if err := db.Put([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("put beta: %v", err)
	}

	if got, err := db.Get([]byte("alpha")); err != nil || !bytes.Equal(got, []byte("one")) {
		if err != nil {
			t.Fatalf("get alpha before close: %v", err)
		}
		t.Fatalf("get alpha before close: got %q, want %q", got, []byte("one"))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	assertNoWALFiles(t, dir)

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if len(reopened.sstables) != 1 {
		t.Fatalf("sstable count after reopen: got %d, want 1", len(reopened.sstables))
	}
	if got, err := reopened.Get([]byte("alpha")); err != nil || !bytes.Equal(got, []byte("one")) {
		if err != nil {
			t.Fatalf("get alpha after reopen: %v", err)
		}
		t.Fatalf("get alpha after reopen: got %q, want %q", got, []byte("one"))
	}
	if got, err := reopened.Get([]byte("beta")); err != nil || !bytes.Equal(got, []byte("two")) {
		if err != nil {
			t.Fatalf("get beta after reopen: %v", err)
		}
		t.Fatalf("get beta after reopen: got %q, want %q", got, []byte("two"))
	}
}

func TestDBRecoversFromWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	if err := db.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("put alpha: %v", err)
	}
	if err := db.Put([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("put beta: %v", err)
	}

	stopDBWithoutFlush(t, db)

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if got, err := reopened.Get([]byte("alpha")); err != nil || !bytes.Equal(got, []byte("one")) {
		if err != nil {
			t.Fatalf("get alpha after WAL recovery: %v", err)
		}
		t.Fatalf("get alpha after WAL recovery: got %q, want %q", got, []byte("one"))
	}
	if got, err := reopened.Get([]byte("beta")); err != nil || !bytes.Equal(got, []byte("two")) {
		if err != nil {
			t.Fatalf("get beta after WAL recovery: %v", err)
		}
		t.Fatalf("get beta after WAL recovery: got %q, want %q", got, []byte("two"))
	}
}

func TestDBStoresValueRefsInMemtable(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20, ValueThreshold: 32}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	key := []byte("alpha")
	value := bytes.Repeat([]byte{'x'}, 128)
	if err := db.Put(key, value); err != nil {
		t.Fatalf("put alpha: %v", err)
	}

	stored := db.memtable.Get(key)
	if bytes.Equal(stored, value) {
		t.Fatalf("memtable stored inline value, expected encoded reference")
	}
	if _, ok, err := decodeValueRef(stored); err != nil || !ok {
		if err != nil {
			t.Fatalf("decodeValueRef: %v", err)
		}
		t.Fatalf("memtable value is not an encoded value reference")
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get alpha: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("get alpha: got %d bytes, want %d", len(got), len(value))
	}
}

func TestDBStoresSmallValuesInline(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20, ValueThreshold: 64}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	key := []byte("alpha")
	value := []byte("small-value")
	if err := db.Put(key, value); err != nil {
		t.Fatalf("put alpha: %v", err)
	}

	stored := db.memtable.Get(key)
	if !bytes.Equal(stored, value) {
		t.Fatalf("memtable stored value: got %q, want %q", stored, value)
	}
	if _, ok, err := decodeValueRef(stored); err != nil || ok {
		if err != nil {
			t.Fatalf("decodeValueRef: %v", err)
		}
		t.Fatalf("small value should stay inline")
	}
}

func TestDBDefaultThresholdKeepsTwoKiBValuesInline(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	key := []byte("alpha")
	value := bytes.Repeat([]byte{'x'}, 2<<10)
	if err := db.Put(key, value); err != nil {
		t.Fatalf("put alpha: %v", err)
	}

	stored := db.memtable.Get(key)
	if !bytes.Equal(stored, value) {
		t.Fatalf("memtable stored value length: got %d, want %d", len(stored), len(value))
	}
	if _, ok, err := decodeValueRef(stored); err != nil || ok {
		if err != nil {
			t.Fatalf("decodeValueRef: %v", err)
		}
		t.Fatalf("2 KiB value should stay inline with default threshold")
	}
}

func TestDBDefaultThresholdOffloadsFourKiBValues(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	key := []byte("alpha")
	value := bytes.Repeat([]byte{'x'}, 4<<10)
	if err := db.Put(key, value); err != nil {
		t.Fatalf("put alpha: %v", err)
	}

	stored := db.memtable.Get(key)
	if bytes.Equal(stored, value) {
		t.Fatalf("memtable stored inline value, expected encoded reference")
	}
	if _, ok, err := decodeValueRef(stored); err != nil || !ok {
		if err != nil {
			t.Fatalf("decodeValueRef: %v", err)
		}
		t.Fatalf("4 KiB value should be stored as a value reference with default threshold")
	}
}

func TestDBDeleteHidesMemtableValueImmediately(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20, ValueThreshold: 128}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	key := []byte("alpha")
	value := bytes.Repeat([]byte{'x'}, 512)
	if err := db.Put(key, value); err != nil {
		t.Fatalf("put alpha: %v", err)
	}
	if err := db.Delete(key); err != nil {
		t.Fatalf("delete alpha: %v", err)
	}

	if got, err := db.Get(key); err != nil || got != nil {
		if err != nil {
			t.Fatalf("get alpha after delete: %v", err)
		}
		t.Fatalf("get alpha after delete: got %q, want nil", got)
	}

	stored := db.memtable.Get(key)
	if !isTombstoneValue(stored) {
		t.Fatalf("memtable stored value after delete is not a tombstone")
	}
}

func TestDBDeleteShadowsOlderSSTValueAfterReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 128, ValueThreshold: 128}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	key := []byte("shared")
	value := bytes.Repeat([]byte{'o'}, 512)
	if err := db.Put(key, value); err != nil {
		t.Fatalf("put shared: %v", err)
	}
	waitForDBState(t, db, func(db *DB) bool {
		return len(db.sstables) == 1
	})

	if err := db.Delete(key); err != nil {
		t.Fatalf("delete shared: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := OpenWithOptions(dir, Options{ValueThreshold: 128})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if got, err := reopened.Get(key); err != nil || got != nil {
		if err != nil {
			t.Fatalf("get shared after reopen: %v", err)
		}
		t.Fatalf("get shared after reopen: got %q, want nil", got)
	}
}

func TestDBDeleteSurvivesWALReplay(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20, ValueThreshold: 128}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	key := []byte("alpha")
	value := bytes.Repeat([]byte{'x'}, 512)
	if err := db.Put(key, value); err != nil {
		t.Fatalf("put alpha: %v", err)
	}
	if err := db.Delete(key); err != nil {
		t.Fatalf("delete alpha: %v", err)
	}

	stopDBWithoutFlush(t, db)

	reopened, err := OpenWithOptions(dir, Options{MemtableMaxBytes: 1 << 20, ValueThreshold: 128})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if got, err := reopened.Get(key); err != nil || got != nil {
		if err != nil {
			t.Fatalf("get alpha after WAL replay: %v", err)
		}
		t.Fatalf("get alpha after WAL replay: got %q, want nil", got)
	}

	stored := reopened.memtable.Get(key)
	if !isTombstoneValue(stored) {
		t.Fatalf("replayed memtable value is not a tombstone")
	}
}

func TestDBReplaysSmallValuesInlineFromWAL(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20, ValueThreshold: 64}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	key := []byte("alpha")
	value := []byte("small-value")
	if err := db.Put(key, value); err != nil {
		t.Fatalf("put alpha: %v", err)
	}

	stopDBWithoutFlush(t, db)

	reopened, err := OpenWithOptions(dir, Options{MemtableMaxBytes: 1 << 20, ValueThreshold: 64})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	stored := reopened.memtable.Get(key)
	if !bytes.Equal(stored, value) {
		t.Fatalf("replayed memtable stored value: got %q, want %q", stored, value)
	}
	if _, ok, err := decodeValueRef(stored); err != nil || ok {
		if err != nil {
			t.Fatalf("decodeValueRef replayed: %v", err)
		}
		t.Fatalf("replayed small value should stay inline")
	}

	got, err := reopened.Get(key)
	if err != nil {
		t.Fatalf("get alpha after reopen: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("get alpha after reopen: got %q, want %q", got, value)
	}
}

func TestDBAsyncWritesReadYourWriteAndPersistOnClose(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20, WALSyncInterval: 50 * time.Millisecond, AsyncWrites: true}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	key := []byte("alpha")
	value := bytes.Repeat([]byte{'v'}, 128)
	if err := db.Put(key, value); err != nil {
		t.Fatalf("put alpha: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get alpha before close: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("get alpha before close: got %d bytes, want %d", len(got), len(value))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := OpenWithOptions(dir, Options{WALSyncInterval: 50 * time.Millisecond, AsyncWrites: true})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	got, err = reopened.Get(key)
	if err != nil {
		t.Fatalf("get alpha after reopen: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("get alpha after reopen: got %d bytes, want %d", len(got), len(value))
	}
}

func TestDBConcurrentPutAndCloseDoesNotDeadlock(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_ = db.Put([]byte("key-"+strings.Repeat("x", i%4)+string(rune('a'+i%26))), []byte("value"))
		}(i)
	}

	close(start)
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()

	putsDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(putsDone)
	}()

	select {
	case <-putsDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("concurrent puts did not complete")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close db: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("close did not complete")
	}
}

func TestDBReadsFromImmutableWhileFlushInProgress(t *testing.T) {
	dir := t.TempDir()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	flushFn := func(baseDir string, sk *skiplist.Skiplist) (*sstable.Table, error) {
		started <- struct{}{}
		<-release
		return sstable.Flush(baseDir, sk)
	}

	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 10}, flushFn)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	oldValue := bytes.Repeat([]byte{'a'}, 1500)
	if err := db.Put([]byte("alpha"), oldValue); err != nil {
		t.Fatalf("put alpha: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatalf("flush did not start")
	}

	if got, err := db.Get([]byte("alpha")); err != nil || !bytes.Equal(got, oldValue) {
		if err != nil {
			t.Fatalf("get alpha during flush: %v", err)
		}
		t.Fatalf("get alpha during flush: got %d bytes, want %d", len(got), len(oldValue))
	}

	newValue := []byte("fresh")
	if err := db.Put([]byte("beta"), newValue); err != nil {
		t.Fatalf("put beta: %v", err)
	}
	if got, err := db.Get([]byte("beta")); err != nil || !bytes.Equal(got, newValue) {
		if err != nil {
			t.Fatalf("get beta during flush: %v", err)
		}
		t.Fatalf("get beta during flush: got %q, want %q", got, newValue)
	}

	close(release)
	waitForDBState(t, db, func(db *DB) bool {
		return len(db.sstables) == 1 && len(db.immutable) == 0
	})

	if got, err := db.Get([]byte("alpha")); err != nil || !bytes.Equal(got, oldValue) {
		if err != nil {
			t.Fatalf("get alpha after flush: %v", err)
		}
		t.Fatalf("get alpha after flush: got %d bytes, want %d", len(got), len(oldValue))
	}
	if got, err := db.Get([]byte("beta")); err != nil || !bytes.Equal(got, newValue) {
		if err != nil {
			t.Fatalf("get beta after flush: %v", err)
		}
		t.Fatalf("get beta after flush: got %q, want %q", got, newValue)
	}
}

func TestDBReadsNewestSSTableFirst(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 128}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	oldValue := bytes.Repeat([]byte{'o'}, 512)
	newValue := bytes.Repeat([]byte{'n'}, 512)
	if err := db.Put([]byte("shared"), oldValue); err != nil {
		t.Fatalf("put old value: %v", err)
	}
	waitForDBState(t, db, func(db *DB) bool {
		return len(db.sstables) == 1
	})

	if err := db.Put([]byte("shared"), newValue); err != nil {
		t.Fatalf("put new value: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if len(reopened.sstables) < 2 {
		t.Fatalf("sstable count after reopen: got %d, want at least 2", len(reopened.sstables))
	}
	if got, err := reopened.Get([]byte("shared")); err != nil || !bytes.Equal(got, newValue) {
		if err != nil {
			t.Fatalf("get shared after reopen: %v", err)
		}
		t.Fatalf("get shared after reopen: got %d bytes, want %d", len(got), len(newValue))
	}
}

func waitForDBState(t *testing.T, db *DB, ok func(db *DB) bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		db.mu.RLock()
		ready := ok(db)
		db.mu.RUnlock()
		if ready {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	t.Fatalf("timed out waiting for db state: immutable=%d sstables=%d flushErr=%v", len(db.immutable), len(db.sstables), db.flushErr)
}

func stopDBWithoutFlush(t *testing.T, db *DB) {
	t.Helper()

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return
	}
	db.closed = true
	db.publishReadStateLocked()
	db.mu.Unlock()

	db.waitForSubmitters()
	db.writeCh <- nil
	db.writeWg.Wait()

	db.mu.Lock()
	activeWal := db.activeWal
	db.activeWal = nil
	db.mu.Unlock()

	if activeWal != nil {
		if err := activeWal.close(); err != nil {
			t.Fatalf("close wal: %v", err)
		}
	}
	close(db.flushCh)
	db.flushWg.Wait()
	if db.valueLog != nil {
		if err := db.valueLog.Close(); err != nil {
			t.Fatalf("close value log: %v", err)
		}
	}

	db.mu.RLock()
	sstables := append([]*sstable.Table(nil), db.sstables...)
	db.mu.RUnlock()
	for _, sst := range sstables {
		if err := sst.Close(); err != nil {
			t.Fatalf("close sstable: %v", err)
		}
	}
}

func assertNoWALFiles(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".wal") {
			t.Fatalf("unexpected WAL file %q", entry.Name())
		}
	}
}
