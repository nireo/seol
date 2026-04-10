package seol

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDBOpenCloseReload(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 10}, flushSkiplist)
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
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20}, flushSkiplist)
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

func TestDBReadsFromImmutableWhileFlushInProgress(t *testing.T) {
	dir := t.TempDir()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	flushFn := func(baseDir string, sk *skiplist) (*sstable, error) {
		started <- struct{}{}
		<-release
		return flushSkiplist(baseDir, sk)
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
	db, err := openDB(dir, Options{MemtableMaxBytes: 128}, flushSkiplist)
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
	activeWal := db.activeWal
	db.activeWal = nil
	db.mu.Unlock()

	if activeWal != nil {
		if err := activeWal.close(); err != nil {
			t.Fatalf("close wal: %v", err)
		}
	}
	close(db.flushCh)
	db.wg.Wait()

	db.mu.RLock()
	sstables := append([]*sstable(nil), db.sstables...)
	db.mu.RUnlock()
	for _, sst := range sstables {
		if err := sst.close(); err != nil {
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
