package seol

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestDBCompactMergesSSTablesAndKeepsNewestValues(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableMaxBytes: 1 << 20, ValueThreshold: 128}
	db, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}

	oldShared := bytes.Repeat([]byte{'o'}, 512)
	newShared := bytes.Repeat([]byte{'n'}, 512)
	oldOnly := bytes.Repeat([]byte{'a'}, 512)
	newOnly := bytes.Repeat([]byte{'b'}, 512)

	if err := db.Put([]byte("shared"), oldShared); err != nil {
		t.Fatalf("put old shared: %v", err)
	}
	if err := db.Put([]byte("old-only"), oldOnly); err != nil {
		t.Fatalf("put old-only: %v", err)
	}
	if err := db.rotateMemtable(); err != nil {
		t.Fatalf("rotateMemtable old run: %v", err)
	}
	waitForDBState(t, db, func(db *DB) bool {
		return len(db.sstables) == 1
	})

	if err := db.Put([]byte("shared"), newShared); err != nil {
		t.Fatalf("put new shared: %v", err)
	}
	if err := db.Put([]byte("new-only"), newOnly); err != nil {
		t.Fatalf("put new-only: %v", err)
	}

	compactDB, err := db.Compact()
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	defer func() { _ = compactDB.Close() }()

	if got, err := compactDB.Get([]byte("shared")); err != nil || !bytes.Equal(got, newShared) {
		if err != nil {
			t.Fatalf("get shared after compact: %v", err)
		}
		t.Fatalf("get shared after compact: got %d bytes, want %d", len(got), len(newShared))
	}
	if got, err := compactDB.Get([]byte("old-only")); err != nil || !bytes.Equal(got, oldOnly) {
		if err != nil {
			t.Fatalf("get old-only after compact: %v", err)
		}
		t.Fatalf("get old-only after compact: got %d bytes, want %d", len(got), len(oldOnly))
	}
	if got, err := compactDB.Get([]byte("new-only")); err != nil || !bytes.Equal(got, newOnly) {
		if err != nil {
			t.Fatalf("get new-only after compact: %v", err)
		}
		t.Fatalf("get new-only after compact: got %d bytes, want %d", len(got), len(newOnly))
	}

	if got := countFilesWithExt(t, dir, ".sst"); got != 1 {
		t.Fatalf("sstable count after compact: got %d, want 1", got)
	}
	if len(compactDB.sstables) != 1 {
		t.Fatalf("open sstable count after compact: got %d, want 1", len(compactDB.sstables))
	}
}

func TestDBCompactDropsTombstones(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableMaxBytes: 1 << 20, ValueThreshold: 128}
	db, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}

	deletedKey := []byte("deleted")
	liveKey := []byte("live")
	if err := db.Put(deletedKey, bytes.Repeat([]byte{'d'}, 512)); err != nil {
		t.Fatalf("put deleted: %v", err)
	}
	if err := db.Put(liveKey, bytes.Repeat([]byte{'l'}, 512)); err != nil {
		t.Fatalf("put live: %v", err)
	}
	if err := db.rotateMemtable(); err != nil {
		t.Fatalf("rotateMemtable initial run: %v", err)
	}
	waitForDBState(t, db, func(db *DB) bool {
		return len(db.sstables) == 1
	})

	if err := db.Delete(deletedKey); err != nil {
		t.Fatalf("delete deleted key: %v", err)
	}

	compactDB, err := db.Compact()
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	defer func() { _ = compactDB.Close() }()

	if got, err := compactDB.Get(deletedKey); err != nil || got != nil {
		if err != nil {
			t.Fatalf("get deleted key after compact: %v", err)
		}
		t.Fatalf("get deleted key after compact: got %q, want nil", got)
	}
	if got, err := compactDB.Get(liveKey); err != nil || got == nil {
		if err != nil {
			t.Fatalf("get live key after compact: %v", err)
		}
		t.Fatalf("get live key after compact: got nil, want value")
	}
	if gcKeyPresentInSSTs(t, compactDB, deletedKey) {
		t.Fatalf("deleted key still present in compacted sstables")
	}
	if got := countFilesWithExt(t, dir, ".sst"); got != 1 {
		t.Fatalf("sstable count after compact: got %d, want 1", got)
	}
}

func countFilesWithExt(t *testing.T, dir, ext string) int {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ext {
			continue
		}
		count++
	}
	return count
}
