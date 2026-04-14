package seol

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/nireo/seol/sstable"
)

func TestDBCreatesManifestOnOpen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	state := readManifestState(t, dir)
	if state.Version != manifestVersion {
		t.Fatalf("manifest version: got %d, want %d", state.Version, manifestVersion)
	}
	if len(state.Tables) != 0 {
		t.Fatalf("manifest table count: got %d, want 0", len(state.Tables))
	}
}

func TestDBManifestTracksFlushedTableMeta(t *testing.T) {
	dir := t.TempDir()
	db, err := openDB(dir, Options{MemtableMaxBytes: 1 << 20, ValueThreshold: 2 << 10}, sstable.Flush)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}

	if err := db.Put([]byte("alpha"), bytes.Repeat([]byte{'a'}, 64)); err != nil {
		t.Fatalf("put alpha: %v", err)
	}
	if err := db.Put([]byte("gamma"), bytes.Repeat([]byte{'g'}, 64)); err != nil {
		t.Fatalf("put gamma: %v", err)
	}
	if err := db.Put([]byte("omega"), bytes.Repeat([]byte{'o'}, 64)); err != nil {
		t.Fatalf("put omega: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	state := readManifestState(t, dir)
	if len(state.Tables) != 1 {
		t.Fatalf("manifest table count: got %d, want 1", len(state.Tables))
	}
	meta := state.Tables[0]
	if meta.Level != 0 {
		t.Fatalf("manifest level: got %d, want 0", meta.Level)
	}
	if got := string(meta.Smallest); got != "alpha" {
		t.Fatalf("manifest smallest: got %q, want %q", got, "alpha")
	}
	if got := string(meta.Largest); got != "omega" {
		t.Fatalf("manifest largest: got %q, want %q", got, "omega")
	}
	if meta.SizeBytes <= 0 {
		t.Fatalf("manifest size bytes: got %d, want > 0", meta.SizeBytes)
	}
	if _, err := os.Stat(filepath.Join(dir, meta.Filename)); err != nil {
		t.Fatalf("manifest table file missing: %v", err)
	}
}

func TestDBCompactionRewritesManifest(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenWithOptions(dir, Options{MemtableMaxBytes: 128, ValueThreshold: 2 << 10})
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}

	if err := db.Put([]byte("shared"), bytes.Repeat([]byte{'a'}, 64)); err != nil {
		t.Fatalf("put shared a: %v", err)
	}
	if err := db.Put([]byte("only-old"), bytes.Repeat([]byte{'o'}, 64)); err != nil {
		t.Fatalf("put only-old: %v", err)
	}
	waitForDBState(t, db, func(db *DB) bool {
		return len(db.sstables) >= 1
	})
	if err := db.Put([]byte("shared"), bytes.Repeat([]byte{'b'}, 64)); err != nil {
		t.Fatalf("put shared b: %v", err)
	}
	compactDB, err := db.Compact()
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	defer func() { _ = compactDB.Close() }()

	state := readManifestState(t, dir)
	if len(state.Tables) != 1 {
		t.Fatalf("manifest table count after compact: got %d, want 1", len(state.Tables))
	}
	meta := state.Tables[0]
	if got := string(meta.Smallest); got != "only-old" {
		t.Fatalf("manifest smallest after compact: got %q, want %q", got, "only-old")
	}
	if got := string(meta.Largest); got != "shared" {
		t.Fatalf("manifest largest after compact: got %q, want %q", got, "shared")
	}
}

func readManifestState(t *testing.T, dir string) manifestState {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, manifestFilename))
	if err != nil {
		t.Fatalf("ReadFile manifest: %v", err)
	}
	var state manifestState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("Unmarshal manifest: %v", err)
	}
	return state
}
