package seol

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nireo/seol/sstable"
)

func TestDBRunValueLogGCCleansStaleVlogData(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableMaxBytes: 128, ValueThreshold: 128}
	db, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}

	const (
		keyCount = 16
		rounds   = 4
	)
	keys := make([][]byte, keyCount)
	expected := make([][]byte, keyCount)
	for i := range keyCount {
		keys[i] = []byte(fmt.Sprintf("key-%02d", i))
	}
	for round := range rounds {
		for i := range keys {
			value := gcTestValue(round, i, 512)
			if err := db.Put(keys[i], value); err != nil {
				t.Fatalf("put round %d key %d: %v", round, i, err)
			}
			expected[i] = value
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db before gc: %v", err)
	}

	before, err := gcDirUsage(dir)
	if err != nil {
		t.Fatalf("gcDirUsage before: %v", err)
	}
	if before.vlog == 0 {
		t.Fatalf("expected stale vlog data before gc")
	}

	reopened, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("reopen before gc: %v", err)
	}
	gcDB, err := reopened.RunValueLogGC()
	if err != nil {
		t.Fatalf("RunValueLogGC: %v", err)
	}
	defer func() { _ = gcDB.Close() }()

	after, err := gcDirUsage(dir)
	if err != nil {
		t.Fatalf("gcDirUsage after: %v", err)
	}
	if after.vlog >= before.vlog {
		t.Fatalf("vlog bytes after gc: got %d, want less than %d", after.vlog, before.vlog)
	}
	if after.total >= before.total {
		t.Fatalf("total bytes after gc: got %d, want less than %d", after.total, before.total)
	}

	for i := range keys {
		got, err := gcDB.Get(keys[i])
		if err != nil {
			t.Fatalf("get key %q after gc: %v", keys[i], err)
		}
		if !bytes.Equal(got, expected[i]) {
			t.Fatalf("get key %q after gc: got %q, want %q", keys[i], got[:16], expected[i][:16])
		}
	}
}

func TestDBRunValueLogGCPreservesInlineAndVlogValues(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MemtableMaxBytes: 1 << 10, ValueThreshold: 128}
	db, err := OpenWithOptions(dir, opts)
	if err != nil {
		t.Fatalf("OpenWithOptions: %v", err)
	}

	smallKey := []byte("small")
	largeKey := []byte("large")
	smallValue := []byte("small-value")
	largeValue := gcTestValue(1, 7, 512)
	if err := db.Put(smallKey, smallValue); err != nil {
		t.Fatalf("put small: %v", err)
	}
	if err := db.Put(largeKey, largeValue); err != nil {
		t.Fatalf("put large: %v", err)
	}

	gcDB, err := db.RunValueLogGC()
	if err != nil {
		t.Fatalf("RunValueLogGC: %v", err)
	}
	defer func() { _ = gcDB.Close() }()

	if got, err := gcDB.Get(smallKey); err != nil || !bytes.Equal(got, smallValue) {
		if err != nil {
			t.Fatalf("get small after gc: %v", err)
		}
		t.Fatalf("get small after gc: got %q, want %q", got, smallValue)
	}
	if got, err := gcDB.Get(largeKey); err != nil || !bytes.Equal(got, largeValue) {
		if err != nil {
			t.Fatalf("get large after gc: %v", err)
		}
		t.Fatalf("get large after gc: got %d bytes, want %d", len(got), len(largeValue))
	}

	smallStored := gcStoredValueFromSSTs(t, gcDB, smallKey)
	if _, ok, err := decodeValueRef(smallStored); err != nil || ok {
		if err != nil {
			t.Fatalf("decodeValueRef small after gc: %v", err)
		}
		t.Fatalf("small value should stay inline after gc")
	}

	largeStored := gcStoredValueFromSSTs(t, gcDB, largeKey)
	if _, ok, err := decodeValueRef(largeStored); err != nil || !ok {
		if err != nil {
			t.Fatalf("decodeValueRef large after gc: %v", err)
		}
		t.Fatalf("large value should stay vlog-backed after gc")
	}
}

type gcFileUsage struct {
	total int64
	vlog  int64
	wal   int64
	sst   int64
}

func gcDirUsage(dir string) (gcFileUsage, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return gcFileUsage{}, err
	}

	var usage gcFileUsage
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return gcFileUsage{}, err
		}
		size := info.Size()
		usage.total += size
		switch filepath.Ext(entry.Name()) {
		case ".vlog":
			usage.vlog += size
		case ".wal":
			usage.wal += size
		case ".sst":
			usage.sst += size
		}
	}
	return usage, nil
}

func gcStoredValueFromSSTs(t *testing.T, db *DB, key []byte) []byte {
	t.Helper()

	db.mu.RLock()
	sstables := append([]*sstable.Table(nil), db.sstables...)
	db.mu.RUnlock()
	for _, table := range sstables {
		stored, err := table.Get(key)
		if err != nil {
			t.Fatalf("sstable get %q: %v", key, err)
		}
		if stored != nil {
			return stored
		}
	}
	t.Fatalf("key %q not found in sstables", key)
	return nil
}

func gcTestValue(round, index, size int) []byte {
	value := bytes.Repeat([]byte{byte('a' + round%26)}, size)
	copy(value, []byte(fmt.Sprintf("value-%02d-%02d", round, index)))
	return value
}
