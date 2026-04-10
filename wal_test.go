package seol

import (
	"bytes"
	"os"
	"testing"
)

func TestWALAppendReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := createWAL(dir)
	if err != nil {
		t.Fatalf("createWAL: %v", err)
	}

	type kv struct {
		key   []byte
		value []byte
	}
	want := []kv{
		{key: []byte("alpha"), value: []byte("one")},
		{key: []byte("beta"), value: bytes.Repeat([]byte{'b'}, 512)},
		{key: []byte("gamma"), value: []byte("three")},
	}
	for _, entry := range want {
		if err := w.appendPut(entry.key, entry.value); err != nil {
			t.Fatalf("appendPut %q: %v", entry.key, err)
		}
	}
	if err := w.close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	var got []kv
	if err := replayWAL(w.path, func(key, value []byte) error {
		got = append(got, kv{key: append([]byte(nil), key...), value: append([]byte(nil), value...)})
		return nil
	}); err != nil {
		t.Fatalf("replayWAL: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("replayed record count: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].key, want[i].key) {
			t.Fatalf("record %d key: got %q, want %q", i, got[i].key, want[i].key)
		}
		if !bytes.Equal(got[i].value, want[i].value) {
			t.Fatalf("record %d value mismatch", i)
		}
	}
}

func TestWALReplayIgnoresTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	w, err := createWAL(dir)
	if err != nil {
		t.Fatalf("createWAL: %v", err)
	}
	if err := w.appendPut([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("appendPut alpha: %v", err)
	}
	if err := w.appendPut([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("appendPut beta: %v", err)
	}
	if err := w.close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	stat, err := os.Stat(w.path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if err := os.Truncate(w.path, stat.Size()-2); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	var keys []string
	if err := replayWAL(w.path, func(key, value []byte) error {
		keys = append(keys, string(key))
		return nil
	}); err != nil {
		t.Fatalf("replayWAL: %v", err)
	}

	if got, want := keys, []string{"alpha"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("replayed keys: got %v, want %v", got, want)
	}
}
