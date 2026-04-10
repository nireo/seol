package seol

import (
	"bytes"
	"os"
	"sync"
	"testing"
	"time"
)

func TestWALAppendReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := createWAL(dir, 0)
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
	w, err := createWAL(dir, 0)
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

func TestWALBatchSyncsSharedDurability(t *testing.T) {
	dir := t.TempDir()
	w, err := createWAL(dir, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("createWAL: %v", err)
	}
	defer func() { _ = w.close() }()

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := w.appendPut([]byte{byte('a' + i)}, bytes.Repeat([]byte{byte('0' + i)}, 32)); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("appendPut: %v", err)
		}
	}

	if got := w.syncCountValue(); got == 0 || got >= 8 {
		t.Fatalf("sync count: got %d, want between 1 and 7", got)
	}
}
