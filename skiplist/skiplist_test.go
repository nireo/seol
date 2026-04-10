package skiplist

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeValue(t *testing.T) {
	offset := uint32(17)
	size := uint32(42)
	encoded := encodeValue(offset, size)
	gotOffset, gotSize := decodeValue(encoded)

	if gotOffset != offset || gotSize != size {
		t.Fatalf("round trip mismatch: got (%d, %d), want (%d, %d)", gotOffset, gotSize, offset, size)
	}
}

func TestSkiplistPutGetOverwrite(t *testing.T) {
	s := New(1 << 20)

	if !s.Empty() {
		t.Fatalf("new skiplist should be empty")
	}
	if got := s.Get([]byte("missing")); got != nil {
		t.Fatalf("get missing: got %q, want nil", got)
	}

	key := []byte("beta")
	val := []byte("one")
	s.Put([]byte("alpha"), []byte("first"))
	s.Put(key, val)
	s.Put([]byte("gamma"), []byte("third"))

	key[0] = 'B'
	val[0] = 'X'

	if s.Empty() {
		t.Fatalf("skiplist should not be empty after inserts")
	}
	if got := s.Get([]byte("alpha")); !bytes.Equal(got, []byte("first")) {
		t.Fatalf("get alpha: got %q, want %q", got, []byte("first"))
	}
	if got := s.Get([]byte("beta")); !bytes.Equal(got, []byte("one")) {
		t.Fatalf("get beta: got %q, want %q", got, []byte("one"))
	}
	if got := s.Get([]byte("delta")); got != nil {
		t.Fatalf("get delta: got %q, want nil", got)
	}

	s.Put([]byte("beta"), []byte("updated"))
	if got := s.Get([]byte("beta")); !bytes.Equal(got, []byte("updated")) {
		t.Fatalf("updated beta: got %q, want %q", got, []byte("updated"))
	}
	if last := s.findLast(); last == nil || !bytes.Equal(last.key(s.arena), []byte("gamma")) {
		if last == nil {
			t.Fatalf("findLast: got nil, want gamma")
		}
		t.Fatalf("findLast: got %q, want %q", last.key(s.arena), []byte("gamma"))
	}
	if got := s.MemSize(); got <= 1 {
		t.Fatalf("memSize: got %d, want > 1", got)
	}
}

func TestSkiplistIterator(t *testing.T) {
	s := New(1 << 20)
	s.Put([]byte("b"), []byte("2"))
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte("d"), []byte("4"))
	s.Put([]byte("c"), []byte("3"))

	it := s.NewIterator()
	defer it.Close()

	it.Rewind()
	var keys []string
	var values []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		values = append(values, string(it.Value()))
		it.Next()
	}
	if got, want := keys, []string{"a", "b", "c", "d"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("iteration order: got %v, want %v", got, want)
	}
	if got, want := values, []string{"1", "2", "3", "4"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("iteration values: got %v, want %v", got, want)
	}

	it.Seek([]byte("bb"))
	if !it.Valid() || !bytes.Equal(it.Key(), []byte("c")) {
		t.Fatalf("seek bb: got %q, want %q", it.Key(), []byte("c"))
	}
	if !bytes.Equal(it.Value(), []byte("3")) {
		t.Fatalf("seek bb value: got %q, want %q", it.Value(), []byte("3"))
	}

	it.SeekForPrev([]byte("bb"))
	if !it.Valid() || !bytes.Equal(it.Key(), []byte("b")) {
		t.Fatalf("seekForPrev bb: got %q, want %q", it.Key(), []byte("b"))
	}
	if !bytes.Equal(it.Value(), []byte("2")) {
		t.Fatalf("seekForPrev bb value: got %q, want %q", it.Value(), []byte("2"))
	}

	it.SeekToLast()
	if !it.Valid() || !bytes.Equal(it.Key(), []byte("d")) {
		t.Fatalf("seekToLast: got %q, want %q", it.Key(), []byte("d"))
	}
	if !bytes.Equal(it.Value(), []byte("4")) {
		t.Fatalf("seekToLast value: got %q, want %q", it.Value(), []byte("4"))
	}

	it.Prev()
	if !it.Valid() || !bytes.Equal(it.Key(), []byte("c")) {
		t.Fatalf("prev from d: got %q, want %q", it.Key(), []byte("c"))
	}
	if !bytes.Equal(it.Value(), []byte("3")) {
		t.Fatalf("prev from d value: got %q, want %q", it.Value(), []byte("3"))
	}

	it.Seek([]byte("z"))
	if it.Valid() {
		t.Fatalf("seek z: expected invalid iterator, got %q", it.Key())
	}

	it.Prev()
	if !it.Valid() || !bytes.Equal(it.Key(), []byte("d")) {
		t.Fatalf("prev from invalid: got %q, want %q", it.Key(), []byte("d"))
	}

	it.Seek([]byte("a"))
	it.Prev()
	if it.Valid() {
		t.Fatalf("prev from first: expected invalid iterator")
	}
}
