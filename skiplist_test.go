package seol

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
	s := newSkiplist(1 << 20)

	if !s.empty() {
		t.Fatalf("new skiplist should be empty")
	}
	if got := s.get([]byte("missing")); got != nil {
		t.Fatalf("get missing: got %q, want nil", got)
	}

	key := []byte("beta")
	val := []byte("one")
	s.put([]byte("alpha"), []byte("first"))
	s.put(key, val)
	s.put([]byte("gamma"), []byte("third"))

	key[0] = 'B'
	val[0] = 'X'

	if s.empty() {
		t.Fatalf("skiplist should not be empty after inserts")
	}
	if got := s.get([]byte("alpha")); !bytes.Equal(got, []byte("first")) {
		t.Fatalf("get alpha: got %q, want %q", got, []byte("first"))
	}
	if got := s.get([]byte("beta")); !bytes.Equal(got, []byte("one")) {
		t.Fatalf("get beta: got %q, want %q", got, []byte("one"))
	}
	if got := s.get([]byte("delta")); got != nil {
		t.Fatalf("get delta: got %q, want nil", got)
	}

	s.put([]byte("beta"), []byte("updated"))
	if got := s.get([]byte("beta")); !bytes.Equal(got, []byte("updated")) {
		t.Fatalf("updated beta: got %q, want %q", got, []byte("updated"))
	}
	if last := s.findLast(); last == nil || !bytes.Equal(last.key(s.arena), []byte("gamma")) {
		if last == nil {
			t.Fatalf("findLast: got nil, want gamma")
		}
		t.Fatalf("findLast: got %q, want %q", last.key(s.arena), []byte("gamma"))
	}
	if got := s.memSize(); got <= 1 {
		t.Fatalf("memSize: got %d, want > 1", got)
	}
}

func TestSkiplistIterator(t *testing.T) {
	s := newSkiplist(1 << 20)
	s.put([]byte("b"), []byte("2"))
	s.put([]byte("a"), []byte("1"))
	s.put([]byte("d"), []byte("4"))
	s.put([]byte("c"), []byte("3"))

	it := s.newIterator()
	defer it.close()

	it.rewind()
	var keys []string
	var values []string
	for it.valid() {
		keys = append(keys, string(it.key()))
		values = append(values, string(it.value()))
		it.next()
	}
	if got, want := keys, []string{"a", "b", "c", "d"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("iteration order: got %v, want %v", got, want)
	}
	if got, want := values, []string{"1", "2", "3", "4"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("iteration values: got %v, want %v", got, want)
	}

	it.seek([]byte("bb"))
	if !it.valid() || !bytes.Equal(it.key(), []byte("c")) {
		t.Fatalf("seek bb: got %q, want %q", it.key(), []byte("c"))
	}
	if !bytes.Equal(it.value(), []byte("3")) {
		t.Fatalf("seek bb value: got %q, want %q", it.value(), []byte("3"))
	}

	it.seekForPrev([]byte("bb"))
	if !it.valid() || !bytes.Equal(it.key(), []byte("b")) {
		t.Fatalf("seekForPrev bb: got %q, want %q", it.key(), []byte("b"))
	}
	if !bytes.Equal(it.value(), []byte("2")) {
		t.Fatalf("seekForPrev bb value: got %q, want %q", it.value(), []byte("2"))
	}

	it.seekToLast()
	if !it.valid() || !bytes.Equal(it.key(), []byte("d")) {
		t.Fatalf("seekToLast: got %q, want %q", it.key(), []byte("d"))
	}
	if !bytes.Equal(it.value(), []byte("4")) {
		t.Fatalf("seekToLast value: got %q, want %q", it.value(), []byte("4"))
	}

	it.prev()
	if !it.valid() || !bytes.Equal(it.key(), []byte("c")) {
		t.Fatalf("prev from d: got %q, want %q", it.key(), []byte("c"))
	}
	if !bytes.Equal(it.value(), []byte("3")) {
		t.Fatalf("prev from d value: got %q, want %q", it.value(), []byte("3"))
	}

	it.seek([]byte("z"))
	if it.valid() {
		t.Fatalf("seek z: expected invalid iterator, got %q", it.key())
	}

	it.prev()
	if !it.valid() || !bytes.Equal(it.key(), []byte("d")) {
		t.Fatalf("prev from invalid: got %q, want %q", it.key(), []byte("d"))
	}

	it.seek([]byte("a"))
	it.prev()
	if it.valid() {
		t.Fatalf("prev from first: expected invalid iterator")
	}
}
