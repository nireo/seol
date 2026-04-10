package skiplist

import (
	"bytes"
	"testing"
)

func TestArenaInitialState(t *testing.T) {
	a := newArena(256)

	if got := a.size(); got != 1 {
		t.Fatalf("size: got %d, want 1", got)
	}
	if a.getNode(0) != nil {
		t.Fatalf("getNode(0): want nil")
	}
	if got := a.getNodeOffset(nil); got != 0 {
		t.Fatalf("getNodeOffset(nil): got %d, want 0", got)
	}
}

func TestArenaPutNodeRoundTrip(t *testing.T) {
	a := newArena(1024)
	offset := a.putNode(3)

	if offset == 0 {
		t.Fatalf("putNode returned nil offset")
	}
	if offset&uint32(nodeAlign) != 0 {
		t.Fatalf("putNode returned unaligned offset %d", offset)
	}

	n := a.getNode(offset)
	if n == nil {
		t.Fatalf("getNode(%d): got nil", offset)
	}
	if got := a.getNodeOffset(n); got != offset {
		t.Fatalf("getNodeOffset: got %d, want %d", got, offset)
	}
}

func TestArenaPutKeyAndValRoundTrip(t *testing.T) {
	a := newArena(256)
	key := []byte("abc")
	val := []byte("value")

	keyOffset := a.putKey(key)
	valOffset := a.putVal(val)

	key[0] = 'z'
	val[0] = 'X'

	if got := a.getKey(keyOffset, uint16(len(key))); !bytes.Equal(got, []byte("abc")) {
		t.Fatalf("getKey: got %q, want %q", got, []byte("abc"))
	}
	if got := a.getVal(valOffset, uint32(len(val))); !bytes.Equal(got, []byte("value")) {
		t.Fatalf("getVal: got %q, want %q", got, []byte("value"))
	}
	if keyOffset == valOffset {
		t.Fatalf("putKey and putVal returned the same offset %d", keyOffset)
	}
}
