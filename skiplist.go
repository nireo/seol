package seol

import (
	"math"
	"sync/atomic"
	"unsafe"
)

type value struct {
	expiresAt uint64
	value     []byte
}

type node struct {
	value     atomic.Uint64
	keyOffset uint32
	keySize   uint16
	height    uint16
	tower     [maxHeight]atomic.Uint32
}

const (
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	nodeAlign   = int(unsafe.Sizeof(uint64(0))) - 1
	maxHeight   = 20
	maxNodeSize = int(unsafe.Sizeof(node{}))

	heightIncrease = math.MaxUint32 / 3
)

// arena implements a lock-free arena. offset=0 is a nil pointer
type arena struct {
	n   atomic.Uint32
	buf []byte
}

type skiplist struct {
	height  atomic.Int32
	head    *node
	ref     atomic.Int32
	arena   *arena
	onClose func()
}

func newArena(n int64) *arena {
	out := &arena{buf: make([]byte, n)}
	out.n.Store(1)

	return out
}

func (s *arena) size() int64 {
	return int64(s.n.Load())
}

func (s *arena) putNode(height int) uint32 {
	unusedSize := (maxHeight - height) * offsetSize
	l := uint32(maxNodeSize - unusedSize + nodeAlign)
	n := s.n.Add(l)

	m := (n - l + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}

func (s *arena) putVal(v []byte) uint32 {
	l := uint32(len(v))
	n := s.n.Add(l)
	m := n - l

	copy(s.buf[m:], v)
	return m
}

func (s *arena) putKey(k []byte) uint32 {
	l := uint32(len(k))
	n := s.n.Add(l)
	m := n - l

	copy(s.buf[m:], k)
	return m
}

func (s *arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}

	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *arena) getVal(offset uint32, size uint32) []byte {
	return s.buf[offset : offset+size]
}

func (s *arena) getNodeOffset(n *node) uint32 {
	if n == nil {
		return 0
	}

	return uint32(uintptr(unsafe.Pointer(n)) - uintptr(unsafe.Pointer(&s.buf[0])))
}
