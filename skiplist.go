package seol

import (
	"bytes"
	"math/rand/v2"
	"sync/atomic"
	"unsafe"
)

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

	heightIncrease = ^uint32(0) / 3
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

type Iterator struct {
	list *skiplist
	n    *node
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

func (s *skiplist) incrRef() {
	s.ref.Add(1)
}

func (s *skiplist) decrRef() {
	newRef := s.ref.Add(-1)
	if newRef > 0 {
		return
	}

	if s.onClose != nil {
		s.onClose()
	}

	// good to set things to nil to allow the gc to actually free them.
	s.arena = nil
	s.head = nil
}

func encodeValue(offset uint32, size uint32) uint64 {
	return uint64(size)<<32 | uint64(offset)
}

func decodeValue(encoded uint64) (uint32, uint32) {
	offset := uint32(encoded)
	vsize := uint32(encoded >> 32)
	return offset, vsize
}

func newNode(arena *arena, key []byte, v []byte, height int) *node {
	offset := arena.putNode(height)
	node := arena.getNode(offset)

	node.keyOffset = arena.putKey(key)
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value.Store(encodeValue(arena.putVal(v), uint32(len(v))))

	return node
}

func newSkiplist(arenaSize int64) *skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, nil, maxHeight)
	s := &skiplist{head: head, arena: arena}
	s.height.Store(1)
	s.ref.Store(1)

	return s
}

func (s *node) getValueOffset() (uint32, uint32) {
	value := s.value.Load()
	return decodeValue(value)
}

func (s *node) key(arena *arena) []byte {
	return arena.getKey(s.keyOffset, s.keySize)
}

func (s *node) setValue(arena *arena, v []byte) {
	valOffset := arena.putVal(v)
	value := encodeValue(valOffset, uint32(len(v)))
	s.value.Store(value)
}

func (s *node) getNextOffset(h int) uint32 {
	return s.tower[h].Load()
}

func (s *node) casNextOffset(h int, old, val uint32) bool {
	return s.tower[h].CompareAndSwap(old, val)
}

func (s *skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && rand.Uint32() <= heightIncrease {
		h++
	}
	return h
}

func (s *skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

func (s *skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.head
	level := int(s.getHeight() - 1)
	for {
		next := s.getNext(x, level)
		if next == nil {
			if level > 0 {
				level--
				continue
			}
			if !less || x == s.head {
				return nil, false
			}
			return x, false
		}

		cmp := bytes.Compare(key, next.key(s.arena))
		if cmp > 0 {
			x = next
			continue
		}
		if cmp == 0 {
			if allowEqual {
				return next, true
			}
			if !less {
				return s.getNext(next, 0), false
			}
			if level > 0 {
				level--
				continue
			}
			if x == s.head {
				return nil, false
			}
			return x, false
		}
		if level > 0 {
			level--
			continue
		}
		if !less {
			return next, false
		}
		if x == s.head {
			return nil, false
		}
		return x, false
	}
}

func (s *skiplist) findSpliceForLevel(key []byte, before *node, level int) (*node, *node) {
	for {
		next := s.getNext(before, level)
		if next == nil {
			return before, nil
		}

		cmp := bytes.Compare(key, next.key(s.arena))
		if cmp == 0 {
			return next, next
		}
		if cmp < 0 {
			return before, next
		}

		before = next
	}
}

func (s *skiplist) getHeight() int32 {
	return s.height.Load()
}

func (s *skiplist) put(key []byte, v []byte) {
	listHeight := s.getHeight()
	var prev [maxHeight + 1]*node
	var next [maxHeight + 1]*node
	prev[listHeight] = s.head

	for i := int(listHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			prev[i].setValue(s.arena, v)
			return
		}
	}

	height := s.randomHeight()
	x := newNode(s.arena, key, v, height)

	listHeight = s.getHeight()
	for height > int(listHeight) {
		if s.height.CompareAndSwap(listHeight, int32(height)) {
			break
		}
		listHeight = s.getHeight()
	}

	for i := 0; i < height; i++ {
		for {
			if prev[i] == nil {
				if i <= 0 {
					panic("skiplist: missing splice on base level")
				}
				prev[i], next[i] = s.findSpliceForLevel(key, s.head, i)
				if prev[i] == next[i] {
					panic("skiplist: duplicate key above base level")
				}
			}

			nextOffset := s.arena.getNodeOffset(next[i])
			x.tower[i].Store(nextOffset)
			if prev[i].casNextOffset(i, nextOffset, s.arena.getNodeOffset(x)) {
				break
			}

			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				if i != 0 {
					panic("skiplist: equality can happen only on base level")
				}
				prev[i].setValue(s.arena, v)
				return
			}
		}
	}
}

func (s *skiplist) empty() bool {
	return s.findLast() == nil
}

func (s *skiplist) findLast() *node {
	n := s.head
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.head {
				return nil
			}
			return n
		}
		level--
	}
}

func (s *skiplist) get(key []byte) []byte {
	n, _ := s.findNear(key, false, true)
	if n == nil {
		return nil
	}
	if !bytes.Equal(key, n.key(s.arena)) {
		return nil
	}

	valOffset, valSize := n.getValueOffset()
	return s.arena.getVal(valOffset, valSize)
}

func (s *skiplist) newIterator() *Iterator {
	s.incrRef()
	return &Iterator{list: s}
}

func (s *skiplist) memSize() int64 {
	return s.arena.size()
}

func (it *Iterator) close() {
	if it.list == nil {
		return
	}
	it.list.decrRef()
	it.list = nil
	it.n = nil
}

func (it *Iterator) valid() bool {
	return it != nil && it.n != nil
}

func (it *Iterator) key() []byte {
	if !it.valid() {
		return nil
	}
	return it.n.key(it.list.arena)
}

func (it *Iterator) value() []byte {
	if !it.valid() {
		return nil
	}
	valOffset, valSize := it.n.getValueOffset()
	return it.list.arena.getVal(valOffset, valSize)
}

func (it *Iterator) seek(target []byte) {
	it.n, _ = it.list.findNear(target, false, true)
}

func (it *Iterator) seekForPrev(target []byte) {
	it.n, _ = it.list.findNear(target, true, true)
}

func (it *Iterator) seekToFirst() {
	it.n = it.list.getNext(it.list.head, 0)
}

func (it *Iterator) seekToLast() {
	it.n = it.list.findLast()
}

func (it *Iterator) rewind() {
	it.seekToFirst()
}

func (it *Iterator) next() {
	if !it.valid() {
		return
	}
	it.n = it.list.getNext(it.n, 0)
}

func (it *Iterator) prev() {
	if !it.valid() {
		it.seekToLast()
		return
	}
	it.n, _ = it.list.findNear(it.key(), true, false)
}
