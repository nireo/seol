package skiplist

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

// arena implements a lock-free arena. offset=0 is a nil pointer.
type arena struct {
	n   atomic.Uint32
	buf []byte
}

// Skiplist stores keys in sorted order inside an arena-backed skiplist.
type Skiplist struct {
	height  atomic.Int32
	head    *node
	ref     atomic.Int32
	arena   *arena
	onClose func()
}

// Iterator walks a skiplist in sorted key order.
type Iterator struct {
	list *Skiplist
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
	// keep every node 64-bit aligned so atomic fields stay naturally aligned.
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

func (s *Skiplist) incrRef() {
	s.ref.Add(1)
}

func (s *Skiplist) decrRef() {
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

// New allocates a skiplist backed by an arena of arenaSize bytes.
func New(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, nil, maxHeight)
	s := &Skiplist{head: head, arena: arena}
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

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && rand.Uint32() <= heightIncrease {
		h++
	}
	return h
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

// findNear returns the closest node around key.
//
// when less is false it finds the first node >= key.
// when less is true it finds the last node <= key.
// when allowEqual is false an exact match is skipped in the requested direction.
func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
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

func (s *Skiplist) findSpliceForLevel(key []byte, before *node, level int) (*node, *node) {
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

func (s *Skiplist) getHeight() int32 {
	return s.height.Load()
}

// Put inserts or overwrites key with v.
func (s *Skiplist) Put(key []byte, v []byte) {
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

			// another writer changed the splice, so recompute it for this level.
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

// Empty reports whether the skiplist contains any entries.
func (s *Skiplist) Empty() bool {
	return s.getNext(s.head, 0) == nil
}

func (s *Skiplist) findLast() *node {
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

// Get returns the value for key, or nil when the key is absent.
func (s *Skiplist) Get(key []byte) []byte {
	n, equal := s.findNear(key, false, true)
	if !equal {
		return nil
	}

	valOffset, valSize := n.getValueOffset()
	return s.arena.getVal(valOffset, valSize)
}

// NewIterator returns an iterator that must be closed when no longer needed.
func (s *Skiplist) NewIterator() *Iterator {
	s.incrRef()
	return &Iterator{list: s}
}

// MemSize returns the bytes currently allocated from the arena.
func (s *Skiplist) MemSize() int64 {
	return s.arena.size()
}

// Close releases the iterator's reference to the skiplist.
func (it *Iterator) Close() {
	if it.list == nil {
		return
	}
	it.list.decrRef()
	it.list = nil
	it.n = nil
}

// Valid reports whether the iterator points at an entry.
func (it *Iterator) Valid() bool {
	return it != nil && it.n != nil
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.n.key(it.list.arena)
}

// Value returns the current value.
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	valOffset, valSize := it.n.getValueOffset()
	return it.list.arena.getVal(valOffset, valSize)
}

// Seek moves the iterator to the first key greater than or equal to target.
func (it *Iterator) Seek(target []byte) {
	it.n, _ = it.list.findNear(target, false, true)
}

// SeekForPrev moves the iterator to the last key less than or equal to target.
func (it *Iterator) SeekForPrev(target []byte) {
	it.n, _ = it.list.findNear(target, true, true)
}

// SeekToFirst moves the iterator to the first key.
func (it *Iterator) SeekToFirst() {
	it.n = it.list.getNext(it.list.head, 0)
}

// SeekToLast moves the iterator to the last key.
func (it *Iterator) SeekToLast() {
	it.n = it.list.findLast()
}

// Rewind moves the iterator to the first key.
func (it *Iterator) Rewind() {
	it.SeekToFirst()
}

// Next advances the iterator by one key.
func (it *Iterator) Next() {
	if !it.Valid() {
		return
	}
	it.n = it.list.getNext(it.n, 0)
}

// Prev moves the iterator back by one key.
func (it *Iterator) Prev() {
	if !it.Valid() {
		it.SeekToLast()
		return
	}
	it.n, _ = it.list.findNear(it.Key(), true, false)
}
