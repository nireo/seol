package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/nireo/seol/bloom"
	"github.com/nireo/seol/skiplist"
)

// sstable files are laid out as:
//
//	bloom filter | data blocks | block index | footer
//
// the bloom filter is stored first so negative point lookups can fail fast.
// data blocks hold sorted length-prefixed entries. the index stores one record
// per block using that block's first key, byte offset, and length. the footer
// is fixed width and points back to the start of the index.
const (
	sstableBlockSize       int    = 4 * 1024 // 4kb
	sstableMagic           uint32 = 0xFF12FF45
	sstableVersion         byte   = 2
	sstableFooterSize      int    = 8 + 4 + 1 // index offset + magic + version
	entryMetaSize          int    = 2 + 4     // uint16 (keylen) + uint32 (vallen)
	dataRangeMetaSize      int    = 2 + 8 + 4 // uint16 (keylen) + uint64 (offset) + uint32 (datablock length)
	bloomFalsePositiveRate        = 0.01
	blockCacheShardCount          = 16
	blockCacheEntryLimit          = 256
)

var dataBlockPool = sync.Pool{
	New: func() any {
		return &pooledDataBlock{}
	},
}

type pooledDataBlock struct {
	buf [sstableBlockSize]byte
}

type leasedDataBlock struct {
	data   []byte
	pooled *pooledDataBlock
}

func getDataBlock(length uint32) leasedDataBlock {
	// most blocks are at or below the configured block size, so keep a pool of
	// fixed-size buffers for the common read path and fall back to a dedicated
	// allocation only for oversized blocks.
	if int(length) > sstableBlockSize {
		return leasedDataBlock{data: make([]byte, int(length))}
	}

	pooled := dataBlockPool.Get().(*pooledDataBlock)
	return leasedDataBlock{
		data:   pooled.buf[:int(length)],
		pooled: pooled,
	}
}

func putDataBlock(block leasedDataBlock) {
	if block.pooled == nil {
		return
	}
	dataBlockPool.Put(block.pooled)
}

type cachedDataBlock struct {
	data         []byte
	entryOffsets []uint32
}

func newCachedDataBlock(raw []byte) (*cachedDataBlock, error) {
	// keep an owned copy of the raw block bytes so the cached block outlives any
	// temporary read buffer, then precompute each entry offset once.
	block := &cachedDataBlock{data: append([]byte(nil), raw...)}
	ptr := 0
	for ptr < len(block.data) {
		if len(block.data)-ptr < entryMetaSize {
			return nil, fmt.Errorf("sstable: truncated entry header")
		}

		block.entryOffsets = append(block.entryOffsets, uint32(ptr))
		klen := int(binary.LittleEndian.Uint16(block.data[ptr:]))
		vlen := int(binary.LittleEndian.Uint32(block.data[ptr+2:]))
		ptr += entryMetaSize
		if ptr+klen+vlen > len(block.data) {
			return nil, fmt.Errorf("sstable: truncated entry body")
		}
		ptr += klen + vlen
	}
	return block, nil
}

func (b *cachedDataBlock) lookup(key []byte) []byte {
	// entries are written in sorted key order, so binary search within the block
	// avoids scanning it linearly on every point lookup.
	idx := sort.Search(len(b.entryOffsets), func(i int) bool {
		entryKey, _ := b.entryAt(i)
		return bytes.Compare(entryKey, key) >= 0
	})
	if idx == len(b.entryOffsets) {
		return nil
	}

	entryKey, value := b.entryAt(idx)
	if !bytes.Equal(entryKey, key) {
		return nil
	}
	return value
}

func (b *cachedDataBlock) entryAt(i int) ([]byte, []byte) {
	ptr := int(b.entryOffsets[i])
	klen := int(binary.LittleEndian.Uint16(b.data[ptr:]))
	vlen := int(binary.LittleEndian.Uint32(b.data[ptr+2:]))
	ptr += entryMetaSize
	key := b.data[ptr : ptr+klen]
	ptr += klen
	value := b.data[ptr : ptr+vlen]
	return key, value
}

type dataBlockCacheShard struct {
	mu         sync.RWMutex
	blocks     map[uint64]*cachedDataBlock
	order      []uint64
	maxEntries int
}

type dataBlockCache struct {
	shards []dataBlockCacheShard
}

func newDataBlockCache(maxEntries int) *dataBlockCache {
	if maxEntries < 1 {
		return nil
	}
	shardCount := blockCacheShardCount
	if maxEntries < shardCount {
		shardCount = maxEntries
	}
	// spread blocks across a small fixed set of shards so reads usually take only
	// one shard lock and hot offsets do not serialize through a single mutex.
	perShard := (maxEntries + shardCount - 1) / shardCount
	cache := &dataBlockCache{shards: make([]dataBlockCacheShard, shardCount)}
	for i := range cache.shards {
		cache.shards[i] = dataBlockCacheShard{
			blocks:     make(map[uint64]*cachedDataBlock, perShard),
			order:      make([]uint64, 0, perShard),
			maxEntries: perShard,
		}
	}
	return cache
}

func (c *dataBlockCache) get(offset uint64) (*cachedDataBlock, bool) {
	if c == nil {
		return nil, false
	}
	shard := c.shard(offset)
	shard.mu.RLock()
	block, ok := shard.blocks[offset]
	shard.mu.RUnlock()
	return block, ok
}

func (c *dataBlockCache) add(offset uint64, block *cachedDataBlock) {
	if c == nil || block == nil {
		return
	}
	shard := c.shard(offset)
	shard.mu.Lock()
	if _, ok := shard.blocks[offset]; ok {
		shard.blocks[offset] = block
		shard.mu.Unlock()
		return
	}
	if len(shard.blocks) >= shard.maxEntries {
		shard.evictOldestLocked()
	}
	shard.blocks[offset] = block
	shard.order = append(shard.order, offset)
	shard.mu.Unlock()
}

func (c *dataBlockCache) clear() {
	if c == nil {
		return
	}
	for i := range c.shards {
		shard := &c.shards[i]
		shard.mu.Lock()
		clear(shard.blocks)
		shard.order = shard.order[:0]
		shard.mu.Unlock()
	}
}

func (c *dataBlockCache) shard(offset uint64) *dataBlockCacheShard {
	return &c.shards[offset%uint64(len(c.shards))]
}

func (s *dataBlockCacheShard) evictOldestLocked() {
	// order can contain stale offsets when a block is replaced in place, so keep
	// popping until we find one that is still present.
	for len(s.order) > 0 {
		oldest := s.order[0]
		s.order = s.order[1:]
		if _, ok := s.blocks[oldest]; ok {
			delete(s.blocks, oldest)
			return
		}
	}
}

type dataRange struct {
	firstKey []byte
	offset   uint64
	length   uint32
}

// encode expects that dst is big enough and that the slice is pointing to the place where to write the values.
func (dr *dataRange) encode(dst []byte) int {
	keyln := uint16(len(dr.firstKey))

	binary.LittleEndian.PutUint16(dst, keyln)
	copy(dst[2:], dr.firstKey)
	binary.LittleEndian.PutUint64(dst[2+keyln:], dr.offset)
	binary.LittleEndian.PutUint32(dst[2+keyln+8:], dr.length)

	return int(keyln) + dataRangeMetaSize
}

// decode decodes into the callee the value and expects the dst slice to be starting at the correct position.
func (dr *dataRange) decode(src []byte) int {
	klen := int(binary.LittleEndian.Uint16(src))
	dr.firstKey = make([]byte, klen)
	copy(dr.firstKey, src[2:])
	dr.offset = binary.LittleEndian.Uint64(src[2+klen:])
	dr.length = binary.LittleEndian.Uint32(src[2+klen+8:])

	return klen + dataRangeMetaSize
}

type tableIndex struct {
	ranges []dataRange // sorted by the first key
}

func (ti *tableIndex) encodeFullRange() []byte {
	if len(ti.ranges) == 0 {
		return []byte{}
	}

	// loop over the list to quickly calculate allocation size to avoid extra allocations.
	sz := 0
	for _, ra := range ti.ranges {
		sz += len(ra.firstKey) + dataRangeMetaSize
	}

	ptr := 0
	res := make([]byte, sz)
	for _, ra := range ti.ranges {
		ptr += ra.encode(res[ptr:])
	}

	return res
}

func (ti *tableIndex) decodeFullRange(data []byte) {
	// the index is stored as back-to-back dataRange encodings with no count
	// prefix, so decoding just walks until the byte slice is exhausted.
	if len(data) == 0 {
		return
	}

	ptr := 0
	for ptr < len(data) {
		dr := dataRange{}
		ptr += dr.decode(data[ptr:])
		ti.ranges = append(ti.ranges, dr)
	}
}

func (ti *tableIndex) findDataRange(key []byte) *dataRange {
	if len(ti.ranges) == 0 {
		return nil
	}

	// find the first range whose first key is greater than the target, then step
	// back to the range that could still contain it.
	idx := sort.Search(len(ti.ranges), func(i int) bool {
		return bytes.Compare(ti.ranges[i].firstKey, key) > 0
	})
	if idx == 0 {
		return nil
	}

	return &ti.ranges[idx-1]
}

func sstableID() string {
	id := time.Now().UnixMicro()
	return fmt.Sprintf("%d.sst", id)
}

func countEntries(sk *skiplist.Skiplist) int {
	count := 0
	it := sk.NewIterator()
	defer it.Close()

	it.Rewind()
	for it.Valid() {
		count++
		it.Next()
	}

	return count
}

func prepareBloomFilter(file *os.File, sk *skiplist.Skiplist) (*bloom.Filter, int, error) {
	// reserve the bloom filter bytes up front so blocks and the index can be
	// streamed once, then backfill the filter after all keys have been added.
	filter := bloom.NewFor(countEntries(sk), bloomFalsePositiveRate)
	filterData, err := filter.MarshalBinary()
	if err != nil {
		return nil, 0, err
	}
	if _, err := file.Seek(int64(len(filterData)), io.SeekStart); err != nil {
		return nil, 0, err
	}
	return filter, len(filterData), nil
}

func appendBlockEntry(block []byte, key, value []byte) []byte {
	// each block is a tight byte buffer of length-prefixed entries.
	entrySize := entryMetaSize + len(key) + len(value)
	entryOffset := len(block)
	block = block[:entryOffset+entrySize]
	binary.LittleEndian.PutUint16(block[entryOffset:], uint16(len(key)))
	binary.LittleEndian.PutUint32(block[entryOffset+2:], uint32(len(value)))
	copy(block[entryOffset+entryMetaSize:], key)
	copy(block[entryOffset+entryMetaSize+len(key):], value)
	return block
}

func writeDataBlocks(bw *bufio.Writer, sk *skiplist.Skiplist, filter *bloom.Filter, startOffset uint64) (tableIndex, uint64, error) {
	// stream the sorted memtable into fixed-size blocks while recording one index
	// entry per block: the first key, on-disk offset, and encoded length.
	idx := tableIndex{}
	it := sk.NewIterator()
	defer it.Close()

	it.Rewind()
	offset := startOffset
	block := make([]byte, 0, sstableBlockSize)
	currentRange := dataRange{}
	flushBlock := func() error {
		if len(block) == 0 {
			return nil
		}

		// firstKey and offset were captured when the block became non-empty.
		currentRange.length = uint32(len(block))
		idx.ranges = append(idx.ranges, currentRange)
		if _, err := bw.Write(block); err != nil {
			return err
		}

		offset += uint64(len(block))
		block = block[:0]
		currentRange = dataRange{}
		return nil
	}

	for it.Valid() {
		key := it.Key()
		value := it.Value()
		filter.Add(key)

		entrySize := entryMetaSize + len(key) + len(value)
		if entrySize > sstableBlockSize {
			return tableIndex{}, 0, fmt.Errorf("sstable: entry for key %q exceeds block size", key)
		}
		if len(block) > 0 && len(block)+entrySize > sstableBlockSize {
			if err := flushBlock(); err != nil {
				return tableIndex{}, 0, err
			}
		}

		if len(block) == 0 {
			currentRange.firstKey = append([]byte(nil), key...)
			currentRange.offset = offset
		}

		block = appendBlockEntry(block, key, value)
		it.Next()
	}

	if err := flushBlock(); err != nil {
		return tableIndex{}, 0, err
	}

	return idx, offset, nil
}

func writeIndexAndFooter(bw *bufio.Writer, idx tableIndex, indexOffset uint64) error {
	// file layout is: bloom filter, data blocks, index, footer. the footer is a
	// fixed-width trailer that points back to the start of the variable-sized
	// index.
	if _, err := bw.Write(idx.encodeFullRange()); err != nil {
		return err
	}

	var footer [sstableFooterSize]byte
	binary.LittleEndian.PutUint64(footer[:], indexOffset)
	binary.LittleEndian.PutUint32(footer[8:], sstableMagic)
	footer[12] = sstableVersion
	_, err := bw.Write(footer[:])
	return err
}

func loadBloomFilter(file *os.File, fileSize int64) (*bloom.Filter, int, error) {
	// the bloom filter lives at the front of the file, but its exact byte size is
	// embedded in the bloom header, so read and validate that header first.
	if fileSize < int64(sstableFooterSize) {
		return nil, 0, fmt.Errorf("sstable: file too small")
	}

	var bloomHeader [bloom.HeaderSize]byte
	if _, err := file.ReadAt(bloomHeader[:], 0); err != nil {
		return nil, 0, err
	}
	if got := binary.LittleEndian.Uint32(bloomHeader[:]); got != bloom.Magic {
		return nil, 0, fmt.Errorf("sstable: invalid bloom magic %#x", got)
	}
	if got := bloomHeader[4]; got != bloom.Version {
		return nil, 0, fmt.Errorf("sstable: unsupported bloom version %d", got)
	}

	bloomBits := binary.LittleEndian.Uint64(bloomHeader[5:])
	if bloomBits == 0 || bloomBits%uint64(bloom.WordBits) != 0 {
		return nil, 0, fmt.Errorf("sstable: invalid bloom bit count %d", bloomBits)
	}
	bloomSize := bloom.HeaderSize + int(bloomBits/uint64(bloom.WordBits))*8
	if fileSize < int64(bloomSize+sstableFooterSize) {
		return nil, 0, fmt.Errorf("sstable: file too small for bloom filter")
	}

	bloomData := make([]byte, bloomSize)
	if _, err := file.ReadAt(bloomData, 0); err != nil {
		return nil, 0, err
	}

	filter, err := bloom.ReadFilter(bloomData)
	if err != nil {
		return nil, 0, err
	}
	return filter, bloomSize, nil
}

func readIndexData(file *os.File, fileSize int64, bloomSize int) ([]byte, error) {
	// the footer is always the last bytes in the file and tells us where the
	// index begins, so the whole index can be sliced out with one read.
	var footer [sstableFooterSize]byte
	footerOffset := fileSize - int64(sstableFooterSize)
	if _, err := file.ReadAt(footer[:], footerOffset); err != nil {
		return nil, err
	}

	if got := binary.LittleEndian.Uint32(footer[8:]); got != sstableMagic {
		return nil, fmt.Errorf("sstable: invalid magic %#x", got)
	}
	if got := footer[12]; got != sstableVersion {
		return nil, fmt.Errorf("sstable: unsupported version %d", got)
	}

	indexOffset := binary.LittleEndian.Uint64(footer[:])
	if indexOffset < uint64(bloomSize) || indexOffset > uint64(footerOffset) {
		return nil, fmt.Errorf("sstable: invalid index offset %d", indexOffset)
	}

	indexSize := int(footerOffset - int64(indexOffset))
	indexData := make([]byte, indexSize)
	if indexSize == 0 {
		return indexData, nil
	}
	if _, err := file.ReadAt(indexData, int64(indexOffset)); err != nil && err != io.EOF {
		return nil, err
	}
	return indexData, nil
}

// Flush writes sk to a new sstable file in baseDir.
func Flush(baseDir string, sk *skiplist.Skiplist) (*Table, error) {
	id := sstableID()
	path := filepath.Join(baseDir, id)

	// don't close here.
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	cleanup := true

	// don't leave a mess when something goes wrong in the process.
	defer func() {
		if !cleanup {
			return
		}
		_ = file.Close()
		_ = os.Remove(path)
	}()

	sst := &Table{
		idx:        tableIndex{},
		f:          file,
		blockCache: newDataBlockCache(blockCacheEntryLimit),
		path:       path,
	}

	filter, bloomSize, err := prepareBloomFilter(file, sk)
	if err != nil {
		return nil, err
	}
	sst.filter = filter

	bw := bufio.NewWriter(file)
	idx, indexOffset, err := writeDataBlocks(bw, sk, filter, uint64(bloomSize))
	if err != nil {
		return nil, err
	}
	sst.idx = idx

	if err := writeIndexAndFooter(bw, sst.idx, indexOffset); err != nil {
		return nil, err
	}

	if err := bw.Flush(); err != nil {
		return nil, err
	}

	// the bloom filter bytes were reserved at the start of the file before any
	// keys were seen, so write them back after the filter has been fully built.
	filterData, err := filter.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if _, err := file.WriteAt(filterData, 0); err != nil {
		return nil, err
	}
	if err := sst.loadMetadata(); err != nil {
		return nil, err
	}

	cleanup = false

	return sst, nil
}

// Open loads an existing sstable from path.
func Open(path string) (*Table, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
		}
	}()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// validate both ends of the file before decoding the in-memory structures so
	// malformed tables fail early.
	filter, bloomSize, err := loadBloomFilter(file, stat.Size())
	if err != nil {
		return nil, err
	}

	indexData, err := readIndexData(file, stat.Size(), bloomSize)
	if err != nil {
		return nil, err
	}

	sst := &Table{f: file, filter: filter, blockCache: newDataBlockCache(blockCacheEntryLimit), path: path}
	sst.idx.decodeFullRange(indexData)
	if err := sst.loadMetadata(); err != nil {
		return nil, err
	}
	cleanup = false
	return sst, nil
}

// Table is an immutable sstable backed by an on-disk file.
type Table struct {
	idx        tableIndex
	filter     *bloom.Filter
	f          *os.File
	blockCache *dataBlockCache
	path       string
	smallest   []byte
	largest    []byte
	sizeBytes  int64
}

// Metadata describes an sstable without exposing its internals.
type Metadata struct {
	Path      string
	Smallest  []byte
	Largest   []byte
	SizeBytes int64
}

// Iterator walks a table in sorted key order.
type Iterator struct {
	table    *Table
	rangeIdx int
	entryIdx int
	block    *cachedDataBlock
	err      error
}

// Get returns a copy of the value for key, or nil when the key is absent.
func (s *Table) Get(key []byte) ([]byte, error) {
	if s.filter != nil && !s.filter.Contains(key) {
		return nil, nil
	}

	ra := s.idx.findDataRange(key)
	if ra == nil {
		return nil, nil
	}

	block, err := s.getCachedBlock(ra)
	if err != nil {
		return nil, err
	}
	value := block.lookup(key)
	if value == nil {
		return nil, nil
	}
	return append([]byte(nil), value...), nil
}

// Scan visits every entry in sorted key order.
func (s *Table) Scan(fn func(key, value []byte) error) error {
	if fn == nil {
		return nil
	}

	for i := range s.idx.ranges {
		ra := &s.idx.ranges[i]
		block, err := s.getCachedBlock(ra)
		if err != nil {
			return err
		}
		for entryIdx := range block.entryOffsets {
			key, value := block.entryAt(entryIdx)
			if err := fn(key, value); err != nil {
				return err
			}
		}
	}

	return nil
}

// Metadata returns a copy of the table metadata.
func (s *Table) Metadata() Metadata {
	if s == nil {
		return Metadata{}
	}
	return Metadata{
		Path:      s.path,
		Smallest:  append([]byte(nil), s.smallest...),
		Largest:   append([]byte(nil), s.largest...),
		SizeBytes: s.sizeBytes,
	}
}

// Close releases the underlying file and clears cached blocks.
func (s *Table) Close() error {
	if s.blockCache != nil {
		s.blockCache.clear()
	}
	return s.f.Close()
}

// NewIterator returns an iterator positioned before the first entry.
func (s *Table) NewIterator() *Iterator {
	return &Iterator{table: s, rangeIdx: -1, entryIdx: -1}
}

// Rewind moves the iterator to the first entry.
func (it *Iterator) Rewind() {
	if it == nil {
		return
	}
	it.err = nil
	it.rangeIdx = 0
	it.entryIdx = 0
	it.block = nil
	it.loadCurrent()
}

// Valid reports whether the iterator points at an entry.
func (it *Iterator) Valid() bool {
	return it != nil && it.err == nil && it.block != nil && it.rangeIdx >= 0 && it.rangeIdx < len(it.table.idx.ranges) && it.entryIdx >= 0 && it.entryIdx < len(it.block.entryOffsets)
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	key, _ := it.block.entryAt(it.entryIdx)
	return key
}

// Value returns the current value.
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	_, value := it.block.entryAt(it.entryIdx)
	return value
}

// Next advances the iterator by one entry.
func (it *Iterator) Next() {
	if !it.Valid() {
		return
	}
	it.entryIdx++
	it.loadCurrent()
}

// Err returns the first iterator error, if any.
func (it *Iterator) Err() error {
	if it == nil {
		return nil
	}
	return it.err
}

func (it *Iterator) loadCurrent() {
	if it == nil || it.err != nil || it.table == nil {
		return
	}
	// keep advancing ranges until we land on a block that still has entries.
	for it.rangeIdx >= 0 && it.rangeIdx < len(it.table.idx.ranges) {
		if it.block == nil {
			block, err := it.table.getCachedBlock(&it.table.idx.ranges[it.rangeIdx])
			if err != nil {
				it.err = err
				it.block = nil
				return
			}
			it.block = block
		}
		if it.entryIdx < len(it.block.entryOffsets) {
			return
		}
		it.rangeIdx++
		it.entryIdx = 0
		it.block = nil
	}
	it.block = nil
}

func (s *Table) getCachedBlock(ra *dataRange) (*cachedDataBlock, error) {
	if block, ok := s.blockCache.get(ra.offset); ok {
		return block, nil
	}

	// cache parsed blocks by file offset so repeated reads do not re-decode them.
	raw := getDataBlock(ra.length)
	defer putDataBlock(raw)
	if _, err := s.f.ReadAt(raw.data, int64(ra.offset)); err != nil && err != io.EOF {
		return nil, err
	}

	block, err := newCachedDataBlock(raw.data)
	if err != nil {
		return nil, err
	}
	s.blockCache.add(ra.offset, block)
	return block, nil
}

func (s *Table) loadMetadata() error {
	if s == nil || s.f == nil {
		return nil
	}
	stat, err := s.f.Stat()
	if err != nil {
		return err
	}
	s.sizeBytes = stat.Size()
	if len(s.idx.ranges) == 0 {
		s.smallest = nil
		s.largest = nil
		return nil
	}
	// the index stores only the first key of each block, so the smallest key is
	// immediate but the largest key has to come from the last parsed block.
	s.smallest = append(s.smallest[:0], s.idx.ranges[0].firstKey...)
	lastRange := &s.idx.ranges[len(s.idx.ranges)-1]
	block, err := s.getCachedBlock(lastRange)
	if err != nil {
		return err
	}
	if len(block.entryOffsets) == 0 {
		s.largest = nil
		return nil
	}
	lastKey, _ := block.entryAt(len(block.entryOffsets) - 1)
	s.largest = append(s.largest[:0], lastKey...)
	return nil
}
