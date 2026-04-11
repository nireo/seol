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

// ideas:
// - block cache to limit disc reads
// - bloom filter
// - prefix sums on disc to save space on keys
// - reuse read buffers to avoid too many allocations

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

	filterData, err := filter.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if _, err := file.WriteAt(filterData, 0); err != nil {
		return nil, err
	}

	cleanup = false

	return sst, nil
}

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

	filter, bloomSize, err := loadBloomFilter(file, stat.Size())
	if err != nil {
		return nil, err
	}

	indexData, err := readIndexData(file, stat.Size(), bloomSize)
	if err != nil {
		return nil, err
	}

	sst := &Table{f: file, filter: filter, blockCache: newDataBlockCache(blockCacheEntryLimit)}
	sst.idx.decodeFullRange(indexData)
	cleanup = false
	return sst, nil
}

type Table struct {
	idx        tableIndex
	filter     *bloom.Filter
	f          *os.File
	blockCache *dataBlockCache
}

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

func (s *Table) Close() error {
	if s.blockCache != nil {
		s.blockCache.clear()
	}
	return s.f.Close()
}

func (s *Table) getCachedBlock(ra *dataRange) (*cachedDataBlock, error) {
	if block, ok := s.blockCache.get(ra.offset); ok {
		return block, nil
	}

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
