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
)

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
	}) - 1
	if idx < 0 {
		return nil
	}

	return &ti.ranges[idx]
}

func sstableID() string {
	id := time.Now().UnixMicro()
	return fmt.Sprintf("%d.sst", id)
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
		idx: tableIndex{},
		f:   file,
	}

	entryCount := 0
	countIt := sk.NewIterator()
	defer countIt.Close()
	countIt.Rewind()
	for countIt.Valid() {
		entryCount++
		countIt.Next()
	}

	filter := bloom.NewFor(entryCount, bloomFalsePositiveRate)
	filterData, err := filter.MarshalBinary()
	if err != nil {
		return nil, err
	}
	bloomSize := len(filterData)
	if _, err := file.Seek(int64(bloomSize), io.SeekStart); err != nil {
		return nil, err
	}
	sst.filter = filter

	it := sk.NewIterator()
	defer it.Close()
	it.Rewind()
	bw := bufio.NewWriter(file)
	offset := uint64(bloomSize)
	block := make([]byte, 0, sstableBlockSize)
	currentRange := dataRange{}

	flushBlock := func() error {
		if len(block) == 0 {
			return nil
		}

		currentRange.length = uint32(len(block))
		sst.idx.ranges = append(sst.idx.ranges, currentRange)
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
			return nil, fmt.Errorf("sstable: entry for key %q exceeds block size", key)
		}

		if len(block) > 0 && len(block)+entrySize > sstableBlockSize {
			if err := flushBlock(); err != nil {
				return nil, err
			}
		}

		if len(block) == 0 {
			currentRange.firstKey = append([]byte(nil), key...)
			currentRange.offset = offset
		}

		entryOffset := len(block)
		block = block[:entryOffset+entrySize]
		binary.LittleEndian.PutUint16(block[entryOffset:], uint16(len(key)))
		binary.LittleEndian.PutUint32(block[entryOffset+2:], uint32(len(value)))
		copy(block[entryOffset+entryMetaSize:], key)
		copy(block[entryOffset+entryMetaSize+len(key):], value)

		it.Next()
	}
	if err := flushBlock(); err != nil {
		return nil, err
	}

	indexOffset := offset
	indexData := sst.idx.encodeFullRange()
	if _, err := bw.Write(indexData); err != nil {
		return nil, err
	}

	footer := make([]byte, sstableFooterSize)
	binary.LittleEndian.PutUint64(footer, indexOffset)
	binary.LittleEndian.PutUint32(footer[8:], sstableMagic)
	footer[12] = sstableVersion
	if _, err := bw.Write(footer); err != nil {
		return nil, err
	}

	if err := bw.Flush(); err != nil {
		return nil, err
	}

	filterData, err = filter.MarshalBinary()
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
	if stat.Size() < int64(sstableFooterSize) {
		return nil, fmt.Errorf("sstable: file too small")
	}

	bloomHeader := make([]byte, bloom.HeaderSize)
	if _, err := file.ReadAt(bloomHeader, 0); err != nil {
		return nil, err
	}
	if got := binary.LittleEndian.Uint32(bloomHeader); got != bloom.Magic {
		return nil, fmt.Errorf("sstable: invalid bloom magic %#x", got)
	}
	if got := bloomHeader[4]; got != bloom.Version {
		return nil, fmt.Errorf("sstable: unsupported bloom version %d", got)
	}

	bloomBits := binary.LittleEndian.Uint64(bloomHeader[5:])
	if bloomBits == 0 || bloomBits%uint64(bloom.WordBits) != 0 {
		return nil, fmt.Errorf("sstable: invalid bloom bit count %d", bloomBits)
	}
	bloomSize := bloom.HeaderSize + int(bloomBits/uint64(bloom.WordBits))*8
	if stat.Size() < int64(bloomSize+sstableFooterSize) {
		return nil, fmt.Errorf("sstable: file too small for bloom filter")
	}

	bloomData := make([]byte, bloomSize)
	if _, err := file.ReadAt(bloomData, 0); err != nil {
		return nil, err
	}
	filter, err := bloom.ReadFilter(bloomData)
	if err != nil {
		return nil, err
	}

	footer := make([]byte, sstableFooterSize)
	footerOffset := stat.Size() - int64(sstableFooterSize)
	if _, err := file.ReadAt(footer, footerOffset); err != nil {
		return nil, err
	}

	if got := binary.LittleEndian.Uint32(footer[8:]); got != sstableMagic {
		return nil, fmt.Errorf("sstable: invalid magic %#x", got)
	}
	if got := footer[12]; got != sstableVersion {
		return nil, fmt.Errorf("sstable: unsupported version %d", got)
	}

	indexOffset := binary.LittleEndian.Uint64(footer)
	if indexOffset < uint64(bloomSize) || indexOffset > uint64(footerOffset) {
		return nil, fmt.Errorf("sstable: invalid index offset %d", indexOffset)
	}

	indexSize := int(footerOffset - int64(indexOffset))
	indexData := make([]byte, indexSize)
	if indexSize > 0 {
		if _, err := file.ReadAt(indexData, int64(indexOffset)); err != nil && err != io.EOF {
			return nil, err
		}
	}

	sst := &Table{f: file, filter: filter}
	sst.idx.decodeFullRange(indexData)
	cleanup = false
	return sst, nil
}

type Table struct {
	idx    tableIndex
	filter *bloom.Filter
	f      *os.File
}

func (s *Table) Get(key []byte) ([]byte, error) {
	if s.filter != nil && !s.filter.Contains(key) {
		return nil, nil
	}

	ra := s.idx.findDataRange(key)
	if ra == nil {
		return nil, nil
	}

	block := make([]byte, ra.length)
	if _, err := s.f.ReadAt(block, int64(ra.offset)); err != nil && err != io.EOF {
		return nil, err
	}

	ptr := 0
	for ptr < len(block) {
		if len(block)-ptr < entryMetaSize {
			return nil, fmt.Errorf("sstable: truncated entry header")
		}

		klen := int(binary.LittleEndian.Uint16(block[ptr:]))
		vlen := int(binary.LittleEndian.Uint32(block[ptr+2:]))
		ptr += entryMetaSize
		if ptr+klen+vlen > len(block) {
			return nil, fmt.Errorf("sstable: truncated entry body")
		}

		entryKey := block[ptr : ptr+klen]
		ptr += klen
		value := block[ptr : ptr+vlen]
		ptr += vlen

		cmp := bytes.Compare(entryKey, key)
		if cmp == 0 {
			return value, nil
		}
		if cmp > 0 {
			return nil, nil
		}
	}

	return nil, nil
}

func (s *Table) Close() error {
	return s.f.Close()
}
