package seol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ideas:
// - block cache to limit disc reads
// - bloom filter
// - prefix sums on disc to save space on keys
// - reuse read buffers to avoid too many allocations

const (
	sstableBlockSize  int    = 4 * 1024 // 4kb
	sstableMagic      uint32 = 0xFF12FF45
	sstableVersion    byte   = 1
	sstableFooterSize int    = 8 + 4 + 1 // index offset + magic + version
	entryMetaSize     int    = 2 + 4     // uint16 (keylen) + uint32 (vallen)
	dataRangeMetaSize int    = 2 + 8 + 4 // uint16 (keylen) + uint64 (offset) + uint32 (datablock length)
)

type dataRange struct {
	firstKey []byte
	offset   uint64
	length   uint32
}

// encode expects that dst is big enough and that the slice is pointing to the place where to write the values
func (dr *dataRange) encode(dst []byte) int {
	keyln := uint16(len(dr.firstKey))

	binary.LittleEndian.PutUint16(dst, keyln)
	copy(dst[2:], dr.firstKey)
	binary.LittleEndian.PutUint64(dst[2+keyln:], dr.offset)
	binary.LittleEndian.PutUint32(dst[2+keyln+8:], dr.length)

	return int(keyln) + dataRangeMetaSize
}

// decode decodes into the callee the value and expects the dst slice to be starting at the correct position
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

	// loop over the list to quickly calculate allocation size to avoid extra allocations
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

func sstableID() string {
	id := time.Now().UnixMicro()
	return fmt.Sprintf("%d.sst", id)
}

func flushSkiplist(baseDir string, sk *skiplist) (*sstable, error) {
	id := sstableID()
	path := filepath.Join(baseDir, id)

	// don't close here
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	cleanup := true

	// dont leave a mess when something goes wrong in the process
	defer func() {
		if !cleanup {
			return
		}
		_ = file.Close()
		_ = os.Remove(path)
	}()

	sst := &sstable{
		idx: tableIndex{},
		f:   file,
	}

	it := sk.newIterator()
	defer it.close()
	it.rewind()
	bw := bufio.NewWriter(file)
	offset := uint64(0)
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

	for it.valid() {
		key := it.key()
		value := it.value()
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

		it.next()
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

	cleanup = false

	return sst, nil
}

type sstable struct {
	idx tableIndex
	f   *os.File
}

func (s *sstable) close() error {
	return s.f.Close()
}
