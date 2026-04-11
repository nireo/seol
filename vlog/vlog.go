package vlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultSegmentMaxBytes int64 = 64 << 20  // 64 mb
	recordHeaderSize             = 4 + 4 + 4 // checksum + keylen + vallen
	EncodedValueRefSize          = 8 + 8 + 4 + 4
)

type Options struct {
	SegmentMaxBytes int64
}

// ValueRef points at a value stored inside a value-log segment.
type ValueRef struct {
	SegmentID uint64
	Offset    uint64
	ValueLen  uint32
	Checksum  uint32
}

func (ref ValueRef) MarshalBinary() ([]byte, error) {
	buf := make([]byte, EncodedValueRefSize)
	binary.LittleEndian.PutUint64(buf, ref.SegmentID)
	binary.LittleEndian.PutUint64(buf[8:], ref.Offset)
	binary.LittleEndian.PutUint32(buf[16:], ref.ValueLen)
	binary.LittleEndian.PutUint32(buf[20:], ref.Checksum)
	return buf, nil
}

func (ref *ValueRef) UnmarshalBinary(data []byte) error {
	if len(data) != EncodedValueRefSize {
		return fmt.Errorf("vlog: invalid value ref size %d", len(data))
	}

	ref.SegmentID = binary.LittleEndian.Uint64(data)
	ref.Offset = binary.LittleEndian.Uint64(data[8:])
	ref.ValueLen = binary.LittleEndian.Uint32(data[16:])
	ref.Checksum = binary.LittleEndian.Uint32(data[20:])
	return nil
}

type Record struct {
	Key   []byte
	Value []byte
}

type segment struct {
	id   uint64
	path string
	f    *os.File
	size uint64
}

// Log manages append-only value-log segments.
type Log struct {
	dir             string
	segmentMaxBytes int64

	mu     sync.RWMutex
	active *segment
	files  map[uint64]*os.File
	closed bool
}

func Open(dir string, opts Options) (*Log, error) {
	if opts.SegmentMaxBytes <= 0 {
		opts.SegmentMaxBytes = defaultSegmentMaxBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	type segmentInfo struct {
		id   uint64
		path string
	}

	var segments []segmentInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".vlog") {
			continue
		}

		id, ok := parseSegmentID(entry.Name())
		if !ok {
			return nil, fmt.Errorf("vlog: invalid segment name %q", entry.Name())
		}
		segments = append(segments, segmentInfo{id: id, path: filepath.Join(dir, entry.Name())})
	}
	if len(segments) == 0 {
		return openEmpty(dir, opts)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].id < segments[j].id
	})

	log := &Log{
		dir:             dir,
		segmentMaxBytes: opts.SegmentMaxBytes,
		files:           make(map[uint64]*os.File, len(segments)),
	}

	for i, info := range segments {
		flags := os.O_RDONLY
		if i == len(segments)-1 {
			flags = os.O_RDWR
		}

		file, err := os.OpenFile(info.path, flags, 0o644)
		if err != nil {
			log.closeFiles()
			return nil, err
		}

		validSize, invalidTail, err := scanSegment(file)
		if err != nil {
			_ = file.Close()
			log.closeFiles()
			return nil, err
		}
		if invalidTail {
			if i != len(segments)-1 {
				_ = file.Close()
				log.closeFiles()
				return nil, fmt.Errorf("vlog: invalid tail in sealed segment %s", filepath.Base(info.path))
			}
			if err := file.Truncate(int64(validSize)); err != nil {
				_ = file.Close()
				log.closeFiles()
				return nil, err
			}
		}

		log.files[info.id] = file
		if i == len(segments)-1 {
			log.active = &segment{id: info.id, path: info.path, f: file, size: validSize}
		}
	}

	if log.active == nil {
		log.closeFiles()
		return nil, errors.New("vlog: missing active segment")
	}

	return log, nil
}

func openEmpty(dir string, opts Options) (*Log, error) {
	log := &Log{
		dir:             dir,
		segmentMaxBytes: opts.SegmentMaxBytes,
		files:           make(map[uint64]*os.File, 1),
	}
	if err := log.rotateSegmentLocked(0); err != nil {
		return nil, err
	}
	return log, nil
}

func (l *Log) Append(key, value []byte) (ValueRef, error) {
	recordSize := recordHeaderSize + len(key) + len(value)
	if uint64(recordSize) < uint64(len(key))+uint64(len(value)) {
		return ValueRef{}, fmt.Errorf("vlog: record too large")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ValueRef{}, errors.New("vlog: closed")
	}
	if l.active == nil || l.active.f == nil {
		return ValueRef{}, errors.New("vlog: missing active segment")
	}

	if l.active.size > 0 && l.active.size+uint64(recordSize) > uint64(l.segmentMaxBytes) {
		if err := l.rotateSegmentLocked(l.active.id); err != nil {
			return ValueRef{}, err
		}
	}

	record := make([]byte, recordSize)
	binary.LittleEndian.PutUint32(record[4:], uint32(len(key)))
	binary.LittleEndian.PutUint32(record[8:], uint32(len(value)))
	copy(record[recordHeaderSize:], key)
	copy(record[recordHeaderSize+len(key):], value)
	binary.LittleEndian.PutUint32(record, crc32.ChecksumIEEE(record[4:]))

	offset := l.active.size
	n, err := l.active.f.WriteAt(record, int64(offset))
	if n > 0 {
		l.active.size += uint64(n)
	}
	if err != nil {
		return ValueRef{}, err
	}
	if n != len(record) {
		return ValueRef{}, io.ErrShortWrite
	}

	return ValueRef{
		SegmentID: l.active.id,
		Offset:    offset,
		ValueLen:  uint32(len(value)),
		Checksum:  crc32.ChecksumIEEE(value),
	}, nil
}

func (l *Log) Read(ref ValueRef) ([]byte, error) {
	record, err := l.ReadRecord(ref)
	if err != nil {
		return nil, err
	}
	return record.Value, nil
}

func (l *Log) ReadRecord(ref ValueRef) (Record, error) {
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return Record{}, errors.New("vlog: closed")
	}
	file := l.files[ref.SegmentID]
	l.mu.RUnlock()
	if file == nil {
		return Record{}, fmt.Errorf("vlog: unknown segment %d", ref.SegmentID)
	}

	header := make([]byte, recordHeaderSize)
	if _, err := file.ReadAt(header, int64(ref.Offset)); err != nil {
		return Record{}, err
	}

	keyLen := binary.LittleEndian.Uint32(header[4:])
	valueLen := binary.LittleEndian.Uint32(header[8:])
	if valueLen != ref.ValueLen {
		return Record{}, fmt.Errorf("vlog: value length mismatch for segment %d offset %d", ref.SegmentID, ref.Offset)
	}

	payloadLen := int(keyLen) + int(valueLen)
	payload := make([]byte, payloadLen)
	if _, err := file.ReadAt(payload, int64(ref.Offset)+recordHeaderSize); err != nil {
		return Record{}, err
	}

	checksum := crc32.NewIEEE()
	_, _ = checksum.Write(header[4:])
	_, _ = checksum.Write(payload)
	if checksum.Sum32() != binary.LittleEndian.Uint32(header) {
		return Record{}, fmt.Errorf("vlog: checksum mismatch for segment %d offset %d", ref.SegmentID, ref.Offset)
	}

	value := payload[keyLen:]
	if crc32.ChecksumIEEE(value) != ref.Checksum {
		return Record{}, fmt.Errorf("vlog: value checksum mismatch for segment %d offset %d", ref.SegmentID, ref.Offset)
	}

	return Record{
		Key:   payload[:keyLen],
		Value: value,
	}, nil
}

func (l *Log) Sync() error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return errors.New("vlog: closed")
	}
	if l.active == nil || l.active.f == nil {
		return errors.New("vlog: missing active segment")
	}
	return l.active.f.Sync()
}

func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	var err error
	if l.active != nil && l.active.f != nil {
		err = l.active.f.Sync()
	}
	l.closed = true
	if closeErr := l.closeFiles(); err == nil {
		err = closeErr
	}
	l.mu.Unlock()

	return err
}

func (l *Log) closeFiles() error {
	var closeErr error
	for id, file := range l.files {
		if file == nil {
			continue
		}
		if err := file.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		l.files[id] = nil
	}

	if l.active != nil {
		l.active.f = nil
	}

	return closeErr
}

func (l *Log) rotateSegmentLocked(lastID uint64) error {
	if l.active != nil && l.active.f != nil {
		if err := l.active.f.Sync(); err != nil {
			return err
		}
	}

	id := nextSegmentID(lastID)
	path := filepath.Join(l.dir, segmentName(id))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}

	l.files[id] = file
	l.active = &segment{id: id, path: path, f: file}

	return nil
}

func scanSegment(file *os.File) (uint64, bool, error) {
	stat, err := file.Stat()
	if err != nil {
		return 0, false, err
	}
	size := uint64(stat.Size())
	header := make([]byte, recordHeaderSize)
	var offset uint64

	for offset < size {
		if size-offset < recordHeaderSize {
			return offset, true, nil
		}
		if _, err := file.ReadAt(header, int64(offset)); err != nil {
			if err == io.EOF {
				return offset, true, nil
			}
			return offset, false, err
		}

		keyLen := binary.LittleEndian.Uint32(header[4:])
		valueLen := binary.LittleEndian.Uint32(header[8:])
		recordSize := uint64(recordHeaderSize) + uint64(keyLen) + uint64(valueLen)
		if recordSize < recordHeaderSize {
			return offset, false, fmt.Errorf("vlog: invalid record size")
		}
		if offset+recordSize > size {
			return offset, true, nil
		}

		payload := make([]byte, int(keyLen)+int(valueLen))
		if _, err := file.ReadAt(payload, int64(offset)+recordHeaderSize); err != nil {
			if err == io.EOF {
				return offset, true, nil
			}
			return offset, false, err
		}

		checksum := crc32.NewIEEE()
		_, _ = checksum.Write(header[4:])
		_, _ = checksum.Write(payload)
		if checksum.Sum32() != binary.LittleEndian.Uint32(header) {
			return offset, true, nil
		}

		offset += recordSize
	}

	return offset, false, nil
}

func parseSegmentID(name string) (uint64, bool) {
	if !strings.HasSuffix(name, ".vlog") {
		return 0, false
	}
	id, err := strconv.ParseUint(strings.TrimSuffix(name, ".vlog"), 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

func segmentName(id uint64) string {
	return fmt.Sprintf("%020d.vlog", id)
}

func nextSegmentID(lastID uint64) uint64 {
	id := uint64(time.Now().UnixNano())
	if id <= lastID {
		return lastID + 1
	}
	return id
}
