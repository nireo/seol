package seol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

const walRecordHeaderSize = 4 + 4 + 4 // checksum + keylen + vallen

type wal struct {
	path    string
	f       *os.File
	bw      *bufio.Writer
	scratch []byte
	hasData bool
}

func walID() string {
	return fmt.Sprintf("%d.wal", time.Now().UnixNano())
}

func createWAL(baseDir string) (*wal, error) {
	path := filepath.Join(baseDir, walID())
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	return &wal{
		path: path,
		f:    file,
		bw:   bufio.NewWriter(file),
	}, nil
}

func openWALForAppend(path string) (*wal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &wal{
		path:    path,
		f:       file,
		bw:      bufio.NewWriter(file),
		hasData: stat.Size() > 0,
	}, nil
}

func replayWAL(path string, fn func(key, value []byte) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	header := make([]byte, walRecordHeaderSize)
	for {
		if _, err := io.ReadFull(file, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		checksum := binary.LittleEndian.Uint32(header)
		keyLen := int(binary.LittleEndian.Uint32(header[4:]))
		valueLen := int(binary.LittleEndian.Uint32(header[8:]))
		if keyLen < 0 || valueLen < 0 {
			return fmt.Errorf("wal: invalid record lengths")
		}

		payload := make([]byte, 8+keyLen+valueLen)
		binary.LittleEndian.PutUint32(payload, uint32(keyLen))
		binary.LittleEndian.PutUint32(payload[4:], uint32(valueLen))
		if _, err := io.ReadFull(file, payload[8:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		if got := crc32.ChecksumIEEE(payload); got != checksum {
			return fmt.Errorf("wal: checksum mismatch in %s", filepath.Base(path))
		}

		if err := fn(payload[8:8+keyLen], payload[8+keyLen:]); err != nil {
			return err
		}
	}
}

func (w *wal) appendPut(key, value []byte) error {
	recordSize := walRecordHeaderSize + len(key) + len(value)
	if cap(w.scratch) < recordSize {
		w.scratch = make([]byte, recordSize)
	}
	record := w.scratch[:recordSize]
	binary.LittleEndian.PutUint32(record[4:], uint32(len(key)))
	binary.LittleEndian.PutUint32(record[8:], uint32(len(value)))
	copy(record[walRecordHeaderSize:], key)
	copy(record[walRecordHeaderSize+len(key):], value)
	binary.LittleEndian.PutUint32(record, crc32.ChecksumIEEE(record[4:]))

	if _, err := w.bw.Write(record); err != nil {
		return err
	}

	if err := w.bw.Flush(); err != nil {
		return err
	}

	if err := w.f.Sync(); err != nil {
		return err
	}

	w.hasData = true
	return nil
}

func (w *wal) close() error {
	if w == nil || w.f == nil {
		return nil
	}

	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		w.f = nil
		return err
	}

	err := w.f.Close()
	w.f = nil
	return err
}
