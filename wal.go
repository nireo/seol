package seol

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const walRecordHeaderSize = 4 + 4 + 4 // checksum + keylen + vallen

const walBufferedWriterSize = 64 << 10

type wal struct {
	path         string
	f            *os.File
	bw           *bufio.Writer
	scratch      []byte
	syncInterval time.Duration
	wakeCh       chan struct{}
	closeCh      chan struct{}

	mu        sync.Mutex
	cond      *sync.Cond
	pending   uint64
	durable   uint64
	syncCount uint64
	syncErr   error
	closed    bool
	wg        sync.WaitGroup
}

type walBatchEntry struct {
	key   []byte
	value []byte
}

func walID() string {
	return fmt.Sprintf("%d.wal", time.Now().UnixNano())
}

func createWAL(baseDir string, syncInterval time.Duration) (*wal, error) {
	path := filepath.Join(baseDir, walID())
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	w := newWAL(path, file, false, syncInterval)
	return w, nil
}

func openWALForAppend(path string, syncInterval time.Duration) (*wal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return newWAL(path, file, stat.Size() > 0, syncInterval), nil
}

func newWAL(path string, file *os.File, _ bool, syncInterval time.Duration) *wal {
	w := &wal{
		path:         path,
		f:            file,
		bw:           bufio.NewWriterSize(file, walBufferedWriterSize),
		syncInterval: syncInterval,
	}
	w.cond = sync.NewCond(&w.mu)
	if syncInterval > 0 {
		w.wakeCh = make(chan struct{}, 1)
		w.closeCh = make(chan struct{}, 1)
		w.wg.Add(1)
		go w.syncLoop()
	}
	return w
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
	var batch [1]walBatchEntry
	batch[0] = walBatchEntry{key: key, value: value}
	return w.appendBatch(batch[:], true)
}

func (w *wal) appendBatch(entries []walBatchEntry, waitForSync bool) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("wal: closed")
	}
	if w.syncErr != nil {
		return w.syncErr
	}

	totalSize := 0
	for _, entry := range entries {
		totalSize += walRecordHeaderSize + len(entry.key) + len(entry.value)
	}
	if cap(w.scratch) < totalSize {
		w.scratch = make([]byte, totalSize)
	}
	records := w.scratch[:totalSize]
	ptr := 0
	for _, entry := range entries {
		recordSize := walRecordHeaderSize + len(entry.key) + len(entry.value)
		record := records[ptr : ptr+recordSize]
		binary.LittleEndian.PutUint32(record[4:], uint32(len(entry.key)))
		binary.LittleEndian.PutUint32(record[8:], uint32(len(entry.value)))
		copy(record[walRecordHeaderSize:], entry.key)
		copy(record[walRecordHeaderSize+len(entry.key):], entry.value)
		binary.LittleEndian.PutUint32(record, crc32.ChecksumIEEE(record[4:]))
		ptr += recordSize
	}

	w.pending += uint64(len(entries))
	target := w.pending

	if w.syncInterval <= 0 {
		if err := w.writeAndSyncRecordLocked(records); err != nil {
			w.syncErr = err
			w.cond.Broadcast()
			return err
		}
		w.durable = w.pending
		w.cond.Broadcast()
		return nil
	}

	if _, err := w.bw.Write(records); err != nil {
		return err
	}

	w.signalSyncLoop(w.wakeCh)
	if !waitForSync {
		return nil
	}
	for w.durable < target && w.syncErr == nil && !w.closed {
		w.cond.Wait()
	}
	if w.syncErr != nil {
		return w.syncErr
	}
	if w.durable < target {
		return errors.New("wal: closed before record became durable")
	}
	return nil
}

func (w *wal) close() error {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	if w.f == nil {
		w.mu.Unlock()
		return nil
	}
	if w.syncInterval <= 0 {
		if w.pending > w.durable && w.syncErr == nil {
			if err := w.flushAndSyncLocked(); err != nil {
				w.syncErr = err
			}
			w.durable = w.pending
		}
		w.closed = true
		w.cond.Broadcast()
		file := w.f
		w.f = nil
		err := w.syncErr
		w.mu.Unlock()
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		return err
	}

	w.closed = true
	w.cond.Broadcast()
	w.signalSyncLoop(w.closeCh)
	file := w.f
	w.mu.Unlock()

	w.wg.Wait()
	w.mu.Lock()
	w.f = nil
	w.mu.Unlock()
	err := w.syncErr
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (w *wal) syncLoop() {
	defer w.wg.Done()

	var (
		timer   *time.Timer
		timerCh <-chan time.Time
	)
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerCh = nil
	}
	startTimer := func() {
		if timerCh != nil {
			return
		}
		if timer == nil {
			timer = time.NewTimer(w.syncInterval)
			timerCh = timer.C
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(w.syncInterval)
		timerCh = timer.C
	}

	for {
		select {
		case <-w.wakeCh:
			startTimer()
		case <-timerCh:
			w.mu.Lock()
			if w.pending > w.durable && w.syncErr == nil {
				if err := w.flushAndSyncLocked(); err != nil {
					w.syncErr = err
				} else {
					w.durable = w.pending
				}
			}
			w.cond.Broadcast()
			shouldExit := w.closed
			w.mu.Unlock()
			stopTimer()
			if shouldExit {
				return
			}
		case <-w.closeCh:
			stopTimer()
			w.mu.Lock()
			if w.pending > w.durable && w.syncErr == nil {
				if err := w.flushAndSyncLocked(); err != nil {
					w.syncErr = err
				} else {
					w.durable = w.pending
				}
			}
			w.cond.Broadcast()
			w.mu.Unlock()
			return
		}
	}
}

func (w *wal) flushAndSyncLocked() error {
	if w.bw != nil && w.bw.Buffered() > 0 {
		if err := w.bw.Flush(); err != nil {
			return err
		}
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.syncCount++
	return nil
}

func (w *wal) writeAndSyncRecordLocked(record []byte) error {
	if err := writeAll(w.f, record); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.syncCount++
	return nil
}

func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func (w *wal) signalSyncLoop(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (w *wal) syncCountValue() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncCount
}
