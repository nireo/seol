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

type wal struct {
	path         string
	f            *os.File
	bw           *bufio.Writer
	scratch      []byte
	hasData      bool
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

func newWAL(path string, file *os.File, hasData bool, syncInterval time.Duration) *wal {
	w := &wal{
		path:         path,
		f:            file,
		bw:           bufio.NewWriter(file),
		hasData:      hasData,
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
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("wal: closed")
	}
	if w.syncErr != nil {
		return w.syncErr
	}

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
	w.hasData = true
	w.pending++
	target := w.pending

	if w.syncInterval <= 0 {
		if err := w.flushAndSyncLocked(); err != nil {
			w.syncErr = err
			w.cond.Broadcast()
			return err
		}
		w.durable = w.pending
		w.cond.Broadcast()
		return nil
	}

	w.signalSyncLoop(w.wakeCh)
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
	w.f = nil
	w.mu.Unlock()

	w.wg.Wait()
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
	resetTimer := func() {
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
			resetTimer()
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
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.syncCount++
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
