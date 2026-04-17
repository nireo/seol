//go:build darwin

package seol

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const directoryLockFilename = "LOCK"

type fileDirectoryLock struct {
	file *os.File
}

func acquireDirectoryLock(dir string) (directoryLock, error) {
	path := filepath.Join(dir, directoryLockFilename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("seol: open directory lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrDatabaseLocked
		}
		return nil, fmt.Errorf("seol: lock directory: %w", err)
	}
	return &fileDirectoryLock{file: file}, nil
}

func (l *fileDirectoryLock) close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	if err != nil {
		return fmt.Errorf("seol: unlock directory: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("seol: close directory lock: %w", closeErr)
	}
	return nil
}
