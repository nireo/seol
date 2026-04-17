package seol

import "errors"

var ErrDatabaseLocked = errors.New("seol: database directory is locked")

type directoryLock interface {
	close() error
}
