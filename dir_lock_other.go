//go:build !darwin

package seol

type noopDirectoryLock struct{}

func acquireDirectoryLock(dir string) (directoryLock, error) {
	return noopDirectoryLock{}, nil
}

func (noopDirectoryLock) close() error {
	return nil
}
