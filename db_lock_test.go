//go:build darwin

package seol

import (
	"errors"
	"testing"
)

func TestDBOpenFailsWhenDirectoryIsLocked(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open first db: %v", err)
	}
	defer func() { _ = db.Close() }()

	second, err := Open(dir)
	if err == nil {
		_ = second.Close()
		t.Fatal("Open second db: expected lock error")
	}
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("Open second db: got %v, want ErrDatabaseLocked", err)
	}
}
