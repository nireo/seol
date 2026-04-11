package vlog

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestValueRefMarshalBinaryRoundTrip(t *testing.T) {
	ref := ValueRef{
		SegmentID: 42,
		Offset:    128,
		ValueLen:  512,
		Checksum:  99,
	}

	data, err := ref.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}

	var decoded ValueRef
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}

	if decoded != ref {
		t.Fatalf("decoded ref: got %+v, want %+v", decoded, ref)
	}
}

func TestLogAppendReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, Options{SegmentMaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	alphaRef, err := log.Append([]byte("alpha"), []byte("one"))
	if err != nil {
		t.Fatalf("Append alpha: %v", err)
	}
	betaRef, err := log.Append([]byte("beta"), bytes.Repeat([]byte{'b'}, 512))
	if err != nil {
		t.Fatalf("Append beta: %v", err)
	}

	alpha, err := log.ReadRecord(alphaRef)
	if err != nil {
		t.Fatalf("ReadRecord alpha: %v", err)
	}
	if !bytes.Equal(alpha.Key, []byte("alpha")) || !bytes.Equal(alpha.Value, []byte("one")) {
		t.Fatalf("alpha record: got key=%q value=%q", alpha.Key, alpha.Value)
	}

	beta, err := log.Read(betaRef)
	if err != nil {
		t.Fatalf("Read beta: %v", err)
	}
	if !bytes.Equal(beta, bytes.Repeat([]byte{'b'}, 512)) {
		t.Fatalf("beta value mismatch: got %d bytes", len(beta))
	}

	betaFast, err := log.ReadValue(betaRef)
	if err != nil {
		t.Fatalf("ReadValue beta: %v", err)
	}
	if !bytes.Equal(betaFast, bytes.Repeat([]byte{'b'}, 512)) {
		t.Fatalf("beta fast value mismatch: got %d bytes", len(betaFast))
	}

	if err := log.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(dir, Options{SegmentMaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	reloaded, err := reopened.Read(betaRef)
	if err != nil {
		t.Fatalf("Read after reopen: %v", err)
	}
	if !bytes.Equal(reloaded, bytes.Repeat([]byte{'b'}, 512)) {
		t.Fatalf("reloaded value mismatch: got %d bytes", len(reloaded))
	}
}

func TestLogRotatesSegments(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, Options{SegmentMaxBytes: 96})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = log.Close() }()

	firstRef, err := log.Append([]byte("alpha"), bytes.Repeat([]byte{'a'}, 32))
	if err != nil {
		t.Fatalf("Append first: %v", err)
	}
	secondRef, err := log.Append([]byte("beta"), bytes.Repeat([]byte{'b'}, 40))
	if err != nil {
		t.Fatalf("Append second: %v", err)
	}

	if firstRef.SegmentID == secondRef.SegmentID {
		t.Fatalf("expected segment rotation, both refs use %d", firstRef.SegmentID)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	segments := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".vlog" {
			segments++
		}
	}
	if segments != 2 {
		t.Fatalf("segment count: got %d, want 2", segments)
	}

	firstValue, err := log.Read(firstRef)
	if err != nil {
		t.Fatalf("Read first: %v", err)
	}
	if !bytes.Equal(firstValue, bytes.Repeat([]byte{'a'}, 32)) {
		t.Fatalf("first value mismatch")
	}

	secondValue, err := log.Read(secondRef)
	if err != nil {
		t.Fatalf("Read second: %v", err)
	}
	if !bytes.Equal(secondValue, bytes.Repeat([]byte{'b'}, 40)) {
		t.Fatalf("second value mismatch")
	}
}

func TestOpenTruncatesNewestSegmentTail(t *testing.T) {
	dir := t.TempDir()
	log, err := Open(dir, Options{SegmentMaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	firstRef, err := log.Append([]byte("alpha"), []byte("one"))
	if err != nil {
		t.Fatalf("Append first: %v", err)
	}
	if _, err := log.Append([]byte("beta"), []byte("two")); err != nil {
		t.Fatalf("Append second: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segmentPath := onlySegmentPath(t, dir)
	stat, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if err := os.Truncate(segmentPath, stat.Size()-2); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	reopened, err := Open(dir, Options{SegmentMaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	firstValue, err := reopened.Read(firstRef)
	if err != nil {
		t.Fatalf("Read first after reopen: %v", err)
	}
	if !bytes.Equal(firstValue, []byte("one")) {
		t.Fatalf("first value after reopen: got %q, want %q", firstValue, []byte("one"))
	}

	thirdRef, err := reopened.Append([]byte("gamma"), []byte("three"))
	if err != nil {
		t.Fatalf("Append third after reopen: %v", err)
	}
	thirdValue, err := reopened.Read(thirdRef)
	if err != nil {
		t.Fatalf("Read third after reopen: %v", err)
	}
	if !bytes.Equal(thirdValue, []byte("three")) {
		t.Fatalf("third value after reopen: got %q, want %q", thirdValue, []byte("three"))
	}
}

func onlySegmentPath(t *testing.T, dir string) string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".vlog" {
			return filepath.Join(dir, entry.Name())
		}
	}
	t.Fatalf("no .vlog segment found")
	return ""
}
