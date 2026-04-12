package seol

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/nireo/seol/vlog"
)

func TestEncodeValueRefUsesCompactFormatForEncodableOffsets(t *testing.T) {
	ref := vlog.ValueRef{
		SegmentID: 42,
		Offset:    128,
		ValueLen:  512,
		Checksum:  99,
	}

	encoded := encodeValueRef(ref)
	if got, want := len(encoded), len(valueRefPrefix)+compactEncodedValueRefSize; got != want {
		t.Fatalf("encoded length: got %d, want %d", got, want)
	}

	decoded, ok, err := decodeValueRef(encoded)
	if err != nil {
		t.Fatalf("decodeValueRef: %v", err)
	}
	if !ok {
		t.Fatalf("decodeValueRef did not recognize compact value ref")
	}
	if decoded != ref {
		t.Fatalf("decoded ref: got %+v, want %+v", decoded, ref)
	}
}

func TestEncodeValueRefFallsBackToLegacyFormatForLargeOffsets(t *testing.T) {
	ref := vlog.ValueRef{
		SegmentID: 42,
		Offset:    uint64(math.MaxUint32) + 1,
		ValueLen:  512,
		Checksum:  99,
	}

	encoded := encodeValueRef(ref)
	if got, want := len(encoded), len(valueRefPrefix)+vlog.EncodedValueRefSize; got != want {
		t.Fatalf("encoded length: got %d, want %d", got, want)
	}

	decoded, ok, err := decodeValueRef(encoded)
	if err != nil {
		t.Fatalf("decodeValueRef: %v", err)
	}
	if !ok {
		t.Fatalf("decodeValueRef did not recognize legacy value ref")
	}
	if decoded != ref {
		t.Fatalf("decoded ref: got %+v, want %+v", decoded, ref)
	}
}

func TestDecodeValueRefSupportsLegacyEncoding(t *testing.T) {
	ref := vlog.ValueRef{
		SegmentID: 77,
		Offset:    4096,
		ValueLen:  1024,
		Checksum:  1234,
	}
	encoded := make([]byte, len(valueRefPrefix)+vlog.EncodedValueRefSize)
	copy(encoded, valueRefPrefix)
	binary.LittleEndian.PutUint64(encoded[len(valueRefPrefix):], ref.SegmentID)
	binary.LittleEndian.PutUint64(encoded[len(valueRefPrefix)+8:], ref.Offset)
	binary.LittleEndian.PutUint32(encoded[len(valueRefPrefix)+16:], ref.ValueLen)
	binary.LittleEndian.PutUint32(encoded[len(valueRefPrefix)+20:], ref.Checksum)

	decoded, ok, err := decodeValueRef(encoded)
	if err != nil {
		t.Fatalf("decodeValueRef: %v", err)
	}
	if !ok {
		t.Fatalf("decodeValueRef did not recognize legacy value ref")
	}
	if decoded != ref {
		t.Fatalf("decoded ref: got %+v, want %+v", decoded, ref)
	}
}
