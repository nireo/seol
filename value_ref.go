package seol

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/nireo/seol/vlog"
)

var valueRefPrefix = []byte{0x00, 's', 'e', 'o', 'l', 'v', 'r', 0x01}

func encodeValueRef(ref vlog.ValueRef) []byte {
	buf := make([]byte, len(valueRefPrefix)+vlog.EncodedValueRefSize)
	copy(buf, valueRefPrefix)
	binary.LittleEndian.PutUint64(buf[len(valueRefPrefix):], ref.SegmentID)
	binary.LittleEndian.PutUint64(buf[len(valueRefPrefix)+8:], ref.Offset)
	binary.LittleEndian.PutUint32(buf[len(valueRefPrefix)+16:], ref.ValueLen)
	binary.LittleEndian.PutUint32(buf[len(valueRefPrefix)+20:], ref.Checksum)
	return buf
}

func decodeValueRef(data []byte) (vlog.ValueRef, bool, error) {
	if len(data) != len(valueRefPrefix)+vlog.EncodedValueRefSize {
		return vlog.ValueRef{}, false, nil
	}
	if !bytes.Equal(data[:len(valueRefPrefix)], valueRefPrefix) {
		return vlog.ValueRef{}, false, nil
	}

	ref := vlog.ValueRef{
		SegmentID: binary.LittleEndian.Uint64(data[len(valueRefPrefix):]),
		Offset:    binary.LittleEndian.Uint64(data[len(valueRefPrefix)+8:]),
		ValueLen:  binary.LittleEndian.Uint32(data[len(valueRefPrefix)+16:]),
		Checksum:  binary.LittleEndian.Uint32(data[len(valueRefPrefix)+20:]),
	}
	return ref, true, nil
}

func memtableEntrySize(key, value []byte) int64 {
	return int64(len(key) + len(value))
}

func storeValueForLSM(valueLog *vlog.Log, valueThreshold int, key, value []byte) ([]byte, error) {
	if len(value) <= valueThreshold {
		return value, nil
	}
	if valueLog == nil {
		return nil, fmt.Errorf("seol: value log is nil")
	}

	ref, err := valueLog.Append(key, value)
	if err != nil {
		return nil, fmt.Errorf("seol: append value log: %w", err)
	}
	return encodeValueRef(ref), nil
}

func resolveStoredValue(valueLog *vlog.Log, stored []byte) ([]byte, error) {
	ref, ok, err := decodeValueRef(stored)
	if err != nil {
		return nil, err
	}
	if !ok {
		return stored, nil
	}
	if valueLog == nil {
		return nil, fmt.Errorf("seol: value log is nil")
	}

	value, err := valueLog.Read(ref)
	if err != nil {
		return nil, fmt.Errorf("seol: read value log: %w", err)
	}
	return value, nil
}
