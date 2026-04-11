package sstable

import (
	"bytes"
	"encoding/binary"
	"os"
	"strconv"
	"testing"

	"github.com/nireo/seol/bloom"
	"github.com/nireo/seol/skiplist"
)

func TestTableIndexEncodeDecodeFullRange(t *testing.T) {
	original := tableIndex{
		ranges: []dataRange{
			{firstKey: []byte("aaa"), offset: 100, length: 50},
			{firstKey: []byte("bbb"), offset: 200, length: 75},
			{firstKey: []byte("ccc"), offset: 300, length: 100},
		},
	}

	encoded := original.encodeFullRange()

	var decoded tableIndex
	decoded.decodeFullRange(encoded)

	if len(decoded.ranges) != len(original.ranges) {
		t.Fatalf("range count: got %d, want %d", len(decoded.ranges), len(original.ranges))
	}

	for i := range original.ranges {
		got := decoded.ranges[i]
		want := original.ranges[i]

		if !bytes.Equal(got.firstKey, want.firstKey) {
			t.Fatalf("range %d firstKey: got %q, want %q", i, got.firstKey, want.firstKey)
		}

		if got.offset != want.offset {
			t.Fatalf("range %d offset: got %d, want %d", i, got.offset, want.offset)
		}

		if got.length != want.length {
			t.Fatalf("range %d length: got %d, want %d", i, got.length, want.length)
		}
	}
}

func TestFlushSkiplistWritesBlocksIndexAndFooter(t *testing.T) {
	dir := t.TempDir()
	s := skiplist.New(1 << 20)

	type kv struct {
		key   string
		value []byte
	}

	inserted := []kv{
		{key: "c", value: bytes.Repeat([]byte{'C'}, 1400)},
		{key: "a", value: bytes.Repeat([]byte{'A'}, 1400)},
		{key: "e", value: bytes.Repeat([]byte{'E'}, 1400)},
		{key: "b", value: bytes.Repeat([]byte{'B'}, 1400)},
		{key: "d", value: bytes.Repeat([]byte{'D'}, 1400)},
	}
	for _, entry := range inserted {
		s.Put([]byte(entry.key), entry.value)
	}

	sst, err := Flush(dir, s)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	defer func() { _ = sst.Close() }()

	data, err := os.ReadFile(sst.f.Name())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) < sstableFooterSize {
		t.Fatalf("sstable too small: got %d bytes", len(data))
	}

	bloomData, err := sst.filter.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	storedBloom, err := bloom.ReadFilter(data[:len(bloomData)])
	if err != nil {
		t.Fatalf("ReadFilter: %v", err)
	}
	storedBloomData, err := storedBloom.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary stored bloom: %v", err)
	}
	if !bytes.Equal(storedBloomData, bloomData) {
		t.Fatalf("stored bloom differs from in-memory bloom")
	}
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		if !storedBloom.ContainsString(key) {
			t.Fatalf("stored bloom missing %q", key)
		}
	}

	footerOffset := len(data) - sstableFooterSize
	indexOffset := binary.LittleEndian.Uint64(data[footerOffset:])
	if got := binary.LittleEndian.Uint32(data[footerOffset+8:]); got != sstableMagic {
		t.Fatalf("footer magic: got %#x, want %#x", got, sstableMagic)
	}
	if got := data[footerOffset+12]; got != sstableVersion {
		t.Fatalf("footer version: got %d, want %d", got, sstableVersion)
	}

	var decoded tableIndex
	decoded.decodeFullRange(data[indexOffset:footerOffset])
	if len(decoded.ranges) != 3 {
		t.Fatalf("index range count: got %d, want 3", len(decoded.ranges))
	}

	wantFirstKeys := []string{"a", "c", "e"}
	for i, want := range wantFirstKeys {
		if got := string(decoded.ranges[i].firstKey); got != want {
			t.Fatalf("range %d first key: got %q, want %q", i, got, want)
		}
	}

	if len(decoded.ranges) != len(sst.idx.ranges) {
		t.Fatalf("sst index range count: got %d, want %d", len(decoded.ranges), len(sst.idx.ranges))
	}
	for i := range decoded.ranges {
		if !bytes.Equal(decoded.ranges[i].firstKey, sst.idx.ranges[i].firstKey) {
			t.Fatalf("sst index first key %d: got %q, want %q", i, decoded.ranges[i].firstKey, sst.idx.ranges[i].firstKey)
		}
		if decoded.ranges[i].offset != sst.idx.ranges[i].offset {
			t.Fatalf("sst index offset %d: got %d, want %d", i, decoded.ranges[i].offset, sst.idx.ranges[i].offset)
		}
		if decoded.ranges[i].length != sst.idx.ranges[i].length {
			t.Fatalf("sst index length %d: got %d, want %d", i, decoded.ranges[i].length, sst.idx.ranges[i].length)
		}
	}

	expected := []kv{
		{key: "a", value: bytes.Repeat([]byte{'A'}, 1400)},
		{key: "b", value: bytes.Repeat([]byte{'B'}, 1400)},
		{key: "c", value: bytes.Repeat([]byte{'C'}, 1400)},
		{key: "d", value: bytes.Repeat([]byte{'D'}, 1400)},
		{key: "e", value: bytes.Repeat([]byte{'E'}, 1400)},
	}

	dataEnd := uint64(len(bloomData))
	entryIdx := 0
	for i, ra := range decoded.ranges {
		if ra.length == 0 || int(ra.length) > sstableBlockSize {
			t.Fatalf("range %d length: got %d, want 1..%d", i, ra.length, sstableBlockSize)
		}
		if ra.offset != dataEnd {
			t.Fatalf("range %d offset: got %d, want %d", i, ra.offset, dataEnd)
		}

		block := data[ra.offset : ra.offset+uint64(ra.length)]
		ptr := 0
		entriesInBlock := 0
		for ptr < len(block) {
			if len(block)-ptr < entryMetaSize {
				t.Fatalf("range %d truncated entry header", i)
			}

			klen := int(binary.LittleEndian.Uint16(block[ptr:]))
			vlen := int(binary.LittleEndian.Uint32(block[ptr+2:]))
			ptr += entryMetaSize
			if ptr+klen+vlen > len(block) {
				t.Fatalf("range %d truncated entry body", i)
			}

			key := block[ptr : ptr+klen]
			ptr += klen
			value := block[ptr : ptr+vlen]
			ptr += vlen

			if entriesInBlock == 0 && !bytes.Equal(key, ra.firstKey) {
				t.Fatalf("range %d first key mismatch: got %q, want %q", i, key, ra.firstKey)
			}
			if entryIdx >= len(expected) {
				t.Fatalf("decoded too many entries")
			}
			if got := string(key); got != expected[entryIdx].key {
				t.Fatalf("entry %d key: got %q, want %q", entryIdx, got, expected[entryIdx].key)
			}
			if !bytes.Equal(value, expected[entryIdx].value) {
				t.Fatalf("entry %d value mismatch", entryIdx)
			}

			entryIdx++
			entriesInBlock++
		}
		if ptr != len(block) {
			t.Fatalf("range %d did not decode cleanly", i)
		}

		dataEnd = ra.offset + uint64(ra.length)
	}

	if entryIdx != len(expected) {
		t.Fatalf("decoded entries: got %d, want %d", entryIdx, len(expected))
	}
	if dataEnd != indexOffset {
		t.Fatalf("index offset: got %d, want %d", indexOffset, dataEnd)
	}
}

func TestFlushSkiplistRejectsOversizedEntry(t *testing.T) {
	dir := t.TempDir()
	s := skiplist.New(1 << 20)
	s.Put([]byte("a"), bytes.Repeat([]byte{'x'}, sstableBlockSize))

	if _, err := Flush(dir, s); err == nil {
		t.Fatalf("Flush: expected oversized entry error")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected cleanup after failure, found %d files", len(entries))
	}
}

func TestOpenSSTableGet(t *testing.T) {
	dir := t.TempDir()
	s := skiplist.New(1 << 20)

	expected := map[string][]byte{
		"a": bytes.Repeat([]byte{'A'}, 1400),
		"b": bytes.Repeat([]byte{'B'}, 1400),
		"c": bytes.Repeat([]byte{'C'}, 1400),
		"d": bytes.Repeat([]byte{'D'}, 1400),
		"e": bytes.Repeat([]byte{'E'}, 1400),
	}
	for key, value := range expected {
		s.Put([]byte(key), value)
	}

	flushed, err := Flush(dir, s)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	path := flushed.f.Name()
	if err := flushed.Close(); err != nil {
		t.Fatalf("close flushed table: %v", err)
	}

	loaded, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if loaded.filter == nil {
		t.Fatalf("expected bloom filter to be loaded")
	}

	for key, want := range expected {
		got, err := loaded.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %q: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get %q: got %d bytes, want %d", key, len(got), len(want))
		}
	}

	got, err := loaded.Get([]byte("aa"))
	if err != nil {
		t.Fatalf("get aa: %v", err)
	}
	if got != nil {
		t.Fatalf("get aa: got %q, want nil", got)
	}

	got, err = loaded.Get([]byte("z"))
	if err != nil {
		t.Fatalf("get z: %v", err)
	}
	if got != nil {
		t.Fatalf("get z: got %q, want nil", got)
	}

	missing := ""
	for i := 0; i < 1024; i++ {
		candidate := "missing-" + strconv.Itoa(i)
		if !loaded.filter.ContainsString(candidate) {
			missing = candidate
			break
		}
	}
	if missing == "" {
		t.Fatalf("failed to find bloom-negative key")
	}

	if err := loaded.Close(); err != nil {
		t.Fatalf("close loaded table: %v", err)
	}

	got, err = loaded.Get([]byte(missing))
	if err != nil {
		t.Fatalf("get %q after close: %v", missing, err)
	}
	if got != nil {
		t.Fatalf("get %q after close: got %q, want nil", missing, got)
	}
}

func TestOpenSSTableGetUsesParsedBlockLookups(t *testing.T) {
	dir := t.TempDir()
	s := skiplist.New(1 << 20)

	expected := make(map[string][]byte, 64)
	for i := range 64 {
		key := "key-" + strconv.Itoa(i)
		value := []byte("value-" + strconv.Itoa(i))
		expected[key] = value
		s.Put([]byte(key), value)
	}

	flushed, err := Flush(dir, s)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	path := flushed.f.Name()
	if err := flushed.Close(); err != nil {
		t.Fatalf("close flushed table: %v", err)
	}

	loaded, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = loaded.Close() }()

	for pass := 0; pass < 2; pass++ {
		for key, want := range expected {
			got, err := loaded.Get([]byte(key))
			if err != nil {
				t.Fatalf("get %q on pass %d: %v", key, pass, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("get %q on pass %d: got %q, want %q", key, pass, got, want)
			}
		}
	}
}
