package seol

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/zeebo/xxh3"
)

// avoid results being optimized away
var benchmarkBoolSink bool

func TestAddContains(t *testing.T) {
	f := New(1024, 4)

	for i := range 5000 {
		value := "member-" + strconv.Itoa(i)
		if f.AddString(value) && i == 0 {
			t.Fatalf("first insert reported already present")
		}
	}

	for i := range 5000 {
		value := "member-" + strconv.Itoa(i)
		if !f.ContainsString(value) {
			t.Fatalf("missing inserted value %q", value)
		}
	}
}

func TestRepeatInsert(t *testing.T) {
	f := New(1024, 4)

	if f.AddString("hello") {
		t.Fatalf("first insert should report false")
	}
	if !f.AddString("hello") {
		t.Fatalf("second insert should report true")
	}
}

func TestReset(t *testing.T) {
	f := New(4096, 4)

	for i := range 1000 {
		f.AddString("member-" + strconv.Itoa(i))
	}
	f.Reset()

	for i := range 1000 {
		value := "member-" + strconv.Itoa(i)
		if f.ContainsString(value) {
			t.Fatalf("value %q still present after reset", value)
		}
	}
}

func TestSeededFiltersAreDeterministic(t *testing.T) {
	left := NewForSeeded(2000, 0.01, 42)
	right := NewForSeeded(2000, 0.01, 42)
	otherSeed := NewForSeeded(2000, 0.01, 43)

	for i := range 2000 {
		value := "member-" + strconv.Itoa(i)
		left.AddString(value)
		right.AddString(value)
		otherSeed.AddString(value)
	}

	if !equalWords(left.words, right.words) {
		t.Fatalf("same seed produced different filters")
	}
	if equalWords(left.words, otherSeed.words) {
		t.Fatalf("different seeds produced the same filter")
	}
}

func TestAddHashMatchesHashedPaths(t *testing.T) {
	f := NewSeeded(2048, 4, 99)
	hash := xxh3.HashStringSeed("shared", f.seed)

	if f.AddHash(hash) {
		t.Fatalf("first prehashed insert should report false")
	}
	if !f.ContainsString("shared") {
		t.Fatalf("string path did not match prehashed insert")
	}
	if !f.Contains([]byte("shared")) {
		t.Fatalf("byte path did not match prehashed insert")
	}
}

func TestNewForChoosesUsableParameters(t *testing.T) {
	f := NewFor(1000, 0.01)

	if f.NumBits() < 64 {
		t.Fatalf("unexpected bit count %d", f.NumBits())
	}
	if f.NumHashes() < 1 {
		t.Fatalf("unexpected hash count %d", f.NumHashes())
	}
}

func TestFalsePositiveRateIsReasonable(t *testing.T) {
	const (
		expectedItems = 5000
		targetFP      = 0.01
		samples       = 50000
	)

	f := NewForSeeded(expectedItems, targetFP, 7)
	for i := range expectedItems {
		f.AddString("member-" + strconv.Itoa(i))
	}

	falsePositives := 0
	for i := expectedItems; i < expectedItems+samples; i++ {
		if f.ContainsString("non-member-" + strconv.Itoa(i)) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / float64(samples)
	if rate > targetFP*1.5 {
		t.Fatalf("false positive rate too high: got %.4f want <= %.4f", rate, targetFP*1.5)
	}
}

func TestMarshalBinaryRoundTrip(t *testing.T) {
	original := NewForSeeded(2000, 0.01, 42)
	for i := range 2000 {
		original.AddString("member-" + strconv.Itoa(i))
	}

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	if len(data) != bloomHeaderSize+len(original.words)*8 {
		t.Fatalf("encoded length: got %d, want %d", len(data), bloomHeaderSize+len(original.words)*8)
	}
	if got := binary.LittleEndian.Uint32(data); got != bloomMagic {
		t.Fatalf("magic: got %#x, want %#x", got, bloomMagic)
	}
	if got := data[4]; got != bloomVersion {
		t.Fatalf("version: got %d, want %d", got, bloomVersion)
	}

	decoded, err := ReadFilter(data)
	if err != nil {
		t.Fatalf("ReadFilter: %v", err)
	}
	if decoded.numBits != original.numBits {
		t.Fatalf("numBits: got %d, want %d", decoded.numBits, original.numBits)
	}
	if decoded.numHashes != original.numHashes {
		t.Fatalf("numHashes: got %d, want %d", decoded.numHashes, original.numHashes)
	}
	if decoded.seed != original.seed {
		t.Fatalf("seed: got %d, want %d", decoded.seed, original.seed)
	}
	if !equalWords(decoded.words, original.words) {
		t.Fatalf("decoded words differ from original")
	}

	for i := range 2000 {
		value := "member-" + strconv.Itoa(i)
		if !decoded.ContainsString(value) {
			t.Fatalf("decoded filter missing %q", value)
		}
	}
	if decoded.ContainsString("definitely-not-present") && !original.ContainsString("definitely-not-present") {
		t.Fatalf("decoded filter diverged from original")
	}
}

func TestUnmarshalBinaryRejectsInvalidData(t *testing.T) {
	if _, err := ReadFilter([]byte("short")); err == nil {
		t.Fatalf("expected short data error")
	}

	valid := NewSeeded(1024, 4, 7)
	valid.AddString("hello")
	data, err := valid.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}

	badMagic := bytes.Clone(data)
	binary.LittleEndian.PutUint32(badMagic, 0)
	if _, err := ReadFilter(badMagic); err == nil {
		t.Fatalf("expected bad magic error")
	}

	badVersion := bytes.Clone(data)
	badVersion[4]++
	if _, err := ReadFilter(badVersion); err == nil {
		t.Fatalf("expected bad version error")
	}

	badLength := data[:len(data)-1]
	if _, err := ReadFilter(badLength); err == nil {
		t.Fatalf("expected bad length error")
	}
}

func BenchmarkAddString(b *testing.B) {
	values := benchmarkStrings()
	f := NewForSeeded(len(values), 0.01, 1)
	b.ReportAllocs()

	var result bool
	for i := 0; b.Loop(); i++ {
		result = f.AddString(values[i%len(values)])
	}
	benchmarkBoolSink = result
}

func BenchmarkContainsString(b *testing.B) {
	values := benchmarkStrings()
	f := NewForSeeded(len(values), 0.01, 1)
	for _, value := range values {
		f.AddString(value)
	}
	b.ReportAllocs()

	var result bool
	for i := 0; b.Loop(); i++ {
		result = f.ContainsString(values[i%len(values)])
	}
	benchmarkBoolSink = result
}

func BenchmarkAddBytes(b *testing.B) {
	values := benchmarkBytes()
	f := NewForSeeded(len(values), 0.01, 1)
	b.ReportAllocs()

	var result bool
	for i := 0; b.Loop(); i++ {
		result = f.Add(values[i%len(values)])
	}
	benchmarkBoolSink = result
}

func BenchmarkContainsBytes(b *testing.B) {
	values := benchmarkBytes()
	f := NewForSeeded(len(values), 0.01, 1)
	for _, value := range values {
		f.Add(value)
	}
	b.ReportAllocs()

	var result bool
	for i := 0; b.Loop(); i++ {
		result = f.Contains(values[i%len(values)])
	}
	benchmarkBoolSink = result
}

func BenchmarkAddHash(b *testing.B) {
	hashes := benchmarkHashes(1)
	f := NewForSeeded(len(hashes), 0.01, 1)
	b.ReportAllocs()

	var result bool
	for i := 0; b.Loop(); i++ {
		result = f.AddHash(hashes[i%len(hashes)])
	}
	benchmarkBoolSink = result
}

func BenchmarkContainsHash(b *testing.B) {
	hashes := benchmarkHashes(1)
	f := NewForSeeded(len(hashes), 0.01, 1)
	for _, hash := range hashes {
		f.AddHash(hash)
	}
	b.ReportAllocs()

	var result bool
	for i := 0; b.Loop(); i++ {
		result = f.ContainsHash(hashes[i%len(hashes)])
	}
	benchmarkBoolSink = result
}

func equalWords(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func benchmarkStrings() []string {
	const size = 1 << 16
	values := make([]string, size)
	for i := range values {
		values[i] = "member-" + strconv.Itoa(i)
	}
	return values
}

func benchmarkBytes() [][]byte {
	strings := benchmarkStrings()
	values := make([][]byte, len(strings))
	for i, value := range strings {
		values[i] = []byte(value)
	}
	return values
}

func benchmarkHashes(seed uint64) []uint64 {
	strings := benchmarkStrings()
	hashes := make([]uint64, len(strings))
	for i, value := range strings {
		hashes[i] = xxh3.HashStringSeed(value, seed)
	}
	return hashes
}
