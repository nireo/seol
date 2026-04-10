package skiplist

import (
	"strconv"
	"testing"
)

// avoid results being optimized away
var (
	benchmarkSkiplistBytesSink []byte
	benchmarkSkiplistIntSink   int
)

func BenchmarkSkiplistPut(b *testing.B) {
	const size = 1 << 14
	keys := benchmarkSkiplistKeys(size)
	values := benchmarkSkiplistValues(size)
	s := New(benchmarkSkiplistArenaSize(size))
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		if i > 0 && i%size == 0 {
			b.StopTimer()
			s = New(benchmarkSkiplistArenaSize(size))
			b.StartTimer()
		}
		s.Put(keys[i%size], values[i%size])
	}

	benchmarkSkiplistIntSink = int(s.MemSize())
}

func BenchmarkSkiplistGet(b *testing.B) {
	s, keys, _ := newBenchmarkSkiplist(1 << 16)
	b.ReportAllocs()

	var value []byte
	for i := 0; b.Loop(); i++ {
		value = s.Get(keys[i%len(keys)])
	}

	benchmarkSkiplistBytesSink = value
}

func BenchmarkSkiplistSeek(b *testing.B) {
	s, keys, _ := newBenchmarkSkiplist(1 << 16)
	it := s.NewIterator()
	defer it.Close()
	b.ReportAllocs()

	total := 0
	for i := 0; b.Loop(); i++ {
		it.Seek(keys[i%len(keys)])
		total += len(it.Key()) + len(it.Value())
	}

	benchmarkSkiplistIntSink = total
}

func BenchmarkSkiplistIterate(b *testing.B) {
	s, _, _ := newBenchmarkSkiplist(1 << 16)
	it := s.NewIterator()
	defer it.Close()
	it.SeekToFirst()
	b.ReportAllocs()

	total := 0
	for b.Loop() {
		if !it.Valid() {
			it.SeekToFirst()
		}
		total += len(it.Key()) + len(it.Value())
		it.Next()
	}

	benchmarkSkiplistIntSink = total
}

func benchmarkSkiplistKeys(size int) [][]byte {
	keys := make([][]byte, size)
	for i := range size {
		keys[i] = []byte("key-" + strconv.Itoa(i))
	}
	return keys
}

func benchmarkSkiplistValues(size int) [][]byte {
	values := make([][]byte, size)
	for i := range size {
		values[i] = []byte("value-" + strconv.Itoa(i))
	}
	return values
}

func benchmarkSkiplistArenaSize(entries int) int64 {
	return int64(entries+1)*int64(maxNodeSize+32) + (1 << 20)
}

func newBenchmarkSkiplist(size int) (*Skiplist, [][]byte, [][]byte) {
	keys := benchmarkSkiplistKeys(size)
	values := benchmarkSkiplistValues(size)
	s := New(benchmarkSkiplistArenaSize(size))
	for i := range size {
		s.Put(keys[i], values[i])
	}
	return s, keys, values
}
