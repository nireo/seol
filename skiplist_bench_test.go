package seol

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
	s := newSkiplist(benchmarkSkiplistArenaSize(size))
	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		if i > 0 && i%size == 0 {
			b.StopTimer()
			s = newSkiplist(benchmarkSkiplistArenaSize(size))
			b.StartTimer()
		}
		s.put(keys[i%size], values[i%size])
	}

	benchmarkSkiplistIntSink = int(s.memSize())
}

func BenchmarkSkiplistGet(b *testing.B) {
	s, keys, _ := newBenchmarkSkiplist(1 << 16)
	b.ReportAllocs()

	var value []byte
	for i := 0; b.Loop(); i++ {
		value = s.get(keys[i%len(keys)])
	}

	benchmarkSkiplistBytesSink = value
}

func BenchmarkSkiplistSeek(b *testing.B) {
	s, keys, _ := newBenchmarkSkiplist(1 << 16)
	it := s.newIterator()
	defer it.close()
	b.ReportAllocs()

	total := 0
	for i := 0; b.Loop(); i++ {
		it.seek(keys[i%len(keys)])
		total += len(it.key()) + len(it.value())
	}

	benchmarkSkiplistIntSink = total
}

func BenchmarkSkiplistIterate(b *testing.B) {
	s, _, _ := newBenchmarkSkiplist(1 << 16)
	it := s.newIterator()
	defer it.close()
	it.seekToFirst()
	b.ReportAllocs()

	total := 0
	for b.Loop() {
		if !it.valid() {
			it.seekToFirst()
		}
		total += len(it.key()) + len(it.value())
		it.next()
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

func newBenchmarkSkiplist(size int) (*skiplist, [][]byte, [][]byte) {
	keys := benchmarkSkiplistKeys(size)
	values := benchmarkSkiplistValues(size)
	s := newSkiplist(benchmarkSkiplistArenaSize(size))
	for i := range size {
		s.put(keys[i], values[i])
	}
	return s, keys, values
}
