package seol

import (
	"testing"
)

var (
	bitvecBoolSink bool
	bitvecSink     *bitvec
)

const (
	benchmarkBitvecBlocks   = 1024
	benchmarkBitvecPoolSize = 256
)

func newBenchmarkBitvec() *bitvec {
	return &bitvec{bits: make([]uint64, benchmarkBitvecBlocks)}
}

func newFilledBenchmarkBitvec(fill func(int) uint64) *bitvec {
	bv := newBenchmarkBitvec()
	for i := range bv.bits {
		bv.bits[i] = fill(i)
	}
	return bv
}

func benchmarkMutatingBitvecOp(b *testing.B, src, other *bitvec, op func(*bitvec, *bitvec)) {
	pool := make([]*bitvec, benchmarkBitvecPoolSize)
	for i := range pool {
		pool[i] = src.clone()
	}

	dst := pool[0]
	i := 0
	for b.Loop() {
		if i == len(pool) {
			b.StopTimer()
			for _, bv := range pool {
				copy(bv.bits, src.bits)
			}
			b.StartTimer()
			i = 0
		}

		dst = pool[i]
		op(dst, other)
		i++
	}

	bitvecSink = dst
}

func TestBitvecCoord(t *testing.T) {
	block, bit := coord(0)
	if block != 0 || bit != 1 {
		t.Errorf("coord(0): got (%d, %d), want (0, 1)", block, bit)
	}

	block, bit = coord(63)
	if block != 0 || bit != (1<<63) {
		t.Errorf("coord(63): got (%d, %d), want (0, 1<<63)", block, bit)
	}

	block, bit = coord(64)
	if block != 1 || bit != 1 {
		t.Errorf("coord(64): got (%d, %d), want (1, 1)", block, bit)
	}
}

func TestBitvecBitlen(t *testing.T) {
	bv := &bitvec{bits: make([]uint64, 2)}
	if bv.bitlen() != 128 { // 2 uint64s = 16 bytes = 128 bits
		t.Errorf("bitlen: got %d, want 16", bv.bitlen())
	}
}

func TestBitvecCheck(t *testing.T) {
	bv := &bitvec{bits: make([]uint64, 1)}
	if bv.check(0) {
		t.Error("check(0): want false, got true")
	}

	bv.set(0)
	if !bv.check(0) {
		t.Error("check(0): want true, got false")
	}

	bv.set(63)
	if !bv.check(63) {
		t.Error("check(63): want true, got false")
	}
}

func TestBitvecSet(t *testing.T) {
	bv := &bitvec{bits: make([]uint64, 1)}

	if bv.set(0) {
		t.Error("set(0): want false (first set), got true")
	}

	if !bv.set(0) {
		t.Error("set(0): want true (already set), got false")
	}
}

func TestBitvecUnion(t *testing.T) {
	bv1 := &bitvec{bits: []uint64{0b0001, 0b0000}}
	bv2 := &bitvec{bits: []uint64{0b0010, 0b1000}}

	bv1.union(bv2)

	if bv1.bits[0] != 0b0011 {
		t.Errorf("union: bits[0] got %b, want 0b0011", bv1.bits[0])
	}
	if bv1.bits[1] != 0b1000 {
		t.Errorf("union: bits[1] got %b, want 0b1000", bv1.bits[1])
	}
}

func TestBitvecIntersect(t *testing.T) {
	bv1 := &bitvec{bits: []uint64{0b1101}}
	bv2 := &bitvec{bits: []uint64{0b1011}}

	bv1.intersect(bv2)

	if bv1.bits[0] != 0b1001 {
		t.Errorf("intersect: got %b, want 0b1001", bv1.bits[0])
	}
}

func TestBitvecEq(t *testing.T) {
	bv1 := &bitvec{bits: []uint64{0b1101}}
	bv2 := &bitvec{bits: []uint64{0b1101}}
	bv3 := &bitvec{bits: []uint64{0b1011}}

	if !bv1.eq(bv2) {
		t.Error("eq: equal vectors should be equal")
	}
	if bv1.eq(bv3) {
		t.Error("eq: different vectors should not be equal")
	}

	bv4 := &bitvec{bits: []uint64{0b1101, 0b0000}}
	if bv1.eq(bv4) {
		t.Error("eq: different length vectors should not be equal")
	}
}

func BenchmarkBitvecCheck(b *testing.B) {
	bv := newBenchmarkBitvec()
	bv.set(500)
	var ok bool
	for b.Loop() {
		ok = bv.check(500)
	}
	bitvecBoolSink = ok
}

func BenchmarkBitvecSet(b *testing.B) {
	bv := newBenchmarkBitvec()
	bitlen := bv.bitlen()
	var wasSet bool
	i := 0
	for b.Loop() {
		if i > 0 && i%bitlen == 0 {
			b.StopTimer()
			bv.clear()
			b.StartTimer()
		}
		wasSet = bv.set(i % bitlen)
		i++
	}
	bitvecBoolSink = wasSet
}

func BenchmarkBitvecClear(b *testing.B) {
	bv := newBenchmarkBitvec()
	for b.Loop() {
		bv.clear()
	}
}

func BenchmarkBitvecUnion(b *testing.B) {
	bv1 := newFilledBenchmarkBitvec(func(i int) uint64 { return ^uint64(i) })
	bv2 := newFilledBenchmarkBitvec(func(i int) uint64 { return uint64(i) })
	benchmarkMutatingBitvecOp(b, bv1, bv2, (*bitvec).union)
}

func BenchmarkBitvecIntersect(b *testing.B) {
	bv1 := newFilledBenchmarkBitvec(func(i int) uint64 { return uint64(i) })
	bv2 := newFilledBenchmarkBitvec(func(i int) uint64 { return uint64(i) >> 1 })
	benchmarkMutatingBitvecOp(b, bv1, bv2, (*bitvec).intersect)
}

func BenchmarkBitvecEq(b *testing.B) {
	bv1 := newFilledBenchmarkBitvec(func(i int) uint64 { return uint64(i) })
	bv2 := newFilledBenchmarkBitvec(func(i int) uint64 { return uint64(i) })
	var eq bool
	for b.Loop() {
		eq = bv1.eq(bv2)
	}
	bitvecBoolSink = eq
}

func BenchmarkBitvecClone(b *testing.B) {
	bv := newFilledBenchmarkBitvec(func(i int) uint64 { return uint64(i) })
	var clone *bitvec
	for b.Loop() {
		clone = bv.clone()
	}
	bitvecSink = clone
}
