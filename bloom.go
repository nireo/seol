package seol

const (
	bitsPerUint = 32 << (^uint(0) >> 63)
)

// bitvec represents a non-concurrency safe bit vector which is partitioned into blocks of uint64.
type bitvec struct {
	bits []uint64
}

func coord(index int) (int, uint64) {
	// >> 6 divides by 64 and the bitmask for a given bit index
	return index >> 6, uint64(1) << (uint64(index) & 0b111111)
}

// bitlen returns the amount of bits in a given vector
func (bv *bitvec) bitlen() int {
	return len(bv.bits) * bitsPerUint
}

func (bv *bitvec) check(index int) bool {
	i, bit := coord(index)
	return (bv.bits[i] & bit) > 0
}

func (bv *bitvec) set(index int) bool {
	i, bit := coord(index)
	prev := (bv.bits[i] & bit) > 0
	bv.bits[i] |= bit
	return prev
}

func (bv *bitvec) clear() {
	for i := range len(bv.bits) {
		bv.bits[i] = 0
	}
}

func (bv *bitvec) union(other *bitvec) {
	if len(bv.bits) != len(other.bits) {
		panic("mismatch length in bytes union")
	}

	for i := range len(bv.bits) {
		bv.bits[i] |= other.bits[i]
	}
}

func (bv *bitvec) intersect(other *bitvec) {
	if len(bv.bits) != len(other.bits) {
		panic("mismatch length in bytes interest")
	}

	for i := range len(bv.bits) {
		bv.bits[i] &= other.bits[i]
	}
}

func (bv *bitvec) eq(other *bitvec) bool {
	if len(bv.bits) != len(other.bits) {
		return false
	}

	for i := range len(bv.bits) {
		if bv.bits[i] != other.bits[i] {
			return false
		}
	}

	return true
}

func (bv *bitvec) clone() *bitvec {
	newbits := make([]uint64, len(bv.bits))
	copy(newbits, bv.bits)
	return &bitvec{bits: newbits}
}
