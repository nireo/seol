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
	i := index >> 6
	bit := uint64(1) << (uint(index) & 0b111111)
	return bv.bits[i]&bit != 0
}

func (bv *bitvec) set(index int) bool {
	i := index >> 6
	bit := uint64(1) << (uint(index) & 0b111111)
	prev := bv.bits[i]&bit != 0
	bv.bits[i] |= bit
	return prev
}

func (bv *bitvec) clear() {
	clear(bv.bits)
}

func (bv *bitvec) union(other *bitvec) {
	if len(bv.bits) != len(other.bits) {
		panic("mismatch length in bytes union")
	}

	bits := bv.bits
	otherBits := other.bits
	n := len(bits)
	i := 0

	for ; i+4 <= n; i += 4 {
		bits[i] |= otherBits[i]
		bits[i+1] |= otherBits[i+1]
		bits[i+2] |= otherBits[i+2]
		bits[i+3] |= otherBits[i+3]
	}

	for ; i < n; i++ {
		bits[i] |= otherBits[i]
	}
}

func (bv *bitvec) intersect(other *bitvec) {
	if len(bv.bits) != len(other.bits) {
		panic("mismatch length in bytes interest")
	}

	bits := bv.bits
	otherBits := other.bits
	n := len(bits)
	i := 0

	for ; i+4 <= n; i += 4 {
		bits[i] &= otherBits[i]
		bits[i+1] &= otherBits[i+1]
		bits[i+2] &= otherBits[i+2]
		bits[i+3] &= otherBits[i+3]
	}

	for ; i < n; i++ {
		bits[i] &= otherBits[i]
	}
}

func (bv *bitvec) eq(other *bitvec) bool {
	if len(bv.bits) != len(other.bits) {
		return false
	}

	bits := bv.bits
	otherBits := other.bits
	n := len(bits)
	i := 0

	for ; i+4 <= n; i += 4 {
		if (bits[i]^otherBits[i])|
			(bits[i+1]^otherBits[i+1])|
			(bits[i+2]^otherBits[i+2])|
			(bits[i+3]^otherBits[i+3]) != 0 {
			return false
		}
	}

	for ; i < n; i++ {
		if bits[i] != otherBits[i] {
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
