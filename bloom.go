package seol

import (
	"math"
	"math/bits"

	"github.com/zeebo/xxh3"
)

const (
	derivedHashMultiplier uint64 = 0x517cc1b727220a95
	wordBits              int    = 64
)

// Filter is a non-concurrent Bloom filter backed by 64-bit words.
// False positives are possible, but false negatives are not.
type Filter struct {
	words     []uint64
	numBits   uint64
	numHashes uint32
	seed      uint64
}

// New returns a filter with the requested bit count and hash count.
// The bit count is rounded up to a whole number of 64-bit words.
func New(numBits, numHashes int) *Filter {
	return NewSeeded(numBits, numHashes, 0)
}

// NewSeeded returns a filter with the requested bit count, hash count, and xxh3 seed.
func NewSeeded(numBits, numHashes int, seed uint64) *Filter {
	if numBits <= 0 {
		panic("bloom: numBits must be > 0")
	}

	words := wordCount(numBits)
	return &Filter{
		words:     make([]uint64, words),
		numBits:   uint64(words * wordBits),
		numHashes: normalizeHashes(numHashes),
		seed:      seed,
	}
}

// NewFor returns a filter sized for expectedItems and the target false positive rate.
func NewFor(expectedItems int, falsePositiveRate float64) *Filter {
	return NewForSeeded(expectedItems, falsePositiveRate, 0)
}

// NewForSeeded returns a filter sized for expectedItems, the target false positive rate,
// and the provided xxh3 seed.
func NewForSeeded(expectedItems int, falsePositiveRate float64, seed uint64) *Filter {
	if math.IsNaN(falsePositiveRate) || falsePositiveRate <= 0 || falsePositiveRate >= 1 {
		panic("bloom: falsePositiveRate must be in (0, 1)")
	}

	expectedItems = normalizeItems(expectedItems)
	numBits := optimalBits(expectedItems, falsePositiveRate)
	words := wordCount(numBits)
	actualBits := words * wordBits

	return &Filter{
		words:     make([]uint64, words),
		numBits:   uint64(actualBits),
		numHashes: optimalHashes(actualBits, expectedItems),
		seed:      seed,
	}
}

// NumBits returns the number of addressable bits in the filter.
func (f *Filter) NumBits() int {
	return int(f.numBits)
}

// NumHashes returns the number of hashes checked per item.
func (f *Filter) NumHashes() int {
	return int(f.numHashes)
}

// Add inserts b and reports whether it may have already been present.
func (f *Filter) Add(b []byte) bool {
	return f.AddHash(xxh3.HashSeed(b, f.seed))
}

// Contains reports whether b may be present in the filter.
func (f *Filter) Contains(b []byte) bool {
	return f.ContainsHash(xxh3.HashSeed(b, f.seed))
}

// AddString inserts s and reports whether it may have already been present.
func (f *Filter) AddString(s string) bool {
	return f.AddHash(xxh3.HashStringSeed(s, f.seed))
}

// ContainsString reports whether s may be present in the filter.
func (f *Filter) ContainsString(s string) bool {
	return f.ContainsHash(xxh3.HashStringSeed(s, f.seed))
}

// AddHash inserts a precomputed source hash and reports whether it may have already been present.
func (f *Filter) AddHash(hash uint64) bool {
	idx := index(f.numBits, hash)
	previouslyContained := f.set(idx)
	if f.numHashes == 1 {
		return previouslyContained
	}

	h1 := hash
	h2 := hash * derivedHashMultiplier
	for i := uint32(1); i < f.numHashes; i++ {
		h1 = bits.RotateLeft64(h1, 5) + h2
		wasSet := f.set(index(f.numBits, h1))
		previouslyContained = previouslyContained && wasSet
	}

	return previouslyContained
}

// ContainsHash reports whether a precomputed source hash may be present in the filter.
func (f *Filter) ContainsHash(hash uint64) bool {
	if !f.check(index(f.numBits, hash)) {
		return false
	}
	if f.numHashes == 1 {
		return true
	}

	h1 := hash
	h2 := hash * derivedHashMultiplier
	for i := uint32(1); i < f.numHashes; i++ {
		h1 = bits.RotateLeft64(h1, 5) + h2
		if !f.check(index(f.numBits, h1)) {
			return false
		}
	}

	return true
}

// Reset clears all bits in the filter.
func (f *Filter) Reset() {
	clear(f.words)
}

func (f *Filter) check(idx uint64) bool {
	word := int(idx >> 6)
	mask := uint64(1) << (idx & 63)
	return f.words[word]&mask != 0
}

func (f *Filter) set(idx uint64) bool {
	word := int(idx >> 6)
	mask := uint64(1) << (idx & 63)
	previouslyContained := f.words[word]&mask != 0
	f.words[word] |= mask
	return previouslyContained
}

func index(numBits, hash uint64) uint64 {
	hi, _ := bits.Mul64(hash, numBits)
	return hi
}

func wordCount(numBits int) int {
	return (numBits + 63) / 64
}

func normalizeItems(expectedItems int) int {
	if expectedItems < 1 {
		return 1
	}
	return expectedItems
}

func normalizeHashes(numHashes int) uint32 {
	if numHashes < 1 {
		return 1
	}
	return uint32(numHashes)
}

func optimalHashes(numBits, expectedItems int) uint32 {
	hashes := math.Round(math.Ln2 * float64(numBits) / float64(expectedItems))
	if hashes < 1 {
		return 1
	}
	return uint32(hashes)
}

func optimalBits(expectedItems int, falsePositiveRate float64) int {
	bits := math.Ceil(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2))
	if bits < 64 {
		return 64
	}

	return int(bits)
}
