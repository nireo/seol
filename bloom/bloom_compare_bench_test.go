package bloom

import (
	"testing"

	bitsbloom "github.com/bits-and-blooms/bloom/v3"
)

const benchmarkComparisonFalsePositiveRate = 0.01

func BenchmarkComparisonAddString(b *testing.B) {
	values := benchmarkStrings()
	numBits, numHashes := benchmarkComparisonParameters(len(values), benchmarkComparisonFalsePositiveRate)

	b.Run("seol", func(b *testing.B) {
		f := NewSeeded(numBits, numHashes, 1)
		b.ReportAllocs()

		var result bool
		for i := 0; b.Loop(); i++ {
			result = f.AddString(values[i%len(values)])
		}
		benchmarkBoolSink = result
	})

	b.Run("bits-and-blooms", func(b *testing.B) {
		f := bitsbloom.New(uint(numBits), uint(numHashes))
		b.ReportAllocs()

		var result bool
		for i := 0; b.Loop(); i++ {
			result = f.TestAndAddString(values[i%len(values)])
		}
		benchmarkBoolSink = result
	})
}

func BenchmarkComparisonContainsString(b *testing.B) {
	values := benchmarkStrings()
	numBits, numHashes := benchmarkComparisonParameters(len(values), benchmarkComparisonFalsePositiveRate)

	b.Run("seol", func(b *testing.B) {
		f := NewSeeded(numBits, numHashes, 1)
		for _, value := range values {
			f.AddString(value)
		}
		b.ReportAllocs()

		var result bool
		for i := 0; b.Loop(); i++ {
			result = f.ContainsString(values[i%len(values)])
		}
		benchmarkBoolSink = result
	})

	b.Run("bits-and-blooms", func(b *testing.B) {
		f := bitsbloom.New(uint(numBits), uint(numHashes))
		for _, value := range values {
			f.AddString(value)
		}
		b.ReportAllocs()

		var result bool
		for i := 0; b.Loop(); i++ {
			result = f.TestString(values[i%len(values)])
		}
		benchmarkBoolSink = result
	})
}

func BenchmarkComparisonAddBytes(b *testing.B) {
	values := benchmarkBytes()
	numBits, numHashes := benchmarkComparisonParameters(len(values), benchmarkComparisonFalsePositiveRate)

	b.Run("seol", func(b *testing.B) {
		f := NewSeeded(numBits, numHashes, 1)
		b.ReportAllocs()

		var result bool
		for i := 0; b.Loop(); i++ {
			result = f.Add(values[i%len(values)])
		}
		benchmarkBoolSink = result
	})

	b.Run("bits-and-blooms", func(b *testing.B) {
		f := bitsbloom.New(uint(numBits), uint(numHashes))
		b.ReportAllocs()

		var result bool
		for i := 0; b.Loop(); i++ {
			result = f.TestAndAdd(values[i%len(values)])
		}
		benchmarkBoolSink = result
	})
}

func BenchmarkComparisonContainsBytes(b *testing.B) {
	values := benchmarkBytes()
	numBits, numHashes := benchmarkComparisonParameters(len(values), benchmarkComparisonFalsePositiveRate)

	b.Run("seol", func(b *testing.B) {
		f := NewSeeded(numBits, numHashes, 1)
		for _, value := range values {
			f.Add(value)
		}
		b.ReportAllocs()

		var result bool
		for i := 0; b.Loop(); i++ {
			result = f.Contains(values[i%len(values)])
		}
		benchmarkBoolSink = result
	})

	b.Run("bits-and-blooms", func(b *testing.B) {
		f := bitsbloom.New(uint(numBits), uint(numHashes))
		for _, value := range values {
			f.Add(value)
		}
		b.ReportAllocs()

		var result bool
		for i := 0; b.Loop(); i++ {
			result = f.Test(values[i%len(values)])
		}
		benchmarkBoolSink = result
	})
}

func benchmarkComparisonParameters(expectedItems int, falsePositiveRate float64) (numBits int, numHashes int) {
	numBits = wordCount(optimalBits(expectedItems, falsePositiveRate)) * WordBits
	numHashes = int(optimalHashes(numBits, expectedItems))
	return numBits, numHashes
}
