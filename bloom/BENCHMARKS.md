# Bloom Benchmarks

Comparison between this package and `github.com/bits-and-blooms/bloom/v3`.

## Environment

- `go1.26.1`
- `darwin/arm64`
- `Apple M5`
- Command: `go test ./bloom -run '^$' -bench '^BenchmarkComparison' -benchmem -count 5 -benchtime 500ms`

## Setup

- `65,536` unique keys
- Shared Bloom filter sizing derived for a `1%` false-positive target
- Insert benchmarks compare `Add*` against `TestAndAdd*` to match the "was it already present" return value
- Lookup benchmarks compare `Contains*` against `Test*`

## Results

Median of 5 runs. All benchmarks were `0 B/op` and `0 allocs/op` for both implementations.

| Benchmark | seol bloom | bits-and-blooms | seol faster |
| --- | ---: | ---: | ---: |
| `AddString` | `7.008 ns/op` | `31.19 ns/op` | `4.45x` |
| `ContainsString` | `5.965 ns/op` | `21.46 ns/op` | `3.60x` |
| `Add([]byte)` | `7.429 ns/op` | `32.59 ns/op` | `4.39x` |
| `Contains([]byte)` | `6.369 ns/op` | `22.17 ns/op` | `3.48x` |

The comparison benchmarks live in `bloom/bloom_compare_bench_test.go`.
