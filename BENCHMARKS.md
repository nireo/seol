# Benchmarks

All run on a M5 Macbook Pro

### Bit vector

Naive simple implementation:

```
goos: darwin
goarch: arm64
pkg: github.com/nireo/seol
cpu: Apple M5
BenchmarkBitvecCheck-10         57083636                 1.870 ns/op
BenchmarkBitvecSet-10           71359548                 1.682 ns/op
BenchmarkBitvecClear-10           480055               241.4 ns/op
BenchmarkBitvecUnion-10           309600               426.7 ns/op
BenchmarkBitvecIntersect-10       275911               429.1 ns/op
BenchmarkBitvecEq-10              503548               232.5 ns/op
BenchmarkBitvecClone-10           160548               734.8 ns/op
PASS
```

Loop unrolling and inline functions:

```
goos: darwin
goarch: arm64
pkg: github.com/nireo/seol
cpu: Apple M5
BenchmarkBitvecCheck-10         52667174                 2.008 ns/op
BenchmarkBitvecSet-10           69792680                 1.700 ns/op
BenchmarkBitvecClear-10          3299614                36.80 ns/op
BenchmarkBitvecUnion-10           571846               239.3 ns/op
BenchmarkBitvecIntersect-10       487994               249.8 ns/op
BenchmarkBitvecEq-10              668631               178.8 ns/op
BenchmarkBitvecClone-10           158654               736.8 ns/op
PASS
```

### Skiplist

Initial implementation is really fast due to the arena. Requiring 0 allocations for all basic operations.

```
BenchmarkSkiplistPut-10        	3022083	       99.52 ns/op	      0 B/op	      0 allocs/op
BenchmarkSkiplistGet-10        	3138979	       95.35 ns/op	      0 B/op	      0 allocs/op
BenchmarkSkiplistSeek-10       	3345700	       89.39 ns/op	      0 B/op	      0 allocs/op
BenchmarkSkiplistIterate-10    	89793474	        3.018 ns/op	      0 B/op	      0 allocs/o
```
