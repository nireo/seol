# Benchmarks

All run on a M5 Macbook Pro

### Skiplist

Initial implementation is really fast due to the arena. Requiring 0 allocations for all basic operations.

```
BenchmarkSkiplistPut-10        	3022083	       99.52 ns/op	      0 B/op	      0 allocs/op
BenchmarkSkiplistGet-10        	3138979	       95.35 ns/op	      0 B/op	      0 allocs/op
BenchmarkSkiplistSeek-10       	3345700	       89.39 ns/op	      0 B/op	      0 allocs/op
BenchmarkSkiplistIterate-10    	89793474	        3.018 ns/op	      0 B/op	      0 allocs/o
```
