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

### Compaction

Environment:

- `go1.26.1`
- `darwin/arm64`
- `Apple M5`
- Command: `go test -run '^$' -bench '^BenchmarkDBCompact$' -benchmem -count 1`

Setup:

- `256` keys
- `16 B` keys
- `512 B` values kept inline with `ValueThreshold = 2 KiB`
- `32 KiB` memtable to force multi-SST fixtures before compaction
- `overwrite_live`: rewrite the full keyset `4` times so compaction collapses duplicate versions
- `tombstone_half`: write the full keyset once, delete half the keys, and keep rewriting the surviving half so compaction drops tombstones and stale versions

Results:

| Benchmark | Time | Input SSTs | Output SSTs | Input SST bytes | Output SST bytes | Reclaimed SST bytes | Live logical bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkDBCompact/overwrite_live` | `3.84 s/op` | `17` | `1` | `553,176` | `138,164` | `415,012` | `135,168` |
| `BenchmarkDBCompact/tombstone_half` | `2.90 s/op` | `11` | `1` | `350,032` | `69,120` | `280,912` | `67,584` |

Additional metrics from the same run:

- `overwrite_live`: `0.14 MB/s`, `0.2498 sst-output/input`, `540,672 written-logical-B/op`
- `tombstone_half`: `0.12 MB/s`, `0.1975 sst-output/input`, `341,248 written-logical-B/op`, `128 tombstones/op`

These are one-off local baseline numbers for the current full-compaction implementation.
