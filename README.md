# seol

A fast embeddable Go LSMT key-value store. Uses many concepts from the WiscKey paper. Additionally the project aims to create very high performance Go data structures that can be used in any database.

## Benchmarks

### Bloom Filter Comparison

Current comparison results are in `bloom/BENCHMARKS.md`.

### DB Engine Comparison

`benchmarks/dbcompare` contains a separate benchmark module for comparing `seol` against `goleveldb` over single-threaded and multi-threaded `Put`, `Get`, and `Open` workloads.

### Local Baselines

Current local baseline results for core structures and the compaction path are in `BENCHMARKS.md`.
