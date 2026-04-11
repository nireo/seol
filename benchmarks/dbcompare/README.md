# DB Comparison Benchmarks

This benchmark module compares `github.com/nireo/seol` against `github.com/syndtr/goleveldb` without adding `goleveldb` to the root module.

## Workloads

- `BenchmarkComparePutSingle`: single-threaded writes
- `BenchmarkComparePutParallel`: multi-threaded writes
- `BenchmarkCompareGetSingle`: single-threaded reads from a reopened on-disk fixture
- `BenchmarkCompareGetParallel`: multi-threaded reads from a reopened on-disk fixture
- `BenchmarkCompareOpen`: open an existing on-disk fixture

## Write Profiles

- `durable`: `seol` syncs each write and `goleveldb` uses `Sync: true`
- `eventual`: `seol` uses async writes with a `5ms` sync window and `goleveldb` uses `Sync: false`

`goleveldb` is opened with a `32 MiB` write buffer and compression disabled so the write path is closer to the current `seol` configuration.

## Run

From this directory:

```sh
go test -run '^$' -bench '^BenchmarkCompare' -benchmem -count 3 -benchtime 500ms ./...
```

From the repo root:

```sh
./scripts/bench-db-leveldb.sh run local
```

## Latest Results

Measured on `darwin/arm64`, `Apple M5`, `go1.26.1` with:

```sh
BENCHTIME=250ms COUNT=1 ./scripts/bench-db-leveldb.sh run readme
```

| Benchmark | seol | goleveldb | Result |
| --- | ---: | ---: | --- |
| `PutSingle/durable` | `3.90 ms/op` | `3.95 ms/op` | Roughly tied, slight edge to `seol` |
| `PutSingle/eventual` | `900.8 ns/op` | `1.44 us/op` | `seol` faster by `1.60x` |
| `PutParallel/durable` | `817.8 us/op` | `740.1 us/op` | `goleveldb` faster by `1.10x` |
| `PutParallel/eventual` | `1.315 us/op` | `2.009 us/op` | `seol` faster by `1.53x` |
| `GetSingle` | `472.9 ns/op` | `568.7 ns/op` | `seol` faster by `1.20x` |
| `GetParallel` | `2.378 us/op` | `877.0 ns/op` | `goleveldb` faster by `2.71x` |
| `Open` | `2.12 ms/op` | `22.96 ms/op` | `seol` faster by `10.80x` |

### Allocation notes

- `seol` stayed at `0 allocs/op` for all write benchmarks in this run
- `goleveldb` allocated on every write case in this run
- `seol` read path was lower allocation in both single-threaded and parallel `Get`
- `Open` was much lighter for `seol` in both time and allocation count

### Takeaways

- The current `seol` implementation is strongest on single-threaded reads, async-style writes, and database open time
- `goleveldb` currently wins on the parallel durable write case by a small margin
- `goleveldb` also wins the parallel read benchmark by a larger margin, which is the clearest area for follow-up optimization in `seol`
- For workloads that care about write-path allocations, `seol` is in a strong position already

These are point-in-time numbers from one run, not a statistical summary. For change-to-change comparisons, use the baseline and compare commands from the repo root.
