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
BENCHTIME=500ms COUNT=1 ./scripts/bench-db-leveldb.sh run readme-refresh
```

| Benchmark | seol | goleveldb | Result |
| --- | ---: | ---: | --- |
| `PutSingle/durable` | `3.76 ms/op` | `3.58 ms/op` | `goleveldb` faster by `1.05x` |
| `PutSingle/eventual` | `901.6 ns/op` | `1.55 us/op` | `seol` faster by `1.72x` |
| `PutParallel/durable` | `787.2 us/op` | `678.0 us/op` | `goleveldb` faster by `1.16x` |
| `PutParallel/eventual` | `1.255 us/op` | `2.104 us/op` | `seol` faster by `1.68x` |
| `GetSingle` | `115.1 ns/op` | `535.0 ns/op` | `seol` faster by `4.65x` |
| `GetParallel` | `273.3 ns/op` | `856.6 ns/op` | `seol` faster by `3.13x` |
| `Open` | `2.20 ms/op` | `22.85 ms/op` | `seol` faster by `10.38x` |

### Allocation notes

- `seol` stayed at `0 allocs/op` for all write benchmarks in this run
- `goleveldb` allocated on every write case in this run
- `seol` read path was much lower allocation in `GetSingle` and roughly allocation-parity in `GetParallel`, while still being faster in both
- `Open` was much lighter for `seol` in both time and allocation count

### Takeaways

- The current `seol` implementation is strongest on reads, async-style writes, and database open time
- `goleveldb` still leads in the durable write cases, especially under parallel write load
- `seol` now leads clearly in both single-threaded and parallel `Get`, which is a big shift from the earlier read-path numbers
- For workloads that care about write-path allocations, `seol` remains in a strong position

These are point-in-time numbers from one run, not a statistical summary. For change-to-change comparisons, use the baseline and compare commands from the repo root.
