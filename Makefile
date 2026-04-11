.PHONY: test bench bench-db bench-db-baseline bench-db-compare bench-db-latest bench-db-leveldb bench-db-leveldb-baseline bench-db-leveldb-compare bench-db-leveldb-latest bench-compare

BENCH ?= .
BENCHTIME ?= 250ms
COUNT ?= 1
NAME ?=

test:
	go test ./...

bench:
	go test -run=^$$ -bench=$(BENCH) -benchmem -benchtime=$(BENCHTIME) -count=$(COUNT) ./...

bench-db:
	PATTERN='^BenchmarkDB' BENCHTIME=$(BENCHTIME) COUNT=$(COUNT) ./scripts/bench-db.sh run "$(NAME)"

bench-db-baseline:
	PATTERN='^BenchmarkDB' BENCHTIME=$(BENCHTIME) COUNT=$(COUNT) ./scripts/bench-db.sh baseline "$(NAME)"

bench-db-compare:
	PATTERN='^BenchmarkDB' BENCHTIME=$(BENCHTIME) COUNT=$(COUNT) ./scripts/bench-db.sh compare "$(NAME)"

bench-db-latest:
	./scripts/bench-db.sh latest

bench-db-leveldb:
	PATTERN='^BenchmarkCompare' BENCHTIME=$(BENCHTIME) COUNT=$(COUNT) ./scripts/bench-db-leveldb.sh run "$(NAME)"

bench-db-leveldb-baseline:
	PATTERN='^BenchmarkCompare' BENCHTIME=$(BENCHTIME) COUNT=$(COUNT) ./scripts/bench-db-leveldb.sh baseline "$(NAME)"

bench-db-leveldb-compare:
	PATTERN='^BenchmarkCompare' BENCHTIME=$(BENCHTIME) COUNT=$(COUNT) ./scripts/bench-db-leveldb.sh compare "$(NAME)"

bench-db-leveldb-latest:
	./scripts/bench-db-leveldb.sh latest

bench-compare:
	./scripts/bench-compare.sh $(OLD) $(NEW)
