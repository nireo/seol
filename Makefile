.PHONY: test-all benchmark-all

BENCHTIME ?= 250ms

test:
	go test ./...

bench:
	go test -run=^$$ -bench=. -benchtime=$(BENCHTIME) ./...
