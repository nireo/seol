.PHONY: test-all benchmark-all

BENCHTIME ?= 100ms

test-all:
	go test ./...

benchmark-all:
	go test -run=^$$ -bench=. -benchtime=$(BENCHTIME) ./...
