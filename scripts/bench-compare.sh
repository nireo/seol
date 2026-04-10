#!/bin/sh

set -eu

RESULTS_DIR=${RESULTS_DIR:-benchmark-results/db}

if ! command -v benchstat >/dev/null 2>&1; then
	echo "benchstat not found. Install it with: go install golang.org/x/perf/cmd/benchstat@latest" >&2
	exit 1
fi

if [ "$#" -eq 0 ]; then
	if [ ! -f "$RESULTS_DIR/baseline.txt" ] || [ ! -f "$RESULTS_DIR/latest.txt" ]; then
		echo "missing $RESULTS_DIR/baseline.txt or $RESULTS_DIR/latest.txt" >&2
		exit 1
	fi
	exec benchstat "$RESULTS_DIR/baseline.txt" "$RESULTS_DIR/latest.txt"
fi

if [ "$#" -eq 2 ]; then
	exec benchstat "$1" "$2"
fi

echo "usage: $0 [old.txt new.txt]" >&2
exit 1
