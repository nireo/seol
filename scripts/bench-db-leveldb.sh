#!/bin/sh

set -eu

COMMAND=${1:-run}
LABEL=${2:-}
PATTERN=${PATTERN:-^BenchmarkCompare}
BENCHTIME=${BENCHTIME:-500ms}
COUNT=${COUNT:-5}
MODULE_DIR=${MODULE_DIR:-benchmarks/dbcompare}
RESULTS_DIR=${RESULTS_DIR:-benchmark-results/dbcompare}
LAST_OUT=

usage() {
	echo "usage: $0 [run|baseline|compare|latest] [label]" >&2
	exit 1
}

require_benchstat() {
	if ! command -v benchstat >/dev/null 2>&1; then
		echo "benchstat not found. Install it with: go install golang.org/x/perf/cmd/benchstat@latest" >&2
		exit 1
	fi
}

timestamp() {
	date +%Y%m%d-%H%M%S
}

slug() {
	value=$1
	if [ -z "$value" ]; then
		echo run
		return
	fi
	printf '%s' "$value" | tr ' ' '-' | tr -cd '[:alnum:]-_.'
}

run_bench() {
	out=$1
	mkdir -p "$RESULTS_DIR"
	(
		cd "$MODULE_DIR"
		go test -run '^$' -bench "$PATTERN" -benchmem -benchtime "$BENCHTIME" -count "$COUNT" ./...
	) | tee "$out"
}

save_latest() {
	src=$1
	cp "$src" "$RESULTS_DIR/latest.txt"
}

save_baseline() {
	src=$1
	cp "$src" "$RESULTS_DIR/baseline.txt"
}

run_named() {
	kind=$1
	label=$(slug "$2")
	LAST_OUT="$RESULTS_DIR/$(timestamp)-$kind-$label.txt"
	run_bench "$LAST_OUT"
	save_latest "$LAST_OUT"
	printf 'saved benchmark output to %s\n' "$LAST_OUT" >&2
}

compare_files() {
	old=$1
	new=$2
	require_benchstat
	benchstat "$old" "$new"
}

compare_latest_two() {
	require_benchstat
	set -- "$RESULTS_DIR"/*.txt
	if [ "$1" = "$RESULTS_DIR/*.txt" ]; then
		echo "no saved benchmark runs found in $RESULTS_DIR" >&2
		exit 1
	fi
	count=$#
	if [ "$count" -lt 2 ]; then
		echo "need at least two saved benchmark runs in $RESULTS_DIR" >&2
		exit 1
	fi
	old=
	new=
	for file in "$@"; do
		case $(basename "$file") in
			baseline.txt|latest.txt)
				continue
				;;
		esac
		old=$new
		new=$file
	done
	if [ -z "$old" ] || [ -z "$new" ]; then
		echo "need at least two timestamped benchmark runs in $RESULTS_DIR" >&2
		exit 1
	fi
	printf 'comparing %s\n' "$old" >&2
	printf 'against   %s\n\n' "$new" >&2
	compare_files "$old" "$new"
}

case "$COMMAND" in
	run)
		run_named run "$LABEL"
		printf '%s\n' "$LAST_OUT"
		;;
	baseline)
		run_named baseline "${LABEL:-baseline}"
		save_baseline "$LAST_OUT"
		printf 'updated baseline: %s\n' "$RESULTS_DIR/baseline.txt" >&2
		;;
	compare)
		if [ ! -f "$RESULTS_DIR/baseline.txt" ]; then
			echo "missing $RESULTS_DIR/baseline.txt. Run '$0 baseline [label]' first." >&2
			exit 1
		fi
		run_named candidate "${LABEL:-candidate}"
		printf 'comparing %s\n' "$RESULTS_DIR/baseline.txt" >&2
		printf 'against   %s\n\n' "$LAST_OUT" >&2
		compare_files "$RESULTS_DIR/baseline.txt" "$LAST_OUT"
		;;
	latest)
		compare_latest_two
		;;
	*)
		usage
		;;
esac
