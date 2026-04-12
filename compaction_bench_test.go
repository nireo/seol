package seol

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const (
	benchmarkCompactionEntryCount    = 1 << 8
	benchmarkCompactionValueSize     = 512
	benchmarkCompactionMemtableBytes = 32 << 10
	benchmarkCompactionRounds        = 4
)

type benchmarkCompactionProfile struct {
	name  string
	build func(b *testing.B, db *DB, keys [][]byte) benchmarkCompactionFixtureStats
}

type benchmarkCompactionFixtureStats struct {
	liveLogicalBytes    int64
	writtenLogicalBytes int64
	liveKeys            int
	tombstones          int
}

var benchmarkCompactionProfiles = []benchmarkCompactionProfile{
	{name: "overwrite_live", build: benchmarkBuildOverwriteCompactionFixture},
	{name: "tombstone_half", build: benchmarkBuildTombstoneCompactionFixture},
}

func BenchmarkDBCompact(b *testing.B) {
	keys, _, _ := benchmarkDBRecords(benchmarkCompactionEntryCount, benchmarkDBKeySize, benchmarkCompactionValueSize)
	for _, profile := range benchmarkCompactionProfiles {
		b.Run(profile.name, func(b *testing.B) {
			root := b.TempDir()
			fixtureDir := filepath.Join(root, "fixture")
			fixtureStats, fixtureUsage, inputSSTCount := benchmarkPrepareCompactionFixture(b, fixtureDir, keys, profile)

			var totalAfter benchmarkFileUsage
			var totalOutputSSTCount int64

			b.ReportAllocs()
			b.SetBytes(fixtureUsage.sst)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				iterDir := filepath.Join(root, fmt.Sprintf("compact-%s-%06d", profile.name, i))
				if err := os.MkdirAll(iterDir, 0o755); err != nil {
					b.Fatalf("mkdir iteration dir: %v", err)
				}
				if err := benchmarkCopyDir(fixtureDir, iterDir); err != nil {
					b.Fatalf("copy fixture dir: %v", err)
				}

				b.StartTimer()
				db := benchmarkOpenExistingDB(b, iterDir, benchmarkCompactionOptions())
				compacted, err := db.Compact()
				b.StopTimer()
				if err != nil {
					b.Fatalf("compact db: %v", err)
				}
				if err := compacted.Close(); err != nil {
					b.Fatalf("close compacted db: %v", err)
				}

				afterUsage, err := benchmarkDirUsage(iterDir)
				if err != nil {
					b.Fatalf("measure dir usage: %v", err)
				}
				totalAfter.total += afterUsage.total
				totalAfter.wal += afterUsage.wal
				totalAfter.sst += afterUsage.sst
				totalAfter.vlog += afterUsage.vlog
				totalOutputSSTCount += int64(benchmarkCountFilesWithExt(b, iterDir, ".sst"))

				if err := os.RemoveAll(iterDir); err != nil {
					b.Fatalf("remove iteration dir: %v", err)
				}
			}

			afterTotal := float64(totalAfter.total) / float64(b.N)
			afterSST := float64(totalAfter.sst) / float64(b.N)
			afterOutputSSTCount := float64(totalOutputSSTCount) / float64(b.N)
			b.ReportMetric(float64(fixtureStats.liveLogicalBytes), "live-logical-B/op")
			b.ReportMetric(float64(fixtureStats.writtenLogicalBytes), "written-logical-B/op")
			b.ReportMetric(float64(fixtureStats.liveKeys), "live-keys/op")
			b.ReportMetric(float64(fixtureStats.tombstones), "tombstones/op")
			b.ReportMetric(float64(inputSSTCount), "input-sst/op")
			b.ReportMetric(afterOutputSSTCount, "output-sst/op")
			b.ReportMetric(float64(fixtureUsage.total), "input-total-B/op")
			b.ReportMetric(afterTotal, "output-total-B/op")
			b.ReportMetric(float64(fixtureUsage.total)-afterTotal, "reclaimed-total-B/op")
			b.ReportMetric(float64(fixtureUsage.sst), "input-sst-B/op")
			b.ReportMetric(afterSST, "output-sst-B/op")
			b.ReportMetric(float64(fixtureUsage.sst)-afterSST, "reclaimed-sst-B/op")
			b.ReportMetric(afterSST/float64(fixtureUsage.sst), "sst-output/input")
		})
	}
}

func benchmarkPrepareCompactionFixture(b *testing.B, dir string, keys [][]byte, profile benchmarkCompactionProfile) (benchmarkCompactionFixtureStats, benchmarkFileUsage, int) {
	b.Helper()

	db := benchmarkResetDB(b, dir, benchmarkCompactionOptions())
	stats := profile.build(b, db, keys)
	if err := db.Close(); err != nil {
		b.Fatalf("close fixture db: %v", err)
	}

	usage, err := benchmarkDirUsage(dir)
	if err != nil {
		b.Fatalf("measure fixture dir usage: %v", err)
	}
	return stats, usage, benchmarkCountFilesWithExt(b, dir, ".sst")
}

func benchmarkCompactionOptions() Options {
	return Options{
		MemtableMaxBytes: benchmarkCompactionMemtableBytes,
		ValueThreshold:   2 << 10,
	}
}

func benchmarkBuildOverwriteCompactionFixture(b *testing.B, db *DB, keys [][]byte) benchmarkCompactionFixtureStats {
	b.Helper()

	roundValues := benchmarkDBRoundValues(len(keys), benchmarkCompactionValueSize, benchmarkCompactionRounds)
	for round := range roundValues {
		for i := range keys {
			if err := db.Put(keys[i], roundValues[round][i]); err != nil {
				b.Fatalf("put overwrite round %d: %v", round, err)
			}
		}
	}

	logicalPerKey := int64(len(keys[0]) + benchmarkCompactionValueSize)
	return benchmarkCompactionFixtureStats{
		liveLogicalBytes:    logicalPerKey * int64(len(keys)),
		writtenLogicalBytes: logicalPerKey * int64(len(keys)) * benchmarkCompactionRounds,
		liveKeys:            len(keys),
	}
}

func benchmarkBuildTombstoneCompactionFixture(b *testing.B, db *DB, keys [][]byte) benchmarkCompactionFixtureStats {
	b.Helper()

	roundValues := benchmarkDBRoundValues(len(keys), benchmarkCompactionValueSize, benchmarkCompactionRounds)
	for i := range keys {
		if err := db.Put(keys[i], roundValues[0][i]); err != nil {
			b.Fatalf("put initial value %d: %v", i, err)
		}
	}
	for round := 1; round < benchmarkCompactionRounds; round++ {
		for i := range keys {
			if i%2 == 0 {
				if round == 1 {
					if err := db.Delete(keys[i]); err != nil {
						b.Fatalf("delete key %d: %v", i, err)
					}
				}
				continue
			}
			if err := db.Put(keys[i], roundValues[round][i]); err != nil {
				b.Fatalf("put live key round %d index %d: %v", round, i, err)
			}
		}
	}

	liveKeys := len(keys) / 2
	logicalPerKey := int64(len(keys[0]) + benchmarkCompactionValueSize)
	tombstoneBytes := int64(len(keys[0]) + len(tombstoneValue))
	writtenLogicalBytes := logicalPerKey*int64(len(keys)) + tombstoneBytes*int64(len(keys)-liveKeys) + logicalPerKey*int64(liveKeys*(benchmarkCompactionRounds-1))
	return benchmarkCompactionFixtureStats{
		liveLogicalBytes:    logicalPerKey * int64(liveKeys),
		writtenLogicalBytes: writtenLogicalBytes,
		liveKeys:            liveKeys,
		tombstones:          len(keys) - liveKeys,
	}
}

func benchmarkCountFilesWithExt(b *testing.B, dir, ext string) int {
	b.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Fatalf("ReadDir: %v", err)
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ext {
			continue
		}
		count++
	}
	return count
}
