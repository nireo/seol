package seol

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nireo/seol/sstable"
)

const (
	benchmarkDBEntryCount    = 1 << 9
	benchmarkDBKeySize       = 16
	benchmarkDBValueSize     = 128
	benchmarkDBMemtableBytes = 32 << 20
	benchmarkDBFlushBytes    = 4 << 10
	benchmarkDBSSTBytes      = 256 << 10
	benchmarkDBOverwriteRuns = 4
)

type benchmarkDBValueProfile struct {
	name           string
	valueSize      int
	valueThreshold int
}

type benchmarkDBThresholdCandidate struct {
	name      string
	threshold int
}

var benchmarkDBBytesSink []byte

var benchmarkDBValueProfiles = []benchmarkDBValueProfile{
	{name: "inline_128_t128", valueSize: 128, valueThreshold: 128},
	{name: "vlog_129_t128", valueSize: 129, valueThreshold: 128},
	{name: "vlog_256_t128", valueSize: 256, valueThreshold: 128},
	{name: "vlog_1k_t128", valueSize: 1 << 10, valueThreshold: 128},
	{name: "vlog_4k_t128", valueSize: 4 << 10, valueThreshold: 128},
}

var benchmarkDBThresholdCandidates = []benchmarkDBThresholdCandidate{
	{name: "t128", threshold: 128},
	{name: "t256", threshold: 256},
	{name: "t512", threshold: 512},
	{name: "t1k", threshold: 1 << 10},
	{name: "t2k", threshold: 2 << 10},
}

var benchmarkDBThresholdSweepSizes = []struct {
	name      string
	valueSize int
}{
	{name: "v256", valueSize: 256},
	{name: "v1k", valueSize: 1 << 10},
	{name: "v2k", valueSize: 2 << 10},
	{name: "v4k", valueSize: 4 << 10},
}

func BenchmarkDBPut(b *testing.B) {
	keys, values, _ := benchmarkDBRecords(1<<12, benchmarkDBKeySize, benchmarkDBValueSize)
	cases := []struct {
		name         string
		syncInterval time.Duration
		asyncWrites  bool
	}{
		{name: "sync_immediate", syncInterval: 0},
		{name: "sync_batched_5ms", syncInterval: 5 * time.Millisecond},
		{name: "async_batched_5ms", syncInterval: 5 * time.Millisecond, asyncWrites: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			opts := Options{MemtableMaxBytes: benchmarkDBMemtableBytes, WALSyncInterval: tc.syncInterval, AsyncWrites: tc.asyncWrites}
			db := benchmarkResetDB(b, dir, opts)
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()

			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				if i > 0 && i%len(keys) == 0 {
					b.StopTimer()
					if err := db.Close(); err != nil {
						b.Fatalf("close db: %v", err)
					}
					db = benchmarkResetDB(b, dir, opts)
					b.StartTimer()
				}

				if err := db.Put(keys[i%len(keys)], values[i%len(values)]); err != nil {
					b.Fatalf("put: %v", err)
				}
			}
		})
	}
}

func BenchmarkDBPutValueProfiles(b *testing.B) {
	cases := []struct {
		name         string
		syncInterval time.Duration
		asyncWrites  bool
	}{
		{name: "sync_immediate", syncInterval: 0},
		{name: "sync_batched_5ms", syncInterval: 5 * time.Millisecond},
		{name: "async_batched_5ms", syncInterval: 5 * time.Millisecond, asyncWrites: true},
	}

	for _, profile := range benchmarkDBValueProfiles {
		keys, values, _ := benchmarkDBRecords(1<<12, benchmarkDBKeySize, profile.valueSize)
		for _, tc := range cases {
			b.Run(profile.name+"/"+tc.name, func(b *testing.B) {
				dir := b.TempDir()
				opts := Options{
					MemtableMaxBytes: benchmarkDBMemtableBytes,
					WALSyncInterval:  tc.syncInterval,
					ValueThreshold:   profile.valueThreshold,
					AsyncWrites:      tc.asyncWrites,
				}
				db := benchmarkResetDB(b, dir, opts)
				defer func() {
					if db != nil {
						_ = db.Close()
					}
				}()

				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					if i > 0 && i%len(keys) == 0 {
						b.StopTimer()
						if err := db.Close(); err != nil {
							b.Fatalf("close db: %v", err)
						}
						db = benchmarkResetDB(b, dir, opts)
						b.StartTimer()
					}

					if err := db.Put(keys[i%len(keys)], values[i%len(values)]); err != nil {
						b.Fatalf("put: %v", err)
					}
				}
			})
		}
	}
}

func BenchmarkDBPutParallel(b *testing.B) {
	keys, values, _ := benchmarkDBRecords(1<<12, benchmarkDBKeySize, benchmarkDBValueSize)
	cases := []struct {
		name         string
		syncInterval time.Duration
		asyncWrites  bool
	}{
		{name: "sync_immediate", syncInterval: 0},
		{name: "sync_batched_5ms", syncInterval: 5 * time.Millisecond},
		{name: "async_batched_5ms", syncInterval: 5 * time.Millisecond, asyncWrites: true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			opts := Options{MemtableMaxBytes: benchmarkDBMemtableBytes, WALSyncInterval: tc.syncInterval, AsyncWrites: tc.asyncWrites}
			db := benchmarkResetDB(b, dir, opts)
			defer func() { _ = db.Close() }()

			var idx atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := int(idx.Add(1) - 1)
					if err := db.Put(keys[i%len(keys)], values[i%len(values)]); err != nil {
						b.Errorf("put: %v", err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkDBGetMemtableHit(b *testing.B) {
	keys, values, _ := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, benchmarkDBValueSize)
	dir := b.TempDir()
	db := benchmarkResetDB(b, dir, Options{MemtableMaxBytes: benchmarkDBMemtableBytes})
	defer func() { _ = db.Close() }()

	for i := range keys {
		if err := db.Put(keys[i], values[i]); err != nil {
			b.Fatalf("put fixture: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		value, err := db.Get(keys[i%len(keys)])
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		benchmarkDBBytesSink = value
	}
}

func BenchmarkDBGetMemtableHitValueProfiles(b *testing.B) {
	for _, profile := range benchmarkDBValueProfiles {
		keys, values, _ := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, profile.valueSize)
		b.Run(profile.name, func(b *testing.B) {
			dir := b.TempDir()
			db := benchmarkResetDB(b, dir, Options{MemtableMaxBytes: benchmarkDBMemtableBytes, ValueThreshold: profile.valueThreshold})
			defer func() { _ = db.Close() }()

			for i := range keys {
				if err := db.Put(keys[i], values[i]); err != nil {
					b.Fatalf("put fixture: %v", err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				value, err := db.Get(keys[i%len(keys)])
				if err != nil {
					b.Fatalf("get: %v", err)
				}
				benchmarkDBBytesSink = value
			}
		})
	}
}

func BenchmarkDBGetSSTHit(b *testing.B) {
	keys, values, _ := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, benchmarkDBValueSize)
	dir := b.TempDir()
	db := benchmarkResetDB(b, dir, Options{MemtableMaxBytes: benchmarkDBFlushBytes})
	for i := range keys {
		if err := db.Put(keys[i], values[i]); err != nil {
			b.Fatalf("put fixture: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatalf("close fixture db: %v", err)
	}

	db = benchmarkOpenExistingDB(b, dir, Options{})
	defer func() { _ = db.Close() }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		value, err := db.Get(keys[i%len(keys)])
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		benchmarkDBBytesSink = value
	}
}

func BenchmarkDBGetSSTHitValueProfiles(b *testing.B) {
	for _, profile := range benchmarkDBValueProfiles {
		keys, values, _ := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, profile.valueSize)
		b.Run(profile.name, func(b *testing.B) {
			dir := b.TempDir()
			db := benchmarkResetDB(b, dir, Options{MemtableMaxBytes: benchmarkDBSSTBytes, ValueThreshold: profile.valueThreshold})
			for i := range keys {
				if err := db.Put(keys[i], values[i]); err != nil {
					b.Fatalf("put fixture: %v", err)
				}
			}
			if err := db.Close(); err != nil {
				b.Fatalf("close fixture db: %v", err)
			}

			db = benchmarkOpenExistingDB(b, dir, Options{ValueThreshold: profile.valueThreshold})
			defer func() { _ = db.Close() }()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				value, err := db.Get(keys[i%len(keys)])
				if err != nil {
					b.Fatalf("get: %v", err)
				}
				benchmarkDBBytesSink = value
			}
		})
	}
}

func BenchmarkDBOpenReplayWAL(b *testing.B) {
	root := b.TempDir()
	fixtureDir := filepath.Join(root, "fixture")
	if err := os.MkdirAll(fixtureDir, 0o755); err != nil {
		b.Fatalf("mkdir fixture dir: %v", err)
	}

	keys, values, _ := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, benchmarkDBValueSize)
	fixture := benchmarkOpenExistingDB(b, fixtureDir, Options{MemtableMaxBytes: benchmarkDBMemtableBytes})
	for i := range keys {
		if err := fixture.Put(keys[i], values[i]); err != nil {
			b.Fatalf("put fixture: %v", err)
		}
	}
	benchmarkStopDBWithoutFlush(b, fixture)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		iterDir := filepath.Join(root, fmt.Sprintf("wal-replay-%06d", i))
		if err := os.MkdirAll(iterDir, 0o755); err != nil {
			b.Fatalf("mkdir iteration dir: %v", err)
		}
		if err := benchmarkCopyDir(fixtureDir, iterDir); err != nil {
			b.Fatalf("copy fixture dir: %v", err)
		}

		b.StartTimer()
		db, err := Open(iterDir)
		b.StopTimer()
		if err != nil {
			b.Fatalf("open replay fixture: %v", err)
		}
		if err := db.Close(); err != nil {
			b.Fatalf("close replay db: %v", err)
		}
		if err := os.RemoveAll(iterDir); err != nil {
			b.Fatalf("remove iteration dir: %v", err)
		}
	}
}

func BenchmarkDBOpenReplayWALValueProfiles(b *testing.B) {
	for _, profile := range benchmarkDBValueProfiles {
		keys, values, _ := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, profile.valueSize)
		b.Run(profile.name, func(b *testing.B) {
			root := b.TempDir()
			fixtureDir := filepath.Join(root, "fixture")
			if err := os.MkdirAll(fixtureDir, 0o755); err != nil {
				b.Fatalf("mkdir fixture dir: %v", err)
			}

			fixture := benchmarkOpenExistingDB(b, fixtureDir, Options{MemtableMaxBytes: benchmarkDBMemtableBytes, ValueThreshold: profile.valueThreshold})
			for i := range keys {
				if err := fixture.Put(keys[i], values[i]); err != nil {
					b.Fatalf("put fixture: %v", err)
				}
			}
			benchmarkStopDBWithoutFlush(b, fixture)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				iterDir := filepath.Join(root, fmt.Sprintf("wal-replay-%s-%06d", profile.name, i))
				if err := os.MkdirAll(iterDir, 0o755); err != nil {
					b.Fatalf("mkdir iteration dir: %v", err)
				}
				if err := benchmarkCopyDir(fixtureDir, iterDir); err != nil {
					b.Fatalf("copy fixture dir: %v", err)
				}

				b.StartTimer()
				db, err := OpenWithOptions(iterDir, Options{ValueThreshold: profile.valueThreshold})
				b.StopTimer()
				if err != nil {
					b.Fatalf("open replay fixture: %v", err)
				}
				if err := db.Close(); err != nil {
					b.Fatalf("close replay db: %v", err)
				}
				if err := os.RemoveAll(iterDir); err != nil {
					b.Fatalf("remove iteration dir: %v", err)
				}
			}
		})
	}
}

func BenchmarkDBWriteFootprint(b *testing.B) {
	keys, values, logicalBytes := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, benchmarkDBValueSize)
	root := b.TempDir()

	var totalUsage benchmarkFileUsage
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		iterDir := filepath.Join(root, fmt.Sprintf("footprint-%06d", i))
		if err := os.MkdirAll(iterDir, 0o755); err != nil {
			b.Fatalf("mkdir iteration dir: %v", err)
		}

		b.StartTimer()
		db := benchmarkOpenExistingDB(b, iterDir, Options{MemtableMaxBytes: benchmarkDBFlushBytes})
		for j := range keys {
			if err := db.Put(keys[j], values[j]); err != nil {
				b.Fatalf("put: %v", err)
			}
		}
		if err := db.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
		b.StopTimer()

		usage, err := benchmarkDirUsage(iterDir)
		if err != nil {
			b.Fatalf("measure dir usage: %v", err)
		}
		totalUsage.total += usage.total
		totalUsage.wal += usage.wal
		totalUsage.sst += usage.sst
		totalUsage.vlog += usage.vlog

		if err := os.RemoveAll(iterDir); err != nil {
			b.Fatalf("remove iteration dir: %v", err)
		}
	}

	b.ReportMetric(float64(logicalBytes), "logical-B/op")
	b.ReportMetric(float64(totalUsage.total)/float64(b.N), "disk-B/op")
	b.ReportMetric(float64(totalUsage.sst)/float64(b.N), "sst-B/op")
	b.ReportMetric(float64(totalUsage.wal)/float64(b.N), "wal-B/op")
	b.ReportMetric(float64(totalUsage.vlog)/float64(b.N), "vlog-B/op")
	b.ReportMetric(float64(totalUsage.total)/float64(b.N)/float64(logicalBytes), "disk/logical")
}

func BenchmarkDBWriteFootprintValueProfiles(b *testing.B) {
	for _, profile := range benchmarkDBValueProfiles {
		keys, values, logicalBytes := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, profile.valueSize)
		b.Run(profile.name, func(b *testing.B) {
			root := b.TempDir()
			var totalUsage benchmarkFileUsage

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				iterDir := filepath.Join(root, fmt.Sprintf("footprint-%s-%06d", profile.name, i))
				if err := os.MkdirAll(iterDir, 0o755); err != nil {
					b.Fatalf("mkdir iteration dir: %v", err)
				}

				b.StartTimer()
				db := benchmarkOpenExistingDB(b, iterDir, Options{MemtableMaxBytes: benchmarkDBSSTBytes, ValueThreshold: profile.valueThreshold})
				for j := range keys {
					if err := db.Put(keys[j], values[j]); err != nil {
						b.Fatalf("put: %v", err)
					}
				}
				if err := db.Close(); err != nil {
					b.Fatalf("close db: %v", err)
				}
				b.StopTimer()

				usage, err := benchmarkDirUsage(iterDir)
				if err != nil {
					b.Fatalf("measure dir usage: %v", err)
				}
				totalUsage.total += usage.total
				totalUsage.wal += usage.wal
				totalUsage.sst += usage.sst
				totalUsage.vlog += usage.vlog

				if err := os.RemoveAll(iterDir); err != nil {
					b.Fatalf("remove iteration dir: %v", err)
				}
			}

			b.ReportMetric(float64(logicalBytes), "logical-B/op")
			b.ReportMetric(float64(totalUsage.total)/float64(b.N), "disk-B/op")
			b.ReportMetric(float64(totalUsage.sst)/float64(b.N), "sst-B/op")
			b.ReportMetric(float64(totalUsage.wal)/float64(b.N), "wal-B/op")
			b.ReportMetric(float64(totalUsage.vlog)/float64(b.N), "vlog-B/op")
			b.ReportMetric(float64(totalUsage.total)/float64(b.N)/float64(logicalBytes), "disk/logical")
		})
	}
}

func BenchmarkDBOverwriteFootprintValueProfiles(b *testing.B) {
	for _, profile := range benchmarkDBValueProfiles {
		keys, _, liveLogicalBytes := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, profile.valueSize)
		roundValues := benchmarkDBRoundValues(len(keys), profile.valueSize, benchmarkDBOverwriteRuns)
		writtenLogicalBytes := liveLogicalBytes * benchmarkDBOverwriteRuns

		b.Run(profile.name, func(b *testing.B) {
			root := b.TempDir()
			var totalUsage benchmarkFileUsage

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				iterDir := filepath.Join(root, fmt.Sprintf("overwrite-footprint-%s-%06d", profile.name, i))
				if err := os.MkdirAll(iterDir, 0o755); err != nil {
					b.Fatalf("mkdir iteration dir: %v", err)
				}

				b.StartTimer()
				db := benchmarkOpenExistingDB(b, iterDir, Options{MemtableMaxBytes: benchmarkDBMemtableBytes, ValueThreshold: profile.valueThreshold})
				for round := range roundValues {
					for j := range keys {
						if err := db.Put(keys[j], roundValues[round][j]); err != nil {
							b.Fatalf("put overwrite round %d: %v", round, err)
						}
					}
				}
				if err := db.Close(); err != nil {
					b.Fatalf("close db: %v", err)
				}
				b.StopTimer()

				usage, err := benchmarkDirUsage(iterDir)
				if err != nil {
					b.Fatalf("measure dir usage: %v", err)
				}
				totalUsage.total += usage.total
				totalUsage.wal += usage.wal
				totalUsage.sst += usage.sst
				totalUsage.vlog += usage.vlog

				if err := os.RemoveAll(iterDir); err != nil {
					b.Fatalf("remove iteration dir: %v", err)
				}
			}

			b.ReportMetric(float64(liveLogicalBytes), "live-logical-B/op")
			b.ReportMetric(float64(writtenLogicalBytes), "written-logical-B/op")
			b.ReportMetric(float64(totalUsage.total)/float64(b.N), "disk-B/op")
			b.ReportMetric(float64(totalUsage.sst)/float64(b.N), "sst-B/op")
			b.ReportMetric(float64(totalUsage.wal)/float64(b.N), "wal-B/op")
			b.ReportMetric(float64(totalUsage.vlog)/float64(b.N), "vlog-B/op")
			b.ReportMetric(float64(totalUsage.total)/float64(b.N)/float64(liveLogicalBytes), "disk/live")
			b.ReportMetric(float64(totalUsage.total)/float64(b.N)/float64(writtenLogicalBytes), "disk/written")
		})
	}
}

func BenchmarkDBThresholdSweepGetSSTHit(b *testing.B) {
	for _, size := range benchmarkDBThresholdSweepSizes {
		keys, values, _ := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, size.valueSize)
		for _, candidate := range benchmarkDBThresholdCandidates {
			b.Run(size.name+"/"+candidate.name, func(b *testing.B) {
				dir := b.TempDir()
				db := benchmarkResetDB(b, dir, Options{MemtableMaxBytes: benchmarkDBSSTBytes, ValueThreshold: candidate.threshold})
				for i := range keys {
					if err := db.Put(keys[i], values[i]); err != nil {
						b.Fatalf("put fixture: %v", err)
					}
				}
				if err := db.Close(); err != nil {
					b.Fatalf("close fixture db: %v", err)
				}

				db = benchmarkOpenExistingDB(b, dir, Options{ValueThreshold: candidate.threshold})
				defer func() { _ = db.Close() }()

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; b.Loop(); i++ {
					value, err := db.Get(keys[i%len(keys)])
					if err != nil {
						b.Fatalf("get: %v", err)
					}
					benchmarkDBBytesSink = value
				}
			})
		}
	}
}

func BenchmarkDBThresholdSweepWriteFootprint(b *testing.B) {
	for _, size := range benchmarkDBThresholdSweepSizes {
		keys, values, logicalBytes := benchmarkDBRecords(benchmarkDBEntryCount, benchmarkDBKeySize, size.valueSize)
		for _, candidate := range benchmarkDBThresholdCandidates {
			b.Run(size.name+"/"+candidate.name, func(b *testing.B) {
				root := b.TempDir()
				var totalUsage benchmarkFileUsage

				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					iterDir := filepath.Join(root, fmt.Sprintf("threshold-footprint-%s-%s-%06d", size.name, candidate.name, i))
					if err := os.MkdirAll(iterDir, 0o755); err != nil {
						b.Fatalf("mkdir iteration dir: %v", err)
					}

					b.StartTimer()
					db := benchmarkOpenExistingDB(b, iterDir, Options{MemtableMaxBytes: benchmarkDBSSTBytes, ValueThreshold: candidate.threshold})
					for j := range keys {
						if err := db.Put(keys[j], values[j]); err != nil {
							b.Fatalf("put: %v", err)
						}
					}
					if err := db.Close(); err != nil {
						b.Fatalf("close db: %v", err)
					}
					b.StopTimer()

					usage, err := benchmarkDirUsage(iterDir)
					if err != nil {
						b.Fatalf("measure dir usage: %v", err)
					}
					totalUsage.total += usage.total
					totalUsage.wal += usage.wal
					totalUsage.sst += usage.sst
					totalUsage.vlog += usage.vlog

					if err := os.RemoveAll(iterDir); err != nil {
						b.Fatalf("remove iteration dir: %v", err)
					}
				}

				b.ReportMetric(float64(logicalBytes), "logical-B/op")
				b.ReportMetric(float64(totalUsage.total)/float64(b.N), "disk-B/op")
				b.ReportMetric(float64(totalUsage.sst)/float64(b.N), "sst-B/op")
				b.ReportMetric(float64(totalUsage.wal)/float64(b.N), "wal-B/op")
				b.ReportMetric(float64(totalUsage.vlog)/float64(b.N), "vlog-B/op")
				b.ReportMetric(float64(totalUsage.total)/float64(b.N)/float64(logicalBytes), "disk/logical")
			})
		}
	}
}

type benchmarkFileUsage struct {
	total int64
	wal   int64
	sst   int64
	vlog  int64
}

func benchmarkResetDB(b *testing.B, dir string, opts Options) *DB {
	b.Helper()
	if err := os.RemoveAll(dir); err != nil {
		b.Fatalf("remove db dir: %v", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("mkdir db dir: %v", err)
	}
	return benchmarkOpenExistingDB(b, dir, opts)
}

func benchmarkOpenExistingDB(b *testing.B, dir string, opts Options) *DB {
	b.Helper()
	db, err := OpenWithOptions(dir, opts)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	return db
}

func benchmarkStopDBWithoutFlush(b *testing.B, db *DB) {
	b.Helper()

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return
	}
	db.closed = true
	db.publishReadStateLocked()
	db.mu.Unlock()

	db.waitForSubmitters()
	db.writeCh <- nil
	db.writeWg.Wait()

	db.mu.Lock()
	activeWal := db.activeWal
	db.activeWal = nil
	db.mu.Unlock()

	if activeWal != nil {
		if err := activeWal.close(); err != nil {
			b.Fatalf("close wal: %v", err)
		}
	}
	close(db.flushCh)
	db.flushWg.Wait()
	if db.valueLog != nil {
		if err := db.valueLog.Close(); err != nil {
			b.Fatalf("close value log: %v", err)
		}
	}

	db.mu.RLock()
	sstables := append([]*sstable.Table(nil), db.sstables...)
	db.mu.RUnlock()
	for _, sst := range sstables {
		if err := sst.Close(); err != nil {
			b.Fatalf("close sstable: %v", err)
		}
	}
}

func benchmarkDBRecords(entries, keySize, valueSize int) ([][]byte, [][]byte, int64) {
	keys := make([][]byte, entries)
	values := make([][]byte, entries)
	var logicalBytes int64

	for i := range entries {
		keys[i] = benchmarkSizedRecord('k', i, keySize)
		values[i] = benchmarkSizedRecord('v', i, valueSize)
		logicalBytes += int64(len(keys[i]) + len(values[i]))
	}

	return keys, values, logicalBytes
}

func benchmarkSizedRecord(fill byte, index, size int) []byte {
	base := []byte(fmt.Sprintf("%c-%08d", fill, index))
	if len(base) >= size {
		return append([]byte(nil), base[:size]...)
	}

	buf := make([]byte, size)
	copy(buf, base)
	for i := len(base); i < len(buf); i++ {
		buf[i] = fill
	}
	return buf
}

func benchmarkDBRoundValues(entries, valueSize, rounds int) [][][]byte {
	valuesByRound := make([][][]byte, rounds)
	for round := range rounds {
		values := make([][]byte, entries)
		fill := byte('a' + round%26)
		for i := range entries {
			values[i] = benchmarkSizedRecord(fill, i, valueSize)
		}
		valuesByRound[round] = values
	}
	return valuesByRound
}

func benchmarkCopyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		data, err := os.ReadFile(srcPath)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if err := os.WriteFile(dstPath, data, info.Mode()); err != nil {
			return err
		}
	}

	return nil
}

func benchmarkDirUsage(dir string) (benchmarkFileUsage, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return benchmarkFileUsage{}, err
	}

	var usage benchmarkFileUsage
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return benchmarkFileUsage{}, err
		}
		size := info.Size()
		usage.total += size
		switch filepath.Ext(entry.Name()) {
		case ".wal":
			usage.wal += size
		case ".sst":
			usage.sst += size
		case ".vlog":
			usage.vlog += size
		}
	}

	return usage, nil
}
