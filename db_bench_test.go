package seol

import (
	"fmt"
	"os"
	"path/filepath"
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
)

var benchmarkDBBytesSink []byte

func BenchmarkDBPut(b *testing.B) {
	keys, values, _ := benchmarkDBRecords(1<<12, benchmarkDBKeySize, benchmarkDBValueSize)
	cases := []struct {
		name         string
		syncInterval time.Duration
	}{
		{name: "sync_immediate", syncInterval: 0},
		{name: "sync_batched_5ms", syncInterval: 5 * time.Millisecond},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			opts := Options{MemtableMaxBytes: benchmarkDBMemtableBytes, WALSyncInterval: tc.syncInterval}
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
	activeWal := db.activeWal
	db.activeWal = nil
	db.mu.Unlock()

	if activeWal != nil {
		if err := activeWal.close(); err != nil {
			b.Fatalf("close wal: %v", err)
		}
	}
	close(db.flushCh)
	db.wg.Wait()
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
