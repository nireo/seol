package dbcompare

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	seol "github.com/nireo/seol"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	benchmarkComparePutEntryCount = 1 << 16
	benchmarkCompareGetEntryCount = 1 << 12
	benchmarkCompareKeySize       = 16
	benchmarkCompareValueSize     = 128
	benchmarkCompareMemtableBytes = 32 << 20
	benchmarkCompareSyncWindow    = 5 * time.Millisecond
)

var (
	benchmarkCompareValueSink []byte
	benchmarkCompareSizeSink  int64
)

type benchmarkCompareDB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Close() error
}

type benchmarkCompareEngine struct {
	name string
	open func(b *testing.B, dir string, profile benchmarkCompareWriteProfile) benchmarkCompareDB
}

type benchmarkCompareWriteProfile struct {
	name           string
	seolOptions    seol.Options
	levelWriteOpts *opt.WriteOptions
}

var benchmarkCompareEngines = []benchmarkCompareEngine{
	{name: "seol", open: benchmarkCompareOpenSeol},
	{name: "goleveldb", open: benchmarkCompareOpenLevelDB},
}

var benchmarkCompareWriteProfiles = []benchmarkCompareWriteProfile{
	{
		name: "durable",
		seolOptions: seol.Options{
			MemtableMaxBytes: benchmarkCompareMemtableBytes,
			WALSyncInterval:  0,
		},
		levelWriteOpts: &opt.WriteOptions{Sync: true},
	},
	{
		name: "eventual",
		seolOptions: seol.Options{
			MemtableMaxBytes: benchmarkCompareMemtableBytes,
			WALSyncInterval:  benchmarkCompareSyncWindow,
			AsyncWrites:      true,
		},
		levelWriteOpts: &opt.WriteOptions{Sync: false},
	},
}

var benchmarkCompareReadProfile = benchmarkCompareWriteProfiles[0]

func BenchmarkComparePutSingle(b *testing.B) {
	keys, values := benchmarkCompareRecords(benchmarkComparePutEntryCount, benchmarkCompareKeySize, benchmarkCompareValueSize)

	for _, profile := range benchmarkCompareWriteProfiles {
		for _, engine := range benchmarkCompareEngines {
			b.Run(profile.name+"/"+engine.name, func(b *testing.B) {
				dir := b.TempDir()
				db := benchmarkCompareResetDB(b, engine, dir, profile)
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
						db = benchmarkCompareResetDB(b, engine, dir, profile)
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

func BenchmarkComparePutParallel(b *testing.B) {
	keys, values := benchmarkCompareRecords(benchmarkComparePutEntryCount, benchmarkCompareKeySize, benchmarkCompareValueSize)

	for _, profile := range benchmarkCompareWriteProfiles {
		for _, engine := range benchmarkCompareEngines {
			b.Run(profile.name+"/"+engine.name, func(b *testing.B) {
				dir := b.TempDir()
				db := benchmarkCompareResetDB(b, engine, dir, profile)
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
}

func BenchmarkCompareGetSingle(b *testing.B) {
	keys, values := benchmarkCompareRecords(benchmarkCompareGetEntryCount, benchmarkCompareKeySize, benchmarkCompareValueSize)

	for _, engine := range benchmarkCompareEngines {
		b.Run(engine.name, func(b *testing.B) {
			dir := b.TempDir()
			benchmarkCompareBuildReadFixture(b, engine, dir, keys, values)
			db := engine.open(b, dir, benchmarkCompareReadProfile)
			defer func() { _ = db.Close() }()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				value, err := db.Get(keys[i%len(keys)])
				if err != nil {
					b.Fatalf("get: %v", err)
				}
				benchmarkCompareValueSink = value
			}
		})
	}
}

func BenchmarkCompareGetParallel(b *testing.B) {
	keys, values := benchmarkCompareRecords(benchmarkCompareGetEntryCount, benchmarkCompareKeySize, benchmarkCompareValueSize)

	for _, engine := range benchmarkCompareEngines {
		b.Run(engine.name, func(b *testing.B) {
			dir := b.TempDir()
			benchmarkCompareBuildReadFixture(b, engine, dir, keys, values)
			db := engine.open(b, dir, benchmarkCompareReadProfile)
			defer func() { _ = db.Close() }()

			var idx atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var lastSize int64
				for pb.Next() {
					i := int(idx.Add(1) - 1)
					value, err := db.Get(keys[i%len(keys)])
					if err != nil {
						b.Errorf("get: %v", err)
						return
					}
					lastSize = int64(len(value))
				}
				atomic.StoreInt64(&benchmarkCompareSizeSink, lastSize)
			})
		})
	}
}

func BenchmarkCompareOpen(b *testing.B) {
	keys, values := benchmarkCompareRecords(benchmarkCompareGetEntryCount, benchmarkCompareKeySize, benchmarkCompareValueSize)

	for _, engine := range benchmarkCompareEngines {
		b.Run(engine.name, func(b *testing.B) {
			root := b.TempDir()
			fixtureDir := filepath.Join(root, "fixture")
			if err := os.MkdirAll(fixtureDir, 0o755); err != nil {
				b.Fatalf("mkdir fixture dir: %v", err)
			}
			benchmarkCompareBuildReadFixture(b, engine, fixtureDir, keys, values)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				iterDir := filepath.Join(root, fmt.Sprintf("open-%06d", i))
				if err := os.MkdirAll(iterDir, 0o755); err != nil {
					b.Fatalf("mkdir iteration dir: %v", err)
				}
				if err := benchmarkCompareCopyDir(fixtureDir, iterDir); err != nil {
					b.Fatalf("copy fixture dir: %v", err)
				}

				b.StartTimer()
				db := engine.open(b, iterDir, benchmarkCompareReadProfile)
				b.StopTimer()
				if err := db.Close(); err != nil {
					b.Fatalf("close db: %v", err)
				}
				if err := os.RemoveAll(iterDir); err != nil {
					b.Fatalf("remove iteration dir: %v", err)
				}
			}
		})
	}
}

type benchmarkCompareSeolDB struct {
	db *seol.DB
}

func benchmarkCompareOpenSeol(b *testing.B, dir string, profile benchmarkCompareWriteProfile) benchmarkCompareDB {
	b.Helper()
	db, err := seol.OpenWithOptions(dir, profile.seolOptions)
	if err != nil {
		b.Fatalf("open seol db: %v", err)
	}
	return &benchmarkCompareSeolDB{db: db}
}

func (db *benchmarkCompareSeolDB) Put(key, value []byte) error {
	return db.db.Put(key, value)
}

func (db *benchmarkCompareSeolDB) Get(key []byte) ([]byte, error) {
	return db.db.Get(key)
}

func (db *benchmarkCompareSeolDB) Close() error {
	return db.db.Close()
}

type benchmarkCompareLevelDB struct {
	db           *leveldb.DB
	writeOptions *opt.WriteOptions
}

func benchmarkCompareOpenLevelDB(b *testing.B, dir string, profile benchmarkCompareWriteProfile) benchmarkCompareDB {
	b.Helper()
	db, err := leveldb.OpenFile(dir, &opt.Options{
		Compression: opt.NoCompression,
		WriteBuffer: benchmarkCompareMemtableBytes,
	})
	if err != nil {
		b.Fatalf("open goleveldb db: %v", err)
	}
	return &benchmarkCompareLevelDB{db: db, writeOptions: profile.levelWriteOpts}
}

func (db *benchmarkCompareLevelDB) Put(key, value []byte) error {
	return db.db.Put(key, value, db.writeOptions)
}

func (db *benchmarkCompareLevelDB) Get(key []byte) ([]byte, error) {
	value, err := db.db.Get(key, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, nil
	}
	return value, err
}

func (db *benchmarkCompareLevelDB) Close() error {
	return db.db.Close()
}

func benchmarkCompareResetDB(b *testing.B, engine benchmarkCompareEngine, dir string, profile benchmarkCompareWriteProfile) benchmarkCompareDB {
	b.Helper()
	if err := os.RemoveAll(dir); err != nil {
		b.Fatalf("remove db dir: %v", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatalf("mkdir db dir: %v", err)
	}
	return engine.open(b, dir, profile)
}

func benchmarkCompareBuildReadFixture(b *testing.B, engine benchmarkCompareEngine, dir string, keys, values [][]byte) {
	b.Helper()
	db := benchmarkCompareResetDB(b, engine, dir, benchmarkCompareReadProfile)
	for i := range keys {
		if err := db.Put(keys[i], values[i]); err != nil {
			b.Fatalf("put fixture: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatalf("close fixture db: %v", err)
	}
}

func benchmarkCompareRecords(entries, keySize, valueSize int) ([][]byte, [][]byte) {
	keys := make([][]byte, entries)
	values := make([][]byte, entries)
	for i := range entries {
		keys[i] = benchmarkCompareSizedRecord('k', i, keySize)
		values[i] = benchmarkCompareSizedRecord('v', i, valueSize)
	}
	return keys, values
}

func benchmarkCompareSizedRecord(fill byte, index, size int) []byte {
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

func benchmarkCompareCopyDir(src, dst string) error {
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
