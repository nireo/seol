package seol

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nireo/seol/skiplist"
	"github.com/nireo/seol/sstable"
	"github.com/nireo/seol/vlog"
)

func (db *DB) RunValueLogGC() (*DB, error) {
	opts := db.optionsSnapshot()
	dir := db.dir
	flushFn := db.flushFn

	if err := db.Close(); err != nil {
		return nil, err
	}
	if err := rewriteValueLogFiles(dir, opts, flushFn); err != nil {
		return nil, err
	}
	return openDB(dir, opts, flushFn)
}

func (db *DB) optionsSnapshot() Options {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return Options{
		MemtableMaxBytes:        db.memtableMaxBytes,
		WALSyncInterval:         db.walSyncInterval,
		ValueLogSegmentMaxBytes: db.valueLogSegmentMaxBytes,
		ValueThreshold:          db.valueThreshold,
		AsyncWrites:             db.asyncWrites,
		WriteBatchWindow:        db.writeBatchWindow,
	}
}

func rewriteValueLogFiles(dir string, opts Options, flushFn func(baseDir string, sk *skiplist.Skiplist) (*sstable.Table, error)) error {
	opts = normalizeOptions(opts)
	if flushFn == nil {
		flushFn = sstable.Flush
	}
	manifest, err := openManifest(dir)
	if err != nil {
		return err
	}
	tableMeta := manifest.Tables()

	oldSSTs, err := openSSTablesFromMeta(dir, tableMeta)
	if err != nil {
		return err
	}
	defer func() { closeSSTables(oldSSTs) }()
	levels, _, _, err := buildLevelState(tableMeta, oldSSTs)
	if err != nil {
		return err
	}

	oldVlog, err := vlog.Open(dir, vlog.Options{SegmentMaxBytes: opts.ValueLogSegmentMaxBytes})
	if err != nil {
		return err
	}
	defer func() {
		if oldVlog != nil {
			_ = oldVlog.Close()
		}
	}()

	tempDir, err := os.MkdirTemp(dir, ".vlog-gc-")
	if err != nil {
		return err
	}
	cleanupTempDir := true
	defer func() {
		if cleanupTempDir {
			_ = os.RemoveAll(tempDir)
		}
	}()

	newVlog, err := vlog.Open(tempDir, vlog.Options{SegmentMaxBytes: opts.ValueLogSegmentMaxBytes})
	if err != nil {
		return err
	}

	newSSTs, newMeta, err := rebuildLiveState(tempDir, levels, oldVlog, newVlog, opts, flushFn)
	if err != nil {
		_ = newVlog.Close()
		closeSSTables(newSSTs)
		return err
	}
	closeSSTables(newSSTs)
	if err := newVlog.Close(); err != nil {
		return err
	}
	if err := oldVlog.Close(); err != nil {
		return err
	}
	oldVlog = nil
	closeSSTables(oldSSTs)
	oldSSTs = nil

	if err := replaceRewrittenFiles(dir, manifest, tableMeta, newMeta, tempDir); err != nil {
		return err
	}
	cleanupTempDir = false
	return nil
}

func rebuildLiveState(tempDir string, levels []tableLevelState, oldVlog, newVlog *vlog.Log, opts Options, flushFn func(baseDir string, sk *skiplist.Skiplist) (*sstable.Table, error)) ([]*sstable.Table, []TableMeta, error) {
	seen := make(map[string]struct{})
	flushed := make([]*sstable.Table, 0)
	flushedMeta := make([]TableMeta, 0)

	for _, level := range levels {
		for i, table := range level.tables {
			sourceMeta := level.meta[i]
			builder := skiplist.New(memtableArenaSize(opts.MemtableMaxBytes))
			builderBytes := int64(0)
			flushBuilder := func() error {
				if builder.Empty() {
					return nil
				}
				sst, err := flushFn(tempDir, builder)
				if err != nil {
					return err
				}
				meta := tableMetaFromSST(sst.Metadata(), sourceMeta.Level)
				meta.CreatedAt = sourceMeta.CreatedAt
				flushed = append(flushed, sst)
				flushedMeta = append(flushedMeta, meta)
				builder = skiplist.New(memtableArenaSize(opts.MemtableMaxBytes))
				builderBytes = 0
				return nil
			}

			if err := table.Scan(func(key, stored []byte) error {
				keyStr := string(key)
				if _, ok := seen[keyStr]; ok {
					return nil
				}
				seen[keyStr] = struct{}{}

				value, deleted, err := resolveStoredValue(oldVlog, stored)
				if err != nil {
					return err
				}
				if deleted {
					return nil
				}
				rewritten, err := storeValueForLSM(newVlog, opts.ValueThreshold, key, value)
				if err != nil {
					return err
				}
				builder.Put(key, rewritten)
				builderBytes += memtableEntrySize(key, value)
				if builderBytes < opts.MemtableMaxBytes {
					return nil
				}
				return flushBuilder()
			}); err != nil {
				closeSSTables(flushed)
				return nil, nil, err
			}
			if err := flushBuilder(); err != nil {
				closeSSTables(flushed)
				return nil, nil, err
			}
		}
	}

	return flushed, flushedMeta, nil
}

func replaceRewrittenFiles(dir string, manifest *manifestStore, oldTables, newTables []TableMeta, tempDir string) error {
	oldVlogPaths, err := scanValueLogPaths(dir)
	if err != nil {
		return err
	}

	newEntries, err := os.ReadDir(tempDir)
	if err != nil {
		return err
	}
	newNames := make(map[string]struct{}, len(newEntries))
	for _, entry := range newEntries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".sst") && !strings.HasSuffix(name, ".vlog") {
			continue
		}
		newNames[name] = struct{}{}
		if err := os.Rename(filepath.Join(tempDir, name), filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	for _, table := range oldTables {
		base := table.Filename
		if _, ok := newNames[base]; ok {
			continue
		}
		if removeErr := os.Remove(filepath.Join(dir, base)); removeErr != nil && !os.IsNotExist(removeErr) {
			return fmt.Errorf("seol: remove old data file %s: %w", base, removeErr)
		}
	}
	for _, path := range oldVlogPaths {
		base := filepath.Base(path)
		if _, ok := newNames[base]; ok {
			continue
		}
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			return fmt.Errorf("seol: remove old data file %s: %w", base, removeErr)
		}
	}
	updatedTables := cloneTableMeta(newTables)
	orderTableMetaForManifest(updatedTables)
	if err := manifest.ReplaceTables(updatedTables); err != nil {
		return err
	}

	return os.RemoveAll(tempDir)
}

func scanValueLogPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".vlog") {
			continue
		}
		paths = append(paths, filepath.Join(dir, entry.Name()))
	}
	return paths, nil
}
