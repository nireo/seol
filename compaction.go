package seol

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nireo/seol/skiplist"
	"github.com/nireo/seol/sstable"
)

type compactionPlan struct {
	inputPaths        []string
	targetOutputBytes int64
	dropTombstones    bool
}

type compactionPlanner interface {
	Plan(dir string, opts Options) (*compactionPlan, error)
}

type fullCompactionPlanner struct{}

func (fullCompactionPlanner) Plan(dir string, opts Options) (*compactionPlan, error) {
	sstPaths, _, err := scanDataFiles(dir)
	if err != nil {
		return nil, err
	}
	if len(sstPaths) == 0 {
		return nil, nil
	}
	return &compactionPlan{
		inputPaths:        sstPaths,
		targetOutputBytes: compactionOutputBytes(opts),
		dropTombstones:    true,
	}, nil
}

func (db *DB) Compact() (*DB, error) {
	opts := db.optionsSnapshot()
	dir := db.dir
	flushFn := db.flushFn
	planner := fullCompactionPlanner{}

	if err := db.Close(); err != nil {
		return nil, err
	}
	planned, err := planner.Plan(dir, opts)
	if err != nil {
		return nil, err
	}
	if err := executeCompactionPlan(dir, planned, flushFn); err != nil {
		return nil, err
	}
	return openDB(dir, opts, flushFn)
}

func compactionOutputBytes(opts Options) int64 {
	if opts.MemtableMaxBytes > defaultMemtableMaxBytes {
		return opts.MemtableMaxBytes
	}
	return defaultMemtableMaxBytes
}

func executeCompactionPlan(dir string, plan *compactionPlan, flushFn func(baseDir string, sk *skiplist.Skiplist) (*sstable.Table, error)) error {
	if plan == nil || len(plan.inputPaths) == 0 {
		return nil
	}
	if flushFn == nil {
		flushFn = sstable.Flush
	}

	inputs, err := openSSTables(plan.inputPaths)
	if err != nil {
		return err
	}
	defer func() { closeSSTables(inputs) }()

	tempDir, err := os.MkdirTemp(dir, ".compact-")
	if err != nil {
		return err
	}
	cleanupTempDir := true
	defer func() {
		if cleanupTempDir {
			_ = os.RemoveAll(tempDir)
		}
	}()

	outputs, err := compactTables(tempDir, inputs, plan, flushFn)
	if err != nil {
		closeSSTables(outputs)
		return err
	}
	closeSSTables(outputs)
	closeSSTables(inputs)
	inputs = nil

	if err := replaceCompactedTables(dir, plan.inputPaths, tempDir); err != nil {
		return err
	}
	cleanupTempDir = false
	return nil
}

func compactTables(tempDir string, tables []*sstable.Table, plan *compactionPlan, flushFn func(baseDir string, sk *skiplist.Skiplist) (*sstable.Table, error)) ([]*sstable.Table, error) {
	targetBytes := plan.targetOutputBytes
	if targetBytes <= 0 {
		targetBytes = defaultMemtableMaxBytes
	}

	builder := skiplist.New(memtableArenaSize(targetBytes))
	builderBytes := int64(0)
	outputs := make([]*sstable.Table, 0, len(tables))

	flushBuilder := func() error {
		if builder.Empty() {
			return nil
		}
		sst, err := flushFn(tempDir, builder)
		if err != nil {
			return err
		}
		outputs = append(outputs, sst)
		builder = skiplist.New(memtableArenaSize(targetBytes))
		builderBytes = 0
		return nil
	}

	iter := newCompactionMergeIterator(tables)
	for iter.Valid() {
		key := iter.Key()
		stored := iter.Value()
		if !(plan.dropTombstones && isTombstoneValue(stored)) {
			builder.Put(key, stored)
			builderBytes += int64(len(key) + len(stored))
			if builderBytes >= targetBytes {
				if err := flushBuilder(); err != nil {
					closeSSTables(outputs)
					return nil, err
				}
			}
		}
		iter.Next()
	}
	if err := iter.Err(); err != nil {
		closeSSTables(outputs)
		return nil, err
	}
	if err := flushBuilder(); err != nil {
		closeSSTables(outputs)
		return nil, err
	}
	return outputs, nil
}

func replaceCompactedTables(dir string, inputPaths []string, tempDir string) error {
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		return err
	}
	newNames := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sst" {
			continue
		}
		newNames[entry.Name()] = struct{}{}
		if err := os.Rename(filepath.Join(tempDir, entry.Name()), filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}

	for _, path := range inputPaths {
		base := filepath.Base(path)
		if _, ok := newNames[base]; ok {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("seol: remove compacted table %s: %w", base, err)
		}
	}
	if err := rebuildManifestFromDisk(dir); err != nil {
		return err
	}
	return os.RemoveAll(tempDir)
}

type compactionMergeIterator struct {
	inputs []compactionMergeInput
	key    []byte
	value  []byte
	err    error
	valid  bool
}

type compactionMergeInput struct {
	priority int
	iter     *sstable.Iterator
}

func newCompactionMergeIterator(tables []*sstable.Table) *compactionMergeIterator {
	inputs := make([]compactionMergeInput, 0, len(tables))
	for i, table := range tables {
		iter := table.NewIterator()
		iter.Rewind()
		inputs = append(inputs, compactionMergeInput{priority: i, iter: iter})
	}
	it := &compactionMergeIterator{inputs: inputs}
	it.advance()
	return it
}

func (it *compactionMergeIterator) Valid() bool {
	return it != nil && it.valid && it.err == nil
}

func (it *compactionMergeIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.key
}

func (it *compactionMergeIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.value
}

func (it *compactionMergeIterator) Next() {
	if !it.Valid() {
		return
	}
	it.advance()
}

func (it *compactionMergeIterator) Err() error {
	if it == nil {
		return nil
	}
	return it.err
}

func (it *compactionMergeIterator) advance() {
	if it == nil || it.err != nil {
		return
	}

	chosen := -1
	for i := range it.inputs {
		input := &it.inputs[i]
		if err := input.iter.Err(); err != nil {
			it.err = err
			it.valid = false
			return
		}
		if !input.iter.Valid() {
			continue
		}
		if chosen == -1 {
			chosen = i
			continue
		}
		cmp := bytes.Compare(input.iter.Key(), it.inputs[chosen].iter.Key())
		if cmp < 0 || (cmp == 0 && input.priority < it.inputs[chosen].priority) {
			chosen = i
		}
	}
	if chosen == -1 {
		it.key = nil
		it.value = nil
		it.valid = false
		return
	}

	winnerKey := it.inputs[chosen].iter.Key()
	it.key = winnerKey
	it.value = it.inputs[chosen].iter.Value()
	it.valid = true

	for i := range it.inputs {
		input := &it.inputs[i]
		if !input.iter.Valid() || !bytes.Equal(input.iter.Key(), winnerKey) {
			continue
		}
		input.iter.Next()
		if err := input.iter.Err(); err != nil {
			it.err = err
			it.valid = false
			return
		}
	}
}
