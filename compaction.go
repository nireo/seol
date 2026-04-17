package seol

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/nireo/seol/skiplist"
	"github.com/nireo/seol/sstable"
	"github.com/nireo/seol/vlog"
)

type tableLevelState struct {
	level  int
	meta   []TableMeta
	tables []*sstable.Table
}

type compactionPlan struct {
	sourceLevel       int
	targetLevel       int
	sourceTables      []TableMeta
	targetTables      []TableMeta
	targetOutputBytes int64
	dropTombstones    bool
}

type compactionPlanner interface {
	Plan(dir string, opts Options) (*compactionPlan, error)
}

type leveledCompactionPlanner struct{}

func (leveledCompactionPlanner) Plan(dir string, opts Options) (*compactionPlan, error) {
	manifest, err := openManifest(dir)
	if err != nil {
		return nil, err
	}

	levels := groupTableMetaByLevel(manifest.Tables())
	if len(levels) == 0 || len(levels[0].meta) == 0 {
		return nil, nil
	}

	sourceTables := selectL0CompactionSource(levels[0].meta)
	smallest, largest := tableMetaBounds(sourceTables)
	targetTables := overlappingTableMeta(levels, 1, smallest, largest)
	maxLevel := maxTableLevel(levels)

	return &compactionPlan{
		sourceLevel:       0,
		targetLevel:       1,
		sourceTables:      sourceTables,
		targetTables:      targetTables,
		targetOutputBytes: compactionOutputBytes(opts),
		dropTombstones:    maxLevel <= 1,
	}, nil
}

func (db *DB) Compact() (*DB, error) {
	opts := db.optionsSnapshot()
	dir := db.dir
	flushFn := db.flushFn
	planner := leveledCompactionPlanner{}

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
	if plan == nil || len(plan.sourceTables) == 0 {
		return nil
	}
	if flushFn == nil {
		flushFn = sstable.Flush
	}
	manifest, err := openManifest(dir)
	if err != nil {
		return err
	}

	inputs, err := openSSTablesFromMeta(dir, planInputTables(plan))
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
	outputMeta := make([]TableMeta, 0, len(outputs))
	for _, table := range outputs {
		outputMeta = append(outputMeta, tableMetaFromSST(table.Metadata(), plan.targetLevel))
	}
	closeSSTables(outputs)
	closeSSTables(inputs)
	inputs = nil

	if err := replaceCompactedTables(dir, manifest, plan, outputMeta, tempDir); err != nil {
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

func replaceCompactedTables(dir string, manifest *manifestStore, plan *compactionPlan, outputMeta []TableMeta, tempDir string) error {
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

	removeNames := make(map[string]struct{}, len(plan.sourceTables)+len(plan.targetTables))
	for _, table := range append(cloneTableMeta(plan.sourceTables), plan.targetTables...) {
		removeNames[table.Filename] = struct{}{}
	}
	for base := range removeNames {
		if _, ok := newNames[base]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(dir, base)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("seol: remove compacted table %s: %w", base, err)
		}
	}
	updatedTables := make([]TableMeta, 0, len(manifest.state.Tables)-len(removeNames)+len(outputMeta))
	for _, table := range manifest.Tables() {
		if _, ok := removeNames[table.Filename]; ok {
			continue
		}
		updatedTables = append(updatedTables, table)
	}
	updatedTables = append(updatedTables, outputMeta...)
	orderTableMetaForManifest(updatedTables)
	if err := manifest.ReplaceTables(updatedTables); err != nil {
		return err
	}
	return os.RemoveAll(tempDir)
}

func planInputTables(plan *compactionPlan) []TableMeta {
	inputs := make([]TableMeta, 0, len(plan.sourceTables)+len(plan.targetTables))
	inputs = append(inputs, cloneTableMeta(plan.sourceTables)...)
	inputs = append(inputs, cloneTableMeta(plan.targetTables)...)
	return inputs
}

func groupTableMetaByLevel(tables []TableMeta) []tableLevelState {
	if len(tables) == 0 {
		return nil
	}
	grouped := make(map[int][]TableMeta)
	levels := make([]int, 0, len(tables))
	seen := make(map[int]struct{})
	for _, table := range tables {
		grouped[table.Level] = append(grouped[table.Level], table)
		if _, ok := seen[table.Level]; ok {
			continue
		}
		seen[table.Level] = struct{}{}
		levels = append(levels, table.Level)
	}
	sort.Ints(levels)
	result := make([]tableLevelState, 0, len(levels))
	for _, level := range levels {
		meta := cloneTableMeta(grouped[level])
		sortTableMetaForLevel(level, meta)
		result = append(result, tableLevelState{level: level, meta: meta})
	}
	return result
}

func tableMetaBounds(tables []TableMeta) ([]byte, []byte) {
	if len(tables) == 0 {
		return nil, nil
	}
	smallest := append([]byte(nil), tables[0].Smallest...)
	largest := append([]byte(nil), tables[0].Largest...)
	for _, table := range tables[1:] {
		if bytes.Compare(table.Smallest, smallest) < 0 {
			smallest = append(smallest[:0], table.Smallest...)
		}
		if bytes.Compare(table.Largest, largest) > 0 {
			largest = append(largest[:0], table.Largest...)
		}
	}
	return smallest, largest
}

func overlappingTableMeta(levels []tableLevelState, level int, smallest, largest []byte) []TableMeta {
	for _, candidate := range levels {
		if candidate.level != level {
			continue
		}
		var overlaps []TableMeta
		for _, table := range candidate.meta {
			if bytes.Compare(table.Largest, smallest) < 0 || bytes.Compare(table.Smallest, largest) > 0 {
				continue
			}
			overlaps = append(overlaps, table)
		}
		return overlaps
	}
	return nil
}

func maxTableLevel(levels []tableLevelState) int {
	if len(levels) == 0 {
		return 0
	}
	return levels[len(levels)-1].level
}

func selectL0CompactionSource(l0 []TableMeta) []TableMeta {
	if len(l0) == 0 {
		return nil
	}
	oldest := l0[len(l0)-1]
	smallest := append([]byte(nil), oldest.Smallest...)
	largest := append([]byte(nil), oldest.Largest...)
	selected := make(map[string]struct{}, len(l0))
	selected[oldest.Filename] = struct{}{}

	changed := true
	for changed {
		changed = false
		for _, table := range l0 {
			if _, ok := selected[table.Filename]; ok {
				continue
			}
			if bytes.Compare(table.Largest, smallest) < 0 || bytes.Compare(table.Smallest, largest) > 0 {
				continue
			}
			selected[table.Filename] = struct{}{}
			if bytes.Compare(table.Smallest, smallest) < 0 {
				smallest = append(smallest[:0], table.Smallest...)
			}
			if bytes.Compare(table.Largest, largest) > 0 {
				largest = append(largest[:0], table.Largest...)
			}
			changed = true
		}
	}

	result := make([]TableMeta, 0, len(selected))
	for _, table := range l0 {
		if _, ok := selected[table.Filename]; ok {
			result = append(result, table)
		}
	}
	return result
}

func orderTableMetaForManifest(tables []TableMeta) {
	if len(tables) == 0 {
		return
	}
	ordered := groupTableMetaByLevel(tables)
	flattened := make([]TableMeta, 0, len(tables))
	for _, level := range ordered {
		flattened = append(flattened, level.meta...)
	}
	copy(tables, flattened)
}

func buildLevelState(meta []TableMeta, tables []*sstable.Table) ([]tableLevelState, []TableMeta, []*sstable.Table, error) {
	if len(meta) == 0 {
		return nil, nil, nil, nil
	}

	tableByName := make(map[string]*sstable.Table, len(tables))
	for _, table := range tables {
		tableMeta := table.Metadata()
		tableByName[filepath.Base(tableMeta.Path)] = table
	}

	grouped := make(map[int][]TableMeta)
	levels := make([]int, 0, len(meta))
	seenLevels := make(map[int]struct{})
	for _, table := range meta {
		grouped[table.Level] = append(grouped[table.Level], table)
		if _, ok := seenLevels[table.Level]; ok {
			continue
		}
		seenLevels[table.Level] = struct{}{}
		levels = append(levels, table.Level)
	}
	sort.Ints(levels)

	result := make([]tableLevelState, 0, len(levels))
	flatMeta := make([]TableMeta, 0, len(meta))
	flatTables := make([]*sstable.Table, 0, len(meta))
	for _, level := range levels {
		orderedMeta := cloneTableMeta(grouped[level])
		sortTableMetaForLevel(level, orderedMeta)
		levelState := tableLevelState{level: level, meta: orderedMeta, tables: make([]*sstable.Table, 0, len(orderedMeta))}
		for _, tableMeta := range orderedMeta {
			table := tableByName[tableMeta.Filename]
			if table == nil {
				return nil, nil, nil, fmt.Errorf("seol: missing sstable for manifest entry %s", tableMeta.Filename)
			}
			levelState.tables = append(levelState.tables, table)
			flatTables = append(flatTables, table)
		}
		flatMeta = append(flatMeta, orderedMeta...)
		result = append(result, levelState)
	}
	return result, flatMeta, flatTables, nil
}

func sortTableMetaForLevel(level int, tables []TableMeta) {
	if level == 0 {
		sort.Slice(tables, func(i, j int) bool {
			if tables[i].CreatedAt == tables[j].CreatedAt {
				return tables[i].Filename > tables[j].Filename
			}
			return tables[i].CreatedAt > tables[j].CreatedAt
		})
		return
	}

	sort.Slice(tables, func(i, j int) bool {
		cmp := bytes.Compare(tables[i].Smallest, tables[j].Smallest)
		if cmp != 0 {
			return cmp < 0
		}
		if tables[i].CreatedAt == tables[j].CreatedAt {
			return tables[i].Filename < tables[j].Filename
		}
		return tables[i].CreatedAt < tables[j].CreatedAt
	})
}

func cloneTableLevels(src []tableLevelState) []tableLevelState {
	if len(src) == 0 {
		return nil
	}

	out := make([]tableLevelState, len(src))
	for i := range src {
		out[i] = tableLevelState{
			level:  src[i].level,
			meta:   cloneTableMeta(src[i].meta),
			tables: append([]*sstable.Table(nil), src[i].tables...),
		}
	}

	return out
}

func lookupLevels(levels []tableLevelState, valueLog *vlog.Log, key []byte) ([]byte, error) {
	for _, level := range levels {
		if level.level == 0 {
			for i := range level.tables {
				stored, err := level.tables[i].Get(key)
				if err != nil {
					return nil, err
				}
				if stored != nil {
					return readStoredValue(valueLog, stored)
				}
			}
			continue
		}

		idx := findLevelTable(level.meta, key)
		if idx < 0 {
			continue
		}
		stored, err := level.tables[idx].Get(key)
		if err != nil {
			return nil, err
		}
		if stored != nil {
			return readStoredValue(valueLog, stored)
		}
	}
	return nil, nil
}

func findLevelTable(meta []TableMeta, key []byte) int {
	if len(meta) == 0 {
		return -1
	}
	idx := sort.Search(len(meta), func(i int) bool {
		return bytes.Compare(meta[i].Largest, key) >= 0
	})
	if idx == len(meta) {
		return -1
	}
	if bytes.Compare(key, meta[idx].Smallest) < 0 {
		return -1
	}
	return idx
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
