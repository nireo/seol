package seol

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/nireo/seol/sstable"
	"github.com/nireo/seol/vlog"
)

type tableLevelState struct {
	level  int
	meta   []TableMeta
	tables []*sstable.Table
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
