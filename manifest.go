package seol

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nireo/seol/sstable"
)

const (
	manifestFilename = "MANIFEST"
	manifestVersion  = 1
)

type TableMeta struct {
	Filename  string `json:"filename"`
	Level     int    `json:"level"`
	Smallest  []byte `json:"smallest"`
	Largest   []byte `json:"largest"`
	SizeBytes int64  `json:"size_bytes"`
	CreatedAt int64  `json:"created_at"`
}

type manifestState struct {
	Version int         `json:"version"`
	Tables  []TableMeta `json:"tables"`
}

type manifestStore struct {
	path  string
	state manifestState
}

func openManifest(dir string) (*manifestStore, error) {
	path := filepath.Join(dir, manifestFilename)
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		state, rebuildErr := rebuildManifestStateFromDisk(dir)
		if rebuildErr != nil {
			return nil, rebuildErr
		}
		store := &manifestStore{path: path, state: state}
		if err := store.write(); err != nil {
			return nil, err
		}
		return store, nil
	}

	var state manifestState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("seol: decode manifest: %w", err)
	}
	if state.Version != manifestVersion {
		return nil, fmt.Errorf("seol: unsupported manifest version %d", state.Version)
	}
	state.Tables = cloneTableMeta(state.Tables)
	return &manifestStore{path: path, state: state}, nil
}

func rebuildManifestFromDisk(dir string) error {
	state, err := rebuildManifestStateFromDisk(dir)
	if err != nil {
		return err
	}
	return (&manifestStore{path: filepath.Join(dir, manifestFilename), state: state}).write()
}

func rebuildManifestStateFromDisk(dir string) (manifestState, error) {
	sstPaths, _, err := scanDataFiles(dir)
	if err != nil {
		return manifestState{}, err
	}
	tables := make([]TableMeta, 0, len(sstPaths))
	for _, path := range sstPaths {
		table, err := sstable.Open(path)
		if err != nil {
			return manifestState{}, err
		}
		meta := tableMetaFromSST(table.Metadata(), 0)
		if err := table.Close(); err != nil {
			return manifestState{}, err
		}
		tables = append(tables, meta)
	}
	return manifestState{Version: manifestVersion, Tables: tables}, nil
}

func (m *manifestStore) Tables() []TableMeta {
	if m == nil {
		return nil
	}
	return cloneTableMeta(m.state.Tables)
}

func (m *manifestStore) ReplaceTables(tables []TableMeta) error {
	if m == nil {
		return fmt.Errorf("seol: manifest is nil")
	}
	m.state.Tables = cloneTableMeta(tables)
	return m.write()
}

func (m *manifestStore) write() error {
	data, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return err
	}
	tmpPath := m.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, m.path)
}

func cloneTableMeta(src []TableMeta) []TableMeta {
	if len(src) == 0 {
		return nil
	}
	out := make([]TableMeta, len(src))
	for i := range src {
		out[i] = TableMeta{
			Filename:  src[i].Filename,
			Level:     src[i].Level,
			Smallest:  append([]byte(nil), src[i].Smallest...),
			Largest:   append([]byte(nil), src[i].Largest...),
			SizeBytes: src[i].SizeBytes,
			CreatedAt: src[i].CreatedAt,
		}
	}
	return out
}

func tableMetaFromSST(meta sstable.Metadata, level int) TableMeta {
	return TableMeta{
		Filename:  filepath.Base(meta.Path),
		Level:     level,
		Smallest:  append([]byte(nil), meta.Smallest...),
		Largest:   append([]byte(nil), meta.Largest...),
		SizeBytes: meta.SizeBytes,
		CreatedAt: tableCreatedAt(filepath.Base(meta.Path)),
	}
}

func tableCreatedAt(filename string) int64 {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	if ts, err := strconv.ParseInt(base, 10, 64); err == nil {
		return ts
	}
	return time.Now().UnixMicro()
}
