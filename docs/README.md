# Docs

```text
write:
  Put/Delete
    -> WAL
    -> memtable
    -> flush to L0 SSTable
    -> later compaction to lower levels

large values:
  value may be written to the value log
  SSTable stores a small reference instead of the full value

read:
  memtable
    -> immutable memtables
    -> L0 tables (newest first)
    -> L1+ tables (by key range)
    -> value log if the stored value is a value reference

metadata:
  MANIFEST tracks which SSTables exist and which level they belong to
```

Pages:

- `overview.md`: the whole storage engine in one pass
- `memtable.md`: the in-memory write buffer
- `wal.md`: the write-ahead log and durability model
- `sstable.md`: immutable sorted tables on disk
- `value-log.md`: large-value storage and garbage collection
- `manifest.md`: table metadata and level membership
- `compaction.md`: how SSTables are merged and moved between levels
- `file-formats.md`: on-disk and encoded byte layouts
