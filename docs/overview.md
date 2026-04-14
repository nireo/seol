# Overview

`seol` is an LSM-style key-value store with a separate value log for large values.

The main idea is simple:

- writes land in memory first
- writes are also appended to a WAL for crash recovery
- memory is flushed into immutable sorted files called SSTables
- large values can live in a separate append-only value log
- compaction merges older SSTables into cleaner lower levels

## Main parts

### Memtable

The memtable is the current mutable in-memory table.

- it is implemented as a skiplist
- it accepts new writes quickly
- reads check it first because it contains the newest data

### WAL

The write-ahead log is the durability and crash-recovery journal.

- every write goes to the WAL before it is considered durable
- if the process dies, the WAL is replayed on open

### SSTables

When the memtable gets big enough, it is flushed to disk as an SSTable.

- SSTables are sorted by key
- they are immutable once written
- newer SSTables shadow older SSTables with the same key

### Value log

Large values can be written to a separate append-only log.

- the SSTable stores a small reference instead of the full value
- this keeps SSTables smaller when values are large
- point lookups may need one extra read from the value log

### Manifest

The manifest is the persistent catalog of SSTables.

- it records which SSTables exist
- it records their levels and key ranges
- on open, the database rebuilds its table view from the manifest

### Compaction

Compaction merges SSTables.

- duplicate older versions are removed
- tombstones can eventually remove deleted keys
- data moves from `L0` to lower levels

## Read path

Point lookups go from newest state to oldest state:

```text
memtable
  -> immutable memtables
  -> L0 SSTables (newest first, may overlap)
  -> L1+ SSTables (select by key-range metadata)
  -> value log if the stored entry is a value reference
```

This ordering is important. It is how the system resolves visibility and returns the newest version of a key.

## Write path

Writes go through these steps:

```text
Put/Delete
  -> append to WAL
  -> encode value for storage
       small value  -> inline bytes
       large value  -> value-log reference
       delete       -> tombstone marker
  -> insert into memtable
  -> flush to SSTable later
```

## Deletes

Deletes are stored as tombstones.

- a tombstone means "this key was deleted"
- reads stop when they see a tombstone
- compaction can remove older versions hidden by that tombstone

## Current level model

Today the code supports:

- `L0`: overlapping tables created by flushes
- `L1+`: range-ordered tables tracked by the manifest

The current compaction step is a basic leveled compaction from `L0` into `L1`.

## What this design is trying to optimize

- fast writes
- straightforward crash recovery
- small sorted files for range-aware lookup
- less SSTable bloat when values are large

Every subsystem is a tradeoff. The rest of the docs explain those tradeoffs one part at a time.
