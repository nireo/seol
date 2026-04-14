# WAL

WAL means write-ahead log.

It is the durability layer for new writes.

## Why it exists

The memtable is in memory. If the process crashes, memory disappears.

The WAL fixes that by recording each write to disk first.

Simple rule:

```text
if it is in the WAL, it can be replayed
if it is only in memory, it can be lost on crash
```

## What goes into the WAL

Each record contains:

- checksum
- key length
- value length
- key bytes
- value bytes

Deletes are stored as tombstone values, so they use the same record shape as puts.

## Write flow

```text
Put/Delete
  -> append WAL record
  -> wait for sync if needed
  -> insert into memtable
```

The WAL record is written before the write is considered durable.

## Sync modes

There are two broad modes:

- immediate sync
- buffered sync with a time interval

Buffered sync lets several writes share one `fsync`, which is much faster, but it also means a small amount of very recent acknowledged state may be lost if the machine crashes before the next sync.

## WAL rotation

When the memtable rotates, the current WAL is closed and becomes tied to that immutable memtable.

That old WAL can be deleted only after the matching memtable has been flushed safely to an SSTable.

## Replay on open

When the database starts:

1. it finds old WAL files
2. it replays them in filename order
3. each record is reinserted into the memtable

That restores the newest in-memory state that had not yet been flushed to SSTables.

## Corruption handling

Each WAL record has a CRC32 checksum.

If the process stopped mid-write, replay stops cleanly at a truncated tail instead of trying to treat garbage as a valid record.

## Mental model

Think of the WAL as a recovery script for rebuilding the memtable.

The memtable is the live state.
The WAL is the durable log for recreating that state after a crash.
