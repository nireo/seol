# Memtable

The memtable is the current in-memory state of the database.

It is the first mutable structure a new write reaches after the WAL append succeeds.

## Why it exists

Writing directly into sorted on-disk files would be slow.

The memtable solves that by giving the database a fast mutable structure in memory. Later, that structure is written to disk as one sequential flush.

## Data structure

The memtable is a skiplist backed by an arena allocator.

Simple version:

```text
head -> key1 -> key4 -> key8 -> ...
          \      \      \
           -> key2 -> key6 -> key9 -> ...
```

Why this is useful:

- keys stay sorted
- inserts and lookups are fast
- iteration is simple
- the arena avoids lots of small heap allocations

## Arena idea

Instead of allocating each key, value, and node separately, the skiplist writes them into one large byte buffer.

That means:

- fewer allocations
- better locality
- simple lifetime: free the whole arena when the memtable is done

## What gets stored as the value

The memtable does not always store the original user value directly.

It stores one of these encoded forms:

- inline value bytes
- value-log reference bytes for large values
- tombstone marker for deletes

So the memtable holds the same logical content that will later be flushed into an SSTable.

## Size accounting

The database tracks approximate memtable size as:

```text
len(key) + len(original logical value)
```

That is a practical threshold for when to rotate the memtable and flush it.

## Flush lifecycle

When the memtable gets large enough:

1. the current WAL is closed
2. the memtable becomes immutable
3. a fresh memtable and fresh WAL are created
4. the immutable memtable is flushed to an SSTable in the background

Diagram:

```text
active memtable
  -> too big
  -> immutable memtable
  -> flush to SSTable

new empty memtable starts accepting writes immediately
```

## Reads

Reads check the memtable first because it has the newest version of each key.

If the memtable has:

- a normal value: return it
- a value-log reference: read the real value from the value log
- a tombstone: treat the key as deleted

## Why the memtable matters

The memtable is the write buffer for the whole system.

If you understand the memtable, the rest of the design is easier to follow:

- WAL makes memtable writes durable
- SSTables are old memtables written to disk
- compaction merges old memtables after they have become SSTables
