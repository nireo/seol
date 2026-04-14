# SSTable

SSTable means sorted string table.

It is the main on-disk storage format for keys and stored values.

## What it is

An SSTable is an immutable file containing keys in sorted order.

That gives the database two big advantages:

- efficient lookup by key
- efficient merging during compaction

## Why immutability helps

Once an SSTable is written, it never changes.

That means:

- no in-place updates
- no page rewrite logic
- compaction can treat tables as stable inputs

## Internal layout

At a high level an SSTable contains:

```text
[ bloom filter ][ data blocks ][ index ][ footer ]
```

### Bloom filter

The Bloom filter is a fast "probably present / definitely absent" check.

If the Bloom filter says a key is definitely absent, the table can be skipped without reading data blocks.

### Data blocks

Data blocks store actual entries.

- blocks are currently about `4 KiB`
- entries inside a block are sorted by key
- each entry stores key length, value length, key bytes, and value bytes

### Index

The index maps the first key of each data block to:

- block offset
- block length

That lets the read path jump to the right data block quickly.

### Footer

The footer stores:

- index offset
- magic number
- format version

## Lookup flow

Reading a key from one SSTable is roughly:

```text
1. check Bloom filter
2. use the index to find the candidate block
3. read that block
4. binary-search inside the block
```

## Block cache

The code keeps a small in-memory cache of decoded data blocks.

That helps when many reads hit the same hot block.

## What the value field contains

Like the memtable, an SSTable value may be:

- inline user bytes
- a value-log reference
- a tombstone marker

The SSTable does not care which one it is. It just stores bytes.

## Levels

SSTables are grouped into levels.

- `L0` tables may overlap
- `L1+` tables should not overlap by key range

That rule is what makes range-based lookup possible for lower levels.

## Mental model

An SSTable is a frozen memtable.

It is the durable sorted snapshot of what the memtable looked like at flush time.
