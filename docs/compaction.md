# Compaction

Compaction is how the database cleans up old SSTables.

It merges sorted files, removes obsolete versions, and moves data into lower levels.

## Why it exists

Every memtable flush creates a new SSTable.

If the database never compacted anything, it would accumulate:

- too many tables
- duplicate old versions of keys
- tombstones that never get cleaned up

Compaction fixes that.

## Current level model

Today the system uses a simple leveled model:

- flushes create `L0` tables
- `L0` tables may overlap
- lower levels are intended to be range ordered and non-overlapping

## Current compaction step

The current `Compact()` call is one manual leveled compaction step.

It does roughly this:

```text
1. choose an L0 input set
2. expand that set by overlapping L0 key ranges
3. find overlapping L1 tables by key range
4. merge all chosen tables in sorted key order
5. write output tables at L1
6. update MANIFEST
```

## Why L0 is special

`L0` tables can overlap freely because they come directly from flushes.

That means reads must check them newest first.

Compaction is what turns that messy overlapping state into cleaner lower-level tables.

## Merge rule

When several tables contain the same key, the newest visible version wins.

That means compaction keeps:

- the newest normal value, or
- the newest tombstone

and discards older versions that are no longer visible.

## Tombstones during compaction

Tombstones are not always safe to drop.

They can only be dropped when the compaction is sure there are no older hidden values in deeper levels that still need to be masked.

The code uses a simple rule today:

- if compacting into the bottommost level seen by the planner, tombstones may be dropped
- otherwise they are kept

## Output tables

Compaction writes new SSTables rather than editing old ones.

After the new tables are written:

- old input tables are removed
- the manifest is updated
- the new tables become the official view of that key range

## What it does not do yet

The current compaction system is still basic.

It does not yet include:

- automatic scoring and background scheduling
- multiple lower levels with size targets
- compaction status tracking for concurrency
- subcompactions
- value-log discard accounting

## Mental model

Compaction is housekeeping for sorted files.

It takes a set of overlapping table history and turns it into a smaller, cleaner, more searchable set of files.
