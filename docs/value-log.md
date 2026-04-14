# Value Log

The value log is where large values can live.

It is a separate append-only file family stored as `.vlog` segments.

## Why it exists

Large values make compaction expensive because the same payload may be rewritten many times.

If the database kept every large value directly inside SSTables, compaction would keep rewriting those bytes again and again.

The value log changes that:

- large values are appended once to the value log
- SSTables store a small reference instead of the full value

That makes SSTables smaller and can reduce write amplification for large values.

## Threshold

The database uses a `ValueThreshold`.

- values at or below the threshold stay inline
- values above the threshold go to the value log

The current default is `2 KiB`.

## Segments

The value log is split into segment files:

```text
123456789.vlog
123456790.vlog
123456791.vlog
```

Only one segment is active for appends at a time.
When it fills up, a new segment is created.

## Value references

If a value goes to the value log, the memtable and SSTable store a reference instead.

That reference encodes:

- which segment file to read
- which byte offset in that segment
- how long the value is
- what checksum the final value should have

## Read flow

```text
Get(key)
  -> find stored bytes in memtable/SSTable
  -> if bytes are inline, return them
  -> if bytes are a value reference, read the value log record
```

## Stored forms

For any logical value, the storage layer may hold:

- inline bytes
- tombstone marker
- value reference

This is why the code has a separate encode/decode step for stored values.

## Deletes and the value log

Deletes do not append anything to the value log.

They store a tombstone marker in the memtable/SSTable path.

Older value-log entries for that key become stale and are reclaimed later by value-log GC.

## Value-log GC

Value-log GC is an offline rewrite today.

It does this:

1. close the DB
2. walk the live keyspace in read order
3. keep only the newest visible values
4. rewrite live large values into fresh `.vlog` files
5. rewrite SSTs so their references point at the new value-log locations

Important detail:

- GC preserves table levels now
- it does not flatten everything back to `L0`

## Tradeoff

The value log helps with large-value rewrite cost, but it also adds:

- extra indirection on reads
- another file family to manage
- separate GC work

That is why threshold tuning matters so much.
