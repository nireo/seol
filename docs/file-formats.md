# File Formats

This page focuses on the byte layouts used by the system.

It is intentionally practical rather than theoretical.

## Stored value forms

Before looking at files, it helps to know that the storage layer can represent a logical value in three different ways:

```text
1. inline user value
2. tombstone marker
3. value-log reference
```

### Tombstone marker

Current marker bytes:

```text
00 73 65 6f 6c 74 6f 6d 62 01
```

That is a sentinel byte sequence used internally to mean "this key was deleted".

### Value reference

Current prefix:

```text
00 73 65 6f 6c 76 72 01
```

Current compact payload:

```text
[ segment_id:u64 ][ offset:u32 ][ value_len:u32 ][ checksum:u32 ]
```

There is also a legacy larger form that keeps `offset` as `u64`. The decoder accepts both layouts.

## WAL format

Each WAL record is:

```text
[ checksum:u32 ][ key_len:u32 ][ value_len:u32 ][ key bytes ][ value bytes ]
```

Notes:

- checksum is CRC32 of everything after the checksum field
- deletes are just normal records whose value bytes are the tombstone marker

## Value-log record format

Each value-log record is:

```text
[ checksum:u32 ][ key_len:u32 ][ value_len:u32 ][ key bytes ][ value bytes ]
```

This looks a lot like the WAL on purpose. It is a simple append-only record format.

The `ValueRef` identifies one of these records by:

- segment id
- record offset
- expected value length
- expected value checksum

## SSTable format

High-level layout:

```text
[ bloom filter ][ data blocks ][ index ][ footer ]
```

### SSTable entry format

Each key/value entry inside a data block is:

```text
[ key_len:u16 ][ value_len:u32 ][ key bytes ][ value bytes ]
```

### Block index entry format

The SSTable index stores one entry per data block:

```text
[ first_key_len:u16 ][ first_key bytes ][ block_offset:u64 ][ block_length:u32 ]
```

This points to one whole data block, not to one single key.

### Footer format

The footer is:

```text
[ index_offset:u64 ][ magic:u32 ][ version:u8 ]
```

Current values:

- magic: `0xFF12FF45`
- version: `2`

## MANIFEST format

The current manifest is JSON.

Shape:

```json
{
  "version": 1,
  "tables": [
    {
      "filename": "123456789.sst",
      "level": 0,
      "smallest": "...",
      "largest": "...",
      "size_bytes": 12345,
      "created_at": 123456789
    }
  ]
}
```

The `smallest` and `largest` fields are raw byte slices encoded as JSON base64 strings because keys are arbitrary bytes.

## File families on disk

The main on-disk files are:

```text
MANIFEST
1234567890123.wal
1234567890123.sst
1234567890123.vlog
```

Meaning:

- `.wal`: crash recovery
- `.sst`: sorted immutable table
- `.vlog`: value-log segment
- `MANIFEST`: table metadata and level membership

## Quick mental map

```text
logical key/value
  -> WAL record for durability
  -> memtable entry in memory
  -> SSTable entry on flush
  -> maybe a value-log record if the value is large
  -> manifest entry so the DB knows where the SST belongs
```
