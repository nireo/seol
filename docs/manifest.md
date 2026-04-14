# Manifest

The manifest is the table catalog.

It tells the database which SSTables exist and how they should be interpreted.

## Why it exists

Without a manifest, the database only sees filenames on disk.

That is not enough for leveled compaction because the database also needs table metadata like:

- which level a table belongs to
- the key range the table covers
- the size of the table
- the relative age of the table

## What it stores

Each table entry in `MANIFEST` contains:

- `filename`
- `level`
- `smallest`
- `largest`
- `size_bytes`
- `created_at`

## Current format

The current manifest is a single JSON file.

That is simple and easy to inspect by hand.

It is not the final format you would choose for a heavily optimized production engine, but it is a good first step because it keeps the state model easy to reason about.

## How it is used on open

When the database opens:

1. it reads `MANIFEST`
2. it opens the listed SSTables
3. it builds in-memory level state from the metadata

If the manifest is missing, it can be rebuilt from the SST files on disk.

## How it changes

The manifest is updated when:

- a flush creates a new `L0` SSTable
- compaction replaces some SSTables with new output SSTables
- value-log GC rewrites SSTables and value refs

## Ordering rules

The manifest is not just a bag of table entries.

The order matters.

- `L0` entries are ordered newest first
- `L1+` entries are ordered by smallest key

That ordering makes the read path and compaction planner much simpler.

## Mental model

Think of the manifest as the database’s table-of-contents file.

The SST files contain the actual data.
The manifest explains how those files fit together.
