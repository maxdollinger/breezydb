# breezyDB

A database that gets out of your way.

Talk to it over plain HTTP, no client library, no driver, no connection
pool to configure. There's nothing to tune: no buffer sizes, no cache
knobs, no vacuum schedules to babysit. Migrations, backups, and
replication are built in, not bolted on.

breezyDB will be good enough for almost every project, and you'll spend approximately zero time
operating it.

## Status

This vision sits ahead of the current code.

## Principles

- **No driver.** Plain HTTP  JSON in, out.
- **No knobs.** Sane defaults, not a settings page.
- **Batteries included.** Migrations, backup, and replication ship with
  the database, not as separate projects to wire up.
- **Good enough, for most.

## Storage Engine

Storage engine

Everything lives in one file. There's no write-ahead log, no side files, nothing to keep in sync. The data file is the log, appended to and never overwritten in place.

One writer, many readers, with MVCC so readers never block and always see a consistent snapshot. Writes commit in a single durable step; a crash can only ever leave an incomplete transaction at the very end, which recovery discards.

Nothing about the file's structure is cached on disk. Startup reconstructs what it needs by reading the file itself, so there's no derived state that can drift out of sync with reality, and no repair tool for when it does.

Deleted rows are marked, not erased, and their space is recovered by a background process that works in small steps. There is no vacuum to schedule and nothing that stops the world to run.

### S — Pre-Build Spikes (gate)

- [x] S.1 Synthetic file generator
- [ ] S.2 Scan-rate spike: seconds/GB, both modes, cold cache
- [ ] S.3 Index memory spike: bytes/row RSS
- [ ] S.4 Go/no-go note: thresholds written before S.2/S.3 run

### M0 — File Primitives + I/O Boundary

- [ ] 0.1 `Device` interface (incl. `zero_range`, `sector_size`)
- [ ] 0.2 `OsDevice` + zeroing fallback path
- [ ] 0.3 Superblock create
- [ ] 0.4 Superblock parse + validate
- [ ] 0.5 Frame addressing + 20-frame extension
- [ ] 0.6 Extension durability crash behavior

### M1 — Record Codec (table 0 hardcoded)

- [ ] 1.1 40-byte record header encode/decode
- [ ] 1.2 Salted hash
- [ ] 1.3 Reserved-zero sentinels (`write_seq`/`frame_seq` ≥ 1)
- [ ] 1.4 Table 0 schema + record codec
- [ ] 1.5 Round-trip test suite

### M2 — Frame Lifecycle + Fake Device

- [ ] 2.1 Frame header open (header before first record)
- [ ] 2.2 Frame seal (seal ≠ commit)
- [ ] 2.3 Free-list vs. extend allocation
- [ ] 2.4 **Fake device v1**
- [ ] 2.5 Torn frame header

### M3 — Transactions + Crash Injection

- [ ] 3.1 `txn_id` / `txn_cnt` assignment
- [ ] 3.2 One fsync per transaction
- [ ] 3.3 Commit-record ordering across frames
- [ ] 3.4 Named injection points
- [ ] 3.5 Seeded RNG reproducibility
- [ ] 3.6 Torn tail
- [ ] 3.7 Landed-but-unfsynced

### M4 — Recovery (critical path)

- [ ] 4.1 Pass 1: four-way classification + `next_frame_seq`
- [ ] 4.2 Pass 2: trusting / paranoid modes + append point
- [ ] 4.3 Cross-frame transaction reassembly (a/b/c)
- [ ] 4.4 Repair sweep
- [ ] 4.5 Duplicate resolution (highest `frame_seq` wins)
- [ ] 4.6 Scenario-per-invariant suite + mutation check
- [ ] 4.7 Full recovery integration loop (500 seeds)

### M5 — MVCC Read Path

- [ ] 5.1 `write_seq` counter + commit watermark
- [ ] 5.2 Reader snapshot creation
- [ ] 5.3 Visibility filter
- [ ] 5.4 PK version chain index
- [ ] 5.5 Concurrent reader/writer scenario

### M6 — General Schema Support (deferrable)

- [ ] 6.1 Field descriptor encode/decode
- [ ] 6.2 Five types + null bitmap
- [ ] 6.3 Var-capacity prefix encoding
- [ ] 6.4 Insert-time schema validation
- [ ] 6.5 Real schema catalog migration
- [ ] 6.6 Re-run crash scenarios on schema-driven tables

### M7 — Compaction / Eviction

- [ ] 7.1 `min_write_seq` tracking
- [ ] 7.2 Victim selection
- [ ] 7.3 Eviction copy
- [ ] 7.4 Release + zero (both paths)
- [ ] 7.5 Trigger logic (never mid-transaction)
- [ ] 7.6 Eviction injection points
- [ ] 7.7 Reader watermark tracking

### M8 — Fuzzing

- [ ] 8.1 Randomized injection harness
- [ ] 8.2 Failure triage / minimal repro
- [ ] 8.3 Fuzz+fix loop

### M9 — Instrument the Hypotheses

- [ ] 9.1 Write amplification (copy vs. zeroing, separate)
- [ ] 9.2 Startup time
- [ ] 9.3 Resident memory
- [ ] 9.4 File-size plateau under churn
