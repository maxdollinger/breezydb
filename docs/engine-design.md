# Storage Engine: Design

**Draft 1.** This draft exists to verify the engine concept end to end. Breaking format changes are expected. Nothing here is designed for forward compatibility and no migration path is provided. Files written by one build are not expected to be readable by the next.

---

## 1. Model

Single file, single writer, multiple in-process readers. Log-structured and append-only: the data file is the log, so there is no WAL and no accompanying files. MVCC over a global write sequence.

## 2. File layout

```
offset 0        superblock            (64 kB)
offset 65536    frame 0
offset 131072   frame 1
...
offset(frame_idx) = (frame_idx + 1) × 65536
```

All frames are 64 kB. Frame location is pure arithmetic, so the file can be walked at startup without knowing any table's record size.

The file is extended in units of **20 frames (1.25 MiB)** using `fallocate`, not `ftruncate`. The space must be really allocated or `ENOSPC` reappears at write time. Newly allocated frames read as zeros and are therefore born free. Extension size is a tuning constant, not a format property.

**Every frame in the file stays allocated for the life of the file.** Release (§9) returns a frame to the zeroed state with `FALLOC_FL_ZERO_RANGE`, which converts the extent to unwritten without deallocating it. `FALLOC_FL_PUNCH_HOLE` is deliberately not used. It would make the free list a list of addresses rather than a space reservation, moving `ENOSPC` from write time to frame-open time, and it would fragment the extent map that pass 1 (§10) strides through. The consequence is that the file never shrinks: space is reused, not returned to the filesystem.

## 3. Superblock

Written once at file creation and never rewritten. There is no second copy, no generation counter and no clean-shutdown flag. Every field is a constant, and the frames alone are sufficient to reconstruct the file's contents.

| Field | Notes |
|---|---|
| magic | file identification |
| `format_version` | **checked at open; refuse to proceed on mismatch** |
| `frame_size` | 65536 |
| `checksum_id` | which algorithm verifies every hash in the file |
| `uuid` | database identity |
| self-hash | validates the above |

`format_version` is load-bearing for this draft specifically. The layout will change repeatedly, and a stale file must fail loudly rather than parse far enough to look plausible.

Used space is kept within the first 4 kB, so on a 4 kB-sector device the superblock is written in one block.

## 4. Frames

A frame is **open** (appendable, never a compaction victim) or **sealed** (immutable). Layout is `[header, records…, seal]`.

Within one allocation a frame is written exactly twice: the header when it is opened, the seal when it fills. A frame returned to the free list and reused is re-opened with a fresh header carrying a **new `frame_seq`**. `frame_seq` is monotonic and never reissued, which §5 depends on.

**Header, 64 B**

| Field | Purpose |
|---|---|
| magic | non-zero, so a zeroed frame is unambiguously free |
| `frame_seq` | monotonic allocation order. Load-bearing; see §5 and §9 |
| `frame_idx` | self-check only. A header at offset X claiming a different index is a misdirected write. Nothing branches on it |
| `table_id` | |
| `table_version` | the `write_seq` of the schema record defining this version |

**Seal, 64 B:** `min_write_seq` (victim selection key; see §9), `max_write_seq`, record count, hash.

Usable space per frame: **65,408 bytes**.

## 5. Records

Fixed size per table, determined by the schema. Layout `[header, data, hash]`: the hash is appended after the data, not carried inside the header. The PK sits at a fixed schema-known offset.

**Header fields**, in fixed order:

| Field | Purpose |
|---|---|
| `write_seq` | globally monotonic across all tables |
| `txn_id` | `write_seq` of the transaction's last record (§8) |
| `txn_cnt` | records in the transaction (§8) |
| flags | tombstone |
| null bitmap | one bit per field, so its width caps the field count (§6) |

Field widths, padding and total header size are settled at implementation time. Nothing in this design depends on the specific numbers, only on the header being a fixed size for every record in the file.

**The hash covers the whole record and is salted with the containing frame's `frame_seq`:**

```
hash = checksum(frame_seq ‖ record_header ‖ data)
```

The salt is not stored. It is read from the frame header at verification time. Because `frame_seq` is monotonic and never reissued (§4), a record written under one allocation of a frame cannot validate under any later allocation of that same frame. Three things depend on this:

- **Freeing can stay derived.** Release zeroes the victim (§9), but zeroing is not crash-atomic: a crash mid-release leaves records intact in a frame that recovery will correctly judge free and hand out for reuse. Those leftovers are self-invalidating, because the reused frame carries a higher `frame_seq` and the stale bytes fail the checksum. Without the salt, leftovers landing on slot boundaries at the same `record_size` would verify cleanly and could outlive the tombstone that masked them. **Zeroing is a data-safety measure; the salt is the correctness mechanism.** Neither substitutes for the other.
- **The append point of an open frame is recoverable.** Sealed frames carry a record count; open frames do not. Recovery scans an open frame until the first record that fails validation, which is only sound if stale bytes cannot validate.
- **Misdirected writes are caught**, in the same way `frame_idx` catches them at header granularity.

The cost is that an eviction copy is payload-identical to its original but not byte-identical, since the hash is recomputed under the destination frame's `frame_seq`. See §10.

The hash **field** is a fixed width regardless of `checksum_id`, wide enough for the largest algorithm under consideration, so the algorithm can change without changing any record size. crc32c (§14) occupies part of it and the remainder is zero-padded. Per-record overhead is therefore constant: header plus hash field, independent of schema.

Maximum record size is derived, not chosen:

```
max_record = 65536 − 64 (header) − 64 (seal) = 65,408 bytes
```

The schema validator rejects any schema whose computed record size exceeds it. Records per frame is `floor(65408 / record_size)`; the remainder is dead space in every frame of that table.

## 6. Schema catalog, `table_id = 0`

Nothing lives outside the file, so schemas are records in table 0, read by the same path as any other table.

**Records are fixed-size**: 6,400 bytes of descriptors plus the constant per-record overhead, giving roughly 10 per frame. There is no variable-length record path anywhere in the engine.

**Field descriptors are 64 bytes, 100 maximum:**

| Bytes | Field |
|---|---|
| 60 | key name. UTF-8, null-padded, 60 *bytes* not characters |
| 1 | type, including a nullable bit |
| 3 | declared byte length |

The descriptor maximum and the record header's null bitmap width must agree: the bitmap needs a bit per possible field, so whichever is chosen first constrains the other.

The declared length is **authoritative for var-capacity types** (string, binary) and **derived for fixed-width types**. The type byte selects which rule applies, and load-time validation rejects a mismatch: a `u64` declaring anything but 8 is a corrupt record.

**Types (v1).** Five value types. Null is not a sixth type. It rides the existing nullable bit plus the record's null bitmap (§5), so any type can be nullable.

| Type | Storage width | Declared length |
|---|---|---|
| `uint` | 8 B | derived (must declare 8) |
| `int` | 8 B | derived (must declare 8) |
| `float` | 8 B | derived (must declare 8) |
| `str` | capacity + prefix (§7) | authoritative = capacity |
| `byte` | capacity + prefix (§7) | authoritative = capacity |

`str` and `byte` share encoding (capacity-tiered length prefix, no padding). `str` additionally requires UTF-8 validation on write.

**Insert-time validation.** A schema is checked before it is appended, not after. An invalid schema is never persisted, and no `write_seq` or `frame_seq` is consumed for a rejected candidate:

```
field_storage_size = 8                        for uint / int / float
                    = capacity + prefix(capacity)   for str / byte

record_size = record_header + Σ field_storage_size + hash_field
reject if record_size > 65,408
```

This is a single summed check, not a per-field cap. One `str`/`byte` column may legally claim up to the full remaining budget, but any field added after that fails the schema. Order of descriptors doesn't affect the check, only later offset computation.

Table 0's own schema is hardcoded, which is nearly free: the format is the schema.

Table 0 is an ordinary table. Its frames are subject to eviction and reuse like any other, and it holds no reserved position in the file. Recovery finds it by `table_id`, never by offset.

## 7. Variable-capacity types

String and binary columns declare a fixed capacity, paid per row. Capacity is not occupancy, so each slot carries an in-slot length prefix sized from the declared capacity:

| Declared capacity | Prefix |
|---|---|
| ≤ 255 | 1 B |
| ≤ 65,535 | 2 B |
| larger | 3 B |

Resolved once at schema load. Padding is not used, because binary may legitimately contain zero bytes. Field offsets remain statically known.

## 8. Transactions and visibility

A transaction is N records appended and made durable with one fsync. `txn_id` is the `write_seq` of the transaction's last record; `txn_cnt` is N.

A transaction is committed iff every `write_seq` in `[txn_id − txn_cnt + 1, txn_id]` is present and hash-valid. Because the writer is single and fsyncs are serial, only the final transaction in the file can be incomplete, so this check runs against the tail alone.

A reader takes a snapshot equal to the highest committed `txn_id`. A record is visible iff its `txn_id ≤ snapshot`. The effective value for a PK is its highest visible `write_seq`, absent if that version is a tombstone. Filtering on `txn_id` rather than `write_seq` is what keeps transactions atomic for readers.

## 9. Compaction

By eviction, performed by the writer thread. The victim is the oldest frame by `min_write_seq` that contains any garbage. Only sealed frames are candidates, which is also when `min_write_seq` becomes known. Live records are copied, **retaining their original `write_seq`**, into an eviction frame, which is sealed when full.

Triggered after a transaction completes, if that transaction opened a new frame, or if the free list has fallen below its low-water mark. Running after commit rather than at frame-open means sealed and committed coincide by construction.

**Freeing is derived, not marked.** A frame with no live records, after duplicate resolution (§10) and below the minimum active reader's watermark, is free. Nothing on disk says so. Freeness is a conclusion drawn from the frame's contents, and it holds whether or not the release below has run.

**Release zeroes the frame.** On joining the free list, the victim is zeroed with `FALLOC_FL_ZERO_RANGE` (§2). A released frame is then byte-identical to a never-used one, so preallocation and reclamation produce one uniform pool that recovery recognises with a single test.

This is what keeps deleted data from remaining readable in the file. Two limits on that claim, both stated in §13: a deleted row's older versions are erased only when age-ordered eviction reaches their frames, which is eventual and unscheduled; and zeroing a logical range is not physical erasure on a device with an FTL or a copy-on-write allocator.

Ordering: the eviction frame must be sealed and durable before the victim is released. The zeroing itself needs no fsync and is not on the commit path. If it is lost to a crash, recovery either finds the frame free and re-zeroes it (§10), or finds it already reused, in which case the salt has made the leftovers unreadable anyway. Eviction adds no fsync of its own.

**Invariant, no minimum-garbage threshold.** Age-ordering is what guarantees a masked version is collected before the tombstone masking it. Any threshold permits a tombstone to be dropped while an older version survives, resurrecting a deleted row.

**Invariant, victim selection uses `min_write_seq`, never `frame_seq`.** An eviction frame is newly allocated but full of old records: high `frame_seq`, low `min_write_seq`. Selecting on allocation order would make eviction frames look young and exempt them from re-collection, breaking the ordering the invariant above depends on.

**Invariant, superseded schema records are not garbage.** A sealed frame written under `table_version` v is unreadable without v, so a table 0 version is collectable only when no frame references it. This is a min-referenced-version watermark per table, mechanically parallel to the reader watermark.

## 10. Recovery

Full file scan on startup, clean or otherwise.

**Pass 1, frame headers.** Stride the file at 64 kB from offset 65536 **to end of file**, not to the first invalid frame: free frames are zero-holes scattered through the file, not confined to the tail. Classify each frame:

- **Zeroed** (magic absent). Never used, or used and released. The two are indistinguishable by construction (§9) and need not be distinguished. Straight to the free list.
- **Headed.** Record the `frame_seq`, `table_id`, `table_version`, and whether a valid seal is present.

Collect table 0 frames and build the catalog. Set `next_frame_seq = max(frame_seq) + 1`.

**Pass 2, records.** Verify hashes using the record sizes the catalog now provides and each frame's `frame_seq` as salt (§5). Sealed frames carry a record count; open frames are scanned until the first record that fails validation, which is the append point. Build PK version chains, per-frame live-byte counters and the zone map. Discard the tail transaction if incomplete.

**Repair sweep.** Pass 1 finds free frames by their zeroing, which covers every frame released cleanly. It misses one case: a crash between a frame becoming free and its zeroing landing. Such a frame is headed but holds no live records, which pass 2 detects, since liveness is a property of the whole file and cannot be judged earlier. Each is zeroed and joins the free list.

This is a repair path, not the normal one. It is bounded by the victims of a single eviction cycle, so an ordinary startup finds nothing to do here.

**Duplicate resolution.** A crash between an eviction frame being sealed and the victim being released leaves two records sharing one `write_seq`. **Highest `frame_seq` wins.** Payloads are identical, the copy differing only in its hash, recomputed under the destination's salt, so the choice is never a correctness question. Only space reclamation depends on it, and resolving it here is what allows freeing to be derived rather than marked. It generalizes without special cases: a record evicted A→B→C resolves to C.

Stale records left in a reused frame are *not* duplicates and never reach this rule. They fail validation in pass 2 and are never admitted.

## 11. State held only in memory

Catalog, zone map, free list, PK index (version chains per key), per-frame live-byte counters, reader watermarks, commit watermark, next `write_seq`, next `frame_seq`.

Nothing derived is persisted, so nothing derived can disagree with the frames.

## 12. Guarantees

- **Atomicity** from transaction-extent completeness, with only the tail able to tear.
- **Consistency** of PK uniqueness via the in-memory index.
- **Isolation** is snapshot isolation. Since all writes are serial, read-only transactions are serializable.
- **Durability** from one fsync per transaction. The superblock is not on the commit path.

## 13. Accepted limits

- All readers live in the writer's process, so the reader watermark and PK index have no durable home.
- Resident memory scales with row count.
- Reads are full table scans; no indices.
- Startup time scales with file size.
- A long-lived reader pins frames, bounding reclamation and inflating file size.
- Write amplification under threshold-free age-ordered eviction is untuned.
- **Declared capacity is paid per row regardless of occupancy.** A `binary(1MB)` column makes every row 1 MB whether it holds one byte or a megabyte. This is the price of having no overflow pages.
- **Internal fragmentation is bounded by `record_size / 65408`.** Rows under ~4 kB waste under 6%; a 32,705-byte record fits once and strands half of every frame.
- **Narrow rows pay heavily in metadata.** Per-record overhead is constant, so for a payload of a few tens of bytes it approaches or exceeds the payload itself.
- **Erasure is eventual, not prompt.** A deleted row's older versions stay readable in the file until age-ordered eviction reaches their frames. There is no delete-on-demand path and no bound on the delay.
- **Zeroing is logical, not physical.** On an SSD or a copy-on-write filesystem the prior contents may persist on the medium after the range reads as zeros. The guarantee is that the data is unreachable through the file, not that it is unrecoverable from the device. Encryption at rest is the answer to the latter and is out of scope here.
- **The file never shrinks.** Space is reused, never returned to the filesystem (§2).

## 14. Not in this draft

Indices. Group commit. Variable frame sizes. Cross-process readers. Any persisted derived state or checkpointing: recovery always full-scans, and a checkpoint is the thing that would make the superblock mutable again. Checksum selection is settled by picking the fastest available (crc32c) and revisiting under measurement.

## 15. What this draft exists to measure

Four claims above are hypotheses. Each, if wrong, forces a redesign rather than a tweak, so instrument them before refining anything at field level.

1. **Write amplification** under threshold-free age-ordered eviction. This is the mechanism protecting the tombstone-ordering invariant; if its cost is intolerable, that invariant needs a different mechanism entirely. Instrument the copy cost and the release-zeroing cost separately: the first scales with live bytes in the victim, the second is flat per reclaim, and only the first speaks to the invariant.
2. **Startup time** against file size. If a few GB takes minutes, "no persisted derived state" collapses and checkpointing returns, with the mutable superblock and cache-versus-truth problems it brings.
3. **Resident memory** per row. The PK index and version chains have no durable home, so this sets the maximum viable database size.
4. **Whether reclaimed space actually returns.** Now that the free list is derived from the scan rather than marked on disk, this is the least-proven claim in the design and the cheapest to test: under a steady insert/delete workload, does file size plateau or climb without bound?
