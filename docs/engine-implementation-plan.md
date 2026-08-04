# Storage Engine: Implementation Plan (Unit-Level)

## Overall goal

Build a single-file, single-writer, log-structured storage engine (draft 1) far enough to test four specific hypotheses about it under real crash conditions: write amplification, startup time, resident memory, and whether reclaimed space actually gets reused. This is explicitly a throwaway-format prototype, with no forward compatibility and no migration path. The point isn't to ship a database; it's to validate or falsify the architecture cheaply before investing in a stable format.

Two of those four hypotheses (startup time, resident memory) can be given a cheap early read *before* the engine exists. That work is pulled out into milestone S below and gates the rest of the plan.

## Design overview

**Physical layout.** The file is a superblock (first 64 kB, written once, never rewritten) followed by fixed-size 64 kB frames. Frame `idx` (0-based) lives at byte offset `(idx + 1) × 65536`. Frame location is pure arithmetic; no index is needed to find frame N. The file grows in 20-frame chunks via `fallocate`, and once a frame is allocated it stays allocated for the file's life. Nothing is ever returned to the filesystem.

**Frames** are 128 bytes of header followed by records. A frame is in one of four states:

| State | On-disk appearance | Meaning |
|---|---|---|
| Free | all zeros | available for allocation |
| Open | valid header, no seal | appendable, belongs to one table |
| Sealed | valid header + valid seal | immutable, eligible for eviction |
| Undecodable | non-zero, header fails to parse or fails its hash | crash residue; reclaimed by the repair sweep |

Undecodable is a first-class state, not an error. It is what a torn frame-header write looks like on reload, and recovery must reclaim it rather than reject the file.

Each frame carries a monotonic `frame_seq` that is never reused. This single property is what makes stale leftover bytes in a reused frame self-invalidating, and what lets recovery derive frame liveness instead of tracking it explicitly.

**Sentinel zero.** `write_seq` and `frame_seq` both start at 1; zero is reserved as "invalid" for both. This makes "an all-zero region never decodes as a live record" a structural guarantee rather than a probabilistic property of the hash, which matters because the open-frame append point is found by scanning to the first invalid record. `frame_idx` remains 0-based and is unaffected.

**Records** are fixed-size per table (schema-determined) and laid out `[header, data, hash]`. The header carries `write_seq`, `txn_id`, `txn_cnt`, flags including tombstone, and a null bitmap whose width caps the field count. The hash is appended after the data, not carried in the header, and covers `frame_seq ‖ header ‖ data`. Field widths and the resulting header and hash-field sizes are an implementation choice; only their being fixed and uniform matters here. `record_size` is header plus payload plus hash field, and is capped at 65,408 bytes (65,536 minus frame header and seal). crc32c occupies part of the hash field, with the remainder zeroed and reserved for a wider hash later.

There's no WAL. The data file is the log.

**Transactions and commit.** A transaction is N records sharing a `txn_id` (the `write_seq` of the last record) plus one fsync. Records are written in `write_seq` order, and the record whose `write_seq == txn_id` is written last. A transaction is committed iff all `txn_cnt` records bearing that `txn_id` are present and hash-valid.

Two consequences the earlier revision glossed over:

- **A transaction can span frames.** It may touch two tables (two open frames), or fill and seal a frame mid-transaction. Completeness is therefore evaluated *per transaction across all open frames*, not per frame. Only transactions above the highest confirmed committed `txn_id` need checking, but "the tail" is plural.
- **Sealing is not a commit signal.** A sealed frame can contain the prefix of a transaction that never committed. Recovery must exclude those records from visibility, and eviction must never copy records above the committed watermark.

**Frame-header durability ordering.** A frame header is written before any record is appended to that frame, within the same fsync barrier. A durable record therefore implies a durable header for its frame. This is what makes it safe for recovery to derive `next_frame_seq` from only the *decodable* frames: an undecodable header was never durable, so no durable record was ever salted with its `frame_seq`, so reusing that value after zeroing the frame cannot resurrect anything.

**Everything derivable is kept out of the superblock.** Catalog, free list, PK index and watermarks are rebuilt in memory from a full scan on every startup, in two passes: classify frames, then verify records and build indexes. This is deliberate. "Nothing persisted can disagree with the frames" is the core safety property, bought at the cost of scan-bound startup time (hypothesis #2).

Pass 2 runs in one of two verification modes: **trusting** (sealed frames' record counts and seal hashes are believed; records inside are not re-hashed) or **paranoid** (every record hashed). Trusting is the default. Paranoid exists both as a debugging tool and because the delta between them is the main tuning knob if hypothesis #2 comes back badly.

**Deletion and compaction are decoupled.** Deletes are tombstones; space is reclaimed later by an eviction process that copies live records out of the oldest garbage-containing frame and zeroes the original. Age-ordering (oldest `min_write_seq` first) is what guarantees a tombstone is never outlived by the version it masks. This is the single invariant the whole compaction design is built to protect, and it is also hypothesis #1 (write amplification) and #4 (does space actually get reused).

**Concurrency model is intentionally narrow**: one writer, multiple in-process readers, snapshot isolation via a global `write_seq` watermark.

**Out of scope for draft 1**, explicitly: cross-process readers, indices beyond the PK, group commit, and **schema evolution**. The `table_version` field stays in the frame header (fixed layout, free to carry) but is always 1 in draft 1, and nothing in this plan reads it. Evolution is a new milestone, not a clause bolted onto 7.7.

## How to use this plan

Each unit below produces one concrete, checkable artifact. Units within a milestone are meant to be done in order; milestones are ordered by dependency, not by calendar time.

---

## S: Pre-Build Feasibility Spikes

Throwaway code, deleted after. Nothing here shares an interface with M0+. The purpose is an early read on the two hypotheses most likely to kill the architecture, before building the engine that would test them properly.

### S.1 Synthetic file generator

**Artifact:** Standalone script producing a file of the intended shape at arbitrary size (64 kB frames, plausible headers, plausible fixed-size records with crc32c fields) with no engine behind it.
**Acceptance:** Generates 100 MB / 1 GB / 10 GB files; frame and record layout match the design overview (128 bytes of frame header and seal, plus whatever record header and hash-field sizes the implementation has settled on).

### S.2 Scan-rate spike (early read on hypothesis #2)

**Artifact:** Program that strides a synthetic file frame by frame and measures wall-clock time, in both verification modes (headers only, vs. crc32c over every record), on cold cache.
**Acceptance:** A seconds-per-GB figure for each mode at each file size, on the real target device, with cache-drop procedure documented.

### S.3 Index memory spike (early read on hypothesis #3)

**Artifact:** Program that builds the intended in-memory structure (PK to ordered version chain) at 1M / 10M / 100M rows with representative PK sizes, and measures RSS.
**Acceptance:** A bytes-per-row figure, and the row count at which the structure exceeds a stated memory budget.

### S.4 Go / no-go note

**Artifact:** A short written judgment against thresholds stated *before* S.2 and S.3 were run.
**Acceptance:** Thresholds written down first; the note states plainly whether the numbers clear them, and if not, which design change is on the table (persisted catalog, on-disk PK index, smaller reachable-size target). Proceeding past S.4 without meeting the thresholds requires recording why.

---

## M0: File Primitives + I/O Boundary

### 0.1 Device interface

**Artifact:** `Device` trait/interface with zero callers yet: `read_at`, `write_at`, `fsync`, `allocate(range)`, `zero_range(range)`, `size`, `sector_size`.
**Acceptance:** Compiles standalone; no other module in the codebase calls OS file I/O directly, verified by grepping for raw syscalls outside this module. `zero_range` and `sector_size` are on the interface from the start, because M7.4 needs a portable zeroing path and M2.4 needs to model sub-write tearing at sector granularity.

### 0.2 Real OS device implementation

**Artifact:** `OsDevice` implementing the trait against a real file.
**Acceptance:** Can create a file, write bytes at an offset, read them back, fsync without error, and report correct size after `allocate`. `zero_range` uses `FALLOC_FL_ZERO_RANGE` where available and falls back to explicit zero writes otherwise; both paths produce byte-identical results, verified by a test that forces the fallback. `sector_size` reports the device's actual logical block size.

### 0.3 Superblock create

**Artifact:** Function that writes a valid superblock (magic, format_version, frame_size, checksum_id, uuid, self-hash) to a fresh file.
**Acceptance:** Byte layout matches spec; self-hash validates; used space fits in first 4 kB.

### 0.4 Superblock parse + validate

**Artifact:** Parser that reads offset 0 and returns a typed superblock or a typed error.
**Acceptance:** Valid file parses correctly. Corrupted self-hash, wrong magic, and mismatched `format_version` each produce a distinct rejection; none silently succeed.

### 0.5 Frame addressing + extension

**Artifact:** `frame_offset(idx)` function and an extend-by-20-frames routine using `allocate`, not `ftruncate`.
**Acceptance:** Offsets match `(idx + 1) × 65536` for a range of indices. After extension, reading any newly allocated frame returns all zeros. Extending twice yields contiguous, non-overlapping frame ranges.

### 0.6 Extension durability behavior

**Artifact:** Test (against the real device, and later re-run on the fake one) covering a crash between `allocate` and the fsync that would make the size change durable.
**Acceptance:** Both outcomes, extension survived and extension lost, leave the file in a state startup handles cleanly: a lost extension yields a shorter file that gets re-extended, and a surviving one yields zeroed frames indistinguishable from never-used. Neither is reported as corruption.

---

## M1: Record Codec (table 0 hardcoded)

### 1.1 Record header encode/decode

**Artifact:** Struct plus encode/decode for the record header (`write_seq`, `txn_id`, `txn_cnt`, flags, null bitmap). Field widths and padding are chosen here and become the reference for every later unit; the hash is not part of the header.
**Acceptance:** Round-trip encode to decode is lossless for boundary values (1, max value of each field's type, all-null bitmap, tombstone flag set). Encoded length equals the declared header size exactly, with no gaps or overlaps, and the null bitmap covers at least the schema's maximum field count.

### 1.2 Salted hash

**Artifact:** `hash(frame_seq, header, data)` using crc32c, written to the hash field appended after the data, with any unused bytes of that field zeroed.
**Acceptance:** Same payload under different `frame_seq` produces different hash; same payload plus same `frame_seq` is deterministic; a single-bit flip in any input changes the hash. The salt is **prepended to the message**, not XORed into the CRC register or combined algebraically with the result: crc32c is linear, and an algebraic combine would let a forged or stale record be made to validate under a chosen salt. A test asserts the prepend construction by checking that `hash(f1, m)` cannot be derived from `hash(f2, m)` by CRC's combine operation.

### 1.3 Reserved-zero sentinels

**Artifact:** Enforcement that `write_seq ≥ 1` and `frame_seq ≥ 1` at allocation, plus a decode-time precondition that rejects a record whose `write_seq` is 0 before the hash is even computed.
**Acceptance:** A fully-zeroed record slot is rejected as invalid, structurally and deterministically, not because the hash happened to mismatch. A fully-zeroed frame header is likewise rejected on `frame_seq == 0`. This is the property M4.2's append-point scan depends on, so it is tested here rather than assumed there.

### 1.4 Table 0 hardcoded schema + record encode/decode

**Artifact:** Fixed struct definition matching table 0's schema, with full record encode/decode (header, data, hash).
**Acceptance:** A table-0 record can be built, encoded to bytes, decoded back, and hash-verified against a stored `frame_seq`, all in memory, with no disk involved yet.

### 1.5 Round-trip test suite

**Artifact:** Test file covering encode/decode/hash for table 0 across edge cases (empty strings if applicable, max field values, corrupted single byte producing a hash mismatch, all-zero slot rejected per 1.3).
**Acceptance:** All tests pass; a deliberately corrupted record is reliably rejected by hash check.

---

## M2: Frame Lifecycle + Fake Device v1

### 2.1 Frame header open

**Artifact:** Function that writes a 128-byte frame header (magic, `frame_seq`, `frame_idx`, `table_id`, `table_version`, header hash) when a frame is opened for writing.
**Acceptance:** Written header round-trips through parse; `frame_seq` is strictly increasing across successive opens in a test run and starts at 1; `table_version` is 1. The header is written before any record is appended to the frame (the ordering rule from the design overview), verified by write-order instrumentation.

### 2.2 Frame seal

**Artifact:** Function that writes the seal (`min_write_seq`, `max_write_seq`, record count, hash) when a frame fills.
**Acceptance:** Seal is written only when the frame has no room for another record of the table's `record_size`; seal hash validates; sealed frame is subsequently treated as immutable by the write path. A test asserts explicitly that sealing does **not** mark the contained records committed: a frame sealed mid-transaction is sealed with an uncommitted prefix inside, and the seal records that state without editorializing.

### 2.3 Free-list vs. extend allocation

**Artifact:** Allocator that returns either a free frame index or triggers file extension (M0.5) when the free list is empty.
**Acceptance:** Given a file with N free frames, N allocations succeed without extension; the (N+1)th triggers exactly one extension of 20 frames.

### 2.4 Fake device v1

**Artifact:** In-memory `Device` implementation modeling realistic writeback, not all-or-nothing buffering. It must be able to persist, on `crash()`: an arbitrary *subset* of unfsynced writes, and an arbitrary *prefix or sector-aligned fragment* of any individual write. Ordering between unfsynced writes is not guaranteed. Anything before a completed fsync is durable in full.
**Acceptance:** Four behaviors are independently demonstrable: (a) unfsynced write fully dropped, (b) unfsynced write fully persisted, (c) unfsynced write persisted *torn* at a sector boundary partway through, (d) two unfsynced writes persisted out of order. Post-fsync writes always survive `crash()`.

This is the load-bearing unit of the whole test strategy. An all-or-nothing crash model would make M2.5, M4.4 and most of M8 pass without testing anything, because the torn states they exist to catch would be unreachable.

### 2.5 First crash test: torn frame header

**Artifact:** Test using the fake device that opens a frame, crashes with the header write torn mid-sector (per 2.4c), and asserts the reopened file classifies the frame correctly.
**Acceptance:** Test passes deterministically under a fixed seed. The frame is classified as **undecodable**, not free and not a fatal parse error, and is subsequently zeroed by the repair sweep (M4.4), after which it is indistinguishable from never-used. A second variant where the header write is dropped entirely yields a frame classified as free.

---

## M3: Transactions + Crash-Point Injection

### 3.1 txn_id / txn_cnt assignment

**Artifact:** Append path that assigns `write_seq` globally starting at 1, and stamps every record in a batch with the batch's final `write_seq` as `txn_id` and the batch size as `txn_cnt`.
**Acceptance:** A 5-record transaction produces 5 records sharing one `txn_id`, each with `txn_cnt = 5`; a subsequent single-record transaction gets a new, higher `txn_id`.

### 3.2 Single fsync per transaction

**Artifact:** Batching logic that writes all records of a transaction, then issues exactly one fsync.
**Acceptance:** Instrumented fsync-call counter shows exactly 1 call per transaction regardless of record count, table count, or frame-boundary crossings within that transaction.

### 3.3 Commit-record write ordering across frames

**Artifact:** Explicit rule, enforced in the append path, that records within a transaction are issued in ascending `write_seq` order and that the record carrying `write_seq == txn_id` is issued last, regardless of which frame it lands in.
**Acceptance:** A transaction spanning two tables (two open frames) and a transaction that fills and seals a frame partway through both issue their commit-carrying record last, verified by write-order instrumentation. A test documents which frame that record landed in for each case, since recovery must not assume it's any particular one.

### 3.4 Named injection points

**Artifact:** Fake device extended with labeled injection points (e.g. `before_fsync`, `mid_write`, `after_write_before_fsync`) that a test can target by name, composable with the tearing modes from 2.4.
**Acceptance:** A test can request "crash at `mid_write` on the 3rd record of this transaction, torn at the first sector boundary" and the simulator reliably stops there, reproducibly.

### 3.5 Seeded RNG reproducibility

**Artifact:** Simulator wrapper that takes a seed and derives all randomized injection decisions from it, including which writes persist, where they tear, and in what order.
**Acceptance:** Two runs with the same seed produce byte-identical crash points and byte-identical resulting file state.

### 3.6 Torn-tail test: transaction genuinely incomplete

**Artifact:** Test that writes a multi-record transaction and crashes with the **final record torn** (partially persisted), then reopens and runs the completeness check.
**Acceptance:** The completeness check identifies the transaction as uncommitted, because not all `txn_cnt` records bearing that `txn_id` are present and hash-valid. All of its records, including the intact earlier ones, are excluded from visibility.

### 3.7 Landed-but-unfsynced test: transaction legitimately committable

**Artifact:** Test that writes a multi-record transaction where every record persists intact but the crash lands *before* the fsync returns.
**Acceptance:** The completeness check accepts the transaction as committed and its records are visible. This is correct behavior, not a bug: the commit rule is presence-and-validity of all `txn_cnt` records, and a lost fsync that happened to persist everything satisfies it. The test exists specifically to stop someone "fixing" 3.6 by adding a durability signal the design deliberately doesn't have.

---

## M4: Recovery + Simulator-Driven Development

### 4.1 Pass 1: frame classification + catalog

**Artifact:** Full-file stride from offset 65536 to EOF, classifying each frame as free / open / sealed / undecodable, building the table-0 frame list and `next_frame_seq`.
**Acceptance:** On a file with a mix of all four states (constructed via fake device), pass 1 classifies every frame correctly. `next_frame_seq = max(frame_seq over decodable frames) + 1`; a test constructs the case where an undecodable frame's lost header held a higher `frame_seq` and asserts that reusing that value is safe, because the repair sweep zeroes the frame first and no durable record could have been salted with it (per the durability-ordering invariant).

### 4.2 Pass 2: hash verification + append point

**Artifact:** Per-frame record scan using catalog-provided record sizes and the frame's `frame_seq` as salt, with a mode flag: **trusting** (sealed frames' counts believed, contents not re-hashed) or **paranoid** (every record hashed). Open frames are always scanned to the first invalid record.
**Acceptance:** On a sealed frame, all records validate in paranoid mode and the count matches the seal. On an open frame with a valid prefix and garbage tail, the detected append point matches the last valid record's end exactly. Both modes are selectable at startup and produce identical *logical* results on an uncorrupted file; the difference is only cost, which is what M9.2 measures.

### 4.3 Cross-frame transaction reassembly

**Artifact:** Completeness evaluation that gathers candidate tail transactions from *all* open frames plus the sealed frames holding their prefixes, and decides commit per `txn_id` rather than per frame.
**Acceptance:** Three scenarios pass. (a) A transaction split across two tables' open frames, with the tail torn in one of them, is correctly rejected as a whole. (b) A transaction whose prefix sits in a **sealed** frame and whose tail is torn in an open frame is rejected as a whole, and the sealed-frame prefix records are excluded from visibility. (c) A transaction split across frames that landed completely is accepted. Scenario (b) is the one that falsifies "sealed means committed."

### 4.4 Repair sweep

**Artifact:** Post-pass-2 step that zeroes (i) headed frames with zero live records and (ii) all frames classified undecodable in pass 1.
**Acceptance:** Two simulator scenarios, a crash between "frame has no live records" and "zeroing lands", and the torn-header case from M2.5, each reliably produce a frame that the repair sweep catches and zeroes, after which pass 1 on a re-run classifies it as free. A normal clean-shutdown run finds nothing to repair.

### 4.5 Duplicate resolution

**Artifact:** Logic that, when two records share a `write_seq`, keeps the one with the higher `frame_seq`.
**Acceptance:** Simulator scenario (eviction frame sealed but victim not yet released, then crash) produces two live copies of one `write_seq`; recovery keeps exactly one, and it's the higher-`frame_seq` copy. An A→B→C eviction chain resolves to C.

### 4.6 Scenario-per-invariant test suite

**Artifact:** One simulator test per named invariant, each explicitly cross-referenced to the invariant it checks. At minimum: stale bytes in a reused frame are unreadable; a misdirected write is caught by `frame_idx`; an all-zero slot never decodes as a live record (1.3); a durable record implies a durable frame header (4.1); sealing does not imply commit (4.3b); `frame_seq` is never reused while any record salted with it is durable.
**Acceptance:** Every named invariant has at least one corresponding test; all pass; and for each, removing the mechanism the invariant depends on, as a deliberate source mutation, makes that test fail. A test that still passes under its mutation is not testing its invariant.

### 4.7 Full recovery integration test

**Artifact:** Kill-and-restart loop: run a workload against the fake device, crash at a randomized point with randomized tearing, reopen, run full recovery (passes 1+2, cross-frame reassembly, repair, dup resolution), assert internal consistency.
**Acceptance:** N iterations (e.g. 500) with different seeds all complete with no invariant violation and no panic; a summary report of crash points and tearing modes exercised is produced.

---

## M5: MVCC Read Path

### 5.1 write_seq counter + commit watermark

**Artifact:** In-memory global `write_seq` counter and "highest committed `txn_id`" watermark, initialized from recovery output.
**Acceptance:** After recovery on a file with a torn tail (M3.6), the watermark reflects the last *complete* transaction, not the torn one. After recovery on the M3.7 file, it includes the unfsynced-but-intact transaction.

### 5.2 Reader snapshot creation

**Artifact:** API that hands a reader the current commit watermark as its snapshot value.
**Acceptance:** Two readers started before and after a commit get different snapshot values; a snapshot value never changes once taken.

### 5.3 Visibility filter

**Artifact:** Function `visible(record, snapshot)` returning true iff `record.txn_id <= snapshot`, plus effective-value resolution (highest visible `write_seq` per PK, absent if tombstoned).
**Acceptance:** Given a PK with 3 versions across different `txn_id`s including a tombstone, each of 3 different snapshot values returns the correct effective value, including "not found" where the tombstone is the visible version.

### 5.4 PK version chain index

**Artifact:** In-memory index mapping PK to an ordered list of `write_seq` versions, built during recovery pass 2.
**Acceptance:** Built index matches a hand-verified expected chain for a small fixture file with inserts, updates and deletes on the same PK. Records belonging to transactions rejected by 4.3 do not appear in any chain.

### 5.5 Concurrent reader/writer simulator scenario

**Artifact:** Simulator test: writer commits a transaction while a reader holds a snapshot taken mid-write.
**Acceptance:** Reader never observes a partial transaction (either sees none of its records or all of them, never some), including when the transaction spans frames.

---

## M6: General Schema Support

### 6.1 Field descriptor encode/decode

**Artifact:** 64-byte field descriptor struct (name, type plus nullable bit, declared length) with encode/decode.
**Acceptance:** Round-trips for all five types; name field correctly null-pads/truncates at 60 bytes.

### 6.2 Five types + null bitmap wiring

**Artifact:** Type-aware encode/decode for uint/int/float/str/byte wired into the generic record codec (replacing the M1.4 hardcoded version), using the header's null bitmap.
**Acceptance:** A record with a mix of null and non-null fields across all five types round-trips correctly; reading a null field returns the correct "absent" representation without reading garbage bytes; a schema declaring more fields than the bitmap has bits is rejected.

### 6.3 Var-capacity prefix encoding

**Artifact:** Capacity-tiered length-prefix encode/decode for str/byte (1/2/3-byte prefix by declared capacity).
**Acceptance:** A str field with a short actual value inside a large declared capacity round-trips with the correct prefix size and no padding bytes written; binary data containing zero bytes round-trips exactly.

### 6.4 Insert-time schema validation

**Artifact:** Validator computing `record_size` (header plus payload plus hash field) from field descriptors and rejecting schemas exceeding 65,408 bytes, before any `write_seq` or `frame_seq` is consumed.
**Acceptance:** A schema at exactly 65,408 is accepted; one byte over is rejected; a rejected schema leaves `next_write_seq` and `next_frame_seq` unchanged, verified by counter inspection.

### 6.5 Real schema catalog migration

**Artifact:** Table 0 now populated with real schema records (via 6.1 to 6.4) instead of the M1.4 hardcoded struct; all table creation goes through this path.
**Acceptance:** A newly created user table's schema is recoverable purely from scanning table 0 after a fresh recovery run, with no in-code hardcoding involved for that table.

### 6.6 Re-run crash scenarios against schema-driven tables

**Artifact:** The M3.6, M3.7, M4.3, M4.6 and M4.7 suites re-parameterized to run against at least one non-table-0, schema-driven table with a different `record_size`.
**Acceptance:** All previously-passing crash scenarios still pass with a realistic user schema (mixed fixed and var-capacity fields), not just table 0's original hardcoded layout. In particular, 4.3's cross-frame scenarios run with two tables of *different* record sizes, since that's the case where per-frame stride assumptions are most likely to be wrong.

---

## M7: Compaction / Eviction

### 7.1 min_write_seq tracking

**Artifact:** Seal logic updated to record `min_write_seq` correctly as records are appended to a frame.
**Acceptance:** For a frame containing records with `write_seq` values [12, 15, 20], the seal's `min_write_seq` is 12 regardless of insertion order.

### 7.2 Victim selection

**Artifact:** Function selecting the sealed frame with the lowest `min_write_seq` among frames containing garbage.
**Acceptance:** Given a set of sealed frames with known `min_write_seq` and known live/garbage status, selection picks the correct victim; a frame with high `frame_seq` but low `min_write_seq` is still correctly selected, not skipped due to recency.

### 7.3 Eviction copy

**Artifact:** Logic copying live records from a victim into an eviction frame, preserving the original `write_seq`, recomputing the hash under the destination `frame_seq`.
**Acceptance:** Post-copy, the record's payload is byte-identical to the original but the hash differs and validates under the new frame's salt; the original `write_seq` is unchanged. Records with `txn_id` above the commit watermark are **not** copied: they are uncommitted, and copying them would launder an uncommitted transaction prefix into a fresh frame. A test constructs a victim containing such a prefix (via the 4.3b path) and asserts it is dropped, not copied.

### 7.4 Release + zero

**Artifact:** Victim release via the device's `zero_range` (M0.1/0.2) after the eviction frame is sealed and durable.
**Acceptance:** After release, the victim frame reads as all zeros and is indistinguishable, per pass 1 classification, from a never-used frame. Both the `FALLOC_FL_ZERO_RANGE` path and the explicit-write fallback are exercised and produce identical classification results.

### 7.5 Trigger logic

**Artifact:** Eviction trigger wired to: transaction completion that opened a new frame, or free list below low-water mark. Eviction never runs mid-transaction.
**Acceptance:** In a controlled test workload, eviction fires on exactly the expected transactions and not on others (instrumented counter matches hand-computed expectation), and never fires while a transaction is in flight.

### 7.6 Simulator: eviction-specific injection points

**Artifact:** Fake device injection points for "after eviction-seal, before victim release" and "mid-zeroing", the latter using the sector-tearing mode from 2.4 so a partially-zeroed frame is reachable.
**Acceptance:** Both scenarios are independently triggerable and reproducible under a seed. The first feeds M4.5 (duplicate resolution) and passes. The second produces a frame that is neither fully zeroed nor a valid header, i.e. undecodable, which M4.4's repair sweep must reclaim; that test passes too.

### 7.7 Reader watermark tracking

**Artifact:** Reader-minimum watermark, consulted before a frame is deemed collectible.
**Acceptance:** A frame containing a record visible to a still-open reader snapshot is never selected as a victim. (The `table_version` half of this unit is cut: schema evolution is out of scope for draft 1 and nothing produces a second version.)

---

## M8: Crash-Injection Hardening / Fuzzing

### 8.1 Randomized injection harness

**Artifact:** Driver that runs a randomized workload (mixed inserts/updates/deletes/eviction triggers, single- and multi-table transactions) against the fake device with randomized crash points and randomized tearing across all injection points established in M2 to M7.
**Acceptance:** Harness runs N iterations (target: thousands) unattended, each with a distinct seed, and reports pass/fail with the seed for any failure.

### 8.2 Failure triage tooling

**Artifact:** Minimal-repro tool that takes a failing seed and reduces the workload and crash-point sequence to the smallest one still reproducing the failure.
**Acceptance:** Given a synthetic known-bad seed, the tool produces a repro at least 10× shorter than the original failing run, and the repro still fails.

### 8.3 Fuzz run + fix loop

**Artifact:** A run log documenting each failure found, its root cause and the fix, until a target iteration count passes clean, plus a coverage histogram over injection points and tearing modes.
**Acceptance:** Two conditions, both required: (i) 10,000 consecutive seeds with no invariant violation, and (ii) every named injection point and every tearing mode from 2.4 hit at least K times (K stated in advance) across that run. Condition (ii) exists because (i) alone is vacuously satisfiable by a harness that stopped reaching the interesting states.

---

## M9: Instrument the Four Hypotheses

### 9.1 Write amplification

**Artifact:** Instrumentation separating eviction-copy bytes from release-zeroing cost, run against a real device under a sustained insert/delete workload.
**Acceptance:** Report showing bytes-copied vs. live-bytes-in-victim ratio over time, with copy cost and zeroing cost reported as separate line items.

### 9.2 Startup time vs. file size

**Artifact:** Benchmark harness measuring cold-start recovery time across a range of file sizes (100 MB / 1 GB / 10 GB), in **both** verification modes from 4.2.
**Acceptance:** Report plotting recovery time against file size for each mode, with an explicit pass/fail judgment against the tolerance stated in S.4 (e.g. "under N seconds per GB"). The trusting-vs-paranoid delta is reported as its own figure, since it is the tuning knob if the answer is bad. Results are compared against the S.2 spike numbers, and any large divergence is explained rather than ignored: a real engine much slower than the synthetic stride means the cost is in structure-building, not I/O, which is a different problem with different fixes.

### 9.3 Resident memory vs. row count

**Artifact:** Benchmark measuring RSS growth of the PK index plus version chains across increasing row counts.
**Acceptance:** Report giving bytes-per-row overhead and a concrete maximum viable database size for a target memory budget, compared against the S.3 spike figure.

### 9.4 File-size plateau under churn

**Artifact:** Long-running test applying steady insert/delete load and tracking file size over time (hours-to-days scale, or accelerated equivalent).
**Acceptance:** Report showing whether file size plateaus or grows unbounded, with the eviction low-water-mark and trigger settings used recorded alongside the result.

---

## Cross-cutting notes

- **The fake device's fidelity is the plan's biggest single risk.** Every durability claim in M3 to M8 is only as true as M2.4's writeback model. If it can't produce torn sub-writes and reordered writes, the crash suite is theatre. Treat any weakening of 2.4 as a change to the plan's conclusions, not an implementation detail.
- **Simulator is not a milestone.** It's an interface at M0.1, gets its first real implementation at M2.4, and grows at M3.4, M4 (used throughout) and M7.6. Treat any milestone that touches durability without a corresponding simulator scenario as incomplete.
- **M4 is the critical-path bottleneck.** M5, M7 and M8 all depend on recovery being correct; don't compress it to protect the schedule.
- **S gates everything.** If S.2 or S.3 miss their thresholds badly, the fix is a design change (persisted catalog, on-disk PK index, or a smaller target size), and it is much cheaper to make before M0 than after M8.
- **M6 is the most deferrable.** It can slide after M7 if you want compaction validated against one fixed record shape first, at the cost of not exercising eviction against variable record sizes until later. This interacts with 6.6: deferring M6 also defers the different-record-size variant of the cross-frame reassembly tests, which is the riskiest thing 6.6 covers.
