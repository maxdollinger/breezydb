# breezyDB

!! This is a personal research project.

- plain HTTP, no client library, no driver, no connection
- nothing to tune: sensible defaults
- batteries included: Migrations, backups, and replication built in

## Write API

One request is one **transaction**: a list of ops applied atomically.

```sh
curl -X POST localhost:3000/tx -H 'content-type: application/json' -d '[
  {"op":"entity","uuid":"e1","as":"$e"},
  {"op":"add","e":"$e","a":2,"v":"Mad Max"}
]'
```

The body is a bare array of ops or `{ "ops": [...] }`. Ops:

| op | shape | effect |
|---|---|---|
| `entity` | `{uuid?, as?}` | create an entity (uuid generated if omitted), optionally bind its id |
| `add` | `{e, a, v}` | add a fact `(e, a, v)` |
| `close` | `{e, a}` | close the live value of a cardinality-one attribute |
| `invalidate` | `{e, a, v}` | invalidate a specific value of a cardinality-many attribute |
| `resolve` | `{a, v, as?}` | resolve a lookup ref via the identity attribute |

An entity ref (`e`) is a numeric id, a `"$name"` binding from an earlier `as`,
or a lookup ref `{a, v}`. The response carries the `tx` id, per-op `results`,
and the `bindings`. A failing op aborts the whole transaction with HTTP 409 and rolls
it back; other transactions are unaffected.

The data model is EAV (`concept`/`attr`/`entity`/`fact`, with `fact` versioned
by `tx_from`/`tx_to`). Concepts and attributes are seeded out of band (see the
`seed` example, `examples/seed.rs`, for the shape).

## Storage Engine

**Current implementation (MVP).** A single writer thread owns one bundled-SQLite
([`rusqlite`](https://crates.io/crates/rusqlite)) connection; writes arrive on a
bounded queue, and whatever is queued is drained into one `IMMEDIATE`
transaction (**group commit**: N breezy transactions, one WAL media-fsync). Each
transaction runs inside its own `SAVEPOINT`, so one failing transaction rolls
back alone without poisoning the batch. A second thread runs `PASSIVE`
checkpoints off the commit path, switching to `TRUNCATE` once the WAL passes
1 GiB. Every connection runs `page_size = 4096`, WAL,
`wal_autocheckpoint = 0`, and — since power-loss durability became a
requirement — `synchronous = FULL` + `fullfsync = ON` by default: a commit is
media-fsynced (`F_FULLFSYNC` on macOS) before it succeeds, and the group commit
amortizes that fsync across the batch. See "Load testing" for what it costs.

**Planned.** Replace TursoDB with the native format below:

- Single Writer, multiple readers
- No WAL, multiple append only files, with merging of sealed files
- fixed size, typed schemas

- records, get DB unique counting _seq, txn_id = _seq of first record of txn, txn_cnt, schema: _seq
- schemas written in the DB file as records with special flag

- search buckets
  - schema declares that a field should be indexed by a search bucket
  - search bucket is an index value -> _seq
  - B+Tree

- DB index for _seq -> (file_id, offset): get's created on DB server start as linearHashMap

## Load testing

The server reads `DB` (default `data/test.db`) and `PORT` (default `3000`), so it
can be pointed at a scratch file. Seed concepts/attributes out of band first —
the write path does not create them. The `seed` example opens the same file and
applies the schema plus a concept and a `name` attribute:

```sh
# build both the server and the seed helper (a bare
# `cargo build --release --example seed` builds only the example, not the bin)
cargo build --release --bin breezydb --example seed
rm -f /tmp/breezy-load.db*
./target/release/examples/seed /tmp/breezy-load.db
DB=/tmp/breezy-load.db PORT=3000 ./target/release/breezydb &
```

Load is generated with [`oha`](https://github.com/hatoo/oha). Its `-q` flag
sends requests at a fixed rate *independent of responses* and
`--latency-correction` compensates for coordinated omission, so it is a true
open-loop driver rather than a closed-loop one:

```sh
# pure server (no DB), open-loop
oha -z 10s -c 128 -q 50000 --latency-correction --no-tui \
  http://127.0.0.1:3000/noop

# DB write path: one entity + one fact per request.
# Omit `uuid` so the server generates it — keeps every entity unique from a
# static body.
BODY='{"ops":[{"op":"entity","as":"$e"},{"op":"add","e":"$e","a":2,"v":"load"}]}'

# open-loop at a fixed 40 k offered transactions/s
oha -z 10s -c 128 -q 40000 --latency-correction --no-tui \
  -m POST -T application/json -d "$BODY" http://127.0.0.1:3000/tx

# closed-loop (max throughput): drop -q and --latency-correction
oha -z 6s -c 128 --no-tui -m POST -T application/json -d "$BODY" \
  http://127.0.0.1:3000/tx
```

`localhost` resolves to IPv6 (`::1`) on macOS while the server binds IPv4 only,
so point the load generator at `127.0.0.1`.

Measurements on macOS (aarch64, release build, bundled SQLite via rusqlite,
`page_size = 4096`), driven by `oha`. **Durability is the default**: every
connection runs `synchronous = FULL` + `fullfsync = ON` — a WAL commit is
media-fsynced (`F_FULLFSYNC`, ~400 µs measured on this NVMe) before it
succeeds, so an HTTP 200 means power-loss durable, not just crash-safe
(`BREEZY_SYNCHRONOUS` / `NO_BREEZY_FULLFSYNC` are the opt-out experiments).

- **Sustained open-loop capacity (stall harness, mixed 2–20 facts per tx):**

  | offered | achieved | committed `tx` rows | p50 | p99 | flat? |
  |---|---|---|---|---|---|
  | 4000/s × 4 min | 4000/s | 960,702 (= rate × duration) | 23 ms | 443 ms | yes, 1.3 GB |
  | 4500/s × 5 min | 4500/s | 1,350,075 (= rate × duration) | 36 ms | 654 ms | yes, 1.9 GB |

  Both runs: 100% real 200s, zero failures, zero orphaned facts, WAL held at
  0 bytes by the checkpointer, throughput dead flat minute-over-minute. The
  knee is **≥ 4500 tx/s (~10.8 k facts/s)** and has not been found yet.

- Why durability is cheap here: the single writer **group-commits** — every
  queued breezy transaction shares one `IMMEDIATE` SQL transaction, so N
  transactions cost one media fsync, and each still rolls back alone inside
  its `SAVEPOINT`. Full durability costs ~20% vs `synchronous = NORMAL`
  (6.6 k → 5.3 k tx/s closed-loop burst on the mixed workload).

### Postgres comparison (same workload, same machine, equal durability)

The backend was temporarily ported to Postgres 18 (tokio-postgres + deadpool,
one SQL transaction per breezy tx) to benchmark against it, then reverted to
SQLite. Numbers under equal power-loss durability (`wal_sync_method =
fsync_writethrough` + `commit_delay = 1000`), crash-verified by `kill -9` of
the postmaster mid-load (every 200 survived WAL recovery):

| backend | sustained mixed tx/s | fsync cost model |
|---|---|---|
| **SQLite (current)** | **≥ 4500** | group commit: 1 fsync per batch |
| Postgres 18 | ~2700 (3500 offered, 4 min) | per-commit fsync, grouped via `commit_delay` |

What moved the needle on Postgres (measured): single-statement CTE per
transaction (~15 round trips → 1: 2.2 k → 3.6 k burst), unix socket vs TCP
loopback (2.9×), `commit_delay = 1000` (group commit: 2.2 k → 3.6 k), and an
in-process `attr` cache. None beat SQLite's group commit for this workload.

### Non-durable reference numbers (`synchronous = NORMAL`, old default)

Treat as opt-out experiment baselines, not defaults: `GET /noop` ~50 k req/s
(p50 2.3 ms); `POST /tx` 1-fact closed-loop **~43 k tx/s**; open-loop knee
~42–43 k on the 1-fact workload:

| offered /tx | achieved | p50 | p95 | p99 |
|---|---|---|---|---|
| 20 k | 20.0 k | 1.1 ms | 2.0 ms | 27 ms |
| 30 k | 30.0 k | 1.3 ms | 11.3 ms | 40 ms |
| 40 k | 40.0 k | 1.7 ms | 16.3 ms | 31 ms |
| 44 k | 43.3 k | 23 ms | 84 ms | 92 ms |
| 48 k | 43.2 k | 257 ms | 567 ms | 596 ms |

### Tuning notes

The single biggest knob was not a pragma: **avoid `INSERT ... RETURNING`**. On
Turso 0.8.3 the transaction path's two `INSERT ... RETURNING id` statements ran at
~2.4 k transactions/s; replacing them with plain `INSERT` + `last_insert_rowid()`
(see `WriteTx`) took the same workload to **~53 k transactions/s** storage-only and
~43 k over HTTP — roughly **25x**. No savepoint, index, or pragma change came
close. (Values are read off the writer's connection, which the pre-prepared
statement clones share.)

Everything else measured was noise or a regression (`/tx`, closed-loop):

| change | transactions/s |
|---|---|
| baseline (WAL, `synchronous=NORMAL`, `page_size=4096`, all indexes) | 41.8 k |
| `synchronous = OFF` | 41.9 k |
| `cache_size = 512 MiB` | 41.9 k |
| `page_size = 65536` | 42.8 k |
| drop the 4 secondary `fact` indexes | 42.8 k |
| **remove the checkpoint thread** | **29.7 k** |

So the pragmas do nothing here, the secondary indexes are nearly free, and the
custom `PASSIVE` checkpoint thread is worth keeping — without it the WAL grows
unchecked and throughput drops ~30%. The defaults are kept. Page size is a mild
knob: at `page_size = 65536` each dirty page writes a 64 KiB WAL frame, the WAL
balloons and trips the 1 GiB `TRUNCATE` cap, and the gain is within noise, so
4096 stays.

> Earlier revisions reported ~27 k tx/s from a stale binary left over from the
> libsql backend; and the Turso-era knee (~42–43 k, 1-fact workload,
> non-durable). Both superseded by the tables above.

## State

### Design validation

- [ ] Synthetic file generator
- [ ] Scan-rate test
- [ ] Index test
