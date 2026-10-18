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

**Current implementation (MVP).** A single writer thread owns one TursoDB
([`turso`](https://crates.io/crates/turso) 0.8.3) connection; writes arrive on a
bounded queue, and whatever is queued is drained into one `IMMEDIATE`
transaction (group commit). Each transaction runs inside its own `SAVEPOINT`, so one
failing transaction rolls back alone without poisoning the batch. A second thread runs
`PASSIVE` checkpoints off the commit path, switching to `TRUNCATE` once the WAL
passes 1 GiB. The connection uses `page_size = 4096`, `synchronous = NORMAL`,
WAL, and `wal_autocheckpoint = 0`.

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

Measurements on macOS (aarch64, release build, TursoDB 0.8.3) with
`synchronous = NORMAL` and `page_size = 4096`, driven by `oha`:

- `GET /noop` **offered 50 k req/s** (open-loop) — keeps up, p50 2.3 ms, p99
  19 ms. Closed-loop it tops out at ~49.7 k, p99 3.2 ms.
- `POST /tx` (one entity + one fact) **closed-loop** (`-c 128`) — **~43 k
  transactions/s**, p50 2.9 ms, p95 3.3 ms, p99 3.7 ms, 0 failures.

Open-loop, the knee is around **~42–43 k transactions/s**. The server absorbs the
offered rate with a flat p50 until it reaches the ceiling, then the fixed
offered rate outruns the writer and the queue inflates the tail while the
*achieved* rate stays pinned:

| offered /tx | achieved | p50 | p95 | p99 |
|---|---|---|---|---|
| 20 k | 20.0 k | 1.1 ms | 2.0 ms | 27 ms |
| 30 k | 30.0 k | 1.3 ms | 11.3 ms | 40 ms |
| 40 k | 40.0 k | 1.7 ms | 16.3 ms | 31 ms |
| 44 k | 43.3 k | 23 ms | 84 ms | 92 ms |
| 48 k | 43.2 k | 257 ms | 567 ms | 596 ms |

### Turso tuning notes

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
unchecked and throughput drops ~30%. The defaults are kept.

**Where the ceiling is now.** `/tx` (~43 k) sits close to `/noop` (~50 k), so
after the `RETURNING` fix the single writer is no longer dramatically ahead of
everything else — the remaining gap is the per-transaction statement work (six
statements: savepoint, `tx`, entity, `attr` lookup, fact, release) spread over
one thread. It is comfortably enough for the workloads this is aimed at, and the
planned native format's opt-in "search buckets" would raise it later without
changing the write model.

Page size is a mild knob. At `page_size = 65536` each dirty page writes a 64 KiB
WAL frame, so the WAL balloons and trips the 1 GiB `TRUNCATE` checkpoint; the
throughput gain is within noise, so 4096 stays. Automatic
checkpoint/truncation (`wal_autocheckpoint = 1000` + `journal_size_limit = 4 MB`,
custom thread off) pins the WAL but is slower and heavier-tailed; the custom
`PASSIVE` thread (TRUNCATE only past 1 GiB) is kept.

> Earlier revisions of this section reported ~27 k transactions/s and a ~23.5 k
> open-loop knee. Those numbers came from a stale `target/release/breezydb`
> left over from the libsql backend — `cargo build --release --example seed`
> builds the example, not the binary. The figures above are the actual TursoDB
> build.

## State

### Design validation

- [ ] Synthetic file generator
- [ ] Scan-rate test
- [ ] Index test
