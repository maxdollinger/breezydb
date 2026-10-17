# breezyDB

!! This is a personal research project.

- plain HTTP, no client library, no driver, no connection
- nothing to tune: sensible defaults
- batteries included: Migrations, backups, and replication built in

## Write API

One request is one **frame**: a list of ops executed in a single transaction.

```sh
curl -X POST localhost:3000/tx -H 'content-type: application/json' -d '[
  {"op":"entity","uuid":"e1","as":"$e"},
  {"op":"assert","e":"$e","a":2,"v":"Mad Max"}
]'
```

The body is a bare array of ops or `{ "ops": [...] }`. Ops:

| op | shape | effect |
|---|---|---|
| `entity` | `{uuid?, as?}` | create an entity (uuid generated if omitted), optionally bind its id |
| `assert` | `{e, a, v}` | assert a fact `(e, a, v)` |
| `close` | `{e, a}` | close the live value of a cardinality-one attribute |
| `retract` | `{e, a, v}` | retract a specific value of a cardinality-many attribute |
| `resolve` | `{a, v, as?}` | resolve a lookup ref via the identity attribute |

An entity ref (`e`) is a numeric id, a `"$name"` binding from an earlier `as`,
or a lookup ref `{a, v}`. The response carries the `tx` id, per-op `results`,
and the `bindings`. A failing op aborts the whole frame with HTTP 409 and rolls
it back; other frames are unaffected.

The data model is EAV (`concept`/`attr`/`entity`/`fact`, with `fact` versioned
by `tx_from`/`tx_to`). Concepts and attributes are seeded out of band (see
`sketch/loadtest/seed.ts` for the shape).

## Storage Engine

**Current implementation (MVP).** A single writer thread owns one libsql
connection; writes arrive on a bounded queue, and whatever is queued is drained
into one `IMMEDIATE` transaction (group commit). Each frame runs inside its own
`SAVEPOINT`, so one failing frame rolls back alone without poisoning the batch.
A second thread runs `PASSIVE` checkpoints off the commit path, switching to
`TRUNCATE` once the WAL passes 1 GiB. The connection uses `page_size = 4096`,
`synchronous = NORMAL`, WAL, and `wal_autocheckpoint = 0`.

**Planned.** Replace libsql with the native format below:

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
can be pointed at a scratch file. The sketch's zero-dependency open-loop runner
works against it unchanged (`openloop.ts` substitutes `$RANDOM` per request):

```sh
cargo build --release
DB=/tmp/breezy-load.db PORT=3000 ./target/release/breezydb &

# seed concepts/attrs (the write path does not create them)
sqlite3 /tmp/breezy-load.db "
  INSERT INTO concept (id,ident) VALUES (1,'node');
  INSERT INTO concept_ancestor (concept,ancestor) VALUES (1,1);
  INSERT INTO attr (id,ident,domain,value_type,range,card_one,is_unique,is_identity)
    VALUES (2,'name',1,'string',NULL,1,0,0);"

cd sketch
# pure server (no DB)
bun run loadtest/openloop.ts http://localhost:3000/noop 5000 15
# DB write path: one entity + one fact per request
METHOD=POST HEADER='content-type: application/json' \
  BODY='{"ops":[{"op":"entity","uuid":"$RANDOM","as":"$e"},{"op":"assert","e":"$e","a":2,"v":"load"}]}' \
  bun run loadtest/openloop.ts http://localhost:3000/tx 1000 10
```

The Bun client saturates at ~24 k req/s on loopback and hides the server's real
ceiling. For actual capacity numbers use a compiled generator. `wrk` needs no
per-request body: omit `uuid` so the server generates it, which keeps every
entity unique with a static body.

```sh
cat > /tmp/post.lua <<'LUA'
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
wrk.body = '{"ops":[{"op":"entity","as":"$e"},{"op":"assert","e":"$e","a":2,"v":"load"}]}'
LUA
wrk -t4 -c128 -d10s --latency -s /tmp/post.lua http://localhost:3000/tx
```

Measurements on macOS (aarch64, release build) with `synchronous = NORMAL` and
`page_size = 4096`, driven by `wrk`:

- `GET /noop` — ~53 k req/s, p99 2.5 ms.
- `POST /tx` (one entity + one fact) — ~30 k frames/s, p50 4.4 ms, p99 9 ms, 0
  failures. Throughput plateaus across c=32–256 connections, so the server — not
  the client — is the cap.

**The single writer thread is the limit — not JSON, HTTP, or the checkpoint.**
Under `/tx` load the `storage-writer` thread is ~93% busy on one core while the
16 tokio workers sit at ~8% each. Probe routes running the exact `/tx` parse
path without storage (including its current double-then-typed parse) hit the same
~53 k as `/noop`, so JSON parsing is free at this shape; only the DB costs.

That writer CPU is dominated by **index maintenance on `fact`**. Dropping the
four secondary indexes (`fact_eav`, `fact_ave`, `fact_hist`, `fact_one`) lifts
`/tx` from ~28 k to ~47 k. Base inserts, the per-frame `SAVEPOINT`, group commit,
and HTTP are minor by comparison — removing the savepoint entirely changed
nothing.

This ceiling is **accepted, not a target**. The indexes are required for the
read/query paths, and ~20–30 k write tx/s from a single writer is comfortably
enough for the workloads this is aimed at. The numbers above are to understand
where the floor is, not to remove indexes. The planned native format's opt-in
"search buckets" would raise it later without changing the write model.

Page size is the other dominant knob. At `page_size = 65536` each dirty page
writes a 64 KiB WAL frame, the WAL ballooned to ~1.4 GB in 6 s and tripped the
1 GiB `TRUNCATE` checkpoint, and the writer managed only ~12 k frames/s. At 4096
the WAL stays ~30× smaller and throughput roughly doubles.

Automatic checkpoint/truncation (`wal_autocheckpoint = 1000` +
`journal_size_limit = 4 MB`, custom thread off) pins the WAL at ~4 MB but is
slower and heavier-tailed — the frequent truncations cost more than they save.
The custom `PASSIVE` thread (TRUNCATE only past 1 GiB) is kept.

## State

### Design validation

- [ ] Synthetic file generator
- [ ] Scan-rate test
- [ ] Index test
