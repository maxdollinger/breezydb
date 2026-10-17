- The PK field of a schema is never allowed to change.
- Schema needs and stable name.
- Need to figure out how migrations work

## EAV write path (MVP)

- `fact` PK is `(e, a, tx_from)` on purpose: a transaction must not overwrite
  its own value, and this keeps strict transaction ordering. Consequence: one
  frame may touch a given `(e, a)` at most once.
- Group commit: many queued frames share one SQL transaction, but each frame is
  wrapped in a `SAVEPOINT` so it rolls back alone. A logical error never poisons
  the writer; only a failed commit does.
- Concepts/attrs are seeded out of band; the API only writes entities/facts.
