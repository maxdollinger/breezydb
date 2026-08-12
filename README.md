# breezyDB

!! This is a personal research project.

- plain HTTP, no client library, no driver, no connection
- nothing to tune: sensible defaults
- batteries included: Migrations, backups, and replication built in

## Storage Engine

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

## State

### Design validation

- [ ] Synthetic file generator
- [ ] Scan-rate test
- [ ] Index test
