//! TursoDB-backed EAV storage: one writer thread, group commits.
//!
//! Writes are serialized onto a single dedicated OS thread that owns a Turso
//! connection. It is independent of the async worker pool, so a busy HTTP
//! server cannot starve it. [`Writer`] clones feed that thread; [`Handle`] owns
//! it and surfaces the last error on shutdown.
//!
//! The data model is EAV (entities, concepts, attributes, and versioned facts),
//! ported from the design sketch. A client sends a *transaction* — a list of
//! `entity` / `add` / `close` / `invalidate` / `resolve` ops — which is applied
//! atomically. Multiple queued transactions are committed together in one SQL
//! transaction (group commit), each isolated by a `SAVEPOINT`, so N
//! transactions cost one WAL fsync and a single failing transaction does not
//! take down the batch.

pub mod storage;

pub use storage::{
    DB_SCHEMA, Handle, MAX_BATCH_BYTES, WAL_TRUNCATE_BYTES, WRITE_QUEUE_DEPTH, Writer, spawn,
};

pub(crate) use storage::WriteTx;
