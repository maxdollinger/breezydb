//! libsql-backed EAV storage: one writer thread, group commits.
//!
//! Writes are serialized onto a single dedicated OS thread that owns a libsql
//! connection. It is independent of the async worker pool, so a busy HTTP
//! server cannot starve it. [`Writer`] clones feed that thread; [`Handle`] owns
//! it and surfaces the last error on shutdown.
//!
//! The data model is EAV (entities, concepts, attributes, and versioned facts),
//! ported from the design sketch. A client sends a *frame* — a list of
//! `entity` / `assert` / `close` / `retract` / `resolve` ops — which runs in one
//! transaction. Multiple queued frames are committed in a single transaction
//! (group commit), each isolated by a `SAVEPOINT`, so N frames cost one WAL
//! fsync and a single failing frame does not take down the batch.

pub mod storage;

pub use storage::{
    DB_SCHEMA, Handle, MAX_BATCH_BYTES, WAL_TRUNCATE_BYTES, WRITE_QUEUE_DEPTH, Writer, spawn,
};

pub(crate) use storage::WriteTx;
