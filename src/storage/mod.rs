//! libsql-backed append-only storage: one writer thread, group commits.
//!
//! Writes are serialized onto a single dedicated OS thread that owns a libsql
//! connection. It is independent of the async worker pool, so a busy HTTP
//! server cannot starve it. [`Writer`] clones feed that thread; [`Handle`] owns
//! it and surfaces the last error on shutdown.
//!
//! Every write lands in one table:
//!
//! ```sql
//! data (id INTEGER PRIMARY KEY AUTOINCREMENT, schema INTEGER, data BLOB)
//! ```
//!
//! Appends are only ever inserted. Multiple queued appends are committed in a
//! single transaction (group commit), so N writes cost one WAL fsync.

pub mod storage;

pub use storage::{Append, Handle, Writer, spawn};
