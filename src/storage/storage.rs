//! The TursoDB writer thread and the handles that talk to it.
//!
//! Writes are serialized onto one dedicated OS thread that owns a single
//! Turso [`Connection`]. That thread is independent of the async worker pool,
//! so a saturated HTTP server cannot starve it.
//!
//! Group commit: the thread drains whatever is queued into one `IMMEDIATE`
//! SQL transaction and commits it once, so N queued transactions cost one WAL
//! fsync. Because the data model is EAV (a transaction adds/invalidates facts
//! rather than appending), each queued transaction still needs its own
//! isolation: every one runs inside a `SAVEPOINT`, so a transaction that fails
//! rolls back alone while the rest of the batch commits. A failure does not
//! poison the writer.
//!
//! Autocheckpoint is off. A second thread owns its own connection and runs a
//! `PASSIVE` checkpoint whenever the writer pings it, so the WAL stays bounded
//! without the writer paying for checkpoints on its commit path.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value as Json;
use tokio::runtime::Handle as RtHandle;
use tokio::sync::{mpsc, oneshot};
use turso::transaction::TransactionBehavior;
use turso::{Builder, Connection, Error, Statement, Value};

use crate::transaction::{TransactionOp, TransactionResult, run_transaction};

/// How many queued transactions the writer holds before producers block.
pub const WRITE_QUEUE_DEPTH: usize = 4096;

/// Cap on bytes absorbed into a single group commit.
pub const MAX_BATCH_BYTES: usize = 16 << 20;

/// Checkpoint `PASSIVE` normally, `TRUNCATE` once the WAL file passes this.
pub const WAL_TRUNCATE_BYTES: u64 = 1 << 30;

const CHECKPOINT_PASSIVE: &str = "PRAGMA wal_checkpoint(PASSIVE)";
const CHECKPOINT_TRUNCATE: &str = "PRAGMA wal_checkpoint(TRUNCATE)";

// Tuning knobs, overridable by env var so the load harness can sweep them
// without a rebuild. Defaults are the shipped values.
fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The EAV schema. Ported from the design sketch. Every statement is
/// `IF NOT EXISTS` so opening an existing database is idempotent.
pub const DB_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS tx (
  id  INTEGER PRIMARY KEY,
  at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
) STRICT;

CREATE TABLE IF NOT EXISTS concept (
  id    INTEGER PRIMARY KEY,
  ident TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE IF NOT EXISTS concept_parent (
  concept INTEGER NOT NULL REFERENCES concept(id),
  parent  INTEGER NOT NULL REFERENCES concept(id),
  PRIMARY KEY (concept, parent),
  CHECK (concept <> parent)
) STRICT;

CREATE TABLE IF NOT EXISTS concept_ancestor (
  concept  INTEGER NOT NULL REFERENCES concept(id),
  ancestor INTEGER NOT NULL REFERENCES concept(id),
  PRIMARY KEY (concept, ancestor)
) STRICT;

CREATE TABLE IF NOT EXISTS attr (
  id          INTEGER PRIMARY KEY,
  ident       TEXT NOT NULL UNIQUE,
  domain      INTEGER REFERENCES concept(id),
  value_type  TEXT NOT NULL CHECK (value_type IN ('string','int','float','bool','date','ref','concept')),
  range       INTEGER REFERENCES concept(id),
  card_one    INTEGER NOT NULL DEFAULT 1 CHECK (card_one IN (0,1)),
  is_unique   INTEGER NOT NULL DEFAULT 0 CHECK (is_unique IN (0,1)),
  is_identity INTEGER NOT NULL DEFAULT 0 CHECK (is_identity IN (0,1)),
  CHECK ((value_type = 'ref') = (range IS NOT NULL)),
  CHECK (is_identity = 0 OR is_unique = 1)
) STRICT;

CREATE TABLE IF NOT EXISTS entity (
  id         INTEGER PRIMARY KEY,
  uuid       TEXT NOT NULL UNIQUE,
  created_tx INTEGER NOT NULL REFERENCES tx(id)
) STRICT;

CREATE TABLE IF NOT EXISTS fact (
  e        INTEGER NOT NULL REFERENCES entity(id),
  a        INTEGER NOT NULL REFERENCES attr(id),
  v        ANY     NOT NULL,
  card_one INTEGER NOT NULL CHECK (card_one IN (0,1)),
  tx_from  INTEGER NOT NULL REFERENCES tx(id),
  tx_to    INTEGER REFERENCES tx(id),
  PRIMARY KEY (e, a, tx_from),
  CHECK (tx_to IS NULL OR tx_to >= tx_from)
) STRICT;

CREATE INDEX IF NOT EXISTS fact_eav  ON fact(e, a, v) WHERE tx_to IS NULL;
CREATE INDEX IF NOT EXISTS fact_ave  ON fact(a, v, e) WHERE tx_to IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS fact_one  ON fact(e, a)    WHERE tx_to IS NULL AND card_one = 1;
CREATE UNIQUE INDEX IF NOT EXISTS fact_many ON fact(e, a, v) WHERE tx_to IS NULL AND card_one = 0;";

/// Drop every `CREATE [UNIQUE] INDEX` line. Used by the `BREEZY_NO_FACT_INDEXES`
/// experiment to isolate secondary-index maintenance on the write path.
pub fn schema_without_fact_indexes(schema: &str) -> String {
    schema
        .lines()
        .filter(|l| {
            let l = l.trim_start().to_ascii_uppercase();
            !l.starts_with("CREATE INDEX") && !l.starts_with("CREATE UNIQUE INDEX")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The schema the server and the seeder should use, honoring the index
/// experiments:
///
/// - `BREEZY_NO_FACT_INDEXES` — drop all secondary indexes.
/// - `BREEZY_DROP_FACT_INDEXES=a,b` — drop only the named indexes (e.g.
///   `fact_hist`), so the cost can be attributed index by index.
pub fn schema_for_env() -> String {
    if std::env::var("BREEZY_NO_FACT_INDEXES").is_ok() {
        return schema_without_fact_indexes(DB_SCHEMA);
    }
    let Ok(list) = std::env::var("BREEZY_DROP_FACT_INDEXES") else {
        return DB_SCHEMA.to_string();
    };
    let names: Vec<&str> = list
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    DB_SCHEMA
        .lines()
        .filter(|l| {
            let up = l.trim_start().to_ascii_uppercase();
            let is_index = up.starts_with("CREATE INDEX") || up.starts_with("CREATE UNIQUE INDEX");
            !is_index || !names.iter().any(|n| l.contains(n))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The statements the writer runs. Prepared once, reused for every transaction.
///
/// Turso's [`Statement::query`] / [`Statement::execute`] take `&mut self`. The
/// writer is a single thread that owns these outright, so it threads
/// `&mut Statements` through instead of reaching for interior mutability.
pub(crate) struct Statements {
    open_tx: Statement,
    insert_entity: Statement,
    resolve_ref: Statement,
    close_value: Statement,
    invalidate_value: Statement,
    add_fact: Statement,
    attr_card_one: Statement,
    savepoint: Statement,
    release: Statement,
    rollback_to: Statement,
    /// When set, skip the per-transaction `SAVEPOINT` (an isolation experiment:
    /// the savepoint makes the pager page-journal every page a tx touches).
    no_savepoint: bool,
}

async fn prepare(conn: &Connection, sql: &str) -> io::Result<Statement> {
    conn.prepare(sql).await.map_err(to_io)
}

impl Statements {
    async fn new(conn: &Connection) -> io::Result<Statements> {
        Ok(Statements {
            open_tx: prepare(conn, "INSERT INTO tx DEFAULT VALUES").await?,
            insert_entity: prepare(
                conn,
                "INSERT INTO entity (uuid, created_tx) VALUES (?1, ?2)",
            )
            .await?,
            resolve_ref: prepare(
                conn,
                "SELECT e FROM fact WHERE a = ?1 AND v = ?2 AND tx_to IS NULL",
            )
            .await?,
            close_value: prepare(
                conn,
                "UPDATE fact SET tx_to = ?3 WHERE e = ?1 AND a = ?2 AND tx_to IS NULL",
            )
            .await?,
            invalidate_value: prepare(
                conn,
                "UPDATE fact SET tx_to = ?4 WHERE e = ?1 AND a = ?2 AND v = ?3 AND tx_to IS NULL",
            )
            .await?,
            add_fact: prepare(
                conn,
                "INSERT INTO fact (e, a, v, card_one, tx_from) VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .await?,
            attr_card_one: prepare(conn, "SELECT card_one FROM attr WHERE id = ?1").await?,
            savepoint: prepare(conn, "SAVEPOINT breezy_transaction").await?,
            release: prepare(conn, "RELEASE breezy_transaction").await?,
            rollback_to: prepare(conn, "ROLLBACK TO breezy_transaction").await?,
            no_savepoint: std::env::var("BREEZY_NO_SAVEPOINT").is_ok(),
        })
    }
}

/// A transaction-scoped view of the EAV statements. The `tx` row's id is opened
/// once per transaction and injected into every write, so callers never pass
/// `tx_from` / `tx_to` by hand.
///
/// The writer reads generated ids with `last_insert_rowid()` rather than
/// `INSERT ... RETURNING`: in Turso 0.8.3 `RETURNING` is ~25x slower on this
/// write path, while `last_insert_rowid()` is free. The `Statement` clones all
/// share the writer's connection, so the value is visible here right after each
/// insert.
pub(crate) struct WriteTx<'a> {
    /// id of the `tx` row opened for this transaction.
    pub id: i64,
    stmts: &'a mut Statements,
    conn: &'a Connection,
}

impl WriteTx<'_> {
    /// Create an entity stamped with this transaction; returns its id.
    pub(crate) async fn insert_entity(&mut self, uuid: &str) -> io::Result<i64> {
        self.stmts
            .insert_entity
            .execute(vec![Value::Text(uuid.to_owned()), Value::Integer(self.id)])
            .await
            .map_err(to_io)?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Resolve an identity / lookup ref; returns the entity id or `None`.
    pub(crate) async fn resolve_ref(&mut self, a: i64, v: &Json) -> io::Result<Option<i64>> {
        let mut rows = self
            .stmts
            .resolve_ref
            .query(vec![Value::Integer(a), json_to_sql(v)])
            .await
            .map_err(to_io)?;
        let out: Option<i64> = match rows.next().await.map_err(to_io)? {
            Some(row) => Some(row.get(0).map_err(to_io)?),
            None => None,
        };
        // Step the statement to completion so it isn't "in progress" at commit.
        while rows.next().await.map_err(to_io)?.is_some() {}
        Ok(out)
    }

    /// Close the live value of a cardinality-one attribute; returns rows changed.
    pub(crate) async fn close_value(&mut self, e: i64, a: i64) -> io::Result<usize> {
        let changed = self
            .stmts
            .close_value
            .execute(vec![
                Value::Integer(e),
                Value::Integer(a),
                Value::Integer(self.id),
            ])
            .await
            .map_err(to_io)?;
        Ok(changed as usize)
    }

    /// Invalidate a specific value of a cardinality-many attribute.
    pub(crate) async fn invalidate_value(&mut self, e: i64, a: i64, v: &Json) -> io::Result<usize> {
        let changed = self
            .stmts
            .invalidate_value
            .execute(vec![
                Value::Integer(e),
                Value::Integer(a),
                json_to_sql(v),
                Value::Integer(self.id),
            ])
            .await
            .map_err(to_io)?;
        Ok(changed as usize)
    }

    /// Add a fact stamped with this transaction.
    pub(crate) async fn add_fact(
        &mut self,
        e: i64,
        a: i64,
        v: &Json,
        card_one: i64,
    ) -> io::Result<()> {
        self.stmts
            .add_fact
            .execute(vec![
                Value::Integer(e),
                Value::Integer(a),
                json_to_sql(v),
                Value::Integer(card_one),
                Value::Integer(self.id),
            ])
            .await
            .map_err(to_io)?;
        Ok(())
    }

    /// An attribute's cardinality, or `None` if the attribute is unknown.
    pub(crate) async fn attr_card_one(&mut self, a: i64) -> io::Result<Option<i64>> {
        let mut rows = self
            .stmts
            .attr_card_one
            .query(vec![Value::Integer(a)])
            .await
            .map_err(to_io)?;
        let out: Option<i64> = match rows.next().await.map_err(to_io)? {
            Some(row) => Some(row.get(0).map_err(to_io)?),
            None => None,
        };
        while rows.next().await.map_err(to_io)?.is_some() {}
        Ok(out)
    }
}

/// A batch of queued transactions plus the channel each result is delivered on.
type AckResult = Result<TransactionResult, Arc<io::Error>>;

enum Cmd {
    Transaction {
        ops: Vec<TransactionOp>,
        ack: oneshot::Sender<AckResult>,
    },
}

impl Cmd {
    fn bytes(&self) -> usize {
        match self {
            Cmd::Transaction { ops, .. } => serde_json::to_vec(ops).map(|b| b.len()).unwrap_or(0),
        }
    }

    fn ack(self) -> oneshot::Sender<AckResult> {
        match self {
            Cmd::Transaction { ack, .. } => ack,
        }
    }
}

/// Open the database, apply the tuning pragmas, create the EAV schema, and
/// start the writer thread.
///
/// The pragmas run on the connection that becomes the writer, before it is
/// handed to the thread. `page_size` only takes effect on a fresh database
/// file; an existing file keeps its old page size unless vacuumed.
pub async fn spawn(path: impl AsRef<Path>) -> io::Result<(Writer, Handle)> {
    let path = path.as_ref();
    let path_str = path
        .to_str()
        .ok_or_else(|| io::Error::other("database path is not valid UTF-8"))?;
    let db = Builder::new_local(path_str).build().await.map_err(to_io)?;
    let conn = db.connect().map_err(to_io)?;

    // Pragmas that return a row (`journal_mode`) cannot go through
    // `execute_batch`; `pragma_update` drains the result and discards it.
    let mut pragmas: Vec<(&str, String)> = vec![
        ("page_size", env_str("BREEZY_PAGE_SIZE", "4096")),
        ("synchronous", env_str("BREEZY_SYNCHRONOUS", "NORMAL")),
        ("journal_mode", "WAL".to_string()),
        ("wal_autocheckpoint", "0".to_string()),
        ("foreign_keys", "ON".to_string()),
    ];
    if let Ok(cs) = std::env::var("BREEZY_CACHE_SIZE") {
        pragmas.push(("cache_size", cs));
    }
    // macOS: `synchronous` decides *when* to sync; `fullfsync` decides *how*.
    // Without it the sync is plain fsync(), which only reaches the drive cache,
    // not the media. (F_FULLFSYNC. No-op off Apple, so gate it.)
    if cfg!(target_vendor = "apple") && std::env::var("BREEZY_FULLFSYNC").is_ok() {
        pragmas.push(("fullfsync", "ON".to_string()));
    }
    for (name, value) in &pragmas {
        conn.pragma_update(*name, value.as_str())
            .await
            .map_err(to_io)?;
    }

    let schema = schema_for_env();
    conn.execute_batch(&schema).await.map_err(to_io)?;
    conn.busy_timeout(Duration::from_secs(5)).map_err(to_io)?;

    let stmts = Statements::new(&conn).await?;
    let checkpoint_conn = db.connect().map_err(to_io)?;
    let checkpoint_db = db.clone();
    checkpoint_conn
        .busy_timeout(Duration::from_secs(5))
        .map_err(to_io)?;
    if cfg!(target_vendor = "apple") && std::env::var("BREEZY_FULLFSYNC").is_ok() {
        checkpoint_conn
            .pragma_update("fullfsync", "ON")
            .await
            .map_err(to_io)?;
    }

    let wal_path = {
        let mut os = path.as_os_str().to_owned();
        os.push("-wal");
        PathBuf::from(os)
    };

    let rt = RtHandle::current();
    let queue_depth = env_usize("BREEZY_WRITE_QUEUE_DEPTH", WRITE_QUEUE_DEPTH);
    let max_batch_bytes = env_usize("BREEZY_MAX_BATCH_BYTES", MAX_BATCH_BYTES);
    let wal_truncate_bytes = env_u64("BREEZY_WAL_TRUNCATE_BYTES", WAL_TRUNCATE_BYTES);
    let (tx, rx) = mpsc::channel(queue_depth);
    let (notify_tx, notify_rx) = mpsc::channel::<()>(1);

    let writer_rt = rt.clone();
    let writer_join = std::thread::Builder::new()
        .name("storage-writer".into())
        .spawn(move || {
            let _db = db;
            writer_loop(conn, stmts, writer_rt, rx, notify_tx, max_batch_bytes)
        })
        .expect("spawn writer thread");

    let checkpoint_join = std::thread::Builder::new()
        .name("storage-checkpoint".into())
        .spawn(move || {
            let _db = checkpoint_db;
            checkpoint_loop(checkpoint_conn, rt, wal_path, notify_rx, wal_truncate_bytes)
        })
        .expect("spawn checkpoint thread");

    Ok((
        Writer { tx },
        Handle {
            writer_join,
            checkpoint_join,
        },
    ))
}

fn writer_loop(
    mut conn: Connection,
    mut stmts: Statements,
    rt: RtHandle,
    mut rx: mpsc::Receiver<Cmd>,
    notify_tx: mpsc::Sender<()>,
    max_batch_bytes: usize,
) -> Option<Arc<io::Error>> {
    let mut poison: Option<Arc<io::Error>> = None;

    while let Some(cmd) = rx.blocking_recv() {
        let mut batch = vec![cmd];
        let mut bytes = batch[0].bytes();

        while bytes < max_batch_bytes {
            match rx.try_recv() {
                Ok(cmd) => {
                    bytes += cmd.bytes();
                    batch.push(cmd);
                }
                Err(_) => break,
            }
        }

        let res: io::Result<()> = match poison.clone() {
            Some(e) => {
                // The connection is already dead; fail every waiter, per transaction.
                let e = Arc::new(io::Error::new(e.kind(), e.to_string()));
                for cmd in batch {
                    let _ = cmd.ack().send(Err(Arc::clone(&e)));
                }
                continue;
            }
            None => rt.block_on(commit_batch(&mut conn, &mut stmts, batch)),
        };

        match res {
            Ok(()) => {
                let _ = notify_tx.try_send(());
            }
            Err(e) => {
                let e = Arc::new(e);
                poison = Some(Arc::clone(&e));
            }
        }
    }

    poison
}

/// One group commit: every queued transaction in a single `IMMEDIATE` SQL
/// transaction, each wrapped in a savepoint so a failing transaction rolls back
/// alone.
async fn commit_batch(
    conn: &mut Connection,
    stmts: &mut Statements,
    batch: Vec<Cmd>,
) -> io::Result<()> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await
        .map_err(to_io)?;

    // Run every transaction, remember each result, and only ack after the commit so a
    // commit failure is reported to all waiters instead of a false success.
    let mut pending: Vec<(oneshot::Sender<AckResult>, AckResult)> = Vec::with_capacity(batch.len());
    for cmd in batch {
        let Cmd::Transaction { ops, ack } = cmd;
        let res = execute_transaction(&tx, stmts, &ops).await;
        pending.push((ack, res));
    }

    match tx.commit().await {
        Ok(()) => {
            for (ack, res) in pending {
                let _ = ack.send(res);
            }
            Ok(())
        }
        Err(e) => {
            let e = Arc::new(to_io(e));
            for (ack, _) in pending {
                let _ = ack.send(Err(Arc::clone(&e)));
            }
            Err(clone_err(&e))
        }
    }
}

/// Run one transaction inside its own savepoint: on failure roll back to the
/// savepoint so the transaction leaves no trace, then release it either way.
async fn execute_transaction(
    conn: &Connection,
    stmts: &mut Statements,
    ops: &[TransactionOp],
) -> AckResult {
    if stmts.no_savepoint {
        // Isolation experiment only: no per-tx rollback safety.
        return transaction_body(conn, stmts, ops).await;
    }

    stmts.savepoint.execute(()).await.map_err(arc_err)?;

    match transaction_body(conn, stmts, ops).await {
        Ok(result) => {
            stmts.release.execute(()).await.map_err(arc_err)?;
            Ok(result)
        }
        Err(e) => {
            let _ = stmts.rollback_to.execute(()).await;
            let _ = stmts.release.execute(()).await;
            Err(e)
        }
    }
}

/// Open this transaction's `tx` row, then run its ops against the EAV statements.
async fn transaction_body(
    conn: &Connection,
    stmts: &mut Statements,
    ops: &[TransactionOp],
) -> AckResult {
    stmts.open_tx.execute(()).await.map_err(arc_err)?;
    let id = conn.last_insert_rowid();

    let mut tx = WriteTx { id, stmts, conn };
    run_transaction(&mut tx, ops).await.map_err(arc_err)
}

/// Checkpointing on its own connection.
///
/// The writer pings `rx` after each commit; capacity-1 `try_send` drops the
/// ping when one is already queued, so bursts collapse into a single checkpoint.
/// `PASSIVE` normally, `TRUNCATE` once the WAL file passes [`WAL_TRUNCATE_BYTES`].
fn checkpoint_loop(
    conn: Connection,
    rt: RtHandle,
    wal_path: PathBuf,
    mut rx: mpsc::Receiver<()>,
    wal_truncate_bytes: u64,
) {
    while rx.blocking_recv().is_some() {
        let sql = if wal_bytes(&wal_path) > wal_truncate_bytes {
            CHECKPOINT_TRUNCATE
        } else {
            CHECKPOINT_PASSIVE
        };
        if let Err(e) = rt.block_on(checkpoint(&conn, sql)) {
            eprintln!("wal_checkpoint failed: {e}");
        }
    }
}

fn wal_bytes(path: &Path) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

async fn checkpoint(conn: &Connection, sql: &str) -> io::Result<()> {
    let mut rows = conn.query(sql, ()).await.map_err(to_io)?;
    while rows.next().await.map_err(to_io)?.is_some() {}
    Ok(())
}

/// Cloned per task. Every clone feeds the same writer thread.
#[derive(Clone)]
pub struct Writer {
    tx: mpsc::Sender<Cmd>,
}

impl Writer {
    /// Queue one transaction and wait for its result.
    pub async fn write_transaction(
        &self,
        ops: Vec<TransactionOp>,
    ) -> io::Result<TransactionResult> {
        let (ack, done) = oneshot::channel();
        self.tx
            .send(Cmd::Transaction { ops, ack })
            .await
            .map_err(|_| io::Error::other("failed channel send"))?;
        match done.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(e)) => Err(clone_err(&e)),
            Err(_) => Err(io::Error::other("failed channel await")),
        }
    }
}

/// Owns the writer and checkpoint threads. Never cloned.
pub struct Handle {
    writer_join: std::thread::JoinHandle<Option<Arc<io::Error>>>,
    checkpoint_join: std::thread::JoinHandle<()>,
}

impl Handle {
    /// Wait for both threads to finish and surface the last error.
    ///
    /// Drop every [`Writer`] clone first, or this waits forever: the writer loop
    /// exits when the command channel closes, which drops the checkpoint
    /// notify sender, which ends the checkpoint loop.
    pub async fn close(self) -> io::Result<()> {
        tokio::task::spawn_blocking(move || {
            let written = self.writer_join.join();
            let checked = self.checkpoint_join.join();

            let store = match written {
                Ok(None) => Ok(()),
                Ok(Some(e)) => Err(clone_err(&e)),
                Err(_) => Err(io::Error::other("storage writer thread panicked")),
            };

            match (store, checked) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(e), _) => Err(e),
                (Ok(()), Err(_)) => Err(io::Error::other("checkpoint thread panicked")),
            }
        })
        .await
        .map_err(|e| io::Error::other(format!("join task failed: {e}")))?
    }
}

/// JSON has no direct SQL type, so map scalars through and fall back to a JSON
/// text encoding for arrays and objects (the `fact.v` column is `ANY`).
fn json_to_sql(v: &Json) -> Value {
    match v {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Integer(*b as i64),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Real(n.as_f64().unwrap_or(0.0))
            }
        }
        Json::String(s) => Value::Text(s.clone()),
        other => Value::Text(other.to_string()),
    }
}

fn to_io(e: Error) -> io::Error {
    io::Error::other(e.to_string())
}

/// Wrap any error (Turso or io) into the shared error form used by transaction acks.
fn arc_err<E: std::fmt::Display>(e: E) -> Arc<io::Error> {
    Arc::new(io::Error::other(e.to_string()))
}

/// `io::Error` is not `Clone`, and the batch fan-out needs one error per waiter.
fn clone_err(e: &io::Error) -> io::Error {
    match e.raw_os_error() {
        Some(code) => io::Error::from_raw_os_error(code),
        None => io::Error::new(e.kind(), e.to_string()),
    }
}
