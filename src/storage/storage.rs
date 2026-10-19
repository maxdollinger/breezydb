//! The SQLite writer thread and the handles that talk to it.
//!
//! Writes are serialized onto one dedicated OS thread that owns a single
//! [`rusqlite::Connection`] (real SQLite, bundled). That thread is independent
//! of the async worker pool, so a saturated HTTP server cannot starve it.
//!
//! Group commit: the thread drains whatever is queued into one `IMMEDIATE`
//! SQL transaction and commits it once, so N queued transactions cost one WAL
//! fsync. Because the data model is EAV (a transaction adds/invalidates facts
//! rather than appending), each queued transaction still needs its own
//! isolation: every one runs inside a `SAVEPOINT`, so a transaction that fails
//! rolls back alone while the rest of the batch commits. A failure does not
//! poison the writer.
//!
//! Autocheckpoint is off (`PRAGMA wal_autocheckpoint=0` — enforced by real
//! SQLite, unlike Turso where it was a silent no-op and an inline checkpoint
//! fired every ~1000 frames on the writer's commit path). A second thread owns
//! its own connection and runs a `PASSIVE` checkpoint when the writer pings
//! it, so the WAL stays bounded without the writer paying for checkpoints on
//! its commit path.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rusqlite::types::Value;
use rusqlite::{Connection, TransactionBehavior, params};
use serde_json::Value as Json;
use tokio::sync::{mpsc, oneshot};

use crate::transaction::{TransactionOp, TransactionResult, run_transaction};

/// How many queued transactions the writer holds before producers block.
pub const WRITE_QUEUE_DEPTH: usize = 4096;

/// Cap on bytes absorbed into a single group commit.
pub const MAX_BATCH_BYTES: usize = 16 << 20;

/// Cap on WAL file size: past this the writer truncates it (see `writer_loop`).
pub const WAL_TRUNCATE_BYTES: u64 = 1 << 30;

/// Minimum spacing between the checkpoint thread's `PASSIVE` backfills.
/// See [`checkpoint_loop`].
pub const CHECKPOINT_PASSIVE_INTERVAL_MS: u64 = 500;

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

/// The pragmas every breezyDB connection runs. `page_size` only takes effect
/// on a fresh database file; an existing file keeps its old page size unless
/// vacuumed. The seeders must run `page_size` themselves before the first
/// page exists.
fn pragmas() -> Vec<String> {
    let mut pragmas: Vec<String> = vec![
        format!("PRAGMA page_size={};", env_str("BREEZY_PAGE_SIZE", "4096")),
        // Durability is not negotiable: every commit is fsynced, and on macOS
        // the fsync forces the drive write cache (F_FULLFSYNC). `OFF` /
        // `NORMAL` are opt-out experiments (BREEZY_SYNCHRONOUS /
        // BREEZY_FULLFSYNC), not defaults.
        format!("PRAGMA synchronous={};", env_str("BREEZY_SYNCHRONOUS", "FULL")),
        "PRAGMA journal_mode=WAL;".to_string(),
        "PRAGMA wal_autocheckpoint=0;".to_string(),
        "PRAGMA foreign_keys=ON;".to_string(),
    ];
    if let Ok(cs) = std::env::var("BREEZY_CACHE_SIZE") {
        pragmas.push(format!("PRAGMA cache_size={cs};"));
    }
    // macOS: `synchronous` decides *when* to sync; `fullfsync` decides *how*.
    // Without it the sync is plain fsync(), which only reaches the drive cache,
    // not the media. (F_FULLFSYNC. No-op off Apple, so gate it.) Default ON:
    // a 200 must mean power-loss durable, not just crash-durable.
    if cfg!(target_vendor = "apple") && std::env::var("NO_BREEZY_FULLFSYNC").is_err() {
        pragmas.push("PRAGMA fullfsync=ON;".to_string());
    }
    pragmas
}

/// Run one SQL statement that may return rows (pragmas, checkpoints),
/// stepping through and discarding them.
fn run_rows(conn: &Connection, sql: &str) -> io::Result<()> {
    let mut stmt = conn.prepare(sql).map_err(to_io)?;
    let mut rows = stmt.query(()).map_err(to_io)?;
    while rows.next().map_err(to_io)?.is_some() {}
    Ok(())
}

/// Run one row-free SQL statement through the per-connection statement cache.
fn exec(conn: &Connection, sql: &str) -> io::Result<()> {
    let mut stmt = conn.prepare_cached(sql).map_err(to_io)?;
    stmt.execute(()).map_err(to_io)?;
    Ok(())
}

/// Open a connection with the breezyDB pragmas and busy timeout applied.
/// `PRAGMA` statements that set a value return no rows; `journal_mode=WAL`
/// returns one — [`run_rows`] handles both.
pub fn open_connection(path: &str) -> io::Result<Connection> {
    let conn = Connection::open(path).map_err(to_io)?;
    for pragma in pragmas() {
        run_rows(&conn, &pragma)?;
    }
    conn.busy_timeout(Duration::from_secs(5))
        .map_err(to_io)?;
    Ok(conn)
}

/// A transaction-scoped view of the EAV statements. The `tx` row's id is opened
/// once per transaction and injected into every write, so callers never pass
/// `tx_from` / `tx_to` by hand.
///
/// Statements come from the connection's [`rusqlite::Connection::prepare_cached`]
/// pool, prepared once and reset per use — same shape the Turso version had
/// with its own prepared handles.
pub(crate) struct WriteTx<'a> {
    /// id of the `tx` row opened for this transaction.
    pub id: i64,
    conn: &'a Connection,
}

impl WriteTx<'_> {
    /// Create an entity stamped with this transaction; returns its id.
    pub(crate) fn insert_entity(&mut self, uuid: &str) -> io::Result<i64> {
        let mut stmt = self
            .conn
            .prepare_cached("INSERT INTO entity (uuid, created_tx) VALUES (?1, ?2)")
            .map_err(to_io)?;
        stmt.execute(params![Value::Text(uuid.to_owned()), Value::Integer(self.id)])
            .map_err(to_io)?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Resolve an identity / lookup ref; returns the entity id or `None`.
    pub(crate) fn resolve_ref(&mut self, a: i64, v: &Json) -> io::Result<Option<i64>> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT e FROM fact WHERE a = ?1 AND v = ?2 AND tx_to IS NULL",
            )
            .map_err(to_io)?;
        let mut rows = stmt
            .query(params![Value::Integer(a), json_to_sql(v)])
            .map_err(to_io)?;
        let out: Option<i64> = match rows.next().map_err(to_io)? {
            Some(row) => Some(row.get(0).map_err(to_io)?),
            None => None,
        };
        // Step the statement to completion so it isn't "in progress" at commit.
        while rows.next().map_err(to_io)?.is_some() {}
        Ok(out)
    }

    /// Close the live value of a cardinality-one attribute; returns rows changed.
    pub(crate) fn close_value(&mut self, e: i64, a: i64) -> io::Result<usize> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "UPDATE fact SET tx_to = ?3 WHERE e = ?1 AND a = ?2 AND tx_to IS NULL",
            )
            .map_err(to_io)?;
        let changed = stmt
            .execute(params![
                Value::Integer(e),
                Value::Integer(a),
                Value::Integer(self.id)
            ])
            .map_err(to_io)?;
        Ok(changed)
    }

    /// Invalidate a specific value of a cardinality-many attribute.
    pub(crate) fn invalidate_value(&mut self, e: i64, a: i64, v: &Json) -> io::Result<usize> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "UPDATE fact SET tx_to = ?4 WHERE e = ?1 AND a = ?2 AND v = ?3 AND tx_to IS NULL",
            )
            .map_err(to_io)?;
        let changed = stmt
            .execute(params![
                Value::Integer(e),
                Value::Integer(a),
                json_to_sql(v),
                Value::Integer(self.id)
            ])
            .map_err(to_io)?;
        Ok(changed)
    }

    /// Add a fact stamped with this transaction.
    pub(crate) fn add_fact(
        &mut self,
        e: i64,
        a: i64,
        v: &Json,
        card_one: i64,
    ) -> io::Result<()> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO fact (e, a, v, card_one, tx_from) VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .map_err(to_io)?;
        stmt.execute(params![
            Value::Integer(e),
            Value::Integer(a),
            json_to_sql(v),
            Value::Integer(card_one),
            Value::Integer(self.id)
        ])
        .map_err(to_io)?;
        Ok(())
    }

    /// An attribute's cardinality, or `None` if the attribute is unknown.
    pub(crate) fn attr_card_one(&mut self, a: i64) -> io::Result<Option<i64>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT card_one FROM attr WHERE id = ?1")
            .map_err(to_io)?;
        let mut rows = stmt.query(params![Value::Integer(a)]).map_err(to_io)?;
        let out: Option<i64> = match rows.next().map_err(to_io)? {
            Some(row) => Some(row.get(0).map_err(to_io)?),
            None => None,
        };
        while rows.next().map_err(to_io)?.is_some() {}
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
pub async fn spawn(path: impl AsRef<Path>) -> io::Result<(Writer, Handle)> {
    let path = path.as_ref();
    let path_str = path
        .to_str()
        .ok_or_else(|| io::Error::other("database path is not valid UTF-8"))?;

    let conn = open_connection(path_str)?;
    let schema = schema_for_env();
    conn.execute_batch(&schema).map_err(to_io)?;
    let checkpoint_conn = open_connection(path_str)?;
    // TRUNCATE needs the exclusive WRITER lock, which the writer holds nearly
    // continuously under load. A failed attempt must not stall this thread's
    // PASSIVE backfills behind the default 5s busy retry — fail fast instead.
    checkpoint_conn
        .busy_timeout(Duration::from_millis(250))
        .map_err(to_io)?;

    let wal_path = {
        let mut os = path.as_os_str().to_owned();
        os.push("-wal");
        PathBuf::from(os)
    };

    let queue_depth = env_usize("BREEZY_WRITE_QUEUE_DEPTH", WRITE_QUEUE_DEPTH);
    let max_batch_bytes = env_usize("BREEZY_MAX_BATCH_BYTES", MAX_BATCH_BYTES);
    let wal_truncate_bytes = env_u64("BREEZY_WAL_TRUNCATE_BYTES", WAL_TRUNCATE_BYTES);
    let no_savepoint = std::env::var("BREEZY_NO_SAVEPOINT").is_ok();
    let (tx, rx) = mpsc::channel(queue_depth);
    let (notify_tx, notify_rx) = mpsc::channel::<()>(1);

    let writer_wal_path = wal_path.clone();
    let writer_join = std::thread::Builder::new()
        .name("storage-writer".into())
        .spawn(move || {
            writer_loop(
                conn,
                writer_wal_path,
                rx,
                notify_tx,
                max_batch_bytes,
                wal_truncate_bytes,
                no_savepoint,
            )
        })
        .expect("spawn writer thread");

    let checkpoint_join = std::thread::Builder::new()
        .name("storage-checkpoint".into())
        .spawn(move || {
            checkpoint_loop(checkpoint_conn, notify_rx);
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
    wal_path: PathBuf,
    mut rx: mpsc::Receiver<Cmd>,
    notify_tx: mpsc::Sender<()>,
    max_batch_bytes: usize,
    wal_truncate_bytes: u64,
    no_savepoint: bool,
) -> Option<Arc<io::Error>> {
    let mut poison: Option<Arc<io::Error>> = None;
    let mut last_trunc_log = std::time::Instant::now()
        .checked_sub(Duration::from_secs(15))
        .unwrap_or_else(std::time::Instant::now);

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
            None => commit_batch(&mut conn, batch, no_savepoint),
        };

        match res {
            Ok(()) => {
                let _ = notify_tx.try_send(());
                // Cap the WAL: truncate from *this* connection, between
                // transactions. Resetting the log needs the exclusive WAL
                // WRITER lock, which this thread holds nearly continuously
                // under load, so an external checkpointer almost always
                // fails busy (its PASSIVEs still backfill every frame; the
                // reset is all that's missing). Here the lock is free by
                // construction, so the truncate succeeds on the first try.
                if wal_bytes(&wal_path) > wal_truncate_bytes {
                    match checkpoint_status(&conn, CHECKPOINT_TRUNCATE) {
                        Ok((busy, _, _)) if busy == 0 => {}
                        Ok((busy, log, ckpt)) => {
                            if last_trunc_log.elapsed() >= Duration::from_secs(15) {
                                last_trunc_log = std::time::Instant::now();
                                eprintln!(
                                    "writer wal_checkpoint(TRUNCATE) busy={busy} wal={log} ckpt={ckpt}"
                                );
                            }
                        }
                        Err(e) => eprintln!("writer wal_checkpoint(TRUNCATE) failed: {e}"),
                    }
                }
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
fn commit_batch(conn: &mut Connection, batch: Vec<Cmd>, no_savepoint: bool) -> io::Result<()> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(to_io)?;
    let db: &Connection = &tx;

    // Run every transaction, remember each result, and only ack after the commit so a
    // commit failure is reported to all waiters instead of a false success.
    let mut pending: Vec<(oneshot::Sender<AckResult>, AckResult)> = Vec::with_capacity(batch.len());
    for cmd in batch {
        let Cmd::Transaction { ops, ack } = cmd;
        let res = execute_transaction(db, &ops, no_savepoint);
        pending.push((ack, res));
    }

    match tx.commit() {
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
fn execute_transaction(conn: &Connection, ops: &[TransactionOp], no_savepoint: bool) -> AckResult {
    if no_savepoint {
        // Isolation experiment only: no per-tx rollback safety.
        return transaction_body(conn, ops);
    }

    exec(conn, "SAVEPOINT breezy_transaction").map_err(arc_err)?;

    match transaction_body(conn, ops) {
        Ok(result) => {
            exec(conn, "RELEASE breezy_transaction").map_err(arc_err)?;
            Ok(result)
        }
        Err(e) => {
            let _ = exec(conn, "ROLLBACK TO breezy_transaction");
            let _ = exec(conn, "RELEASE breezy_transaction");
            Err(e)
        }
    }
}

/// Open this transaction's `tx` row, then run its ops against the EAV statements.
fn transaction_body(conn: &Connection, ops: &[TransactionOp]) -> AckResult {
    exec(conn, "INSERT INTO tx DEFAULT VALUES").map_err(arc_err)?;
    let id = conn.last_insert_rowid();

    let mut tx = WriteTx { id, conn };
    run_transaction(&mut tx, ops).map_err(arc_err)
}

/// Checkpointing on its own connection.
///
/// The writer pings `rx` after each commit. `PASSIVE` runs at most once per
/// [`CHECKPOINT_PASSIVE_INTERVAL_MS`]: it needs no exclusive locks and keeps
/// the DB file current. Running it on *every* ping instead would hold the
/// CHECKPOINTER lock nearly continuously under load and starve the writer's
/// `TRUNCATE` of it.
///
/// Capping the WAL is the writer's job (`writer_loop`): resetting/truncating
/// the log needs the exclusive WAL WRITER lock, which the writer holds almost
/// continuously under load, so an external checkpointer nearly always fails
/// busy. Between the writer's own batches the lock is free by construction,
/// so its `TRUNCATE` succeeds on the first try.
fn checkpoint_loop(conn: Connection, mut rx: mpsc::Receiver<()>) {
    let interval = Duration::from_millis(env_u64(
        "BREEZY_CHECKPOINT_PASSIVE_INTERVAL_MS",
        CHECKPOINT_PASSIVE_INTERVAL_MS,
    ));
    let mut next = std::time::Instant::now();
    while rx.blocking_recv().is_some() {
        let now = std::time::Instant::now();
        if now < next {
            // Pings arrive per batch commit — thousands per second under load.
            // Collapse the burst; the next ping re-arms.
            continue;
        }
        next = now + interval;
        if let Err(e) = run_rows(&conn, CHECKPOINT_PASSIVE) {
            eprintln!("wal_checkpoint(PASSIVE) failed: {e}");
        }
    }
}

/// `PRAGMA wal_checkpoint` returns `(busy, log, checkpointed)`. `busy != 0`
/// means it could not complete (locks contended) — the pragma itself still
/// succeeds, so the rows must be read to notice.
fn checkpoint_status(conn: &Connection, sql: &str) -> io::Result<(i64, i64, i64)> {
    let mut stmt = conn.prepare(sql).map_err(to_io)?;
    let mut rows = stmt.query(()).map_err(to_io)?;
    match rows.next().map_err(to_io)? {
        Some(row) => Ok((
            row.get(0).map_err(to_io)?,
            row.get(1).map_err(to_io)?,
            row.get(2).map_err(to_io)?,
        )),
        None => Ok((0, 0, 0)),
    }
}

fn wal_bytes(path: &Path) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
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

fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

/// Wrap any error (rusqlite or io) into the shared error form used by transaction acks.
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