//! The libsql writer thread and the two handles that talk to it.
//!
//! Writes are serialized onto one dedicated OS thread that owns a single
//! libsql [`Connection`]. That thread is independent of the async worker pool,
//! so a saturated HTTP server cannot starve it.
//!
//! Group commit: the thread drains whatever is queued into one `IMMEDIATE`
//! transaction and commits it once, so N appends cost one WAL fsync.

use std::io;
use std::path::Path;
use std::sync::Arc;

use libsql::{Builder, Connection, Statement, TransactionBehavior, params};
use tokio::runtime::Handle as RtHandle;
use tokio::sync::{mpsc, oneshot};

/// How many queued appends the writer holds before producers block.
pub const WRITE_QUEUE_DEPTH: usize = 4096;

/// Cap on bytes absorbed into a single group commit.
pub const MAX_BATCH_BYTES: usize = 16 << 20;

/// The single append-only table every write lands in.
pub const CREATE_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS data (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    schema INTEGER NOT NULL,
    data   BLOB    NOT NULL
);";

/// The one insert the writer ever runs. Prepared once, reused for every row.
pub const INSERT_SQL: &str = "INSERT INTO data (schema, data) VALUES (?1, ?2)";

/// One row to append. Each row carries its own schema.
pub struct Append {
    pub schema: i64,
    pub data: Vec<u8>,
}

type AckResult = Result<(), Arc<io::Error>>;

enum Cmd {
    AppendMany {
        rows: Vec<Append>,
        ack: oneshot::Sender<AckResult>,
    },
}

impl Cmd {
    fn bytes(&self) -> usize {
        match self {
            Cmd::AppendMany { rows, .. } => rows.iter().map(|r| r.data.len()).sum(),
        }
    }

    fn rows(&self) -> &[Append] {
        match self {
            Cmd::AppendMany { rows, .. } => rows,
        }
    }

    fn ack(self) -> oneshot::Sender<AckResult> {
        match self {
            Cmd::AppendMany { ack, .. } => ack,
        }
    }
}

/// Open the database, apply the tuning pragmas, create the table, and start the
/// writer thread.
///
/// The pragmas run on the connection that becomes the writer, before it is
/// handed to the thread. `page_size` only takes effect on a fresh database
/// file; an existing file keeps its old page size unless vacuumed.
pub async fn spawn(path: impl AsRef<Path>) -> io::Result<(Writer, Handle)> {
    let db = Builder::new_local(path).build().await.map_err(to_io)?;
    let conn = db.connect().map_err(to_io)?;

    conn.execute_batch(
        "PRAGMA page_size = 65536;
         PRAGMA journal_mode = WAL;",
    )
    .await
    .map_err(to_io)?;
    conn.execute_batch(CREATE_TABLE).await.map_err(to_io)?;

    let insert = conn.prepare(INSERT_SQL).await.map_err(to_io)?;

    let rt = RtHandle::current();
    let (tx, rx) = mpsc::channel(WRITE_QUEUE_DEPTH);
    let join = std::thread::Builder::new()
        .name("storage-writer".into())
        .spawn(move || {
            let _db = db;
            writer_loop(conn, insert, rt, rx)
        })
        .expect("spawn writer thread");

    Ok((Writer { tx }, Handle { join }))
}

fn writer_loop(
    conn: Connection,
    insert: Statement,
    rt: RtHandle,
    mut rx: mpsc::Receiver<Cmd>,
) -> Option<Arc<io::Error>> {
    let mut poison: Option<Arc<io::Error>> = None;

    while let Some(cmd) = rx.blocking_recv() {
        let mut batch = vec![cmd];
        let mut bytes = batch[0].bytes();

        while bytes < MAX_BATCH_BYTES {
            match rx.try_recv() {
                Ok(cmd) => {
                    bytes += cmd.bytes();
                    batch.push(cmd);
                }
                Err(_) => break,
            }
        }

        let res: AckResult = match poison.clone() {
            Some(e) => Err(e),
            None => rt
                .block_on(commit_batch(&conn, &insert, &batch))
                .map_err(|e| {
                    let e = Arc::new(e);
                    poison = Some(Arc::clone(&e));
                    e
                }),
        };

        for cmd in batch {
            let _ = cmd.ack().send(res.clone());
        }
    }

    poison
}

/// One group commit: every queued append in a single `IMMEDIATE` transaction.
async fn commit_batch(
    conn: &Connection,
    insert: &Statement,
    batch: &[Cmd],
) -> io::Result<()> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await
        .map_err(to_io)?;

    for cmd in batch {
        for row in cmd.rows() {
            insert.reset();
            insert
                .execute(params![row.schema, row.data.as_slice()])
                .await
                .map_err(to_io)?;
        }
    }

    tx.commit().await.map_err(to_io)
}

/// Cloned per task. Every clone feeds the same writer thread.
#[derive(Clone)]
pub struct Writer {
    tx: mpsc::Sender<Cmd>,
}

impl Writer {
    pub async fn append_many(&self, rows: Vec<Append>) -> io::Result<()> {
        let (ack, done) = oneshot::channel();
        self.tx
            .send(Cmd::AppendMany { rows, ack })
            .await
            .map_err(|_| io::Error::other("failed channel send"))?;
        match done.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(clone_err(&e)),
            Err(_) => Err(io::Error::other("failed channel await")),
        }
    }
}

/// Owns the writer thread. Never cloned.
pub struct Handle {
    join: std::thread::JoinHandle<Option<Arc<io::Error>>>,
}

impl Handle {
    /// Wait for the writer thread to finish and surface its last error.
    ///
    /// Drop every [`Writer`] clone first, or this waits forever: the loop exits
    /// when the command channel closes. `Drop` cannot report an error, which is
    /// why this exists.
    pub async fn close(self) -> io::Result<()> {
        tokio::task::spawn_blocking(move || match self.join.join() {
            Ok(None) => Ok(()),
            Ok(Some(e)) => Err(clone_err(&e)),
            Err(_) => Err(io::Error::other("storage writer thread panicked")),
        })
        .await
        .map_err(|e| io::Error::other(format!("join task failed: {e}")))?
    }
}

fn to_io(e: libsql::Error) -> io::Error {
    io::Error::other(e.to_string())
}

/// `io::Error` is not `Clone`, and the batch fan-out needs one error per waiter.
fn clone_err(e: &io::Error) -> io::Error {
    match e.raw_os_error() {
        Some(code) => io::Error::from_raw_os_error(code),
        None => io::Error::new(e.kind(), e.to_string()),
    }
}
