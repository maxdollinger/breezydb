//! The writer thread and the three handles that talk to it.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::storage::frame::FRAME_MAX_SIZE;

use super::{Reader, Storage};

/// Soft cap on a group commit. Once the scratch buffer reaches this, the loop
/// stops absorbing and writes what it has.
const MAX_BATCH: usize = 4 << 20;

/// Blocking reads in flight, by default. Bounded because `spawn_blocking`'s own
/// pool is far larger than what a disk can usefully service at once.
pub const DEFAULT_READ_CONCURRENCY: usize = 32;

type AckResult = Result<(), Arc<io::Error>>;

enum Cmd {
    Append {
        data: Vec<u8>,
        ack: oneshot::Sender<AckResult>,
    },
    /// Contributes no bytes; just forces the batch it lands in to sync.
    Sync { ack: oneshot::Sender<AckResult> },
}

pub fn spawn<S: Storage>(store: S) -> (Writer, ReadHandle<S::Reader>, Handle<S>) {
    let start = store.len();

    let len = Arc::new(AtomicU64::new(start));

    let reader = ReadHandle {
        inner: store.reader(),
        len: Arc::clone(&len),
        reads: Arc::new(Semaphore::new(DEFAULT_READ_CONCURRENCY)),
    };

    let (tx, rx) = mpsc::channel(4096);
    let join = std::thread::Builder::new()
        .name("storage-writer".into())
        .spawn(move || writer_loop(store, rx, len))
        .expect("spawn writer thread");

    let writer = Writer {
        tx,
        buffered_size: Arc::new(Semaphore::new(DEFAULT_QUEUED_BYTES)),
    };

    (writer, reader, Handle { join })
}

fn writer_loop<S: Storage>(
    mut s: S,
    mut rx: mpsc::Receiver<Cmd>,
    durable_bytes: Arc<AtomicU64>,
) -> (S, Option<Arc<io::Error>>) {
    // Private to this loop. `visible` trails it, and only by a synced batch.
    let mut scratch: Vec<u8> = Vec::with_capacity(FRAME_MAX_SIZE as usize);
    let mut waiters: Vec<oneshot::Sender<AckResult>> = Vec::new();
    let mut poison: Option<Arc<io::Error>> = None;

    while let Some(cmd) = rx.blocking_recv() {
        absorb(cmd, &mut scratch, &mut waiters);

        while let Ok(cmd) = rx.try_recv()
            && scratch.len() < MAX_BATCH
        {
            absorb(cmd, &mut scratch, &mut waiters);
        }

        let res: AckResult = match poison.clone() {
            Some(e) => Err(e),
            None => commit(&mut s, &scratch, &durable_bytes).map_err(|e| {
                let e = Arc::new(e);
                poison = Some(Arc::clone(&e));
                e
            }),
        };

        for w in waiters.drain(..) {
            let _ = w.send(res.clone());
        }

        if let Some(cause) = poison.clone() {
            let offset = durable_bytes.load(Ordering::Relaxed);
            poison = match s.truncate(offset) {
                Ok(()) => None,
                Err(e) => Some(Arc::new(io::Error::other(format!(
                    "{cause}; repair failed: {e}"
                )))),
            };
        }

        scratch.clear();
    }

    (s, poison)
}

fn commit<S: Storage>(s: &mut S, batch: &[u8], visible: &AtomicU64) -> io::Result<()> {
    let written = s.append(batch)?;
    visible.fetch_add(written, Ordering::Release);
    Ok(())
}

fn absorb(cmd: Cmd, scratch: &mut Vec<u8>, waiters: &mut Vec<oneshot::Sender<AckResult>>) {
    match cmd {
        Cmd::Append { mut data, ack } => {
            scratch.append(&mut data);
            waiters.push(ack);
        }
        Cmd::Sync { ack } => {
            waiters.push(ack);
        }
    }
}

pub const DEFAULT_QUEUED_BYTES: usize = 32 << 20;

/// Cloned per task. Every clone feeds the same writer thread.
#[derive(Clone)]
pub struct Writer {
    tx: mpsc::Sender<Cmd>,
    buffered_size: Arc<Semaphore>,
}

impl Writer {
    pub async fn append(&self, data: Vec<u8>) -> io::Result<()> {
        if data.len() > FRAME_MAX_SIZE as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame to big".to_string(),
            ));
        }

        let _permit = Arc::clone(&self.buffered_size)
            .acquire_many_owned(data.len() as u32)
            .await
            .map_err(|_| stopped())?;

        self.dispatch(|ack| Cmd::Append { data, ack }).await
    }

    pub async fn sync(&self) -> io::Result<()> {
        self.dispatch(|ack| Cmd::Sync { ack }).await
    }

    async fn dispatch(
        &self,
        make: impl FnOnce(oneshot::Sender<AckResult>) -> Cmd + Send,
    ) -> io::Result<()> {
        let (ack, done) = oneshot::channel();
        self.tx.send(make(ack)).await.map_err(|_| stopped())?;
        match done.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(clone_err(&e)),
            Err(_) => Err(stopped()),
        }
    }
}

/// Cloned per request. Cloning is two refcount bumps.
/// Every read is clamped to the visibility watermark, so a reader can never
/// observe bytes that a crash would take back.
#[derive(Clone)]
pub struct ReadHandle<R> {
    inner: R,
    len: Arc<AtomicU64>,
    reads: Arc<Semaphore>,
}

impl<R: Reader> ReadHandle<R> {
    pub fn len(&self) -> u64 {
        self.len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let end = self.len();
        if offset >= end {
            return Ok(0);
        }
        let want = ((end - offset) as usize).min(buf.len());
        if want == 0 {
            return Ok(0);
        }
        self.inner.read_at(offset, &mut buf[..want])
    }

    /// [`read_at`](Self::read_at) from async code, returning what was visible.
    pub async fn read(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.blocking(move |r| {
            let mut buf = vec![0u8; len];
            let n = r.read_at(offset, &mut buf)?;
            buf.truncate(n);
            Ok(buf)
        })
        .await
    }

    async fn blocking<T, F>(&self, f: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Self) -> io::Result<T> + Send + 'static,
    {
        let permit = Arc::clone(&self.reads)
            .acquire_owned()
            .await
            .map_err(|_| io::Error::other("read semaphore closed"))?;
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(&this)
        })
        .await
        .map_err(|e| io::Error::other(format!("blocking read task failed: {e}")))?
    }
}

/// Owns the writer thread. Never cloned.
pub struct Handle<S> {
    join: std::thread::JoinHandle<(S, Option<Arc<io::Error>>)>,
}

impl<S: Send + 'static> Handle<S> {
    /// Wait for the writer thread to finish and surface its last error.
    ///
    /// Drop every [`Writer`] clone first, or this waits forever: the loop exits
    /// when the command channel closes. `Drop` cannot report an error, which is
    /// why this exists.
    pub async fn close(self) -> io::Result<S> {
        tokio::task::spawn_blocking(move || match self.join.join() {
            Ok((store, None)) => Ok(store),
            Ok((_, Some(e))) => Err(clone_err(&e)),
            Err(_) => Err(io::Error::other("storage writer thread panicked")),
        })
        .await
        .map_err(|e| io::Error::other(format!("join task failed: {e}")))?
    }

    /// Blocking [`close`](Self::close), for shutdown paths with no runtime.
    pub fn close_blocking(self) -> io::Result<S> {
        match self.join.join() {
            Ok((store, None)) => Ok(store),
            Ok((_, Some(e))) => Err(clone_err(&e)),
            Err(_) => Err(io::Error::other("storage writer thread panicked")),
        }
    }
}

fn stopped() -> io::Error {
    io::Error::other("storage writer stopped")
}

/// `io::Error` is not `Clone`, and the batch fan-out needs one error per
/// waiter.
fn clone_err(e: &io::Error) -> io::Error {
    match e.raw_os_error() {
        Some(code) => io::Error::from_raw_os_error(code),
        None => io::Error::new(e.kind(), e.to_string()),
    }
}
