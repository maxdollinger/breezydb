//! Append-only storage: one writer, many readers.
//!
//! The design is two traits and three handles.
//!
//! [`Storage`] takes `&mut self` for writes; [`Reader`] takes `&self` for
//! reads. That asymmetry is the whole thing — it is why writes are serialized
//! onto a single thread and reads are not serialized at all.
//!
//! The three handles are produced together by [`spawn`]:
//!
//! | Type | Cloned | Purpose |
//! |---|---|---|
//! | [`Writer`] | per task | wraps an `mpsc::Sender`, async `append` |
//! | [`ReadHandle`] | per request | wraps a `Reader` plus the visibility watermark |
//! | [`Handle`] | never | owns the writer thread, returns the store on shutdown |
//!
//! # Invariants
//!
//! - **`visible <= pos <= file size`.** `pos` is private to the writer loop;
//!   `visible` is published only after `sync()` succeeds. The gap between them
//!   is the crash-losable window, and readers are fenced out of it.
//! - **One writer of the watermark, N readers.** `Release` store in the loop,
//!   `Acquire` load in [`ReadHandle`]. No lock.
//! - **Poison on failure.** A failed `append` leaves `pos` unknown, so the loop
//!   records the error and fails everything after it. `visible` simply stops
//!   advancing, hiding the bad tail.
//! - **Recovery is derived, not stored.** See [`frame`].
//! - **Shutdown is explicit.** Drop every [`Writer`] clone, then await
//!   [`Handle::close`] to surface the last error. `Drop` cannot do this, which
//!   is why `close` exists.

use std::io;

pub mod file;
pub mod storage;

pub use file::{FileReader, FileStorage};
pub use storage::{DEFAULT_READ_CONCURRENCY, Handle, Writer, spawn};

/// What a scan should do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Keep going, reading from this **absolute** offset next.
    ///
    /// It has to be strictly past the offset the callback was handed, or the
    /// same bytes come back forever. [`Reader::scan_all`] rejects anything else
    /// rather than spinning on it.
    Continue(u64),
    /// Stop here.
    ///
    /// [`Reader::scan_all`] then reports the offset the final chunk started at,
    /// since a callback that stops partway through one cannot say how far into
    /// it it got.
    Stop,
}

/// The write side. Blocking, and exclusively owned by the writer thread.
///
/// Implementations see opaque bytes: framing is applied before `append` is
/// called. The one exception is recovery, which has to understand frames in
/// order to find the last intact one.
pub trait Storage: Send + 'static {
    type Reader: Reader;

    // apends are always durable
    fn append(&mut self, data: &[u8]) -> io::Result<u64>;

    fn append_sync(&mut self, data: &[u8]) -> io::Result<u64>;

    fn durable_pos(&self) -> u64;

    fn pos(&self) -> u64;

    fn truncate(&mut self, offset: u64) -> io::Result<()>;

    /// A read handle onto the same data. Called once, before the store moves
    /// onto the writer thread.
    fn reader(&self) -> Self::Reader;
}

/// The read side. Blocking, positional, and shared freely.
pub trait Reader: Send + Sync + Clone + 'static {
    /// Read at `offset` into `buf`, returning the byte count. `0` means end of
    /// data.
    ///
    /// Implementations must fill `buf` unless the data ends first. `pread` is
    /// allowed to come up short, so this is a real obligation, not a restatement:
    ///
    /// the recovery scan decodes frames out of one chunk at a time, and a read
    /// that stopped early mid-log would be indistinguishable from a torn tail and
    /// would cut the log there.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Read from byte zero in `buf`-sized chunks, handing each to `f`.
    ///
    /// `f` owns the cursor: it receives the absolute offset the chunk starts at
    /// and says where to resume with [`Step::Continue`]. That is what lets a
    /// callback parsing records leave a partial one at the end of a chunk and
    /// pick it up whole on the next read, since chunk boundaries fall wherever
    /// the buffer happens to end.
    ///
    /// Returns the offset the scan reached.
    fn scan_all<F>(&self, buf: &mut [u8], mut f: F) -> io::Result<u64>
    where
        F: FnMut(u64, &[u8]) -> io::Result<Step>,
        Self: Sized,
    {
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "scan buffer must be non-empty",
            ));
        }

        let mut pos = 0;
        loop {
            let n = self.read_at(pos, buf)?;
            if n == 0 {
                break;
            }

            match f(pos, &buf[..n])? {
                Step::Stop => break,
                Step::Continue(resume) => {
                    // A resume point that does not advance would hand the
                    // callback the same bytes forever. `read_at` only reports
                    // `0` at EOF, so nothing else here would ever break the
                    // loop.
                    if resume <= pos {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("scan resumed at {resume}, which is not past {pos}"),
                        ));
                    }
                    pos = resume;
                }
            }
        }

        Ok(pos)
    }
}
