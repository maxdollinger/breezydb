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
use std::ops::Range;

pub mod file;
pub mod frame;
mod writer;

pub use writer::{DEFAULT_READ_CONCURRENCY, Handle, ReadHandle, Writer, spawn};

/// Whether a scan should keep going.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Continue,
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

    /// Durable byte count.
    fn len(&self) -> u64;

    fn is_empty(&self) -> bool;

    fn truncate(&mut self, offset: u64) -> io::Result<()>;

    /// A read handle onto the same data. Called once, before the store moves
    /// onto the writer thread.
    fn reader(&self) -> Self::Reader;
}

/// The read side. Blocking, positional, and shared freely.
pub trait Reader: Send + Sync + Clone + 'static {
    /// Read at `offset` into `buf`, returning the byte count. A short read is
    /// not an error; `0` means end of data.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Returns the offset the scan stopped at.
    fn scan_each<F>(&self, range: Range<u64>, buf: &mut [u8], mut f: F) -> io::Result<u64>
    where
        F: FnMut(u64, &[u8]) -> io::Result<Step>,
        Self: Sized,
    {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut pos = range.start;
        while pos < range.end {
            let want = ((range.end - pos) as usize).min(buf.len());
            let n = self.read_at(pos, &mut buf[..want])?;
            if n == 0 {
                break;
            }
            let step = f(pos, &buf[..n])?;
            pos += n as u64;
            if step == Step::Stop {
                break;
            }
        }

        Ok(pos)
    }
}
