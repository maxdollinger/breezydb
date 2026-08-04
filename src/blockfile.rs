use std::fs::File;
use std::io::{Error, Result};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;

use crate::frame::FRAME_LEN;

pub fn write_frame(file: &File, idx: u64, data: &[u8; FRAME_LEN]) -> Result<()> {
    file.write_all_at(data, idx * FRAME_LEN as u64)
}

/// Makes the preceding write durable. Returns whether a syscall was actually issued, so the
/// caller can count durability points without assuming what the mode does.
pub fn sync_frame(file: &File) -> Result<bool> {
    file.sync_all().map(|()| true)
    //barrier_sync(file).map(|()| true)
}

#[cfg(target_vendor = "apple")]
fn barrier_sync(file: &File) -> Result<()> {
    // Defined locally rather than taken from libc, which does not export it on every
    // apple target. Available since macOS 10.13.
    const F_BARRIERFSYNC: libc::c_int = 85;

    if unsafe { libc::fcntl(file.as_raw_fd(), F_BARRIERFSYNC) } == -1 {
        return Err(Error::last_os_error());
    }

    Ok(())
}

#[cfg(not(target_vendor = "apple"))]
fn barrier_sync(file: &File) -> Result<()> {
    file.sync_data()
}

/// Reserves real blocks for the whole file up front.
///
/// `set_len` on its own is `ftruncate`, which leaves a sparse file: blocks are then allocated
/// lazily inside every write, and each per-frame sync also has to flush the resulting
/// extent-tree metadata. Preallocating moves that work out of the timed loop and is what
/// docs/engine-design.md §2 requires ("the space must be really allocated or ENOSPC reappears
/// at write time").
pub fn preallocate(file: &File, len: u64) -> Result<()> {
    reserve_blocks(file, len)?;

    // Reserving blocks does not move EOF, so the size still has to be published.
    file.set_len(len)
}

#[cfg(target_vendor = "apple")]
fn reserve_blocks(file: &File, len: u64) -> Result<()> {
    let fd = file.as_raw_fd();
    let mut store = libc::fstore_t {
        fst_flags: (libc::F_ALLOCATECONTIG | libc::F_ALLOCATEALL) as libc::c_uint,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: len as libc::off_t,
        fst_bytesalloc: 0,
    };

    if unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut store) } != -1 {
        return Ok(());
    }

    // A contiguous run this large may simply not exist on a fragmented volume. That is
    // expected, not an error: retry allowing the allocation to be split up.
    store.fst_flags = libc::F_ALLOCATEALL as libc::c_uint;

    if unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut store) } == -1 {
        return Err(Error::last_os_error());
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn reserve_blocks(file: &File, len: u64) -> Result<()> {
    // posix_fallocate reports through its return value and does not set errno.
    match unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, len as libc::off_t) } {
        0 => Ok(()),
        err => Err(Error::from_raw_os_error(err)),
    }
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
fn reserve_blocks(_file: &File, _len: u64) -> Result<()> {
    // No portable way to reserve blocks; set_len alone still gives a correctly sized file.
    Ok(())
}
