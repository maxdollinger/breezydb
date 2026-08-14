//! A single-file backend.
//!
//! One `File`, opened read-write, shared with readers through an `Arc`. All
//! access is positional (`pwrite`/`pread`), so the file cursor is never
//! load-bearing and `len` is the only thing that says where the end is.

use std::fs::{File, OpenOptions};
use std::io::{self};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::frame;
use super::{Reader, Storage};

pub struct FileStorage {
    file: Arc<File>,
    path: PathBuf,
    len: u64,
    last_seq: u64,
}

impl FileStorage {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let existed = path.try_exists()?;

        let file = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?,
        );

        let file_len = file.metadata()?.len();
        let file_reader = FileReader { file: file.clone() };

        let recovered = frame::scan_valid_len(file_reader)?;

        if recovered.valid_size < file_len {
            file.set_len(recovered.valid_size)?;
            file.sync_all()?;
        }

        if !existed {
            sync_parent_dir(&path)?;
        }

        Ok(Self {
            file,
            path,
            len: recovered.valid_size,
            last_seq: recovered.last_seq,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }
}

impl Storage for FileStorage {
    type Reader = FileReader;

    fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        if data.is_empty() {
            self.file.sync_data()?;
            return Ok(0);
        }

        self.file.write_all_at(data, self.len)?;
        self.file.sync_data()?;

        self.len += data.len() as u64;
        Ok(data.len() as u64)
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn truncate(&mut self, offset: u64) -> io::Result<()> {
        if offset > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot truncate to {offset}, past the durable length {}",
                    self.len
                ),
            ));
        }

        self.file.set_len(offset)?;
        self.file.sync_all()?;
        self.len = offset;
        Ok(())
    }

    fn reader(&self) -> FileReader {
        FileReader {
            file: Arc::clone(&self.file),
        }
    }
}

#[derive(Clone)]
pub struct FileReader {
    file: Arc<File>,
}

impl Reader for FileReader {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let mut pos = 0;
        while pos < buf.len() {
            match self.file.read_at(&mut buf[pos..], offset + pos as u64) {
                Ok(0) => break,
                Ok(n) => pos += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(pos)
    }
}

fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::frame::{HASH_SIZE, frame_encode_into, frame_len};
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_path(tag: &str) -> PathBuf {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("breezydb-{}-{tag}-{n}.log", std::process::id()))
    }

    /// `seq` is 1-based: recovery requires it to strictly increase from a `0`
    /// sentinel, so a frame numbered 0 ends the prefix before it starts.
    fn frame(seq: u64, payload: &[u8]) -> Vec<u8> {
        assert!(seq >= 1, "seq is 1-based");
        let mut buf = vec![0u8; frame_len(payload.len()) as usize];
        frame_encode_into(&mut buf, seq, payload).unwrap();
        buf
    }

    fn physical_len(path: &Path) -> u64 {
        std::fs::metadata(path).unwrap().len()
    }

    #[test]
    fn open_creates_an_empty_log() {
        let path = temp_path("create");
        let s = FileStorage::open(&path).unwrap();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert_eq!(s.last_seq(), 0);
        assert!(path.exists());
    }

    #[test]
    fn append_returns_the_byte_count_and_advances_len() {
        let path = temp_path("append");
        let mut s = FileStorage::open(&path).unwrap();

        let batch = frame(1, b"alpha");
        assert_eq!(s.append(&batch).unwrap(), batch.len() as u64);
        assert_eq!(s.len(), batch.len() as u64);
        assert_eq!(physical_len(&path), batch.len() as u64);
    }

    #[test]
    fn an_empty_append_syncs_and_writes_nothing() {
        let path = temp_path("empty-append");
        let mut s = FileStorage::open(&path).unwrap();
        let batch = frame(1, b"alpha");
        s.append(&batch).unwrap();

        assert_eq!(s.append(&[]).unwrap(), 0);
        assert_eq!(s.len(), batch.len() as u64);
        assert_eq!(physical_len(&path), batch.len() as u64);
    }

    #[test]
    fn batched_frames_survive_a_reopen() {
        let path = temp_path("reopen");
        let mut batch = Vec::new();
        for i in 1..=5u64 {
            batch.extend_from_slice(&frame(i, format!("record-{i}").as_bytes()));
        }

        let mut s = FileStorage::open(&path).unwrap();
        s.append(&batch).unwrap();
        drop(s);

        // Five frames in one batch land in a single scan chunk, so this also
        // covers a scan that advances by the wrong amount within a chunk.
        let s = FileStorage::open(&path).unwrap();
        assert_eq!(s.len(), batch.len() as u64);
        assert_eq!(s.last_seq(), 5);
    }

    #[test]
    fn a_torn_tail_is_cut_on_open() {
        let path = temp_path("torn");
        let good = {
            let mut s = FileStorage::open(&path).unwrap();
            s.append(&frame(1, b"alpha")).unwrap()
        };

        // A crash partway through the next frame.
        let mut partial = frame(2, b"beta");
        partial.truncate(partial.len() - 2);
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&partial).unwrap();
        f.sync_all().unwrap();
        drop(f);
        assert!(physical_len(&path) > good);

        let s = FileStorage::open(&path).unwrap();
        assert_eq!(s.len(), good);
        assert_eq!(s.last_seq(), 1);
        // The cut is durable, not just in memory.
        assert_eq!(physical_len(&path), good);
    }

    #[test]
    fn a_corrupt_frame_hides_everything_after_it() {
        let path = temp_path("corrupt");
        let mut s = FileStorage::open(&path).unwrap();
        let first = frame(1, b"alpha");
        s.append(&first).unwrap();
        s.append(&frame(2, b"beta")).unwrap();
        s.append(&frame(3, b"gamma")).unwrap();
        drop(s);

        // Flip a bit in the second frame's length field.
        let mut bytes = std::fs::read(&path).unwrap();
        let target = first.len() + HASH_SIZE;
        bytes[target] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();

        let s = FileStorage::open(&path).unwrap();
        assert_eq!(s.len(), first.len() as u64);
        assert_eq!(physical_len(&path), first.len() as u64);
    }

    #[test]
    fn truncate_resets_len_and_physical_size() {
        let path = temp_path("truncate");
        let mut s = FileStorage::open(&path).unwrap();
        let first = frame(1, b"alpha");
        s.append(&first).unwrap();
        let keep = s.len();
        s.append(&frame(2, b"beta")).unwrap();

        s.truncate(keep).unwrap();
        // Both halves of the contract the writer loop depends on.
        assert_eq!(s.len(), keep);
        assert_eq!(physical_len(&path), keep);

        // And the log is usable again: the next append lands at `keep`.
        let next = frame(2, b"beta-again");
        s.append(&next).unwrap();
        assert_eq!(s.len(), keep + next.len() as u64);

        drop(s);
        let s = FileStorage::open(&path).unwrap();
        assert_eq!(s.len(), keep + next.len() as u64);
        assert_eq!(s.last_seq(), 2);
    }

    #[test]
    fn truncate_to_zero_empties_the_log() {
        let path = temp_path("truncate-zero");
        let mut s = FileStorage::open(&path).unwrap();
        s.append(&frame(1, b"alpha")).unwrap();

        s.truncate(0).unwrap();
        assert!(s.is_empty());
        assert_eq!(physical_len(&path), 0);
    }

    #[test]
    fn truncate_past_the_end_is_rejected() {
        let path = temp_path("truncate-grow");
        let mut s = FileStorage::open(&path).unwrap();
        s.append(&frame(1, b"alpha")).unwrap();
        let len = s.len();

        let err = s.truncate(len + 1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(s.len(), len);
    }

    #[test]
    fn a_reader_taken_before_the_writes_sees_them() {
        let path = temp_path("reader");
        let mut s = FileStorage::open(&path).unwrap();
        // The writer loop takes the reader once, up front, before the store
        // moves onto its thread.
        let r = s.reader();

        let batch = frame(1, b"payload");
        s.append(&batch).unwrap();

        let mut buf = vec![0u8; batch.len()];
        assert_eq!(r.read_at(0, &mut buf).unwrap(), batch.len());
        assert_eq!(buf, batch);
        // Past the end is not an error, it is zero bytes.
        assert_eq!(r.read_at(batch.len() as u64, &mut buf).unwrap(), 0);
    }

    #[test]
    fn readers_are_clones_of_one_file() {
        let path = temp_path("reader-clone");
        let mut s = FileStorage::open(&path).unwrap();
        let a = s.reader();
        let b = a.clone();
        let batch = frame(1, b"shared");
        s.append(&batch).unwrap();

        let mut buf = vec![0u8; batch.len()];
        assert_eq!(a.read_at(0, &mut buf).unwrap(), batch.len());
        let mut other = vec![0u8; batch.len()];
        assert_eq!(b.read_at(0, &mut other).unwrap(), batch.len());
        assert_eq!(buf, other);
    }
}
