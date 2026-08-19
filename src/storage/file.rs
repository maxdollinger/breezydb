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

use super::{Reader, Storage};

pub struct FileStorage {
    file: Arc<File>,
    path: PathBuf,
    pos: u64,
    durable: u64,
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

        if !existed {
            sync_parent_dir(&path)?;
        }

        Ok(Self {
            file,
            path,
            pos: file_len,
            durable: file_len,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Storage for FileStorage {
    type Reader = FileReader;

    fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        if data.is_empty() {
            return Ok(0);
        }

        self.file.write_all_at(data, self.pos)?;

        self.pos += data.len() as u64;
        Ok(data.len() as u64)
    }

    fn append_sync(&mut self, data: &[u8]) -> io::Result<u64> {
        if data.is_empty() {
            self.file.sync_data()?;
            return Ok(0);
        }

        self.file.write_all_at(data, self.pos)?;
        self.pos += data.len() as u64;

        self.file.sync_data()?;
        self.durable = self.pos;
        Ok(data.len() as u64)
    }

    fn durable_pos(&self) -> u64 {
        self.durable
    }

    fn pos(&self) -> u64 {
        self.pos
    }

    fn truncate(&mut self, offset: u64) -> io::Result<()> {
        if offset > self.pos {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot truncate to {offset}, past the durable length {}",
                    self.pos
                ),
            ));
        }

        self.file.set_len(offset)?;
        self.pos = offset;
        self.file.sync_all()?;
        self.durable = self.pos;
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
