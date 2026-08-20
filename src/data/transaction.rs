use std::io::{self};

use crc32c::crc32c;

use crate::{
    Reader, Step,
    data::{
        record::Record,
        util::{
            copy_slice_at, u32_first_chunk, u32_last_chunk, u32_write_at, u64_from_pos,
            u64_write_at,
        },
    },
};

// A Frame is = to one transaction. The payload could be 1:n records
// [len, min_seq, max_seq, data, hash]
#[derive(Debug, Default)]
pub struct Transaction {
    len: usize,
    buf: Vec<u8>,
}

impl Transaction {
    // `[u32 len][u64 min_seq][u64 max_seq][data][u64 hash]`.
    pub const HEADER_LEN: usize = 20;
    pub const HASH_SIZE: usize = 4;
    pub const MIN_SIZE: usize = Transaction::HEADER_LEN + Transaction::HASH_SIZE;
    pub const MAX_SIZE: usize = Record::MAX_SIZE + Transaction::MIN_SIZE;

    pub fn new() -> Self {
        Transaction {
            len: 0,
            buf: vec![0u8; 2 * Transaction::MAX_SIZE],
        }
    }

    pub fn open(&mut self) {
        self.len = 0;
        let mut pos = self.len;
        let t = self.buf.as_mut_slice();
        pos += u32_write_at(t, pos, 0);
        pos += u64_write_at(t, pos, u64::MAX);
        u64_write_at(t, pos, 0);
        self.len = pos
    }

    pub fn add(&mut self, seq: (u64, u64), data: &[u8]) {
        let new_size = self.len + data.len();
        if new_size > self.buf.len() {
            self.buf.resize(new_size, 0u8);
        }

        self.set_min_seq(seq.0);
        self.set_max_seq(seq.1);

        self.len += copy_slice_at(self.buf.as_mut_slice(), self.len, data);
    }

    pub fn commit(&mut self) -> &[u8] {
        u32_write_at(self.buf.as_mut_slice(), 0, self.len as u32 + 4);
        let hash = crc32c(&self.buf[..self.len]);
        self.len += u32_write_at(self.buf.as_mut_slice(), self.len, hash);

        &self.buf[..self.len]
    }

    pub fn size(&self) -> (usize, usize) {
        (self.len, self.buf.len())
    }

    fn get_max_seq(&self) -> u64 {
        u64_from_pos(&self.buf, 4)
    }

    fn set_min_seq(&mut self, seq: u64) {
        if seq < u64_from_pos(&self.buf, 4) {
            u64_write_at(self.buf.as_mut_slice(), 4, seq);
        }
    }

    fn set_max_seq(&mut self, seq: u64) {
        if seq > u64_from_pos(&self.buf, 12) {
            u64_write_at(self.buf.as_mut_slice(), 12, seq);
        }
    }
    pub fn decode(src: &[u8]) -> io::Result<Self> {
        if src.len() < Transaction::MIN_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "transaction shorter than a header",
            ));
        }

        let (len, _) = u32_first_chunk(src);
        let frame_len = len as usize;
        if frame_len < Transaction::MIN_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame length {} out of range", frame_len),
            ));
        }
        if src.len() < frame_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "frame length overruns the buffer",
            ));
        }

        let (_, stored_hash) = u32_last_chunk(&src[..frame_len]);
        let computed_hash = crc32c(&src[..frame_len - Transaction::HASH_SIZE]);
        if stored_hash != computed_hash {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame corrupted hashes did not match",
            ));
        }

        Ok(Transaction {
            len: frame_len,
            buf: src[..frame_len].to_vec(),
        })
    }

    pub fn scan_all<R: Reader, F>(r: R, mut f: F) -> io::Result<()>
    where
        F: FnMut(&Transaction, u64) -> io::Result<()>,
    {
        let mut good = 0u64;
        let mut last_seq = 0;

        let mut buf = vec![0u8; Transaction::MAX_SIZE];

        r.scan_all(buf.as_mut_slice(), |offset, chunk| {
            let mut buf_pos = 0;
            loop {
                let txn = match Transaction::decode(&chunk[buf_pos..]) {
                    Err(_) => break,
                    Ok(txn) => txn,
                };

                if last_seq >= txn.get_max_seq() {
                    return Ok(Step::Stop);
                }

                let frame_offset = offset + buf_pos as u64;
                last_seq = txn.get_max_seq();
                buf_pos += txn.len;
                good = offset + buf_pos as u64;

                f(&txn, frame_offset)?;
            }

            if buf_pos > 0 {
                Ok(Step::Continue(good))
            } else {
                Ok(Step::Stop)
            }
        })?;

        Ok(())
    }
}
