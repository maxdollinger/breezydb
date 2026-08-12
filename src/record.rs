use std::io::{self};

use crc32c::crc32c;

#[derive(Debug)]
pub struct RecordHeader {
    pub magic: u8,
    pub len: u32,
    pub seq: u64,
    pub txn: u64,
    pub txn_cnt: u16,
    pub schema_seq: u64,
}

impl RecordHeader {
    pub const HEADER_SIZE: usize = 31;

    pub fn from_slice(src: &[u8]) -> Result<Self, io::Error> {
        let h_buf = src
            .first_chunk::<{ RecordHeader::HEADER_SIZE }>()
            .ok_or(io::ErrorKind::StorageFull)?;

        let (chunk, b) = h_buf.split_first_chunk::<1>().unwrap();
        let magic = chunk[0];

        let (chunk, b) = b.split_first_chunk::<4>().unwrap();
        let len = u32::from_le_bytes(*chunk);

        let (chunk, b) = b.split_first_chunk::<8>().unwrap();
        let seq = u64::from_le_bytes(*chunk);

        let (chunk, b) = b.split_first_chunk::<8>().unwrap();
        let txn = u64::from_le_bytes(*chunk);

        let (chunk, b) = b.split_first_chunk::<2>().unwrap();
        let txn_cnt = u16::from_le_bytes(*chunk);

        let (chunk, _) = b.split_first_chunk::<8>().unwrap();
        let schema_seq = u64::from_le_bytes(*chunk);

        Ok(RecordHeader {
            magic,
            len,
            seq,
            txn,
            txn_cnt,
            schema_seq,
        })
    }

    pub fn write(&self, target: &mut [u8]) -> Result<usize, io::Error> {
        let buf = target
            .first_chunk_mut::<{ RecordHeader::HEADER_SIZE }>()
            .ok_or(io::ErrorKind::StorageFull)?;

        buf[0] = self.magic;
        buf[1..5].copy_from_slice(&self.len.to_le_bytes());
        buf[5..13].copy_from_slice(&self.seq.to_le_bytes());
        buf[13..21].copy_from_slice(&self.txn.to_le_bytes());
        buf[21..23].copy_from_slice(&self.txn_cnt.to_le_bytes());
        buf[23..31].copy_from_slice(&self.schema_seq.to_le_bytes());

        Ok(RecordHeader::HEADER_SIZE)
    }
}

#[derive(Debug)]
pub struct Record<'a> {
    pub header: RecordHeader,
    pub data: &'a [u8],
    pub hash: u32,
}

impl<'a> Record<'a> {
    pub const RECORD_MAGIC: u8 = b'R';
    pub const HASH_SIZE: usize = 4;

    pub fn new(seq: u64, txn: u64, txn_cnt: u16, schema_seq: u64, data: &'a [u8]) -> Self {
        Record {
            header: RecordHeader {
                magic: Record::RECORD_MAGIC,
                len: data.len() as u32,
                seq,
                txn,
                txn_cnt,
                schema_seq,
            },
            data,
            hash: 0,
        }
    }

    pub fn total_len(&self) -> usize {
        RecordHeader::HEADER_SIZE + self.data.len() + Record::HASH_SIZE
    }

    pub fn from_slice(src: &'a [u8]) -> Result<Self, io::Error> {
        let (header_slice, src) = src.split_at(RecordHeader::HEADER_SIZE);

        let header = RecordHeader::from_slice(header_slice)?;

        let needed_len = header.len as usize + Record::HASH_SIZE;
        if src.len() < needed_len {
            return Err(io::ErrorKind::StorageFull.into());
        }

        let (data, b) = src[..needed_len].split_last_chunk::<4>().unwrap();
        let hash = u32::from_le_bytes(*b);

        Ok(Record { header, data, hash })
    }

    pub fn write(&self, target: &mut [u8]) -> Result<usize, io::Error> {
        let rec_len = self.total_len();
        if target.len() < rec_len {
            return Err(io::ErrorKind::StorageFull.into());
        }

        let mut pos = 0_usize;
        pos += self.header.write(target)?;
        target[pos..pos + self.data.len()].copy_from_slice(self.data);
        pos += self.data.len();

        let hash = crc32c(&target[..pos]);
        target[pos..pos + Record::HASH_SIZE].copy_from_slice(&hash.to_le_bytes());
        pos += Record::HASH_SIZE;

        Ok(pos)
    }
}
