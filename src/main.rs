use std::{error::Error, sync::atomic::AtomicU64};

use crc32c::crc32c;

#[derive(Debug)]
struct Record<'a> {
    magic: u8,
    seq: u64,
    txn: u64,
    txn_cnt: u16,
    schema_seq: u64,
    data: &'a [u8],
    hash: u32,
}

impl<'a> Record<'a> {
    fn new(txn: u64, txn_cnt: u16, schema_seq: u64, data: &'a [u8]) -> Self {
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        Record {
            magic: b'R',
            seq,
            txn,
            txn_cnt,
            schema_seq,
            data,
            hash: 0,
        }
    }

    pub fn from_slice(src: &'a [u8]) -> Result<Self, Box<dyn Error>> {
        let mut cursor = 0;
        let magic: u8 = u8::from_le_bytes(src[cursor..cursor + 1].try_into()?);
        cursor += 1;

        if magic != b'R' {
            return Err("Not valid record magic number".into());
        }

        let len = u32::from_le_bytes(src[cursor..cursor + 4].try_into()?);
        cursor += 4;
        let seq = u64::from_le_bytes(src[cursor..cursor + 8].try_into()?);
        cursor += 8;
        let txn = u64::from_le_bytes(src[cursor..cursor + 8].try_into()?);
        cursor += 8;
        let txn_cnt = u16::from_le_bytes(src[cursor..cursor + 2].try_into()?);
        cursor += 2;
        let schema_seq = u64::from_le_bytes(src[cursor..cursor + 8].try_into()?);
        cursor += 8;
        let data = &src[cursor..(len as usize - 4)];
        cursor = len as usize - 4;
        let hash = u32::from_le_bytes(src[cursor..cursor + 4].try_into()?);

        Ok(Record {
            magic,
            seq,
            txn,
            txn_cnt,
            schema_seq,
            data,
            hash,
        })
    }

    fn len(&self) -> usize {
        1 + 4 + 8 + 8 + 2 + 8 + self.data.len() + 4
    }

    fn write(&self, target: &mut [u8]) -> Result<usize, Box<dyn Error>> {
        let len: usize = self.len();

        if target.len() < len {
            return Err(format!(
                "write targte has not enough space! wanted: {}bytes; actual: {}bytes",
                len,
                target.len()
            )
            .into());
        }

        target[0..1].copy_from_slice(&self.magic.to_le_bytes());
        target[1..5].copy_from_slice(&(len as u32).to_le_bytes());
        target[5..13].copy_from_slice(&self.seq.to_le_bytes());
        target[13..21].copy_from_slice(&self.txn.to_le_bytes());
        target[21..23].copy_from_slice(&self.txn_cnt.to_le_bytes());
        target[23..31].copy_from_slice(&self.schema_seq.to_le_bytes());
        target[31..(31 + self.data.len())].copy_from_slice(self.data);

        let hash = crc32c(&target[..len - 4]);
        println!("seq: {}, hash: {}", self.seq, hash);
        target[(len - 4)..len].copy_from_slice(&hash.to_le_bytes());

        Ok(len)
    }
}

static SEQ: AtomicU64 = AtomicU64::new(1);

pub fn main() {
    let mut file = [0u8; 65_536];
    let mut write_cursor: usize = 0;
    let mut buf = [0u8; 4096];

    loop {
        let seq = SEQ.load(std::sync::atomic::Ordering::Relaxed);
        let rand = rand::random::<u8>() as usize;
        rand::fill(&mut buf[..rand]);

        let rec = Record::new(seq, 1, 1, &buf[..rand]);
        write_cursor += rec.write(&mut file[write_cursor..]).unwrap();
        println!("cursor: {}", write_cursor);

        if seq >= 10 {
            break;
        }
    }
    println!("wrote {}bytes to file buf", write_cursor);

    let mut read_cursor = 0_usize;
    while read_cursor < write_cursor {
        let rec = match Record::from_slice(&file[read_cursor..]) {
            Ok(rec) => {
                let computed_hash = crc32c(&file[read_cursor..(read_cursor + rec.len() - 4)]);
                if computed_hash != rec.hash {
                    eprintln!("hashes do not match");
                }
                rec
            }
            Err(e) => {
                eprintln!("Failed getting record from slice: {e}");
                break;
            }
        };

        println!("seq: {}, hash: {}", rec.seq, rec.hash);
        read_cursor += rec.len();
    }
}
