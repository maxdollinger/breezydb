mod record;

use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, Read, Write},
    sync::atomic::{AtomicU16, AtomicU64},
    time::Instant,
};

use crate::record::Record;

static REC_SEQ: AtomicU64 = AtomicU64::new(1);
static FILE_SEQ: AtomicU16 = AtomicU16::new(1);

struct DbFile {
    seq: u16,
    file: File,
    written: usize,
    min_seq: u64,
    max_seq: u64,
    hash: u32,
}

impl DbFile {
    pub const FILE_SIZE: usize = 1000 * 1024 * 1024;

    pub fn new(rec_seq: u64) -> io::Result<Self> {
        let seq = FILE_SEQ.fetch_and(1, std::sync::atomic::Ordering::SeqCst);
        let name = format!("{seq}.breezy");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&name)?;

        file.set_len(DbFile::FILE_SIZE as u64)?;

        file.write_all(&seq.to_le_bytes())?;

        Ok(DbFile {
            seq,
            file,
            written: 8,
            min_seq: rec_seq,
            max_seq: u64::MIN,
            hash: 0,
        })
    }

    pub fn write(&mut self, data: &[u8], seq: u64) -> io::Result<()> {
        self.file.write_all(data)?;
        self.file.sync_all()?;

        self.written += data.len();
        self.max_seq = seq;

        Ok(())
    }

    pub fn has_space(&self, len: usize) -> bool {
        self.written + len < DbFile::FILE_SIZE - 20
    }

    pub fn seal(&mut self) -> io::Result<()> {
        let mut footer_buf = [0_u8; 20];
        footer_buf[0..8].copy_from_slice(&self.min_seq.to_le_bytes());
        footer_buf[8..16].copy_from_slice(&self.max_seq.to_le_bytes());

        let mut reader = BufReader::with_capacity(2 * 1024 * 1024, &self.file);
        let mut crc = 0u32;
        let mut buf = [0u8; 1024 * 1024];

        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            crc = crc32c::crc32c_append(crc, &buf[..n]);
        }

        footer_buf[16..20].copy_from_slice(&crc.to_le_bytes());

        self.file.write_all(&footer_buf)?;

        Ok(())
    }
}

pub fn main() -> io::Result<()> {
    let mut cnt = 0;
    let mut txn_buf = vec![0u8; 255 * 64 * 1024];
    let mut write_cursor: usize = 0;
    let mut rec_buf = [0u8; 64 * 1024];
    let mut file_list: Vec<DbFile> = Vec::with_capacity(10);

    if file_list.is_empty() {
        file_list.push(DbFile::new(
            REC_SEQ.load(std::sync::atomic::Ordering::Relaxed),
        )?);
    }

    let file = file_list.last_mut().unwrap();
    let start = Instant::now();
    loop {
        let txn_seq = REC_SEQ.load(std::sync::atomic::Ordering::SeqCst);
        let txn_cnt = rand::random_range(1..=20);
        for _ in 0..txn_cnt {
            let seq = REC_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let rand = rand::random::<u16>() as usize;
            rand::fill(&mut rec_buf[..rand]);

            let rec = Record::new(seq, txn_seq, txn_cnt, 1, &rec_buf[..rand]);
            write_cursor += rec.write(&mut txn_buf[write_cursor..]).unwrap();
        }

        if !file.has_space(write_cursor) {
            file.seal()?;
            break;
        }

        file.write(
            &txn_buf[..write_cursor],
            REC_SEQ.load(std::sync::atomic::Ordering::Relaxed) - 1,
        )?;
        cnt += 1;
        println!("write cursor on: {}; txn_cnt at: {}", file.written, cnt);

        write_cursor = 0;
    }

    let elapsed = start.elapsed();

    println!("run took {:.3?}", elapsed);
    println!(
        "{} records -> {:.0} records/s",
        file.max_seq,
        file.max_seq as f64 / elapsed.as_secs_f64()
    );
    println!(
        "{} txn -> {:.0} txn/s",
        cnt,
        cnt as f64 / elapsed.as_secs_f64()
    );
    println!(
        "write speed {:.2}mB/s",
        (file.written as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
    );

    Ok(())
}
