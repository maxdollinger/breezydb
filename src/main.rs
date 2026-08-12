mod file;
mod record;

use core::time;
use std::fs;
use std::os::unix::fs::FileExt;
use std::thread::sleep;
use std::{
    io::{self},
    sync::atomic::{AtomicU16, AtomicU64},
    time::Instant,
};

use crate::{file::DbFile, record::Record};

const MB: u64 = 1024 * 1024;
const MAX_RECORD_SIZE: usize = 2 * 1024;
const MAX_TXN_CNT: u16 = 20;
static REC_SEQ: AtomicU64 = AtomicU64::new(1);
static FILE_SEQ: AtomicU16 = AtomicU16::new(1);

fn create_txn(r_buf: &mut [u8], t_buf: &mut [u8]) -> usize {
    let mut pos: usize = 0;
    let txn_seq = REC_SEQ.load(std::sync::atomic::Ordering::SeqCst);
    let txn_cnt = rand::random_range(1..=MAX_TXN_CNT);
    for _ in 0..txn_cnt {
        let seq = REC_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let rand = rand::random_range(32..=MAX_RECORD_SIZE);
        let schema = txn_cnt % 10;
        rand::fill(&mut r_buf[..rand]);

        let rec = Record::new(seq, txn_seq, txn_cnt, schema as u64, &r_buf[..rand]);
        pos += rec.write(&mut t_buf[pos..]).unwrap();
    }

    pos
}

fn create_syntetic_db(file_size: u64, rec_cnt: u64) -> io::Result<()> {
    let mut file = DbFile::new(
        file_size,
        FILE_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        REC_SEQ.load(std::sync::atomic::Ordering::Relaxed),
    )?;

    let mut txn_buf = vec![0u8; MAX_TXN_CNT as usize * MAX_RECORD_SIZE];
    let mut r_buf = [0u8; MAX_RECORD_SIZE];

    loop {
        let pos = create_txn(r_buf.as_mut_slice(), txn_buf.as_mut_slice());

        if !file.has_space(pos) {
            let start = Instant::now();
            // seal should be done async takes > 60ms => create new file continue writing async seal
            // old one
            file.seal()?;
            println!(
                "seq: {}, rec_cnt: {}, max_seq: {}, seal took: {:.3?}",
                file.seq,
                file.max_seq - file.min_seq,
                file.max_seq,
                start.elapsed()
            );

            file = DbFile::new(
                1024 * MB,
                FILE_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                REC_SEQ.load(std::sync::atomic::Ordering::Relaxed),
            )?;
        }

        let seq = REC_SEQ.load(std::sync::atomic::Ordering::Relaxed) - 1;
        file.write(&txn_buf[..pos], seq)?;

        if seq >= rec_cnt {
            break;
        }
    }

    let start = Instant::now();
    file.seal()?;
    println!(
        "seq: {}, rec_cnt: {}, max_seq: {}, seal took: {:.3?}",
        file.seq,
        file.max_seq - file.min_seq,
        file.max_seq,
        start.elapsed()
    );

    Ok(())
}

fn find_breezy_files(dir: &str) -> std::io::Result<Vec<String>> {
    let mut matches = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("breezy") {
            matches.push(path.to_string_lossy().into_owned());
        }
    }
    Ok(matches)
}

pub fn main() -> io::Result<()> {
    println!("=== create db ===");
    let start = Instant::now();

    create_syntetic_db(2048 * MB, 2000000)?;

    let elapsed = start.elapsed();

    let seq = REC_SEQ.load(std::sync::atomic::Ordering::Relaxed) - 1;
    println!(
        "run took {:.3?} -> write speed {:.2} records/s",
        elapsed,
        seq as f64 / elapsed.as_secs_f64()
    );

    println!("=== init db ===");

    let file_names = find_breezy_files("data")?;
    let mut files: Vec<DbFile> = Vec::with_capacity(file_names.len() + 5);

    for name in file_names {
        println!("{name}");
        let start = Instant::now();
        files.push(DbFile::from_name(name)?);
        println!("file open took: {:.3?}", start.elapsed());

        let f = files.last().unwrap();
        println!(
            "seq: {}, min_seq: {} max_seq: {}",
            f.seq, f.min_seq, f.max_seq
        );
    }

    //Test read all records from first file
    let start = Instant::now();
    let mut seq_idx: Vec<(u64, u32, u32)> = Vec::with_capacity(500000);
    let f = files.first().unwrap();
    let mut rb = vec![0u8; 5 * 1024 * 1024];
    let mut pos: usize = 2;

    loop {
        if pos >= (f.written - 21) {
            break;
        }

        let n = f.file.read_at(rb.as_mut_slice(), pos as u64)?;
        let rec_buf = &rb[..n];
        let mut rec_pos: usize = 0;
        loop {
            let rec = match Record::from_slice(&rec_buf[rec_pos..]) {
                Ok(rec) => rec,
                Err(e) if e.kind() == io::ErrorKind::StorageFull => {
                    break;
                }
                Err(e) => {
                    println!("{:?}", e);
                    return Err(io::ErrorKind::Other.into());
                }
            };

            seq_idx.push((
                rec.header.seq,
                (pos + rec_pos) as u32,
                rec.total_len() as u32,
            ));
            rec_pos += rec.total_len();
        }

        pos += rec_pos;
    }

    println!("Scan took: {:.3?}", start.elapsed());

    sleep(time::Duration::from_secs(5));

    let start = Instant::now();
    let lookup_seq = rand::random_range(f.min_seq..=f.max_seq);
    let pos = match seq_idx.binary_search_by_key(&lookup_seq, |&(a, _, _)| a) {
        Ok(pos) => pos,
        Err(_) => {
            println!("seq {lookup_seq} is not in the index");
            return Err(io::ErrorKind::NotFound.into());
        }
    };
    let rec_idx = seq_idx.get(pos).unwrap();

    println!("file idx is {:?} ", rec_idx);

    let mut buf: Vec<u8> = Vec::with_capacity(MAX_RECORD_SIZE);
    buf.resize(rec_idx.2 as usize, 0u8);
    f.file.read_exact_at(buf.as_mut_slice(), rec_idx.1 as u64)?;

    let rec = Record::from_slice(buf.as_slice())?;

    println!("Record lookup took: {:.3?}", start.elapsed());

    let m = rec.header.seq == rec_idx.0;
    println!("Records did match: {}", m);

    Ok(())
}
