mod file;
mod record;

use std::fs;
use std::path::PathBuf;
use std::{
    io::{self},
    sync::atomic::{AtomicU16, AtomicU64},
    time::Instant,
};

use crate::{file::DbFile, record::Record};

const MB: u64 = 1024 * 1024;
const MAX_RECORD_SIZE: usize = 2 * 1024;
static REC_SEQ: AtomicU64 = AtomicU64::new(1);
static FILE_SEQ: AtomicU16 = AtomicU16::new(1);

fn create_txn(arena: &mut [u8]) -> usize {
    let (r_buf, t_buf) = arena.split_at_mut(MAX_RECORD_SIZE);
    let mut pos: usize = 0;
    let txn_seq = REC_SEQ.load(std::sync::atomic::Ordering::SeqCst);
    let txn_cnt = rand::random_range(1..=20);
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

fn create_syntetic_db(rec_cnt: u64) -> io::Result<()> {
    let mut file = DbFile::new(
        512 * MB,
        FILE_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        REC_SEQ.load(std::sync::atomic::Ordering::Relaxed),
    )?;

    let mut txn_buf = vec![0u8; 256 * 64 * 1024];

    loop {
        let pos = create_txn(txn_buf.as_mut_slice());

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

    create_syntetic_db(1000000)?;

    let elapsed = start.elapsed();

    let seq = REC_SEQ.load(std::sync::atomic::Ordering::Relaxed) - 1;
    println!(
        "run took {:.3?} -> {}",
        elapsed,
        seq as f64 / elapsed.as_secs_f64()
    );

    println!("=== init db ===");

    let file_names = find_breezy_files("data")?;
    let mut files: Vec<DbFile> = Vec::with_capacity(file_names.len() + 5);

    for name in file_names {
        println!("{name}");
        files.push(DbFile::from_name(name)?);
        let f = files.last().unwrap();
        println!("read min_seq: {} max_seq: {}", f.min_seq, f.max_seq);
        println!("bytes written to file: {}", f.written);
    }

    Ok(())
}
