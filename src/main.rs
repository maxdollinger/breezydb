mod file;
mod record;

use std::{
    io::{self},
    sync::atomic::{AtomicU16, AtomicU64},
    time::Instant,
};

use crate::{file::DbFile, record::Record};

const MAX_RECORD_SIZE: usize = 64 * 1024;
static REC_SEQ: AtomicU64 = AtomicU64::new(1);
static FILE_SEQ: AtomicU16 = AtomicU16::new(1);

fn create_txn(arena: &mut [u8]) -> usize {
    let (r_buf, t_buf) = arena.split_at_mut(MAX_RECORD_SIZE);
    let mut pos: usize = 0;
    let txn_seq = REC_SEQ.load(std::sync::atomic::Ordering::SeqCst);
    let txn_cnt = rand::random_range(1..=20);
    for _ in 0..txn_cnt {
        let seq = REC_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let rand = rand::random::<u16>() as usize;
        rand::fill(&mut r_buf[..rand]);

        let rec = Record::new(seq, txn_seq, txn_cnt, 1, &r_buf[..rand]);
        pos += rec.write(&mut t_buf[pos..]).unwrap();
    }

    pos
}

fn create_syntetic_file(file: &mut DbFile) -> io::Result<usize> {
    let mut cnt: usize = 0;
    let mut txn_buf = vec![0u8; 256 * 64 * 1024];

    loop {
        let pos = create_txn(txn_buf.as_mut_slice());

        if !file.has_space(pos) {
            let start = Instant::now();
            file.seal()?;
            println!("seal took: {:.3?}", start.elapsed());
            break;
        }

        let seq = REC_SEQ.load(std::sync::atomic::Ordering::Relaxed) - 1;
        file.write(&txn_buf[..pos], seq)?;
        cnt += 1;
    }

    Ok(cnt)
}

pub fn main() -> io::Result<()> {
    let mut file = DbFile::new(
        FILE_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        REC_SEQ.load(std::sync::atomic::Ordering::Relaxed),
    )?;

    let start = Instant::now();

    let txn_cnt = create_syntetic_file(&mut file)?;

    let elapsed = start.elapsed();

    println!("run took {:.3?}", elapsed);
    println!(
        "{} records -> {:.0} records/s",
        file.max_seq,
        file.max_seq as f64 / elapsed.as_secs_f64()
    );
    println!(
        "{} txn -> {:.0} txn/s",
        txn_cnt,
        txn_cnt as f64 / elapsed.as_secs_f64()
    );
    println!(
        "write speed {:.2}mB/s",
        (file.written as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
    );

    println!("=== reading file ===");

    let f = DbFile::from_name("1.breezy".to_string())?;

    println!("read min_seq: {} max_seq: {}", f.min_seq, f.max_seq);
    println!("bytes written to file: {}", f.written);

    Ok(())
}
