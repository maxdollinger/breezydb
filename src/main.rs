mod blockfile;
mod frame;
mod record;

use std::{
    env::args,
    error::Error,
    fs::File,
    os::unix::fs::FileExt,
    thread::sleep,
    time::{self, Instant},
};

use rand::{self, RngExt};

use crate::{
    blockfile::{barrier_sync, preallocate, write_frame},
    frame::{
        FRAME_DATA_SIZE, FRAME_HEADER_LEN, FRAME_LEN, get_frame_record_len, get_frame_seq,
        get_min_max_wseq, open_frame, seal_frame, verify_frame,
    },
    record::{RECORD_HASH_LEN, RECORD_HEADER_LEN, get_record_seq, verify_record, write_record},
};

const INVALID_SIZE_VALUE: &str = "size must be specified like [number][B | K | M | G]";

const SUPER_BLOCK: usize = 128;

fn get_size_arg() -> Option<String> {
    let mut args = args();

    args.position(|a| a == "--size")?;

    args.next()
}

fn get_verify_arg() -> Option<String> {
    let mut args = args();

    args.position(|a| a == "--verify")?;

    args.next()
}

fn parse_size_args(mut size: String) -> Result<usize, Box<dyn Error>> {
    let multiplier: u64 = match size.pop().unwrap() {
        'B' => 1,
        'K' => 1024,
        'M' => 1024_u64.pow(2),
        'G' => 1024_u64.pow(3),
        _ => return Err(INVALID_SIZE_VALUE.into()),
    };

    let number: f64 = size.parse()?;

    Ok((multiplier as f64 * number).round() as usize)
}

fn write_file(file: &File, frame_cnt: usize) -> std::io::Result<u64> {
    let record_len = RECORD_HEADER_LEN + RECORD_HASH_LEN + 24;
    let record_cnt = FRAME_DATA_SIZE / record_len;
    let mut rec_seq: u64 = 1;
    let mut frame = [0u8; FRAME_LEN];
    let mut rec_data = [0u8; 24];

    for i in 0..frame_cnt {
        open_frame(frame.as_mut_slice(), i as u64, i as u64, record_len as u64).expect("new frame");
        let frame_data = &mut frame[FRAME_HEADER_LEN..];

        let min_rseq = rec_seq;
        for c in 0..record_cnt {
            rec_data[..8].copy_from_slice(rec_seq.to_le_bytes().as_slice());
            rand::fill(&mut rec_data[8..]);
            let rec_start = c * record_len;
            let rec_end = rec_start + record_len;
            write_record(&mut frame_data[rec_start..rec_end], rec_seq, &rec_data)
                .expect("write record");
            rec_seq += 1;
        }

        seal_frame(frame.as_mut_slice(), min_rseq, rec_seq - 1);
        write_frame(file, i as u64, &frame).expect("write frame to file");

        if i % 20 == 0 {
            file.sync_all()?;
        } else {
            barrier_sync(file)?;
        }
    }

    file.sync_all()?;
    Ok(rec_seq - 1)
}

fn verify_file(file: &File) -> Result<Vec<(u32, u32)>, Box<dyn Error>> {
    let size = file.metadata()?.len() as usize;
    let frame_cnt = size / FRAME_LEN;

    let mut frame = [0u8; FRAME_LEN];
    let mut pk_idx: Vec<(u32, u32)> = Vec::with_capacity(frame_cnt);

    for i in 0..frame_cnt {
        file.read_exact_at(frame.as_mut_slice(), (i * FRAME_LEN) as u64)?;
        verify_frame(frame.as_slice()).map_err(|e| format!("frame {i}: {e}"))?;

        let rec_len = get_frame_record_len(&frame)? as usize;
        if rec_len == 0 || rec_len > FRAME_DATA_SIZE {
            return Err(format!("frame {i}: invalid record len {rec_len}").into());
        }

        let rec_cnt = FRAME_DATA_SIZE / rec_len;
        let frame_data = &frame[FRAME_HEADER_LEN..FRAME_HEADER_LEN + FRAME_DATA_SIZE];

        if i == 0 {
            // Sized off the first frame so growing the index does not pollute the timing.
            pk_idx.reserve(frame_cnt * rec_cnt);
        }

        for r in 0..rec_cnt {
            let start = r * rec_len;
            let rec = &frame_data[start..start + rec_len];

            let pk = u64::from_le_bytes(rec[RECORD_HEADER_LEN..RECORD_HEADER_LEN + 8].try_into()?);

            pk_idx.push((pk as u32, i as u32));
        }
    }

    pk_idx.sort_by_key(|a| a.0);

    Ok(pk_idx)
}

fn find_record(file: &File, pk: u32, idx: u32) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut frame = [0u8; FRAME_LEN];
    file.read_exact_at(frame.as_mut_slice(), (idx as usize * FRAME_LEN) as u64)?;

    let rec_len = get_frame_record_len(&frame)? as usize;
    if rec_len == 0 || rec_len > FRAME_DATA_SIZE {
        return Err(format!("frame {idx}: invalid record len {rec_len}").into());
    }

    let rec_cnt = FRAME_DATA_SIZE / rec_len;
    let frame_data = &frame[FRAME_HEADER_LEN..FRAME_HEADER_LEN + FRAME_DATA_SIZE];

    for r in 0..rec_cnt {
        let start = r * rec_len;
        let rec = &frame_data[start..start + rec_len];

        let stored_pk =
            u64::from_le_bytes(rec[RECORD_HEADER_LEN..RECORD_HEADER_LEN + 8].try_into()?);

        if stored_pk == pk as u64 {
            return Ok(rec.to_vec());
        }
    }

    Err("not found".into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let file_size_arg = match get_size_arg() {
        Some(arg) => parse_size_args(arg)?,
        None => 0,
    };

    let verify = match get_verify_arg() {
        Some(arg) => arg,
        None => "".to_string(),
    };
    let is_verify = !verify.is_empty();

    let file_name = if is_verify {
        verify
    } else {
        "test.breezy".to_string()
    };

    if file_size_arg > 0 {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_name)?;

        let frame_cnt = (file_size_arg - SUPER_BLOCK) / FRAME_LEN;
        let file_size = frame_cnt * FRAME_LEN;
        println!(
            "filename: {}\nsize: {:.2}MiB\nframes: {}",
            file_name,
            file_size as f64 / (1024.0 * 1024.0),
            frame_cnt
        );

        let started = Instant::now();

        preallocate(&file, file_size as u64)?;

        let records_written = write_file(&file, frame_cnt)?;

        let elapsed = started.elapsed();
        println!(
            "end file creation: {} records in {:.3?} ({:.2} MiB/s, {:.0} records/s)",
            records_written,
            elapsed,
            file_size as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
            records_written as f64 / elapsed.as_secs_f64(),
        );
    }

    if is_verify {
        let file = std::fs::OpenOptions::new().read(true).open(&file_name)?;

        let start_read = Instant::now();
        let pk_idx = verify_file(&file)?;
        let elapsed = start_read.elapsed();

        let size = file.metadata()?.len() as usize;
        let frame_cnt = size / FRAME_LEN;
        println!(
            "start scan took {:.3?} ({:.2} MiB/s)",
            elapsed,
            (frame_cnt * FRAME_LEN) as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        );

        let start = Instant::now();
        for _ in 0..100 {
            let mut rng = rand::rng();
            let pk = rng.random_range(0..pk_idx.len());
            let idx = pk_idx
                .binary_search_by_key(&pk, |&(pk, _)| pk as usize)
                .expect("pk to exist");
            let (_, frame_idx) = pk_idx[idx];

            let record = find_record(&file, pk as u32, frame_idx)?;
            // verify_record(record.as_slice())?;
        }
        let elapsed = start.elapsed();

        println!("5 random lookups in avg {:.3?}", elapsed / 100);
    }

    sleep(time::Duration::from_secs(10));

    Ok(())
}
