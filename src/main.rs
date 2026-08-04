mod blockfile;
mod frame;
mod record;

use std::{
    env::args,
    error::Error,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    blockfile::{preallocate, sync_frame, write_frame},
    frame::{FRAME_DATA_SIZE, FRAME_HEADER_LEN, FRAME_LEN, open_frame, seal_frame},
    record::{RECORD_HASH_LEN, RECORD_HEADER_LEN, write_record},
};

const MISSING_SIZE_ARG: &str = "--size [value] is requiered";
const MISSING_SIZE_VALUE: &str = "--size requieres a value like 500M or 2G";
const INVALID_SIZE_VALUE: &str = "size must be specified like [number][B | K | M | G]";

const SUPER_BLOCK: usize = 128;

fn get_size_arg() -> Result<String, Box<dyn Error>> {
    let mut args = args();

    args.position(|a| a == "--size").ok_or(MISSING_SIZE_ARG)?;
    args.next().ok_or(MISSING_SIZE_VALUE.into())
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

fn main() -> Result<(), Box<dyn Error>> {
    let file_size_arg = get_size_arg().and_then(parse_size_args)?;
    let frame_cnt = (file_size_arg - SUPER_BLOCK) / FRAME_LEN;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("get current utc system time");

    let file_name = format!("{}.brezzy", now.as_secs());
    let file_size = frame_cnt * FRAME_LEN;

    println!(
        "filename: {}.db ; size: soll {}Bytes / ist {}Bytes",
        now.as_secs(),
        file_size_arg,
        file_size,
    );

    let started = Instant::now();
    println!("start file creation: {} frames", frame_cnt);

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(file_name)?;

    preallocate(&file, file_size as u64)?;

    let record_len = RECORD_HEADER_LEN + RECORD_HASH_LEN + 24;
    let record_cnt = FRAME_DATA_SIZE / record_len;
    let mut rec_seq: u64 = 1;
    let mut frame = [0u8; FRAME_LEN];
    let mut rec_data = vec![0u8; 24 * record_cnt];
    rand::fill(rec_data.as_mut_slice());

    for i in 0..frame_cnt {
        open_frame(frame.as_mut_slice(), i as u64, i as u64, record_len as u64).expect("new frame");
        let frame_data = &mut frame[FRAME_HEADER_LEN..];

        for c in 0..record_cnt {
            let rec_start = c * record_len;
            let rec_end = rec_start + record_len;
            let rec_d_start = 24 * c;
            let rec_d_end = rec_d_start + 24;
            write_record(
                &mut frame_data[rec_start..rec_end],
                rec_seq,
                &rec_data[rec_d_start..rec_d_end],
            )
            .expect("write record");
            rec_seq += 1;
        }

        seal_frame(frame.as_mut_slice());
        write_frame(&file, i as u64, &frame).expect("write frame to file");
        sync_frame(&file)?;
        frame.fill(0u8);
    }

    let elapsed = started.elapsed();
    let records_written = rec_seq - 1;
    println!(
        "end file creation: {} records in {:.3?} ({:.2} MiB/s, {:.0} records/s)",
        records_written,
        elapsed,
        (frame_cnt * FRAME_LEN) as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        records_written as f64 / elapsed.as_secs_f64(),
    );

    Ok(())
}
