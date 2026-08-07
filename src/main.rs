mod blockfile;
mod extsort;
mod frame;
mod pkindex;
mod record;

use std::{
    env::args,
    error::Error,
    fs::File,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    thread::sleep,
    time::{self, Instant},
};

use rand::{self, RngExt};

use crate::{
    blockfile::{barrier_sync, preallocate, write_frame},
    extsort::EntrySorter,
    frame::{
        FRAME_DATA_SIZE, FRAME_HEADER_LEN, FRAME_LEN, get_frame_record_len, get_frame_seq,
        get_min_max_wseq, open_frame, seal_frame, verify_frame,
    },
    pkindex::PkIndex,
    record::{RECORD_HASH_LEN, RECORD_HEADER_LEN, get_record_seq, verify_record, write_record},
};

const INVALID_SIZE_VALUE: &str = "size must be specified like [number][B | K | M | G]";

const SUPER_BLOCK: usize = 128;

/// In-memory budget for the index build's sort stage. Everything beyond it spills.
const DEFAULT_INDEX_MEM: usize = 64 * 1024 * 1024;

const LOOKUPS: u32 = 200;

fn get_arg(name: &str) -> Option<String> {
    let mut args = args();

    args.position(|a| a == name)?;

    args.next()
}

fn has_flag(name: &str) -> bool {
    args().any(|a| a == name)
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

struct ScanStats {
    frames: usize,
    sort_runs: usize,
    scan: time::Duration,
    build: time::Duration,
}

/// Scans every frame and writes the pk index to `index_path` as it goes. The index replaces
/// the in-memory `Vec<(pk, frame_idx)>` this used to return: resident cost is now the sort
/// budget rather than 8 bytes times the record count.
fn verify_file(
    file: &File,
    index_path: &Path,
    sort_budget: usize,
    cache_pages: usize,
) -> Result<(PkIndex, ScanStats), Box<dyn Error>> {
    let size = file.metadata()?.len() as usize;
    let frame_cnt = size / FRAME_LEN;

    let spill_path = index_path.with_extension("sort");
    let mut sorter = EntrySorter::new(sort_budget, &spill_path);

    let scan_started = Instant::now();
    let mut frame = [0u8; FRAME_LEN];

    for i in 0..frame_cnt {
        file.read_exact_at(frame.as_mut_slice(), (i * FRAME_LEN) as u64)?;
        verify_frame(frame.as_slice()).map_err(|e| format!("frame {i}: {e}"))?;

        let rec_len = get_frame_record_len(&frame)? as usize;
        if rec_len == 0 || rec_len > FRAME_DATA_SIZE {
            return Err(format!("frame {i}: invalid record len {rec_len}").into());
        }

        let rec_cnt = FRAME_DATA_SIZE / rec_len;
        let frame_data = &frame[FRAME_HEADER_LEN..FRAME_HEADER_LEN + FRAME_DATA_SIZE];

        for r in 0..rec_cnt {
            let start = r * rec_len;
            let rec = &frame_data[start..start + rec_len];

            let pk = u64::from_le_bytes(rec[RECORD_HEADER_LEN..RECORD_HEADER_LEN + 8].try_into()?);

            sorter.push(pk as u32, i as u32)?;
        }
    }

    let scan = scan_started.elapsed();

    let build_started = Instant::now();
    let mut builder = pkindex::Builder::create(index_path, size as u64, frame_cnt as u32)?;
    let sort_runs = sorter.drain(|pk, frame_idx| builder.push(pk, frame_idx))?;
    let index = builder.finish(cache_pages)?;
    let build = build_started.elapsed();

    Ok((
        index,
        ScanStats {
            frames: frame_cnt,
            sort_runs,
            scan,
            build,
        },
    ))
}

fn find_record(
    file: &File,
    index: &mut PkIndex,
    pk: u32,
) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
    let Some(idx) = index.lookup(pk)? else {
        return Ok(None);
    };

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
            return Ok(Some(rec.to_vec()));
        }
    }

    // The index pointed at this frame, so the record has to be in it.
    Err(format!("pk {pk} not in frame {idx} the index points at").into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let file_size_arg = match get_arg("--size") {
        Some(arg) => parse_size_args(arg)?,
        None => 0,
    };

    let verify = match get_arg("--verify") {
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

        let index_path = PathBuf::from(format!("{file_name}.idx"));
        let sort_budget = match get_arg("--index-mem") {
            Some(arg) => parse_size_args(arg)?,
            None => DEFAULT_INDEX_MEM,
        };
        let cache_pages = match get_arg("--index-cache") {
            Some(arg) => parse_size_args(arg)? / pkindex::PAGE_LEN,
            None => pkindex::DEFAULT_CACHE_PAGES,
        };
        let mutations: u32 = match get_arg("--mutate") {
            Some(arg) => arg.parse()?,
            None => 0,
        };

        let size = file.metadata()?.len();
        let frame_cnt = (size as usize / FRAME_LEN) as u32;

        // A clean index over this exact file is still good, so the scan can be skipped
        // entirely. Anything else — dirty, stale, missing — means rebuild.
        let existing = match has_flag("--reuse-index") {
            true => match PkIndex::open(&index_path, size, frame_cnt, cache_pages) {
                Ok(index) => Some(index),
                Err(e) => {
                    println!("rebuilding index: {e}");
                    None
                }
            },
            false => None,
        };

        let started = Instant::now();
        let mut index = match existing {
            Some(index) => {
                println!("reused index: {} records", index.entry_cnt());
                index
            }
            None => {
                let (index, stats) = verify_file(&file, &index_path, sort_budget, cache_pages)?;

                let scanned = stats.frames * FRAME_LEN;
                println!(
                    "start scan took {:.3?} ({:.2} MiB/s): {} frames, {} records",
                    stats.scan,
                    scanned as f64 / (1024.0 * 1024.0) / stats.scan.as_secs_f64(),
                    stats.frames,
                    index.entry_cnt(),
                );
                println!(
                    "index build took {:.3?} ({} sort runs, {} leaves, height {}, {:.2}MiB on disk)",
                    stats.build,
                    stats.sort_runs,
                    index.leaf_cnt(),
                    index.height(),
                    index.size_on_disk() as f64 / (1024.0 * 1024.0),
                );
                index
            }
        };
        println!("startup total {:.3?}", started.elapsed());

        if index.entry_cnt() == 0 {
            return Ok(());
        }

        let (min_key, max_key) = index.key_bounds();
        let mut rng = rand::rng();
        let mut hits = 0u32;

        let start = Instant::now();
        for _ in 0..LOOKUPS {
            let pk = rng.random_range(min_key..=max_key);

            if let Some(record) = find_record(&file, &mut index, pk)? {
                // verify_record(record.as_slice())?;
                hits += 1;
                std::hint::black_box(&record);
            }
        }
        let elapsed = start.elapsed();

        println!(
            "random lookups in avg {:.3?} ({hits}/{LOOKUPS} hit)",
            elapsed / LOOKUPS
        );

        if mutations > 0 {
            let pages_before = index.page_cnt();

            // Repointing existing keys never splits; appending past max_key always lands in
            // the rightmost leaf, which is the worst case for split churn.
            let start = Instant::now();
            for i in 0..mutations {
                let pk = rng.random_range(min_key..=max_key);
                index.upsert(pk, rng.random_range(0..frame_cnt))?;

                if i % 4 == 0 {
                    index.upsert(max_key + 1 + i / 4, frame_cnt - 1)?;
                }
            }
            let elapsed = start.elapsed();
            let ops = mutations + mutations.div_ceil(4);

            println!(
                "{ops} writes in {:.3?} ({:.0} writes/s, {} pages allocated, height {})",
                elapsed,
                ops as f64 / elapsed.as_secs_f64(),
                index.page_cnt() - pages_before,
                index.height(),
            );

            let start = Instant::now();
            index.flush()?;
            println!("index flush took {:.3?}", start.elapsed());
        }

        let (reads, writes) = index.cache_stats();
        println!("index cache: {reads} page reads, {writes} page writes");
    }

    sleep(time::Duration::from_secs(10));

    Ok(())
}
