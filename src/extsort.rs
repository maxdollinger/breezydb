//! Bounded-memory sort for `(pk, frame_idx)` pairs.
//!
//! The B+tree bulk loader needs its keys in ascending order, but the scan produces them in
//! frame order. Sorting the whole set in memory is what the on-disk index exists to avoid —
//! 167M entries is 2.5 GB resident — so entries are buffered up to a budget, sorted, spilled
//! as a run, and k-way merged back out on drain.
//!
//! If everything fits in the budget no spill file is ever created and the merge degenerates
//! into iterating the buffer.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const ENTRY_LEN: usize = 8;

/// Entries moved per read/write against the spill file. At 8 bytes each this is a 64 kB
/// buffer per run, so merge memory is `runs * 64 kB`.
const IO_CHUNK: usize = 8 * 1024;

const MIN_BUDGET_ENTRIES: usize = 4 * IO_CHUNK;

pub struct EntrySorter {
    budget: usize,
    buf: Vec<(u32, u32)>,
    spill: Option<File>,
    spill_path: PathBuf,
    spill_end: u64,
    /// `(byte offset, entry count)` per spilled run.
    runs: Vec<(u64, u64)>,
}

impl EntrySorter {
    /// `budget_bytes` caps the in-memory buffer. Everything beyond it goes through the
    /// spill file at `spill_path`, which is removed on drain or drop.
    pub fn new(budget_bytes: usize, spill_path: &Path) -> Self {
        let budget = (budget_bytes / ENTRY_LEN).max(MIN_BUDGET_ENTRIES);

        Self {
            budget,
            buf: Vec::with_capacity(budget),
            spill: None,
            spill_path: spill_path.to_path_buf(),
            spill_end: 0,
            runs: Vec::new(),
        }
    }

    pub fn push(&mut self, key: u32, value: u32) -> io::Result<()> {
        self.buf.push((key, value));

        if self.buf.len() == self.budget {
            self.flush_run()?;
        }

        Ok(())
    }

    /// Emits every entry in ascending key order. Returns how many spilled runs it merged,
    /// which is 0 when the whole set fit in the budget.
    pub fn drain<F>(mut self, mut sink: F) -> Result<usize, Box<dyn Error>>
    where
        F: FnMut(u32, u32) -> Result<(), Box<dyn Error>>,
    {
        if self.runs.is_empty() {
            self.buf.sort_unstable_by_key(|&(key, _)| key);
            for &(key, value) in self.buf.iter() {
                sink(key, value)?;
            }
            return Ok(0);
        }

        self.flush_run()?;
        self.buf.shrink_to_fit();

        let file = self.spill.take().expect("spill file after a flushed run");
        let mut cursors: Vec<Cursor> = self
            .runs
            .iter()
            .map(|&(offset, count)| Cursor::new(offset, count))
            .collect();

        let mut scratch = vec![0u8; IO_CHUNK * ENTRY_LEN];
        let mut heap: BinaryHeap<Reverse<(u32, usize)>> = BinaryHeap::with_capacity(cursors.len());

        for i in 0..cursors.len() {
            cursors[i].refill(&file, &mut scratch)?;
            if let Some((key, _)) = cursors[i].peek() {
                heap.push(Reverse((key, i)));
            }
        }

        while let Some(Reverse((_, i))) = heap.pop() {
            let (key, value) = cursors[i].peek().expect("heap entry has a cursor entry");
            cursors[i].pos += 1;

            if cursors[i].pos == cursors[i].buf.len() {
                cursors[i].refill(&file, &mut scratch)?;
            }
            if let Some((next, _)) = cursors[i].peek() {
                heap.push(Reverse((next, i)));
            }

            sink(key, value)?;
        }

        drop(file);
        let _ = std::fs::remove_file(&self.spill_path);

        Ok(self.runs.len())
    }

    fn flush_run(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }

        self.buf.sort_unstable_by_key(|&(key, _)| key);

        if self.spill.is_none() {
            self.spill = Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.spill_path)?,
            );
        }

        let file = self.spill.as_ref().expect("spill file just opened");
        let start = self.spill_end;
        let mut at = start;
        let mut bytes = Vec::with_capacity(IO_CHUNK * ENTRY_LEN);

        for chunk in self.buf.chunks(IO_CHUNK) {
            bytes.clear();
            for &(key, value) in chunk {
                bytes.extend_from_slice(&key.to_le_bytes());
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            file.write_all_at(&bytes, at)?;
            at += bytes.len() as u64;
        }

        self.runs.push((start, self.buf.len() as u64));
        self.spill_end = at;
        self.buf.clear();

        Ok(())
    }
}

impl Drop for EntrySorter {
    fn drop(&mut self) {
        if self.spill.is_some() {
            let _ = std::fs::remove_file(&self.spill_path);
        }
    }
}

/// Read cursor over one spilled run.
struct Cursor {
    at: u64,
    unread: u64,
    buf: Vec<(u32, u32)>,
    pos: usize,
}

impl Cursor {
    fn new(offset: u64, count: u64) -> Self {
        Self {
            at: offset,
            unread: count,
            buf: Vec::with_capacity(IO_CHUNK),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<(u32, u32)> {
        self.buf.get(self.pos).copied()
    }

    fn refill(&mut self, file: &File, scratch: &mut [u8]) -> io::Result<()> {
        self.buf.clear();
        self.pos = 0;

        if self.unread == 0 {
            return Ok(());
        }

        let take = IO_CHUNK.min(self.unread as usize);
        let bytes = &mut scratch[..take * ENTRY_LEN];
        file.read_exact_at(bytes, self.at)?;

        for entry in bytes.chunks_exact(ENTRY_LEN) {
            self.buf.push((
                u32::from_le_bytes(entry[..4].try_into().unwrap()),
                u32::from_le_bytes(entry[4..].try_into().unwrap()),
            ));
        }

        self.at += bytes.len() as u64;
        self.unread -= take as u64;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn spill_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("breezydb-extsort-{name}.test"));
        let _ = std::fs::remove_file(&path);
        path
    }

    /// Returns the sorted entries and how many runs the drain merged.
    fn collect(sorter: EntrySorter) -> (Vec<(u32, u32)>, usize) {
        let mut out = Vec::new();
        let runs = sorter
            .drain(|key, value| {
                out.push((key, value));
                Ok(())
            })
            .expect("drain");
        (out, runs)
    }

    /// Deterministic shuffle, so the test does not depend on an rng.
    fn scrambled(count: u32) -> Vec<(u32, u32)> {
        (0..count).map(|i| (i.wrapping_mul(2_654_435_761) % count, i)).collect()
    }

    #[test]
    fn test_sorts_in_memory_without_spilling() {
        let path = spill_path("in-memory");
        let mut sorter = EntrySorter::new(64 * 1024 * 1024, &path);

        for &(key, value) in &[(9u32, 1u32), (3, 2), (7, 3), (1, 4)] {
            sorter.push(key, value).expect("push");
        }

        let (sorted, runs) = collect(sorter);

        assert_eq!(runs, 0);
        assert_eq!(sorted, vec![(1, 4), (3, 2), (7, 3), (9, 1)]);
        assert!(!path.exists(), "no spill file for an in-memory sort");
    }

    #[test]
    fn test_merges_spilled_runs() {
        let path = spill_path("spilled");

        // The budget floor is 4 * IO_CHUNK entries, so push well past it to force runs.
        let entries = scrambled(MIN_BUDGET_ENTRIES as u32 * 5 + 17);
        let mut sorter = EntrySorter::new(0, &path);
        for &(key, value) in &entries {
            sorter.push(key, value).expect("push");
        }
        let (sorted, runs) = collect(sorter);
        assert!(runs >= 6, "expected several runs, got {runs}");

        let mut want = entries;
        want.sort_by_key(|&(key, _)| key);

        assert_eq!(sorted.len(), want.len());
        assert!(sorted.windows(2).all(|w| w[0].0 <= w[1].0));
        assert_eq!(
            sorted.iter().map(|&(k, _)| k).collect::<Vec<_>>(),
            want.iter().map(|&(k, _)| k).collect::<Vec<_>>()
        );
        assert!(!path.exists(), "spill file removed on drain");
    }

    #[test]
    fn test_empty_sorter() {
        let path = spill_path("empty");
        let sorter = EntrySorter::new(1024, &path);

        assert_eq!(collect(sorter), (Vec::new(), 0));
    }
}
