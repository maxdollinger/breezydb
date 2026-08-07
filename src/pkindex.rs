//! Disk-persisted primary key index: a mutable B+tree over 4 kB pages.
//!
//! The index is a derived artifact — it can always be rebuilt by rescanning the frames — so
//! it carries **no per-page checksums**. A torn page is not something to detect and repair,
//! it is something to throw away. What the header does carry is a `clean` flag, fsynced to 0
//! before the first mutation of an uptime window and back to 1 only by an explicit `flush`.
//! An index that opens dirty was not closed, so its contents mean nothing and the caller
//! rebuilds. Source length and frame count are stamped alongside it, so an index that opens
//! clean but belongs to a different file is rejected too.
//!
//! Layout:
//!
//! ```text
//! page 0                header
//! page 1 ..             tree pages; leaves first (bulk load), then whatever splits allocate
//! ```
//!
//! A page is `[magic, level, count, next, entries…]` — 8 bytes of header, 511 8-byte entries.
//! Interior entries are `(first_key_of_child, child_page)`, so descent is "last child whose
//! separator is <= the search key" and a node split is a plain array split: no separator has
//! to be lifted out of the children. The one invariant everything rests on is that every key
//! under child `i` is below separator `i+1`; descent establishes it and splits preserve it.
//!
//! Bulk load packs to `FILL_PERCENT`, not full, so the inserts that follow have somewhere to
//! go. Deletion is lazy: the entry goes, the page stays. Underfull pages are never merged and
//! nothing is ever handed back to the allocator, which is affordable precisely because the
//! whole index gets rebuilt on the next start. Emptied pages are not leaked either — they sit
//! in the key range they always covered and take inserts again.

use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub const PAGE_LEN: usize = 4096;

const PAGE_HEADER_LEN: usize = 8;
const PAGE_BODY_LEN: usize = PAGE_LEN - PAGE_HEADER_LEN;

const MAGIC_OFFSET: usize = 0;
const LEVEL_OFFSET: usize = 1;
const COUNT_OFFSET: usize = 2;
const NEXT_OFFSET: usize = 4;

const ENTRY_LEN: usize = 8;
/// 511 with the current page size, and the fanout of every level.
pub const MAX_ENTRIES: usize = PAGE_BODY_LEN / ENTRY_LEN;

/// How full bulk load leaves a page. The remaining 30% is the budget for inserts that arrive
/// before the next rebuild; lower it and the index is smaller but splits sooner.
const FILL_PERCENT: usize = 70;
const BULK_FILL: usize = MAX_ENTRIES * FILL_PERCENT / 100;

const MAGIC_HEAD: u8 = b'X';
const MAGIC_NODE: u8 = b'N';
const MAGIC_LEAF: u8 = b'L';

const FORMAT_VERSION: u32 = 2;

/// Sibling pointer of the rightmost leaf, and the id of a pool slot holding nothing.
pub const NO_PAGE: u32 = u32::MAX;

/// Pages of buffer during bulk load, so the build does one pwrite per 256 kB.
const WRITE_BATCH_PAGES: usize = 64;

/// Ceiling on tree height, so a descent path is a stack array rather than an allocation per
/// insert. A split always halves a full node, so no node below the root ever drops under
/// `MAX_ENTRIES / 2` children and the real height cannot pass 5 for a u32 key space. 16 is
/// slack, not a design limit.
const MAX_HEIGHT: usize = 16;

/// Tree pages held in the buffer pool by default: 16 MiB. Every interior level of a tree with
/// 167M entries fits in well under a thousand pages, so the default keeps all of them
/// resident and only the leaf read of a lookup can miss.
pub const DEFAULT_CACHE_PAGES: usize = 4096;

type Page = [u8; PAGE_LEN];

// -- page accessors ----------------------------------------------------------------------

fn init_page(page: &mut Page, magic: u8, level: u8) {
    page.fill(0);
    page[MAGIC_OFFSET] = magic;
    page[LEVEL_OFFSET] = level;
    set_next(page, NO_PAGE);
}

fn get_level(page: &Page) -> u8 {
    page[LEVEL_OFFSET]
}

fn get_count(page: &Page) -> usize {
    u16::from_le_bytes(page[COUNT_OFFSET..COUNT_OFFSET + 2].try_into().unwrap()) as usize
}

fn set_count(page: &mut Page, count: usize) {
    page[COUNT_OFFSET..COUNT_OFFSET + 2].copy_from_slice(&(count as u16).to_le_bytes());
}

fn get_next(page: &Page) -> u32 {
    u32::from_le_bytes(page[NEXT_OFFSET..NEXT_OFFSET + 4].try_into().unwrap())
}

fn set_next(page: &mut Page, next: u32) {
    page[NEXT_OFFSET..NEXT_OFFSET + 4].copy_from_slice(&next.to_le_bytes());
}

fn get_key(page: &Page, slot: usize) -> u32 {
    let at = PAGE_HEADER_LEN + slot * ENTRY_LEN;
    u32::from_le_bytes(page[at..at + 4].try_into().unwrap())
}

fn get_value(page: &Page, slot: usize) -> u32 {
    let at = PAGE_HEADER_LEN + slot * ENTRY_LEN + 4;
    u32::from_le_bytes(page[at..at + 4].try_into().unwrap())
}

fn set_value(page: &mut Page, slot: usize, value: u32) {
    let at = PAGE_HEADER_LEN + slot * ENTRY_LEN + 4;
    page[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_entry(page: &mut Page, slot: usize, key: u32, value: u32) {
    let at = PAGE_HEADER_LEN + slot * ENTRY_LEN;
    page[at..at + 4].copy_from_slice(&key.to_le_bytes());
    page[at + 4..at + 8].copy_from_slice(&value.to_le_bytes());
}

fn fill_entries(page: &mut Page, entries: &[(u32, u32)]) {
    for (slot, &(key, value)) in entries.iter().enumerate() {
        put_entry(page, slot, key, value);
    }
    set_count(page, entries.len());
}

fn insert_at(page: &mut Page, count: usize, pos: usize, key: u32, value: u32) {
    let from = PAGE_HEADER_LEN + pos * ENTRY_LEN;
    let end = PAGE_HEADER_LEN + count * ENTRY_LEN;

    page.copy_within(from..end, from + ENTRY_LEN);
    put_entry(page, pos, key, value);
    set_count(page, count + 1);
}

fn remove_at(page: &mut Page, count: usize, pos: usize) {
    let from = PAGE_HEADER_LEN + (pos + 1) * ENTRY_LEN;
    let end = PAGE_HEADER_LEN + count * ENTRY_LEN;

    page.copy_within(from..end, from - ENTRY_LEN);
    set_count(page, count - 1);
}

/// Last child whose separator is <= `key`, clamped to child 0. The clamp is what lets the
/// leftmost separator be stale: anything below the whole subtree still lands in child 0.
fn descend_slot(page: &Page, count: usize, key: u32) -> usize {
    let (mut lo, mut hi) = (0usize, count);

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if get_key(page, mid) <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    lo.saturating_sub(1)
}

/// `Ok(slot)` if the key is present, `Err(slot)` with the position it would occupy if not.
fn leaf_slot(page: &Page, count: usize, key: u32) -> Result<usize, usize> {
    let (mut lo, mut hi) = (0usize, count);

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let found = get_key(page, mid);

        if found == key {
            return Ok(mid);
        }
        if found < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    Err(lo)
}

// -- buffer pool -------------------------------------------------------------------------

struct Slot {
    id: u32,
    page: Box<Page>,
    dirty: bool,
    /// Clock reference bit.
    used: bool,
}

/// Write-back page cache with clock eviction. Pages are reached through closures rather than
/// handed out as references, which keeps pinning out of the picture entirely — there is only
/// ever one page in flight.
struct Pool {
    file: File,
    slots: Vec<Slot>,
    map: HashMap<u32, usize>,
    hand: usize,
    cap: usize,
    reads: u64,
    writes: u64,
}

impl Pool {
    fn new(file: File, cap: usize) -> Self {
        let cap = cap.max(8);

        Self {
            file,
            slots: Vec::with_capacity(cap.min(1024)),
            map: HashMap::with_capacity(cap),
            hand: 0,
            cap,
            reads: 0,
            writes: 0,
        }
    }

    fn with<R>(&mut self, id: u32, f: impl FnOnce(&Page) -> R) -> io::Result<R> {
        let slot = self.slot_for(id, true)?;
        Ok(f(&*self.slots[slot].page))
    }

    fn with_mut<R>(&mut self, id: u32, f: impl FnOnce(&mut Page) -> R) -> io::Result<R> {
        let slot = self.slot_for(id, true)?;
        self.slots[slot].dirty = true;
        Ok(f(&mut *self.slots[slot].page))
    }

    /// Like `with_mut` for a page that does not exist on disk yet, so nothing is read in.
    fn alloc<R>(&mut self, id: u32, f: impl FnOnce(&mut Page) -> R) -> io::Result<R> {
        let slot = self.slot_for(id, false)?;
        self.slots[slot].dirty = true;
        Ok(f(&mut *self.slots[slot].page))
    }

    fn slot_for(&mut self, id: u32, load: bool) -> io::Result<usize> {
        if let Some(&slot) = self.map.get(&id) {
            self.slots[slot].used = true;
            return Ok(slot);
        }

        let slot = self.victim()?;

        if load {
            self.file
                .read_exact_at(self.slots[slot].page.as_mut_slice(), page_at(id))?;
            self.reads += 1;
        } else {
            self.slots[slot].page.fill(0);
        }

        self.slots[slot].id = id;
        self.slots[slot].dirty = !load;
        self.slots[slot].used = true;
        self.map.insert(id, slot);

        Ok(slot)
    }

    /// A slot with no mapping, growing the pool up to `cap` before evicting.
    fn victim(&mut self) -> io::Result<usize> {
        if self.slots.len() < self.cap {
            self.slots.push(Slot {
                id: NO_PAGE,
                page: Box::new([0u8; PAGE_LEN]),
                dirty: false,
                used: true,
            });
            return Ok(self.slots.len() - 1);
        }

        loop {
            let slot = self.hand;
            self.hand = (self.hand + 1) % self.slots.len();

            if self.slots[slot].used {
                self.slots[slot].used = false;
                continue;
            }
            if self.slots[slot].dirty {
                self.write_back(slot)?;
            }

            self.map.remove(&self.slots[slot].id);
            return Ok(slot);
        }
    }

    fn write_back(&mut self, slot: usize) -> io::Result<()> {
        let entry = &mut self.slots[slot];

        self.file.write_all_at(entry.page.as_slice(), page_at(entry.id))?;
        entry.dirty = false;
        self.writes += 1;

        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        for slot in 0..self.slots.len() {
            if self.slots[slot].dirty {
                self.write_back(slot)?;
            }
        }

        Ok(())
    }

    /// Bypasses the cache. Only the header page uses this — it is not a tree page and must
    /// be ordered against fsyncs by hand.
    fn write_direct(&self, id: u32, page: &Page) -> io::Result<()> {
        self.file.write_all_at(page, page_at(id))
    }

    fn read_direct(&self, id: u32, page: &mut Page) -> io::Result<()> {
        self.file.read_exact_at(page, page_at(id))
    }

    fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}

fn page_at(id: u32) -> u64 {
    id as u64 * PAGE_LEN as u64
}

// -- header ------------------------------------------------------------------------------

/// Everything about the tree that is not in a tree page. Kept as a struct so `Builder` and
/// `PkIndex` encode and decode it the same way.
#[derive(Clone, Copy)]
struct Header {
    root: u32,
    height: u32,
    first_leaf: u32,
    next_page: u32,
    leaf_cnt: u32,
    entry_cnt: u64,
    min_key: u32,
    max_key: u32,
    source_len: u64,
    source_frame_cnt: u32,
    clean: bool,
}

impl Header {
    fn encode(&self) -> Box<Page> {
        let mut page = Box::new([0u8; PAGE_LEN]);
        init_page(&mut page, MAGIC_HEAD, 0);

        let body = &mut page[PAGE_HEADER_LEN..];
        body[0..4].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        body[4..8].copy_from_slice(&(PAGE_LEN as u32).to_le_bytes());
        body[8..12].copy_from_slice(&self.root.to_le_bytes());
        body[12..16].copy_from_slice(&self.height.to_le_bytes());
        body[16..20].copy_from_slice(&self.first_leaf.to_le_bytes());
        body[20..24].copy_from_slice(&self.next_page.to_le_bytes());
        body[24..32].copy_from_slice(&self.entry_cnt.to_le_bytes());
        body[32..40].copy_from_slice(&self.source_len.to_le_bytes());
        body[40..44].copy_from_slice(&self.source_frame_cnt.to_le_bytes());
        body[44..48].copy_from_slice(&self.leaf_cnt.to_le_bytes());
        body[48..52].copy_from_slice(&self.min_key.to_le_bytes());
        body[52..56].copy_from_slice(&self.max_key.to_le_bytes());
        body[56] = self.clean as u8;

        page
    }

    fn decode(page: &Page) -> Result<Self, Box<dyn Error>> {
        if page[MAGIC_OFFSET] != MAGIC_HEAD {
            return Err("not a pk index file".into());
        }

        let body = &page[PAGE_HEADER_LEN..];
        let u32_at = |at: usize| u32::from_le_bytes(body[at..at + 4].try_into().unwrap());
        let u64_at = |at: usize| u64::from_le_bytes(body[at..at + 8].try_into().unwrap());

        let version = u32_at(0);
        if version != FORMAT_VERSION {
            return Err(
                format!("index format version {version}, expected {FORMAT_VERSION}").into(),
            );
        }

        let page_len = u32_at(4) as usize;
        if page_len != PAGE_LEN {
            return Err(format!("index page size {page_len}, expected {PAGE_LEN}").into());
        }

        Ok(Self {
            root: u32_at(8),
            height: u32_at(12),
            first_leaf: u32_at(16),
            next_page: u32_at(20),
            entry_cnt: u64_at(24),
            source_len: u64_at(32),
            source_frame_cnt: u32_at(40),
            leaf_cnt: u32_at(44),
            min_key: u32_at(48),
            max_key: u32_at(52),
            clean: body[56] != 0,
        })
    }
}

// -- bulk load ---------------------------------------------------------------------------

/// Batches sequentially allocated pages into one pwrite per `WRITE_BATCH_PAGES`.
struct PageWriter {
    file: File,
    buf: Vec<u8>,
    base: u32,
    next: u32,
}

impl PageWriter {
    fn new(file: File, first: u32) -> Self {
        Self {
            file,
            buf: Vec::with_capacity(WRITE_BATCH_PAGES * PAGE_LEN),
            base: first,
            next: first,
        }
    }

    fn append(&mut self, page: &Page) -> io::Result<u32> {
        let id = self.next;
        self.next += 1;

        self.buf.extend_from_slice(page);
        if self.buf.len() >= WRITE_BATCH_PAGES * PAGE_LEN {
            self.flush()?;
        }

        Ok(id)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }

        self.file.write_all_at(&self.buf, page_at(self.base))?;
        self.base = self.next;
        self.buf.clear();

        Ok(())
    }
}

/// Bulk loader for a fresh index. Keys must arrive strictly ascending; see `extsort` for
/// getting them that way.
///
/// Leaves stream straight to disk, so the only thing held in memory is one separator per
/// leaf — `entries / BULK_FILL * 8` bytes, ~3.8 MB at 167M records.
pub struct Builder {
    pages: PageWriter,
    leaf: Box<Page>,
    leaf_fill: usize,
    first_leaf: u32,
    leaf_cnt: u32,
    entry_cnt: u64,
    min_key: u32,
    max_key: u32,
    last_key: Option<u32>,
    /// `(separator, child_page)` for the level directly above the leaves.
    parents: Vec<(u32, u32)>,
    source_len: u64,
    source_frame_cnt: u32,
}

impl Builder {
    pub fn create(
        path: &Path,
        source_len: u64,
        source_frame_cnt: u32,
    ) -> Result<Self, Box<dyn Error>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // Page 0 is the header, written last once the root is known.
        let first_leaf = 1;

        let mut leaf = Box::new([0u8; PAGE_LEN]);
        init_page(&mut leaf, MAGIC_LEAF, 0);

        Ok(Self {
            pages: PageWriter::new(file, first_leaf),
            leaf,
            leaf_fill: 0,
            first_leaf,
            leaf_cnt: 0,
            entry_cnt: 0,
            min_key: 0,
            max_key: 0,
            last_key: None,
            parents: Vec::new(),
            source_len,
            source_frame_cnt,
        })
    }

    pub fn push(&mut self, key: u32, frame_idx: u32) -> Result<(), Box<dyn Error>> {
        match self.last_key {
            None => self.min_key = key,
            Some(last) if key == last => {
                return Err(format!("duplicate pk {key} in index build").into());
            }
            Some(last) if key < last => {
                return Err(format!("pk {key} arrived after {last}, input is not sorted").into());
            }
            Some(_) => {}
        }
        self.last_key = Some(key);
        self.max_key = key;

        put_entry(&mut self.leaf, self.leaf_fill, key, frame_idx);
        self.leaf_fill += 1;
        self.entry_cnt += 1;

        if self.leaf_fill == BULK_FILL {
            self.flush_leaf(false)?;
        }

        Ok(())
    }

    fn flush_leaf(&mut self, force: bool) -> io::Result<()> {
        if self.leaf_fill == 0 && !force {
            return Ok(());
        }

        let separator = if self.leaf_fill > 0 {
            get_key(&self.leaf, 0)
        } else {
            0
        };

        set_count(&mut self.leaf, self.leaf_fill);
        // Bulk-loaded leaves are contiguous, so the sibling is always the next page. The
        // final leaf is corrected in `finish`; splits maintain the links from then on.
        let id = self.pages.next;
        set_next(&mut self.leaf, id + 1);

        self.pages.append(&self.leaf)?;
        self.parents.push((separator, id));
        self.leaf_cnt += 1;

        init_page(&mut self.leaf, MAGIC_LEAF, 0);
        self.leaf_fill = 0;

        Ok(())
    }

    /// Seals the index and returns it open for reading and writing.
    pub fn finish(mut self, cache_pages: usize) -> Result<PkIndex, Box<dyn Error>> {
        // An empty index still gets a root, so lookups have a page to land on.
        self.flush_leaf(self.entry_cnt == 0)?;

        let mut level: u8 = 1;
        let mut current = std::mem::take(&mut self.parents);
        let mut page = Box::new([0u8; PAGE_LEN]);

        while current.len() > 1 {
            let mut up = Vec::with_capacity(current.len().div_ceil(BULK_FILL));

            for chunk in current.chunks(BULK_FILL) {
                init_page(&mut page, MAGIC_NODE, level);
                fill_entries(&mut page, chunk);

                let id = self.pages.append(&page)?;
                up.push((chunk[0].0, id));
            }

            current = up;
            level += 1;
        }

        let header = Header {
            root: current[0].1,
            height: level as u32,
            first_leaf: self.first_leaf,
            next_page: self.pages.next,
            leaf_cnt: self.leaf_cnt,
            entry_cnt: self.entry_cnt,
            min_key: self.min_key,
            max_key: self.max_key,
            source_len: self.source_len,
            source_frame_cnt: self.source_frame_cnt,
            clean: true,
        };

        self.pages.flush()?;
        self.patch_last_leaf()?;

        let file = self.pages.file;
        file.write_all_at(header.encode().as_slice(), 0)?;
        file.sync_all()?;

        Ok(PkIndex {
            pool: Pool::new(file, cache_pages),
            header,
        })
    }

    /// The rightmost leaf has no sibling, but `flush_leaf` cannot know which leaf is last.
    fn patch_last_leaf(&self) -> io::Result<()> {
        let id = self.first_leaf + self.leaf_cnt - 1;

        let mut page = Box::new([0u8; PAGE_LEN]);
        self.pages.file.read_exact_at(page.as_mut_slice(), page_at(id))?;
        set_next(&mut page, NO_PAGE);

        self.pages.file.write_all_at(page.as_slice(), page_at(id))
    }
}

// -- the tree ----------------------------------------------------------------------------

pub struct PkIndex {
    pool: Pool,
    header: Header,
}

impl PkIndex {
    /// Opens an existing index. Fails if it was not closed cleanly or was built over a
    /// different file — in both cases the caller's move is to rebuild, not to repair.
    pub fn open(
        path: &Path,
        source_len: u64,
        source_frame_cnt: u32,
        cache_pages: usize,
    ) -> Result<Self, Box<dyn Error>> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let pool = Pool::new(file, cache_pages);

        let mut page = Box::new([0u8; PAGE_LEN]);
        pool.read_direct(0, &mut page)?;
        let header = Header::decode(&page)?;

        if !header.clean {
            return Err("index was not closed cleanly, rebuild it".into());
        }
        if header.source_len != source_len || header.source_frame_cnt != source_frame_cnt {
            return Err(format!(
                "index is stale: built over {} bytes / {} frames, source is {source_len} bytes \
                 / {source_frame_cnt} frames",
                header.source_len, header.source_frame_cnt
            )
            .into());
        }

        Ok(Self { pool, header })
    }

    pub fn entry_cnt(&self) -> u64 {
        self.header.entry_cnt
    }

    pub fn leaf_cnt(&self) -> u32 {
        self.header.leaf_cnt
    }

    pub fn height(&self) -> u32 {
        self.header.height
    }

    pub fn page_cnt(&self) -> u32 {
        self.header.next_page
    }

    /// Key bounds. Exact after a bulk load and after inserts; deletes never narrow them, so
    /// treat them as bounds rather than as the actual extremes.
    pub fn key_bounds(&self) -> (u32, u32) {
        (self.header.min_key, self.header.max_key)
    }

    pub fn cache_stats(&self) -> (u64, u64) {
        (self.pool.reads, self.pool.writes)
    }

    pub fn size_on_disk(&self) -> u64 {
        page_at(self.header.next_page)
    }

    /// Resolves a pk to the frame holding it: `height` page reads, all but the last of which
    /// come out of the pool in steady state.
    pub fn lookup(&mut self, key: u32) -> io::Result<Option<u32>> {
        let leaf = self.descend(key)?;

        self.pool.with(leaf, |page| {
            leaf_slot(page, get_count(page), key)
                .ok()
                .map(|slot| get_value(page, slot))
        })
    }

    /// Inserts the key, or repoints an existing one. Returns true when the key was new.
    pub fn upsert(&mut self, key: u32, frame_idx: u32) -> io::Result<bool> {
        if self.header.height as usize >= MAX_HEIGHT {
            return Err(io::Error::other("pk index exceeded its height ceiling"));
        }
        self.begin_write()?;

        let mut path = [(0u32, 0usize); MAX_HEIGHT];
        let (leaf, depth) = self.descend_recording(key, &mut path)?;

        let placed = self.pool.with_mut(leaf, |page| {
            let count = get_count(page);
            match leaf_slot(page, count, key) {
                Ok(slot) => {
                    set_value(page, slot, frame_idx);
                    Some(false)
                }
                Err(slot) if count < MAX_ENTRIES => {
                    insert_at(page, count, slot, key, frame_idx);
                    Some(true)
                }
                // Full, and the key is new: the leaf has to split.
                Err(_) => None,
            }
        })?;

        match placed {
            Some(false) => return Ok(false),
            Some(true) => {}
            None => self.split_leaf(leaf, key, frame_idx, &path, depth)?,
        }

        self.header.entry_cnt += 1;
        self.note_key(key);

        Ok(true)
    }

    /// Removes the key. Returns true when it was there.
    ///
    /// Lazy: the page keeps its size and is never merged with a neighbour. See the module
    /// note on why the rebuild-on-start design makes that the right trade.
    pub fn remove(&mut self, key: u32) -> io::Result<bool> {
        self.begin_write()?;

        let leaf = self.descend(key)?;
        let removed = self.pool.with_mut(leaf, |page| {
            let count = get_count(page);
            match leaf_slot(page, count, key) {
                Ok(slot) => {
                    remove_at(page, count, slot);
                    true
                }
                Err(_) => false,
            }
        })?;

        if removed {
            self.header.entry_cnt -= 1;
        }

        Ok(removed)
    }

    /// Makes the index durable and reusable by the next process. Without this the index
    /// stays marked dirty and the next `open` rejects it.
    pub fn flush(&mut self) -> io::Result<()> {
        self.pool.flush()?;
        // Pages first: the clean flag must not become visible ahead of what it vouches for.
        self.pool.sync()?;

        self.header.clean = true;
        self.pool.write_direct(0, &self.header.encode())?;
        self.pool.sync()
    }

    fn descend(&mut self, key: u32) -> io::Result<u32> {
        let mut id = self.header.root;

        for _ in 1..self.header.height {
            id = self.pool.with(id, |page| {
                get_value(page, descend_slot(page, get_count(page), key))
            })?;
        }

        Ok(id)
    }

    /// Descent that remembers `(node, chosen slot)` per interior level, which is what a split
    /// needs to walk back up. Returns the leaf and how much of `path` it filled.
    fn descend_recording(
        &mut self,
        key: u32,
        path: &mut [(u32, usize); MAX_HEIGHT],
    ) -> io::Result<(u32, usize)> {
        let mut id = self.header.root;
        let mut depth = 0;

        for _ in 1..self.header.height {
            let (slot, child) = self.pool.with(id, |page| {
                let slot = descend_slot(page, get_count(page), key);
                (slot, get_value(page, slot))
            })?;

            path[depth] = (id, slot);
            depth += 1;
            id = child;
        }

        Ok((id, depth))
    }

    /// Clears the on-disk clean flag before the first mutation of this uptime window, and
    /// makes that durable. One fsync per window, not per write.
    fn begin_write(&mut self) -> io::Result<()> {
        if !self.header.clean {
            return Ok(());
        }

        self.header.clean = false;
        self.pool.write_direct(0, &self.header.encode())?;
        self.pool.sync()
    }

    fn note_key(&mut self, key: u32) {
        if self.header.entry_cnt == 1 {
            self.header.min_key = key;
            self.header.max_key = key;
            return;
        }
        self.header.min_key = self.header.min_key.min(key);
        self.header.max_key = self.header.max_key.max(key);
    }

    fn alloc_page(&mut self) -> u32 {
        let id = self.header.next_page;
        self.header.next_page += 1;
        id
    }

    fn split_leaf(
        &mut self,
        leaf: u32,
        key: u32,
        value: u32,
        path: &[(u32, usize); MAX_HEIGHT],
        depth: usize,
    ) -> io::Result<()> {
        let mut merged = Vec::with_capacity(MAX_ENTRIES + 1);

        let sibling = self.pool.with(leaf, |page| {
            let count = get_count(page);
            let pos = match leaf_slot(page, count, key) {
                Ok(slot) | Err(slot) => slot,
            };

            for slot in 0..pos {
                merged.push((get_key(page, slot), get_value(page, slot)));
            }
            merged.push((key, value));
            for slot in pos..count {
                merged.push((get_key(page, slot), get_value(page, slot)));
            }

            get_next(page)
        })?;

        let mid = merged.len() / 2;
        let right = self.alloc_page();

        self.pool.with_mut(leaf, |page| {
            fill_entries(page, &merged[..mid]);
            set_next(page, right);
        })?;
        self.pool.alloc(right, |page| {
            init_page(page, MAGIC_LEAF, 0);
            fill_entries(page, &merged[mid..]);
            set_next(page, sibling);
        })?;
        self.header.leaf_cnt += 1;

        self.grow(merged[0].0, merged[mid].0, right, path, depth)
    }

    /// Walks `path` back up inserting `(separator, child)` into each parent, splitting as
    /// needed, and finally raising a new root if the split reached the top.
    fn grow(
        &mut self,
        mut left_separator: u32,
        mut separator: u32,
        mut child: u32,
        path: &[(u32, usize); MAX_HEIGHT],
        mut depth: usize,
    ) -> io::Result<()> {
        let mut merged = Vec::with_capacity(MAX_ENTRIES + 1);

        while depth > 0 {
            depth -= 1;
            let (node, slot) = path[depth];

            // The new child belongs directly to the right of the one we descended into.
            let pos = slot + 1;

            let placed = self.pool.with_mut(node, |page| {
                let count = get_count(page);
                if count < MAX_ENTRIES {
                    insert_at(page, count, pos, separator, child);
                    return true;
                }
                false
            })?;

            if placed {
                return Ok(());
            }

            merged.clear();
            let level = self.pool.with(node, |page| {
                let count = get_count(page);

                for i in 0..pos {
                    merged.push((get_key(page, i), get_value(page, i)));
                }
                merged.push((separator, child));
                for i in pos..count {
                    merged.push((get_key(page, i), get_value(page, i)));
                }

                get_level(page)
            })?;

            let mid = merged.len() / 2;
            let right = self.alloc_page();

            self.pool
                .with_mut(node, |page| fill_entries(page, &merged[..mid]))?;
            self.pool.alloc(right, |page| {
                init_page(page, MAGIC_NODE, level);
                fill_entries(page, &merged[mid..]);
            })?;

            left_separator = merged[0].0;
            separator = merged[mid].0;
            child = right;
        }

        // The split propagated past the root, so the tree gets one level taller.
        let old_root = self.header.root;
        let level = self.header.height as u8;
        let new_root = self.alloc_page();

        self.pool.alloc(new_root, |page| {
            init_page(page, MAGIC_NODE, level);
            put_entry(page, 0, left_separator, old_root);
            put_entry(page, 1, separator, child);
            set_count(page, 2);
        })?;

        self.header.root = new_root;
        self.header.height += 1;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    struct TempIndex(PathBuf);

    impl TempIndex {
        fn new(name: &str) -> Self {
            let mut path = std::env::temp_dir();
            path.push(format!("breezydb-pkidx-{name}.test"));
            let _ = std::fs::remove_file(&path);
            Self(path)
        }
    }

    impl Drop for TempIndex {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn build(path: &Path, entries: &[(u32, u32)]) -> PkIndex {
        let mut builder = Builder::create(path, 4096, 1).expect("create index");
        for &(key, frame) in entries {
            builder.push(key, frame).expect("push entry");
        }
        builder.finish(64).expect("finish index")
    }

    /// Deterministic scramble, so the tests do not depend on an rng.
    fn scramble(i: u32, span: u32) -> u32 {
        i.wrapping_mul(2_654_435_761) % span
    }

    #[test]
    fn test_lookup_across_three_levels() {
        let tmp = TempIndex::new("three-levels");

        // At a bulk fill of 357 this is 841 leaves, which needs two interior levels.
        let entries: Vec<(u32, u32)> = (0..300_000u32).map(|i| (i * 2 + 1, i / 7)).collect();
        let mut index = build(&tmp.0, &entries);

        assert_eq!(index.height(), 3);
        assert_eq!(index.entry_cnt(), 300_000);
        assert_eq!(index.key_bounds(), (1, 599_999));

        for &(key, frame) in &[
            entries[0],
            entries[BULK_FILL - 1],
            entries[BULK_FILL],
            entries[150_000],
            entries[entries.len() - 1],
        ] {
            assert_eq!(index.lookup(key).expect("lookup"), Some(frame), "pk {key}");
        }

        // Below the tree, between two keys, and past the rightmost leaf.
        assert_eq!(index.lookup(0).expect("lookup"), None);
        assert_eq!(index.lookup(2).expect("lookup"), None);
        assert_eq!(index.lookup(1_000_000).expect("lookup"), None);
    }

    #[test]
    fn test_single_leaf_index() {
        let tmp = TempIndex::new("single-leaf");
        let mut index = build(&tmp.0, &[(7, 0), (9, 1), (11, 2)]);

        assert_eq!(index.height(), 1);
        assert_eq!(index.leaf_cnt(), 1);
        assert_eq!(index.lookup(9).expect("lookup"), Some(1));
        assert_eq!(index.lookup(8).expect("lookup"), None);
    }

    #[test]
    fn test_upsert_repoints_without_growing() {
        let tmp = TempIndex::new("upsert");
        let entries: Vec<(u32, u32)> = (0..10_000u32).map(|i| (i, i / 7)).collect();
        let mut index = build(&tmp.0, &entries);

        let pages = index.page_cnt();

        assert!(!index.upsert(500, 999).expect("upsert"));
        assert_eq!(index.lookup(500).expect("lookup"), Some(999));
        assert_eq!(index.entry_cnt(), 10_000);
        assert_eq!(index.page_cnt(), pages, "an update must not allocate");
    }

    #[test]
    fn test_inserts_split_leaves_and_raise_the_root() {
        let tmp = TempIndex::new("splits");

        // One leaf, filled to BULK_FILL. Every insert past MAX_ENTRIES forces a split, and
        // enough of them push a new level above the root.
        let entries: Vec<(u32, u32)> = (0..BULK_FILL as u32).map(|i| (i * 1000, i)).collect();
        let mut index = build(&tmp.0, &entries);
        assert_eq!(index.height(), 1);

        // Fill the gaps between the bulk-loaded keys.
        let mut expected = 0u64;
        for i in 0..BULK_FILL as u32 {
            for step in 1..4u32 {
                assert!(index.upsert(i * 1000 + step, i + step).expect("insert"));
                expected += 1;
            }
        }

        assert!(index.height() >= 2, "the root should have been raised");
        assert_eq!(index.entry_cnt(), BULK_FILL as u64 + expected);

        for i in 0..BULK_FILL as u32 {
            assert_eq!(index.lookup(i * 1000).expect("lookup"), Some(i));
            for step in 1..4u32 {
                assert_eq!(
                    index.lookup(i * 1000 + step).expect("lookup"),
                    Some(i + step),
                    "pk {}",
                    i * 1000 + step
                );
            }
        }
    }

    #[test]
    fn test_matches_a_btreemap_under_mixed_writes() {
        let tmp = TempIndex::new("oracle");

        const SPAN: u32 = 40_000;
        let seed: Vec<(u32, u32)> = (0..8_000u32).map(|i| (i * 3, i)).collect();
        let mut index = build(&tmp.0, &seed);
        let mut oracle: BTreeMap<u32, u32> = seed.iter().copied().collect();

        for i in 0..60_000u32 {
            let key = scramble(i, SPAN);

            match i % 5 {
                0 => {
                    let hit = index.remove(key).expect("remove");
                    assert_eq!(hit, oracle.remove(&key).is_some(), "remove {key}");
                }
                _ => {
                    let fresh = index.upsert(key, i).expect("upsert");
                    assert_eq!(fresh, oracle.insert(key, i).is_none(), "upsert {key}");
                }
            }
        }

        assert_eq!(index.entry_cnt(), oracle.len() as u64);

        for key in 0..SPAN {
            assert_eq!(
                index.lookup(key).expect("lookup"),
                oracle.get(&key).copied(),
                "pk {key}"
            );
        }
    }

    #[test]
    fn test_survives_a_reopen_after_flush() {
        let tmp = TempIndex::new("reopen");
        let seed: Vec<(u32, u32)> = (0..20_000u32).map(|i| (i * 2, i)).collect();

        let mut index = build(&tmp.0, &seed);
        for i in 0..5_000u32 {
            index.upsert(i * 2 + 1, i + 7).expect("insert");
        }
        index.remove(0).expect("remove");
        index.flush().expect("flush");
        let height = index.height();
        drop(index);

        let mut reopened = PkIndex::open(&tmp.0, 4096, 1, 64).expect("reopen");

        assert_eq!(reopened.height(), height);
        assert_eq!(reopened.entry_cnt(), 24_999);
        assert_eq!(reopened.lookup(0).expect("lookup"), None);
        assert_eq!(reopened.lookup(4).expect("lookup"), Some(2));
        assert_eq!(reopened.lookup(4_001).expect("lookup"), Some(2_007));
    }

    #[test]
    fn test_reject_index_left_dirty() {
        let tmp = TempIndex::new("dirty");

        let mut index = build(&tmp.0, &[(1, 0), (2, 0)]);
        index.upsert(3, 1).expect("insert");
        drop(index); // no flush: this is what a crash looks like

        assert!(
            PkIndex::open(&tmp.0, 4096, 1, 64)
                .unwrap_err()
                .to_string()
                .starts_with("index was not closed cleanly")
        );
    }

    #[test]
    fn test_reject_stale_index() {
        let tmp = TempIndex::new("stale");
        build(&tmp.0, &[(1, 0), (2, 0)]);

        assert!(
            PkIndex::open(&tmp.0, 8192, 2, 64)
                .unwrap_err()
                .to_string()
                .starts_with("index is stale")
        );
    }

    #[test]
    fn test_reject_unsorted_bulk_input() {
        let tmp = TempIndex::new("unsorted");

        let mut builder = Builder::create(&tmp.0, 4096, 1).expect("create index");
        builder.push(5, 0).expect("first key");

        assert!(
            builder
                .push(4, 1)
                .unwrap_err()
                .to_string()
                .contains("is not sorted")
        );
        assert!(
            builder
                .push(5, 1)
                .unwrap_err()
                .to_string()
                .starts_with("duplicate pk")
        );
    }
}
