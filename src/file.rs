use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::FileExt,
    time::Instant,
};

#[derive(Debug)]
pub struct DbFile {
    pub seq: u16,
    file: File,
    pub written: usize,
    pub max_size: u64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub write_ops: u8,
    pub hash: u32,
}

impl DbFile {
    pub fn new(size: u64, file_seq: u16, rec_seq: u64) -> io::Result<Self> {
        let name = format!("data/{file_seq}.breezy");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&name)?;

        file.set_len(size)?;

        file.write_all(&file_seq.to_le_bytes())?;
        file.sync_all()?;

        Ok(DbFile {
            seq: file_seq,
            file,
            written: 2,
            max_size: size,
            min_seq: rec_seq,
            max_seq: rec_seq,
            write_ops: 0,
            hash: 0,
        })
    }

    pub fn from_name(file_name: String) -> io::Result<Self> {
        let mut file = OpenOptions::new().write(true).read(true).open(&file_name)?;
        let size = file.metadata()?.len();

        let mut b = [0u8; 2];
        file.read_exact(b.as_mut_slice())?;
        let seq = u16::from_le_bytes(b);

        let mut footer = [0_u8; 21];
        file.read_exact_at(footer.as_mut_slice(), size - 21)?;

        if footer[20] != b'C' {
            println!("active file");
            return Err(io::ErrorKind::InvalidData.into());
        }

        let (b, tail) = footer.split_first_chunk::<8>().unwrap();
        let min_seq = u64::from_le_bytes(*b);

        let (b, tail) = tail.split_first_chunk::<8>().unwrap();
        let max_seq = u64::from_le_bytes(*b);

        let b = tail.first_chunk::<4>().unwrap();
        let hash = u32::from_le_bytes(*b);

        let file = DbFile {
            seq,
            file,
            written: size as usize,
            max_size: size,
            min_seq,
            max_seq,
            write_ops: 0,
            hash,
        };

        if file.calc_hash(true)? != file.hash {
            println!("hashes do not match");
            return Err(io::ErrorKind::InvalidData.into());
        }

        Ok(file)
    }

    pub fn write(&mut self, data: &[u8], seq: u64) -> io::Result<()> {
        self.file.write_all(data)?;

        self.written += data.len();
        self.max_seq = seq;
        self.write_ops += 1;

        // Test speed up: consider feature like every n txn or if last txn was n time ago
        if self.write_ops >= 100 {
            self.file.sync_all()?;
            self.write_ops = 0;
        }

        Ok(())
    }

    pub fn has_space(&self, len: usize) -> bool {
        self.written + len < self.max_size as usize - 21
    }

    pub fn seal(&mut self) -> io::Result<()> {
        let mut footer_buf = [0_u8; 21];
        footer_buf[0..8].copy_from_slice(&self.min_seq.to_le_bytes());
        footer_buf[8..16].copy_from_slice(&self.max_seq.to_le_bytes());

        let crc = self.calc_hash(false)?;

        footer_buf[16..20].copy_from_slice(&crc.to_le_bytes());
        footer_buf[20] = b'C';

        self.file.write_all(&footer_buf)?;
        self.written += 21;
        self.file.set_len(self.written as u64)?;
        self.file.sync_all()?;

        Ok(())
    }

    pub fn calc_hash(&self, without_footer: bool) -> io::Result<u32> {
        let file_size = self.written;
        let mut crc = 0u32;
        let mut buf = vec![0u8; 5 * 1024 * 1024];
        let mut pos: usize = 0;

        loop {
            let mut n_max = buf.len();
            if pos + n_max > file_size {
                n_max = file_size - pos;

                if without_footer {
                    n_max -= 21;
                }
            }

            pos += self.file.read_at(&mut buf[..n_max], pos as u64)?;

            crc = crc32c::crc32c_append(crc, &buf[..n_max]);

            if pos >= file_size || (without_footer && pos + 21 >= file_size) {
                break;
            }
        }

        Ok(crc)
    }
}
