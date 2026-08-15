use std::io::{self};

use crc32c::crc32c;

use crate::{Reader, Step};

// IDEA: A Frame is = to one transaction. The payload could be 1:n records but has a max size of 8Mb

pub const FRAME_MAGIC: u32 = u32::from_le_bytes(*b"brzy");
/// `[u32 magic][u32 len][u64 seq]`.
pub const HEADER_LEN: usize = 16;
pub const HASH_SIZE: usize = 4;
pub const FRAME_MAX_SIZE: u32 = 8 << 20;

pub const MIN_FRAME_LEN: usize = HEADER_LEN + HASH_SIZE;

pub fn frame_len(payload_len: usize) -> u32 {
    let total = HEADER_LEN as u64 + payload_len as u64 + HASH_SIZE as u64;
    u32::try_from(total).unwrap_or(u32::MAX)
}

pub fn frame_encode_into(target: &mut [u8], seq: u64, payload: &[u8]) -> io::Result<usize> {
    let frame_len = frame_len(payload.len());

    if frame_len > FRAME_MAX_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("total frame size exceeded: {} Byte", frame_len),
        ));
    }
    if target.len() < frame_len as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "frame buffer too small: {} Byte for a {frame_len} Byte frame",
                target.len()
            ),
        ));
    }

    target[..4].copy_from_slice(&FRAME_MAGIC.to_le_bytes());
    target[4..8].copy_from_slice(&frame_len.to_le_bytes());
    target[8..16].copy_from_slice(&seq.to_le_bytes());

    let seal_offset = HEADER_LEN + payload.len();
    target[HEADER_LEN..seal_offset].copy_from_slice(payload);

    let hash = crc32c(&target[4..seal_offset]);
    target[seal_offset..seal_offset + HASH_SIZE].copy_from_slice(&hash.to_le_bytes());

    Ok(frame_len as usize)
}

pub fn frame_decode_into<'a>(raw: &'a [u8], frame: &mut Frame<'a>) -> io::Result<()> {
    if raw.len() < MIN_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "frame shorter than a header",
        ));
    }

    let magic = u32::from_le_bytes(raw[0..4].try_into().unwrap());
    if magic != FRAME_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame did not start with magic number",
        ));
    }

    let total = u32::from_le_bytes(raw[4..8].try_into().unwrap());
    if (total as usize) < MIN_FRAME_LEN || total > FRAME_MAX_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {total} out of range"),
        ));
    }
    let total = total as usize;
    if raw.len() < total {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "frame length overruns the buffer",
        ));
    }

    frame.len = total as u32;

    frame.seq = u64::from_le_bytes(raw[8..16].try_into().unwrap());

    let seal_offset = total - HASH_SIZE;
    frame.payload = &raw[HEADER_LEN..seal_offset];

    let stored_hash = u32::from_le_bytes(raw[seal_offset..total].try_into().unwrap());
    let computed_hash = crc32c(&raw[..seal_offset]);
    if stored_hash != computed_hash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame corrupted hashes did not match",
        ));
    }

    Ok(())
}

pub const SCAN_BUF_SIZE: usize = 8 << 20;

// A frame larger than a chunk could never be decoded: the scan would find
// nothing at its offset, stop, and silently discard it and everything after it.
const _: () = assert!(FRAME_MAX_SIZE as usize <= SCAN_BUF_SIZE);

#[derive(Debug, Default)]
pub struct Frame<'a> {
    pub offset: u64,
    pub seq: u64,
    pub len: u32,
    pub payload: &'a [u8],
}

pub fn frame_scan_all<R: Reader, F>(r: R, mut f: F) -> io::Result<()>
where
    F: FnMut(&Frame) -> io::Result<()>,
{
    let mut good = 0u64;
    let mut last_seq = 0;

    let mut buf = vec![0u8; SCAN_BUF_SIZE];

    r.scan_all(buf.as_mut_slice(), |offset, chunk| {
        let mut buf_pos = 0;
        loop {
            let mut frame = Frame::default();
            match frame_decode_into(&chunk[buf_pos..], &mut frame) {
                Err(_) => break,
                Ok(frame) => frame,
            };

            if last_seq >= frame.seq {
                return Ok(Step::Stop);
            }

            frame.offset = offset + buf_pos as u64;
            last_seq = frame.seq;
            buf_pos += frame.len as usize;
            good = offset + buf_pos as u64;

            f(&frame)?;
        }

        if buf_pos > 0 {
            Ok(Step::Continue(good))
        } else {
            Ok(Step::Stop)
        }
    })?;

    Ok(())
}
