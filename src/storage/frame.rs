use std::io::{self};

use crc32c::crc32c;

pub const FRAME_MAGIC: u32 = u32::from_le_bytes(*b"brzy");
/// `[u32 magic][u32 len][u64 seq]`.
pub const HEADER_LEN: usize = 16;
pub const HASH_SIZE: usize = 4;
pub const MAX_FRAME_LEN: u32 = 64 << 20;

/// On-disk size of a frame holding `payload_len` bytes.
pub fn frame_len(payload_len: usize) -> u32 {
    HEADER_LEN as u32 + payload_len as u32 + HASH_SIZE as u32
}

pub fn frame_encode_into(target: &mut [u8], seq: u64, payload: &[u8]) -> io::Result<usize> {
    let frame_len = frame_len(payload.len());

    if frame_len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("total frame size exceeded: {} Byte", frame_len),
        ));
    }

    target[..4].copy_from_slice(&FRAME_MAGIC.to_le_bytes());
    target[4..8].copy_from_slice(&frame_len.to_le_bytes());
    target[8..16].copy_from_slice(&seq.to_le_bytes());
    target[16..payload.len()].copy_from_slice(payload);

    let seal_offset = HEADER_LEN + payload.len();
    let hash = crc32c(&target[..seal_offset]);
    target[seal_offset..seal_offset + 4].copy_from_slice(&hash.to_le_bytes());

    Ok(frame_len as usize)
}

pub fn frame_decode(frame: &[u8]) -> io::Result<(u64, &[u8])> {
    let (chunk, tail) = frame.split_first_chunk::<4>().unwrap();
    let magic = u32::from_le_bytes(*chunk);

    if magic != FRAME_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame did not start with magic number".to_string(),
        ));
    }

    let (chunk, tail) = tail.split_first_chunk::<4>().unwrap();
    let len = u32::from_le_bytes(*chunk);

    let (chunk, tail) = tail.split_first_chunk::<8>().unwrap();
    let seq = u64::from_le_bytes(*chunk);

    let (payload, tail) = tail.split_at(len as usize - HEADER_LEN);

    let chunk = tail.first_chunk::<4>().unwrap();
    let stored_hash = u32::from_le_bytes(*chunk);

    let computed_hash = crc32c(&frame[..(len as usize - 4)]);

    if stored_hash != computed_hash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame corrupted hashes did not match".to_string(),
        ));
    }

    Ok((seq, payload))
}
