use std::error::Error;
use std::io::Write;

use crc32c::crc32c;

const FRAME_MAGIC_BYTE: &[u8; 1] = b"F";
pub const FRAME_LEN: usize = 64 * 1024;
pub const FRAME_HEADER_LEN: usize = 25;
const FRAME_SEQ_OFFSET: usize = 9;
const FRAME_REC_LEN_OFFSET: usize = 17;
const FRAME_HASH_LEN: usize = 32;
pub const FRAME_HASH_OFFSET: usize = FRAME_LEN - FRAME_HASH_LEN;
pub const FRAME_DATA_SIZE: usize = FRAME_LEN - FRAME_HEADER_LEN - FRAME_HASH_LEN;
pub const FRAME_WSEQ_OFFSET: usize = FRAME_LEN - 16;

pub fn open_frame(
    mut frame: &mut [u8],
    idx: u64,
    seq: u64,
    record_len: u64,
) -> Result<(), Box<dyn Error>> {
    frame.write_all(FRAME_MAGIC_BYTE)?;
    frame.write_all(&idx.to_le_bytes())?;
    frame.write_all(&seq.to_le_bytes())?;
    frame.write_all(&record_len.to_le_bytes())?;

    frame.fill(0u8);

    Ok(())
}

pub fn get_frame_record_len(frame: &[u8]) -> Result<u64, Box<dyn Error>> {
    Ok(u64::from_le_bytes(
        frame[FRAME_REC_LEN_OFFSET..FRAME_REC_LEN_OFFSET + 8].try_into()?,
    ))
}

pub fn get_frame_seq(frame: &[u8]) -> Result<u64, Box<dyn Error>> {
    Ok(u64::from_le_bytes(
        frame[FRAME_SEQ_OFFSET..FRAME_SEQ_OFFSET + 8].try_into()?,
    ))
}

pub fn get_min_max_wseq(frame: &[u8]) -> Result<(u64, u64), Box<dyn Error>> {
    let min = u64::from_le_bytes(frame[FRAME_WSEQ_OFFSET..FRAME_WSEQ_OFFSET + 8].try_into()?);
    let max = u64::from_le_bytes(frame[FRAME_WSEQ_OFFSET + 8..].try_into()?);

    Ok((min, max))
}

pub fn seal_frame(frame: &mut [u8], min_seq: u64, max_seq: u64) {
    let hash = crc32c(&frame[..FRAME_HASH_OFFSET]);

    frame[FRAME_HASH_OFFSET..FRAME_HASH_OFFSET + 4].copy_from_slice(&hash.to_le_bytes());
    frame[FRAME_HASH_OFFSET + 4..FRAME_WSEQ_OFFSET].fill(0u8);
    frame[FRAME_WSEQ_OFFSET..FRAME_WSEQ_OFFSET + 8].copy_from_slice(&min_seq.to_le_bytes());
    frame[FRAME_WSEQ_OFFSET + 8..].copy_from_slice(&max_seq.to_le_bytes());
}

pub fn verify_frame(frame: &[u8]) -> Result<(), Box<dyn Error>> {
    if frame[0] != FRAME_MAGIC_BYTE[0] {
        return Err("magic byte does not match".into());
    }

    let stored_hash = u32::from_le_bytes(
        frame[FRAME_HASH_OFFSET..FRAME_HASH_OFFSET + 4]
            .try_into()
            .unwrap(),
    );

    let computed_hash = crc32c(&frame[..FRAME_HASH_OFFSET]);

    if stored_hash != computed_hash {
        return Err("hashes do not match".into());
    }

    Ok(())
}
