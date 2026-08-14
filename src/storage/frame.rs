use std::io::{self};

use crc32c::crc32c;

use crate::{Reader, Step};

pub const FRAME_MAGIC: u32 = u32::from_le_bytes(*b"brzy");
/// `[u32 magic][u32 len][u64 seq]`.
pub const HEADER_LEN: usize = 16;
pub const HASH_SIZE: usize = 4;
pub const FRAME_MAX_SIZE: u32 = 4 << 20;

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

    let hash = crc32c(&target[..seal_offset]);
    target[seal_offset..seal_offset + HASH_SIZE].copy_from_slice(&hash.to_le_bytes());

    Ok(frame_len as usize)
}

pub fn frame_decode(frame: &[u8]) -> io::Result<(u64, &[u8], usize)> {
    if frame.len() < MIN_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "frame shorter than a header",
        ));
    }

    let magic = u32::from_le_bytes(frame[0..4].try_into().unwrap());
    if magic != FRAME_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame did not start with magic number",
        ));
    }

    let total = u32::from_le_bytes(frame[4..8].try_into().unwrap());
    if (total as usize) < MIN_FRAME_LEN || total > FRAME_MAX_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {total} out of range"),
        ));
    }
    let total = total as usize;
    if frame.len() < total {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "frame length overruns the buffer",
        ));
    }

    let seq = u64::from_le_bytes(frame[8..16].try_into().unwrap());

    let seal_offset = total - HASH_SIZE;
    let payload = &frame[HEADER_LEN..seal_offset];

    let stored_hash = u32::from_le_bytes(frame[seal_offset..total].try_into().unwrap());
    let computed_hash = crc32c(&frame[..seal_offset]);
    if stored_hash != computed_hash {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame corrupted hashes did not match",
        ));
    }

    Ok((seq, payload, total))
}

pub const SCAN_BUF_SIZE: usize = 4 << 20;

// A frame larger than a chunk could never be decoded: the scan would find
// nothing at its offset, stop, and silently discard it and everything after it.
const _: () = assert!(FRAME_MAX_SIZE as usize <= SCAN_BUF_SIZE);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recovered {
    pub valid_size: u64,
    pub last_seq: u64,
}

pub fn scan_valid_len<R: Reader>(r: R) -> io::Result<Recovered> {
    let mut good = 0u64;
    let mut last_seq = 0;

    let mut buf = vec![0u8; SCAN_BUF_SIZE];

    r.scan_all(buf.as_mut_slice(), |offset, chunk| {
        let mut buf_pos = 0;
        loop {
            let (seq, _, len) = match frame_decode(&chunk[buf_pos..]) {
                Err(_) => break,
                Ok(frame) => frame,
            };

            if last_seq >= seq {
                return Ok(Step::Stop);
            }

            buf_pos += len;
            last_seq = seq;
            good = offset + buf_pos as u64;
        }

        if buf_pos > 0 {
            Ok(Step::Continue(good))
        } else {
            Ok(Step::Stop)
        }
    })?;

    Ok(Recovered {
        valid_size: good,
        last_seq,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// An in-memory [`Reader`], so framing can be tested without a file.
    #[derive(Clone)]
    struct Bytes(Arc<Vec<u8>>);

    impl Reader for Bytes {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
            let Ok(offset) = usize::try_from(offset) else {
                return Ok(0);
            };
            if offset >= self.0.len() {
                return Ok(0);
            }
            let n = buf.len().min(self.0.len() - offset);
            buf[..n].copy_from_slice(&self.0[offset..offset + n]);
            Ok(n)
        }
    }

    fn encode(seq: u64, payload: &[u8]) -> Vec<u8> {
        let mut buf = vec![0u8; frame_len(payload.len()) as usize];
        let n = frame_encode_into(&mut buf, seq, payload).unwrap();
        assert_eq!(n, buf.len());
        buf
    }

    /// Payloads as `&str` so every element of a literal list has one type; the
    /// oversize tests build their logs with [`encode`] directly.
    fn log(frames: &[(u64, &str)]) -> Vec<u8> {
        frames
            .iter()
            .flat_map(|(seq, payload)| encode(*seq, payload.as_bytes()))
            .collect()
    }

    fn scan(bytes: &[u8]) -> io::Result<Recovered> {
        scan_valid_len(Bytes(Arc::new(bytes.to_vec())))
    }

    #[test]
    fn roundtrip() {
        let frame = encode(7, b"hello");
        let (seq, payload, total) = frame_decode(&frame).unwrap();
        assert_eq!(seq, 7);
        assert_eq!(payload, b"hello".as_slice());
        assert_eq!(total, frame.len());
    }

    #[test]
    fn roundtrip_empty_payload() {
        let frame = encode(1, b"");
        assert_eq!(frame.len(), MIN_FRAME_LEN);
        let (seq, payload, _) = frame_decode(&frame).unwrap();
        assert_eq!(seq, 1);
        assert!(payload.is_empty());
    }

    #[test]
    fn payload_larger_than_the_header() {
        let big = vec![b'x'; 5000];
        let frame = encode(1, &big);
        assert_eq!(frame_decode(&frame).unwrap().1, &big[..]);
    }

    #[test]
    fn decode_of_a_short_buffer_errors_instead_of_panicking() {
        let frame = encode(1, b"abc");
        for cut in 0..frame.len() {
            assert!(frame_decode(&frame[..cut]).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn decode_rejects_a_bad_magic() {
        let mut frame = encode(1, b"abc");
        frame[0] ^= 0xff;
        assert!(frame_decode(&frame).is_err());
    }

    #[test]
    fn decode_rejects_a_flipped_bit_anywhere() {
        let base = encode(3, b"abcdef");
        // Magic and length are checked structurally; seq and payload only by the
        // hash, which is why it covers the header.
        for byte in [8, 12, HEADER_LEN, base.len() - HASH_SIZE - 1] {
            let mut frame = base.clone();
            frame[byte] ^= 0x01;
            assert!(frame_decode(&frame).is_err(), "byte {byte}");
        }
    }

    #[test]
    fn encode_rejects_a_buffer_that_is_too_small() {
        let mut buf = vec![0u8; MIN_FRAME_LEN];
        assert!(frame_encode_into(&mut buf, 1, b"payload").is_err());
    }

    #[test]
    fn scan_of_an_empty_log_is_empty() {
        let rec = scan(&[]).unwrap();
        assert_eq!(rec.valid_size, 0);
        assert_eq!(rec.last_seq, 0);
    }

    #[test]
    fn scan_accepts_a_whole_log() {
        let bytes = log(&[(1, "a"), (2, "bb"), (3, "ccc"), (4, "dddd")]);
        let rec = scan(&bytes).unwrap();
        assert_eq!(rec.valid_size, bytes.len() as u64);
        assert_eq!(rec.last_seq, 4);
    }

    #[test]
    fn scan_walks_every_frame_in_one_chunk() {
        // The whole log lands in a single chunk, so this is what catches a scan
        // that only advances by the last frame it saw.
        let frames: Vec<(u64, &str)> = (1..=50u64).map(|i| (i, "payload")).collect();
        let bytes = log(&frames);
        let rec = scan(&bytes).unwrap();
        assert_eq!(rec.valid_size, bytes.len() as u64);
        assert_eq!(rec.last_seq, 50);
    }

    #[test]
    fn scan_terminates_on_a_torn_tail() {
        // A partial frame leaves bytes at the cursor that never parse. If the
        // scan asks to resume there, `read_at` keeps returning them and the loop
        // never ends.
        let mut bytes = log(&[(1, "first")]);
        let good = bytes.len() as u64;
        bytes.extend_from_slice(&encode(2, b"second")[..7]);

        let rec = scan(&bytes).unwrap();
        assert_eq!(rec.valid_size, good);
        assert_eq!(rec.last_seq, 1);
    }

    #[test]
    fn scan_terminates_on_a_log_that_is_entirely_garbage() {
        let rec = scan(&[0xab; 64]).unwrap();
        assert_eq!(rec.valid_size, 0);
        assert_eq!(rec.last_seq, 0);
    }

    #[test]
    fn scan_terminates_on_a_short_garbage_tail() {
        // Fewer bytes than a header, so nothing at the cursor can even be sized.
        let mut bytes = log(&[(1, "first")]);
        let good = bytes.len() as u64;
        bytes.extend_from_slice(&[0u8; 3]);
        assert_eq!(scan(&bytes).unwrap().valid_size, good);
    }

    #[test]
    fn scan_stops_at_a_corrupt_frame() {
        let mut bytes = log(&[(1, "first")]);
        let good = bytes.len() as u64;
        bytes.extend_from_slice(&encode(2, b"second"));
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;

        assert_eq!(scan(&bytes).unwrap().valid_size, good);
    }

    #[test]
    fn scan_stops_at_a_zero_filled_tail() {
        let mut bytes = log(&[(1, "first")]);
        let good = bytes.len() as u64;
        bytes.extend_from_slice(&[0u8; 128]);
        assert_eq!(scan(&bytes).unwrap().valid_size, good);
    }

    #[test]
    fn scan_stops_where_seq_fails_to_advance() {
        let mut bytes = log(&[(1, "a"), (2, "b")]);
        let good = bytes.len() as u64;
        // Valid frame, valid hash, but its seq went backwards.
        bytes.extend_from_slice(&encode(2, b"replayed"));

        let rec = scan(&bytes).unwrap();
        assert_eq!(rec.valid_size, good);
        assert_eq!(rec.last_seq, 2);
    }

    #[test]
    fn encode_rejects_a_frame_that_would_not_fit_a_scan_chunk() {
        // The bound the scan relies on, checked from the writing side: a frame
        // recovery could not decode must be impossible to produce.
        let big = vec![b'z'; SCAN_BUF_SIZE];
        let mut buf = vec![0u8; frame_len(big.len()) as usize];
        assert!(frame_encode_into(&mut buf, 1, &big).is_err());
    }

    #[test]
    fn the_largest_encodable_frame_fits_a_scan_chunk() {
        let payload = vec![b'z'; FRAME_MAX_SIZE as usize - MIN_FRAME_LEN];
        let frame = encode(1, &payload);
        assert_eq!(frame.len(), FRAME_MAX_SIZE as usize);
        assert!(frame.len() <= SCAN_BUF_SIZE);

        let rec = scan(&frame).unwrap();
        assert_eq!(rec.valid_size, frame.len() as u64);
        assert_eq!(rec.last_seq, 1);
    }
}
