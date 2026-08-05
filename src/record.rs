use std::array::TryFromSliceError;
use std::error::Error;

use crc32c::crc32c;

pub const RECORD_HEADER_LEN: usize = 8;
pub const RECORD_HASH_LEN: usize = 32;

pub fn write_record(slot: &mut [u8], write_seq: u64, data: &[u8]) -> Result<usize, Box<dyn Error>> {
    let body_len = RECORD_HEADER_LEN + data.len();
    let record_len = body_len + RECORD_HASH_LEN;

    slot[..8].copy_from_slice(&write_seq.to_le_bytes());
    slot[8..body_len].copy_from_slice(data);

    let hash = crc32c(&slot[..body_len]);

    slot[body_len..body_len + 4].copy_from_slice(&hash.to_le_bytes());
    slot[body_len + 4..record_len].fill(0u8);

    Ok(record_len)
}

pub fn verify_record(record: &[u8]) -> Result<(), Box<dyn Error>> {
    if record.len() < RECORD_HASH_LEN + RECORD_HEADER_LEN + 1 {
        return Err("Record is to short".into());
    }

    let computed_hash = crc32c(&record[..record.len() - RECORD_HASH_LEN]);
    let stored_hash = match get_record_hash(record) {
        Ok(hash) => hash,
        Err(_) => return Err("could not get hash from record".into()),
    };

    match computed_hash == stored_hash {
        true => Ok(()),
        false => Err(format!(
            "hashes are not equal stored: {}, computed: {}",
            stored_hash, computed_hash
        )
        .into()),
    }
}

pub fn get_record_hash(record: &[u8]) -> Result<u32, TryFromSliceError> {
    let hash_offset = record.len() - RECORD_HASH_LEN;
    Ok(u32::from_le_bytes(
        record[hash_offset..hash_offset + 4].try_into()?,
    ))
}

#[cfg(test)]
mod test {
    use super::*;

    const DATA_LEN: usize = 24;
    const RECORD_LEN: usize = RECORD_HEADER_LEN + DATA_LEN + RECORD_HASH_LEN;

    fn rand_u8_data() -> [u8; DATA_LEN] {
        let mut data = [0u8; DATA_LEN];
        rand::fill(&mut data);

        data
    }

    #[test]
    fn test_write_valid_record() {
        let mut frame = [0u8; RECORD_LEN * 4];

        let data = rand_u8_data();

        _ = write_record(&mut frame[..RECORD_LEN], 1, &data).expect("should write second record");

        let record = &frame[..RECORD_LEN];

        assert!(&frame[RECORD_LEN * 2..].iter().all(|b| *b == 0_u8));

        assert!(verify_record(record).is_ok());
    }

    #[test]
    fn test_fail_on_invalid_record() {
        let mut record = [0u8; RECORD_LEN];
        let data = rand_u8_data();

        _ = write_record(&mut record, 1, &data).expect("should write second record");

        if record[RECORD_HEADER_LEN + 1] <= 1 {
            record[RECORD_HEADER_LEN + 1] = 42_u8
        } else {
            record[RECORD_HEADER_LEN + 1] = 0;
        }

        assert!(
            verify_record(&record)
                .unwrap_err()
                .to_string()
                .starts_with("hashes are not equal")
        );
    }
}
