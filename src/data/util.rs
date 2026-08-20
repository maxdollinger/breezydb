pub fn u8_first_chunk(s: &[u8]) -> (u8, &[u8]) {
    let (chunk, rest) = s.split_first_chunk::<1>().unwrap();
    (u8::from_le_bytes(*chunk), rest)
}

pub fn u8_write_at(s: &mut [u8], pos: usize, n: u8) -> usize {
    s[pos..pos + 1].copy_from_slice(&n.to_le_bytes());
    1
}

pub fn u32_first_chunk(s: &[u8]) -> (u32, &[u8]) {
    let (chunk, rest) = s.split_first_chunk::<4>().unwrap();
    (u32::from_le_bytes(*chunk), rest)
}

pub fn u32_last_chunk(s: &[u8]) -> (&[u8], u32) {
    let (head, chunk) = s.split_last_chunk::<4>().unwrap();
    (head, u32::from_le_bytes(*chunk))
}

pub fn u32_write_at(s: &mut [u8], pos: usize, n: u32) -> usize {
    s[pos..pos + 4].copy_from_slice(&n.to_le_bytes());
    4
}

pub fn u64_first_chunk(s: &[u8]) -> (u64, &[u8]) {
    let (chunk, rest) = s.split_first_chunk::<8>().unwrap();
    (u64::from_le_bytes(*chunk), rest)
}

pub fn u64_from_pos(s: &[u8], pos: usize) -> u64 {
    let chunk = s[pos..pos + 8].first_chunk::<8>().unwrap();
    u64::from_le_bytes(*chunk)
}

pub fn u64_write_at(s: &mut [u8], pos: usize, n: u64) -> usize {
    s[pos..pos + 8].copy_from_slice(&n.to_le_bytes());
    8
}

pub fn copy_slice_at<'a>(s: &'a mut [u8], pos: usize, data: &'a [u8]) -> usize {
    s[pos..pos + data.len()].copy_from_slice(data);
    data.len()
}
