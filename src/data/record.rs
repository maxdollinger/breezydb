use std::io;

use crate::data::util::{
    copy_slice_at, u8_first_chunk, u8_write_at, u32_first_chunk, u32_write_at, u64_first_chunk,
    u64_write_at,
};

#[derive(Debug, Default)]
pub struct Record {
    len: usize,
    buf: Vec<u8>,
}

impl Record {
    // [len, type, seq, schema_seq, data]
    pub const HEADER_SIZE: usize = 21;
    pub const MAX_SIZE: usize = 4 << 20;
    pub const TYPE: u8 = 1;

    pub fn new() -> Self {
        let mut vec = vec![0u8; Record::HEADER_SIZE + 500];
        u32_write_at(vec.as_mut_slice(), 0, 21);
        u8_write_at(vec.as_mut_slice(), 4, Record::TYPE);
        Record { len: 5, buf: vec }
    }

    pub fn size(&self) -> usize {
        self.len + Record::HEADER_SIZE
    }

    pub fn write_seq(&mut self, seq: u64) {
        u64_write_at(self.buf.as_mut_slice(), 5, seq);
    }

    pub fn write_schema_seq(&mut self, seq: u64) {
        u64_write_at(self.buf.as_mut_slice(), 13, seq);
    }
}
