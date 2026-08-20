use std::io;

use crate::data::util::{
    copy_slice_at, u8_first_chunk, u8_write_at, u32_first_chunk, u32_write_at, u64_first_chunk,
    u64_write_at,
};

#[derive(Clone, Copy, Default, Debug)]
pub struct Record<'a> {
    len: u32,
    seq: u64,
    schema_seq: u64,
    data: &'a [u8],
}

impl<'a> Record<'a> {
    pub const HEADER_SIZE: usize = 21;
    pub const MAX_SIZE: usize = 4 << 20;
    pub const TYPE: u8 = 1;

    pub fn new(seq: u64, schema_seq: u64, data: &'a [u8]) -> io::Result<Self> {
        let record_len = data.len() + Record::HEADER_SIZE;
        if record_len > Record::MAX_SIZE {
            return Err(io::Error::new(io::ErrorKind::StorageFull, "payload to big"));
        }

        Ok(Record {
            len: record_len as u32,
            seq,
            schema_seq,
            data,
        })
    }

    pub fn size(&self) -> usize {
        self.data.len() + Record::HEADER_SIZE
    }

    pub fn decode(src: &'a [u8]) -> io::Result<Self> {
        if src.len() < Record::HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "record shorter than a header",
            ));
        }

        let (len, rest) = u32_first_chunk(src);
        let (typ, rest) = u8_first_chunk(rest);
        if typ != Record::TYPE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "type field is not record type",
            ));
        }

        let (seq, rest) = u64_first_chunk(rest);
        let (schema_seq, data) = u64_first_chunk(rest);

        if data.len() != len as usize - Record::HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "record data len is false",
            ));
        }

        Ok(Record {
            len,
            seq,
            schema_seq,
            data,
        })
    }

    pub fn encode(&self, t: &'a mut [u8]) -> io::Result<usize> {
        if t.len() < self.size() {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "not enough space for record",
            ));
        }

        let mut pos = 0;
        pos += u32_write_at(t, pos, self.len);
        pos += u8_write_at(t, pos, Record::TYPE);
        pos += u64_write_at(t, pos, self.seq);
        pos += u64_write_at(t, pos, self.schema_seq);
        pos += copy_slice_at(t, pos, self.data);

        Ok(pos)
    }
}
