enum DataType {
    Int,
    Uint,
    String,
    Bool,
    Float,
    Blob,
    TimeStamp,
}

struct Field {
    name: [u8; 64],
    offset: u32,
    size: u32,
    ty: DataType,
}

struct Schema {
    name: [u8; 64],
    version: u16,
    fields: Vec<Field>,
}
