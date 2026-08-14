use std::{fs::File, sync::Arc};

pub struct FileStorage {
    file: Arc<File>,
    pos: u64,
}
