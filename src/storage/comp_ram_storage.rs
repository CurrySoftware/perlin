use std::fs::File;
use std::sync::Arc;
use std::marker::PhantomData;

use utils::persistence::{Persistent, Volatile};

pub struct CompressedRamStorage<T> {
    entries: Vec<(u64, u32)>,
    data: Vec<u8>,
    current_offset: u64,
    current_id: u64,
    data_file: Option<File>,
    entries_file: Option<File>,
    _item_type: PhantomData<T>
}
