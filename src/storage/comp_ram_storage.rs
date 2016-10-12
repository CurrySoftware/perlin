use std::fs::{File, OpenOptions};
use std::path::Path;
use std::io::Read;
use std::sync::Arc;
use std::marker::PhantomData;

use utils::persistence::{Persistent, Volatile};
use storage::{Storage, Result, StorageError};
use storage::{vbyte_encode, VByteDecoder, ByteEncodable, ByteDecodable};

const ENTRIES_FILENAME: &'static str = "entries.bin";
const DATA_FILENAME: &'static str = "data.bin";
const ASSOCIATED_FILES: &'static [&'static str; 2] = &[ENTRIES_FILENAME, DATA_FILENAME];

pub struct CompressedRamStorage<T> {
    entries: Vec<(u64, u32)>,
    data: Vec<u8>,
    current_offset: u64,
    current_id: u64,
    data_file: Option<File>,
    entries_file: Option<File>,
    _item_type: PhantomData<T>
}




impl<T> Persistent for CompressedRamStorage<T> {

    fn create(path: &Path) -> Result<Self> {
        assert!(path.is_dir(),
                "CompressedRamStorage::create expects a directory not a file!");
        Ok(CompressedRamStorage{
            entries: Vec::new(),
            data: Vec::new(),
            current_offset: 0,
            current_id: 0,
            data_file: Some(try!(OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .open(path.join(ENTRIES_FILENAME)))),
            entries_file: Some(try!(OpenOptions::new()
                                    .write(true)
                                    .create(true)
                                    .truncate(true)
                                    .open(path.join(DATA_FILENAME)))),
            _item_type: PhantomData
        })
    }

    fn load(path: &Path) -> Result<Self> {
        let mut entries = Vec::new();
        let entries_file = try!(OpenOptions::new().read(true).open(path.join(ENTRIES_FILENAME)));
        let mut decoder = VByteDecoder::new(entries_file.bytes());

        let mut current_id: u64 = 0;
        let mut current_offset: u64 = 0;
        while let Some((id, len)) = decode_entry(&mut decoder) {
            current_id += id as u64;
            entries.push((current_offset, len));
            current_offset += len as u64;            
        }

        let mut data_file = try!(OpenOptions::new()
                                 .read(true)
                                 .append(true)
                                 .open(path.join(DATA_FILENAME)));

        let mut data = Vec::new();
        try!(data_file.read_to_end(&mut data));
        Ok(CompressedRamStorage{
            current_id: current_id,
            current_offset: current_offset,
            entries: entries,
            data_file: Some(data_file),
            entries_file: Some(try!(OpenOptions::new()
                                    .append(true)
                                    .open(path.join(ENTRIES_FILENAME)))),
            data: data,                            
            _item_type: PhantomData
        })

    }

    fn associated_files() -> &'static [&'static str] {
        ASSOCIATED_FILES
    }
}

impl<T> Volatile for CompressedRamStorage<T>
{
    fn new() -> Self {
        CompressedRamStorage{
            current_id: 0,
            current_offset: 0,
            entries: Vec::new(),
            data: Vec::new(),
            data_file: None,
            entries_file: None,
            _item_type: PhantomData
        }
    }
}

impl<T: ByteDecodable + ByteEncodable + Sync + Send> Storage<T> for CompressedRamStorage<T>
{
    fn get(&self, id: u64) -> Result<Arc<T>>{
        if let Some(&(offset, len)) = self.entries.get(id as usize) {

            let mut bytes = &self.data[offset as usize..(offset+len as u64) as usize];
            let item = T::decode(&mut bytes).unwrap();
            Ok(Arc::new(item))
            
        } else {
            Err(StorageError::KeyNotFound)
        }
    }

    fn store(&mut self, id: u64, data: T) -> Result<()> {
        let mut bytes = data.encode();
        self.data.append(&mut bytes);
        self.entries.push((self.current_offset, bytes.len() as u32));

        // let entry_bytes = encode_entry(self.current_id, id, bytes.len() as u32);

        self.current_id = id;
        self.current_offset += bytes.len() as u64;
        Ok(())
    }

}


fn encode_entry(current_id: u64, id: u64, length: u32) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    bytes.append(&mut vbyte_encode((id - current_id) as usize));
    bytes.append(&mut vbyte_encode(length as usize));
    bytes
}

fn decode_entry<R: Read>(decoder: &mut VByteDecoder<R>) -> Option<(u32, u32)> {
    let delta_id = try_option!(decoder.next()) as u32;
    let length = try_option!(decoder.next()) as u32;

    Some((delta_id, length))
}


