use std::fs::{File, OpenOptions};
use std::path::Path;
use std::io::Read;
use std::sync::Arc;
use std::marker::PhantomData;

use utils::persistence::{Persistent, Volatile};
use storage::{Storage, Result, StorageError};
use storage::{ByteEncodable, ByteDecodable};
use storage::compression::{vbyte_encode, VByteDecoder};

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

    fn len(&self) -> usize {
        self.entries.len()
    }
    
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
        let len = bytes.len();
        self.data.append(&mut bytes);
        self.entries.push((self.current_offset, len as u32));

        //TODO: Persist
        // let entry_bytes = encode_entry(self.current_id, id, bytes.len() as u32);

        self.current_id = id;
        self.current_offset += len as u64;
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


#[cfg(test)]
mod tests {
    use std::fs::create_dir_all;
    use std::path::Path;

    use super::*;
    use utils::persistence::{Volatile, Persistent};
    use storage::{Storage, StorageError};

    #[test]
    fn basic() {
        let item1: u32 = 15;
        let item2: u32 = 32;
        assert!(create_dir_all(Path::new("/tmp/comp_test_index")).is_ok());
        let mut prov = CompressedRamStorage::create(Path::new("/tmp/comp_test_index")).unwrap();
        assert!(prov.store(0, item1.clone()).is_ok());
        assert_eq!(prov.get(0).unwrap().as_ref(), &item1);
        assert!(prov.store(1, item2.clone()).is_ok());
        assert_eq!(prov.get(1).unwrap().as_ref(), &item2);
        assert!(prov.get(0).unwrap().as_ref() != &item2);
        assert_eq!(prov.get(0).unwrap().as_ref(), &item1);
    }

    #[test]
    pub fn comp_basic() {
        let posting1 = vec![(0, vec![0, 1, 2, 3, 4]), (1, vec![5])];
        let posting2 = vec![(0, vec![0, 1, 4]), (1, vec![5]), (5, vec![0, 24, 56])];
        let mut prov = CompressedRamStorage::new();
        assert!(prov.store(0, posting1.clone()).is_ok());
        assert_eq!(prov.get(0).unwrap().as_ref(), &posting1);
        assert!(prov.store(1, posting2.clone()).is_ok());
        assert_eq!(prov.get(1).unwrap().as_ref(), &posting2);
        assert!(prov.get(0).unwrap().as_ref() != &posting2);
    }

    #[test]
    fn not_found() {
        let posting1 = vec![(10, vec![0, 1, 2, 3, 4]), (1, vec![15])];
        let posting2 = vec![(0, vec![0, 1, 4]), (1, vec![5, 15566, 3423565]), (5, vec![0, 24, 56])];
        assert!(create_dir_all(Path::new("/tmp/comp_test_index2")).is_ok());
        let mut prov = CompressedRamStorage::create(Path::new("/tmp/comp_test_index2")).unwrap();
        assert!(prov.store(0, posting1.clone()).is_ok());
        assert!(prov.store(1, posting2.clone()).is_ok());
        assert!(if let StorageError::KeyNotFound = prov.get(2).err().unwrap() {
            true
        } else {
            false
        });
    }

    #[test]
    fn persistence() {
        let item1 = 1556;
        let item2 = 235425354;
        let item3 = 234543463709865987;
        assert!(create_dir_all(Path::new("/tmp/test_index3")).is_ok());
        {
            let mut prov1 = CompressedRamStorage::create(Path::new("/tmp/test_index3")).unwrap();
            assert!(prov1.store(0, item1.clone()).is_ok());
            assert!(prov1.store(1, item2.clone()).is_ok());
        }

        {
            let mut prov2: CompressedRamStorage<usize> = CompressedRamStorage::load(Path::new("/tmp/test_index3")).unwrap();
            assert_eq!(prov2.get(0).unwrap().as_ref(), &item1);
            assert_eq!(prov2.get(1).unwrap().as_ref(), &item2);
            assert!(prov2.store(2, item3.clone()).is_ok());
            assert_eq!(prov2.get(2).unwrap().as_ref(), &item3);
        }

        {
            let prov3: CompressedRamStorage<usize> = CompressedRamStorage::load(Path::new("/tmp/test_index3")).unwrap();
            assert_eq!(prov3.get(0).unwrap().as_ref(), &item1);
            assert_eq!(prov3.get(1).unwrap().as_ref(), &item2);
            assert_eq!(prov3.get(2).unwrap().as_ref(), &item3);
        }
    }
}
