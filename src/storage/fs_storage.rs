use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::marker::PhantomData;

use storage::compression::{vbyte_encode, VByteDecoder};
use storage::{ByteEncodable, ByteDecodable};
use utils::persistence::Persistent;

use storage::{Result, StorageError, Storage};

const ENTRIES_FILENAME: &'static str = "entries.bin";
const DATA_FILENAME: &'static str = "data.bin";
const ASSOCIATED_FILES: &'static [&'static str; 2] = &[ENTRIES_FILENAME, DATA_FILENAME];

/// Writes datastructures to a filesystem. Compressed and retrievable.
pub struct FsStorage<TItem> {
    // Stores for every id the offset in the file and the length
    entries: BTreeMap<u64, (u64 /* offset */, u32 /* length */)>,
    persistent_entries: File,
    data: File,
    current_offset: u64,
    current_id: u64,
    _item_type: PhantomData<TItem>,
}

impl<TItem> Persistent for FsStorage<TItem> {
    /// Creates a new and empty instance of FsStorage which can be loaded
    /// afterwards
    fn create(path: &Path) -> Result<Self> {
        assert!(path.is_dir(),
                "FsStorage::create expects a directory not a file!");
        Ok(FsStorage {
            current_offset: 0,
            current_id: 0,
            entries: BTreeMap::new(),
            persistent_entries: try!(OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.join(ENTRIES_FILENAME))),
            data: try!(OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.join(DATA_FILENAME))),
            _item_type: PhantomData,
        })
    }

    /// Reads a FsStorage from an previously populated folder.
    fn load(path: &Path) -> Result<Self> {
        // Read from entry file to BTreeMap.
        let mut entries = BTreeMap::new();
        // 1. Open file and pass it to the decoder
        let entries_file = try!(OpenOptions::new().read(true).open(path.join(ENTRIES_FILENAME)));
        let mut decoder = VByteDecoder::new(entries_file.bytes());
        // 2. Decode entries and write them to BTreeMap
        let mut current_id: u64 = 0;
        let mut current_offset: u64 = 0;
        while let Some((id, len)) = decode_entry(&mut decoder) {
            current_id += id as u64;
            entries.insert(current_id, (current_offset, len));
            current_offset += len as u64;
        }

        Ok(FsStorage {
            current_id: current_id,
            current_offset: current_offset,
            entries: entries,
            persistent_entries: try!(OpenOptions::new()
                .append(true)
                .open(path.join(ENTRIES_FILENAME))),
            data: try!(OpenOptions::new()
                .read(true)
                .append(true)
                .open(path.join(DATA_FILENAME))),
            _item_type: PhantomData,
        })
    }

    fn associated_files() -> &'static [&'static str] {
        ASSOCIATED_FILES
    }
}




impl<TItem: ByteDecodable + ByteEncodable + Sync + Send> Storage<TItem> for FsStorage<TItem> {
    fn get(&self, id: u64) -> Result<Arc<TItem>> {
        // TODO: Think through this once more. Now with the new Read approach in ByteDecodable
        if let Some(item_position) = self.entries.get(&id) {
            // Get filehandle
            let mut f = self.data.try_clone().unwrap();
            // Seek to position of item
            f.seek(SeekFrom::Start(item_position.0)).unwrap();
            let mut bytes = vec![0; item_position.1 as usize];
            // Read all bytes
            f.read_exact(&mut bytes).unwrap();
            // Decode item
            let item = TItem::decode(&mut bytes.as_slice()).unwrap();
            Ok(Arc::new(item))
        } else {
            Err(StorageError::KeyNotFound)
        }
    }

    fn store(&mut self, id: u64, data: TItem) -> Result<()> {

        // Encode the data
        let bytes = data.encode();
        // Append it to the file
        if let Err(e) = self.data.write_all(&bytes) {
            return Err(StorageError::WriteError(Some(e)));
        }
        // And save the offset and the number of bytes written for later recovery
        self.entries.insert(id, (self.current_offset, bytes.len() as u32));
        // Also write the id, offset and number of bytes written to file for persistence
        let entry_bytes = encode_entry(self.current_id, id, bytes.len() as u32);
        if let Err(e) = self.persistent_entries.write_all(&entry_bytes) {
            return Err(StorageError::WriteError(Some(e)));
        }

        // Update id and offset
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




#[cfg(test)]
mod tests {
    use std::fs::create_dir_all;
    use std::path::Path;

    use super::*;
    use utils::persistence::Persistent;
    use storage::{Storage, StorageError};

    #[test]
    fn basic() {
        let item1: u32 = 15;
        let item2: u32 = 32;
        assert!(create_dir_all(Path::new("/tmp/test_index")).is_ok());
        let mut prov = FsStorage::create(Path::new("/tmp/test_index")).unwrap();
        assert!(prov.store(0, item1.clone()).is_ok());
        assert_eq!(prov.get(0).unwrap().as_ref(), &item1);
        assert!(prov.store(1, item2.clone()).is_ok());
        assert_eq!(prov.get(1).unwrap().as_ref(), &item2);
        assert!(prov.get(0).unwrap().as_ref() != &item2);
        assert_eq!(prov.get(0).unwrap().as_ref(), &item1);
    }

    #[test]
    fn not_found() {
        let posting1 = vec![(10, vec![0, 1, 2, 3, 4]), (1, vec![15])];
        let posting2 = vec![(0, vec![0, 1, 4]), (1, vec![5, 15566, 3423565]), (5, vec![0, 24, 56])];
        assert!(create_dir_all(Path::new("/tmp/test_index2")).is_ok());
        let mut prov = FsStorage::create(Path::new("/tmp/test_index2")).unwrap();
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
            let mut prov1 = FsStorage::create(Path::new("/tmp/test_index3")).unwrap();
            assert!(prov1.store(0, item1.clone()).is_ok());
            assert!(prov1.store(1, item2.clone()).is_ok());
        }

        {
            let mut prov2: FsStorage<usize> = FsStorage::load(Path::new("/tmp/test_index3")).unwrap();
            assert_eq!(prov2.get(0).unwrap().as_ref(), &item1);
            assert_eq!(prov2.get(1).unwrap().as_ref(), &item2);
            assert!(prov2.store(2, item3.clone()).is_ok());
            assert_eq!(prov2.get(2).unwrap().as_ref(), &item3);
        }

        {
            let prov3: FsStorage<usize> = FsStorage::load(Path::new("/tmp/test_index3")).unwrap();
            assert_eq!(prov3.get(0).unwrap().as_ref(), &item1);
            assert_eq!(prov3.get(1).unwrap().as_ref(), &item2);
            assert_eq!(prov3.get(2).unwrap().as_ref(), &item3);
        }
    }
}
