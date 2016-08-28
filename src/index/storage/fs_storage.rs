use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;



use index::storage::{Result, Storage, StorageError};
// TODO: WRONG! FIX
use index::boolean_index::posting::Posting;
use utils::compression::{vbyte_encode, VByteDecoder};

pub struct FsPostingStorage {
    // Stores for every id the offset in the file and the length
    entries: BTreeMap<u64, (u64 /* offset */, u32 /* length */)>,
    persistent_entries: File,
    data: File,
    current_offset: u64,
}

impl FsPostingStorage {
    /// Creates a new and empty instance of FsPostingStorage
    pub fn new(path: &Path) -> Self {
        assert!(path.is_dir(),
                "FsStorage::new expects a directory not a file!");
        FsPostingStorage {
            current_offset: 0,
            entries: BTreeMap::new(),
            persistent_entries: OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.join("entries.bin"))
                .unwrap(),
            data: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.join("data.bin"))
                .unwrap(),
        }
    }

    /// Reads a FsStorage from an previously populated folder.
    // TODO: Return result
    pub fn from_folder(path: &Path) -> Self {
        // Read from entry file to BTreeMap.
        let mut entries = BTreeMap::new();
        // 1. Open file
        let mut entries_file =
            OpenOptions::new().read(true).open(path.join("entries.bin")).unwrap();
        let mut bytes = Vec::with_capacity(entries_file.metadata().unwrap().len() as usize);
        // 2. Read file
        assert!(entries_file.read_to_end(&mut bytes).is_ok());
        let mut decoder = VByteDecoder::new(bytes.into_iter());
        // 3. Decode entries and write them to BTreeMap
        while let Some(entry) = decode_entry(&mut decoder) {
            entries.insert(entry.0, (entry.1, entry.2));
        }

        // Get data file length for offset
        let offset = File::open(path.join("data.bin"))
            .unwrap()
            .metadata()
            .unwrap()
            .len();

        FsPostingStorage {
            current_offset: offset,
            entries: entries,
            persistent_entries: OpenOptions::new()
                .append(true)
                .open(path.join("entries.bin"))
                .unwrap(),
            data: OpenOptions::new()
                .read(true)
                .append(true)
                .open(path.join("data.bin"))
                .unwrap(),
        }
    }
}


impl Storage<Vec<Posting>> for FsPostingStorage {
    fn get(&self, id: u64) -> Result<Arc<Vec<Posting>>> {
        if let Some(posting_offset) = self.entries.get(&id) {
            let mut f = self.data.try_clone().unwrap();
            f.seek(SeekFrom::Start(posting_offset.0)).unwrap();
            let mut bytes = vec![0; posting_offset.1 as usize];
            f.read_exact(&mut bytes).unwrap();
            let mut decoder = VByteDecoder::new(bytes.into_iter());
            let dec_id = decoder.next().unwrap() as u64;
            assert_eq!(id, dec_id);
            let postings = decode_listing(decoder);
            Ok(Arc::new(postings))
        } else {
            Err(StorageError::KeyNotFound)
        }
    }

    fn store(&mut self, id: u64, data: Vec<Posting>) -> Result<()> {
        // Encode the data
        let bytes = encode_listing(id, &data);
        // Append it to the file
        if let Err(e) = self.data.write_all(&bytes) {
            return Err(StorageError::WriteError(Some(e)));
        }
        // And save the offset and the number of bytes written for later recovery
        self.entries.insert(id, (self.current_offset, bytes.len() as u32));
        // Also write the id, offset and number of bytes written to file for persistence
        let entry_bytes = encode_entry(id, self.current_offset, bytes.len() as u32);
        if let Err(e) = self.persistent_entries.write_all(&entry_bytes) {
            return Err(StorageError::WriteError(Some(e)));
        }

        // Update offset
        self.current_offset += bytes.len() as u64;
        Ok(())
    }
}

// TODO: Remove theses methods from here. They do not belog here.
// Probably belong in index::boolean_index::posting or similar
fn decode_listing(mut decoder: VByteDecoder) -> Vec<Posting> {
    let postings_len = decoder.next().unwrap();
    let mut postings = Vec::with_capacity(postings_len);
    for _ in 0..postings_len {
        let doc_id = decoder.next().unwrap();
        let positions_len = decoder.next().unwrap();
        let mut positions = Vec::with_capacity(positions_len as usize);
        let mut last_position = 0;
        for _ in 0..positions_len {
            last_position += decoder.next().unwrap();
            positions.push(last_position as u32);
        }
        postings.push((doc_id as u64, positions));
    }
    postings
}

fn encode_entry(id: u64, offset: u64, length: u32) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    bytes.append(&mut vbyte_encode(id as usize));
    bytes.append(&mut vbyte_encode(offset as usize));
    bytes.append(&mut vbyte_encode(length as usize));
    bytes
}

fn decode_entry(decoder: &mut VByteDecoder) -> Option<(u64, u64, u32)> {
    let id = try_option!(decoder.next()) as u64;
    let offset = try_option!(decoder.next()) as u64;
    let length = try_option!(decoder.next()) as u32;
    Some((id, offset, length))
}

fn encode_listing(term_id: u64, listing: &[Posting]) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    bytes.append(&mut vbyte_encode(term_id as usize));
    bytes.append(&mut vbyte_encode(listing.len()));
    for posting in listing {
        bytes.append(&mut vbyte_encode(posting.0 as usize));
        bytes.append(&mut vbyte_encode(posting.1.len() as usize));
        let mut last_position = 0;
        for position in &posting.1 {
            bytes.append(&mut vbyte_encode((*position - last_position) as usize));
            last_position = *position;
        }
    }
    bytes
}



#[cfg(test)]
mod tests {
    use std::fs::create_dir_all;
    use std::path::Path;

    use super::*;
    use index::storage::{Storage, StorageError};


    #[test]
    pub fn basic() {
        let posting1 = vec![(10, vec![0, 1, 2, 3, 4]), (1, vec![15])];
        let posting2 = vec![(0, vec![0, 1, 4]), (1, vec![5, 15566, 3423565]), (5, vec![0, 24, 56])];
        assert!(create_dir_all(Path::new("/tmp/test_index")).is_ok());
        let mut prov = FsPostingStorage::new(Path::new("/tmp/test_index"));
        assert!(prov.store(0, posting1.clone()).is_ok());
        assert_eq!(prov.get(0).unwrap().as_ref(), &posting1);
        assert!(prov.store(1, posting2.clone()).is_ok());
        assert_eq!(prov.get(1).unwrap().as_ref(), &posting2);
        assert!(prov.get(0).unwrap().as_ref() != &posting2);
        assert_eq!(prov.get(0).unwrap().as_ref(), &posting1);
    }

    #[test]
    pub fn not_found() {
        let posting1 = vec![(10, vec![0, 1, 2, 3, 4]), (1, vec![15])];
        let posting2 = vec![(0, vec![0, 1, 4]), (1, vec![5, 15566, 3423565]), (5, vec![0, 24, 56])];
        assert!(create_dir_all(Path::new("/tmp/test_index")).is_ok());
        let mut prov = FsPostingStorage::new(Path::new("/tmp/test_index"));
        assert!(prov.store(0, posting1.clone()).is_ok());
        assert!(prov.store(1, posting2.clone()).is_ok());
        assert!(if let StorageError::KeyNotFound = prov.get(2).err().unwrap() {
            true
        } else {
            false
        });
    }

    #[test]
    pub fn persistence() {
        let posting1 = vec![(10, vec![0, 1, 2, 3, 4]), (1, vec![15])];
        let posting2 = vec![(0, vec![0, 1, 4]), (1, vec![5, 15566, 3423565]), (5, vec![0, 24, 56])];
        let posting3 =
            vec![(15, vec![24, 745, 6946]), (37, vec![234, 2356, 12345]), (98, vec![0, 1, 2, 3])];
        assert!(create_dir_all(Path::new("/tmp/test_index2")).is_ok());
        {
            let mut prov1 = FsPostingStorage::new(Path::new("/tmp/test_index2"));
            assert!(prov1.store(0, posting1.clone()).is_ok());
            assert!(prov1.store(1, posting2.clone()).is_ok());
        }

        {
            let mut prov2 = FsPostingStorage::from_folder(Path::new("/tmp/test_index2"));
            assert_eq!(prov2.get(0).unwrap().as_ref(), &posting1);
            assert_eq!(prov2.get(1).unwrap().as_ref(), &posting2);
            assert!(prov2.store(2, posting3.clone()).is_ok());
            assert_eq!(prov2.get(2).unwrap().as_ref(), &posting3);
        }

        {
            let prov3 = FsPostingStorage::from_folder(Path::new("/tmp/test_index2"));
            assert_eq!(prov3.get(0).unwrap().as_ref(), &posting1);
            assert_eq!(prov3.get(1).unwrap().as_ref(), &posting2);
            assert_eq!(prov3.get(2).unwrap().as_ref(), &posting3);
        }
    }
}
