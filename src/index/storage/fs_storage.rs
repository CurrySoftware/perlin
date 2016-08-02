use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;



use index::storage::{Result, Storage};
//TODO: WRONG! FIX
use index::boolean_index::posting::Posting;
//WRONG TOO! (At leas probably!)
use index::boolean_index::persistence::{vbyte_encode, VByteDecoder};


pub struct FsPostingStorage {
    // Stores for every id the offset in the file and the length
    data: BTreeMap<u64, (u64, u32)>,
    dir: File,
    offset: u64,
}

impl FsPostingStorage {
    pub fn new(path: &Path) -> Self {
        FsPostingStorage {
            offset: 0,
            data: BTreeMap::new(),
            dir: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .unwrap(),
        }
    }
}


impl Storage<Vec<Posting>> for FsPostingStorage {
    fn get(&self, id: u64) -> Result<Arc<Vec<Posting>>> {
        let posting_offset = self.data.get(&id).unwrap();
        let mut f = self.dir.try_clone().unwrap();
        f.seek(SeekFrom::Start(posting_offset.0)).unwrap();
        let mut bytes = vec![0; posting_offset.1 as usize];
        f.read_exact(&mut bytes).unwrap();
        let mut decoder = VByteDecoder::new(bytes.into_iter());
        let dec_id = decoder.next().unwrap() as u64;
        assert_eq!(id, dec_id);
        let postings = decode_listing(decoder);
        Ok(Arc::new(postings))
    }

    fn store(&mut self, id: u64, data: Vec<Posting>) -> Result<()>{
        let bytes = encode_listing(id, &data);
        self.dir.write_all(&bytes);
        self.data.insert(id, (self.offset, bytes.len() as u32));
        self.offset += bytes.len() as u64;
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

    #[test]
    pub fn fs_provider() {
        let posting1 = vec![(10, vec![0, 1, 2, 3, 4]), (1, vec![15])];
        let posting2 = vec![(0, vec![0, 1, 4]), (1, vec![5, 15566, 3423565]), (5, vec![0, 24, 56])];
        let mut prov = FsPostingStorage::new(Path::new("/tmp/test_index.bin"));
        prov.store(0, posting1.clone());
        assert_eq!(prov.get(0).unwrap().as_ref(), &posting1);
        prov.store(1, posting2.clone()); 
        assert_eq!(prov.get(1).unwrap().as_ref(), &posting2);
        assert!(prov.get(0).unwrap().as_ref() != &posting2);
        assert_eq!(prov.get(0).unwrap().as_ref(), &posting1);
    }
