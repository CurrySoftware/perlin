use std::slice;
use std::fmt;
use std::mem;
use std::io;
use std::ops::{Index, IndexMut};

use page_manager:: {BLOCKSIZE, Block, BlockId};

pub const PAGESIZE: usize = 64;

#[derive(Copy)]
pub struct Page(pub [Block; PAGESIZE]);

#[derive(Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Debug)]
pub struct PageId(pub u64);

impl Page {
    pub fn empty() -> Self {
        Page([Block::empty(); PAGESIZE])
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(&self.0[0].0 as *const u8, BLOCKSIZE*PAGESIZE) }
    }

    pub fn from_read<R: io::Read>(source: &mut R) -> Page {
        let mut raw: [u8; BLOCKSIZE*PAGESIZE] = unsafe {mem::uninitialized()};
        source.read_exact(&mut raw).unwrap();
        unsafe {mem::transmute(raw)}
    }

}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", &self.0 as &[Block])
    }
}

impl PartialEq for Page {
    fn eq(&self, other: &Self) -> bool {
        &self.0 as &[Block] == &other.0 as &[Block]
    }
}

impl Eq for Page {}

impl Clone for Page {
    fn clone(&self) -> Page { *self }
}


impl Index<BlockId> for Page {
    type Output = Block;
    
    fn index<'a>(&'a self, _index: BlockId) -> &'a Block {
        &self.0[_index.0 as usize]
    }
}

impl IndexMut<BlockId> for Page {
    fn index_mut<'a>(&'a mut self, _index: BlockId) -> &'a mut Block {
        &mut self.0[_index.0 as usize]
    }
}
