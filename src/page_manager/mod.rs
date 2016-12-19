use std::fmt;
use std::sync::Arc;
use std::ops::{Index, IndexMut};

mod ram_page_manager;

const PAGESIZE: usize = 64;
const BLOCKSIZE: usize = 64;

#[derive(Copy)]
struct Block([u8; BLOCKSIZE]);
#[derive(Copy)]
struct Page([Block; PAGESIZE]);

#[derive(Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Debug)]
struct PageId(u64);
#[derive(Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Debug)]
struct BlockId(u16);

impl Page {
    fn empty() -> Self {
        Page([Block::empty(); PAGESIZE])
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

impl Block {
    fn empty() -> Self {
        Block([0; BLOCKSIZE])
    }
}

impl BlockId {
    fn first() -> BlockId {
        BlockId(0)
    }
    
    fn from_page_capacity(page_capa: usize) -> BlockId {
        BlockId((PAGESIZE - page_capa) as u16) 
    }
}


impl Clone for Block {
    fn clone(&self) -> Block { *self }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", &self.0 as &[u8])
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        &self.0 as &[u8] == &other.0 as &[u8]
    }
}

impl Eq for Block {}



trait PageManager {
    fn store_page(&mut self, Page) -> PageId;
    fn delete_page(&mut self, PageId);
}

trait PageCache {
    fn get_page(&self, PageId) -> Arc<Page>;
}

trait PageStore {
    fn get_page(&self, PageId) -> Page;
}

trait BlockManager {
    fn store_block(&mut self, Block) -> PageId;
    fn store_in_page(&mut self, PageId, Block) -> Result<BlockId, PageId>;
}



