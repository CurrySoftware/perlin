use std::ops::Add;
use std::fmt;

use page_manager::PAGESIZE;

pub const BLOCKSIZE: usize = 64;

#[derive(Copy)]
pub struct Block(pub [u8; BLOCKSIZE]);

#[derive(Copy, Clone, Ord, PartialOrd, PartialEq, Eq, Debug)]
pub struct BlockId(pub u16);


impl BlockId {
    pub fn first() -> BlockId {
        BlockId(0)
    }

    pub fn last() -> BlockId {
        BlockId(PAGESIZE as u16 -1)
    }
    
    pub fn from_page_capacity(page_capa: usize) -> BlockId {
        BlockId((PAGESIZE - page_capa) as u16) 
    }

    pub fn inc(&mut self) {
        self.0 += 1;
        self.0 %= BLOCKSIZE as u16;
    }

    pub fn dec(&mut self) {
        self.0 = ((self.0 as usize + BLOCKSIZE - 1) % BLOCKSIZE) as u16;
    }
}

impl Block {
    pub fn empty() -> Self {
        Block([0; BLOCKSIZE])
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

