use perlin::index::Index;
use rand::{random};
use rand::Rand;

pub fn prepare_index<TIndex: Index<usize>>(documents: usize, document_size: usize) -> TIndex {
    let mut index = TIndex::new();
    for _ in 0..documents {
        index.index_document((0..document_size));
    }
    index
}
