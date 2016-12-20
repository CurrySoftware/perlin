#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Posting(pub DocId);
#[derive(Debug, PartialEq, Copy, Clone)]
pub struct DocId(pub u64);

impl DocId {
    pub fn none() -> DocId {
        DocId(u64::max_value())
    }
}
