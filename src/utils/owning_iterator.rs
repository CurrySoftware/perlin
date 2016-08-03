use std::sync::Arc;
use std::cell::Cell;


pub trait OwningIterator<'a> {
    type Item;
    fn next(&'a self) -> Option<Self::Item>;
    fn peek(&'a self) -> Option<Self::Item>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
}

pub struct ArcIter<T> {
    data: Arc<Vec<T>>,
    pos: Cell<usize>,
}

impl<'a, T: 'a> OwningIterator<'a> for ArcIter<T> {
    type Item = &'a T;

    fn next(&'a self) -> Option<Self::Item> {
        if self.pos.get() < self.data.len() {
            self.pos.set(self.pos.get() + 1);
            return Some(&self.data[self.pos.get() - 1]);
        }
        None
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn peek(&'a self) -> Option<Self::Item> {
        if self.pos.get() >= self.len() {
            None
        } else {
            Some(&self.data[self.pos.get()])
        }
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}


impl<T> ArcIter<T>{
    pub fn new(data: Arc<Vec<T>>) -> Self {
        ArcIter {
            data: data,
            pos: Cell::new(0)
        }
    }
}
