use std::mem;

const SIZE: usize = 64;

pub struct RingBuffer<T> {
    buff: [T; SIZE],
    start: usize,
    count: usize,
}

impl<T> RingBuffer<T> {
    fn new() -> Self {
        RingBuffer {
            buff: unsafe { mem::uninitialized() },
            start: 0,
            count: 0,
        }
    }

    fn push_back(&mut self, element: T) {
        debug_assert!(self.count < SIZE);
        self.buff[(self.start + self.count) % SIZE] = element;
        self.count += 1;
    }

    fn pop_back(&mut self) -> Option<T> {
        if self.count > 0 {
            self.count -= 1;
            Some(mem::replace(&mut self.buff[(self.start + self.count) % SIZE],
                              unsafe { mem::uninitialized() }))

        } else {
            None
        }
    }

    fn push_front(&mut self, element: T) {
        debug_assert!(self.count < SIZE);
        self.start += SIZE - 1;
        self.start %= SIZE;
        self.buff[self.start] = element;
        self.count += 1;
    }

    fn pop_front(&mut self) -> Option<T> {
        if self.count > 0 {
            let element = Some(mem::replace(&mut self.buff[self.start],
                                            unsafe { mem::uninitialized() }));
            self.count -= 1;
            self.start += 1;
            self.start %= SIZE;
            return element;
        } else {
            None
        }
    }

    #[inline]
    fn count(&self) -> usize {
        self.count
    }
}

#[cfg(test)]
mod tests {
    use super::{SIZE, RingBuffer};

    #[test]
    fn creating() {
        let buffer = RingBuffer::<u64>::new();
        assert_eq!(buffer.count(), 0);
    }

    #[test]
    fn push_back() {
        let mut buffer = RingBuffer::<u64>::new();
        buffer.push_back(10);
        assert_eq!(buffer.count(), 1);
    }

    #[test]
    fn pop_back() {
        let mut buffer = RingBuffer::<u64>::new();
        buffer.push_back(10);
        assert_eq!(buffer.count(), 1);
        assert_eq!(buffer.pop_front(), Some(10));
        assert_eq!(buffer.count(), 0);
    }

    #[test]
    fn push_front() {
        let mut buffer = RingBuffer::<u64>::new();
        buffer.push_front(10);
        assert_eq!(buffer.count(), 1);
    }

    #[test]
    fn pop_front() {
        let mut buffer = RingBuffer::<u64>::new();
        buffer.push_front(10);
        assert_eq!(buffer.count(), 1);
        assert_eq!(buffer.pop_front(), Some(10));
        assert_eq!(buffer.count(), 0);
    }

    #[test]
    fn extended_front() {
        let mut buffer = RingBuffer::<u64>::new();        
        buffer.push_back(10);
        buffer.push_back(15);
        buffer.push_front(5);
        //5, 10, 15
        assert_eq!(buffer.count(), 3);
        assert_eq!(buffer.pop_front(), Some(5));
        assert_eq!(buffer.count(), 2);
        assert_eq!(buffer.pop_front(), Some(10));
        assert_eq!(buffer.count(), 1);
    }

    #[test]
    fn full() {
        let mut buffer = RingBuffer::new();
        for i in 0..SIZE {
            buffer.push_back(i);
        }
        assert_eq!(buffer.count(), SIZE);
        assert_eq!(buffer.pop_front(), Some(0));
        assert_eq!(buffer.pop_back(), Some(SIZE-1));
        assert_eq!(buffer.count(), SIZE-2);
    }

    #[test]
    fn empty() {
        let mut buffer = RingBuffer::<usize>::new();
        assert_eq!(buffer.pop_front(), None);
        assert_eq!(buffer.pop_back(), None);
    }
}
