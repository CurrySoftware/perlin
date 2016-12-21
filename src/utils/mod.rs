pub mod ring_buffer;

pub trait Baseable<T> {
    fn base_on(&mut self, T);
}
