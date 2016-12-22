pub mod ring_buffer;
#[macro_use]
pub mod try_option;

pub trait Baseable<T> {
    fn base_on(&mut self, T);
}
