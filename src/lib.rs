#![cfg_attr(feature = "bench", feature(test))]

#[macro_use]
mod utils;
mod compressor;
pub mod page_manager;
pub mod index;

#[cfg(test)]
pub mod test_utils;

#[cfg(all(feature = "bench", test))]
extern crate test;
