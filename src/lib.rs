#![cfg_attr(feature = "unstable", feature(test))]

#[macro_use]
mod utils;
mod page_manager;
mod compressor;
pub mod index;

#[cfg(test)]
pub mod test_utils;

#[cfg(all(feature = "unstable", test))]
extern crate test;
