//! This module contains traits and implementations regarding integer
//! compression.
//!
//!
//! `EncodingScheme` and `DecodingScheme` can be used with single integer values
//!
//! `BatchEncodingScheme` and `BatchDecodingScheme` can be used with a batch of
//! integer values
use std::io::{Read, Write, Result};

pub mod vbyte;
pub mod fixed_width;

pub use storage::compression::vbyte::VByteCode;
pub use storage::compression::fixed_width::FixedWidthCode;

/// Provides means to encode a number to a byte-stream
pub trait EncodingScheme<W: Write> {
    /// Encode a number to a target byte-stream. Return the number of bytes
    /// written!
    fn encode_to_stream(number: usize, target: &mut W) -> Result<usize>;
}
/// Provides means to decode a byte-stream
pub trait DecodingScheme<R: Read> {
    /// The type of the iterator returned by `decode_from_stream`
    type ResultIter;
    /// Returns an iterator that decodes the byte stream
    fn decode_from_stream(source: R) -> Self::ResultIter;
}

/// Provides means to batch encode a list of numbers to a byte stream
pub trait BatchEncodingScheme<W: Write> {
    /// Encodes a slice numbers to a target byte stream. Returns the number of
    /// bytes written!
    fn batch_encode(data: &[u64], target: &mut W) -> Result<usize>;
}

/// Provides means to batch decode a list of numbers from a byte stream
pub trait BatchDecodingScheme<R: Read> {
    /// Decodes a batch wich was previouly encoded to the byte stream. Returns
    /// the vector of results
    fn batch_decode(data: &mut R) -> Result<Vec<u64>>;
}
