//! This module currently provides utility methods and structs for variable
//! byte codes as described in
//! http://nlp.stanford.edu/IR-book/html/htmledition/variable-byte-codes-1.html.
//!
//! Encode unsigned integers by using the `vbyte_encode` method.
//!
//! Decode a bytestream by instatiating a `VByteDecoder` and using its iterator
//! implementation.
//!
//! #Example
//!
//! ```rust,ignore
//!
//! use perlin::utils::compression::{vbyte_encode, VByteDecoder};
//!
//! let bytes = vbyte_encode(3);
//! let three = VByteDecoder::new(bytes.into_iter()).next().unwrap();
//! assert_eq!(3, three);
//! ```
use std::io::{Read, Write, Result};

pub mod vbyte;
pub mod fixed_width;

pub use storage::compression::vbyte::VByteCode;
pub use storage::compression::fixed_width::FixedWidthCode;

/// Provides means to encode a number to a byte-stream
pub trait EncodingScheme<W: Write> {
    /// Encode a number to a target byte-stream. Return the number of bytes written!
    fn encode_to_stream(number: usize, target: &mut W) -> Result<usize>;

}
/// Provides means to decode a byte-stream
pub trait DecodingScheme<R: Read> {
    type ResultIter;
    /// Returns an iterator that decodes the byte stream
    fn decode_from_stream(source: R) -> Self::ResultIter;
}

/// Provides means to batch encode a list of numbers to a byte stream
pub trait BatchEncodingScheme<W: Write> {
    /// Encodes a slice numbers to a target byte stream. Returns the number of bytes written!
    fn batch_encode(data: &[u64], target: &mut W) -> Result<usize>;
}

/// Provides means to batch decode a list of numbers from a byte stream
pub trait BatchDecodingScheme<R: Read> {
    /// Decodes a batch wich was previouly encoded to the byte stream. Returns the vector of results
    fn batch_decode(data: &mut R) -> Result<Vec<u64>>;
}
