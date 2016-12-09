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
use std::io::{Read, Write, Error};

pub mod vbyte;

pub use storage::compression::vbyte::VByteCode;


pub trait EncodingScheme<W: Write> {
    
    fn encode_to_stream(number: usize, target: &mut W) -> Result<usize, Error>;

}
pub trait DecodingScheme<R: Read> {
    type ResultIter;
    fn decode_from_stream(source: R) -> Self::ResultIter;
}

    
