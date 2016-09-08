//! This module provides trait definitions for encoding as bytes and decoding
//! from bytes.
//! These traits are necessary to be able to fully persist an index on a
//! filesystem.
//! The module also provides implementations for commonly used types (for now
//! only String and usize).

use std::result::Result;
use utils::compression::{vbyte_encode, VByteDecoder};

/// Defines a method that allows an object to be encoded as a variable number
/// of bytes
pub trait ByteEncodable {
    fn encode(&self) -> Vec<u8>;
}

/// Defines a method that allows an object to be decoded from a variable number
/// of bytes
pub trait ByteDecodable
    where Self: Sized
{
    fn decode<TIterator: Iterator<Item = u8>>(bytes: TIterator) -> Result<Self, String>;
}


impl ByteEncodable for String {
    fn encode(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.len());
        result.extend_from_slice(self.as_bytes());
        result
    }
}

impl ByteDecodable for String {
    fn decode<TIterator: Iterator<Item = u8>>(bytes: TIterator) -> Result<Self, String> {
        String::from_utf8(bytes.collect()).map_err(|e| format!("{:?}", e))
    }
}

impl ByteEncodable for usize {
    fn encode(&self) -> Vec<u8> {
        vbyte_encode(*self)
    }
}

impl ByteDecodable for usize {
    fn decode<TIterator: Iterator<Item = u8>>(bytes: TIterator) -> Result<Self, String> {
        let mut decoder = VByteDecoder::new(bytes.into_iter());
        if let Some(res) = decoder.next() {
            Ok(res)
        } else {
            Err("Tried to decode bytevector /
                 with variable byte code. Failed"
                .to_string())
        }
    }
}

// TODO: Custom Decode, Encode Errors
