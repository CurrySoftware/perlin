//! This module provides trait definitions for encoding as bytes and decoding
//! from bytes.
//! These traits are necessary to be able to fully persist an index on a
//! filesystem.
//! The module also provides implementations for commonly used types (for now
//! only String and usize).

use std::result::Result;
use storage::{vbyte_encode, VByteDecoder};

/// Defines a method that allows an object to be encoded as a variable number
/// of bytes
pub trait ByteEncodable {
    /// Encodes the object as a vector of bytes
    fn encode(&self) -> Vec<u8>;
}

/// Defines a method that allows an object to be decoded from a variable number
/// of bytes
pub trait ByteDecodable
    where Self: Sized
{
    /// Decodes an object from a byte iterator
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

impl ByteEncodable for u64 {
    fn encode(&self) -> Vec<u8> {
        vbyte_encode(*self as usize)
    }
}

impl ByteDecodable for u64 {
    fn decode<TIterator: Iterator<Item = u8>>(bytes: TIterator) -> Result<Self, String> {
        let mut decoder = VByteDecoder::new(bytes.into_iter());
        if let Some(res) = decoder.next() {
            Ok(res as u64)
        } else {
            Err("Tried to decode bytevector /
                 with variable byte code. Failed"
                .to_string())
        }
    }
}

impl ByteEncodable for u32 {
    fn encode(&self) -> Vec<u8> {
        vbyte_encode(*self as usize)
    }
}

impl ByteDecodable for u32 {
    fn decode<TIterator: Iterator<Item = u8>>(bytes: TIterator) -> Result<Self, String> {
        let mut decoder = VByteDecoder::new(bytes.into_iter());
        if let Some(res) = decoder.next() {
            Ok(res as u32)
        } else {
            Err("Tried to decode bytevector /
                 with variable byte code. Failed"
                .to_string())
        }
    }
}

impl ByteEncodable for u16 {
    fn encode(&self) -> Vec<u8> {
        vbyte_encode(*self as usize)
    }
}

impl ByteDecodable for u16 {
    fn decode<TIterator: Iterator<Item = u8>>(bytes: TIterator) -> Result<Self, String> {
        let mut decoder = VByteDecoder::new(bytes.into_iter());
        if let Some(res) = decoder.next() {
            Ok(res as u16)
        } else {
            Err("Tried to decode bytevector /
                 with variable byte code. Failed"
                .to_string())
        }
    }
}

// TODO: Custom Decode, Encode Errors
