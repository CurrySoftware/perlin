//! This module provides trait definitions for encoding as bytes and decoding
//! from bytes.
//! These traits are necessary to be able to fully persist an index on a
//! filesystem.
//! The module also provides implementations for commonly used types (for now
//! only String and usize).

use std::error::Error;
use std::io;
use std::io::Read;
use std::result;

use storage::compression::{DecodingScheme, VByteCode};

/// Wraps the Result of a decoding operation
pub type DecodeResult<T> = result::Result<T, DecodeError>;

#[derive(Debug)]
/// Error kinds that can occur during a decoding operation
pub enum DecodeError {
    /// Error occured during an IO operation
    IO(io::Error),
    /// Some error occured
    Other(Box<Error + Send>),
    /// Error occured due to malformed input
    MalformedInput,
}

impl From<io::Error> for DecodeError {
    fn from(err: io::Error) -> Self {
        DecodeError::IO(err)
    }
}

/// Defines a method that allows an object to be encoded as a variable number
/// of bytes
pub trait ByteEncodable {
    /// Encodes the object as a vector of bytes
    // TODO:
    // Most probably wrong and should use Write instead of returning a vector of bytes.
    fn encode(&self) -> Vec<u8>;
}

/// Defines a method that allows an object to be decoded from a variable number
/// of bytes
pub trait ByteDecodable
    where Self: Sized
{
    /// Decodes an object from a byte iterator
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self>;
}


impl ByteEncodable for String {
    fn encode(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.len());
        result.extend_from_slice(self.as_bytes());
        result
    }
}

impl ByteDecodable for String {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut bytes = vec![];
        try!(read.read_to_end(&mut bytes));
        String::from_utf8(bytes).map_err(|e| DecodeError::Other(Box::new(e)))
    }
}

impl ByteEncodable for usize {
    fn encode(&self) -> Vec<u8> {
        vbyte_encode(*self)
    }
}

impl ByteDecodable for usize {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut decoder = VByteCode::decode_from_stream(read);
        if let Some(res) = decoder.next() {
            Ok(res)
        } else {
            Err(DecodeError::MalformedInput)
        }
    }
}

impl ByteEncodable for u64 {
    fn encode(&self) -> Vec<u8> {
        vbyte_encode(*self as usize)
    }
}

impl ByteDecodable for u64 {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut decoder = VByteCode::decode_from_stream(read);
        if let Some(res) = decoder.next() {
            Ok(res as u64)
        } else {
            Err(DecodeError::MalformedInput)
        }
    }
}

impl ByteEncodable for u32 {
    fn encode(&self) -> Vec<u8> {
        vbyte_encode(*self as usize)
    }
}

impl ByteDecodable for u32 {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut decoder = VByteCode::decode_from_stream(read);
        if let Some(res) = decoder.next() {
            Ok(res as u32)
        } else {
            Err(DecodeError::MalformedInput)
        }
    }
}

impl ByteEncodable for u16 {
    fn encode(&self) -> Vec<u8> {
        vbyte_encode(*self as usize)
    }
}

impl ByteDecodable for u16 {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut decoder = VByteCode::decode_from_stream(read);
        if let Some(res) = decoder.next() {
            Ok(res as u16)
        } else {
            Err(DecodeError::MalformedInput)
        }
    }
}

/// Encode an usigned integer as a variable number of bytes
fn vbyte_encode(mut number: usize) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        result.insert(0, (number % 128) as u8);
        if number < 128 {
            break;
        } else {
            number /= 128;
        }
    }
    let len = result.len();
    result[len - 1] += 128;
    result
}
