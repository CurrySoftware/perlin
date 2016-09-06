use std::result::Result;
use utils::compression::{vbyte_encode, VByteDecoder};

pub trait ByteEncodable {
    fn encode(&self) -> Vec<u8>;
}

pub trait ByteDecodable where Self: Sized {
    fn decode<TIterator: Iterator<Item=u8>>(bytes: TIterator) -> Result<Self, String>;
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

//TODO: Custom Decode, Encode Errors
