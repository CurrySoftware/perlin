use std::result::Result;

pub trait ByteEncodable {
    fn encode(&self) -> Vec<u8>;
}

pub trait ByteDecodable where Self: Sized {
    fn decode<TIterator: Iterator<Item=u8>>(bytes: TIterator) -> Result<Self, String>;
}

//TODO: Custom Decode, Encode Errors
