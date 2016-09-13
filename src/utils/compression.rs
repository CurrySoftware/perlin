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


    /// Encode an usigned integer as a variable number of bytes
    pub fn vbyte_encode(mut number: usize) -> Vec<u8> {
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

    // TODO: VByteDecoder to take a Iterator<Item=&u8> not Iterator<Item=u8>
    /// Iterator that decodes a bytestream to unsigned integers
    pub struct VByteDecoder<'a> {
        bytes: Box<Iterator<Item = u8> + 'a>,
    }

    impl<'a> VByteDecoder<'a> {
        /// Create a new VByteDecoder by passing a bytestream
        pub fn new<T: Iterator<Item = u8> + 'a>(bytes: T) -> Self {
            VByteDecoder { bytes: Box::new(bytes) }
        }

        /// Sometimes it is convenient to look at the original bytestream itself
        /// (e.g. when not only vbyte encoded integers are in the bytestream)
        /// This method provides access to the underlying bytestream in form of
        /// a
        /// mutable borrow
        pub fn underlying_iterator(&mut self) -> &mut Iterator<Item = u8> {
            &mut self.bytes
        }
    }

    impl<'a> Iterator for VByteDecoder<'a> {
        type Item = usize;

        /// Returns the next unsigned integer which is encoded in the underlying
        /// bytestream
        /// May iterate the underlying bytestream an arbitrary number of times
        /// Returns None when the underlying bytream returns None
        fn next(&mut self) -> Option<Self::Item> {

            let mut result: usize = 0;
            loop {
                result *= 128;
                let val = try_option!(self.bytes.next());
                result += val as usize;
                if val >= 128 {
                    result -= 128;
                    break;
                }
            }
            Some(result)
        }
    }


#[cfg(test)]
mod tests {

    use super::*;
    
    #[test]
    fn test_vbyte_encode() {
        assert_eq!(vbyte_encode(0), vec![0x80]);
        assert_eq!(vbyte_encode(5), vec![0x85]);
        assert_eq!(vbyte_encode(127), vec![0xFF]);
        assert_eq!(vbyte_encode(128), vec![0x01, 0x80]);
        assert_eq!(vbyte_encode(130), vec![0x01, 0x82]);
        assert_eq!(vbyte_encode(255), vec![0x01, 0xFF]);
        assert_eq!(vbyte_encode(20_000), vec![0x01, 0x1C, 0xA0]);
        assert_eq!(vbyte_encode(0xFFFF), vec![0x03, 0x7F, 0xFF]);
    }

    #[test]
    fn test_vbyte_decode() {
        assert_eq!(VByteDecoder::new(vec![0x80].into_iter()).collect::<Vec<_>>(),
                   vec![0]);
        assert_eq!(VByteDecoder::new(vec![0x85].into_iter()).collect::<Vec<_>>(),
                   vec![5]);
        assert_eq!(VByteDecoder::new(vec![0xFF].into_iter()).collect::<Vec<_>>(),
                   vec![127]);
        assert_eq!(VByteDecoder::new(vec![0x80, 0x81].into_iter()).collect::<Vec<_>>(),
                   vec![0, 1]);
        assert_eq!(VByteDecoder::new(vec![0x03, 0x7F, 0xFF, 0x01, 0x82, 0x85].into_iter())
                       .collect::<Vec<_>>(),
                   vec![0xFFFF, 130, 5]);
        assert_eq!(VByteDecoder::new(vec![0x80].into_iter()).collect::<Vec<_>>(),
                   vec![0]);
    }
}
