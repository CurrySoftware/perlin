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

use std::io::{Bytes, Read};

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

/// Performs a vbyte encode without allocating memory on the heap
/// Memory layout of the result: [ payload | payload | payload | ... | used_bytes ]
pub fn heapless_vbyte_encode(mut number: usize) -> [u8; 11]
{
    let mut count = 0;
    let mut result = [0u8; 11];
    loop {
        result[9 - count] = (number % 128) as u8;
        count += 1;
        if number < 128 {
            break;
        } else {
            number /= 128;
        }
    }    
    result[9] += 128;
    result[10] = count as u8;
    result        
}


/// Iterator that decodes a bytestream to unsigned integers
pub struct VByteDecoder<R> {
    bytes: Bytes<R>
}

impl<R: Read> VByteDecoder<R> {
    /// Create a new VByteDecoder by passing a bytestream
    pub fn new(read: Bytes<R>) -> Self {
        VByteDecoder { bytes:  read }
    }

    /// Sometimes it is convenient to look at the original bytestream itself
    /// (e.g. when not only vbyte encoded integers are in the bytestream)
    /// This method provides access to the underlying bytestream in form of
    /// a
    /// mutable borrow
    pub fn underlying_iterator(&mut self) -> &mut Bytes<R> {
         &mut self.bytes
    }
}

impl<R: Read> Iterator for VByteDecoder<R> {
    type Item = usize;

    /// Returns the next unsigned integer which is encoded in the underlying
    /// bytestream
    /// May iterate the underlying bytestream an arbitrary number of times
    /// Returns None when the underlying bytream returns None
    fn next(&mut self) -> Option<Self::Item> {

        let mut result: usize = 0;
        loop {
            result *= 128;
            let val = try_option!(self.bytes.next()).unwrap();
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
    use std::io::Read;
    use std;

    #[test]
    fn test_heapless_vbyte_encode() {
        println!("{:?}", heapless_vbyte_encode(0));
        println!("{:?}", heapless_vbyte_encode(128));
        assert_eq!(heapless_vbyte_encode(0)[9], 0x80);
        assert_eq!(heapless_vbyte_encode(0)[10], 0x01);
        assert_eq!(heapless_vbyte_encode(128)[8..10], [0x01, 0x80]);
        assert_eq!(heapless_vbyte_encode(128)[10], 0x02);
               
        assert_eq!(heapless_vbyte_encode(0xFFFF)[7..10], [0x03, 0x7F, 0xFF]);
        assert_eq!(heapless_vbyte_encode(0xFFFF)[10], 0x03);
        assert_eq!(heapless_vbyte_encode(std::u64::MAX as usize), [1, 127, 127, 127, 127, 127, 127, 127, 127, 255, 10]);
    }
    
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
        assert_eq!(VByteDecoder::new([0x80].bytes()).collect::<Vec<_>>(),
                   vec![0]);
        assert_eq!(VByteDecoder::new([0x85].bytes()).collect::<Vec<_>>(),
                   vec![5]);
        assert_eq!(VByteDecoder::new([0xFF].bytes()).collect::<Vec<_>>(),
                   vec![127]);
        assert_eq!(VByteDecoder::new([0x80, 0x81].bytes()).collect::<Vec<_>>(),
                   vec![0, 1]);
        assert_eq!(VByteDecoder::new([0x03, 0x7F, 0xFF, 0x01, 0x82, 0x85].bytes()).collect::<Vec<_>>(),
                   vec![0xFFFF, 130, 5]);
        assert_eq!(VByteDecoder::new([0x80].bytes()).collect::<Vec<_>>(),
                   vec![0]);
    }
}
