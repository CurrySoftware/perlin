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


//TODO: This module is full with now deprecated or obsolete stuff
//Clean up!

use std::io;
use std::io::{Read, Write, Error};

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

/// Stores the result of a vbyte encode operation without indirection that a `Vec<u8>` would introduce.
/// Can thus be used to `vbyte_encode` on the stack
pub struct VByteEncoded {
    // Memory layout of the result: [ payload | payload | payload | ... | used_bytes ]
    data: [u8; 11],
}

impl VByteEncoded {
    /// Performs a vbyte encode without allocating memory on the heap
    /// Can then be written to a `Write`-implementor
    pub fn new(mut number: usize) -> Self {
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
        VByteEncoded { data: result }
    }

    /// Return how many bytes are used to encode the number. Min: 1 Max: 10
    pub fn bytes_used(&self) -> u8 {
        self.data[10]
    }

    /// Writes the given VByteEncoded number to a target.
    /// Returns the number of bytes written (equal to `bytes_used`) or an `io::Error`
    pub fn write_to<W: Write>(&self, target: &mut W) -> Result<u8, Error> {
        target.write_all(&self.data[(10 - self.bytes_used()) as usize..10]).map(|()| self.bytes_used())
    }
}



/// Iterator that decodes a bytestream to unsigned integers
pub struct VByteDecoder<R> {
    source: R,
    buf: [u8; 10],
    filled: u8,
}

impl<R: Read> VByteDecoder<R> {
    /// Create a new VByteDecoder by passing a bytestream
    pub fn new(source: R) -> Self {
        VByteDecoder {
            source: source,
            buf: [0; 10],
            filled: 0,
        }
    }

    /// Sometimes it is convenient to look at the original bytestream itself
    /// (e.g. when not only vbyte encoded integers are in the bytestream)
    /// This method provides access to the underlying bytestream in form of
    /// a
    /// mutable borrow
    pub fn underlying_iterator(&mut self) -> &mut R {
        &mut self.source
    }
}

impl<R: Read> Read for VByteDecoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut bytes_read = 0;
        if self.filled > 0 {
            // We have some bytes stored. Give them back
            // Read some bytes from self.buf.
            bytes_read += try!((&self.buf[..self.filled as usize]).read(buf));
            // These lines "shift" self.buf to the left. Eliminating the read bytes
            let mut tmp: [u8; 10] = [0; 10];
            self.filled -= bytes_read as u8;
            tmp[..10 - bytes_read].copy_from_slice(&self.buf[bytes_read..]);
            self.buf = tmp;
            // If buf is full. Return
            if bytes_read >= buf.len() {
                return Ok(bytes_read);
            }
        }
        bytes_read += try!(self.source.read(&mut buf[bytes_read..]));
        Ok(bytes_read)
    }
}

impl<R: Read> Iterator for VByteDecoder<R> {
    type Item = usize;

    /// Returns the next unsigned integer which is encoded in the underlying
    /// bytestream
    /// May iterate the underlying bytestream an arbitrary number of times
    /// Returns None when the underlying bytream returns None or delivers corrupt data
    fn next(&mut self) -> Option<Self::Item> {
        let mut result: usize = 0;
        // Read bytes into buffer
        let read = self.source.read(&mut self.buf[self.filled as usize..]).unwrap();
        let mut ptr = 0;
        // Find the last byte of this number (The first that has the 128bit set)
        while ptr < self.buf.len() && self.buf[ptr] < 128 {
            ptr += 1;
        }
        // If we are not filled and we didnt read anything (Source is empty)
        // Or if we have corrupted data (No 128bit flag set)
        // Return none
        if (self.filled == 0 && read == 0) || ptr == 10 {
            return None;
        }
        //Specialcase where 10 bytes are read
        //In this case the first byte can be maximally 1
        //Else its corrupt data
        if self.buf[0] > 1 && ptr == 9 {
            return None;
        }
        let mut tmp: [u8; 10] = [0; 10];
        {
            // Split buffer so that the lhs is the number we want to decode
            let (a, buf) = self.buf.split_at(ptr + 1);
            // Copy the rest into a temporary
            tmp[..buf.len()].copy_from_slice(buf);
            self.filled = buf.len() as u8;
            // Now decode
            for byte in a {
                result *= 128;
                result += (*byte & 127) as usize;
            }
        }
        // Set self.buff and subtract the 128 from the 128 bit flag
        self.buf = tmp;
        Some(result)
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use std;

    #[test]
    fn test_heapless_vbyte_encode() {
        assert_eq!(VByteEncoded::new(0).data[9], 0x80);
        assert_eq!(VByteEncoded::new(0).bytes_used(), 1);
        assert_eq!(VByteEncoded::new(128).data[8..10], [0x01, 0x80]);
        assert_eq!(VByteEncoded::new(128).bytes_used(), 0x02);

        assert_eq!(VByteEncoded::new(0xFFFF).data[7..10], [0x03, 0x7F, 0xFF]);
        assert_eq!(VByteEncoded::new(0xFFFF).bytes_used(), 3);
        assert_eq!(VByteEncoded::new(std::u64::MAX as usize).data,
                   [1, 127, 127, 127, 127, 127, 127, 127, 127, 255, 10]);
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
        assert_eq!(VByteDecoder::new(vec![0x80].as_slice()).collect::<Vec<_>>(),
                   vec![0]);
        assert_eq!(VByteDecoder::new(vec![0x85].as_slice()).collect::<Vec<_>>(),
                   vec![5]);
        assert_eq!(VByteDecoder::new(vec![0xFF].as_slice()).collect::<Vec<_>>(),
                   vec![127]);
        assert_eq!(VByteDecoder::new(vec![0x80, 0x81].as_slice()).collect::<Vec<_>>(),
                   vec![0, 1]);
        assert_eq!(VByteDecoder::new(vec![0x03, 0x7F, 0xFF, 0x01, 0x82, 0x85].as_slice()).collect::<Vec<_>>(),
                   vec![0xFFFF, 130, 5]);
        assert_eq!(VByteDecoder::new(vec![0x80].as_slice()).collect::<Vec<_>>(),
                   vec![0]);
    }

    #[test]
    fn edge_cases() {
        // 0
        assert_eq!(VByteDecoder::new(vbyte_encode(0).as_slice()).collect::<Vec<_>>(),
                   vec![0]);

        // MAX
        assert_eq!(VByteDecoder::new(vbyte_encode(usize::max_value()).as_slice()).collect::<Vec<_>>(),
                   vec![usize::max_value()]);

        // too many bytes = corrupted data
        assert_eq!(VByteDecoder::new(vec![127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 255].as_slice()).collect::<Vec<_>>(), vec![]);
        // MAX + n = corrupted data
        assert_eq!(VByteDecoder::new(vec![2, 127, 127, 127, 127, 127, 127, 127, 127, 255].as_slice()).collect::<Vec<_>>(), vec![]);
        // zero-bytes
        assert_eq!(VByteDecoder::new(vec![0; 100].as_slice()).collect::<Vec<_>>(), vec![]);
    }

}
