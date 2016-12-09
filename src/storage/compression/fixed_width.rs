//! This module introduces a fixed width encoding scheme
//! It is usefull for fast encoding and decoding of numbers that massively range in width
//!
//! But be aware: This gives rather unimpressive compression ratio!

use std::cmp;
use std::mem;

use std::io::{Read, Result, Write};

use storage::compression::{BatchEncodingScheme, BatchDecodingScheme};

/// Implements `BatchEncodingScheme` and `BatchDecodingScheme` using fixed width code.
/// See [the module level documentation](index.html) for more.
pub struct FixedWidthCode;

impl<W: Write> BatchEncodingScheme<W> for FixedWidthCode {
    fn batch_encode(data: &[u64], target: &mut W) -> Result<usize> {
        // Find max width
        // Encode max width with 1 byte
        let max_width = log2(cmp::max(*data.iter().max().unwrap_or(&0), data.len() as u64)) as u8;
        // Rount up
        let bytes = if max_width % 8 == 0 {
            (max_width / 8) as usize
        } else {
            ((max_width / 8) + 1) as usize
        };
        let len_bytes = unsafe { &mem::transmute::<usize, [u8; 8]>(data.len()) };
        // Write the width in bytes
        target.write(&[bytes as u8])?;
        // Write the number of integers encoded
        target.write(&len_bytes[0..bytes])?;
        for num in data {
            //Encode every integer
            let raw = unsafe { &mem::transmute::<u64, [u8; 8]>(*num) };
            target.write(&raw[0..bytes])?;
        }
        target.flush()?;
        //Return the number of bytes written
        Ok(1 + ((data.len() + 1) * bytes))
    }
}

impl<R: Read> BatchDecodingScheme<R> for FixedWidthCode { 
    fn batch_decode(data: &mut R) -> Result<Vec<u64>> {
        let mut mask: [u8; 8] = [0u8; 8];
        let mut bwidth: [u8; 1] = [0u8; 1];

        //Read the width of every number in bytes and decode it
        data.read_exact(&mut bwidth)?;        
        let width = bwidth[0] as usize;
        //Then read the number of encoded integers
        data.read_exact(&mut mask[..width])?;
        let len = unsafe { mem::transmute_copy::<[u8; 8], u64>(&mask) };
        //Create the vector with the results. Memory preallocated
        let mut target = Vec::with_capacity(len as usize);
        for _ in 0..len {
            //Decode the results
            data.read_exact(&mut mask[..width])?;
            target.push(unsafe { mem::transmute_copy::<[u8; 8], u64>(&mask) });
        }
        Ok(target)
    }
}

fn log2(num: u64) -> u64 {
    if num.is_power_of_two() {
        return (num.trailing_zeros() + 1) as u64;
    }
    num.next_power_of_two().trailing_zeros() as u64
}

#[cfg(test)]
mod tests {
    use super::FixedWidthCode;
    use storage::compression::{BatchEncodingScheme, BatchDecodingScheme};

    #[test]
    fn basic(){
        let nums = vec![15, 23445, 42, 15, 0, 0, 98];
        let mut bytes = Vec::new();
        FixedWidthCode::batch_encode(nums.as_slice(), &mut bytes).unwrap();
        assert_eq!(FixedWidthCode::batch_decode(&mut bytes.as_slice()).unwrap(), nums);
    }

    #[test]
    fn extended(){
        let nums = (0..10000).collect::<Vec<_>>();
        let mut bytes = Vec::new();
        FixedWidthCode::batch_encode(nums.as_slice(), &mut bytes).unwrap();
        assert_eq!(FixedWidthCode::batch_decode(&mut bytes.as_slice()).unwrap(), nums);
    }

    #[test]
    fn edge_case() {
        let nums = vec![];
        let mut bytes = Vec::new();
        FixedWidthCode::batch_encode(nums.as_slice(), &mut bytes).unwrap();
        assert_eq!(FixedWidthCode::batch_decode(&mut bytes.as_slice()).unwrap(), nums);
    }

}
