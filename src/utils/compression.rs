macro_rules! unwrap_or_return_none{
    ($operand:expr) => {
        if let Some(x) = $operand {
            x
        } else {
            return None;
        }
    }
}


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



pub struct VByteDecoder<'a> {
    bytes: Box<Iterator<Item=u8> + 'a>
}

impl<'a> VByteDecoder<'a> {
    pub fn new<T: Iterator<Item=u8> + 'a>(bytes: T) -> Self {
        VByteDecoder { bytes: Box::new(bytes) }
    }

    pub fn underlying_iterator(&mut self) -> &mut Iterator<Item=u8> {
       &mut self.bytes
    }
}

impl<'a> Iterator for VByteDecoder<'a> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {

        let mut result: usize = 0;
        loop {
            result *= 128;
            let val = unwrap_or_return_none!(self.bytes.next());
            result += val as usize;
            if val >= 128 {
                result -= 128;
                break;
            }
        }
        Some(result)
    }
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
