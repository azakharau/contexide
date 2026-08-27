use blake3::Hasher;
use std::io::{Read, Result as IoResult};

#[inline]
pub fn blake3_hex(bytes: &[u8]) -> String {
    let mut h = Hasher::new();
    h.update(bytes);
    h.finalize().to_hex().to_string()
}

#[inline]
pub fn blake3_hex_str(s: &str) -> String {
    blake3_hex(s.as_bytes())
}

pub fn blake3_hex_reader<R: Read>(mut reader: R) -> IoResult<(String, u64)> {
    let mut h = Hasher::new();
    const BUFFER_SIZE: usize = 64 * 1024;
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut total = 0u64;

    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        h.update(&buffer[..n]);
    }
    Ok((h.finalize().to_hex().to_string(), total))
}

mod tests {

    #[test]
    fn str_and_bytes_match() {
        let a = super::blake3_hex_str("abc");
        let b = super::blake3_hex("abc".as_bytes());
        assert_eq!(a, b);
    }

    #[test]
    fn reader_hashes_all_bytes_and_equals_nonstreaming() {
        let data = vec![1u8; 123_456];
        let (hex, read) = super::blake3_hex_reader(&data[..]).unwrap();
        assert_eq!(read as usize, data.len());
        assert_eq!(hex, super::blake3_hex(&data));
    }
}
