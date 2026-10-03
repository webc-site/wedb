use wbase::crc64::hash;

const POLY: u64 = 0xad93d23594c935a9;

fn crc64_bitwise(data: &[u8]) -> u64 {
  let mut crc: u64 = 0;
  for &c in data {
    let mut i: u8 = 1;
    while i != 0 {
      let mut bit_set = (crc & 0x8000_0000_0000_0000) != 0;
      if (c & i) != 0 {
        bit_set = !bit_set;
      }
      crc <<= 1;
      if bit_set {
        crc ^= POLY;
      }
      i <<= 1;
    }
  }
  crc.reverse_bits()
}

#[test]
fn test_crc64_table_matches_bitwise_reference() {
  let samples: [&[u8]; 9] = [
    b"",
    b"1",
    b"123456789",
    b"\x00",
    b"\xff\xff\xff\xff\xff\xff\xff\xff",
    b"12345678",
    b"1234567890123456",
    b"The quick brown fox jumps over the lazy dog",
    &[0x80u8, 0x7f, 0x01, 0xfe, 0x55, 0xaa, 0x00, 0xff, 0x10, 0x20],
  ];
  for s in samples {
    let expected = crc64_bitwise(s).to_le_bytes();
    assert_eq!(hash(s), expected, "CRC64 hash mismatch for {s:?}");
  }
}
