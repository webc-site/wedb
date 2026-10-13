use waof::{EMPTY_PAYLOAD_CRC, RECORD_HEADER_LEN, WalFrameHeader};

#[test]
fn test_record_header_roundtrip() {
  let header = WalFrameHeader::new(128, 0x1234_5678);
  let bytes = header.to_bytes();
  let decoded = WalFrameHeader::decode(&bytes).unwrap();
  assert_eq!(decoded, header);
  assert_eq!(decoded.payload_len(), 128);
  assert!(!decoded.is_zero());

  let zero = WalFrameHeader::new(0, 0);
  assert!(zero.is_zero());

  // 空负载头必须带非零 CRC 哨兵，决不能与全零 padding 混淆
  let empty_payload_header = WalFrameHeader::for_payload_parts(&[]);
  assert_eq!(empty_payload_header.payload_len(), 0);
  assert!(
    !empty_payload_header.is_zero(),
    "空有效记录头必须携带非零哨兵 CRC"
  );
  assert_eq!(empty_payload_header.crc32, EMPTY_PAYLOAD_CRC);

  // 校验全零定长数组为 padding / 损坏
  let all_zeros = [0u8; RECORD_HEADER_LEN];
  let zero_decoded = WalFrameHeader::decode(&all_zeros).unwrap();
  assert!(zero_decoded.is_zero(), "全零头唯一标识 padding 或残缺尾部");
}

#[test]
fn test_record_header_for_payload_and_verify() {
  let payload = b"hello aof payload";
  let header = WalFrameHeader::for_payload_parts(&[payload.as_slice()]);
  assert_eq!(header.payload_len(), payload.len());
  assert!(header.verify(payload).is_ok());

  let corrupted = b"hello aof payloae";
  assert!(header.verify(corrupted).is_err());
  assert!(header.verify(&payload[..payload.len() - 1]).is_err());
}

#[test]
fn test_record_header_decode_boundary() {
  let short = [0u8; 7];
  assert!(WalFrameHeader::decode_opt(&short).is_none());
  assert!(WalFrameHeader::decode(&short).is_err());
}
