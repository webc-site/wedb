use waof::{COMMIT_FRAME_PAYLOAD_LEN, CommitMeta, decode_payload, encode_payload, is_commit_frame};

/// 编解码往返与魔数防误判
#[test]
fn codec_roundtrip() {
  let meta = CommitMeta {
    begin: 0x1234_5678,
    cookie: -42,
  };
  let payload = encode_payload(meta);
  assert_eq!(payload.len(), COMMIT_FRAME_PAYLOAD_LEN);
  assert_eq!(decode_payload(&payload), Some(meta));

  // 长度不符 / 魔数不符均判非 commit 帧
  assert!(!is_commit_frame(&payload[1..]));
  assert!(!is_commit_frame(&payload[..16]));
  let mut tampered = payload;
  tampered[0] ^= 0xff;
  assert!(!is_commit_frame(&tampered));
}
