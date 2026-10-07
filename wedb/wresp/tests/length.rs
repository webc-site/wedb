#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wresp::length::{MAX_LENGTH, try_read_length, try_write_length};

#[test]
fn roundtrip_all_buckets() {
  // 三个编码桶边界：0、63、64、16 383、16 384、MAX_LENGTH
  for len in [0u32, 63, 64, 0x3F_FF, 0x40_00, MAX_LENGTH] {
    let mut buf = [0u8; 5];
    let written = try_write_length(len, &mut buf).unwrap();
    let (decoded, read) = try_read_length(&buf[..written]).unwrap();
    assert_eq!((decoded, read), (len, written), "len={len}");
  }
}

#[test]
fn encode_rejects_overflow_and_short_output() {
  let mut buf = [0u8; 5];
  // 超 MAX_LENGTH
  assert_eq!(try_write_length(MAX_LENGTH + 1, &mut buf), None);
  // 缓冲不足：14 位需要 2 字节
  assert_eq!(try_write_length(64, &mut buf[..1]), None);
  // 缓冲不足：32 位需要 5 字节
  assert_eq!(try_write_length(16_384, &mut buf[..4]), None);
}

#[test]
fn decode_rejects_truncated_and_bad_prefix() {
  // 空输入
  assert_eq!(try_read_length(&[]), None);
  // 14 位缺次字节
  assert_eq!(try_read_length(&[1 << 6]), None);
  // 32 位缺长度字节
  assert_eq!(try_read_length(&[2 << 6, 0, 0]), None);
  // 11 前缀非法
  assert_eq!(try_read_length(&[0xC0]), None);
}
