//! 索引检查点二进制编解码与槽位净化测试（自 src/index_ckpt/codec.rs 迁出并强化）
use aok::{OK, Result};
use wcpr::{HEADER_SIZE, IndexCkptHeader, sanitize_data_slot, sanitize_overflow_slot};
use windex::HashBucketEntry;

/// IndexCkptHeader 定长二进制编解码：往返一致、魔数与长度边界
#[test]
fn index_ckpt_header_codec_roundtrip() -> Result<()> {
  let hdr = IndexCkptHeader {
    version: 1,
    crc: 0x1234_5678,
    token: 0xfeed_cafe_dead_beef_0123_4567_89ab_cdef,
    num_buckets: 1024,
    overflow_count: 16,
    entry_count: 5000,
  };
  let bytes = hdr.encode();
  assert_eq!(bytes.len(), HEADER_SIZE);
  // 保留字段（尾部 8 字节）恒为 0
  assert_eq!(&bytes[56..64], &[0u8; 8]);

  let decoded = IndexCkptHeader::decode_opt(&bytes).expect("IndexCkptHeader 解码失败");
  assert_eq!(decoded, hdr);

  // 极值边界往返
  let max_hdr = IndexCkptHeader {
    version: u32::MAX,
    crc: u32::MAX,
    token: u128::MAX,
    num_buckets: u64::MAX,
    overflow_count: u64::MAX,
    entry_count: u64::MAX,
  };
  let max_bytes = max_hdr.encode();
  assert_eq!(max_bytes.len(), HEADER_SIZE);
  assert_eq!(IndexCkptHeader::decode_opt(&max_bytes), Some(max_hdr));

  let zero_hdr = IndexCkptHeader {
    version: 0,
    crc: 0,
    token: 0,
    num_buckets: 0,
    overflow_count: 0,
    entry_count: 0,
  };
  let zero_bytes = zero_hdr.encode();
  assert_eq!(IndexCkptHeader::decode_opt(&zero_bytes), Some(zero_hdr));

  OK
}

/// 魔数错误与切片截断校验：任何损坏或不足 64 字节切片严格返回 None
#[test]
fn index_ckpt_header_decode_failures() -> Result<()> {
  let hdr = IndexCkptHeader {
    version: 1,
    crc: 0x55aa_33cc,
    token: 42,
    num_buckets: 64,
    overflow_count: 2,
    entry_count: 100,
  };
  let bytes = hdr.encode();

  // 校验魔数各个字节损坏分支
  for i in 0..8 {
    let mut bad_magic = bytes;
    bad_magic[i] ^= 0xff;
    assert!(
      IndexCkptHeader::decode_opt(&bad_magic).is_none(),
      "魔数字节 {i} 损坏未被拦截"
    );
  }

  // 长度不足 64 字节一律 None
  assert!(IndexCkptHeader::decode_opt(&[]).is_none());
  assert!(IndexCkptHeader::decode_opt(&bytes[..10]).is_none());
  assert!(IndexCkptHeader::decode_opt(&bytes[..63]).is_none());

  // 大于 64 字节的切片安全解码前 64 字节
  let mut extended = bytes.to_vec();
  extended.extend_from_slice(&[0xee; 64]);
  assert_eq!(IndexCkptHeader::decode_opt(&extended), Some(hdr));

  OK
}

/// 数据槽位净化测试：
/// 1. 0 槽位恒返回 0
/// 2. 试探性标记 (bit 63) 剥离归零
/// 3. 易失 ReadCache 指针 (bit 47) 剥离归零
/// 4. 超过 tail 截断点的历史条目截断归零
/// 5. 正常条目完好保留
#[test]
fn test_sanitize_data_slot_rules() -> Result<()> {
  // 1. 0 槽位
  assert_eq!(sanitize_data_slot(0, None), 0);
  assert_eq!(sanitize_data_slot(0, Some(100)), 0);

  // 2. 正常槽位（有效地址 + 指纹）
  let valid_slot = 0x1234_0000_0000_1000u64; // address = 0x1000, tag = 0x1234
  assert_eq!(sanitize_data_slot(valid_slot, None), valid_slot);
  assert_eq!(sanitize_data_slot(valid_slot, Some(0x2000)), valid_slot);

  // 3. 试探性标记槽位
  let tentative_slot = valid_slot | HashBucketEntry::TENTATIVE_MASK;
  assert_eq!(sanitize_data_slot(tentative_slot, None), 0);
  assert_eq!(sanitize_data_slot(tentative_slot, Some(0x2000)), 0);

  // 4. ReadCache 标记槽位
  let rc_slot = valid_slot | HashBucketEntry::READ_CACHE_BIT;
  assert_eq!(sanitize_data_slot(rc_slot, None), 0);
  assert_eq!(sanitize_data_slot(rc_slot, Some(0x2000)), 0);

  // 5. 两者叠加
  let both_slot = valid_slot | HashBucketEntry::TENTATIVE_MASK | HashBucketEntry::READ_CACHE_BIT;
  assert_eq!(sanitize_data_slot(both_slot, None), 0);

  // 6. tail 边界截断测试
  let addr_1000 = 0x1234_0000_0000_1000u64;
  assert_eq!(
    sanitize_data_slot(addr_1000, Some(0x1000)),
    0,
    "等于 tail 必须截断"
  );
  assert_eq!(
    sanitize_data_slot(addr_1000, Some(0x0fff)),
    0,
    "大于 tail 必须截断"
  );
  assert_eq!(
    sanitize_data_slot(addr_1000, Some(0x1001)),
    addr_1000,
    "小于 tail 放行"
  );

  OK
}

/// 溢出槽位净化测试：
/// 1. 剥离高 16 位并发自旋锁 Latch 瞬态标记
/// 2. 超出 max_overflow 的模糊区增量截断归零
/// 3. ≤ max_overflow 的合法 ID 原样保留
#[test]
fn test_sanitize_overflow_slot_rules() -> Result<()> {
  // 1. 正常在界溢出桶 ID
  assert_eq!(sanitize_overflow_slot(0, 100), 0);
  assert_eq!(sanitize_overflow_slot(1, 100), 1);
  assert_eq!(sanitize_overflow_slot(100, 100), 100);

  // 2. 超出界限截断归零
  assert_eq!(sanitize_overflow_slot(101, 100), 0);
  assert_eq!(sanitize_overflow_slot(0xFFFF_FFFF_FFFF, 100), 0);

  // 3. 高 16 位 Latch / 锁标记剥离
  let latched_valid = 0x8000_0000_0000_0005u64; // bit 63 set, id = 5
  assert_eq!(sanitize_overflow_slot(latched_valid, 10), 5);

  let latched_overflow = 0xFFFF_0000_0000_0050u64; // id = 80 > 10
  assert_eq!(sanitize_overflow_slot(latched_overflow, 10), 0);

  OK
}
