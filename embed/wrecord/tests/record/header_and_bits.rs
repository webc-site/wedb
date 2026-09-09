use std::mem::{align_of, size_of};

use aok::{OK, Void};
use log::info;
use wrecord::{
  ADDRESS_MASK, HEADER_SIZE, IN_NEW_VERSION_BIT, MODIFIED_BIT, READ_CACHE_BIT, RecordHeader,
  RecordMut, RecordRef, SEALED_BIT, TOMBSTONE_BIT, try_encode_to_vec,
};

/// 头部内存排布与字段偏移一致性测试
/// 对标 C# Tsavorite RecordInfo.cs (RecordInfo.Size = 8, RecordDataHeader.Size = 8, Constants.FixedHeaderSize = 16)
/// 内存对齐为 8 字节 (Constants.kRecordAlignment = 8)
#[test]
fn test_header_layout_and_field_offsets() -> Void {
  info!("开始测试: 记录头内存排布与字段偏移一致性");

  assert_eq!(size_of::<RecordHeader>(), 16);
  assert_eq!(HEADER_SIZE, 16);
  assert_eq!(align_of::<RecordHeader>(), 8);

  // 验证各字段小端序排布：
  // [0..8]: prev_address (包含 48 位逻辑地址与高位标志位)
  // [8..12]: key_len (u32)
  // [12..16]: val_len (u32)
  let header = RecordHeader::new(0x0000_1234_5678_9abc, 32, 128, true)?;
  let bytes = header.to_bytes();

  let raw_prev = u64::from_le_bytes(*bytes[0..8].first_chunk::<8>().unwrap());
  let raw_klen = u32::from_le_bytes(*bytes[8..12].first_chunk::<4>().unwrap());
  let raw_vlen = u32::from_le_bytes(*bytes[12..16].first_chunk::<4>().unwrap());

  assert_eq!(raw_prev, 0x0000_1234_5678_9abc | TOMBSTONE_BIT);
  assert_eq!(raw_klen, 32);
  assert_eq!(raw_vlen, 128);

  // 对标 libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:IsNull 与 RecordDataHeader.GetRecordLength 零头守卫
  let null_hdr = RecordHeader::default();
  assert!(null_hdr.is_null());
  assert!(!header.is_null());

  info!("记录头内存排布与字段偏移一致性测试通过");
  OK
}

/// 48-bit 逻辑地址打包与掩码极限测试
/// 对标 C# Tsavorite LogAddress.cs (低 48 位逻辑寻址空间，最大 256TB)
#[test]
fn test_address_48bit_mask_and_packing() -> Void {
  info!("开始测试: 48 位逻辑地址打包与掩码");

  assert_eq!(ADDRESS_MASK, 0x0000_FFFF_FFFF_FFFF);
  assert_eq!(ADDRESS_MASK, (1u64 << 48) - 1);

  // 最大 48 位合法地址编码与还原
  let max_48bit_addr = ADDRESS_MASK;
  let max_encoded = try_encode_to_vec(max_48bit_addr, b"k_limit", b"v_limit", false)?;
  let max_ref = RecordRef::from_slice(&max_encoded)?;
  assert_eq!(max_ref.prev_address(), max_48bit_addr);

  // 零地址（Genesis 创世地址）
  let zero_encoded = try_encode_to_vec(0, b"genesis", b"data", false)?;
  let zero_ref = RecordRef::from_slice(&zero_encoded)?;
  assert_eq!(zero_ref.prev_address(), 0);

  info!("48 位逻辑地址打包与掩码测试通过");
  OK
}

/// 原子位标志生命周期与正交性测试
/// 对标 C# Tsavorite RecordInfo.cs 原子位标志：
/// - MODIFIED_BIT (bit 59): 检查点脏页标记 (RecordInfo.Modified)
/// - SEALED_BIT (bit 60): 槽位密封/冻结 (RecordInfo.Sealed / TrySeal)
/// - IN_NEW_VERSION_BIT (bit 61): 检查点纪元标记 (RecordInfo.IsInNewVersion)
/// - READ_CACHE_BIT (bit 62): 读缓存指针标记 (RecordInfo.IsReadCache)
/// - TOMBSTONE_BIT (bit 63): 墓碑删除标记 (RecordInfo.Tombstone)
#[test]
fn test_record_info_atomic_bits_lifecycle() -> Void {
  info!("开始测试: RecordInfo 原子位标志生命周期与正交性");

  // 1. 验证掩码位定义与正交性
  assert_eq!(MODIFIED_BIT, 1u64 << 59);
  assert_eq!(SEALED_BIT, 1u64 << 60);
  assert_eq!(IN_NEW_VERSION_BIT, 1u64 << 61);
  assert_eq!(READ_CACHE_BIT, 1u64 << 62);
  assert_eq!(TOMBSTONE_BIT, 1u64 << 63);

  let all_flags = MODIFIED_BIT | SEALED_BIT | IN_NEW_VERSION_BIT | READ_CACHE_BIT | TOMBSTONE_BIT;
  assert_eq!(all_flags & ADDRESS_MASK, 0);
  assert_eq!(all_flags & 0x07FF_FFFF_FFFF_FFFF, 0);

  // 2. RecordHeader 原生标志位设置与清除
  let mut hdr = RecordHeader::new(0x0000_AABB_CCDD_EEFF, 32, 64, false)?;
  assert_eq!(hdr.address(), 0x0000_AABB_CCDD_EEFF);
  assert!(!hdr.is_modified());
  assert!(!hdr.is_sealed());
  assert!(!hdr.is_in_new_version());
  assert!(!hdr.is_read_cache());
  assert!(!hdr.is_tombstone());

  hdr.set_modified(true);
  assert!(hdr.is_modified());
  assert_eq!(hdr.address(), 0x0000_AABB_CCDD_EEFF);

  hdr.set_sealed(true);
  assert!(hdr.is_sealed());

  hdr.set_in_new_version(true);
  assert!(hdr.is_in_new_version());

  hdr.set_read_cache(true);
  assert!(hdr.is_read_cache());

  hdr.set_tombstone(true);
  assert!(hdr.is_tombstone());
  assert_eq!(hdr.address(), 0x0000_AABB_CCDD_EEFF);

  // 逐一清除标志
  hdr.set_modified(false);
  assert!(!hdr.is_modified());
  assert!(hdr.is_sealed());

  hdr.set_sealed(false);
  assert!(!hdr.is_sealed());
  assert!(hdr.is_in_new_version());

  hdr.set_in_new_version(false);
  assert!(!hdr.is_in_new_version());
  assert!(hdr.is_read_cache());

  hdr.set_read_cache(false);
  assert!(!hdr.is_read_cache());
  assert!(hdr.is_tombstone());

  hdr.set_tombstone(false);
  assert!(!hdr.is_tombstone());
  assert_eq!(hdr.address(), 0x0000_AABB_CCDD_EEFF);

  // 3. RecordMut / RecordRef 原位字节同步与零拷贝验证
  let key = b"flag_test_key";
  let val = b"flag_test_val";
  let mut raw_buf = try_encode_to_vec(0x0000_1122_3344_5566, key, val, false)?;

  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut raw_buf)?;
    assert_eq!(rec_mut.prev_address(), 0x0000_1122_3344_5566);
    assert!(!rec_mut.is_modified());
    assert!(!rec_mut.is_sealed());
    assert!(!rec_mut.is_in_new_version());
    assert!(!rec_mut.is_read_cache());

    rec_mut.set_modified(true);
    rec_mut.set_sealed(true);
    rec_mut.set_in_new_version(true);
    rec_mut.set_read_cache(true);
  }

  let raw_word = u64::from_le_bytes(raw_buf[0..8].try_into().expect("8 字节切片"));
  assert_eq!(
    raw_word,
    0x0000_1122_3344_5566 | MODIFIED_BIT | SEALED_BIT | IN_NEW_VERSION_BIT | READ_CACHE_BIT
  );

  let rec_ref = RecordRef::from_slice(&raw_buf)?;
  assert_eq!(rec_ref.prev_address(), 0x0000_1122_3344_5566);
  assert!(rec_ref.is_modified());
  assert!(rec_ref.is_sealed());
  assert!(rec_ref.is_in_new_version());
  assert!(rec_ref.is_read_cache());
  assert!(!rec_ref.is_tombstone());

  info!("RecordInfo 原子位标志生命周期与正交性测试通过");
  OK
}

/// 墓碑状态生命周期与地址保持测试
/// 对标 C# Tsavorite RecordInfo.cs (kTombstoneBitMask, SetTombstone, ClearTombstone)
#[test]
fn test_tombstone_lifecycle_and_address_preservation() -> Void {
  info!("开始测试: 墓碑删除状态生命周期与地址保持");

  let key = b"session:user_auth_token";
  let val = b"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
  let initial_prev_addr = 0x0000_1234_abcd_5678_u64;

  let mut buf = try_encode_to_vec(initial_prev_addr, key, val, false)?;
  {
    let rec_ref = RecordRef::from_slice(&buf)?;
    assert!(!rec_ref.is_tombstone());
    assert_eq!(rec_ref.prev_address(), initial_prev_addr);
  }

  // 原位标记为墓碑
  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    rec_mut.set_tombstone(true);
    assert!(rec_mut.is_tombstone());
    assert_eq!(rec_mut.prev_address(), initial_prev_addr);
  }

  // 验证底层二进制字节
  {
    let rec_ref = RecordRef::from_slice(&buf)?;
    assert!(rec_ref.is_tombstone());
    assert_eq!(rec_ref.prev_address(), initial_prev_addr);

    let raw_header = RecordHeader::from_slice(&buf[..HEADER_SIZE])?;
    assert_eq!(raw_header.prev_address, initial_prev_addr | TOMBSTONE_BIT);
    assert_eq!(raw_header.address(), initial_prev_addr);
  }

  // 原位清除墓碑标记（复活）
  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    rec_mut.set_tombstone(false);
    assert!(!rec_mut.is_tombstone());
    assert_eq!(rec_mut.prev_address(), initial_prev_addr);
  }

  {
    let rec_ref = RecordRef::from_slice(&buf)?;
    assert!(!rec_ref.is_tombstone());
    assert_eq!(rec_ref.prev_address(), initial_prev_addr);
    let raw_header = RecordHeader::from_slice(&buf[..HEADER_SIZE])?;
    assert_eq!(raw_header.prev_address, initial_prev_addr);
  }

  // 墓碑标记下更新前驱地址
  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    rec_mut.set_tombstone(true);
    let new_prev_addr = 0x0000_8765_4321_0000_u64;
    rec_mut.set_prev_address(new_prev_addr)?;
    assert!(rec_mut.is_tombstone());
    assert_eq!(rec_mut.prev_address(), new_prev_addr);

    let view = rec_mut.as_ref();
    assert!(view.is_tombstone());
    assert_eq!(view.prev_address(), new_prev_addr);
  }

  info!("墓碑删除状态生命周期与地址保持测试通过");
  OK
}

/// 编译期常量编解码求值一致性测试
/// 对标 C# Tsavorite RecordInfo.InitialValid 与编译期结构初始化
#[test]
fn test_header_const_codec_evaluation() -> Void {
  info!("开始测试: 编译期常量编解码求值");

  const RAW_HEADER: RecordHeader = RecordHeader::from_raw(0x0000_1234_5678_9abc, 16, 64);
  const CONST_BYTES: [u8; HEADER_SIZE] = RAW_HEADER.to_bytes();
  const DECODED_HEADER: RecordHeader = RecordHeader::from_bytes(CONST_BYTES);

  assert_eq!(DECODED_HEADER.address(), 0x0000_1234_5678_9abc);
  assert_eq!(DECODED_HEADER.key_len(), 16);
  assert_eq!(DECODED_HEADER.val_len(), 64);
  assert!(!DECODED_HEADER.is_tombstone());
  assert_eq!(DECODED_HEADER.record_size(), 16 + 16 + 64);

  // 零堆分配切片写入与解析
  let mut slice = [0u8; HEADER_SIZE];
  DECODED_HEADER.write_to_slice(&mut slice)?;
  assert_eq!(slice, CONST_BYTES);
  let parsed = RecordHeader::from_slice(&slice)?;
  assert_eq!(parsed, DECODED_HEADER);

  // flip_tombstone 翻转操作测试
  let mut mut_hdr = DECODED_HEADER;
  let flipped = mut_hdr.flip_tombstone();
  assert!(flipped);
  assert!(mut_hdr.is_tombstone());
  assert_eq!(mut_hdr.address(), 0x0000_1234_5678_9abc);
  let flipped_back = mut_hdr.flip_tombstone();
  assert!(!flipped_back);
  assert!(!mut_hdr.is_tombstone());

  info!("编译期常量编解码求值测试通过");
  OK
}

/// Pad 记录构造、is_pad、is_zero_slice 与快速只读探针测试
#[test]
fn test_pad_and_probes() -> Void {
  use wrecord::PAD_KEY_LEN;

  info!("开始测试: Pad 记录构造与快速只读探针");

  let pad = RecordHeader::pad(128);
  assert!(pad.is_pad());
  assert_eq!(pad.key_len, PAD_KEY_LEN);
  assert_eq!(pad.val_len, 128 - HEADER_SIZE as u32);
  assert_eq!(pad.address(), 0);

  let pad_bytes = pad.to_bytes();
  assert_eq!(RecordHeader::read_is_pad(&pad_bytes), Some(true));
  assert_eq!(RecordHeader::read_key_len(&pad_bytes), Some(PAD_KEY_LEN));
  assert_eq!(
    RecordHeader::read_val_len(&pad_bytes),
    Some(128 - HEADER_SIZE as u32)
  );
  assert_eq!(RecordHeader::read_address(&pad_bytes), Some(0));
  assert_eq!(RecordHeader::read_is_tombstone(&pad_bytes), Some(false));

  assert_eq!(RecordHeader::decode_opt(&pad_bytes), Some(pad));
  assert_eq!(RecordHeader::decode_opt(&pad_bytes[..15]), None);

  // is_zero_slice 快速双字零头探测
  let zeros = [0u8; 32];
  assert!(RecordHeader::is_zero_slice(&zeros));
  assert!(RecordHeader::is_zero_slice(&zeros[..8])); // 长度不足 16 但全零

  let mut small_non_zero = [0u8; 8];
  small_non_zero[3] = 1;
  assert!(!RecordHeader::is_zero_slice(&small_non_zero));

  let mut non_zero = [0u8; 16];
  non_zero[15] = 1;
  assert!(!RecordHeader::is_zero_slice(&non_zero));
  assert_eq!(RecordHeader::read_is_pad(&non_zero), Some(false));

  info!("Pad 记录构造与快速只读探针测试通过");
  OK
}
