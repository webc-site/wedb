use std::{
  mem::{align_of, size_of},
  sync::atomic::{AtomicU64, Ordering},
};

use aok::{OK, Void};
use log::info;
use wbase::addr::ADDRESS_MASK;
use wrecord::{
  HEADER_READ_CACHE_BIT, HEADER_SIZE, IN_NEW_VERSION_BIT, MAX_FILLER_BYTES, MODIFIED_BIT,
  PAD_KEY_LEN, RecordHeader, RecordMut, RecordRef, SEALED_BIT, TOMBSTONE_BIT, try_encode_to_vec,
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

  // 验证各字段小端序排布（对标 C# RecordDataHeader 单 8 字节原子字：filler | key_len | val_len 同字）：
  // [0..8]: RecordInfo 字 (包含 48 位逻辑地址与高位标志位)
  // [8..16]: RDH 原子字 (bits 0..7 filler_words, bits 8..31 key_len, bits 32..63 val_len)
  let header = RecordHeader::new(0x0000_1234_5678_9abc, 32, 128, true)?;
  let bytes = header.to_bytes();

  let raw_prev = u64::from_le_bytes(*bytes[0..8].first_chunk::<8>().unwrap());
  let raw_rdh = u64::from_le_bytes(*bytes[8..16].first_chunk::<8>().unwrap());

  assert_eq!(raw_prev, 0x0000_1234_5678_9abc | TOMBSTONE_BIT);
  assert_eq!(raw_rdh & 0xFF, 0); // filler_words = 0
  assert_eq!((raw_rdh >> 8) & 0xFF_FFFF, 32); // key_len 24 位位段
  assert_eq!(raw_rdh >> 32, 128); // val_len 32 位位段

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
/// - HEADER_READ_CACHE_BIT (bit 62): 读缓存指针标记 (RecordInfo.IsReadCache)
/// - TOMBSTONE_BIT (bit 63): 墓碑删除标记 (RecordInfo.Tombstone)
#[test]
fn test_record_info_atomic_bits_lifecycle() -> Void {
  info!("开始测试: RecordInfo 原子位标志生命周期与正交性");

  // 1. 验证掩码位定义与正交性
  assert_eq!(MODIFIED_BIT, 1u64 << 59);
  assert_eq!(SEALED_BIT, 1u64 << 60);
  assert_eq!(IN_NEW_VERSION_BIT, 1u64 << 61);
  assert_eq!(HEADER_READ_CACHE_BIT, 1u64 << 62);
  assert_eq!(TOMBSTONE_BIT, 1u64 << 63);

  let all_flags =
    MODIFIED_BIT | SEALED_BIT | IN_NEW_VERSION_BIT | HEADER_READ_CACHE_BIT | TOMBSTONE_BIT;
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

  hdr.set_tombstone(true);
  assert!(hdr.is_tombstone());
  assert_eq!(hdr.address(), 0x0000_AABB_CCDD_EEFF);

  // 逐一清除标志
  hdr.set_modified(false);
  assert!(!hdr.is_modified());
  assert!(hdr.is_sealed());

  hdr.set_sealed(false);
  assert!(!hdr.is_sealed());
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
  }

  let raw_word = u64::from_le_bytes(raw_buf[0..8].try_into().expect("8 字节切片"));
  assert_eq!(raw_word, 0x0000_1122_3344_5566 | MODIFIED_BIT | SEALED_BIT);

  let rec_ref = RecordRef::from_slice(&raw_buf)?;
  assert_eq!(rec_ref.prev_address(), 0x0000_1122_3344_5566);
  assert!(rec_ref.is_modified());
  assert!(rec_ref.is_sealed());
  assert!(!rec_ref.is_in_new_version());
  assert!(!rec_ref.is_read_cache());
  assert!(!rec_ref.is_tombstone());

  info!("RecordInfo 原子位标志生命周期与正交性测试通过");
  OK
}

/// 纪元位 setter 与链地址正交性测试
/// 对标 C# Tsavorite RecordInfo.cs (SetIsInNewVersion)
#[test]
fn test_set_in_new_version_preserves_chain_address_and_other_bits() -> Void {
  let mut hdr = RecordHeader::new(0x0000_1234_5678_9ABC, 16, 8, true)?;
  hdr.set_modified(true);
  assert!(!hdr.is_in_new_version());

  hdr.set_in_new_version(true);
  assert!(hdr.is_in_new_version());
  // 置位纪元位不得侵蚀链地址与其他元数据位
  assert_eq!(hdr.address(), 0x0000_1234_5678_9ABC);
  assert!(hdr.is_modified());
  assert!(hdr.is_tombstone());

  hdr.set_in_new_version(false);
  assert!(!hdr.is_in_new_version());
  assert_eq!(hdr.address(), 0x0000_1234_5678_9ABC);
  assert!(hdr.is_modified());
  assert!(hdr.is_tombstone());
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

  // 墓碑标记下前驱地址保持不变
  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    rec_mut.set_tombstone(true);
    assert!(rec_mut.is_tombstone());
    assert_eq!(rec_mut.prev_address(), initial_prev_addr);

    let view = rec_mut.as_ref();
    assert!(view.is_tombstone());
    assert_eq!(view.prev_address(), initial_prev_addr);
  }

  // RecordHeader 直接设置与清空墓碑，以及密封位原位清除（rust 持久化路径恒不置位
  // SEALED，见 whlog SEALED 位约定，无需 C# ClearBitsForDiskImages 刷盘清位对位口）
  {
    let mut hdr = RecordHeader::new(initial_prev_addr, 4, 8, false)?;
    hdr.set_tombstone(true);
    assert!(hdr.is_tombstone());
    hdr.set_sealed(true);
    assert!(hdr.is_sealed());
    hdr.set_sealed(false);
    assert!(!hdr.is_sealed());
    assert!(hdr.is_tombstone());
    hdr.set_tombstone(false);
    assert!(!hdr.is_tombstone());
  }

  info!("墓碑删除状态生命周期与地址保持测试通过");
  OK
}

/// 编译期常量编解码求值一致性测试
/// 对标 C# Tsavorite RecordInfo/RDH 双 8 字节字的编译期常量直构
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

  // 零堆分配常量编码与解析（to_bytes 为定长数组单点序列化出口）
  let slice = DECODED_HEADER.to_bytes();
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
  info!("开始测试: Pad 记录构造与快速只读探针");

  let pad = RecordHeader::pad(128);
  assert!(pad.is_pad());
  assert_eq!(pad.key_len(), PAD_KEY_LEN);
  assert_eq!(pad.val_len(), 128 - HEADER_SIZE as u32);
  assert_eq!(pad.address(), 0);

  // 切片读侧统一经 decode_opt + 实例 getter 单点承接；read_address 为墓碑 CAS 走链
  // 专用首字探针（whlog/wkv 生产在用）
  let pad_bytes = pad.to_bytes();
  let pad_decoded = RecordHeader::decode_opt(&pad_bytes).expect("Pad 头应可解码");
  assert!(pad_decoded.is_pad());
  assert_eq!(pad_decoded.key_len(), PAD_KEY_LEN);
  assert_eq!(pad_decoded.val_len(), 128 - HEADER_SIZE as u32);
  assert!(!pad_decoded.is_tombstone());
  assert_eq!(RecordHeader::read_address(&pad_bytes), Some(0));

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
  assert!(
    !RecordHeader::decode_opt(&non_zero)
      .expect("16 字节头应可解码")
      .is_pad()
  );

  info!("Pad 记录构造与快速只读探针测试通过");
  OK
}

/// 对标 C# Tsavorite RecordInfo.cs 深度对标测试
/// 验证 TrySeal, TryResetModifiedAtomic, TryUpdateAddress, SetInvalidAtomic 等
#[test]
fn test_record_info_cas_and_lifecycle() -> Void {
  info!("开始测试: RecordInfo CAS 与状态机生命周期");

  // 1. 密封新头直构（rust 以整头覆写 + 单字原子发布协议承接 C#
  //    InitializeForNewRecord/WriteInfo 的密封中间态，见 whlog revivify_record_at）
  let mut h = RecordHeader::from_words(SEALED_BIT, 0);
  assert!(h.is_sealed());
  assert!(h.is_closed());
  assert!(!h.is_tombstone());
  assert_eq!(h.address(), 0);

  h.set_address(0x1234_5678)?;
  assert!(h.is_sealed());
  assert!(!h.is_in_new_version());
  assert_eq!(h.address(), 0x1234_5678);

  h.set_sealed(false);
  assert!(!h.is_sealed());
  assert!(!h.is_closed());

  h.set_sealed(true);
  assert!(h.is_sealed());
  assert!(h.is_closed());

  // 2. InPlaceUpdated 别名与 Modified 一致性
  assert!(!h.is_in_place_updated());
  h.set_modified(true);
  assert!(h.is_in_place_updated());
  assert!(h.is_modified());
  h.set_modified(false);
  assert!(!h.is_in_place_updated());
  assert!(!h.is_modified());

  // 3. Display / ToString 输出
  let display_str = format!("{h}");
  assert!(display_str.contains("RecordHeader"));
  assert!(display_str.contains("0x12345678"));

  // 4. 原子 CAS: TrySeal
  let word = AtomicU64::new(0x1000);
  assert!(RecordHeader::try_seal(&word, false));
  assert_eq!(word.load(Ordering::Relaxed) & SEALED_BIT, SEALED_BIT);
  // 已密封再次 try_seal 应返回 false
  assert!(!RecordHeader::try_seal(&word, false));

  // 5. 原子 CAS: TryResetModifiedAtomic
  let word_mod = AtomicU64::new(0x2000 | MODIFIED_BIT);
  assert!(RecordHeader::try_reset_modified_atomic(&word_mod));
  assert_eq!(word_mod.load(Ordering::Relaxed) & MODIFIED_BIT, 0);
  // 再次重置返回 true（幂等）
  assert!(RecordHeader::try_reset_modified_atomic(&word_mod));

  // 6. 原子 CAS: TryUpdateAddress
  let word_addr = AtomicU64::new(0x3000);
  assert!(RecordHeader::try_update_address(&word_addr, 0x3000, 0x4000));
  assert_eq!(word_addr.load(Ordering::Relaxed) & ADDRESS_MASK, 0x4000);
  // expected 不匹配时返回 false
  assert!(!RecordHeader::try_update_address(
    &word_addr, 0x3000, 0x5000
  ));
  assert_eq!(word_addr.load(Ordering::Relaxed) & ADDRESS_MASK, 0x4000);

  // 7. 原子 CAS: SetInvalidAtomic
  RecordHeader::set_invalid_atomic(&word_addr);
  assert_eq!(word_addr.load(Ordering::Relaxed) & SEALED_BIT, SEALED_BIT);

  // 8. RecordMut 上的对应原语
  let mut raw = try_encode_to_vec(0x5000, b"k", b"v", false)?;
  let mut rec_mut = RecordMut::from_slice_mut(&mut raw)?;
  assert!(!rec_mut.is_closed());
  assert!(rec_mut.try_seal(false));
  assert!(rec_mut.is_closed());
  assert!(!rec_mut.try_seal(false)); // 已 sealed 再次失败

  rec_mut.set_modified(true);
  assert!(rec_mut.is_in_place_updated());
  rec_mut.set_sealed(false);
  assert!(rec_mut.try_reset_modified_atomic());
  assert!(!rec_mut.is_in_place_updated());

  assert!(rec_mut.try_update_address(0x5000, 0x6000));
  assert_eq!(rec_mut.prev_address(), 0x6000);
  assert!(rec_mut.is_valid());
  assert!(!rec_mut.is_invalid());
  assert!(!rec_mut.skip_on_scan());

  rec_mut.set_invalid_atomic();
  assert!(rec_mut.is_closed());
  assert!(rec_mut.is_invalid());
  assert!(!rec_mut.is_valid());
  assert!(rec_mut.skip_on_scan());

  // 9. 前驱地址 set_address 直写与 Valid/Invalid/SkipOnScan 状态验证
  //    （rust 无 Valid 位，初始有效头常量 InitialValid 无对位需求，已清理）
  let mut set_addr_hdr = RecordHeader::default();
  set_addr_hdr.set_address(0x7000)?;
  assert_eq!(set_addr_hdr.address(), 0x7000);
  assert!(set_addr_hdr.is_valid());
  assert!(!set_addr_hdr.is_invalid());
  assert!(!set_addr_hdr.skip_on_scan());

  info!("RecordInfo CAS 与状态机生命周期测试通过");
  OK
}

/// FillerWords 独立词级设置与字节级松弛折算测试（对标 RecordDataHeader FillerWords setter 逻辑）
#[test]
fn test_filler_words_field_setter() -> Void {
  info!("开始测试: FillerWords 词级设置与字节级松弛折算");

  let mut hdr = RecordHeader::new(0x10, 8, 16, false)?;
  assert_eq!(hdr.filler_bytes(), 0);

  // 词级设置：4 词 = 32 字节
  hdr.set_filler_words(4);
  assert_eq!(hdr.filler_words(), 4);
  assert_eq!(hdr.filler_bytes(), 32);
  assert_eq!(hdr.address(), 0x10);

  // 字节级设置按词粒度折算（记录 8 字节对齐不变式下松弛差值恒为词整数倍）：
  // 31 字节向下取整为 3 词 = 24 字节
  hdr.set_filler_bytes(31);
  assert_eq!(hdr.filler_words(), 3);
  assert_eq!(hdr.filler_bytes(), 24);

  // 词级设置保留键长与值长位段：5 词 = 40 字节
  hdr.set_filler_words(5);
  assert_eq!(hdr.filler_words(), 5);
  assert_eq!(hdr.filler_bytes(), 40);
  assert_eq!(hdr.address(), 0x10);
  assert_eq!(hdr.key_len(), 8);
  assert_eq!(hdr.val_len(), 16);

  // 上限钳位：2040 = 255 词
  hdr.set_filler_bytes(MAX_FILLER_BYTES + 128);
  assert_eq!(hdr.filler_words(), 255);
  assert_eq!(hdr.filler_bytes(), MAX_FILLER_BYTES);

  OK
}
