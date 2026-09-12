use aok::{OK, Void};
use log::info;
use wrecord::{
  ADDRESS_MASK, Error, HEADER_SIZE, MAX_FILLER_BYTES, RecordHeader, RecordMut, RecordRef,
  TOMBSTONE_BIT, encode_to_slice, record_size, try_encode_to_vec,
};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// 编解码往返一致性测试（支持空与非空键值、常规与边界地址）
#[test]
fn test_roundtrip_encode_decode() -> Void {
  info!("开始测试: 编解码往返一致性");

  // 测试普通键值对
  let key = b"user:10001:profile";
  let val = b"{\"name\":\"Alice\",\"age\":30,\"active\":true}";
  let prev_addr = 0x0000_1234_5678_9abc_u64;

  let encoded = try_encode_to_vec(prev_addr, key, val, false)?;
  assert_eq!(encoded.len(), record_size(key.len(), val.len()));

  let rec_ref = RecordRef::from_slice(&encoded)?;
  assert_eq!(rec_ref.key(), key);
  assert_eq!(rec_ref.value(), val);
  assert_eq!(rec_ref.prev_address(), prev_addr);
  assert!(!rec_ref.is_tombstone());
  assert_eq!(rec_ref.total_size(), encoded.len());

  // 测试空键与空值情况
  let empty_encoded = try_encode_to_vec(0, b"", b"", false)?;
  assert_eq!(empty_encoded.len(), HEADER_SIZE);
  let empty_ref = RecordRef::from_slice(&empty_encoded)?;
  assert_eq!(empty_ref.key(), b"");
  assert_eq!(empty_ref.value(), b"");
  assert_eq!(empty_ref.prev_address(), 0);
  assert!(!empty_ref.is_tombstone());

  // 测试最大 48 位合法地址
  let max_addr = ADDRESS_MASK;
  let max_addr_encoded = try_encode_to_vec(max_addr, b"k", b"v", false)?;
  let max_addr_ref = RecordRef::from_slice(&max_addr_encoded)?;
  assert_eq!(max_addr_ref.prev_address(), max_addr);

  // 测试切片编码与容量足够的大缓冲区
  let mut large_buf = vec![0u8; 1024];
  let written = encode_to_slice(&mut large_buf, prev_addr, key, val, false)?;
  assert_eq!(written, record_size(key.len(), val.len()));

  let slice_ref = RecordRef::from_slice(&large_buf)?;
  assert_eq!(slice_ref.key(), key);
  assert_eq!(slice_ref.value(), val);

  info!("编解码往返一致性测试通过");
  OK
}

/// 墓碑标记设置与检测测试
#[test]
fn test_tombstone_flag() -> Void {
  info!("开始测试: 墓碑标记设置与检测");

  let key = b"deleted_key";
  let val = b"";
  let prev_addr = 0x0000_aabb_ccdd_eeff_u64;

  // 1. 编码为墓碑记录
  let encoded = try_encode_to_vec(prev_addr, key, val, true)?;
  let rec_ref = RecordRef::from_slice(&encoded)?;
  assert!(rec_ref.is_tombstone());
  assert_eq!(rec_ref.prev_address(), prev_addr);

  // 检查底层字节：最高位应已置位
  let raw_header = RecordHeader::from_slice(&encoded[..HEADER_SIZE])?;
  assert_eq!(raw_header.prev_address, prev_addr | TOMBSTONE_BIT);

  // 2. 在 RecordMut 中原位切换墓碑标记
  let mut buf = encoded.clone();
  let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
  assert!(rec_mut.is_tombstone());

  // 取消墓碑标记
  rec_mut.set_tombstone(false);
  assert!(!rec_mut.is_tombstone());
  assert_eq!(rec_mut.prev_address(), prev_addr);

  // 重新置为墓碑
  rec_mut.set_tombstone(true);
  assert!(rec_mut.is_tombstone());
  assert_eq!(rec_mut.prev_address(), prev_addr);

  // 重新从切片解码确认底层数据同步写入
  let check_ref = RecordRef::from_slice(&buf)?;
  assert!(check_ref.is_tombstone());
  assert_eq!(check_ref.prev_address(), prev_addr);

  info!("墓碑标记设置与检测测试通过");
  OK
}

/// 零拷贝借用 RecordRef 测试（指针比对确认无内存分配与拷贝）
#[test]
fn test_record_ref_zero_copy() -> Void {
  info!("开始测试: 零拷贝借用 RecordRef");

  let key = b"my_benchmark_key_12345";
  let val = b"my_benchmark_val_67890";
  let prev_addr = 0x42;

  let encoded = try_encode_to_vec(prev_addr, key, val, false)?;
  let rec_ref = RecordRef::from_slice(&encoded)?;

  // 检验借用切片指针与原始缓冲区精确一致
  let expected_key_ptr = unsafe { encoded.as_ptr().add(HEADER_SIZE) };
  let expected_val_ptr = unsafe { encoded.as_ptr().add(HEADER_SIZE + key.len()) };

  assert_eq!(rec_ref.key().as_ptr(), expected_key_ptr);
  assert_eq!(rec_ref.value().as_ptr(), expected_val_ptr);
  assert_eq!(rec_ref.key().len(), key.len());
  assert_eq!(rec_ref.value().len(), val.len());

  info!("零拷贝借用指针精确匹配测试通过");
  OK
}

/// 原位值更新 RecordMut 测试（成功更新与长度不匹配防御拦截）
#[test]
fn test_record_mut_in_place_update() -> Void {
  info!("开始测试: 原位值更新与防御拦截");

  let key = b"counter_key";
  let initial_val = b"0000000010"; // 10 字节
  let prev_addr = 0x0000_0001_0000_0000_u64;

  let mut buf = try_encode_to_vec(prev_addr, key, initial_val, false)?;
  let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;

  assert_eq!(rec_mut.key(), key);
  assert_eq!(rec_mut.value(), initial_val);

  // 1. 等长原位更新成功
  let updated_val = b"0000000020"; // 同样为 10 字节
  rec_mut.update_value_in_place(updated_val)?;
  assert_eq!(rec_mut.value(), updated_val);
  assert!(rec_mut.is_modified());

  // 2. 长度过短拦截
  let short_val = b"0020";
  let err_short = rec_mut.update_value_in_place(short_val);
  assert_eq!(
    err_short,
    Err(Error::ValueLengthMismatch {
      expected: 10,
      actual: 4,
    })
  );

  // 3. 长度过长拦截
  let long_val = b"00000000000000000020";
  let err_long = rec_mut.update_value_in_place(long_val);
  assert_eq!(
    err_long,
    Err(Error::ValueLengthMismatch {
      expected: 10,
      actual: 20,
    })
  );

  // 确认失败的更新未破坏原有值
  assert_eq!(rec_mut.value(), updated_val);

  // 4. 转换为 RecordRef 检验最终一致性
  let view = rec_mut.as_ref();
  assert_eq!(view.key(), key);
  assert_eq!(view.value(), updated_val);
  assert_eq!(view.prev_address(), prev_addr);

  info!("原位值更新与防御拦截测试通过");
  OK
}

/// 缓冲区截断与越界错误处理测试
#[test]
fn test_buffer_too_short_and_bounds_check() -> Void {
  info!("开始测试: 缓冲区截断与越界防御");

  // 1. 头长度不足 16 字节
  let tiny_buf = [0u8; 15];
  assert_eq!(
    RecordHeader::from_slice(&tiny_buf),
    Err(Error::BufferTooShort {
      expected: 16,
      actual: 15,
    })
  );
  assert_eq!(
    RecordRef::from_slice(&tiny_buf),
    Err(Error::BufferTooShort {
      expected: 16,
      actual: 15,
    })
  );

  let mut tiny_mut_buf = [0u8; 10];
  assert_eq!(
    RecordMut::from_slice_mut(&mut tiny_mut_buf),
    Err(Error::BufferTooShort {
      expected: 16,
      actual: 10,
    })
  );

  // 2. 头合法，但键值数据截断
  let key = b"test_key";
  let val = b"test_val_12345";
  let full_buf = try_encode_to_vec(1, key, val, false)?;
  let total_len = full_buf.len();

  // 模拟截断 1 字节
  let truncated_buf = &full_buf[..total_len - 1];
  assert_eq!(
    RecordRef::from_slice(truncated_buf),
    Err(Error::BufferTooShort {
      expected: total_len,
      actual: total_len - 1,
    })
  );

  // 3. 编码时目标缓冲区容量不足
  let mut small_dst = vec![0u8; total_len - 1];
  assert_eq!(
    encode_to_slice(&mut small_dst, 1, key, val, false),
    Err(Error::BufferTooShort {
      expected: total_len,
      actual: total_len - 1,
    })
  );

  // 4. 地址溢出防御
  let overflow_addr = 1u64 << 48;
  assert_eq!(
    try_encode_to_vec(overflow_addr, key, val, false),
    Err(Error::AddressOverflow(overflow_addr))
  );

  let mut valid_dst = vec![0u8; total_len];
  assert_eq!(
    encode_to_slice(&mut valid_dst, overflow_addr, key, val, false),
    Err(Error::AddressOverflow(overflow_addr))
  );

  assert_eq!(
    RecordHeader::new(overflow_addr, 1, 1, false),
    Err(Error::AddressOverflow(overflow_addr))
  );

  info!("缓冲区截断与越界防御测试通过");
  OK
}

/// RecordHeader 与 RecordMut 墓碑翻转与原位更新判断测试
#[test]
fn test_tombstone_flip_and_can_update_in_place() -> Void {
  info!("开始测试: RecordHeader/RecordMut 翻转墓碑与原位更新判断");

  // 1. RecordHeader 原生测试
  let mut header = RecordHeader::new(0x1234, 10, 20, false)?;
  assert!(!header.is_tombstone());
  assert!(header.can_update_in_place(20));
  assert!(!header.can_update_in_place(19));
  assert!(!header.can_update_in_place(21));

  // 翻转为墓碑
  assert!(header.flip_tombstone());
  assert!(header.is_tombstone());
  // 墓碑状态下禁止原位更新
  assert!(!header.can_update_in_place(20));

  // 再次翻转清除墓碑
  assert!(!header.flip_tombstone());
  assert!(!header.is_tombstone());
  assert!(header.can_update_in_place(20));

  // 2. RecordMut 原位翻转测试
  let mut buf = try_encode_to_vec(0x5678, b"test_key", b"1234567890", false)?;
  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    assert!(!rec_mut.is_tombstone());
    assert!(rec_mut.can_update_in_place(10));
    assert!(!rec_mut.can_update_in_place(5));

    // 原位翻转并检查底层切片同步
    assert!(rec_mut.flip_tombstone());
    assert!(rec_mut.is_tombstone());
    assert_eq!(rec_mut.prev_address(), 0x5678);
    assert!(!rec_mut.can_update_in_place(10));
  }

  // 检查底层字节是否同步改变
  let ref_check = RecordRef::from_slice(&buf)?;
  assert!(ref_check.is_tombstone());
  assert!(!ref_check.can_update_in_place(10));

  // 再次原位翻转取消墓碑
  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    assert!(!rec_mut.flip_tombstone());
    assert!(!rec_mut.is_tombstone());
    assert!(rec_mut.can_update_in_place(10));
  }

  let ref_check2 = RecordRef::from_slice(&buf)?;
  assert!(!ref_check2.is_tombstone());
  assert!(ref_check2.can_update_in_place(10));

  info!("RecordHeader/RecordMut 翻转墓碑与原位更新判断测试通过");
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

/// 墓碑记录普通原位更新防御测试（复活必须走显式复活路径）
#[test]
fn test_tombstone_update_rejected() -> Void {
  info!("开始测试: 墓碑记录普通原位更新拦截");

  // 1. 等长原位更新路径拦截
  let mut buf = try_encode_to_vec(0x66, b"tomb_key", b"val_10___", true)?;
  let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
  assert!(rec_mut.is_tombstone());
  assert_eq!(
    rec_mut.update_value_in_place(b"newval_10"),
    Err(Error::TombstoneUpdate)
  );
  // 被拦截的更新不得触碰底层值区
  assert_eq!(rec_mut.value(), b"val_10___");

  // 2. 动态松弛更新路径拦截（与 can_update_with_slack 查询语义一致）
  assert!(!rec_mut.can_update_with_slack(4));
  assert_eq!(
    rec_mut.update_value_with_slack(b"val"),
    Err(Error::TombstoneUpdate)
  );

  // 3. 复活路径放行并单次覆写清墓碑
  rec_mut.revivify_with_slack(b"revived")?;
  assert!(!rec_mut.is_tombstone());
  assert_eq!(rec_mut.value(), b"revived");
  assert_eq!(rec_mut.val_len(), 7);
  // 复活后普通更新恢复可用（等长 7 字节）
  rec_mut.update_value_in_place(b"back__7")?;
  assert_eq!(rec_mut.value(), b"back__7");

  info!("墓碑记录普通原位更新拦截测试通过");
  OK
}

/// 基于 FillerWords 与动态松弛的原位更新测试（LogRecord.TrySetPinnedValueSpan）
#[test]
fn test_record_mut_dynamic_slack_and_filler_words() -> Void {
  info!("开始测试: FillerWords 动态松弛全生命周期原位覆写与容量自洽");

  let key = b"session:user:1001";
  let initial_val = b"status=active;score=987654;role=admin;meta=verified_2026"; // 56 字节 (8 * 7)
  let prev_addr = 0x0000_1234_5678_0000_u64;

  let mut buf = try_encode_to_vec(prev_addr, key, initial_val, false)?;
  // 记录 8 字节对齐：16 + 17 + 56 = 89 → 对齐逻辑尺寸 96（隐式填充 7 字节）
  let initial_physical_size = buf.len();
  assert_eq!(initial_physical_size, 96);

  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    assert_eq!(rec_mut.val_len(), 56);
    assert_eq!(rec_mut.filler_words(), 0);
    // 容量含可复用隐式对齐填充：96 - 16 - 17 = 63
    assert_eq!(rec_mut.val_capacity(), 63);
    assert_eq!(rec_mut.physical_size(), initial_physical_size);

    // 1. 动态缩短：从 56 字节缩短至 24 字节（对齐(16+17+24)=64，腾出 32 字节 = 4 words filler）
    let short_val = b"status=idle;score=100000"; // 24 字节
    assert!(rec_mut.can_update_with_slack(short_val.len()));
    rec_mut.update_value_with_slack(short_val)?;

    assert_eq!(rec_mut.val_len(), 24);
    assert_eq!(rec_mut.filler_words(), 4); // 32 / 8 = 4
    assert_eq!(rec_mut.filler_bytes(), 32);
    assert_eq!(rec_mut.val_capacity(), 63); // 96 - 16 - 17 = 63 保持不变
    assert_eq!(rec_mut.physical_size(), initial_physical_size); // 物理占用大小绝对恒定
    assert_eq!(rec_mut.value(), short_val);

    // 用 as_ref() 零拷贝视图回读验证
    let rec_ref = rec_mut.as_ref();
    assert_eq!(rec_ref.value(), short_val);
    assert_eq!(rec_ref.val_len(), 24);
    assert_eq!(rec_ref.filler_words(), 4);
    assert_eq!(rec_ref.physical_size(), initial_physical_size);

    // 1.1 非词整数倍差值测试：更新为 25 字节（对齐(16+17+25)=64，
    // 隐式填充吸纳 6 字节差值，显式松弛 32 字节 = 4 words）
    let non_align_val = b"status=idle;score=100000_"; // 25 字节
    assert!(rec_mut.can_update_with_slack(non_align_val.len()));
    rec_mut.update_value_with_slack(non_align_val)?;
    assert_eq!(rec_mut.val_len(), 25);
    assert_eq!(rec_mut.filler_words(), 4); // 32 / 8 = 4
    assert_eq!(rec_mut.filler_bytes(), 32);
    assert_eq!(rec_mut.val_capacity(), 63);
    assert_eq!(rec_mut.physical_size(), initial_physical_size); // 物理占用绝对无任何漂移！
    assert_eq!(rec_mut.value(), non_align_val);

    // 2. 动态扩充：在松弛空间内扩充至 40 字节（对齐(16+17+40)=80，剩余 16 字节 = 2 words filler）
    let medium_val = b"status=active;score=20000;role=moderator"; // 40 字节 (8 * 5)
    assert!(rec_mut.can_update_with_slack(medium_val.len()));
    rec_mut.update_value_with_slack(medium_val)?;

    assert_eq!(rec_mut.val_len(), 40);
    assert_eq!(rec_mut.filler_words(), 2); // (96 - 80) / 8 = 2
    assert_eq!(rec_mut.filler_bytes(), 16);
    assert_eq!(rec_mut.val_capacity(), 63);
    assert_eq!(rec_mut.physical_size(), initial_physical_size);
    assert_eq!(rec_mut.value(), medium_val);

    // 2.1 墓碑化与单次覆写原子链内原地复活测试（revivify_with_slack）
    rec_mut.set_tombstone(true);
    assert!(rec_mut.is_tombstone());
    assert!(!rec_mut.can_update_with_slack(24)); // 墓碑状态下普通更新应被拦截
    rec_mut.revivify_with_slack(short_val)?;
    assert!(!rec_mut.is_tombstone());
    assert_eq!(rec_mut.val_len(), 24);
    assert_eq!(rec_mut.filler_bytes(), 32);
    assert_eq!(rec_mut.val_capacity(), 63);
    assert_eq!(rec_mut.physical_size(), initial_physical_size);
    assert_eq!(rec_mut.value(), short_val);

    // 3. 动态填满：恢复到 56 字节（对齐(16+17+56)=96，消耗完所有 filler 与隐式填充余量）
    rec_mut.update_value_with_slack(initial_val)?;
    assert_eq!(rec_mut.val_len(), 56);
    assert_eq!(rec_mut.filler_words(), 0);
    assert_eq!(rec_mut.val_capacity(), 63);
    assert_eq!(rec_mut.value(), initial_val);

    // 4. 超出容量（67 字节 > 63）必须被拦截
    let overflow_val = b"status=active;score=987654;role=admin;meta=verified_2026_exceed_cap"; // 67 字节
    assert!(!rec_mut.can_update_with_slack(overflow_val.len()));
    let err = rec_mut.update_value_with_slack(overflow_val);
    assert_eq!(
      err,
      Err(Error::ValueLengthMismatch {
        expected: 96,
        actual: 67,
      })
    );
  }

  // 最终从底层原始字节完全回读验证
  let final_ref = RecordRef::from_slice(&buf)?;
  assert_eq!(final_ref.value(), initial_val);
  assert_eq!(final_ref.val_len(), 56);
  assert_eq!(final_ref.filler_words(), 0);
  assert_eq!(final_ref.physical_size(), initial_physical_size);

  info!("FillerWords 动态松弛全生命周期原位覆写与容量自洽测试通过");
  OK
}
