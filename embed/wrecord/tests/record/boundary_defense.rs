use aok::{OK, Void};
use log::info;
use wrecord::{
  ADDRESS_MASK, Error, HEADER_SIZE, RecordHeader, RecordMut, RecordRef, checked_record_size,
  try_encode_to_vec,
};

use super::support::{assert_canary_intact, make_record_with_canary};

/// 破坏性缓冲区逐字节截断探测测试
/// 对标 C# Tsavorite LogRecord.cs / RecordDataHeader.cs 帧边界验证与异常防御：
/// - 从 0 字节到 total_expected - 1 逐字节截断
/// - RecordRef::from_slice, RecordMut::from_slice_mut, split_from_slice_mut 均严格返回 BufferTooShort
#[test]
fn test_buffer_truncation_probing() -> Void {
  info!("开始测试: 破坏性缓冲区逐字节截断探测");

  let key = b"probing_key_001";
  let val = b"probing_val_001_with_some_longer_payload";
  let total_expected = HEADER_SIZE + key.len() + val.len();

  let valid_encoded = try_encode_to_vec(0x1000, key, val, false)?;
  assert_eq!(valid_encoded.len(), total_expected);

  let mut truncated_buf = [0u8; 128];

  for len in 0..total_expected {
    let truncated = &valid_encoded[..len];
    let expected_required = if len < HEADER_SIZE {
      HEADER_SIZE
    } else {
      total_expected
    };

    assert_eq!(
      RecordRef::from_slice(truncated),
      Err(Error::BufferTooShort {
        expected: expected_required,
        actual: len,
      }),
      "RecordRef 在截断长度 {len} 时未能正确返回 BufferTooShort"
    );

    truncated_buf[..len].copy_from_slice(truncated);
    let truncated_mut = &mut truncated_buf[..len];

    assert_eq!(
      RecordMut::from_slice_mut(truncated_mut),
      Err(Error::BufferTooShort {
        expected: expected_required,
        actual: len,
      }),
      "RecordMut 在截断长度 {len} 时未能正确返回 BufferTooShort"
    );

    assert_eq!(
      RecordMut::split_from_slice_mut(truncated_mut).map(|_| ()),
      Err(Error::BufferTooShort {
        expected: expected_required,
        actual: len,
      }),
      "RecordMut::split_from_slice_mut 在截断长度 {len} 时未能正确返回 BufferTooShort"
    );
  }

  info!("破坏性缓冲区逐字节截断探测测试通过");
  OK
}

/// 金丝雀内存越界写防护测试
/// 对标 C# Tsavorite RecordLifecycleTests.cs (InPlaceUpdate 内存边界与越界防护):
/// - 缓冲区后部填充金丝雀字节
/// - RecordMut 进行值原位覆写、地址修改、墓碑翻转等密集操作
/// - 验证金丝雀区域 100% 保持原样，绝不越界写穿
#[test]
fn test_canary_buffer_overrun_defense() -> Void {
  info!("开始测试: 金丝雀内存越界写防护");

  let key = b"probing_key_001";
  let val = b"probing_val_001_with_some_longer_payload";
  let (mut buffer_with_canary, total_expected) =
    make_record_with_canary(0x1000, key, val, false, 184)?;
  assert_eq!(buffer_with_canary.len(), total_expected + 184);

  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buffer_with_canary)?;
    assert_eq!(rec_mut.total_size(), total_expected);

    let new_val = b"probing_val_001_with_some_MODIFY_payload";
    assert_eq!(new_val.len(), val.len());
    rec_mut.update_value_in_place(new_val)?;
    rec_mut.set_prev_address(0x2000)?;
    rec_mut.set_tombstone(true);

    rec_mut.value_mut()[0..4].copy_from_slice(b"TEST");
  }

  // 核心断言：金丝雀区域必须 100% 保持 CANARY_BYTE
  assert_canary_intact(&buffer_with_canary, total_expected);

  info!("金丝雀内存越界写防护测试通过");
  OK
}

/// 接近 4GB 与算术溢出对抗防御测试
/// 对标 C# Tsavorite LogRecord.cs / RecordDataHeader.cs 超大变长 Key/Value 与长度溢出防御 (Length Overflow Defense):
/// - RecordHeader 接近 4GB 长度计算与 checked_record_size 算术溢出检测
/// - 遭遇虚假 4GB 长度标记切片时，安全返回 BufferTooShort 而非整数回绕或 OOM
#[test]
fn test_near_4gb_and_overflow_defense() -> Void {
  info!("开始测试: 接近 4GB 与算术溢出对抗防御");

  // 1. 接近 4GB 键值尺寸计算
  let huge_header = RecordHeader::from_raw(0, u32::MAX, u32::MAX);
  assert_eq!(huge_header.key_len(), u32::MAX);
  assert_eq!(huge_header.val_len(), u32::MAX);

  let checked_size = huge_header.checked_record_size();
  if usize::BITS == 64 {
    assert_eq!(checked_size, Some(HEADER_SIZE + (u32::MAX as usize) * 2));
  } else {
    assert_eq!(checked_size, None);
  }

  // 2. checked_record_size 算术溢出拦截
  assert_eq!(checked_record_size(usize::MAX, 1), None);
  assert_eq!(checked_record_size(1, usize::MAX), None);
  assert_eq!(checked_record_size(usize::MAX - 16, 1), None);

  // 3. 构造伪造超大长度记录头在有限切片中反序列化（栈分配 64 字节）
  let mut fake_huge_slice = [0u8; 64];
  let fake_header = RecordHeader::from_raw(0x10, 0x8000_0000, 10);
  fake_header.write_to_slice(&mut fake_huge_slice[..HEADER_SIZE])?;

  assert!(matches!(
    RecordRef::from_slice(&fake_huge_slice),
    Err(Error::BufferTooShort { .. })
  ));
  assert!(matches!(
    RecordMut::from_slice_mut(&mut fake_huge_slice),
    Err(Error::BufferTooShort { .. })
  ));

  info!("接近 4GB 与算术溢出对抗防御测试通过");
  OK
}

/// 48 位逻辑地址边界与溢出防御测试
/// 对标 C# Tsavorite LogAddress.cs 寻址空间极限 (256TB):
/// - ADDRESS_MASK (256TB 极限) 允许通过
/// - ADDRESS_MASK + 1、u64::MAX 及高位杂音均被 AddressOverflow 拦截
/// - 拦截后原始地址保持不变不被污染
#[test]
fn test_address_48bit_overflow_defense() -> Void {
  info!("开始测试: 48 位逻辑地址边界与溢出防御");

  let legal_max_addr = ADDRESS_MASK;
  let illegal_overflow_addr1 = ADDRESS_MASK + 1;
  let illegal_overflow_addr2 = u64::MAX;
  let illegal_high_bit_noise = 1u64 << 50;

  assert!(RecordHeader::new(legal_max_addr, 1, 1, false).is_ok());
  assert_eq!(
    RecordHeader::new(illegal_overflow_addr1, 1, 1, false),
    Err(Error::AddressOverflow(illegal_overflow_addr1))
  );
  assert_eq!(
    RecordHeader::new(illegal_overflow_addr2, 1, 1, false),
    Err(Error::AddressOverflow(illegal_overflow_addr2))
  );
  assert_eq!(
    RecordHeader::new(illegal_high_bit_noise, 1, 1, false),
    Err(Error::AddressOverflow(illegal_high_bit_noise))
  );

  let mut valid_header = RecordHeader::new(100, 1, 1, false)?;
  assert_eq!(
    valid_header.set_address(illegal_overflow_addr1),
    Err(Error::AddressOverflow(illegal_overflow_addr1))
  );
  assert_eq!(valid_header.address(), 100);

  info!("48 位逻辑地址边界与溢出防御测试通过");
  OK
}
