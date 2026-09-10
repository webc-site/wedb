use aok::{OK, Void};
use log::info;
use wrecord::{
  Error, HEADER_SIZE, RecordHeader, RecordMut, RecordRef, encode_to_slice, record_size,
  try_encode_to_vec,
};

use super::support::make_log_page;

/// 变长键值切片序列化与零拷贝指针一致性测试
/// 对标 C# Tsavorite LogRecordTests.cs / SpanByteAllocator 与 LogRecord 变长布局：
/// - 键值均支持任意变长字节切片
/// - RecordRef::from_slice 为底层切片的纯零拷贝借用
/// - 检验只读借用切片的绝对物理指针与原始缓冲区精确重合
/// - 模拟物理日志页 (Log Page) 中多条连续变长记录的顺序扫描
#[test]
fn test_variable_length_zero_copy_consistency() -> Void {
  info!("开始测试: 变长键值切片序列化与零拷贝指针一致性");

  let large_payload = vec![0xAA; 4096];
  let test_cases: Vec<(&[u8], &[u8], u64, bool)> = vec![
    (b"", b"", 0, false),
    (b"k", b"v", 0x1, false),
    (b"user:name", b"Alice Wonderland", 0x42, false),
    (
      b"sys:cluster:node:region:us-west-2:zone:b:subzone:1:service:analytics:metric:cpu_load",
      b"99.8%",
      0x1000,
      false,
    ),
    (
      b"large_payload",
      &large_payload,
      0x0000_1122_3344_5566,
      false,
    ),
    (
      &[0x00, 0x01, 0xFE, 0xFF],
      &[0xFF, 0x00, 0x7F, 0x80, 0xAA, 0x55],
      0x0000_DEAD_BEEF_CAFE,
      true,
    ),
  ];

  for (i, (k, v, addr, is_tombstone)) in test_cases.iter().enumerate() {
    let expected_record_size = record_size(k.len(), v.len());
    assert_eq!(expected_record_size, HEADER_SIZE + k.len() + v.len());

    let vec_buf = try_encode_to_vec(*addr, k, v, *is_tombstone)?;
    assert_eq!(vec_buf.len(), expected_record_size);

    let mut slice_buf = vec![0u8; expected_record_size];
    let written = encode_to_slice(&mut slice_buf, *addr, k, v, *is_tombstone)?;
    assert_eq!(written, expected_record_size);
    assert_eq!(vec_buf, slice_buf);

    let rec_ref = RecordRef::from_slice(&vec_buf)?;
    assert_eq!(rec_ref.key(), *k);
    assert_eq!(rec_ref.value(), *v);
    assert_eq!(rec_ref.prev_address(), *addr);
    assert_eq!(rec_ref.is_tombstone(), *is_tombstone);
    assert_eq!(rec_ref.total_size(), expected_record_size);

    // 零拷贝指针一致性校验：RecordRef 切片指针必须精确指向缓冲区内部偏移处
    let base_ptr = vec_buf.as_ptr();
    let expected_key_ptr = unsafe { base_ptr.add(HEADER_SIZE) };
    let expected_val_ptr = unsafe { base_ptr.add(HEADER_SIZE + k.len()) };

    assert_eq!(rec_ref.key().as_ptr(), expected_key_ptr);
    assert_eq!(rec_ref.value().as_ptr(), expected_val_ptr);

    info!(
      "用例 #{}: key_len={}, val_len={}, size={}, is_tombstone={} 验证通过",
      i + 1,
      k.len(),
      v.len(),
      expected_record_size,
      is_tombstone
    );
  }

  // 模拟连续物理日志段扫描（Sequential Log Scan）
  let mut log_page = Vec::<u8>::new();
  let sample_records: Vec<(&'static [u8], &'static [u8], u64)> = vec![
    (b"k1", b"val_one", 0),
    (b"key_two_medium", b"val_two_payload_data", 10),
    (b"k3", b"", 20),
    (b"", b"val_four_no_key", 30),
    (b"k5_final", b"complete_record_entry", 40),
  ];

  for (k, v, addr) in &sample_records {
    let bytes = try_encode_to_vec(*addr, k, v, false)?;
    log_page.extend_from_slice(&bytes);
  }

  let mut scan_offset = 0;
  let mut scanned_count = 0;

  while scan_offset < log_page.len() {
    let remaining_slice = &log_page[scan_offset..];
    let rec = RecordRef::from_slice(remaining_slice)?;

    let (expected_k, expected_v, expected_addr) = sample_records[scanned_count];
    assert_eq!(rec.key(), expected_k);
    assert_eq!(rec.value(), expected_v);
    assert_eq!(rec.prev_address(), expected_addr);

    scan_offset += rec.total_size();
    scanned_count += 1;
  }

  assert_eq!(scanned_count, sample_records.len());
  assert_eq!(scan_offset, log_page.len());

  info!("变长键值切片序列化与零拷贝指针一致性测试通过");
  OK
}

/// 极端空键值与零长度边界对抗测试
/// 对标 C# Tsavorite LogRecordTests.cs 变长键值极限边界：
/// - Key 为空 (0B)，Value 为空 (0B)：记录总大小严格为 16 字节 (仅 Header)
/// - Key 为空 (0B)，Value 非空 (128B)：正确读取空 Key 与非空 Value
/// - Key 非空 (17B)，Value 为空 (0B)：原位更新只允许 0 字节更新，非 0 字节被拦截
#[test]
fn test_empty_key_value_boundary() -> Void {
  info!("开始测试: 极端空键值与零长度边界");

  let prev_addr = 0x0000_1234_5678_u64;

  // 1. 全空键值
  let empty_encoded = try_encode_to_vec(prev_addr, b"", b"", false)?;
  assert_eq!(empty_encoded.len(), HEADER_SIZE);

  let rec_ref = RecordRef::from_slice(&empty_encoded)?;
  assert_eq!(rec_ref.key(), b"");
  assert_eq!(rec_ref.value(), b"");
  assert_eq!(rec_ref.prev_address(), prev_addr);
  assert!(!rec_ref.is_tombstone());
  assert_eq!(rec_ref.total_size(), HEADER_SIZE);

  let mut empty_buf = empty_encoded;
  let mut rec_mut = RecordMut::from_slice_mut(&mut empty_buf)?;
  assert_eq!(rec_mut.key(), b"");
  assert_eq!(rec_mut.value(), b"");
  assert_eq!(rec_mut.value_mut(), b"");

  rec_mut.update_value_in_place(b"")?;
  assert_eq!(rec_mut.value(), b"");

  let err_empty = rec_mut.update_value_in_place(b"non_empty");
  assert_eq!(
    err_empty,
    Err(Error::ValueLengthMismatch {
      expected: 0,
      actual: 9,
    })
  );

  // 2. 空 Key，非空 Value
  let val_payload = [0xEE; 128];
  let empty_key_encoded = try_encode_to_vec(prev_addr, b"", &val_payload, false)?;
  assert_eq!(empty_key_encoded.len(), HEADER_SIZE + 128);

  let ref_empty_key = RecordRef::from_slice(&empty_key_encoded)?;
  assert_eq!(ref_empty_key.key(), b"");
  assert_eq!(ref_empty_key.value(), val_payload.as_slice());

  // 3. 非空 Key，空 Value
  let key_payload = b"only_key_no_value";
  let empty_val_encoded = try_encode_to_vec(prev_addr, key_payload, b"", true)?;
  assert_eq!(empty_val_encoded.len(), HEADER_SIZE + key_payload.len());

  let ref_empty_val = RecordRef::from_slice(&empty_val_encoded)?;
  assert_eq!(ref_empty_val.key(), key_payload);
  assert_eq!(ref_empty_val.value(), b"");
  assert!(ref_empty_val.is_tombstone());

  info!("极端空键值与零长度边界测试通过");
  OK
}

/// 流式切分与连续页扫描测试
/// 对标 C# Tsavorite LogRecordTests.cs / HybridLog Page Scan 零拷贝解构：
/// - 验证连续多条变长记录在连续页切片中的无缝流式切分 (RecordRef::split_from_slice)
#[test]
fn test_streaming_split_and_page_scan() -> Void {
  info!("开始测试: 流式切分与连续页扫描");

  const RECORD_COUNT: usize = 10;
  let ground_truth: Vec<(u64, Vec<u8>, Vec<u8>, bool)> = (0..RECORD_COUNT)
    .map(|i| {
      (
        (i as u64) * 0x100,
        format!("stream_key_{i:02}").into_bytes(),
        format!("stream_val_payload_{i:03}").into_bytes(),
        i % 3 == 0,
      )
    })
    .collect();

  let page_buf = make_log_page(
    ground_truth
      .iter()
      .map(|(addr, k, v, is_tomb)| (*addr, k.as_slice(), v.as_slice(), *is_tomb)),
  )?;

  // 1. 测试 RecordRef::split_from_slice 流式切分
  let mut read_slice = page_buf.as_slice();
  let mut parsed_count = 0;

  while !read_slice.is_empty() {
    let (rec, rest) = RecordRef::split_from_slice(read_slice)?;
    let (exp_addr, ref exp_k, ref exp_v, exp_tomb) = ground_truth[parsed_count];

    assert_eq!(rec.prev_address(), exp_addr);
    assert_eq!(rec.key(), exp_k.as_slice());
    assert_eq!(rec.value(), exp_v.as_slice());
    assert_eq!(rec.is_tombstone(), exp_tomb);

    read_slice = rest;
    parsed_count += 1;
  }
  assert_eq!(parsed_count, RECORD_COUNT);
  assert!(read_slice.is_empty());

  info!("流式切分与连续页扫描测试通过");
  OK
}

/// 非对齐内存访问安全性测试
/// 对标 C# Tsavorite LogRecordTests.cs / Direct I/O 环境下非对齐内存切片访问：
/// - RecordHeader 采用小端字节序方法，不依赖裸指针直接强转
/// - 置于 1..=7 字节非 8 字节对齐偏移处，测试编解码、读取与原位修改安全无崩溃
#[test]
fn test_unaligned_memory_access_safety() -> Void {
  info!("开始测试: 非对齐内存访问安全性");

  let key = b"unaligned_test_key";
  let val = b"unaligned_test_val_payload_1234";
  let prev_addr = 0x0000_9876_5432_10fe_u64;

  for unaligned_shift in 1..=7 {
    let mut raw_buffer = [0u8; 256];
    let target_slice = &mut raw_buffer[unaligned_shift..];

    let ptr_val = target_slice.as_ptr() as usize;
    assert_ne!(
      ptr_val % 8,
      0,
      "偏移 {unaligned_shift} 处的指针必须为非 8 字节对齐"
    );

    let written = encode_to_slice(target_slice, prev_addr, key, val, false)?;
    assert_eq!(written, record_size(key.len(), val.len()));

    let hdr = RecordHeader::from_slice(&target_slice[..HEADER_SIZE])?;
    assert_eq!(hdr.address(), prev_addr);
    assert_eq!(hdr.key_len() as usize, key.len());
    assert_eq!(hdr.val_len() as usize, val.len());

    let rec_ref = RecordRef::from_slice(target_slice)?;
    assert_eq!(rec_ref.key(), key);
    assert_eq!(rec_ref.value(), val);
    assert_eq!(rec_ref.prev_address(), prev_addr);

    let mut rec_mut = RecordMut::from_slice_mut(target_slice)?;
    let new_val = b"unaligned_test_val_payload_5678";
    rec_mut.update_value_in_place(new_val)?;
    assert_eq!(rec_mut.value(), new_val);

    rec_mut.set_tombstone(true);
    assert!(rec_mut.is_tombstone());

    let reread_ref = RecordRef::from_slice(target_slice)?;
    assert_eq!(reread_ref.value(), new_val);
    assert_eq!(reread_ref.prev_address(), prev_addr);
    assert!(reread_ref.is_tombstone());
  }

  info!("非对齐内存访问安全性测试通过");
  OK
}
