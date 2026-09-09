use aok::{OK, Result};
use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub, StorageBackend};

/// 测试 35 字节定长 RangeIndexStub 的序列化、反序列化与位掩码
#[test]
fn test_range_index_stub_serialization() -> Result<()> {
  let mut stub = RangeIndexStub::new(
    0x0123_4567_89ab_cdef,
    64 * 1024 * 1024,
    4,
    2048,
    64,
    4096,
    StorageBackend::Std,
  );

  assert_eq!(stub.tree_handle, 0x0123_4567_89ab_cdef);
  assert_eq!(stub.cache_size, 64 * 1024 * 1024);
  assert_eq!(stub.min_record_size, 4);
  assert_eq!(stub.max_record_size, 2048);
  assert_eq!(stub.max_key_len, 64);
  assert_eq!(stub.leaf_page_size, 4096);
  assert_eq!(stub.storage_backend, 0);

  // 标志位操作
  assert!(!stub.is_flushed());
  assert!(!stub.is_recovered());
  assert!(!stub.is_transferred());

  stub.set_flushed(true);
  assert!(stub.is_flushed());
  assert!(!stub.is_recovered());

  stub.set_recovered(true);
  assert!(stub.is_recovered());

  stub.set_transferred(true);
  assert!(stub.is_transferred());

  // 编码与解码
  let encoded = stub.encode();
  assert_eq!(encoded.len(), RANGE_INDEX_STUB_SIZE);

  let decoded = RangeIndexStub::decode(&encoded)?;
  assert_eq!(decoded, stub);
  assert!(decoded.is_flushed());
  assert!(decoded.is_recovered());
  assert!(decoded.is_transferred());

  OK
}

/// 测试 RangeIndexStub 零拷贝切片就地修改方法
#[test]
fn test_range_index_stub_slice_helpers() -> Result<()> {
  let stub = RangeIndexStub::new(
    0x1234_5678_9abc_def0,
    32 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackend::Std,
  );

  let mut buf = [0u8; RANGE_INDEX_STUB_SIZE];
  stub.encode_into(&mut buf)?;
  assert_eq!(RangeIndexStub::decode(&buf)?, stub);

  // 就地置位与清除标志
  RangeIndexStub::slice_set_flushed(&mut buf, true)?;
  let d1 = RangeIndexStub::decode(&buf)?;
  assert!(d1.is_flushed());

  RangeIndexStub::slice_set_transferred(&mut buf, true)?;
  let d2 = RangeIndexStub::decode(&buf)?;
  assert!(d2.is_transferred());

  RangeIndexStub::slice_clear_tree_handle(&mut buf)?;
  let d3 = RangeIndexStub::decode(&buf)?;
  assert_eq!(d3.tree_handle, 0);

  RangeIndexStub::slice_mark_recovered_from_checkpoint(&mut buf)?;
  let d4 = RangeIndexStub::decode(&buf)?;
  assert_eq!(d4.tree_handle, 0);
  assert!(d4.is_recovered());

  RangeIndexStub::slice_recreate_index(&mut buf, 0xcafe_babe_dead_beef)?;
  let d5 = RangeIndexStub::decode(&buf)?;
  assert_eq!(d5.tree_handle, 0xcafe_babe_dead_beef);
  assert!(!d5.is_recovered());

  OK
}

/// 测试动态叶子页面大小计算算法
#[test]
fn test_range_index_compute_leaf_page_size() -> Result<()> {
  assert_eq!(RangeIndexManager::compute_leaf_page_size(1024), 4096);
  assert_eq!(RangeIndexManager::compute_leaf_page_size(2048), 4096);
  assert_eq!(RangeIndexManager::compute_leaf_page_size(3000), 8192);
  assert_eq!(RangeIndexManager::compute_leaf_page_size(8000), 32768);
  assert_eq!(RangeIndexManager::compute_leaf_page_size(100_000), 32768);
  OK
}
