use aok::{OK, Result};
use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub, StorageBackendType};

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
    StorageBackendType::Disk,
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

/// 测试 RangeIndexStub 定长切片写入器 encode_into（存根位变更已收口至宿主
/// wkv/src/range_index/stub.rs 的治愈内核，wbftree 侧切片位写入器已随零调用删除）
#[test]
fn test_range_index_stub_encode_into() -> Result<()> {
  let stub = RangeIndexStub::new(
    0x1234_5678_9abc_def0,
    32 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );

  let mut buf = [0u8; RANGE_INDEX_STUB_SIZE];
  stub.encode_into(&mut buf)?;
  assert_eq!(RangeIndexStub::decode(&buf)?, stub);

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
