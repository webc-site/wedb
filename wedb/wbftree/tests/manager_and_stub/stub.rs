//! 自研依据: doc/zh/collection.md 存根句柄（MetaValue StorageEncoding::FlattenedTree）
use aok::{OK, Result};
use wbftree::{
  RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub, StorageBackendType, TreeTuning,
};

/// 字段偏移量（小端定长布局；自 src/stub.rs cfg(test) 常量随探针迁入测试侧）
const STORAGE_BACKEND_OFFSET: usize = 32;
const FLAGS_OFFSET: usize = 33;
const SERIALIZATION_PHASE_OFFSET: usize = 34;

/// FLUSHED 位掩码（src 私有常量 `FLUSHED_BIT_MASK` = 1 << 0 的测试侧承接）
const FLUSHED_BIT_MASK: u8 = 1 << 0;

/// 快速探针：从切片零拷贝提取在线树句柄（自 src/stub.rs cfg(test) 探针迁入，
/// 逻辑逐字节一致；生产面走 decode + 实例级方法）
const fn read_tree_handle(bytes: &[u8]) -> Option<u64> {
  match bytes {
    [t0, t1, t2, t3, t4, t5, t6, t7, ..] => {
      Some(u64::from_le_bytes([*t0, *t1, *t2, *t3, *t4, *t5, *t6, *t7]))
    }
    _ => None,
  }
}

/// 快速探针：从切片零拷贝提取存储后端类型
const fn read_storage_backend(bytes: &[u8]) -> Option<u8> {
  if bytes.len() > STORAGE_BACKEND_OFFSET {
    Some(bytes[STORAGE_BACKEND_OFFSET])
  } else {
    None
  }
}

/// 快速探针：从切片零拷贝提取序列化阶段号
const fn read_serialization_phase(bytes: &[u8]) -> Option<u8> {
  if bytes.len() > SERIALIZATION_PHASE_OFFSET {
    Some(bytes[SERIALIZATION_PHASE_OFFSET])
  } else {
    None
  }
}

/// 快速探针：从切片零拷贝提取标志位
const fn read_flags(bytes: &[u8]) -> Option<u8> {
  if bytes.len() > FLAGS_OFFSET {
    Some(bytes[FLAGS_OFFSET])
  } else {
    None
  }
}

/// 快速探针：从切片零拷贝检查是否已被刷入冷存储区
const fn read_is_flushed(bytes: &[u8]) -> Option<bool> {
  match read_flags(bytes) {
    Some(f) => Some((f & FLUSHED_BIT_MASK) != 0),
    None => None,
  }
}

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

/// 测试 from_tuning 等价构造、编码 roundtrip 与字节探针（自 src/stub.rs
/// 内联测试 `test_range_index_stub_roundtrip_and_probes` 原样迁入，探针随
/// 迁至本文件顶部）
#[test]
fn test_range_index_stub_roundtrip_and_probes() {
  let tuning = TreeTuning {
    cache_size: 1024 * 1024 * 64,
    min_record_size: 128,
    max_record_size: 1024,
    max_key_len: 256,
    leaf_page_size: 4096,
  };
  let stub_from_tuning =
    RangeIndexStub::from_tuning(0x1234_5678_90ab_cdef, &tuning, StorageBackendType::Memory);

  let mut stub = RangeIndexStub::new(
    0x1234_5678_90ab_cdef,
    1024 * 1024 * 64,
    128,
    1024,
    256,
    4096,
    StorageBackendType::Memory,
  );
  assert_eq!(stub, stub_from_tuning);
  stub.set_flushed(true);
  stub.set_recovered(true);

  let bytes = stub.encode();
  assert_eq!(bytes.len(), RANGE_INDEX_STUB_SIZE);

  let decoded = RangeIndexStub::decode(&bytes).expect("decode failed");
  assert_eq!(decoded, stub);

  // 快速探针断言
  assert_eq!(read_tree_handle(&bytes), Some(0x1234_5678_90ab_cdef));
  assert_eq!(read_is_flushed(&bytes), Some(true));
  assert_eq!(
    read_storage_backend(&bytes),
    Some(StorageBackendType::Memory.to_u8())
  );
  assert_eq!(read_serialization_phase(&bytes), Some(0));

  // 编译期常量构造与探针验证
  const C_STUB: RangeIndexStub = RangeIndexStub {
    tree_handle: 88,
    cache_size: 4096,
    min_record_size: 64,
    max_record_size: 512,
    max_key_len: 128,
    leaf_page_size: 4096,
    storage_backend: 0,
    flags: 1, // FLUSHED_BIT_MASK
    serialization_phase: 2,
  };
  const C_BYTES: [u8; RANGE_INDEX_STUB_SIZE] = C_STUB.encode();
  const C_HANDLE: Option<u64> = read_tree_handle(&C_BYTES);
  assert!(matches!(C_HANDLE, Some(88)));
  const C_FLUSHED: Option<bool> = read_is_flushed(&C_BYTES);
  assert!(matches!(C_FLUSHED, Some(true)));
  const C_BACKEND: Option<u8> = read_storage_backend(&C_BYTES);
  assert!(matches!(C_BACKEND, Some(0)));
  const C_PHASE: Option<u8> = read_serialization_phase(&C_BYTES);
  assert!(matches!(C_PHASE, Some(2)));
  const C_DECODED: Option<RangeIndexStub> = RangeIndexStub::decode_opt(&C_BYTES);
  assert_eq!(C_DECODED, Some(C_STUB));
}
