use aok::{OK, Result};
use wbftree::{
  BfTreeConfig, BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService,
  StorageBackend, StorageBackendType,
};

use super::common::TempTreeGuard;

/// 测试创建磁盘支持的 BfTree 实例及安全释放（对照 C# Garnet: BfTreeInteropTests.CreateAndDispose）
#[test]
fn test_create_and_dispose() -> Result<()> {
  let path = TempTreeGuard::new("create_dispose");
  {
    let tree = BfTreeService::open_disk(&path, 4)?;
    assert_eq!(tree.storage_backend(), StorageBackendType::Disk);
  }
  OK
}

/// 测试使用自定义配置创建 BfTree 实例（对照 C# Garnet: BfTreeInteropTests.CreateWithCustomConfig）
#[test]
fn test_create_with_custom_config() -> Result<()> {
  let path = TempTreeGuard::new("custom_cfg");
  {
    let mut config = BfTreeConfig::default();
    config
      .file_path(&path)
      .cb_size_byte(16 * 1024 * 1024)
      .cb_min_record_size(8)
      .cb_max_record_size(4096)
      .cb_max_key_len(128)
      .leaf_page_size(16384);
    let tree = BfTreeService::new(config)?;
    assert_eq!(tree.storage_backend(), StorageBackendType::Disk);
  }
  OK
}

/// 测试创建纯内存 BfTree 实例及基础读写（对照 C# Garnet: BfTreeInteropTests.CreateMemoryOnly）
#[test]
fn test_create_memory_only() -> Result<()> {
  let tree = BfTreeService::open_memory(4)?;
  assert_eq!(tree.storage_backend(), StorageBackendType::Memory);
  let res = tree.insert(b"testkey", b"testval");
  assert_eq!(res, BfTreeInsertResult::Success);
  let (read_res, val) = tree.read(b"testkey");
  assert_eq!(read_res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"testval".to_vec()));
  OK
}

/// 测试磁盘后端缺失文件路径时抛出配置错误（对照 C# Garnet: BfTreeInteropTests.CreateDiskBacked_MissingPath_Throws）
#[test]
fn test_create_disk_backed_missing_path_throws() -> Result<()> {
  let mut config = BfTreeConfig::default();
  config.storage_backend(StorageBackend::Std);
  let res = BfTreeService::new(config);
  assert!(res.is_err());
  OK
}

/// 测试对已释放的 BfTree 重复调用 dispose 具备幂等性（对照 C# Garnet: BfTreeInteropTests.DoubleDispose_DoesNotThrow）
#[test]
fn test_double_dispose_does_not_throw() -> Result<()> {
  let path = TempTreeGuard::new("double_dispose");
  let tree = BfTreeService::open_disk(&path, 4)?;
  tree.dispose();
  tree.dispose();
  OK
}

/// 测试对已释放的 BfTree 执行读写删除操作均被安全拒绝（对照 C# Garnet: BfTreeInteropTests.OperationsOnDisposedTree_Throw）
#[test]
fn test_operations_on_disposed_tree_throw() -> Result<()> {
  let path = TempTreeGuard::new("op_disposed");
  let tree = BfTreeService::open_disk(&path, 4)?;
  tree.dispose();

  assert_eq!(
    tree.insert(b"k", b"v"),
    BfTreeInsertResult::InvalidArguments
  );
  let (res, _) = tree.read(b"k");
  assert_eq!(res, BfTreeReadResult::InvalidArguments);
  assert_eq!(tree.delete(b"k"), BfTreeDeleteResult::InvalidArguments);

  OK
}
