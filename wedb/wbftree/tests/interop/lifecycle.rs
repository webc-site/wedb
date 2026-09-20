use aok::{OK, Result};
use wbftree::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, StorageBackendType, TreeTuning,
};

use super::common::managed_tree;

/// 测试创建磁盘支持的 BfTree 实例及安全释放（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:CreateAndDispose）
#[test]
fn test_create_and_dispose() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_create_dispose",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;
  assert_eq!(tree.storage_backend(), StorageBackendType::Disk);
  OK
}

/// 测试使用自定义调优参数创建 BfTree 实例（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:CreateWithCustomConfig）
#[test]
fn test_create_with_custom_config() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_custom_cfg",
    StorageBackendType::Disk,
    TreeTuning {
      cache_size: 16 * 1024 * 1024,
      min_record_size: 8,
      max_record_size: 4096,
      max_key_len: 128,
      leaf_page_size: 16384,
    },
  )?;
  assert_eq!(tree.storage_backend(), StorageBackendType::Disk);
  OK
}

/// 测试创建纯内存 BfTree 实例及基础读写（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:CreateMemoryOnly）
#[test]
fn test_create_memory_only() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_memory_only",
    StorageBackendType::Memory,
    TreeTuning::default(),
  )?;
  assert_eq!(tree.storage_backend(), StorageBackendType::Memory);
  let res = tree.insert(b"testkey", b"testval");
  assert_eq!(res, BfTreeInsertResult::Success);
  let (read_res, val) = tree.read(b"testkey");
  assert_eq!(read_res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"testval".to_vec()));
  OK
}

/// 测试对已释放的 BfTree 重复调用 dispose 具备幂等性（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:DoubleDispose_DoesNotThrow）
#[test]
fn test_double_dispose_does_not_throw() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_double_dispose",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;
  tree.dispose();
  tree.dispose();
  assert_eq!(
    tree.insert(b"k", b"v"),
    BfTreeInsertResult::InvalidArguments
  );
  OK
}

/// 测试对已释放的 BfTree 执行读写删除操作均被安全拒绝（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:OperationsOnDisposedTree_Throw）
#[test]
fn test_operations_on_disposed_tree_throw() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_op_disposed",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;
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
