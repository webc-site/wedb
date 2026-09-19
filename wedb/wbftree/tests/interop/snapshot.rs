use std::fs;

use aok::{OK, Result};
use wbftree::{
  BfTreeInsertResult, BfTreeReadResult, BfTreeService, ScanReturnField, StorageBackendType,
  TreeTuning,
};

use super::common::{TestPathGuard, insert_test_data, managed_tree};

/// 测试 CPR 快照保存与磁盘恢复完整往返（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:SnapshotAndRecover_RoundTrip）
#[test]
fn test_snapshot_and_recover_round_trip() -> Result<()> {
  let snap_path = TestPathGuard::new("bftree_test_snap_file", false);

  {
    let (_dir, _manager, tree) = managed_tree(
      "bftree_test_snap_origin",
      StorageBackendType::Disk,
      TreeTuning::default(),
    )?;
    insert_test_data(&tree, 20);
    tree.cpr_snapshot(&snap_path)?;
  }

  let recovered =
    BfTreeService::recover_from_cpr_snapshot(&snap_path, false, StorageBackendType::Disk)?;

  for i in 0..20 {
    let k = format!("key:{:04}", i).into_bytes();
    let expected = format!("val:{}", i).into_bytes();
    let (res, v) = recovered.read(&k);
    assert_eq!(res, BfTreeReadResult::Found);
    assert_eq!(v, Some(expected));
  }

  OK
}

/// 测试从快照恢复后执行范围扫描（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:SnapshotAndRecover_ScanAfterRestore）
#[test]
fn test_snapshot_and_recover_scan_after_restore() -> Result<()> {
  let snap_path = TestPathGuard::new("bftree_test_snap_scan_file", false);

  {
    let (_dir, _manager, tree) = managed_tree(
      "bftree_test_snap_scan_origin",
      StorageBackendType::Disk,
      TreeTuning::default(),
    )?;
    insert_test_data(&tree, 10);
    tree.cpr_snapshot(&snap_path)?;
  }

  let recovered =
    BfTreeService::recover_from_cpr_snapshot(&snap_path, false, StorageBackendType::Disk)?;

  let records = recovered.scan_with_count(b"key:", 100, ScanReturnField::Key)?;
  assert_eq!(records.len(), 10);

  OK
}

/// 测试从不存在的快照文件恢复时抛出错误（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:RecoverNonExistentFile_Throws）
#[test]
fn test_recover_non_existent_file_throws() -> Result<()> {
  let path = TestPathGuard::new("bftree_test_noexist", false);
  let res = BfTreeService::recover_from_cpr_snapshot(&path, false, StorageBackendType::Disk);
  assert!(res.is_err());
  OK
}

/// 测试纯内存实例保存 CPR 快照并恢复（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:MemoryOnly_SnapshotAndRecover_RoundTrip）
#[test]
fn test_memory_only_snapshot_and_recover_round_trip() -> Result<()> {
  let snap_path = TestPathGuard::new("bftree_test_mem_snap", false);

  {
    let (_dir, _manager, tree) = managed_tree(
      "bftree_test_mem_snap_origin",
      StorageBackendType::Memory,
      TreeTuning::default(),
    )?;
    assert_eq!(
      tree.insert(b"testkey", b"testval"),
      BfTreeInsertResult::Success
    );
    tree.cpr_snapshot(&snap_path)?;
  }

  let recovered =
    BfTreeService::recover_from_cpr_snapshot(&snap_path, false, StorageBackendType::Memory)?;
  let (res, val) = recovered.read(b"testkey");
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"testval".to_vec()));

  OK
}

/// 测试纯内存恢复模式下快照文件不存在时抛出错误（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:MemoryOnly_RecoverFromNonExistentFile_Throws）
#[test]
fn test_memory_only_recover_from_non_existent_file_throws() -> Result<()> {
  let path = TestPathGuard::new("bftree_test_mem_noexist", false);
  let res = BfTreeService::recover_from_cpr_snapshot(&path, false, StorageBackendType::Memory);
  assert!(res.is_err());
  OK
}

/// 损坏快照恢复必须返回结构化 Err，绝不 panic
/// （0xFF 填充 / 无魔数 / 魔数截断 / 错误魔数四种损坏形态全覆盖）
#[test]
fn test_recover_from_corrupt_snapshot_returns_err() -> Result<()> {
  use wbftree::Error;

  // 1. 0xFF 填充（非快照布局字节流）
  let fill_path = TestPathGuard::new("bftree_test_corrupt_snap", false);
  fs::write(&fill_path, vec![0xFFu8; 4096])?;
  let res = BfTreeService::recover_from_cpr_snapshot(&fill_path, false, StorageBackendType::Disk);
  assert!(matches!(res, Err(Error::Recovery(_))));

  // 2. 无魔数文件
  let bad_magic = TestPathGuard::new("bftree_test_corrupt_bad_magic", false);
  fs::write(&bad_magic, b"garbage payload")?;
  let res = BfTreeService::recover_from_cpr_snapshot(&bad_magic, false, StorageBackendType::Disk);
  assert!(matches!(res, Err(Error::Recovery(_))));

  // 3. 魔数被截断 (不足 16 字节)
  let truncated = TestPathGuard::new("bftree_test_corrupt_truncated", false);
  fs::write(&truncated, b"BF-TREE")?;
  let res = BfTreeService::recover_from_cpr_snapshot(&truncated, false, StorageBackendType::Disk);
  assert!(matches!(res, Err(Error::Recovery(_))));

  // 4. 错误魔数 (长度合法但内容不符)
  let wrong_magic = TestPathGuard::new("bftree_test_corrupt_wrong_magic", false);
  fs::write(&wrong_magic, b"XX-TREE-V0-BEGIN_PAYLOAD")?;
  let res = BfTreeService::recover_from_cpr_snapshot(&wrong_magic, false, StorageBackendType::Disk);
  assert!(matches!(res, Err(Error::Recovery(_))));

  OK
}
