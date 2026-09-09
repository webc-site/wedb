use std::fs;

use aok::{OK, Result};
use wbftree::{
  BfTreeConfig, BfTreeInsertResult, BfTreeReadResult, BfTreeService, ScanReturnField,
  StorageBackend, StorageBackendType,
};

use super::common::{TempTreeGuard, insert_test_data};

/// 测试 CPR 快照保存与磁盘恢复完整往返（对照 C# Garnet: BfTreeInteropTests.SnapshotAndRecover_RoundTrip）
#[test]
fn test_snapshot_and_recover_round_trip() -> Result<()> {
  let tree_path = TempTreeGuard::new("snap_origin");
  let snap_path = TempTreeGuard::new("snap_file");

  {
    let mut config = BfTreeConfig::default();
    config
      .file_path(&tree_path)
      .use_snapshot(true)
      .cb_min_record_size(4);
    let tree = BfTreeService::new(config)?;
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

/// 测试从快照恢复后执行范围扫描（对照 C# Garnet: BfTreeInteropTests.SnapshotAndRecover_ScanAfterRestore）
#[test]
fn test_snapshot_and_recover_scan_after_restore() -> Result<()> {
  let tree_path = TempTreeGuard::new("snap_scan_origin");
  let snap_path = TempTreeGuard::new("snap_scan_file");

  {
    let mut config = BfTreeConfig::default();
    config
      .file_path(&tree_path)
      .use_snapshot(true)
      .cb_min_record_size(4);
    let tree = BfTreeService::new(config)?;
    insert_test_data(&tree, 10);
    tree.cpr_snapshot(&snap_path)?;
  }

  let recovered =
    BfTreeService::recover_from_cpr_snapshot(&snap_path, false, StorageBackendType::Disk)?;

  let records = recovered.scan_with_count(b"key:", 100, ScanReturnField::Key)?;
  assert_eq!(records.len(), 10);

  OK
}

/// 测试从不存在的快照文件恢复时抛出错误（对照 C# Garnet: BfTreeInteropTests.RecoverNonExistentFile_Throws）
#[test]
fn test_recover_non_existent_file_throws() -> Result<()> {
  let path = TempTreeGuard::new("noexist");
  let res = BfTreeService::recover_from_cpr_snapshot(&path, false, StorageBackendType::Disk);
  assert!(res.is_err());
  OK
}

/// 测试纯内存实例保存 CPR 快照并恢复（对照 C# Garnet: BfTreeInteropTests.MemoryOnly_SnapshotAndRecover_RoundTrip）
#[test]
fn test_memory_only_snapshot_and_recover_round_trip() -> Result<()> {
  let snap_path = TempTreeGuard::new("mem_snap");

  {
    let mut config = BfTreeConfig::default();
    config
      .storage_backend(StorageBackend::Memory)
      .use_snapshot(true)
      .cb_min_record_size(4);
    let mem_tree = BfTreeService::new(config)?;
    assert_eq!(
      mem_tree.insert(b"testkey", b"testval"),
      BfTreeInsertResult::Success
    );
    mem_tree.cpr_snapshot(&snap_path)?;
  }

  let recovered =
    BfTreeService::recover_from_cpr_snapshot(&snap_path, false, StorageBackendType::Memory)?;
  let (res, val) = recovered.read(b"testkey");
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"testval".to_vec()));

  OK
}

/// 测试纯内存恢复模式下快照文件不存在时抛出错误（对照 C# Garnet: BfTreeInteropTests.MemoryOnly_RecoverFromNonExistentFile_Throws）
#[test]
fn test_memory_only_recover_from_non_existent_file_throws() -> Result<()> {
  let path = TempTreeGuard::new("mem_noexist");
  let res = BfTreeService::recover_from_cpr_snapshot(&path, false, StorageBackendType::Memory);
  assert!(res.is_err());
  OK
}

/// 未启用 use_snapshot 的树触发快照必须返回 Err，绝不 panic
#[test]
fn test_cpr_snapshot_without_use_snapshot_returns_err() -> Result<()> {
  let path = TempTreeGuard::new("snap_disabled");
  let snap_path = TempTreeGuard::new("snap_disabled_out");

  let mut config = BfTreeConfig::default();
  config.file_path(&path).cb_min_record_size(4);
  let tree = BfTreeService::new(config)?;
  assert_eq!(tree.insert(b"k", b"val"), BfTreeInsertResult::Success);

  let res = tree.cpr_snapshot(&snap_path);
  assert!(res.is_err());
  assert!(!snap_path.exists());

  OK
}

/// 损坏快照恢复必须返回 Err，绝不 panic
#[test]
fn test_recover_from_corrupt_snapshot_returns_err() -> Result<()> {
  let corrupt_path = TempTreeGuard::new("corrupt_snap");
  fs::write(&corrupt_path, vec![0xFFu8; 4096])?;

  let res = BfTreeService::recover_from_cpr_snapshot(&corrupt_path, false, StorageBackend::Std);
  assert!(res.is_err());

  OK
}
