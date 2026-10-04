//! BfTreeService 生命周期、空值拦截与 CPR 快照边界测试（对标 Garnet BfTreeInteropTests）
use std::sync::Arc;

use aok::{OK, Result};
use bf_tree::Config;
use wbftree::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService, StorageBackendType,
};

#[path = "guard/mod.rs"]
mod guard;
use guard::TestPathGuard;

fn mem_service(enable_snapshots: bool) -> BfTreeService {
  let mut config = Config::default();
  config.cache_only(true);
  if enable_snapshots {
    config.use_snapshot(true);
  }
  BfTreeService::new_with_backend(config, StorageBackendType::Memory, None, enable_snapshots)
    .unwrap()
}

/// 空值插入快速拒绝：绝不透传引擎 (底层叶子插入 debug_assert 非空值，
/// dev/test 构建下空值会 panic)，返回 InvalidKV 结构化结果码
#[test]
fn test_insert_empty_value_rejected_without_engine_panic() -> Result<()> {
  let service = mem_service(false);
  // 空 value 无论 key 长短一律 InvalidKV，且不触发引擎断言
  assert_eq!(
    service.insert(b"long_enough_key", b""),
    BfTreeInsertResult::InvalidKV
  );
  assert_eq!(service.insert(b"k", b""), BfTreeInsertResult::InvalidKV);
  // 空值插入不得产生任何残留条目
  let (res, v) = service.read(b"long_enough_key");
  assert_eq!(res, BfTreeReadResult::NotFound);
  assert_eq!(v, None);
  OK
}

/// 空键快速拒绝：绝不透传引擎，返回结构化错误码
#[test]
fn test_empty_key_rejected() -> Result<()> {
  let service = mem_service(false);
  assert_eq!(service.insert(b"", b"val"), BfTreeInsertResult::InvalidKV);
  let (res, v) = service.read(b"");
  assert_eq!(res, BfTreeReadResult::InvalidKey);
  assert_eq!(v, None);
  assert_eq!(service.delete(b""), BfTreeDeleteResult::Success);
  OK
}

/// 释放后资源完全清空，后续操作返回 InvalidArguments，重复释放幂等
/// （对标 Garnet DoubleDispose_DoesNotThrow 与 OperationsOnDisposedTree_Throw）
#[test]
fn test_dispose_lifecycle() -> Result<()> {
  let service = Arc::new(mem_service(false));
  assert_eq!(
    service.insert(b"key1", b"val1"),
    BfTreeInsertResult::Success
  );
  assert!(!service.is_disposed());

  service.dispose();
  assert!(service.is_disposed());
  assert_eq!(
    service.insert(b"key2", b"val2"),
    BfTreeInsertResult::InvalidArguments
  );
  assert_eq!(
    service.delete(b"key1"),
    BfTreeDeleteResult::InvalidArguments
  );
  let (read_res, val) = service.read(b"key1");
  assert_eq!(read_res, BfTreeReadResult::InvalidArguments);
  assert_eq!(val, None);

  // 重复释放幂等
  service.dispose();
  assert!(service.is_disposed());
  OK
}

/// 磁盘后端必须指定数据文件路径，缺失路径返回结构化 InvalidArgument 错误
/// （对标 Garnet CreateDiskBacked_MissingPath_Throws）
#[test]
fn test_disk_backed_missing_path_returns_err() -> Result<()> {
  let mut config = Config::default();
  config.cb_min_record_size(4);
  let res = BfTreeService::new_with_backend(config, StorageBackendType::Disk, None, false);
  assert!(res.is_err());
  OK
}

/// 未启用 use_snapshot 的树触发快照必须返回 Err，绝不 panic
#[test]
fn test_cpr_snapshot_without_use_snapshot_returns_err() -> Result<()> {
  let dir = TestPathGuard::new("wbftree_snap_disabled", true);
  let work = dir.join("work.bftree");
  let snap = dir.join("snap.bftree");

  let mut config = Config::default();
  config.file_path(&work).cb_min_record_size(4);
  let tree = BfTreeService::new_with_backend(
    config,
    StorageBackendType::Disk,
    Some(work.to_string_lossy().into_owned()),
    false, // use_snapshot
  )?;
  assert_eq!(tree.insert(b"k", b"val"), BfTreeInsertResult::Success);

  let res = tree.cpr_snapshot(&snap);
  assert!(res.is_err());
  assert!(!snap.exists());

  OK
}

/// 纯内存后端支持 CPR 快照与从快照恢复
/// （对标 Garnet MemoryOnly_SnapshotAndRecover_RoundTrip）
#[test]
fn test_memory_only_snapshot_and_recover() -> Result<()> {
  let snap = TestPathGuard::new("wbftree_mem_snap", false);
  {
    let tree = mem_service(true);
    assert_eq!(
      tree.insert(b"mem_key", b"mem_val"),
      BfTreeInsertResult::Success
    );
    tree.cpr_snapshot(&snap)?;
  }

  let recovered =
    BfTreeService::recover_from_cpr_snapshot(&snap, false, StorageBackendType::Memory)?;
  let (res, val) = recovered.read(b"mem_key");
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"mem_val".to_vec()));
  OK
}
