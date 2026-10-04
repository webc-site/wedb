//! BfTree 核心引擎与并发压力测试
use std::{sync::Arc, thread};

use aok::{OK, Result};
use log::info;
use wbftree::{
  BfTreeInsertResult, BfTreeReadResult, BfTreeService, RangeIndexManager, StorageBackendType,
  TreeTuning,
};

#[path = "guard/mod.rs"]
mod guard;

use guard::TestPathGuard;

/// 构造管理器托管的环境与树实例 (对标 C# BfTreeService 构造经 RangeIndexManager.CreateBfTree 托管)
fn managed_env(
  tag: &str,
  backend: StorageBackendType,
  tuning: TreeTuning,
) -> Result<(TestPathGuard, RangeIndexManager, Arc<BfTreeService>)> {
  let dir = TestPathGuard::new(tag, true);
  let manager = RangeIndexManager::new(&dir.path, dir.path.join("cpr"))?;
  let tree = manager.create_bftree(b"tree", backend, tuning)?;
  Ok((dir, manager, tree))
}

/// 测试记录大小与键长度容量边界
#[test]
fn test_record_limits() -> Result<()> {
  // create_bftree 以 tuning.max+1 直达引擎上限，此处选值使引擎限制为
  // min=4 / max_record=33 / max_key_len=17，覆盖过小/过大/过长三类边界
  let (_dir, _manager, tree) = managed_env(
    "bftree_limits",
    StorageBackendType::Memory,
    TreeTuning {
      min_record_size: 4,
      max_record_size: 32,
      max_key_len: 16,
      ..TreeTuning::default()
    },
  )?;

  // 1. 键过长
  let long_key = vec![b'k'; 18];
  assert_eq!(
    tree.insert(&long_key, b"val"),
    BfTreeInsertResult::InvalidKV
  );
  let (res, _) = tree.read(&long_key);
  assert_eq!(res, BfTreeReadResult::InvalidKey);

  // 2. 记录过小
  assert_eq!(tree.insert(b"a", b"b"), BfTreeInsertResult::InvalidKV);

  // 3. 记录过大
  let big_val = vec![b'v'; 32];
  assert_eq!(
    tree.insert(b"large_key", &big_val),
    BfTreeInsertResult::InvalidKV
  );

  // 4. 合法范围
  assert_eq!(
    tree.insert(b"normal_key", b"normal_val"),
    BfTreeInsertResult::Success
  );
  let (res, val) = tree.read(b"normal_key");
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"normal_val".to_vec()));

  OK
}

/// 测试持久化数据重开恢复 (持久化重放)
#[test]
fn test_disk_reopen_recovery() -> Result<()> {
  let snap_dir = TestPathGuard::new("bftree_snap_dir", true);
  let snap_path = snap_dir.path.join("snap.bftree");

  {
    let (_dir, _manager, tree) = managed_env(
      "bftree_reopen",
      StorageBackendType::Disk,
      TreeTuning::default(),
    )?;
    tree.insert(b"reopen_k1", b"val1");
    tree.insert(b"reopen_k2", b"val2");
    tree.delete(b"reopen_k1");
    tree.insert(b"reopen_k3", b"val3");
    tree.cpr_snapshot(&snap_path)?;
  }

  // 从快照恢复
  {
    let tree2 =
      BfTreeService::recover_from_cpr_snapshot(&snap_path, false, StorageBackendType::Disk)?;
    let (res1, _) = tree2.read(b"reopen_k1");
    assert!(matches!(
      res1,
      BfTreeReadResult::Deleted | BfTreeReadResult::NotFound
    ));

    let (res2, v2) = tree2.read(b"reopen_k2");
    assert_eq!(res2, BfTreeReadResult::Found);
    assert_eq!(v2, Some(b"val2".to_vec()));

    let (res3, v3) = tree2.read(b"reopen_k3");
    assert_eq!(res3, BfTreeReadResult::Found);
    assert_eq!(v3, Some(b"val3".to_vec()));
  }

  OK
}

/// 测试并发多线程读写安全性
#[test]
fn test_concurrent_reads_and_writes() -> Result<()> {
  let (_dir, _manager, tree) = managed_env(
    "bftree_concurrent",
    StorageBackendType::Memory,
    TreeTuning::default(),
  )?;

  // 预装数据
  for i in 0..500 {
    let k = format!("k_{:04}", i).into_bytes();
    let v = format!("v_{}", i).into_bytes();
    tree.insert(&k, &v);
  }

  let mut handles = Vec::new();

  // 4 个并发读线程
  for _ in 0..4 {
    let t = Arc::clone(&tree);
    handles.push(thread::spawn(move || {
      for i in 0..500 {
        let k = format!("k_{:04}", i).into_bytes();
        let (res, _) = t.read(&k);
        assert!(matches!(
          res,
          BfTreeReadResult::Found | BfTreeReadResult::Deleted
        ));
      }
    }));
  }

  // 2 个并发写入/更新线程
  for w in 0..2 {
    let t = Arc::clone(&tree);
    handles.push(thread::spawn(move || {
      for i in 500..800 {
        let k = format!("k_{:04}", i).into_bytes();
        let v = format!("writer_{}_{}", w, i).into_bytes();
        t.insert(&k, &v);
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  // 验证写入后数据完整性
  for i in 0..500 {
    let k = format!("k_{:04}", i).into_bytes();
    let (res, _) = tree.read(&k);
    assert!(matches!(
      res,
      BfTreeReadResult::Found | BfTreeReadResult::Deleted
    ));
  }
  info!("并发多线程读写安全性测试通过");
  OK
}
