//! BfTree 核心引擎与并发压力测试
use std::{
  env, fs,
  ops::Deref,
  path::{Path, PathBuf},
  process,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::Duration,
};

use aok::{OK, Result};
use log::info;
use wbftree::{BfTreeConfig, BfTreeInsertResult, BfTreeReadResult, BfTreeService, StorageBackend};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// 测试记录大小与键长度容量边界
#[test]
fn test_record_limits() -> Result<()> {
  let mut cfg = BfTreeConfig::default();
  cfg
    .storage_backend(StorageBackend::Memory)
    .cb_max_key_len(16)
    .cb_min_record_size(4)
    .cb_max_record_size(32);
  let tree = BfTreeService::new(cfg)?;

  // 1. 键过长
  let long_key = vec![b'k'; 17];
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

/// 测试临时文件 RAII 自动清理守卫
struct TempPathGuard(PathBuf);

impl TempPathGuard {
  fn new(prefix: &str) -> Self {
    Self(env::temp_dir().join(format!("bftree_{prefix}_{}.bftree", fastrand::u64(..))))
  }
}

impl Deref for TempPathGuard {
  type Target = Path;
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl AsRef<Path> for TempPathGuard {
  fn as_ref(&self) -> &Path {
    &self.0
  }
}

impl Drop for TempPathGuard {
  fn drop(&mut self) {
    if self.0.exists() {
      let _ = fs::remove_file(&self.0);
    }
  }
}

/// 测试持久化数据重开恢复 (持久化重放)
#[test]
fn test_disk_reopen_recovery() -> Result<()> {
  let path = TempPathGuard::new("reopen");
  let snap_path = TempPathGuard::new("snap");

  {
    let tree = BfTreeService::open_disk(&path, 0)?;
    tree.insert(b"reopen_k1", b"val1");
    tree.insert(b"reopen_k2", b"val2");
    tree.delete(b"reopen_k1");
    tree.insert(b"reopen_k3", b"val3");
    tree.cpr_snapshot(&snap_path)?;
  }

  // 从快照恢复
  {
    let tree2 = BfTreeService::recover_from_cpr_snapshot(&snap_path, false, StorageBackend::Std)?;
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
  let tree = Arc::new(BfTreeService::open_memory(0)?);

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

/// 测试: 计数屏障嵌套语义 (对标 Garnet SetCheckpointBarrier)
/// 1. 持有外层屏障时 insert 阻塞，内层屏障叠加/释放均不提前放行；
/// 2. 最外层守卫丢弃后写入放行且数据完整；
/// 3. 无屏障时 cpr_snapshot 快照与并发写无撕裂 (恢复后点态一致)。
#[test]
fn test_write_barrier_nesting() -> Result<()> {
  let dir = env::temp_dir().join(format!(
    "wbftree_barrier_{}_{}",
    process::id(),
    fastrand::u64(..)
  ));
  fs::create_dir_all(&dir)?;
  let work_path = dir.join("shared.data.bftree");
  let service = Arc::new(BfTreeService::open_disk(&work_path, 4)?);

  let done = Arc::new(AtomicBool::new(false));
  let outer = service.write_barrier();

  // 持外层屏障：后台写任务必须阻塞
  let svc = Arc::clone(&service);
  let done_flag = Arc::clone(&done);
  let writer = thread::spawn(move || {
    svc.insert(b"bk", b"v1");
    done_flag.store(true, Ordering::Release);
  });
  thread::sleep(Duration::from_millis(120));
  assert!(
    !done.load(Ordering::Acquire),
    "外层屏障持有期间写入必须被阻塞"
  );

  // 内层屏障叠加后释放：写者仍须阻塞至最外层释放 (计数语义)
  let inner = service.write_barrier();
  drop(inner);
  thread::sleep(Duration::from_millis(120));
  assert!(
    !done.load(Ordering::Acquire),
    "内层屏障释放不得溶解外层屏障"
  );

  drop(outer);
  writer.join().unwrap();
  assert!(done.load(Ordering::Acquire));
  let (res, v) = service.read(b"bk");
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(v.as_deref(), Some(&b"v1"[..]));

  // 无外部屏障：快照经引擎 CPR 阶段协议与并发写安全共存 (对标 C# 非阻塞语义)，
  // 快照文件点态自洽
  let snap = dir.join("snap.bftree");
  let wsvc = Arc::clone(&service);
  let writer2 = thread::spawn(move || {
    for i in 0..2000u32 {
      let k = format!("k{i:05}");
      wsvc.insert(k.as_bytes(), b"payload");
    }
  });
  service.cpr_snapshot(&snap)?;
  writer2.join().unwrap();

  // 中途快照必须可恢复且内部自洽 (时点子集，不假设具体键)
  let recovered =
    BfTreeService::recover_from_cpr_snapshot(&snap, true, wbftree::StorageBackendType::Disk)?;
  let (res, v) = recovered.read(b"k00000");
  assert!(
    res == BfTreeReadResult::Found || res == BfTreeReadResult::NotFound,
    "快照树点态自洽"
  );
  if res == BfTreeReadResult::Found {
    assert_eq!(v.as_deref(), Some(&b"payload"[..]));
  }
  drop(recovered);

  // 写入全部完成后的终态快照：恢复后必须全量命中
  let snap2 = dir.join("snap2.bftree");
  service.cpr_snapshot(&snap2)?;
  let final_tree =
    BfTreeService::recover_from_cpr_snapshot(&snap2, true, wbftree::StorageBackendType::Disk)?;
  for k in [b"k00000".as_slice(), b"k00999", b"k01999"] {
    let (res, v) = final_tree.read(k);
    assert_eq!(res, BfTreeReadResult::Found, "终态快照必须包含 {k:?}");
    assert_eq!(v.as_deref(), Some(&b"payload"[..]));
  }

  fs::remove_dir_all(&dir)?;
  info!("计数屏障嵌套语义测试通过");
  OK
}
