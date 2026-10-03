//! waof 子日志驱动与快路径无事件开销集成测试
//! （对应 libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs）

use std::{
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use tempfile::tempdir;
use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;
use wnode::aof::waof_sublog::{AofSublog, WaofSublog};

fn test_sublog(tag: &str) -> (tempfile::TempDir, Arc<AofSublog>) {
  let dir = tempdir().expect("tempdir");
  let device = Arc::new(
    SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).expect("SegmentedDevice"),
  );
  let wal = WalLog::new(device, WalConfig::default()).expect("WalLog");
  (dir, Arc::new(WaofSublog::new(Arc::new(wal))))
}

/// 快路径零事件面锚定：首轮裸尝试预占时刷盘推进事件上不得有任何在册监听器
#[test]
fn fast_path_enqueue_registers_no_flush_listener() {
  let (_dir, sublog) = test_sublog("waof_sublog_fastpath_listener");
  let listeners_at_attempt = Arc::new(AtomicUsize::new(usize::MAX));
  let probe = Arc::clone(&listeners_at_attempt);
  let addr = sublog
    .enqueue_with_backpressure(|| {
      probe.store(sublog.flush_event().total_listeners(), Ordering::Relaxed);
      sublog.enqueue(b"fastpath_record")
    })
    .expect("空载快路径入队不应失败");
  assert_eq!(
    listeners_at_attempt.load(Ordering::Relaxed),
    0,
    "快路径预占执行时刷盘事件出现在册监听器（注册先于尝试回潮）"
  );
  assert!(
    sublog.tail_address() > addr,
    "快路径入队未推进尾位点: {addr}"
  );
  assert_eq!(sublog.flush_event().total_listeners(), 0);
}

/// 无运行时装配的回退提交线程：commit 后刷盘推进至尾
#[test]
fn fallback_committer_flushes_without_runtime() {
  let (_dir, sublog) = test_sublog("waof_sublog_fallback");
  let addr = sublog.enqueue(b"fallback_record").unwrap();
  sublog.commit(addr, 0);
  let deadline = Instant::now() + Duration::from_secs(5);
  while sublog.flushed_until_address() <= addr {
    assert!(Instant::now() < deadline, "回退提交线程未在限时内推进刷盘");
    thread::sleep(Duration::from_millis(5));
  }
}
