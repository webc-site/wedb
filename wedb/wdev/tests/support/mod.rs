//! 集成测试共享辅助工具。

use std::{process, sync::mpsc, thread, time::Duration};

/// 生成可复现的确定性字节模式序列（第 i 字节为 `(i * factor + add) & 0xFF`）
#[inline]
pub(crate) fn make_pattern_data(len: usize, factor: usize, add: usize) -> Vec<u8> {
  (0..len).map(|i| (i * factor + add) as u8).collect()
}

/// 测试看门狗：超时未放行即强制退出进程，防止跨线程 sync 场景挂死拖死整个测试二进制
pub(crate) struct Watchdog {
  tx: Option<mpsc::Sender<()>>,
  handle: Option<thread::JoinHandle<()>>,
}

impl Watchdog {
  /// 启动看门狗（`secs` 秒内未放行（Drop）即强制退出进程）
  pub(crate) fn start(secs: u64) -> Self {
    let (tx, rx) = mpsc::channel::<()>();
    let handle = thread::spawn(move || {
      if rx.recv_timeout(Duration::from_secs(secs)).is_err() {
        eprintln!("看门狗超时（{secs}s）：sync 契约测试挂死，强制退出进程");
        process::exit(101);
      }
    });
    Self {
      tx: Some(tx),
      handle: Some(handle),
    }
  }
}

impl Drop for Watchdog {
  fn drop(&mut self) {
    if let Some(tx) = self.tx.take() {
      let _ = tx.send(());
    }
    if let Some(handle) = self.handle.take() {
      let _ = handle.join();
    }
  }
}
