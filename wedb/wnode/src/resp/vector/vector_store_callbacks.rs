//! 磁盘存储回调实现（对标 C# VectorManager.Callbacks.cs）
//!
//! 将 DiskANN 向量图的向量数据、邻接表（图拓扑）、量化状态、属性和 ID 映射
//! 通过统一的 `[命名空间字节][键字节]` 物理键落盘到 wkv 存储引擎（Tsavorite 混合日志）。

use std::{
  future::Future,
  sync::Arc,
  task::{Context as TaskContext, Poll, Wake, Waker},
  thread::{self, Thread},
};

use wdev::Device;
use wkv::StoreSession;
use wvector::store::StoreCallbacks;

/// 轻量 Future 阻塞驱动器
///
/// compio runtime 上下文内（DiskANN 同步回调由 VADD 任务在 runtime 线程上
/// 调入）：走 [`Runtime::block_on`] 内联驱动本 runtime 任务队列与 I/O driver。
/// 禁止 park 等待——慢路径（脏页落盘）需要 driver 收割完成事件，park 住
/// 单线程执行域后永远无人唤醒。
/// 纯线程上下文（无 runtime）：退回 park 式驱动（waker 由外部线程唤醒）
fn block_on<F: Future>(f: F) -> F::Output {
  if let Some(rt) = compio::runtime::Runtime::try_current() {
    return rt.block_on(f);
  }

  struct ThreadWaker(Thread);
  impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
      self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
      self.0.unpark();
    }
  }

  let mut f = Box::pin(f);
  let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
  let mut cx = TaskContext::from_waker(&waker);
  loop {
    match f.as_mut().poll(&mut cx) {
      Poll::Ready(val) => return val,
      Poll::Pending => thread::park(),
    }
  }
}

/// 基于 wkv 存储会话的真实向量磁盘存储回调
pub struct WedbVectorStoreCallbacks<D: Device + 'static> {
  session: Arc<StoreSession<D>>,
}

impl<D: Device + 'static> WedbVectorStoreCallbacks<D> {
  /// 创建存储回调绑定
  pub fn new(session: Arc<StoreSession<D>>) -> Self {
    Self { session }
  }

  /// 获取底层存储会话句柄
  #[inline]
  pub fn session(&self) -> &Arc<StoreSession<D>> {
    &self.session
  }
}

impl<D: Device + 'static> StoreCallbacks for WedbVectorStoreCallbacks<D> {
  fn read_multi<F>(&self, context: u64, keys: &[u8], _length_hint: usize, mut f: F)
  where
    F: FnMut(u32, &[u8]),
  {
    let mut index = 0u32;
    let mut rest = keys;
    while rest.len() >= 4 {
      let len = u32::from_le_bytes(rest[..4].try_into().unwrap_or([0; 4])) as usize;
      let total = 4 + len;
      if rest.len() < total {
        break;
      }
      let key = &rest[4..total];
      let phys_key = self.session.vector_key(context, key);

      // 优先走纯内存同步快速直读
      let read_done = match self
        .session
        .try_read_raw_in_memory(&phys_key, |val| f(index, val))
      {
        Ok(Some(Some(_))) => true,
        Ok(Some(None)) => true, // 明确不存在/墓碑
        _ => false,
      };

      if !read_done {
        // 冷数据落盘回退
        let _ = block_on(self.session.read_raw_with(&phys_key, |val| {
          f(index, val);
        }));
      }

      index += 1;
      rest = &rest[total..];
    }
  }

  fn read<F>(&self, context: u64, key: &[u8], mut f: F) -> bool
  where
    F: FnMut(&[u8]),
  {
    let phys_key = self.session.vector_key(context, key);
    match self.session.try_read_raw_in_memory(&phys_key, |val| f(val)) {
      Ok(Some(Some(_))) => true,
      Ok(Some(None)) => false,
      _ => {
        let mut called = false;
        let res = block_on(self.session.read_raw_with(&phys_key, |val| {
          called = true;
          f(val);
        }));
        res.is_ok_and(|opt| opt.is_some()) && called
      }
    }
  }

  fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    let phys_key = self.session.vector_key(context, key);
    match self.session.try_upsert_raw_sync(&phys_key, value) {
      Ok(Ok(_)) => true,
      _ => block_on(self.session.upsert_raw(&phys_key, value)).is_ok(),
    }
  }

  fn delete(&self, context: u64, key: &[u8]) -> bool {
    let phys_key = self.session.vector_key(context, key);
    block_on(self.session.delete_raw(&phys_key)).unwrap_or(false)
  }

  fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, mut f: F) -> bool
  where
    F: FnMut(&mut [u8]),
  {
    let mut buf = vec![0u8; write_len];
    let _ = self.read(context, key, |curr| {
      let n = buf.len().min(curr.len());
      buf[..n].copy_from_slice(&curr[..n]);
    });
    f(&mut buf);
    self.write(context, key, &buf)
  }

  fn filter(&self, _context: u64, _internal_id: u32) -> bool {
    true
  }

  fn log(&self, _context: u64, msg: &str) {
    log::info!("{msg}");
  }
}
