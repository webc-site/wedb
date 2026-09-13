//! 慢路径执行器（SCAN / KEYS / DBSIZE / CLUSTER RESET 等慢命令的统一异步闭环）
//!
//! 对照 C# 机制：Garnet 网络线程同步执行慢命令（SCAN 直调
//! `storageApi.DbScan`，CLUSTER RESET 内联 `TryReset` 的 HasKeysInSlots
//! 扫描）；rust 存储域为 compio 异步（hlog 冷区扫描 / 清库 / 槽键判定
//! 均须跨 await），同步消费循环无法闭环。
//!
//! 统一模型（与 [`wcol::itembroker::item_broker_face::BlockedWait`]
//! 同形）：命令同步段返回 `Ok(false)`（须异步闭环且不残留输出）→ 会话挂起
//! [`SlowWait`] 并停止消费本批 → 网络泵 `take_slow_wait` 后 await
//! [`SlowWait::resolve`]（compio 挂起不占线程）→ 应答字节按流水线顺序
//! 写回 → 继续消费。存储执行域命令经 [`SlowWait::for_command`] 构造
//! （单次实现，一处定义，多命令复用，杜绝逐命令特设分支）；集群切面
//! 等其他注入方经 [`SlowWait::new`] 携带各自的异步闭环。

use std::{
  future::Future,
  pin::Pin,
  ptr,
  task::{Context, Poll},
};

use wresp::RespCommand;

use super::garnet_api::GarnetApi;

/// 慢路径应答 future（静态虚表单指针 + poll/drop 函数指针零成本抽象）
pub struct SlowFuture {
  ptr: *mut (),
  poll: unsafe fn(*mut (), &mut Context<'_>) -> Poll<Vec<u8>>,
  drop: unsafe fn(*mut ()),
}

impl SlowFuture {
  /// 从具体 Future 构造静态句柄（零 dyn）
  pub fn new<F: Future<Output = Vec<u8>> + 'static>(fut: F) -> Self {
    unsafe fn poll_fn<F: Future<Output = Vec<u8>>>(
      ptr: *mut (),
      cx: &mut Context<'_>,
    ) -> Poll<Vec<u8>> {
      let pin = unsafe { Pin::new_unchecked(&mut *(ptr as *mut F)) };
      pin.poll(cx)
    }

    unsafe fn drop_fn<F>(ptr: *mut ()) {
      unsafe {
        drop(Box::from_raw(ptr as *mut F));
      }
    }

    let b = Box::new(fut);
    Self {
      ptr: Box::into_raw(b) as *mut (),
      poll: poll_fn::<F>,
      drop: drop_fn::<F>,
    }
  }
}

impl Future for SlowFuture {
  type Output = Vec<u8>;

  #[inline]
  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    unsafe { (self.poll)(self.ptr, cx) }
  }
}

impl Drop for SlowFuture {
  fn drop(&mut self) {
    if !self.ptr.is_null() {
      unsafe { (self.drop)(self.ptr) };
      self.ptr = ptr::null_mut();
    }
  }
}

unsafe impl Send for SlowFuture {}

/// 慢命令挂起体：网络泵持有并 await，产出该命令的完整应答字节
pub struct SlowWait {
  /// 异步执行体（存储域命令经 [`GarnetApi::exec_slow`] 分派）
  fut: Option<SlowFuture>,
}

unsafe impl Send for SlowWait {}

impl SlowWait {
  /// 通用构造（集群切面等非存储域注入方使用）
  pub fn new(fut: impl Future<Output = Vec<u8>> + 'static) -> Self {
    Self {
      fut: Some(SlowFuture::new(fut)),
    }
  }

  /// 存储执行域构造：命令与参数快照交 [`GarnetApi`] 慢路径分派表执行
  ///
  /// 参数拷贝脱离接收缓冲生命周期（网络泵 await 期间接收缓冲可被复用）；
  /// 句柄克隆保 Arc 存活，future 借用的执行域在 await 期间有效
  pub fn for_command(api: &GarnetApi, cmd: RespCommand, args: Vec<Vec<u8>>) -> Self {
    Self {
      fut: Some(api.exec_slow(cmd, args)),
    }
  }

  /// 驱动慢路径执行至完成，返回应答字节（空集 = 无应答写出）
  pub async fn resolve(mut self) -> Vec<u8> {
    match self.fut.take() {
      Some(fut) => fut.await,
      None => Vec::new(),
    }
  }
}
