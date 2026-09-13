//! 生命周期释放 (1:1 对标 Garnet BfTreeService.cs:Dispose)

use std::{ptr::null_mut, sync::atomic::Ordering};

use super::{BfTreeService, WriteBarrierGuard};
use crate::error::Result;

impl BfTreeService {
  /// 开启写入屏障并返回 RAII 守卫（兼容保留）
  pub fn write_barrier(&self) -> WriteBarrierGuard<'_> {
    self.barriers.fetch_add(1, Ordering::SeqCst);
    WriteBarrierGuard { service: self }
  }

  /// 释放实例并回收资源 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Dispose)
  ///
  /// 幂等：原子置位 disposed 标记，清空裸指针与底层 Arc 树句柄。
  pub fn dispose(&self) {
    if !self.disposed.swap(true, Ordering::SeqCst) {
      self.raw_tree.store(null_mut(), Ordering::SeqCst);
      self.arc_tree.write().take();
      self.retired_trees.write().clear();
    }
  }

  /// 释放实例（兼容接口，直接调用 [`dispose`](Self::dispose)）
  #[inline]
  pub fn dispose_quiesced(&self) -> Result<()> {
    self.dispose();
    Ok(())
  }
}
