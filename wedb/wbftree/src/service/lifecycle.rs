//! 生命周期释放 (1:1 对标 Garnet BfTreeService.cs:Dispose)

use super::BfTreeService;

impl BfTreeService {
  /// 释放实例并回收资源 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Dispose)
  ///
  /// 幂等：单句柄 `swap(None)` 原子摘除，返回 `Some` 即首次释放
  /// (对标 C# `Interlocked.Exchange(ref _disposed, 1)` + `_tree = 0`；
  /// 摘除后 `native_ptr` 读 0，`is_disposed` 唯一守卫判真)。
  ///
  /// 并发正确性：swap 为 Release 发布，已借出的点读 Guard / 扫描 `Arc`
  /// 保活底层引擎至用毕才随最后一个引用归零析构 (对标 C# LightEpoch
  /// 延迟释放语义)，无需显式排空；「排空后才删数据文件」的删除路径语义
  /// 由 [`crate::RangeIndexManager::dispose_tree_under_lock`] 承担。
  pub fn dispose(&self) {
    drop(self.tree.swap(None));
  }
}
