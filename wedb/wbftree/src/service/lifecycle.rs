//! 生命周期释放 (1:1 对标 Garnet BfTreeService.cs:Dispose)

use std::{ptr::null_mut, sync::atomic::Ordering};

use super::BfTreeService;

impl BfTreeService {
  /// 释放实例并回收资源 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Dispose)
  ///
  /// 幂等：原子置位 disposed 标记，清空裸指针与底层 Arc 树句柄。
  /// 底层树随最后一个 `Arc` 引用归零而析构 (对标 C# LightEpoch 延迟释放语义)：
  /// 扫描/点读已借得的 `&BfTree`/`Arc<BfTree>` 可安全跑完，无需显式排空；
  /// 「排空后才删数据文件」的删除路径语义由
  /// [`crate::RangeIndexManager::dispose_tree_under_lock`] 承担。
  pub fn dispose(&self) {
    if !self.disposed.swap(true, Ordering::SeqCst) {
      self.raw_tree.store(null_mut(), Ordering::SeqCst);
      self.arc_tree.write().take();
    }
  }
}
