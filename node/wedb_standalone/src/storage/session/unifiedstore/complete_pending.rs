//! 统一存 pending 完成器（对标 libs/server/Storage/Session/UnifiedStore/CompletePending.cs）

use wdev::Device;

use super::super::storage_session::StorageSession;

impl<'a, D: Device> StorageSession<'a, D> {
  /// 完成统一存会话挂起操作
  ///
  /// 缺口说明：wkv 读写调用同步闭环，恒无遗留 pending，本方法退化为
  /// 一致性空操作并返回 true。
  ///
  /// libs/server/Storage/Session/UnifiedStore/CompletePending.cs:CompletePendingForUnifiedStoreSession
  pub fn complete_pending_for_unified_store_session(&self) -> bool {
    true
  }
}
