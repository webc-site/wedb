//! 对象存 pending 完成器（对标 libs/server/Storage/Session/ObjectStore/CompletePending.cs）

use wdev::Device;

use super::super::storage_session::StorageSession;

impl<'a, D: Device> StorageSession<'a, D> {
  /// 完成对象存会话的挂起操作
  ///
  /// 缺口说明：wkv 对象存与主存同体（单库信封模型），所有调用同步闭环，
  /// 恒无遗留 pending，本方法退化为一致性空操作并返回 true。
  ///
  /// libs/server/Storage/Session/ObjectStore/CompletePending.cs:CompletePendingForObjectStoreSession
  pub fn complete_pending_for_object_store_session(&self) -> bool {
    true
  }
}
