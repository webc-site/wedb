//! 对象存高级操作（对标 libs/server/Storage/Session/ObjectStore/AdvancedOps.cs，C# 为 StorageSession partial）

use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::api::garnet_status::GarnetStatus;

impl<'a, D: Device> StorageSession<'a, D> {
  /// 对象存通用 RMW：按类型标签装载载荷后交回调变更并回写
  ///
  /// `tag` 取 objectstore::common 的 OBJ_TAG_* 常量；闭包返回 None 表示放弃写入。
  ///
  /// libs/server/Storage/Session/ObjectStore/AdvancedOps.cs:RMW_ObjectStore
  pub async fn rmw_object_store<R>(
    &self,
    key: &[u8],
    tag: u8,
    on_load: impl FnOnce(Option<Vec<u8>>) -> Option<(Vec<u8>, R)>,
  ) -> wkv::Result<Option<R>> {
    self.rmw_object_store_operation(key, tag, on_load).await
  }

  /// 对象存通用读：返回 (状态, 剥壳载荷)
  ///
  /// libs/server/Storage/Session/ObjectStore/AdvancedOps.cs:Read_ObjectStore
  pub async fn read_object_store(
    &self,
    key: &[u8],
    tag: u8,
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    self.read_object_store_operation(key, tag).await
  }
}
