//! 统一存 RMW 过期族处理（对标 libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs）

use wdev::Device;

use crate::{api::garnet_status::GarnetStatus, storage::session::storage_session::StorageSession};

impl<'a, D: Device> StorageSession<'a, D> {
  /// EXPIRE 拷贝更新路径（落盘过期时间戳）
  ///
  /// libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:HandleExpireCopyUpdate
  pub async fn handle_expire_copy_update(
    &self,
    key: &[u8],
    at_ms: u64,
  ) -> wkv::Result<GarnetStatus> {
    let set = self.expire_at_ms(key, at_ms).await?;
    Ok(if set == 1 {
      GarnetStatus::Ok
    } else {
      GarnetStatus::NotFound
    })
  }

  /// EXPIRE 原位更新路径（wkv 键级 TTL 统一走记录写，等价于拷贝路径）
  ///
  /// libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:HandleExpireInPlaceUpdate
  pub async fn handle_expire_in_place_update(
    &self,
    key: &[u8],
    at_ms: u64,
  ) -> wkv::Result<GarnetStatus> {
    self.handle_expire_copy_update(key, at_ms).await
  }

  /// PERSIST 拷贝更新路径
  ///
  /// libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:HandlePersistCopyUpdate
  pub async fn handle_persist_copy_update(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    let removed = self.persist_key(key).await?;
    Ok(if removed == 1 {
      GarnetStatus::Ok
    } else {
      GarnetStatus::NotFound
    })
  }

  /// PERSIST 原位更新路径
  ///
  /// libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:HandlePersistInPlaceUpdate
  pub async fn handle_persist_in_place_update(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    self.handle_persist_copy_update(key).await
  }
}
