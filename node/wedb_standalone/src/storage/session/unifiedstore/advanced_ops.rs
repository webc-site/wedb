//! 统一存高级操作（对标 libs/server/Storage/Session/UnifiedStore/AdvancedOps.cs，C# 为 StorageSession partial）

use wdev::Device;

use super::super::{mainstore::advanced_ops::StringRMWOp, storage_session::StorageSession};
use crate::api::garnet_status::GarnetStatus;

/// 统一存 RMW 操作描述（字符串操作 + TTL 族的联合视图）
#[derive(Debug, Clone, Copy)]
pub enum UnifiedRMWOp<'k> {
  /// 字符串族读改写（INCR/APPEND/SETRANGE）
  String(StringRMWOp<'k>),
  /// 相对毫秒过期（EXPIRE/PXPIRE）
  ExpireMs(u64),
  /// 绝对毫秒过期（EXPIREAT/PEXPIREAT）
  ExpireAtMs(u64),
  /// 移除过期（PERSIST）
  Persist,
}

impl<'a, D: Device> StorageSession<'a, D> {
  /// 统一存通用读（字符串/对象信封命中均返回原始载荷）
  ///
  /// libs/server/Storage/Session/UnifiedStore/AdvancedOps.cs:Read_UnifiedStore
  pub async fn read_unified_store(
    &self,
    key: &[u8],
  ) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    self.read_main_store(key).await
  }

  /// 统一存通用 RMW（按操作描述分发到字符串 / TTL 路径）
  ///
  /// libs/server/Storage/Session/UnifiedStore/AdvancedOps.cs:RMW_UnifiedStore
  pub async fn rmw_unified_store(
    &self,
    key: &[u8],
    op: UnifiedRMWOp<'_>,
  ) -> wkv::Result<GarnetStatus> {
    match op {
      UnifiedRMWOp::String(s) => {
        let (status, _) = self.rmw_main_store(key, s).await?;
        Ok(status)
      }
      UnifiedRMWOp::ExpireMs(ttl) => {
        let set = self.expire_in_ms(key, ttl).await?;
        Ok(if set == 1 {
          GarnetStatus::Ok
        } else {
          GarnetStatus::NotFound
        })
      }
      UnifiedRMWOp::ExpireAtMs(at) => {
        let set = self.expire_at_ms(key, at).await?;
        Ok(if set == 1 {
          GarnetStatus::Ok
        } else {
          GarnetStatus::NotFound
        })
      }
      UnifiedRMWOp::Persist => {
        let removed = self.persist_key(key).await?;
        Ok(if removed == 1 {
          GarnetStatus::Ok
        } else {
          GarnetStatus::NotFound
        })
      }
    }
  }
}
