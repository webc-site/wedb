//! 统一存高级操作（对标 libs/server/Storage/Session/UnifiedStore/AdvancedOps.cs，C# 为 StorageSession partial）

use wdev::Device;

use super::super::{mainstore::advanced_ops::StringRMWOp, storage_session::StorageSession};
use crate::types::GarnetStatus;

/// 统一存 RMW 操作描述（字符串操作 + TTL 族的联合视图；TTL 族一律
/// .NET Ticks 域，对标 C# UnifiedInput 携带的 ExpirationWithOption 字）
#[derive(Debug, Clone, Copy)]
pub enum UnifiedRMWOp<'k> {
  /// 字符串族读改写（INCR/APPEND/SETRANGE）
  String(StringRMWOp<'k>),
  /// 相对时长过期（EXPIRE/PEXPIRE，TimeSpan 口径 ticks）
  ExpireInTicks(i64),
  /// 绝对时刻过期（EXPIREAT/PEXPIREAT，绝对 ticks）
  ExpireAtTicks(i64),
  /// 移除过期（PERSIST）
  Persist,
}

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
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
      UnifiedRMWOp::ExpireInTicks(ttl) => {
        let set = self.expire_in_ticks(key, ttl).await?;
        Ok(if set == 1 {
          GarnetStatus::Ok
        } else {
          GarnetStatus::NotFound
        })
      }
      UnifiedRMWOp::ExpireAtTicks(at) => {
        let set = self.expire_at_ticks(key, at).await?;
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
