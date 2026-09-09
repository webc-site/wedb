//! 统一存操作（对标 libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs，C# 为 StorageSession partial）
//!
//! C# 侧 UnifiedStore 是覆盖"主存 + 对象存"的联合视图；Rust 侧 wkv 单库
//! 信封模型下天然同体，直接在字符串路径上实现统一语义。

use wdev::Device;

use super::super::{common::array_key_iteration_functions, storage_session::StorageSession};
use crate::api::garnet_status::GarnetStatus;

impl<'a, D: Device> StorageSession<'a, D> {
  /// EXISTS：统一视图键存在性（字符串或对象信封命中均算存在）
  ///
  /// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:EXISTS
  pub async fn exists(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    match self.read_string(key).await? {
      Some(_) => Ok(GarnetStatus::Ok),
      None => Ok(GarnetStatus::NotFound),
    }
  }

  /// DELIFEXPIM：键在内存中已到期则删除
  ///
  /// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:DELIFEXPIM
  pub async fn delifexpim(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    let now_ms = coarsetime::Clock::now_since_epoch().as_millis();
    if matches!(self.batch.probe_ttl(key, now_ms), wkv::TtlProbe::Due) {
      let _ = self.delete_string(key).await?;
      return Ok(GarnetStatus::Ok);
    }
    // 未到期键按 C# RMW 语义视为未命中
    Ok(GarnetStatus::NotFound)
  }

  /// RENAMENX：新键不存在时重命名，返回 1/0（同键名恒 1）
  ///
  /// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAMENX
  pub async fn renamenx(&self, old_key: &[u8], new_key: &[u8]) -> wkv::Result<(GarnetStatus, i64)> {
    self.rename_unified(old_key, new_key, true).await
  }

  /// RENAME 内核：读旧值（含信封整体拷贝）→ 写新键 → 删旧键；
  /// TTL 记录随 wkv 键级 TTL 语义同步迁移（读端过期裁决先行）
  pub(crate) async fn rename_unified(
    &self,
    old_key: &[u8],
    new_key: &[u8],
    nx: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    if old_key == new_key {
      return Ok((GarnetStatus::Ok, 1));
    }
    // TTL 迁移：先取旧键剩余生存毫秒（-2 不存在 / -1 无 TTL）
    let pttl = self.pttl_ms(old_key).await?;
    if pttl == -2 {
      return Ok((GarnetStatus::NotFound, 0));
    }
    let Some(val) = self.read_string(old_key).await? else {
      return Ok((GarnetStatus::NotFound, 0));
    };
    if nx && self.read_string(new_key).await?.is_some() {
      return Ok((GarnetStatus::Ok, 0));
    }
    self.upsert_string(new_key, &val).await?;
    if pttl >= 0 {
      let now_ms = coarsetime::Clock::now_since_epoch().as_millis();
      self
        .expire_at_ms(new_key, now_ms.saturating_add(pttl as u64))
        .await?;
    }
    let _ = self.delete_string(old_key).await?;
    let _ = array_key_iteration_functions::TAG_STRING;
    Ok((GarnetStatus::Ok, 1))
  }
}
