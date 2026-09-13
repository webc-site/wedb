//! 统一存操作（对标 libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs，C# 为 StorageSession partial）
//!
//! C# 侧 UnifiedStore 是覆盖"主存 + 对象存"的联合视图；Rust 侧 wkv 单库
//! 信封模型下天然同体，直接在字符串路径上实现统一语义。

use wbase::{convert::TICKS_PER_MILLISECOND, time::now_ticks};
use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::types::GarnetStatus;

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// EXISTS：统一视图键存在性（字符串或对象信封命中均算存在）
  ///
  /// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:EXISTS
  pub async fn exists(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    match self.read_string_with(key, |_| ()).await? {
      Some(_) => Ok(GarnetStatus::Ok),
      None => Ok(GarnetStatus::NotFound),
    }
  }

  /// DELIFEXPIM：键已到期则原子删除
  ///
  /// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:DELIFEXPIM
  ///
  /// C# 语义按 RMW `status.Found` 折算：到期命中 → ExpireAndStop（墓碑化，
  /// SessionFunctionsWrapper 置 SUCCESS/Expired → Found）→ OK；未过期命中 →
  /// NotUpdated（SUCCESS/Found）→ OK；仅键缺失返回 NOTFOUND
  pub async fn delifexpim(&self, key: &[u8]) -> wkv::Result<GarnetStatus> {
    // 判定基准与 TTL 记录同域：.NET Ticks（wkv::probe_ttl 契约）
    if matches!(self.batch.probe_ttl(key, now_ticks()), wkv::TtlProbe::Due) {
      let deleted = self.delete_string(key).await?;
      return Ok(if deleted {
        GarnetStatus::Ok
      } else {
        GarnetStatus::NotFound
      });
    }
    self.exists(key).await
  }

  /// RENAME：重命名键（覆写已存在的新键）
  ///
  /// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAME
  pub async fn rename(&self, old_key: &[u8], new_key: &[u8]) -> wkv::Result<GarnetStatus> {
    let (status, _) = self.rename_unified(old_key, new_key, false).await?;
    Ok(status)
  }

  /// RENAMENX：新键不存在时重命名，返回 1/0（同键名恒 1；旧键缺失
  /// NOTFOUND，判定序与 RENAME 共体见 `Self::rename_unified`)
  ///
  /// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAMENX
  pub async fn renamenx(&self, old_key: &[u8], new_key: &[u8]) -> wkv::Result<(GarnetStatus, i64)> {
    self.rename_unified(old_key, new_key, true).await
  }

  /// RENAME 内核：读旧值（含信封整体拷贝）→ 写新键 → 删旧键；
  /// TTL 记录随 wkv 键级 TTL 语义同步迁移（读端过期裁决先行）
  ///
  /// 判定序对齐 C# UnifiedStoreOps.RENAME：同键名短路 → NX 分支先查新键
  /// （新键已存在即 (Ok, 0)，不论旧键是否存在）→ 末查旧键（缺失 NOTFOUND，
  /// result 置 -1）。
  pub(crate) async fn rename_unified(
    &self,
    old_key: &[u8],
    new_key: &[u8],
    nx: bool,
  ) -> wkv::Result<(GarnetStatus, i64)> {
    if old_key == new_key {
      return Ok((GarnetStatus::Ok, 1));
    }
    // NX 分支先于旧键存在性（C# 先 GET newKey 再 GET oldKey）
    if nx && self.read_string_with(new_key, |_| ()).await?.is_some() {
      return Ok((GarnetStatus::Ok, 0));
    }
    // TTL 迁移：先取旧键剩余生存毫秒（-2 不存在 / -1 无 TTL）
    let pttl = self.pttl_ms(old_key).await?;
    if pttl == -2 {
      // C# 旧键缺失：status NOTFOUND，result 保持初值 -1
      return Ok((GarnetStatus::NotFound, -1));
    }
    let Some(val) = self.read_string(old_key).await? else {
      return Ok((GarnetStatus::NotFound, -1));
    };
    self.upsert_string(new_key, &val).await?;
    if pttl >= 0 {
      // 剩余毫秒 → 相对 ticks 续期（PEXPIRE 语义迁移；RESP 出参毫秒到内部
      // ticks 的唯一换算点）
      self
        .expire_in_ticks(new_key, pttl.saturating_mul(TICKS_PER_MILLISECOND))
        .await?;
    }
    let _ = self.delete_string(old_key).await?;
    Ok((GarnetStatus::Ok, 1))
  }
}
