//! 尾部追加与 HLOG 写入（对标 C# Tsavorite InternalUpsert / InternalDelete 异步落盘驱逐重试循环）

use wdev::Device;

use crate::{error::Result, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 底层物理写入单个键值对（Upsert Raw）
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs:InternalUpsert
  ///
  /// C# 上下文层 Upsert 入口族在本 rust 单点的折叠映射（多态上下文已被统一会话
  /// 消除，一臂承接全部变体）：
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:Upsert
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ITsavoriteContext.cs:Upsert
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:Upsert
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalConsistentReadContext.cs:Upsert
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalUnsafeContext.cs:Upsert
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/UnsafeContext.cs:Upsert
  #[inline(always)]
  pub async fn upsert_raw(&self, key: &[u8], val: &[u8]) -> Result<u64> {
    loop {
      match self.try_upsert_raw_sync(key, val)? {
        Ok(addr) => return Ok(addr),
        Err(page_id) => {
          self.evict_pages_for(page_id).await?;
        }
      }
    }
  }

  /// 墓碑标记底层物理删除单个键（Delete Raw）
  ///
  /// C# 上下文层 Delete 入口族在本 rust 单点的折叠映射（多态上下文已被统一会话
  /// 消除，一臂承接全部变体）：
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:Delete
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ITsavoriteContext.cs:Delete
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:Delete
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalConsistentReadContext.cs:Delete
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalUnsafeContext.cs:Delete
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/UnsafeContext.cs:Delete
  #[inline(always)]
  pub async fn delete_raw(&self, key: &[u8]) -> Result<bool> {
    loop {
      match self.try_delete_raw_sync(key)? {
        Ok(deleted) => return Ok(deleted),
        Err(page_id) if page_id == u64::MAX => {
          if let Some(deleted) = self.delete_raw_disk_slow(key).await? {
            return Ok(deleted);
          }
          continue;
        }
        Err(page_id) => {
          self.evict_pages_for(page_id).await?;
        }
      }
    }
  }
}
