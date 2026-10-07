//! 尾部追加与 HLOG 写入（对标 C# Tsavorite InternalUpsert / InternalDelete 异步落盘驱逐重试循环）

use wdev::Device;
use whlog::Error as WhlogError;

use super::super::DEGRADE_ASYNC;
use crate::{
  error::{Error, Result},
  session::StoreSession,
};

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
      match self.try_upsert_raw_sync(key, val) {
        Ok(Ok(addr)) => return Ok(addr),
        Ok(Err(page_id)) | Err(Error::HLog(WhlogError::PageNotReady(page_id))) => {
          self.evict_pages_for(page_id).await?;
        }
        Err(e) => return Err(e),
      }
    }
  }

  /// 强制在 min_tail 或其后写入单个键值对（跳过原位更新与低于截断线的复活，FLUSHALL 重挂专用）
  #[inline(always)]
  pub async fn upsert_raw_tail(&self, key: &[u8], val: &[u8], min_tail: u64) -> Result<u64> {
    loop {
      match self.try_upsert_raw_tail_sync(key, val, min_tail) {
        Ok(Ok(addr)) => return Ok(addr),
        Ok(Err(page_id)) | Err(Error::HLog(WhlogError::PageNotReady(page_id))) => {
          self.evict_pages_for(page_id).await?;
        }
        Err(e) => return Err(e),
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
      match self.try_delete_raw_sync(key) {
        Ok(Ok(deleted)) => return Ok(deleted),
        Ok(Err(page_id)) if page_id == DEGRADE_ASYNC => {
          if let Some(deleted) = self.delete_raw_disk_slow(key).await? {
            return Ok(deleted);
          }
          continue;
        }
        Ok(Err(page_id)) | Err(Error::HLog(WhlogError::PageNotReady(page_id))) => {
          self.evict_pages_for(page_id).await?;
        }
        Err(e) => return Err(e),
      }
    }
  }

  /// 缺席键盲墓碑追加（缺席删除镜像；物理删除/取删原语对缺席键零追加，本口
  /// 无条件落一条 0 字节墓碑并恰发一次 AOF 镜像）
  ///
  /// 在 garnet 中的相对路径: （rust 存储模型差异投影，C# 无对应原语——C#
  /// 向量索引记录驻主存，DELETE 落主日志随复制链传播；rust 登记态键在 wkv
  /// 双域恒缺席，由宿主缺席观测钩子命中后经本口补发墓碑镜像入 AOF 复制链）
  #[inline(always)]
  pub async fn append_absent_tombstone_raw(&self, key: &[u8]) -> Result<()> {
    loop {
      match self.try_append_absent_tombstone_sync(key) {
        Ok(Ok(())) => return Ok(()),
        Ok(Err(page_id)) | Err(Error::HLog(WhlogError::PageNotReady(page_id))) => {
          self.evict_pages_for(page_id).await?;
        }
        Err(e) => return Err(e),
      }
    }
  }

  /// 取删单个键并回传被摘记录值（Take Raw；[`Self::delete_raw`] 的取值对位，
  /// GETDEL 读删一体慢路径闭环）
  ///
  /// 应答值 = 实际摘除记录的值（捕获与摘除同一临界区，见 `try_take_raw_sync`）；
  /// 键缺席回 `None`，可变区原位不可行时经驱逐重试、冷数据转磁盘取删慢路径
  #[inline(always)]
  pub async fn take_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    loop {
      match self.try_take_raw_sync(key) {
        Ok(Ok(taken)) => return Ok(taken),
        Ok(Err(page_id)) if page_id == DEGRADE_ASYNC => {
          if let Some(taken) = self.take_raw_disk_slow(key).await? {
            return Ok(taken);
          }
          continue;
        }
        Ok(Err(page_id)) | Err(Error::HLog(WhlogError::PageNotReady(page_id))) => {
          self.evict_pages_for(page_id).await?;
        }
        Err(e) => return Err(e),
      }
    }
  }
}
