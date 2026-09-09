//! 统一存 OBJECT 族读处理（对标 libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs）
//!
//! C# 侧为 UnifiedInput 下 OBJECT 命令的 RMW 读处理器；Rust 侧为
//! [`StorageSession`] 方法，读端经字符串路径 + 信封标签实现。

use std::mem;

use wdev::Device;

use crate::{
  api::garnet_status::GarnetStatus,
  storage::session::{
    objectstore::common::{OBJ_TAG_HASH, OBJ_TAG_LIST, OBJ_TAG_SET, OBJ_TAG_SORTED_SET},
    storage_session::StorageSession,
  },
};

impl<'a, D: Device> StorageSession<'a, D> {
  /// OBJECT ENCODING：对象编码名
  ///
  /// libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleObjectEncoding
  pub async fn handle_object_encoding(&self, key: &[u8]) -> wkv::Result<Option<Vec<u8>>> {
    let Some(raw) = self.read_string(key).await? else {
      return Ok(None);
    };
    let name = if raw.first().is_none_or(|&t| !is_obj_tag(t)) {
      "raw"
    } else {
      "bitcode"
    };
    Ok(Some(name.as_bytes().to_vec()))
  }

  /// OBJECT REFCOUNT：引用计数（单进程模型恒 1）
  ///
  /// libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleObjectRefCount
  pub async fn handle_object_ref_count(&self, key: &[u8]) -> wkv::Result<Option<u32>> {
    Ok((self.exists(key).await? == GarnetStatus::Ok).then_some(1))
  }

  /// OBJECT IDLETIME：空闲秒数
  ///
  /// 缺口说明：C# 由 Tsavorite 记录元数据提供最近访问时钟；wkv 无逐键
  /// 访问时间戳，恒返回 0。
  ///
  /// libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleObjectIdleTime
  pub async fn handle_object_idle_time(&self, key: &[u8]) -> wkv::Result<Option<u32>> {
    Ok((self.exists(key).await? == GarnetStatus::Ok).then_some(0))
  }

  /// OBJECT FREQ：LFU 频率
  ///
  /// 缺口说明：wkv 无 LFU 元数据，恒返回 None（对应 RESP 层空响应）。
  ///
  /// libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleObjectFreq
  pub async fn handle_object_freq(&self, _key: &[u8]) -> wkv::Result<Option<u8>> {
    Ok(None)
  }

  /// MEMORY USAGE：键内存占用估计（信封字节数 + 常数开销）
  ///
  /// libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleMemoryUsage
  pub async fn handle_memory_usage(&self, key: &[u8]) -> wkv::Result<Option<usize>> {
    Ok(
      self
        .read_string(key)
        .await?
        .map(|raw| raw.len() + mem::size_of::<u64>()),
    )
  }

  /// TYPE：键类型名（string/hash/list/set/zset）
  ///
  /// libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleType
  pub async fn handle_type(&self, key: &[u8]) -> wkv::Result<Option<Vec<u8>>> {
    let Some(raw) = self.read_string(key).await? else {
      return Ok(None);
    };
    let name = match raw.first() {
      Some(&OBJ_TAG_HASH) => "hash",
      Some(&OBJ_TAG_LIST) => "list",
      Some(&OBJ_TAG_SET) => "set",
      Some(&OBJ_TAG_SORTED_SET) => "zset",
      _ => "string",
    };
    Ok(Some(name.as_bytes().to_vec()))
  }

  /// TTL：键剩余生存秒
  ///
  /// libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleTtl
  pub async fn handle_ttl(&self, key: &[u8]) -> wkv::Result<Option<i64>> {
    let pttl = self.pttl_ms(key).await?;
    Ok((pttl != -2).then(|| if pttl < 0 { -1 } else { (pttl + 999) / 1000 }))
  }

  /// EXPIRETIME：绝对过期秒
  ///
  /// libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleExpireTime
  pub async fn handle_expire_time(&self, key: &[u8]) -> wkv::Result<Option<i64>> {
    let ms = self.expiretime_ms(key).await?;
    Ok((ms != -2).then(|| if ms < 0 { -1 } else { (ms + 999) / 1000 }))
  }

  /// MIGRATE 迁移读取（返回原始信封载荷供序列化）
  ///
  /// libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleMigrate
  pub async fn handle_migrate(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, Option<Vec<u8>>)> {
    self.read_main_store(key).await
  }

  /// RENAME 处理（统一视图重命名）
  ///
  /// libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleRename
  pub async fn handle_rename(&self, old_key: &[u8], new_key: &[u8]) -> wkv::Result<GarnetStatus> {
    let (status, _) = self.rename_unified(old_key, new_key, false).await?;
    Ok(status)
  }
}

/// 信封对象标签判定（1..=4）
fn is_obj_tag(tag: u8) -> bool {
  (OBJ_TAG_SORTED_SET..=OBJ_TAG_SET).contains(&tag)
}
