//! key 级 ETag 旁路记录的会话读写域
//!
//! 对标 C# Tsavorite LogRecord 可选 ETag 字段的生命周期语义
//! （libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs）：C# 将 8B
//! etag 存于记录尾可选字段、随记录物理布局一体维护；Rust 记录头为 16B 定长
//! 双原子字（无 C# 的 HasETag/HasExpiration 可选位），故仿 key 级 TTL 旁路
//! 记录先例（[crate::ttl]）以 `KeyTag::Etag` 独立标签记录实现等价语义：
//!
//! - 键删除（DEL/GETDEL/过期 purge/FLUSHDB）级联删除 etag 记录——挂点在
//!   [crate::session::StoreSession::delete] 与同步删除内核
//!   `try_delete_sync_unprotected`，与 TTL 级联同点；RMW 过期重建挂点
//!   （[`crate::session::RmwWindow::try_rmw_sync`] Due 臂，对位 C#
//!   MainStore/RMWMethods.cs 过期臂 RemoveETag :441-446/:1043-1049）与
//!   TTL 残留清退同点成对；
//! - 普通 SET 覆写保留 etag（C# CopyUpdater SET 分支 `TryCopyOptionals`
//!   连同 etag 复制、InPlace 分支不动 etag）；
//! - 无 etag 记录即 `NoETag = 0`（[wval::NO_ETAG]），条件比较以 0 为基线。

use wdev::Device;
use wval::{KeyTag, TaggedKeyBuf};

use crate::{error::Result, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 生成当前会话专属 key 级 ETag 记录物理键 (KeyTag::Etag)
  ///
  /// 复用标签物理键单点 [`StoreSession::session_tag_key`]，与 [`StoreSession::ttl_key`]
  /// 仅 KeyTag 常量之差：`[NsVarint] + [DbVarint] + 0x0B + [用户键]`
  #[inline(always)]
  pub fn etag_key(&self, user_key: &[u8]) -> TaggedKeyBuf {
    self.session_tag_key(KeyTag::Etag, user_key)
  }

  /// 显式前缀编码 key 级 ETag 记录物理键（循环前缀外提对位，复用
  /// [`StoreSession::session_tag_key_with_prefix`] 单点；rust 工程优化无 c# 对应）
  #[inline(always)]
  pub fn etag_key_with_prefix(prefix: &[u8], user_key: &[u8]) -> TaggedKeyBuf {
    Self::session_tag_key_with_prefix(prefix, KeyTag::Etag, user_key)
  }

  /// 读取 key 的 ETag（None = 无 etag 记录即 NoETag 0；非法值长度按无 etag 容错）
  pub async fn etag_of(&self, user_key: &[u8]) -> Result<Option<i64>> {
    let etag_k = self.etag_key(user_key);
    self.read_i64_sidecar(&etag_k).await
  }

  /// 写入 key 的 ETag 记录（定长 8B，经 I64 旁路写内核 [`StoreSession::put_i64_sidecar`]）
  pub async fn put_etag(&self, user_key: &[u8], etag: i64) -> Result<()> {
    let etag_k = self.etag_key(user_key);
    self.put_i64_sidecar(&etag_k, etag).await
  }

  /// 删除 key 的 ETag 记录（先单次哈希探针初筛：无 etag 键零额外写入）
  ///
  /// AOF 回放清除面（DEL/GETDEL/RENAME 搬离等级联清退的确定性墓碑条目
  /// 重放即经此处，杜绝盘上 ETag 残留令恢复后复活旧 etag），与
  /// [`Self::put_etag`] 成对公开
  pub async fn del_etag(&self, user_key: &[u8]) -> Result<()> {
    let etag_k = self.etag_key(user_key);
    if self.has_etag_key(&etag_k)? {
      self.delete_raw(&etag_k).await?;
    }
    Ok(())
  }

  /// 快速探测 ETag 记录标签是否存在于哈希索引（纯内存单次哈希探针，无记录 I/O）
  #[inline(always)]
  fn has_etag_key(&self, etag_k: &TaggedKeyBuf) -> Result<bool> {
    self.has_tag_key(etag_k)
  }

  /// 在已有纪元保护下快速探测 ETag 记录标签（批处理上下文专用，零原子开销）
  #[inline(always)]
  pub(crate) fn has_etag_key_unprotected(&self, etag_k: &TaggedKeyBuf) -> Result<bool> {
    self.has_tag_key_unprotected(etag_k)
  }
}
