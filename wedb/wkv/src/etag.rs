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
//!   `try_delete_sync_unprotected`，与 TTL 级联同点；
//! - 普通 SET 覆写保留 etag（C# CopyUpdater SET 分支 `TryCopyOptionals`
//!   连同 etag 复制、InPlace 分支不动 etag）；
//! - 无 etag 记录即 `NoETag = 0`（[wval::NO_ETAG]），条件比较以 0 为基线。

use wdev::Device;
use wval::{ETAG_VAL_LEN, EtagCodec, KeyTag, NamespaceDbCodec, TaggedKeyBuf};

use crate::{error::Result, session::StoreSession};

/// ETag 记录值定长字节数（8 字节大端 i64，对标 LogRecord.cs:ETagSize）
pub const ETAG_VALUE_LEN: usize = ETAG_VAL_LEN;

impl<D: Device> StoreSession<D> {
  /// 生成当前会话专属 key 级 ETag 记录物理键 (KeyTag::Etag)
  ///
  /// 镜像 [`StoreSession::ttl_key`]：`[NsVarint] + [DbVarint] + 0x0B + [用户键]`
  #[inline(always)]
  pub fn etag_key(&self, user_key: &[u8]) -> TaggedKeyBuf {
    let prefix = self.session_prefix();
    NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::Etag, user_key)
  }

  /// 读取 key 的 ETag（None = 无 etag 记录即 NoETag 0；非法值长度按无 etag 容错）
  pub async fn etag_of(&self, user_key: &[u8]) -> Result<Option<i64>> {
    let etag_k = self.etag_key(user_key);
    Ok(
      self
        .read_raw_with(&etag_k, EtagCodec::decode)
        .await?
        .flatten(),
    )
  }

  /// 写入 key 的 ETag 记录（定长 8B：可变区原位改写优先，失败降级 RCU 盲插）
  ///
  /// 取舍镜像 [`StoreSession::put_ttl`]：原位改写零追加零索引 CAS，是
  /// SETWITHETAG 族反复推进 etag 的主路径；长度守卫杜绝
  /// `copy_from_slice` 长度失配 panic
  pub async fn put_etag(&self, user_key: &[u8], etag: i64) -> Result<()> {
    let bytes = EtagCodec::encode(etag);
    let etag_k = self.etag_key(user_key);
    let in_place = {
      let _guard = self.participant.enter();
      self
        .try_modify_raw_in_place_unprotected(&etag_k, |slot| {
          if slot.len() != ETAG_VALUE_LEN {
            return None;
          }
          slot.copy_from_slice(&bytes);
          Some(())
        })?
        .is_some()
    };
    if !in_place {
      self.upsert_raw(&etag_k, &bytes).await?;
    }
    Ok(())
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
  fn has_etag_key(&self, etag_k: &TaggedKeyBuf) -> Result<bool> {
    let _guard = self.participant.enter();
    Ok(self.store.index.find_tag(etag_k).is_some())
  }

  /// 在已有纪元保护下快速探测 ETag 记录标签（批处理上下文专用，零原子开销）
  pub(crate) fn has_etag_key_unprotected(&self, etag_k: &TaggedKeyBuf) -> Result<bool> {
    Ok(self.store.index.find_tag(etag_k).is_some())
  }
}
