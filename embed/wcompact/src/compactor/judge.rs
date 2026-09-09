//! 单条记录死亡判定统一口径（四通道短路：墓碑 → 用户谓词 → 集合历史子键 → TTL）

use wval::{KeyTag, NamespaceDbCodec, TtlCodec};

use super::LogCompactor;
use crate::host::{CompactSession, CompactStore};

impl<S: CompactStore> LogCompactor<S> {
  /// 判定方案 A 物理键是否为已废弃的集合历史子键（集合已删除或当前版本大于子键版本）
  #[inline]
  pub(super) fn is_stale_subkey(&self, key: &[u8]) -> bool {
    if let Some((_tag, key_id, sub_version)) = NamespaceDbCodec::decode_subkey_id_version(key) {
      match self.store.get_key_id_meta(key_id) {
        None => false,
        Some((current_version, is_alive)) => !is_alive || current_version > sub_version,
      }
    } else {
      false
    }
  }

  /// 单条记录死亡判定统一入口：墓碑 → 用户过滤谓词 → 集合历史子键淘汰 →
  /// TTL 过期/孤儿，四通道短路判定
  ///
  /// Lookup 逐记录、Scan 阶段 1 与 Scan 阶段 3 CAS 前复查共用同一口径，消除各阶段
  /// 判定逻辑漂移：墓碑/谓词为记录级确定性判定，子键淘汰与 TTL 为「读最新状态」的
  /// 时敏判定——复查时以调用时刻的 `now` 与最新索引/日志状态重估
  pub(super) async fn judge_dead<F>(
    &self,
    session: &S::Session,
    is_tombstone: bool,
    key: &[u8],
    val: &[u8],
    now: u64,
    is_deleted: &mut F,
  ) -> bool
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    is_tombstone
      || is_deleted(key, val)
      || self.is_stale_subkey(key)
      || self
        .is_expired_or_orphan_record(session, key, val, now)
        .await
  }

  /// 判定物理键与载荷是否属于已过期的 TTL 记录、无主孤儿 TTL 记录、或已到期的数据记录
  ///
  /// 返回 true 表示该记录确已死亡（已过期或无主孤儿），紧缩器直接丢弃并从哈希索引中清理，绝不迁移到尾部！
  async fn is_expired_or_orphan_record(
    &self,
    session: &S::Session,
    key: &[u8],
    val: &[u8],
    now: u64,
  ) -> bool {
    let Ok((_ns, _db, tag, user_key)) = NamespaceDbCodec::decode_tagged_key(key) else {
      return false;
    };
    let Some(tag_offset) = key.len().checked_sub(user_key.len() + 1) else {
      return false;
    };

    match tag {
      KeyTag::Ttl => {
        // 1. 若 TTL 自身已到期：直接判死
        if let Some(exp) = TtlCodec::decode(val)
          && exp <= now
        {
          return true;
        }
        // 2. 若 TTL 尚未到期，短路检查其宿主主键在哈希索引中是否存在（优先探查绝大多数场景存在的 String 键）
        let _guard = session.enter_epoch();
        let str_k = NamespaceDbCodec::replace_tag_at(key, tag_offset, KeyTag::String);
        if self.store.index().find_tag(&str_k).is_some() {
          return false;
        }
        let meta_k = NamespaceDbCodec::replace_tag_at(key, tag_offset, KeyTag::Meta);
        // 宿主既无 String 也无 Meta：属于已被删除的主键留下的孤儿 TTL 记录，直接判死丢弃！
        self.store.index().find_tag(&meta_k).is_none()
      }
      KeyTag::String | KeyTag::Meta => {
        // 检查该数据记录是否附带 TTL 且已过期
        let ttl_k = NamespaceDbCodec::replace_tag_at(key, tag_offset, KeyTag::Ttl);
        if let Ok(Some(exp)) = session.read_ttl_expiry(&ttl_k).await {
          exp <= now
        } else {
          false
        }
      }
      _ => false,
    }
  }
}
