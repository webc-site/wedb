//! 单条记录死亡判定统一口径（四通道短路：墓碑 → 用户谓词 → 集合历史子键 → TTL）

use super::LogCompactor;
use crate::host::CompactStore;

impl<S: CompactStore> LogCompactor<S> {
  /// 判定方案 A 物理键是否为已废弃的集合历史子键（集合已删除或当前版本大于子键版本）
  #[inline]
  pub(super) fn is_stale_subkey(&self, key: &[u8]) -> bool {
    self.store.is_stale_subkey(key)
  }

  /// 单条记录死亡判定统一入口：墓碑 → 用户过滤谓词 → 集合历史子键淘汰 →
  /// TTL 过期/孤儿，四通道短路判定
  ///
  /// Lookup 逐记录、Scan 阶段 1 与 Scan 阶段 3 CAS 前复查共用同一口径，消除各阶段
  /// 判定逻辑漂移：墓碑/谓词为记录级确定性判定，子键淘汰与 TTL 为「读最新状态」的
  /// 时敏判定——复查时以调用时刻的 `now`（.NET Ticks，与 TTL 记录存储值同域）与
  /// 最新索引/日志状态重估
  pub(super) async fn judge_dead<F>(
    &self,
    session: &S::Session,
    is_tombstone: bool,
    key: &[u8],
    val: &[u8],
    now: i64,
    is_deleted: &mut F,
  ) -> bool
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    is_tombstone
      || is_deleted(key, val)
      || self.is_stale_subkey(key)
      || self
        .store
        .is_expired_or_orphan_record(session, key, val, now)
        .await
  }
}
