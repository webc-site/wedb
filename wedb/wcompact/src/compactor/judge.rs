//! 单条记录死亡判定统一口径（三通道短路：墓碑 → 用户谓词 → TTL）

use super::LogCompactor;
use crate::host::CompactStore;

impl<S: CompactStore> LogCompactor<S> {
  /// 单条记录死亡判定统一入口：墓碑 → 用户过滤谓词 → TTL 过期/孤儿，
  /// 三通道短路判定
  ///
  /// Lookup 逐记录、Scan 阶段 1 与 Scan 阶段 3 CAS 前复查共用同一口径，消除各阶段
  /// 判定逻辑漂移：墓碑/谓词为记录级确定性判定，TTL 为「读最新状态」的
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
      || self
        .store
        .is_expired_or_orphan_record(session, key, val, now)
        .await
  }
}
