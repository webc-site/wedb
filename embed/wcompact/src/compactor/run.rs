//! 两种紧缩模式实现：Lookup 逐记录判活迁移（对标 libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs:CompactLookup）与
//! Scan 三阶段候选去重迁移（对标 libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs:CompactScan）

use wbase::time::now_ms;
use whasher::{HashMap, new_hash_map};

use super::{
  CompactRunResult, CopyOutcome, LogCompactor, cursor::ScanCursor, scope::MetaDeathScope,
};
use crate::{error::Result, host::CompactStore};

/// 扫描候选记录元数据（用于 Scan 紧缩模式，不缓存值体，空间 O(唯一键)）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CandidateRecord {
  addr: u64,
  /// 死亡判定（墓碑、自定义过滤或集合历史子键，不止墓碑一种形态）
  is_dead: bool,
}

impl<S: CompactStore> LogCompactor<S> {
  /// Lookup 模式紧缩实现（对标 Tsavorite `CompactLookup`）
  ///
  /// 与 C# 的口径对齐：C# 逐记录独立判定（`ConditionalCopyToTail` 对单记录无界重试，
  /// 绝不因单记录竞争放弃整个紧缩区间）；本实现对单记录采用有界重试 + 记录级保守
  /// 保留——CAS 预算耗尽且记录仍存活时仅将该记录标记为保留（截断点下界回退至其
  /// 起始边界），扫描继续推进，后续记录照常判定/迁移。最终截断点取全部保留记录的
  /// 最小起始边界：保留记录原位存活（区间永不被截断到其之上），其后的已迁移记录
  /// 以 RCU 副本形式存在于尾部、原位置留作垃圾由下一轮紧缩回收——与 Scan 模式的
  /// 保守保留回退语义一致。
  pub(super) async fn compact_lookup<F>(
    &self,
    session: &S::Session,
    begin_addr: u64,
    until_address: u64,
    read_only_addr: u64,
    mut is_deleted: F,
  ) -> Result<CompactRunResult>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    let mut cursor = ScanCursor::new(self.store.hlog(), begin_addr, until_address);
    let mut scanned_records: usize = 0;
    let mut live_copied: usize = 0;
    let mut superseded: usize = 0;
    let mut dead_dropped: usize = 0;
    let mut retained: usize = 0;
    // 扫描作用域：集合元数据归属 + key_id 死亡登记（收尾时据此回收死条目）
    let mut scope = MetaDeathScope::new();
    // 截断点始终预推进到最后一条已校验记录的结束边界
    let mut actual_until = begin_addr;
    // 保守保留回退下界：截断点绝不越过仍为最新存活版本的记录起始边界
    let mut retain_floor: Option<u64> = None;
    let now = now_ms();

    while let Some((curr_addr, rec)) = cursor.pull().await? {
      // 物理终点含松弛填充，与扫描游标推进口径一致（对标 C# iter1.NextAddress）
      actual_until = curr_addr + rec.physical_size() as u64;
      if actual_until > read_only_addr {
        // 记录跨出只读区快照：尾部残段不可紧缩，截断点回退至该记录起始边界
        actual_until = curr_addr;
        break;
      }

      scanned_records += 1;
      let is_tombstone = rec.is_tombstone();
      let key = rec.key;
      let val = if is_tombstone { &[] } else { rec.value };
      scope.observe(&*self.store, is_tombstone, key, val, curr_addr);

      // 墓碑短路在前，自定义谓词与子键淘汰仅对非墓碑记录生效（对标 C# `!Tombstone && !IsDeleted`）
      if self
        .judge_dead(session, is_tombstone, key, val, now, &mut is_deleted)
        .await
      {
        dead_dropped += 1;
        self.store.index().delete(key, curr_addr);
      } else {
        let latest = self
          .find_latest_address(session, key, Some((curr_addr, false)))
          .await?;
        if let Some(latest) = latest
          && latest.main_addr == curr_addr
          && !latest.is_tombstone
        {
          match self
            .conditional_copy_to_tail(session, key, val, curr_addr, latest.index_addr)
            .await?
          {
            CopyOutcome::Copied => live_copied += 1,
            // 复核确认已被并发覆盖/删除：安全放弃迁移
            CopyOutcome::Superseded => superseded += 1,
            // 重试耗尽仍为最新存活版本：记录级保守保留，截断点下界回退至其起始
            // 边界，扫描继续推进（论证见本方法文档）
            CopyOutcome::Retain => {
              retained += 1;
              retain_floor = Some(match retain_floor {
                Some(floor) => floor.min(curr_addr),
                None => curr_addr,
              });
            }
          }
        } else {
          // 非最新版本：并发新版本已生效，安全弃迁
          superseded += 1;
        }
      }
    }

    // 游标页跳越出只读区快照时回退到最后校验边界，绝不越界截断
    let cursor_addr = cursor.cursor();
    if cursor_addr <= read_only_addr {
      actual_until = cursor_addr;
    }
    // 保守保留回退：紧缩区间止步于最早仍存活记录的起始边界之前
    if let Some(floor) = retain_floor {
      actual_until = actual_until.min(floor);
    }

    Ok(CompactRunResult {
      scanned_records,
      live_copied,
      superseded,
      dead_dropped,
      retained,
      actual_until,
      scope,
    })
  }

  /// Scan 模式紧缩实现（对标 Tsavorite `CompactScan`）
  pub(super) async fn compact_scan<F>(
    &self,
    session: &S::Session,
    begin_addr: u64,
    until_address: u64,
    read_only_addr: u64,
    mut is_deleted: F,
  ) -> Result<CompactRunResult>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    let mut cursor = ScanCursor::new(self.store.hlog(), begin_addr, until_address);
    let mut scanned_records: usize = 0;
    let mut live_copied: usize = 0;
    let mut superseded: usize = 0;
    let mut dead_dropped: usize = 0;
    let mut retained: usize = 0;
    // 候选表：键 -> 区间内最新版本元数据（值体不缓存，阶段 3 确认存活后按需单次回读）
    let mut candidates: HashMap<Box<[u8]>, CandidateRecord> = new_hash_map();
    // 扫描作用域：集合元数据归属 + key_id 死亡登记（收尾时据此回收死条目）
    let mut scope = MetaDeathScope::new();
    let mut actual_until = begin_addr;
    // 保守保留回退下界：截断点绝不越过仍为最新存活版本的记录起始边界
    let mut retain_floor: Option<u64> = None;
    let now = now_ms();

    // 阶段 1：扫描紧缩区间，收集每键最新版本元数据（对标 C# 临时 KV 的 Upsert/Delete）
    while let Some((curr_addr, rec)) = cursor.pull().await? {
      // 物理终点含松弛填充，与扫描游标推进口径一致（对标 C# iter1.NextAddress）
      actual_until = curr_addr + rec.physical_size() as u64;
      if actual_until > read_only_addr {
        actual_until = curr_addr;
        break;
      }

      scanned_records += 1;
      let is_tombstone = rec.is_tombstone();
      let key = rec.key;
      let raw_val = if is_tombstone { &[] } else { rec.value };
      scope.observe(&*self.store, is_tombstone, key, raw_val, curr_addr);
      let is_dead = self
        .judge_dead(session, is_tombstone, key, raw_val, now, &mut is_deleted)
        .await;

      match candidates.get_mut(key) {
        Some(cand) => {
          // 同键更新版本在区间内出现：旧候选记录被新版本替换，计为弃迁
          // （精确守恒：每键 n 次出现 = n-1 次弃迁 + 末版候选的阶段 3 处置）
          superseded += 1;
          cand.addr = curr_addr;
          cand.is_dead = is_dead;
        }
        None => {
          candidates.insert(
            Box::from(key),
            CandidateRecord {
              addr: curr_addr,
              is_dead,
            },
          );
        }
      }
    }

    // 游标页跳越出只读区快照时回退到最后校验边界，绝不越界截断
    let cursor_addr = cursor.cursor();
    if cursor_addr <= read_only_addr {
      actual_until = cursor_addr;
    }

    // 阶段 2：扫过剩余只读区，单趟剔除在该区间发生过更新/删除的键（候选集清空即提前终止）
    cursor = ScanCursor::new(self.store.hlog(), actual_until, read_only_addr);
    while !candidates.is_empty()
      && let Some((curr_addr, rec)) = cursor.pull().await?
    {
      if curr_addr + rec.physical_size() as u64 > read_only_addr {
        break;
      }
      if candidates.remove(rec.key).is_some() {
        // 阶段 2 剔除：紧缩区候选已被只读尾部区间的新版本/墓碑取代，计为弃迁
        superseded += 1;
      }
    }

    // 阶段 3：校验并迁移幸存候选（索引现值校验 + 条件复制，绝不覆盖并发新版本）。
    // 复查以阶段 3 当下时刻为准：长时间紧缩期间 TTL 可能刚到期、元数据水位可能已
    // 推进，CAS 迁移前必须以最新状态重估
    let now = now_ms();
    for (key, cand) in candidates {
      // 快速清理通道：阶段 1 已判死，或子键淘汰水位在阶段 1 之后已判死——
      // 先于 find_latest/read 的无记录 I/O 快路径（子键淘汰判定与 judge_dead
      // 内同一实现，幂等）
      if cand.is_dead || self.is_stale_subkey(&key) {
        self.store.index().delete(&key, cand.addr);
        dead_dropped += 1;
        continue;
      }

      let latest = self
        .find_latest_address(session, &key, Some((cand.addr, false)))
        .await?;
      let Some(latest) = latest.filter(|l| l.main_addr == cand.addr && !l.is_tombstone) else {
        // 非最新版本：并发新版本已生效，安全弃迁
        superseded += 1;
        continue;
      };

      // 按需单次回读值体（阶段 1 不缓存大值，空间 O(唯一键) 元数据）
      let record = self.store.hlog().read_record(cand.addr).await?;
      let val = record.value()?;
      // CAS 前同口径复查（与 is_stale_subkey 复查同一模式）：阶段 1 之后 TTL 可能
      // 刚到期，以最新 TTL 状态与当下时间统一重判，杜绝长时间紧缩迁移刚过期记录
      if self
        .judge_dead(session, false, &key, val, now, &mut is_deleted)
        .await
      {
        self.store.index().delete(&key, cand.addr);
        dead_dropped += 1;
        continue;
      }

      match self
        .conditional_copy_to_tail(session, &key, val, cand.addr, latest.index_addr)
        .await?
      {
        CopyOutcome::Copied => live_copied += 1,
        // 真并发覆盖：新版本已在紧缩区间之外生效，旧版本安全随截断退役
        CopyOutcome::Superseded => superseded += 1,
        // 重试耗尽仍为最新存活版本：截断点回退至其起始边界，绝不误删
        CopyOutcome::Retain => {
          retained += 1;
          retain_floor = Some(match retain_floor {
            Some(floor) => floor.min(cand.addr),
            None => cand.addr,
          });
        }
      }
    }
    // 保守保留回退：紧缩区间止步于最早仍存活候选的起始边界之前
    if let Some(floor) = retain_floor {
      actual_until = actual_until.min(floor);
    }

    Ok(CompactRunResult {
      scanned_records,
      live_copied,
      superseded,
      dead_dropped,
      retained,
      actual_until,
      scope,
    })
  }
}
