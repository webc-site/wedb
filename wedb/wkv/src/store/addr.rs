use std::{
  path::Path,
  sync::{Arc, atomic::Ordering},
};

use wbase::addr::{is_read_cache, to_absolute};
use wdev::Device;
use whlog::HybridLog;
use windex::{HashBucket, HashBucketEntry};

use super::WedbStore;
use crate::error::{Error, Result};

impl<D: Device> WedbStore<D> {
  /// 获取临时 RangeIndex 目录路径（若未显式指定 range_index_dir）
  #[inline]
  pub fn temp_range_index_dir(&self) -> Option<&Path> {
    self.temp_range_index_dir.as_deref()
  }

  /// 获取哈希索引中已记录的有效条目总数（对标 Tsavorite GetEntryCount）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:GetEntryCount
  pub fn entry_count(&self) -> usize {
    let begin_addr = self.hlog.begin_address();
    let mut count = 0;
    let index = self.index.load();

    for bucket in index.buckets.iter() {
      let mut curr_bucket = bucket;
      loop {
        for item in curr_bucket.entries.iter().take(HashBucket::DATA_ENTRIES) {
          let raw = item.load(Ordering::Acquire);
          if raw == 0 {
            continue;
          }
          let entry = HashBucketEntry::from_raw(raw);
          if entry.is_tentative() {
            continue;
          }
          let addr = entry.address();
          if is_read_cache(addr) {
            let abs_addr = to_absolute(addr);
            if abs_addr >= self.read_cache.head_address()
              && abs_addr < self.read_cache.tail_address()
            {
              count += 1;
            } else {
              // None（滑窗/不可判读）折断开链：本窗口计数快照口径本就保守
              let real_addr = self.read_cache.skip_read_cache(addr).unwrap_or(0);
              if real_addr >= begin_addr {
                count += 1;
              }
            }
          } else if addr >= begin_addr {
            count += 1;
          }
        }

        let overflow_idx = curr_bucket.overflow_index();
        if overflow_idx == 0 {
          break;
        }

        match index.overflow_pool.get(overflow_idx) {
          Some(next) => curr_bucket = next,
          None => break,
        }
      }
    }

    count
  }

  /// 获取当前日志分配尾部逻辑地址（TailAddress）
  #[inline]
  pub fn tail_address(&self) -> u64 {
    self.hlog.tail_address()
  }

  /// 获取当前只读区分界逻辑地址（ReadOnlyAddress）
  #[inline]
  pub fn read_only_address(&self) -> u64 {
    self.hlog.read_only_address()
  }

  /// 获取安全只读区分界逻辑地址（SafeReadOnlyAddress）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:SafeReadOnlyAddress
  #[inline]
  pub fn safe_read_only_address(&self) -> u64 {
    self.hlog.safe_read_only_address()
  }

  /// 获取当前内存头逻辑地址（HeadAddress）
  #[inline]
  pub fn head_address(&self) -> u64 {
    self.hlog.head_address()
  }

  /// 获取有效数据起始逻辑地址（BeginAddress）
  #[inline]
  pub fn begin_address(&self) -> u64 {
    self.hlog.begin_address()
  }

  /// 推进 ReadOnlyAddress（进入该地址之前的记录将变为只读，后续更新触发 CopyUpdate）
  ///
  /// 复活池联动：随只读线推进显式调度 `purge_below`（对标 C# RevivificationManager 随
  /// SafeReadOnlyAddress 推进的过期槽位清扫），避免已滑出可变区的死槽位滞留池内
  /// 挤占分桶容量（`take` 侧另有惰性清扫兜底，此处为主动版）。此处只清复活池、
  /// 不触发 RangeIndex 快照回收，与截断专用的 `after_truncate` 职责不同。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:ShiftReadOnlyAddress
  #[inline]
  pub fn shift_read_only_address(&self, new_ro: u64) {
    self.hlog.shift_read_only_address(new_ro);
    if self.config.enable_revivification {
      self.reviv_pool.purge_below(new_ro);
    }
  }

  /// 推进 HeadAddress（进入该地址之前的记录将被逐出内存，后续读取转为磁盘异步 I/O）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:ShiftHeadAddress
  #[inline]
  pub fn shift_head_address(&self, new_head: u64) {
    self.hlog.shift_head_address(new_head);
  }

  /// 截断水位生效后的联动收口（单一入口）：清退失效复活池槽位 + 回收 RangeIndex
  /// flush 快照。低于新截断线的槽位已物理失效，立即清退防止复活写入命中已截断地址；
  /// 同步通知 RangeIndex 管理器回收地址低于截断线的快照（对标 C#
  /// `GarnetRecordTriggers.OnTruncate` 单点分发，截断后回调只此一处）。
  ///
  /// on_truncate 失败仅告警不上抛：快照回收失败不应让已生效的截断在 FLUSHDB/紧缩
  /// 面回滚。
  ///
  /// libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnTruncate
  #[inline]
  fn after_truncate(&self, addr: u64) {
    if self.config.enable_revivification {
      self.reviv_pool.purge_below(addr);
    }
    if let Err(e) = self.range_index.on_truncate(addr) {
      log::warn!("range_index on_truncate({addr}) 回收快照失败: {e}");
    }
  }

  /// 推进 BeginAddress（进入该地址之前的数据将被截断清理），水位生效后经
  /// `after_truncate` 单点联动收口。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:ShiftBeginAddress
  pub async fn shift_begin_address(&self, new_begin: u64) -> Result<()> {
    self
      .hlog
      .shift_begin_address(new_begin)
      .await
      .map_err(Error::from)?;
    self.after_truncate(new_begin);
    Ok(())
  }

  /// 物理截断历史存储段文件（对标 Garnet `store.Log.Truncate()`）
  ///
  /// 破坏性操作：物理截断并删除底层设备上低于当前 `begin_address` 的所有段文件，
  /// 设备动作生效后经 `after_truncate` 单点联动收口。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:Truncate
  pub async fn truncate(&self) -> Result<()> {
    let begin = self.hlog.begin_address();
    self
      .device
      .truncate_until_address(begin)
      .await
      .map_err(Error::from)?;
    self.after_truncate(begin);
    Ok(())
  }

  /// 获取混合日志分配器引用
  #[inline]
  pub fn hlog(&self) -> &Arc<HybridLog<D>> {
    &self.hlog
  }
}
