use std::sync::{Arc, atomic::Ordering};

use wbase::addr::{is_read_cache, to_absolute};
use wdev::Device;
use whlog::HybridLog;
use windex::{HashBucket, HashBucketEntry};

use super::WedbStore;
use crate::error::{Error, Result};

impl<D: Device> WedbStore<D> {
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

  /// 复活下限逻辑地址单点推导：`tail - (tail - read_only) × revivifiable_fraction`
  ///
  /// 可变区中最靠后的指定比例窗口才允许原地复活墓碑 / 复用空闲槽位，防止复活写紧贴
  /// 只读区边界被并发只读线推进追尾。链内原地复活与池取两臂、以及各处槽位归还门槛
  /// 一律直调本方法，绝不在下游重写公式（对标 C# `GetMinRevivifiableAddress` 单点：
  /// InternalUpsert.cs:125、InternalRMW.cs:126、BlockAllocate.cs:57、
  /// FreeRecordPool.cs:520/:535 全部经 Helpers.cs 这一处）。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:GetMinRevivifiableAddress
  ///
  /// 两参底层式（C# RevivificationManager.GetMinRevivifiableAddress(tail, readOnly)）
  /// 同挂此处：本单点即由 tail/read_only 两水位推导，rust 不再分两层。
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationManager.cs:GetMinRevivifiableAddress
  #[inline]
  pub fn min_revivifiable_address(&self) -> u64 {
    let read_only = self.hlog.read_only_address();
    let tail = self.hlog.tail_address();
    // 水位不变量 read_only <= tail，钳制只为杜绝两水位瞬时交错时减法回绕
    let window = tail.saturating_sub(read_only);
    // f64 比例换算可能因舍入越出窗口，钳制到 [0, window] 保证下限不低于 read_only
    let frac = ((window as f64) * self.config.revivifiable_fraction) as u64;
    tail.saturating_sub(frac.min(window))
  }

  /// 归池前原子密封并注册复活池——全库「槽位移交 FreeRecordPool」唯一单点
  ///
  /// 先对槽位头原子置位 SEALED（可变区外 try_seal 幂等回 false，不破坏已密封旧标记），
  /// 再按复活下限门槛入池。对标 C# `TryTransferToFreeList` 的前置断言
  /// `logRecord.Info.IsClosed`（Helpers.cs:128）：空闲池中的槽位必须恒处闭合密封态，
  /// 使沿日志扫描或沿旧前驱指针回溯的无锁读者立即感知槽位已失效，绝不再解释其内容；
  /// 缺此密封则槽位一经复活方原位覆写，读者即读出新旧混合的撕裂内容。
  /// 入池门槛取自 [`Self::min_revivifiable_address`] 单点（对标 C# FreeRecordPool.TryAdd
  /// 自持 GetMinRevivifiableAddress），与出池 take 同口径，低于下限的缓冲窗死槽一律拒入。
  ///
  /// 复活开关判定（配置门）由调用方自理——与 RetryAlloc::discard 等既有 CAS 失败
  /// 补偿口径一致。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:TryTransferToFreeList
  #[inline]
  pub(crate) fn transfer_to_reviv_pool(&self, addr: u64, size: u32) {
    let _ = self.hlog.try_seal_record(addr, true);
    self
      .reviv_pool
      .put(addr, size, self.min_revivifiable_address());
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
  ///
  /// core 触发器契约同挂此处（宿主回调与 core 接口在 rust 折叠为同一挂点）：
  /// libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/IRecordTriggers.cs:OnTruncate
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
  /// 纯转调分配器内核 [`whlog::HybridLog::truncate`]：安全纪元排空屏障与删段地板
  /// 双重保护全数在内核收口，本层绝不直接触碰设备执行破坏性删段（对标 C#：上层
  /// 存储引擎从不绕过 AllocatorBase 直达设备，物理删段 100% 收敛于分配器内核）。
  /// 设备动作生效后经 `after_truncate` 单点联动收口。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:Truncate
  pub async fn truncate(&self) -> Result<()> {
    let begin = self.hlog.begin_address();
    self.hlog.truncate().await.map_err(Error::from)?;
    self.after_truncate(begin);
    Ok(())
  }

  /// 获取混合日志分配器引用
  #[inline]
  pub fn hlog(&self) -> &Arc<HybridLog<D>> {
    &self.hlog
  }
}
