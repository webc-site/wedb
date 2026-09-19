use std::sync::atomic::Ordering;

use wbase::group_commit::{Enter, GroupCommitStep};
use wbftree::Error as WbftreeError;
use wdev::Device;
use wrecord::{HEADER_SIZE, RecordHeader};
use wval::NamespaceDbCodec;

use super::WedbStore;
use crate::{
  error::{Error, Result},
  range_index::patch_stub_record,
};

impl<D: Device> WedbStore<D> {
  /// 刷盘前遍历指定页面范围内的原位记录，触发 OnFlush 事件
  /// (1:1 对标 C# ObjectAllocatorImpl.cs:FlushRecordsInRange 与 GarnetRecordTriggers.cs:OnFlush)
  ///
  /// 分发闭环说明：C# `GarnetRecordTriggers.CallOnFlush => rangeIndexManager != null`，
  /// OnFlush 的唯一触发类别即 RangeIndex 存根快照（VectorManager 仅挂 OnEvict/
  /// OnDiskRead，无 OnFlush 钩子），本实现只识别 RangeIndex 即与 C# 注册面完全等价，
  /// 无遗漏的通用分发臂。
  pub fn on_flush_pages(&self, start_page: u64, end_page: u64) -> Result<()> {
    let page_size = self.hlog.config.page_size;
    for p in start_page..=end_page {
      if !self.hlog.buffer.is_page_loaded(p) {
        continue;
      }
      let mut page_guard = self.hlog.buffer.write_page(p);
      let page_start = self.hlog.config.page_start_address(p);
      let init_page = self.hlog.config.page_id(self.hlog.config.initial_address);
      let mut offset = if p == init_page {
        self
          .hlog
          .config
          .page_offset(self.hlog.config.initial_address)
      } else {
        0
      };

      while offset + HEADER_SIZE <= page_size {
        let header_bytes = &page_guard[offset..offset + HEADER_SIZE];
        if RecordHeader::is_zero_slice(header_bytes) {
          break;
        }
        let Ok(header) = RecordHeader::from_slice(header_bytes) else {
          break;
        };
        if header.is_pad() || header.is_null() {
          break;
        }
        let physical_size = header.physical_size();
        if physical_size == 0 || offset + physical_size > page_size {
          break;
        }
        let record_addr = page_start + offset as u64;
        let key_len = header.key_len() as usize;
        let val_len = header.val_len() as usize;
        let key_start = offset + HEADER_SIZE;
        let key_end = key_start + key_len;
        let val_end = key_end + val_len;

        if val_end <= offset + physical_size && !header.is_tombstone() {
          let (k_part, v_part) = page_guard.split_at_mut(key_end);
          let key_slice = &k_part[key_start..key_end];
          let val_slice = &mut v_part[..val_len];

          if let Some(user_key) = NamespaceDbCodec::decode_meta_user_key(key_slice) {
            // OnFlush 存根置位一律转调 wkv 唯一治愈内核（对标 C#
            // GarnetRecordTriggers.cs:OnFlush → RangeIndexManager.cs:SnapshotTreeForFlush
            // 成功尾段的 SetFlushedFlag(valueSpan)）：内核识别存活 RangeIndex 元记录
            // 并就地改 35B 存根窗口，页写锁内零复制零分配、值体长度无上限，
            // 杜绝旧实现在此的第四口径（解码后无条件补写 Flushed 位）。
            // 快照未落（所有权已转移 / 工作文件缺失的不变量破坏 → on_flush_address
            // 按 C# LogOnFlushInvariantViolation 明令「must NOT set IsFlushed」拒绝置位）
            // 时内核返回 false 零写，记录留在未刷盘态交由惰性恢复承接
            let mut flush_err: Option<WbftreeError> = None;
            patch_stub_record(val_slice, |stub| {
              if stub.is_flushed() {
                return false;
              }
              match self
                .range_index
                .on_flush_address(user_key, stub, record_addr)
              {
                Ok(()) => stub.is_flushed(),
                Err(e) => {
                  flush_err = Some(e);
                  false
                }
              }
            });
            if let Some(e) = flush_err {
              return Err(e.into());
            }
          }
        }
        offset += physical_size;
      }
    }
    Ok(())
  }

  /// 批量合并落盘指定逻辑页范围，并在落盘前原位触发 OnFlush 刷盘快照
  pub async fn flush_pages_range(&self, start_page: u64, end_page: u64) -> Result<()> {
    if start_page <= end_page {
      self.on_flush_pages(start_page, end_page)?;
      self.hlog.flush_pages_range(start_page, end_page).await?;
    }
    Ok(())
  }

  /// 将内存中所有驻留脏页异步刷盘并同步设备（严格对标 Garnet Group Commit 流水线）
  pub async fn flush_all(&self) -> Result<()> {
    let target = self.tail_address();

    // 1. 快速短路（0 I/O）：目标水位已被硬件 sync 持久化覆盖
    if target <= self.synced_until() {
      return Ok(());
    }

    // 2. 状态机协商：判定成为 Leader 还是 Follower
    match self.flush_pipeline.enter(target, || self.synced_until()) {
      Enter::Done(_) => return Ok(()),
      Enter::Follow(rx) => {
        // 3. Follower 分支：挂起等待 Leader 批量唤醒，绝不重复发起 I/O
        return self
          .flush_pipeline
          .wait(rx, target, || self.synced_until())
          .await
          .map(|_| ())
          .map_err(Error::from);
      }
      // 升级为 Leader，接管物理刷盘管道
      Enter::Lead => {}
    }

    // 4. Leader 级联执行循环（Cascade Loop）
    self
      .flush_pipeline
      .run_leader(FlushStep { store: self })
      .await
      .map(|_| ())
  }

  /// 将内存所有页面刷盘并全部驱逐至磁盘区（对标 Tsavorite FlushAndEvict）
  ///
  /// 次序严格对标 C# `LogAccessor.FlushAndEvict`：`ShiftReadOnlyToTail`（封印并等
  /// 纪元排空 → 由排空动作发起刷盘）→ 等 FlushedUntil 达标 → 推进 head 驱逐。
  /// rust 侧的排空+刷盘折叠在同一个刷盘内核里（whlog `flush_pages_range` 入口先封后刷），
  /// 故此处显式封印的意义是把 wkv 侧联动（复活池清扫）落在驱逐之前，且使
  /// 「先封后刷」在读侧上界单源之上再有一处意图声明；重复封印为 fetch_max 空转，零成本。
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:FlushAndEvict
  pub async fn flush_and_evict_all(&self) -> Result<()> {
    let tail = self.tail_address();
    self.shift_read_only_address(tail);
    self.flush_all().await?;
    self.shift_head_address(tail);
    Ok(())
  }

  /// 读取硬件已完成 sync 持久化的最高连续逻辑地址水位
  #[inline]
  pub(crate) fn synced_until(&self) -> u64 {
    self.synced_until.load(Ordering::Acquire)
  }
}

/// KV 页刷盘步进器：批次目标取混合日志尾地址，水位取硬件 sync 持久化位点，
/// 物理持久化为增量页刷盘 + 硬件 fsync。step 只做增量区间换算，
/// 落盘与原位 OnFlush 触发一律经 store::flush_pages_range 单点分发
/// （对标 C# AllocatorBase.AsyncFlushPagesForSnapshot 的单一刷盘内核形态）
///
/// 读侧上界口径：step 不自备 safe_read_only 门槛，也不另立钳制——刷盘内核
/// （whlog `flush_pages_range`）在写入之前把待刷上界封印为 SafeReadOnlyAddress 并等
/// 纪元排空，故无论宿主是自带封印的检查点链（wcpr create.rs）还是 FLUSHLOG 链
/// （[WedbStore::flush_and_evict_all]），落盘的字节恒已定稿，`flushed_until` 恒不越过
/// 安全只读线（对标 C# 仅由 OnPagesMarkedReadOnly 纪元动作驱动刷盘的单一形态）
struct FlushStep<'a, D: Device> {
  store: &'a WedbStore<D>,
}

impl<D: Device> GroupCommitStep for FlushStep<'_, D> {
  type Error = Error;

  #[inline]
  fn tail(&self) -> u64 {
    self.store.tail_address()
  }

  #[inline]
  fn watermark(&self) -> u64 {
    self.store.synced_until()
  }

  async fn step(&self, target: u64) -> Result<u64> {
    // (1) 增量页刷盘：仅从当前 flushed_until 所在页刷到 target 所在页，
    // 彻底杜绝从 head 到 tail 的全量重复扫描与重复页写锁占用。
    // step 只做增量区间换算，落盘与 OnFlush 触发一律经 store::flush_pages_range
    // 单点（页序守卫在其内部同口径覆盖），杜绝在此另加直连 hlog 的快路径
    let flushed = self.store.hlog.flushed_until_address();
    if target > flushed {
      let start_page = self.store.hlog.config.page_id(flushed);
      let end_page = self.store.hlog.config.page_id(target.saturating_sub(1));
      self.store.flush_pages_range(start_page, end_page).await?;
    }

    // (2) 物理介质持久化（硬件 fsync）：全系统单协程串行执行，彻底消除抖动争抢
    self.store.device.sync().await.map_err(Error::from)?;

    // (3) 推进持久化水位（页粒度刷盘可能越过目标，取 min）
    let new_synced = target.min(self.store.hlog.flushed_until_address());
    self
      .store
      .synced_until
      .fetch_max(new_synced, Ordering::AcqRel);
    Ok(new_synced)
  }
}
