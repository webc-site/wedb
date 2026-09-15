use std::{
  io,
  sync::atomic::Ordering,
};

use wbase::group_commit::{Enter, GroupCommitStep};
use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexStub};
use wdev::Device;
use wrecord::{HEADER_SIZE, RecordHeader};
use wval::{GarnetObjectType, META_VALUE_SIZE, MetaValue, NamespaceDbCodec};

use super::WedbStore;
use crate::error::{Error, Result};

/// 流水线中断错误消息（Follower 侧统一映射）
const PIPELINE_BROKEN: &str = "Flush pipeline broken";

impl<D: Device> WedbStore<D> {
  /// 刷盘前遍历指定页面范围内的原位记录，触发 OnFlush 事件
  /// (1:1 对标 C# ObjectAllocatorImpl.cs:FlushRecordsInRange 与 GarnetRecordTriggers.cs:OnFlush)
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

          if let Some(user_key) = NamespaceDbCodec::decode_meta_user_key(key_slice)
            && val_slice.len() >= META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE
            && let Ok(meta) = MetaValue::from_slice(&val_slice[..META_VALUE_SIZE])
            && meta.collection_type == GarnetObjectType::RangeIndex
          {
            let stub_slice =
              &mut val_slice[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE];
            if let Ok(mut stub) = RangeIndexStub::decode(stub_slice)
              && !stub.is_flushed()
              && !stub.is_transferred()
            {
              self
                .range_index
                .on_flush_address(user_key, &mut stub, record_addr)?;
              RangeIndexStub::slice_set_flushed(stub_slice, true)?;
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
    match self
      .flush_pipeline
      .enter(target, || self.synced_until())
    {
      Enter::Done(_) => return Ok(()),
      Enter::Follow(rx) => {
        // 3. Follower 分支：挂起等待 Leader 批量唤醒，绝不重复发起 I/O
        return self
          .flush_pipeline
          .wait(rx, target, || self.synced_until())
          .await
          .map(|_| ())
          .map_err(|_| io::Error::other(PIPELINE_BROKEN).into());
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
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:FlushAndEvict
  pub async fn flush_and_evict_all(&self) -> Result<()> {
    let tail = self.tail_address();
    self.flush_all().await?;
    self.shift_read_only_address(tail);
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
/// 物理持久化为增量页刷盘（原位 OnFlush 事件 + 页批量落盘）+ 硬件 fsync
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
    // 彻底杜绝从 head 到 tail 的全量重复扫描与重复页写锁占用
    let flushed = self.store.hlog.flushed_until_address();
    if target > flushed {
      let start_page = self.store.hlog.config.page_id(flushed);
      let end_page = self.store.hlog.config.page_id(target.saturating_sub(1));
      if start_page <= end_page {
        self.store.on_flush_pages(start_page, end_page)?;
        self.store.hlog.flush_pages_range(start_page, end_page).await?;
      }
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
