use std::{
  io,
  sync::atomic::{AtomicU64, Ordering},
};

use crossfire::oneshot::{TxOneshot, oneshot};
use parking_lot::Mutex;
use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexStub};
use wdev::Device;
use wrecord::{HEADER_SIZE, RecordHeader};
use wval::{GarnetObjectType, META_VALUE_SIZE, MetaValue, NamespaceDbCodec};

use super::WedbStore;
use crate::error::{Error, Result};

/// Group Commit 挂起等待者契约（对标 Garnet TsavoriteLog.CommitTask）
pub(crate) struct FlushWaiter {
  pub(crate) target: u64,
  pub(crate) tx: Option<TxOneshot<Result<()>>>,
}

/// 刷盘流水线互斥状态
pub(crate) struct FlushState {
  pub(crate) is_flushing: bool,
  pub(crate) waiters: Vec<FlushWaiter>,
}

/// 对标 Garnet 的 Group Commit / Pipeline Flush 控制器
pub struct FlushPipeline {
  pub(crate) state: Mutex<FlushState>,
  /// 硬件已完成 sync 持久化的最高连续逻辑地址水位
  pub synced_until: AtomicU64,
}

impl FlushPipeline {
  pub fn new(initial_addr: u64) -> Self {
    Self {
      state: Mutex::new(FlushState {
        is_flushing: false,
        waiters: Vec::new(),
      }),
      synced_until: AtomicU64::new(initial_addr),
    }
  }

  #[inline]
  pub fn synced_until(&self) -> u64 {
    self.synced_until.load(Ordering::Acquire)
  }
}

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
    if target <= self.flush_pipeline.synced_until() {
      return Ok(());
    }

    // 2. 状态机协商：判定成为 Leader 还是 Follower
    let rx = {
      let mut lock = self.flush_pipeline.state.lock();

      // 双重检查防并发竞态
      if target <= self.flush_pipeline.synced_until() {
        return Ok(());
      }

      if lock.is_flushing {
        // 当前已有 Leader 在刷盘，登记为 Follower 挂起等待，绝不重复发起 I/O
        let (tx, rx) = oneshot::<Result<()>>();
        lock.waiters.push(FlushWaiter {
          target,
          tx: Some(tx),
        });
        Some(rx)
      } else {
        // 升级为 Leader，接管物理刷盘管道
        lock.is_flushing = true;
        None
      }
    };

    // 3. Follower 分支：挂起等待 Leader 批量唤醒
    if let Some(rx) = rx {
      return match rx.await {
        Ok(res) => res,
        Err(_) => Err(io::Error::other("Flush pipeline broken").into()),
      };
    }

    // 4. Leader 级联执行循环（Cascade Loop）
    self.run_flush_pipeline_leader_loop().await
  }

  /// Leader 级联刷盘驱动主循环
  async fn run_flush_pipeline_leader_loop(&self) -> Result<()> {
    loop {
      // (1) 收集当前批次目标：当前 tail 与所有挂起 Follower 的最大需求
      let batch_target = {
        let lock = self.flush_pipeline.state.lock();
        let max_waiter_target = lock.waiters.iter().map(|w| w.target).max().unwrap_or(0);
        let cur_tail = self.tail_address();
        cur_tail.max(max_waiter_target)
      };

      let synced = self.flush_pipeline.synced_until();
      let res: Result<()> = async {
        if batch_target > synced {
          // (2) 增量页刷盘：仅从当前 flushed_until 所在页刷到 batch_target 所在页
          // 彻底杜绝从 head 到 tail 的全量重复扫描与重复页写锁占用
          let flushed = self.hlog.flushed_until_address();
          if batch_target > flushed {
            let start_page = self.hlog.config.page_id(flushed);
            let end_page = self.hlog.config.page_id(batch_target.saturating_sub(1));
            if start_page <= end_page {
              self.on_flush_pages(start_page, end_page)?;
              self.hlog.flush_pages_range(start_page, end_page).await?;
            }
          }

          // (3) 物理介质持久化（硬件 fsync）：全系统单协程串行执行，彻底消除抖动争抢
          self.device.sync().await.map_err(Error::from)?;

          // (4) 推进持久化水位
          let current_flushed = self.hlog.flushed_until_address();
          let new_synced = batch_target.min(current_flushed);
          self
            .flush_pipeline
            .synced_until
            .fetch_max(new_synced, Ordering::AcqRel);
        }
        Ok(())
      }
      .await;

      // (5) 异常分发或批量成功唤醒
      let mut lock = self.flush_pipeline.state.lock();
      match res {
        Ok(()) => {
          let current_synced = self.flush_pipeline.synced_until();
          // 批量精准唤醒所有位点已覆盖的 Follower
          lock.waiters.retain_mut(|w| {
            if w.target <= current_synced {
              if let Some(tx) = w.tx.take() {
                tx.send(Ok(()));
              }
              false
            } else {
              true
            }
          });

          // (6) 级联检查：若仍有更高水位的 Follower 积压，或有新追加记录超过 synced
          let has_higher_waiters = !lock.waiters.is_empty();
          let tail_advanced = self.tail_address() > current_synced;

          if !has_higher_waiters && !tail_advanced {
            // 管道完全排空，释放 Leader 身份并退出
            lock.is_flushing = false;
            return Ok(());
          }
          // 仍有积压，Leader 继续下一轮批处理
        }
        Err(err) => {
          // 发生错误，向所有等待者广播错误，释放 Leader 身份，杜绝死锁
          for mut w in lock.waiters.drain(..) {
            if let Some(tx) = w.tx.take() {
              tx.send(Err(io::Error::other("Pipeline flush failed").into()));
            }
          }
          lock.is_flushing = false;
          return Err(err);
        }
      }
    }
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
}
