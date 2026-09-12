//! 纯引擎 KV 面（对标 C# Garnet ClientSession 的 Upsert/Read/Delete/RMW 快慢路径）
//!
//! 物理键操作：一切 `*_raw` 与 unprotected 变体、内存直读内核（FindTag 单点探针 +
//! TraceBackForKeyMatch 反向回溯 + 多候选扫描）、磁盘冷读回退、批量预取读、
//! Record Elision 删除与盲墓碑追加。用户键便捷层经 [`keys`](super::keys) 编码后
//! 落到同一套物理路径。

mod batch;
mod modify;
mod read;
mod write;

use std::{result::Result as StdResult, sync::atomic::Ordering};

use wdev::Device;
use wrecord::record_size;

use crate::{
  error::{Error, Result},
  session::StoreSession,
};

#[derive(Debug)]
pub(super) enum ReadProbeResult<T> {
  /// Tag 碰撞未匹配：携带 `prev_address` 供磁盘候选链收集
  Miss(u64),
  Tombstone,
  /// 关闭/密封在途记录命中（严格对照 InternalRead.cs:118 IsClosedOrTombstoned →
  /// RETRY_LATER：刷新纪元后整链重试）
  Retry,
  Found(T),
}

#[derive(Debug)]
pub(super) enum TraceBackResult<T> {
  Found(T),
  Tombstone,
  /// 关闭/密封在途记录命中（严格对照 InternalRead.cs:105-106 与 118 IsClosedOrTombstoned
  /// → RETRY_LATER；密封记录按 C# IsValidTracebackRecord 口径参与键比对，命中即降级重试）
  Retry,
  TraceBack(u64),
}

/// 内存直读内部结果（严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:InternalRead 单遍分类，附加磁盘候选透传）
pub(super) enum MemRead<R> {
  /// 内存阶段已闭环：`Some` 为命中值，`None` 为确认不存在（含墓碑，对应 `NOTFOUND`）
  Done(Option<R>),
  /// 记录位于磁盘区：携带按新版本优先降序排列的磁盘候选地址（对应 `RECORD_ON_DISK`）
  OnDisk(windex::CandidateAddresses),
  /// 命中密封在途记录（对应 `RETRY_LATER`）：刷新纪元后整链重试
  Retry,
}

impl<D: Device> StoreSession<D> {
  /// 追加记录到混合日志尾部，并在环形缓冲区耗尽触发 PageNotReady 时自动将旧页刷盘并驱逐至磁盘
  pub async fn append_record(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
  ) -> Result<u64> {
    self
      .append_record_inner(key, val, prev_addr, is_tombstone, true)
      .await
  }

  /// 紧缩搬迁专用追加：旁路写监听
  ///
  /// AOF 只记原始用户写效果；搬迁帧入 AOF 会在并发写下造成恢复回退
  /// （搬迁旧值帧晚于并发新值帧入队，重放序错乱），故物理布局优化不入 AOF
  pub async fn append_record_compacted(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
  ) -> Result<u64> {
    self
      .append_record_inner(key, val, prev_addr, is_tombstone, false)
      .await
  }

  /// 追加公共体：PageNotReady 时驱逐旧页重试；`notify` 控制写监听回调
  async fn append_record_inner(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
    notify: bool,
  ) -> Result<u64> {
    loop {
      match self.store.hlog.append(key, val, prev_addr, is_tombstone) {
        Ok(addr) => {
          if notify {
            self.notify_write_listener(key, val, is_tombstone);
          }
          return Ok(addr);
        }
        Err(whlog::Error::PageNotReady(page_id)) => {
          self.evict_pages_for(page_id).await?;
        }
        Err(e) => return Err(e.into()),
      }
    }
  }

  /// 纯同步尝试分配空闲槽位或追加 Tail（严格对标 C# Garnet BlockAllocate.cs & InternalUpsert.cs）
  /// - Ok(Ok((addr, slot_size))): 纯内存就地分配或追加成功，slot_size 为真实帧足印
  ///   （复活池槽位为整帧尺寸，尾部追加为对齐逻辑尺寸，对标 C# AllocatedSize 含
  ///   FillerWords 口径，供 SaveAllocationForRetry 复用/弃置时精确注册）
  /// - Ok(Err(page_id)): 环形缓冲区翻转（PageNotReady），需调用者执行异步落盘与驱逐
  pub(super) fn try_allocate_or_append_record_sync(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
  ) -> Result<StdResult<(u64, u32), u64>> {
    if self.store.config.enable_revivification {
      let rec_size = record_size(key.len(), val.len());
      let min_reviv_addr = self.store.hlog.read_only_address();

      // 优先从 FreeRecordPool 提取最适配的空闲槽位并就地复活写入
      if let Some((free_addr, slot_size)) =
        self.store.reviv_pool.take(rec_size as u32, min_reviv_addr)
      {
        if self
          .store
          .hlog
          .revivify_record_at(
            free_addr,
            slot_size as usize,
            key,
            val,
            prev_addr,
            is_tombstone,
          )
          .is_ok()
        {
          self.notify_write_listener(key, val, is_tombstone);
          return Ok(Ok((free_addr, slot_size)));
        } else if self.store.hlog.is_mutable(free_addr) {
          // 若临时写入失败且槽位仍在可变区，归还至复活池防槽位丢失
          self
            .store
            .reviv_pool
            .put(free_addr, slot_size, min_reviv_addr);
        }
      }
    }

    match self.store.hlog.append(key, val, prev_addr, is_tombstone) {
      Ok(addr) => {
        self.notify_write_listener(key, val, is_tombstone);
        Ok(Ok((addr, record_size(key.len(), val.len()) as u32)))
      }
      Err(whlog::Error::PageNotReady(page_id)) => Ok(Err(page_id)),
      Err(e) => Err(e.into()),
    }
  }

  /// 触发写监听端口（未注入则零开销跳过；purge 链窗口内本会话通知被抑制）
  ///
  /// purge 链抑制（会话级精确匹配，机制见 [`crate::ttl::PurgeNotifyGuard`] 与
  /// `WedbStore::purge_suppress`）：purge_expired 窗口内仅本会话的物理写镜像
  /// 被跳过——TTL 记录 + 数据两条墓碑折叠为单条确定性逻辑条目（对标 Garnet
  /// `RespInputFlags.Deterministic` 单条目语义）；其他会话（含并发同键写与
  /// 内置 GC 会话）的通知绝不受影响。
  #[inline]
  pub(super) fn notify_write_listener(&self, key: &[u8], val: &[u8], tombstone: bool) {
    if self.store.aof_listeners_paused.load(Ordering::Relaxed) {
      return;
    }
    if self.store.purge_suppress.load(Ordering::Relaxed) == self as *const Self as usize {
      return;
    }
    // TTL 旁路记录（KeyTag::Ttl）分流到 TTL 写端口：设置/更新过期回调
    // Some(ticks)、清除过期（墓碑形态）回调 None；端口未注册时回落通用
    // 写端口（嵌入式无 AOF 场景保持物理镜像行为不变）
    if let Ok((ns, db, tag, user_key)) = wval::NamespaceDbCodec::decode_tagged_key(key)
      && tag == wval::KeyTag::Ttl
      && let Some(listener) = self.store.ttl_write_listener()
    {
      let expire = (!tombstone).then(|| wval::TtlCodec::decode(val)).flatten();
      listener(ns, db, user_key, expire);
      return;
    }
    if let Some(listener) = self.store.write_listener() {
      listener(key, val, tombstone);
    }
  }

  /// 当环形缓冲区发生翻转并遇到槽位尚未驱逐时，异步刷盘并推进 HeadAddress
  pub(super) async fn evict_pages_for(&self, page_id: u64) -> Result<()> {
    let num_pages = self.store.hlog.config.num_pages as u64;
    if page_id < num_pages {
      return Ok(());
    }
    let old_page = page_id - num_pages;
    let curr_tail = self.store.hlog.tail_address();
    let curr_tail_page = self.store.hlog.config.page_id(curr_tail);

    // 批量推进驱逐窗口，平摊磁盘 I/O 成本并避免逐页频繁颠簸
    let batch_pages = (num_pages / 8).clamp(1, 64);
    let target_evict_page = (old_page + batch_pages)
      .min(curr_tail_page.saturating_sub(1))
      .max(old_page);

    let flushed_until = self.store.hlog.flushed_until_address();
    let start_page = self.store.hlog.config.page_id(flushed_until);

    // 关键：在执行页面刷盘与等待 SafeHeadAddress 期间，临时挂起当前会话的纪元保护区，
    // 彻底消除长时间磁盘 I/O 和 safe_head 自旋钉住纪元、阻塞全系统页回收的隐患
    // （严格对标 C# Tsavorite IO/Flush 期间的 UnsafeSuspendThread 协议）
    struct EpochSuspendGuard<'a> {
      participant: &'a wepoch::Participant,
      count: u32,
    }
    impl Drop for EpochSuspendGuard<'_> {
      fn drop(&mut self) {
        for _ in 0..self.count {
          let _ = self.participant.enter();
        }
      }
    }
    let reentrant = self.participant.reentrant_count();
    for _ in 0..reentrant {
      self.participant.exit();
    }
    let _suspend_guard = EpochSuspendGuard {
      participant: &self.participant,
      count: reentrant,
    };

    if start_page <= target_evict_page {
      self
        .store
        .flush_pages_range(start_page, target_evict_page)
        .await?;
      self.store.device.sync().await.map_err(Error::from)?;
    }

    let min_evicted_addr = self
      .store
      .hlog
      .config
      .page_start_address(target_evict_page + 1);
    self.store.shift_read_only_address(min_evicted_addr);
    self.store.shift_head_address(min_evicted_addr);

    let target = min_evicted_addr.min(self.store.hlog.head_address());
    if target > self.store.hlog.safe_head_address() {
      self.store.hlog.wait_safe_head_drained(target).await;
    }

    Ok(())
  }

  /// 底层检查指定物理键是否存在且未被墓碑删除（Contains Key Raw）
  /// 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ContainsKeyInMemory.cs:InternalContainsKeyInMemory 与完整读路径：
  /// 基于 zero-copy 闭包读取，0 堆分配，严格沿 prev_address 反向链回溯处理 Tag 碰撞
  #[inline]
  pub async fn contains_key_raw(&self, key: &[u8]) -> Result<bool> {
    Ok(self.read_raw_with(key, |_| ()).await?.is_some())
  }

  /// 在当前会话纪元保护下按逻辑地址直接读取记录（对标 Tsavorite ReadAtAddress）
  /// 磁盘区记录走免纪元纯设备路径，内存驻留区持短守卫保护
  pub async fn read_record(&self, addr: u64) -> Result<whlog::RecordOutput> {
    if self.store.hlog.is_on_disk(addr) {
      self
        .store
        .hlog
        .read_disk_record(addr)
        .await
        .map_err(Into::into)
    } else {
      let _guard = self.participant.enter();
      self.store.hlog.read_record(addr).await.map_err(Into::into)
    }
  }
}
