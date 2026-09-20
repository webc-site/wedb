//! 纯引擎 KV 面（对标 C# Garnet ClientSession 的 Upsert/Read/Delete/RMW 快慢路径）
//!
//! 物理键操作：一切 `*_raw` 与 unprotected 变体、内存直读内核（FindTag 单点探针 +
//! TraceBackForKeyMatch 反向回溯 + 多候选扫描）、磁盘冷读回退、批量预取读、
//! Record Elision 删除与盲墓碑追加。用户键便捷层经 [`keys`](super::keys) 编码后
//! 落到同一套物理路径。

use wepoch::EpochSuspendGuard;
use whlog::Error as WhlogError;
use wval::{I64Codec, KeyTag, NamespaceDbCodec};

pub(crate) mod batch;
mod modify;
pub(crate) mod read;
mod write;

use std::{result::Result as StdResult, sync::atomic::Ordering};

use wdev::Device;
use wrecord::record_size;
pub(crate) use write::CopyToTailOutcome;
pub use write::RmwGrow;

use crate::{error::Result, session::StoreSession, store::StoreEvent};

/// 内存读驱动环终态（[`StoreSession::drive_mem_read`] 出口：`RETRY_LATER` 的
/// 刷新重试在驱动环内部闭环，绝不外漏；对标 C# 会话层
/// HandleOperationStatus.cs:HandleOperationStatus 的「Refresh the epoch and retry」单点）
pub(super) enum MemDrive<R> {
  /// 内存阶段闭环：`Some` 为命中值，`None` 为确认不存在（NOTFOUND）
  Done(Option<R>),
  /// 存在磁盘候选（RECORD_ON_DISK）：候选按新版本优先降序透传
  OnDisk(windex::CandidateAddresses),
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
            self.notify_write_listener(key, val, is_tombstone)?;
          }
          return Ok(addr);
        }
        Err(WhlogError::PageNotReady(page_id)) => {
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
    // 复活唯一启用门（对标 C# BlockAllocate.cs:57 的 `RevivificationManager.IsEnabled`）：
    // 未启用（--reviv 关）与暂停窗口（检查点封印 / 迁移搬迁）合一，下游不再并列配置开关
    if self.store.reviv_pool.is_enabled() {
      let rec_size = record_size(key.len(), val.len());
      // 优先从 FreeRecordPool 提取最适配的空闲槽位并就地复活写入
      if let Some((free_addr, slot_size)) = self
        .store
        .reviv_pool
        .take(rec_size as u32, self.store.min_revivifiable_address())
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
          self.notify_write_listener(key, val, is_tombstone)?;
          return Ok(Ok((free_addr, slot_size)));
        } else if self.store.hlog.is_mutable(free_addr) {
          // 若临时写入失败且槽位仍在可变区，归还至复活池防槽位丢失
          // （门槛重取最新下限：写入失败多半因水位已推进，旧下限会误留不可复活槽位）
          self
            .store
            .reviv_pool
            .put(free_addr, slot_size, self.store.min_revivifiable_address());
        }
      }
    }

    match self.store.hlog.append(key, val, prev_addr, is_tombstone) {
      Ok(addr) => {
        self.notify_write_listener(key, val, is_tombstone)?;
        Ok(Ok((addr, record_size(key.len(), val.len()) as u32)))
      }
      Err(WhlogError::PageNotReady(page_id)) => Ok(Err(page_id)),
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
  pub(super) fn notify_write_listener(
    &self,
    key: &[u8],
    val: &[u8],
    tombstone: bool,
  ) -> Result<()> {
    if self.store.event_sink.get().is_none()
      || self.store.aof_listeners_paused.load(Ordering::Relaxed)
    {
      return Ok(());
    }
    if self.store.purge_suppress.load(Ordering::Relaxed) == self as *const Self as usize {
      return Ok(());
    }
    // 旁路记录快速分流：先快速判别 tag，仅当为 TTL 或 ETag 时才完整解码用户键
    if let Some(tag) = NamespaceDbCodec::decode_tag(key) {
      if tag == KeyTag::Ttl {
        if let Ok((ns, db, _, user_key)) = NamespaceDbCodec::decode_tagged_key(key) {
          let expire = (!tombstone).then(|| I64Codec::decode(val)).flatten();
          self.store.emit_event(StoreEvent::TtlWrite {
            ns,
            db,
            key: user_key,
            expire_at: expire,
          })?;
          return Ok(());
        }
      } else if tag == KeyTag::Etag
        && let Ok((ns, db, _, user_key)) = NamespaceDbCodec::decode_tagged_key(key)
      {
        let etag = (!tombstone).then(|| I64Codec::decode(val)).flatten();
        self.store.emit_event(StoreEvent::EtagWrite {
          ns,
          db,
          key: user_key,
          etag,
        })?;
        return Ok(());
      }
    }
    self.store.emit_event(StoreEvent::Write {
      key,
      val,
      tombstone,
    })
  }

  /// 当环形缓冲区发生翻转并遇到槽位尚未驱逐时，异步刷盘并推进 HeadAddress
  ///
  /// 读侧上界（先封再刷）单源在 whlog 刷盘内核：[crate::store::WedbStore::flush_pages_range]
  /// 之下、发起任何设备写入之前，内核先把只读线推进到本轮刷盘上界（即
  /// `page_start_address(target_evict_page + 1)`，与本函数显式封印同值）并等纪元排空，
  /// 使上界以下再无在途编码——追加协议「CAS 发布 tail → 裸写 encode_at」下「tail 已越过
  /// 页界」只说明物理空间分配完成，不说明迟到编码已落笔；缺此屏障会把全零/半截记录写入
  /// 设备并让 `flushed_until` 越过该区间，此后该页不再重刷、驱逐时槽位被直接回收，
  /// 记录在设备上永久缺失。本函数的显式封印严格先于刷盘（对标 C#
  /// AllocatorBase.cs:ShiftAddressesWithWait 的「ShiftReadOnlyAddressWithWait →
  /// 等 FlushedUntil 达标 → ShiftHeadAddress」次序），并携带 wkv 侧的复活池联动清扫
  /// （`store::shift_read_only_address`）。
  ///
  /// 前台驱逐不做硬件 sync（对标 C# AllocatorBase.cs:OnPagesClosed——页关闭仅释放内存，
  /// 持久化由异步刷盘管线承担）：本方法只保证页面写入设备（页缓存/提交队列）并推进
  /// head_address 内存水位；硬件持久化屏障由 GroupCommit 流水线（FlushStep 的
  /// device.sync + synced_until 推进）与检查点 flush_all 统一承担，恢复一致性边界
  /// 恒为 synced_until，与驱逐时机无关。在 compio 单线程 reactor 上同步 await 硬件
  /// fsync 会阻塞同线程全部并发任务，故必须剥离。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:OnPagesClosed
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
    let min_evicted_addr = self
      .store
      .hlog
      .config
      .page_start_address(target_evict_page + 1);

    // 关键：在执行页面刷盘与等待 SafeHeadAddress 期间，临时挂起当前会话的纪元保护区，
    // 彻底消除长时间磁盘 I/O 和 safe_head 自旋钉住纪元、阻塞全系统页回收的隐患
    // （严格对标 C# Tsavorite IO/Flush 期间的 UnsafeSuspendThread 协议；
    // EpochSuspendGuard 单点定义于 wepoch，构造即按重入深度逐层退出保护区）
    let _suspend_guard = EpochSuspendGuard::new(&self.participant);

    // 先封：只读线推过待驱逐窗口，刷盘内核随后等其排空方才落笔写入设备
    self.store.shift_read_only_address(min_evicted_addr);

    if start_page <= target_evict_page {
      self
        .store
        .flush_pages_range(start_page, target_evict_page)
        .await?;
    }

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
  /// 冷读分派唯一单点：磁盘区免纪元、内存驻留持短守卫，调用方勿自持纪元
  pub async fn read_record(&self, addr: u64) -> Result<whlog::RecordOutput> {
    if self.store.hlog.is_on_disk(addr) {
      self
        .store
        .hlog
        .read_disk_record(addr)
        .await
        .map_err(Into::into)
    } else {
      let _guard = self.enter_gated();
      self.store.hlog.read_record(addr).await.map_err(Into::into)
    }
  }
}
