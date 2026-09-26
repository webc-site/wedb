//! 纯引擎 KV 面（对标 C# Garnet ClientSession 的 Upsert/Read/Delete/RMW 快慢路径）
//!
//! 物理键操作：一切 `*_raw` 与 unprotected 变体、内存直读内核（FindTag 单点探针 +
//! TraceBackForKeyMatch 反向回溯 + 多候选扫描）、磁盘冷读回退、批量预取读、
//! Record Elision 删除与盲墓碑追加。用户键便捷层经 [`keys`](super::keys) 编码后
//! 落到同一套物理路径。

use wepoch::EpochSuspendGuard;
use whlog::Error as WhlogError;
use wval::{I64Codec, KeyTag, NamespaceDbCodec};

use crate::error::Error;

pub(crate) mod batch;
mod modify;
pub(crate) mod read;
mod write;

use std::{result::Result as StdResult, sync::atomic::Ordering};

use wbase::future::yield_now;
use wdev::Device;
use wrecord::{ValSrc, record_size};
pub(crate) use write::CopyToTailOutcome;
pub use write::RmwGrow;

use crate::{error::Result, session::StoreSession, store::StoreEvent};

/// 同步快路径降级哨兵（全仓单源，对标 C# OperationStatus.RETRY_LATER /
/// RECORD_ON_DISK 以相异类型承载相异信号的口径——rust 以专有名常量收口
/// `u64::MAX` 魔法值）：tag 级写回原语以 `Ok(Err(DEGRADE_ASYNC))` 指示调用方
/// 放弃同步段、整体转异步闭环路径，按调用点承载三种互异语义——
/// 1. SET/DEL 族 tag 级原语（`try_upsert_tag_sync_unprotected_with_prefix` /
///    `try_delete_sync_unprotected_with_prefix`）：Meta 记录在场（复合对象 /
///    分层树 / RangeIndex 存根）或迁移 claim 在册，同步快路径禁做第二套裸
///    清退，降级完整异步路由；
/// 2. 底层删除内核（`delete_or_take_raw_sync_unprotected_with`）：链条伸入
///    磁盘区（冷数据需磁盘确认）或可变区取删无法同临界区闭环；
/// 3. RMW 写回（`RmwWindow::try_rmw_sync`）：TTL 记录有磁盘候选、Meta 分层存根
///    在场（重建臂经 SET 同步内核降级）或清退遭环形页翻转，同步段无法裁决。
///
/// 三义共用一值的收敛依据：tag 级调用方（wnode 快路径 / 批量写 / append.rs
/// 慢路径循环）一律按「降级全异步」分岔，真实精确 page_id 恒走独立通道
/// （`Ok(Err(page_id))` 原样透传 [`StoreSession::evict_pages_for`]），当前无活
/// 路径误喂；evict_pages_for 入口的防御断言把未来误喂从静默灾难（old_page
/// 溢出回绕推飞只读线）转为显性错误。
pub(crate) const DEGRADE_ASYNC: u64 = u64::MAX;

/// 单条记录探针分类（主链回溯与多候选扫描共用，严格对照
/// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:105-131
/// 单遍分类：密封记录按 C# IsValidTracebackRecord 口径参与键比对，命中即降级重试）
#[derive(Debug)]
pub(super) enum ReadProbeResult<T> {
  /// Tag 碰撞未匹配：携带 `prev_address` 供回溯与磁盘候选链收集
  Miss(u64),
  Tombstone,
  /// 关闭/密封在途记录命中（严格对照 InternalRead.cs:118 IsClosedOrTombstoned →
  /// RETRY_LATER：刷新纪元后整链重试）
  Retry,
  Found(T),
}

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
  /// 纯追加原语（对标 C# AllocatorBase.TryAllocate 的纯尾部分配臂）：PageNotReady
  /// 时驱逐旧页重试，恒发写监听。测试预置与不走池取的宿主路径专用——复活池取臂
  /// 编排唯一入口是 [`Self::try_allocate_or_append_record_sync`]，本原语绝不池取
  pub async fn append_record(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
  ) -> Result<u64> {
    loop {
      match self.store.hlog.append(key, val, prev_addr, is_tombstone) {
        Ok((addr, ver)) => {
          self.notify_write_listener_with_version(key, val, is_tombstone, ver)?;
          return Ok(addr);
        }
        Err(WhlogError::PageNotReady(page_id)) => {
          self.evict_pages_for(page_id).await?;
        }
        Err(e) => return Err(e.into()),
      }
    }
  }

  /// 复活池取臂 + 尾部追加的异步编排单点（对标 C# TryCopyToTail.cs:33 以
  /// `AllocateOptions{recycle=true}` 统一经 TryAllocateRecord 的慢路径分配契约，
  /// BlockAllocate.cs:57-82）：池取失败自然回落尾部追加，行为严格超集；
  /// PageNotReady 驱逐旧页后重试至成功
  ///
  /// `min_eligible_addr` 由调用方经 [`Self::reviv_chain_floor`] 按候选链首折算；
  /// copy-to-tail 慢路径全部消费者（删除/治愈内核、read 两条冷读晋升臂、紧缩
  /// 搬迁）共用本单点，杜绝旁路池取的日志空洞收敛失效。恒不发写监听（镜像点
  /// 收口见 [`Self::try_allocate_or_append_record_sync`]），用户写效果的 AOF
  /// 镜像由 copy-to-tail 提交方在 CAS 挂载成功后联动发出
  pub(crate) async fn allocate_record(
    &self,
    key: &[u8],
    val: &[u8],
    prev_addr: u64,
    is_tombstone: bool,
    min_eligible_addr: u64,
  ) -> Result<(u64, u32, i64)> {
    loop {
      match self.try_allocate_or_append_record_sync(
        key,
        val,
        prev_addr,
        is_tombstone,
        min_eligible_addr,
      )? {
        Ok(ok) => return Ok(ok),
        Err(page_id) => self.evict_pages_for(page_id).await?,
      }
    }
  }

  /// 本次申请的复活槽位链首下界（严格对标 C# BlockAllocate.cs:57-62 的
  /// minRevivAddress 链首抬升）：`(chain_head + 1).max(全局复活水位)`，严格保证
  /// 复活槽位地址 > 旧链首，prev_address 恒单调递减、杜绝哈希碰撞链逆向成环。
  /// 快路径 `RetryAlloc::chain_floor` 与 copy-to-tail 慢路径四臂共用本单点，
  /// 杜绝两套口径分叉
  #[inline]
  pub(crate) fn reviv_chain_floor(&self, chain_head: u64) -> u64 {
    chain_head
      .saturating_add(1)
      .max(self.store.min_revivifiable_address())
  }

  /// 纯同步尝试分配空闲槽位或追加 Tail（严格对标 C# Garnet BlockAllocate.cs & InternalUpsert.cs）
  /// - Ok(Ok((addr, frame_size, ver))): 纯内存就地分配或追加成功，frame_size 为本帧实际
  ///   足印（复活池槽位为整槽扣除切出归池 pad 块后的本帧尺寸，尾部追加为对齐逻辑
  ///   尺寸，对标 C# AllocatedSize 含 FillerWords 口径，供 SaveAllocationForRetry
  ///   复用/弃置与败帧归池时精确登记——绝不得覆盖已切出归池的 pad 区间）；ver 为
  ///   分配成功点单读传导的 AOF 版本戳（读点下移，见 [`whlog::HybridLog::append`]
  ///   方法文档），与记录头纪元位同源同点
  /// - Ok(Err(page_id)): 环形缓冲区翻转（PageNotReady），需调用者执行异步落盘与驱逐
  ///
  /// 池取臂对标
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:TryTakeFreeRecord
  /// （启用门校验 + `min_revivifiable_address` 资格窗 + 取槽就地复活写入三段
  /// 协议一致；槽位物理覆写由 whlog `revivify_record_at` 承接）。本函数是全部
  /// 写路径（快路径 upsert/delete 与 copy-to-tail 慢路径四臂）复活池取的唯一编排入口
  ///
  /// `min_eligible_addr` 为本次申请槽位下界（对标 C# BlockAllocate.cs:59-62 将
  /// minRevivAddress 抬升至链首 `hei.Address` 的口径，由调用方按脱钩场景经
  /// [`Self::reviv_chain_floor`] 折算：整链清空脱钩 = 全局复活水位；否则 = 链首
  /// 地址 + 1）。池对低于该线的槽位仅跳过让位不清零（对标 C# FreeRecordPool.cs:
  /// TryTake 低于 minAddress 仅返回 false），严格保证复活槽位地址 > 旧链首，
  /// 杜绝哈希碰撞链 prev_address 逆向成环
  ///
  /// 镜像点收口（票 zcode-r34-writekernel 条目三，对标 C# PostInitialWriter /
  /// PostInitialDeleter 在记录提交后才置 NeedAofLog 的单一「生效后镜像」次序）：
  /// 本原语恒不发写监听——分配成功 ≠ 索引提交，盲追加臂的 AOF 镜像由提交方在
  /// `hei.try_cas` 成功后恰发一次（CAS 败帧暂存复用/弃置均不重发），copy-to-tail
  /// 慢路径由 `cas_mount_copied_frame` 成功后联动发出。内部治愈/晋升/搬迁帧本就
  /// 旁路镜像（物理布局优化而非用户写效果），收口后快慢路径单序无分叉
  pub(super) fn try_allocate_or_append_record_sync<V: ValSrc + ?Sized>(
    &self,
    key: &[u8],
    val: &V,
    prev_addr: u64,
    is_tombstone: bool,
    min_eligible_addr: u64,
  ) -> Result<StdResult<(u64, u32, i64), u64>> {
    // 复活唯一启用门（对标 C# BlockAllocate.cs:57 的 `RevivificationManager.IsEnabled`）：
    // 未启用（--reviv 关）与暂停窗口（检查点封印 / 迁移搬迁）合一，下游不再并列配置开关
    if self.store.reviv_pool.is_enabled() {
      let rec_size = record_size(key.len(), val.val_len());
      // 版本推进窗口期取槽下界抬升（1:1 对标 C# BlockAllocate.cs:71-77：`Ctx.IsInV1`
      // 期把 minRevivAddress 抬升至本轮检查点 startLogicalAddress，自由表绝不发出
      // 地板之下槽位）：池槽若在窗口期于地板之下被整头覆写为携带纪元位的新记录，
      // 该效果落进快照物理收录面（addr < index_start 不在 undoNextVersion 回滚窗）
      // 而 AOF 条目版本戳恒 > covered 必重放——同一效果恰双算。抬升后低于地板的
      // 槽位仅跳过让位不清零（待窗口关闭后照常取用，淘汰仍归水位线与 purge）。
      // 单一抬升点收口于本取槽下界（与 C# 同位：重试复用臂 BlockAllocate.cs:64
      // 维持抬升前下界口径，入池侧不设对称门），链内复活臂与原位冻结门另经
      // whlog 窗口谓词承接，无第二套机制
      let min_eligible_addr = if self.store.is_version_shift_open() {
        min_eligible_addr.max(self.store.version_shift_floor())
      } else {
        min_eligible_addr
      };
      // 优先从 FreeRecordPool 提取最适配的空闲槽位并就地复活写入
      // （双下界：min_revivifiable_address 为全局永久淘汰水位，min_eligible_addr 为
      // 本次申请链首下界，二者在池内分档处置——低于水位清零淘汰、低于下界仅跳过）
      if let Some((free_addr, slot_size)) = self.store.reviv_pool.take(
        rec_size as u32,
        self.store.min_revivifiable_address(),
        min_eligible_addr,
      ) {
        // 池取成功点紧贴单读（读点下移）：复活槽位的纪元位与 AOF 版本戳同源
        // 同点、反映落笔时刻窗口态（一致性契约见
        // [`whlog::HybridLog::version_shift`]），ver 随分配结果向上传导
        let word = self.store.hlog.version_shift_word();
        match self.store.hlog.revivify_record_at(&whlog::RevivifyArgs {
          addr: free_addr,
          slot_size: slot_size as usize,
          key,
          val,
          prev_addr,
          is_tombstone,
          in_new_version: word & whlog::VERSION_SHIFT_OPEN_BIT != 0,
        }) {
          Ok(pad) => {
            // 就地复活切出的 Pad 剩余块归还入池（对标 C# SplitOverflowingFiller 切出
            // 冗余空间后 TryTransferToFreeList 归还空闲列表，RecordDataHeader.cs:509、
            // Helpers.cs:124）：复活槽位已从原哈希链脱钩，不归池即成物理页内孤儿死内存
            if let Some((pad_addr, pad_size)) = pad {
              self.store.transfer_to_reviv_pool(pad_addr, pad_size);
            }
            // 本帧实际足印：整槽扣除切出归池的 pad 块（无切出块即整槽吃满），
            // 败帧归池与暂存复用据此绝不覆盖已属池中他人的 pad 区间
            let frame = match pad {
              Some((_, pad_size)) => slot_size - pad_size,
              None => slot_size,
            };
            return Ok(Ok((free_addr, frame, (word & whlog::VERSION_MASK) as i64)));
          }
          Err(_) => {
            // 若临时写入失败且槽位仍在可变区，归还至复活池防槽位丢失
            // （门槛重取最新下限：写入失败多半因水位已推进，旧下限会误留不可复活槽位）
            if self.store.hlog.is_mutable(free_addr) {
              self.store.transfer_to_reviv_pool(free_addr, slot_size);
            }
          }
        }
      }
    }

    match self.store.hlog.append(key, val, prev_addr, is_tombstone) {
      Ok((addr, ver)) => Ok(Ok((
        addr,
        record_size(key.len(), val.val_len()) as u32,
        ver,
      ))),
      Err(WhlogError::PageNotReady(page_id)) => Ok(Err(page_id)),
      Err(e) => Err(e.into()),
    }
  }

  /// 触发写监听端口（未注入则零开销跳过；purge 链窗口内本会话通知被抑制）
  ///
  /// 原位生效臂专用：AOF 版本戳在生效点后单读（对齐
  /// [`WedbStore::emit_event`] 的分发时点读——原位改写不重新编码记录头，
  /// 纪元位恒为记录落笔时自带的位，本口只补版本戳）。
  ///
  /// purge 链抑制（会话私有窗位 [`StoreSession::purge_window`]，机制与置位见
  /// [`crate::ttl::PurgeNotifyGuard`]）：purge_expired 窗口内仅本会话的物理写镜像
  /// 被跳过——TTL 记录 + 数据两条墓碑折叠为单条确定性逻辑条目（对标 Garnet
  /// `RespInputFlags.Deterministic` 单条目语义）；窗位随会话销毁自然消亡、
  /// 与他会话无从串线，其他会话（含并发同键写与内置 GC 会话）的通知绝不受影响。
  #[inline]
  pub(super) fn notify_write_listener(
    &self,
    key: &[u8],
    val: &[u8],
    tombstone: bool,
  ) -> Result<()> {
    let (_, ver) = self.store.write_window_snapshot();
    self.notify_write_listener_with_version(key, val, tombstone, ver)
  }

  /// 携传导版本戳的写监听（追加域专用，[`Self::notify_write_listener`] 的对位）
  ///
  /// `ver` 为分配成功点单读传导的版本域（hlog append 返回值或池取复活臂紧贴
  /// 单读），与记录头纪元位同源一次读取值——AOF 条目版本戳绝不二次采样。
  #[inline]
  pub(super) fn notify_write_listener_with_version(
    &self,
    key: &[u8],
    val: &[u8],
    tombstone: bool,
    ver: i64,
  ) -> Result<()> {
    if self.store.event_sink.get().is_none()
      || self.store.aof_listeners_paused.load(Ordering::Relaxed)
    {
      return Ok(());
    }
    if self.purge_window.load(Ordering::Relaxed) {
      return Ok(());
    }
    // 旁路记录快速分流：先快速判别 tag，仅当为 TTL 或 ETag 时才完整解码用户键
    if let Some(tag) = NamespaceDbCodec::decode_tag(key) {
      if tag == KeyTag::Ttl {
        if let Ok((ns, db, _, user_key)) = NamespaceDbCodec::decode_tagged_key(key) {
          let expire = (!tombstone).then(|| I64Codec::decode(val)).flatten();
          self.store.emit_event_with_version(
            ver,
            self.aof_session_id,
            StoreEvent::TtlWrite {
              ns,
              db,
              key: user_key,
              expire_at: expire,
            },
          )?;
          return Ok(());
        }
      } else if tag == KeyTag::Etag
        && let Ok((ns, db, _, user_key)) = NamespaceDbCodec::decode_tagged_key(key)
      {
        let etag = (!tombstone).then(|| I64Codec::decode(val)).flatten();
        self.store.emit_event_with_version(
          ver,
          self.aof_session_id,
          StoreEvent::EtagWrite {
            ns,
            db,
            key: user_key,
            etag,
          },
        )?;
        return Ok(());
      }
    }
    self.store.emit_event_with_version(
      ver,
      self.aof_session_id,
      StoreEvent::Write {
        key,
        val,
        tombstone,
      },
    )
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
    // 降级哨兵防错（票 zcode-r34-writekernel 条目四）：DEGRADE_ASYNC 是 tag 级
    // 降级信号，误喂本内核时 old_page = u64::MAX - num_pages 为巨值，
    // target_evict_page 取 max 后同样巨大，page_start_address 回绕出巨地址，
    // shift_read_only_address 会把只读线推过 tail（全表记录被判磁盘区）——
    // debug 断言 + 显性拒绝，把误喂从静默灾难转为错误上抛
    debug_assert!(page_id != DEGRADE_ASYNC, "tag 级降级哨兵严禁喂入页驱逐内核");
    if page_id == DEGRADE_ASYNC {
      return Err(Error::EvictSentinel);
    }
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

    loop {
      let flushed_until = self.store.hlog.flushed_until_address();
      if flushed_until >= min_evicted_addr {
        break;
      }
      let start_page = self.store.hlog.config.page_id(flushed_until);
      if start_page > target_evict_page {
        break;
      }
      match self
        .store
        .flush_pages_range(start_page, target_evict_page)
        .await
      {
        Ok(()) => break,
        Err(Error::HLog(WhlogError::PageNotReady(_))) => {
          let latest_flushed = self.store.hlog.flushed_until_address();
          if latest_flushed >= min_evicted_addr {
            break;
          }
          if latest_flushed > flushed_until {
            continue;
          }
          yield_now().await;
        }
        Err(e) => return Err(e),
      }
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
  ///
  /// 上下文层包装入口同挂此处（rust 一臂承接）：
  /// libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:ContainsKeyInMemory
  #[inline]
  pub async fn contains_key_raw(&self, key: &[u8]) -> Result<bool> {
    Ok(self.read_raw_with(key, |_| ()).await?.is_some())
  }

  /// 在当前会话纪元保护下按逻辑地址直接读取记录（对标 Tsavorite ReadAtAddress）
  /// 磁盘区记录走免纪元纯设备路径，内存驻留区持短守卫保护
  /// 冷读分派唯一单点：磁盘区免纪元、内存驻留持短守卫，调用方勿自持纪元
  ///
  /// C# 上下文层 ReadAtAddress 入口族在本 rust 单点的折叠映射（多态上下文已被
  /// 统一会话消除，按地址读只有此一臂；内部实现 InternalReadAtAddress 已单挂
  /// [`StoreSession::read_raw_with`]）：
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:ReadAtAddress
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:ReadAtAddress
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ITsavoriteContext.cs:ReadAtAddress
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:ReadAtAddress
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalConsistentReadContext.cs:ReadAtAddress
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalUnsafeContext.cs:ReadAtAddress
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/UnsafeContext.cs:ReadAtAddress
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
