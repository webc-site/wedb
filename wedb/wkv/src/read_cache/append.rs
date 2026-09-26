//! 只读缓存记录追加：CAS 预留 + 页内编码 + 哈希索引挂载
//!
//! 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToReadCache.cs:TryCopyToReadCache

use std::{
  ptr::eq,
  slice::from_raw_parts_mut,
  sync::{
    Arc,
    atomic::{
      AtomicU64,
      Ordering::{AcqRel, Acquire, Release},
    },
  },
};

use wbase::{addr::with_read_cache, backoff::Backoff};
use windex::HashIndex;
use wrecord::{RecordHeader, encode_to_slice, record_size};

use super::{INFLIGHT_CLOSED, INFLIGHT_COUNT_MASK, ReadCache};

impl ReadCache {
  /// 向 ReadCache 追加一条只读缓存记录并以链首插入协议 CAS 挂载哈希索引（严格对标
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToReadCache.cs:TryCopyToReadCache）
  ///
  /// `min_valid_addr` 为主日志截断线（调用方传 `store.begin_address()`）：链头定位时
  /// 已被日志回收的死槽位实时清退，与读路径 FindTag 同口径。
  ///
  /// 返回打上 `READ_CACHE_BIT` 的虚拟地址（记录已编码且已挂载）；若未启用、
  /// 单记录超出一页大小、键不在索引或链头 CAS 落败（并发写已 detach）则返回
  /// None——落败时记录被原子作废（对标 C# SetNewRecordInvalid），读取方按未命中
  /// 降级主日志。败帧作废即 C# 静态弃录原语的承接点：
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:ReadCacheAbandonRecord
  ///（rust 以 `RecordHeader::set_invalid_atomic` 单字作废，previousAddress 交由
  /// cleanse 跳过语义兜底，无需 kTempInvalidAddress 中转）。
  ///
  /// # 回写窗撕裂与在途直读双重闭环（原 r1 观察项 + epoch 屏障票）
  ///
  /// CAS 预留 `[curr_tail, new_tail)` 切片与随后的字节编码之间存在无锁窗口。
  /// 原实现下极端停滞（SIGSTOP / VM 暂停恰好落在窗口内且持续超过整环回绕周期）
  /// 时，唤醒后的编码会落入已被换装复用的页槽污损新近记录，且 cleanse_page 扫描
  /// 遇零切片提前 break 截断清洗、令页尾已完成记录的哈希索引悬垂断链。
  /// 现以 [`Self::page_inflight`] 状态字闭环：编码与索引挂载全程持在途注册，
  /// 换页侧 `fetch_or(CLOSED)` 两阶段关闭被复用槽——置位后新注册必被拒绝、
  /// 存量注册计入计数，等待清零即保证页上全部编码 + 挂载完成。发布顺序收敛为
  /// 关闭 → 清洗 → 清页 → tail 发布，滞留编码者的切片恒落在仍有效的旧代页内。
  ///
  /// 在途无锁直读读者由纪元延迟关闭屏障兜底（对标 AllocatorBase.cs:ShiftHeadAddress
  /// 的 `epoch.BumpCurrentEpoch(() => OnPagesClosed)`）：回绕换页（发生驱逐的换页
  /// 事件）武装 head/关闭水位后本方法返回 None——本条记录不晋升，旧页的关闭序列
  /// 挂入纪元延迟队列，待全部旧纪元在途读者退出后才清零/换装槽位；注册由调用方
  /// 在出借期安全点经 [`Self::pump_close_barrier`] 完成，排空等待绝不发生在
  /// turn_lock 内。正向首轮换页（槽位无旧代页、无在途读者）维持内联换装。
  pub fn append(
    &self,
    key: &[u8],
    val: &[u8],
    index: &HashIndex,
    min_valid_addr: u64,
  ) -> Option<u64> {
    if !self.is_enabled {
      return None;
    }

    // 记录对齐逻辑尺寸（含隐式对齐填充，与主日志记录头 8 字节对齐不变式一致）
    let rec_size = record_size(key.len(), val.len());
    if rec_size > self.page_size {
      return None; // 单记录超出页容量，直接跳过缓存
    }

    // 链首插入契约的槽位句柄单点定位（C# stackCtx.hei 由读路径 FindTag 预载，
    // 分配重试不刷新——陈旧快照的 TryCAS 自然落败作废）：无论哈希槽位当前指向
    // 本键旧版本、既有 ReadCache 记录还是 Tag 碰撞键记录，新记录 previousAddress
    // 恒取槽位链头地址并 TryCAS 抢占链头，整条旧链完整串接其后，哈希链不断裂。
    // 碰撞链深层键（其地址仅存于前驱 prev_address、从不在槽位中）由此同样可挂载
    let mut hei =
      index.find_tag_entry_by_hash_with_min_addr(HashIndex::hash_key(key), min_valid_addr)?;

    let mut backoff = Backoff::new();
    loop {
      let curr_tail = self.tail_address.load(Acquire);
      let page_offset = self.buffer.offset_in_page(curr_tail);
      let remaining = (self.page_size - page_offset) as u64;

      if (rec_size as u64) <= remaining {
        let slot = self.buffer.slot_for_address(curr_tail);
        // 预注册在途编码（先于切片抢占 CAS）：换页侧两阶段关闭以注册可见为
        // 等待前提，滞留编码者由此被挡在页槽换装之外
        if self.page_inflight[slot].fetch_add(1, Release) & INFLIGHT_CLOSED != 0 {
          // 目标槽已被换页侧关闭，快照必然过期（tail 已推进），退订重试
          self.page_inflight[slot].fetch_sub(1, Release);
          backoff.snooze();
          continue;
        }
        let new_tail = curr_tail + rec_size as u64;
        if self
          .tail_address
          .compare_exchange_weak(curr_tail, new_tail, AcqRel, Acquire)
          .is_ok()
        {
          let rc_addr = with_read_cache(curr_tail);
          // SAFETY: 当前线程通过 CAS 独占抢占 [page_offset, page_offset + rec_size) 内存切片，
          // 且换页侧经 in-flight 状态字确认本线程挂载完成（退订）后才换装该页槽，
          // 切片独占成立；该页在初始或换页时已置零，多线程在互不重叠的切片内并发编码，完全零锁。
          unsafe {
            let page_ptr = self.buffer.raw_page_ptr_mut(slot);
            let dest = from_raw_parts_mut(page_ptr.add(page_offset), rec_size);
            // 读缓存记录恒不携带版本纪元位：ReadCache 为纯易失面，快照写出前
            // sanitize_data_slot 已将 RC 条目折回主日志真实地址，恢复扫描域内
            // 绝无 RC 记录，不参与 undoNextVersion 回滚判定
            if encode_to_slice(dest, hei.address(), key, val, false, false).is_err() {
              // 编码失败（非法键长/地址超界等）：切片作废，退订后按未命中降级
              self.page_inflight[slot].fetch_sub(1, Release);
              return None;
            }
            // 编码完成后立即链首 CAS 挂载（严格对标 TryCopyToReadCache 的
            // hei.TryCAS 线性化点：挂载是读取方可见性的唯一入口，并发写 detach、
            // 并发读晋升挂载均经同一槽位 CAS 仲裁）。退订先于挂载会重开「清洗
            // 早于挂链」乱序窗，故挂载必须在在途计数覆盖内完成
            if !hei.try_cas(rc_addr) {
              // 链头被并发抢占：对标 C# SetNewRecordInvalid 原子置 SEALED 位作废
              // 本记录——cleanse 按关闭记录跳过（未挂载记录无需哈希链恢复），读侧
              // 按未命中降级主日志
              RecordHeader::set_invalid_atomic(&*page_ptr.add(page_offset).cast::<AtomicU64>());
              self.page_inflight[slot].fetch_sub(1, Release);
              return None;
            }
          }

          self.page_inflight[slot].fetch_sub(1, Release);
          return Some(rc_addr);
        }

        self.page_inflight[slot].fetch_sub(1, Release);
        // CAS 冲突自适应退避，降低 CPU 流水线惩罚与总线锁颠簸
        backoff.snooze();
      } else {
        // 页内剩余空间不足，获取轻量换页互斥锁跳至下一页开头
        let _lock = self.turn_lock.lock();
        let curr_tail2 = self.tail_address.load(Acquire);
        let page_offset2 = self.buffer.offset_in_page(curr_tail2);
        let remaining2 = (self.page_size - page_offset2) as u64;

        if (rec_size as u64) <= remaining2 {
          // 并发换页方已推进 tail，本页空间已足，退锁按页内路径重试
          continue;
        }
        let next_page_start = curr_tail2 + remaining2;
        let next_page_id = self.buffer.page_of_address(next_page_start);
        let slot = self.buffer.page_idx(next_page_id);

        // 环形回绕时被驱逐旧代页的实际末尾边界：目标槽（slot = next_page_id %
        // num_pages）当前承载的旧代页号为 next_page_id - num_pages，其页末地址
        // = (next_page_id - num_pages + 1) * page_size；正向首轮（页号未越过
        // num_pages）不发生驱逐，归 None
        let evicted_page_end = (next_page_id >= self.num_pages as u64)
          .then(|| (next_page_id - self.num_pages as u64 + 1) * self.page_size as u64);

        if let Some(end) = evicted_page_end {
          // 【纪元延迟关闭屏障·武装拍】回绕换页绝不就地清洗/清零旧页——严格对标
          // libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftHeadAddress
          // 的 `epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress))`：旧页的
          // 关闭序列整体挂入纪元延迟队列，待全部注册前受保护的在途读者退出后，由
          // 收割线程执行 [`Self::close_pending_page`]。本调用栈可能处于主日志记录
          // 裸借用期内（`session/raw/read.rs:promote_immutable_read_hit`），而纪元
          // 动作注册会将本线程公布纪元刷至新纪元（wepoch help_drain 契约），故武装
          // 拍只做三件纯原子事即让路返回 None（本条晋升作废，对标 C# 驱逐窗内 copy
          // 操作 RETRY_LATER 由上层读重试；下一次读命中即重新晋升，无正确性损失）：
          // 1. head 推进至被驱逐旧代页实际末尾边界（保留原 MonotonicUpdate 口径与
          //    顺序：旧代页全段地址先严格小于 head，读侧 need_to_wait_for_eviction
          //    据此进入驱逐等待协议，绝不入场直读即将关闭的废页）；
          // 2. fetch_max 登记关闭边界水位（延迟动作执行时以冻结 tail 重算恒等式复核）；
          // 3. 置起 close_armed 边沿标志，待调用方在出借期安全点调
          //    [`Self::pump_close_barrier`] 完成纪元注册。
          self.head_address.fetch_max(end, Release);
          self.pending_close_until.fetch_max(end, Release);
          self.close_armed.store(true, Release);
          return None;
        }

        // ——正向首轮（目标槽从未承载过旧代页，绝无在途读者，可安全内联换装）——
        // 两阶段关闭目标槽：先置 CLOSED 位拒绝新注册（其后任何注册者必见位并
        // 退订重试），再等存量在途编码全部完成（计数清零）——滞留编码者（停滞
        // 横跨整环回绕，切片落在被驱逐页）由此先完成编码才放行清洗与换装，唤醒
        // 后的编码只能落入仍有效的旧代切片，绝不污损换装后的新页（对标 C#
        // AllocatorBase.cs:OnPagesClosed 的 epoch-gated 排空语义）
        if self.page_inflight[slot].fetch_or(INFLIGHT_CLOSED, AcqRel) & INFLIGHT_COUNT_MASK != 0 {
          let mut turn_backoff = Backoff::new();
          while self.page_inflight[slot].load(Acquire) & INFLIGHT_COUNT_MASK != 0 {
            turn_backoff.snooze();
          }
        }

        self.buffer.clear_page(next_page_id);
        // 最后发布 tail 再重置状态字：tail 发布后新快照全部落在新页（新注册者
        // 暂见 CLOSED 退订重试，重置后正常注册），被驱逐页自此不再有任何切片抢占
        self.tail_address.store(next_page_start, Release);
        self.page_inflight[slot].store(0, Release);
      }
    }
  }

  /// 纪元关闭屏障注册点（严格对标
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftHeadAddress
  /// 的 `epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress))`：head 推进后，
  /// 旧页关闭/清洗/清零全部作为纪元延迟动作执行，分配方本身绝不阻塞等待排空）
  ///
  /// 与 C# 的唯一形态差异：C# 允许受保护线程就地 BumpCurrentEpoch；本仓
  /// `wepoch::LightEpoch::bump_current_epoch_action` 注册尾随刷新本线程公布纪元
  /// （契约见 wepoch help_drain 注释：调用方不得跨刷新窗口持有旧纪元裸指针），
  /// 而换页武装点正处于记录裸借用闭包内，故注册后置到调用方出借期结束的安全点
  /// ——生产调入点为 `session/raw/read.rs` 的 `promote_immutable_read_hit` 收尾与
  /// `read_from_disk` 磁盘回填臂（均在借用闭包返回之后）。
  ///
  /// 幂等边沿：`close_armed` 以 swap 单点消费，未武装时零开销直返。纪元排空即
  /// 本屏障的等待本体：延迟动作只可能在全部注册前受保护线程（含 window.rs
  /// `PageView::Fast` 裸切片借入 disclose 闭包的无锁直读读者）刷新退出之后执行，
  /// 由此杜绝 clear_page 的 `fill(0)` 与新代 `encode_to_slice` 覆写与在途直读
  /// 切片的并发数据竞争。turn_lock 等待者绝不在锁内等纪元排空（注册与执行体
  /// 均不持 turn_lock 进入排空等待），mod.rs `page_inflight` 注释所规避的死锁
  /// 形态不复活。
  ///
  /// # grow 迁移窗双表恢复面（wkv-growsplit-rc-evicted-dead-slot-doublewrite）
  ///
  /// `old_index` 为注册时快照的扩容迁移源表（调用方取
  /// `store.resize.old_index.load_full()`，与活跃表 `index` 同拍捕获）。C# 扩容
  /// 时序（IndexResizeSMTask.cs:52 先翻 resizeInfo.version、:76 后 SplitAllBuckets）
  /// 决定切表后迁移期间新武装的清洗闭包持新表 B，而被驱逐记录所在分块尚未迁移
  /// 时 B 中无此键——evict_chain 的 find_tag 落空即 return，旧表 A 槽位的死 RC
  /// 地址无人恢复，迁移读到即把含 RC 位的死地址双写新表左右子桶（split.rs else
  /// 臂），该键后续读永久 Retry 耗尽预算报 LockTimeout。恢复面必须并覆迁移源表：
  /// 对捕获双表各跑一遍 cleanse_page（同表判等跳过；evict_chain 槽位 CAS 与
  /// set_invalid_atomic 幂等收敛，同表多一遍零危害）。非扩容期 `old_index` 恒
  /// None 零开销，扩容期多一遍旧表恢复为控制面动作，不渗数据面读路径。
  pub fn pump_close_barrier(
    self: &Arc<Self>,
    index: &Arc<HashIndex>,
    old_index: Option<&Arc<HashIndex>>,
  ) {
    if !self.close_armed.swap(false, AcqRel) {
      return;
    }
    let rc = Arc::clone(self);
    let idx = Arc::clone(index);
    // 注册时快照随闭包捕获（执行时再取可能已被 grow_index 收口清空，丢失旧表
    // 在场证据——切表前武装的清洗已由 grow_index 步骤 1b bump_and_wait 强制收割，
    // 危害窗恒为「切表后武装、闭包持新表」）
    let old = old_index.cloned();
    self
      .epoch
      .bump_current_epoch_action(move || rc.close_pending_page(&idx, old.as_deref()));
  }

  /// 旧页关闭序列（对标
  /// libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:OnPagesClosed →
  /// OnPagesClosedWorker → OnPagesClosedWorkerCore 在纪元排空完成后的同步化收敛体，
  /// 由 [`Self::pump_close_barrier`] 注册的延迟动作在收割线程上执行）
  ///
  /// tail 仅由本序列发布：武装后、关闭前页内 CAS 预留恒被页满分支挡回，故执行时
  /// 重算的换页几何与武装时恒等（不携带陈旧参数，杜绝多轮武装错配）。水位发布序
  /// 保持 C# 口径：SafeHeadAddress 先于 ClosedUntilAddress（`safe <= closed <=
  /// head` 不变量），被 [`super::window::ReadCache::spin_wait_until_record_is_closed`]
  /// 自旋的读者以 ClosedUntil 为解除判据回链头重探。
  fn close_pending_page(&self, index: &HashIndex, old_index: Option<&HashIndex>) {
    let want = self.pending_close_until.load(Acquire);
    if self.safe_head_address.load(Acquire) >= want {
      // 同边界重复动作（边沿标志竞态下的罕见双注册）：前驱已完成，幂等短路
      return;
    }
    let _lock = self.turn_lock.lock();
    let curr_tail = self.tail_address.load(Acquire);
    let page_offset = self.buffer.offset_in_page(curr_tail);
    let next_page_start = curr_tail + (self.page_size - page_offset) as u64;
    let next_page_id = self.buffer.page_of_address(next_page_start);
    let slot = self.buffer.page_idx(next_page_id);
    let end = if next_page_id >= self.num_pages as u64 {
      (next_page_id - self.num_pages as u64 + 1) * self.page_size as u64
    } else {
      0
    };
    let barrier = want.max(end);
    self.head_address.fetch_max(barrier, Release);
    if self.buffer.is_page_loaded(next_page_id) {
      // 防御护栏（冻结 tail 不变量下理论不可达）：目标槽已被换装发布，仅补水位
      self.safe_head_address.fetch_max(barrier, Release);
      self.closed_until_address.fetch_max(barrier, Release);
      return;
    }

    // 纪元排空已完成（本体即延迟动作）：在途无锁直读读者全部退出。先标定
    // SafeHead 对位水位，再两阶段关闭目标槽：置 CLOSED 拒绝新注册（其后注册者
    // 必见位并退订重试），等存量在途编码（滞留写者）全部清零——滞留编码者由此
    // 先完成编码才放行清洗与换装
    self.safe_head_address.fetch_max(barrier, Release);
    if self.page_inflight[slot].fetch_or(INFLIGHT_CLOSED, AcqRel) & INFLIGHT_COUNT_MASK != 0 {
      let mut turn_backoff = Backoff::new();
      while self.page_inflight[slot].load(Acquire) & INFLIGHT_COUNT_MASK != 0 {
        turn_backoff.snooze();
      }
    }

    // 若发生环形回绕，在清空旧槽位前扫描被驱逐的旧页，原子更新哈希索引恢复指向主日志地址。
    // grow 迁移窗双表并洗（见 [`Self::pump_close_barrier`] 文档）：活跃表恢复之后，
    // 对注册时捕获的迁移源旧表再跑一遍——被驱逐记录未迁入新表时仅新表清洗会漏覆
    // 旧表槽位，死 RC 地址随迁移双写落库；同表判等跳过，evict_chain 幂等收敛
    if next_page_id >= self.num_pages as u64 {
      let evicted = next_page_id - self.num_pages as u64;
      self.cleanse_page(evicted, index);
      if let Some(old) = old_index.filter(|old| !eq(*old, index)) {
        self.cleanse_page(evicted, old);
      }
    }
    // 驱逐清洗完成后将 ClosedUntilAddress 单调推进至该被清洗旧代页的实际末尾边界
    //（严格对标 AllocatorBase.cs:OnPagesClosedWorkerCore 逐页 MonotonicUpdate，
    // end 恒为该页页界且 <= HeadAddress），唤醒读侧
    // spin_wait_until_record_is_closed 等待者回链头重探
    self.closed_until_address.fetch_max(barrier, Release);
    self.buffer.clear_page(next_page_id);
    // 最后发布 tail 再重置状态字：tail 发布后新快照全部落在新页（新注册者暂见
    // CLOSED 退订重试，重置后正常注册），被驱逐页自此不再有任何切片抢占
    self.tail_address.store(next_page_start, Release);
    self.page_inflight[slot].store(0, Release);
  }
}
