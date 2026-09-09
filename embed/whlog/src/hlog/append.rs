use std::{hint::spin_loop, sync::atomic::Ordering};

use log::trace;
use wdev::Device;

use super::{HybridLog, RecParams};
use crate::error::{Error, Result};

impl<D: Device> HybridLog<D> {
  /// 检查目标逻辑页槽位是否可以安全分配与初始化（防止覆盖尚未驱逐的旧页或活跃读者）
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:NeedToWaitForFlush / NeedToWaitForClose：环形回绕复用槽位前，
  /// 旧页必须同时满足已落盘（flushed_until）、已驱逐（head）且纪元排空（safe_head，
  /// 保证所有 epoch 保护下的读者已退出）。C# 通过 flushEvent 挂起等待，此处
  /// 在线程每核模型下改为返回 [Error::PageNotReady] 交由调用方异步刷盘后重试。
  pub(super) fn ensure_page_ready(&self, page_id: u64) -> Result<()> {
    if page_id >= self.config.num_pages as u64 {
      let old_page = page_id - self.config.num_pages as u64;
      let min_evicted_addr = self.config.page_start_address(old_page + 1);

      // 若旧页已安全落盘但 head 尚未推进，自动单调推进 head
      if self.addresses.flushed_until() >= min_evicted_addr
        && self.addresses.head() < min_evicted_addr
      {
        self.shift_head_address(min_evicted_addr);
      }

      // 如果当前 safe_head 尚未赶上，尝试推进并清理纪元动作
      if self.addresses.safe_head() < min_evicted_addr && self.epoch.has_pending_drain() {
        self.epoch.bump_epoch();
      }

      // 槽位上一轮承载的旧页必须已安全从内存中驱逐且所有读者已退出，且旧页已安全落盘
      if self.addresses.head() < min_evicted_addr
        || self.addresses.safe_head() < min_evicted_addr
        || self.addresses.flushed_until() < min_evicted_addr
      {
        return Err(Error::PageNotReady(page_id));
      }
    }
    Ok(())
  }

  /// 追加一条新记录到日志尾部
  ///
  /// - 计算记录大小，若当前活跃页剩余空间不足以容纳，进入换页逻辑写入 Pad 标记并跳到下一页开头
  ///   （保证记录决不跨页边界，严格对齐 Tsavorite 行为）；
  /// - 严格对标 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AllocatorBase.cs TryAllocate / HandlePageOverflow：
  ///   - 页内空间充足时：通过原子 CAS ([AtomicU64::compare_exchange_weak]) 独占瓜分物理空间，
  ///     直接使用裸指针无锁并发写入，彻底消除全局串行锁；
  ///   - 跨页边界时：仅在换页时获取轻量 page_turn_lock，由首个跨越线程负责打 Pad、
  ///     初始化新页并以 CAS 推进 tail；新记录编码统一发生在 tail 发布之后
  ///     （先分配后写，杜绝 CAS 竞争失败遗留孤儿字节）。
  /// - 工程注记：C# TryAllocate 的 fetch_add 方案依赖「未掩码 32 位 offset 字段」使越界预留线程可自检
  ///   （Offset > PageSize 即 RETRY）；本实现 tail 为完整掩码地址，越界预留经掩码后与合法预留不可区分，
  ///   无法安全复刻，故保留 CAS 快路径并以预检越界直通换页消除注定失败的 CAS 自旋。
  ///
  /// # 可见性契约
  /// 返回的地址在 `Ok` 发布时记录字节已完整编码；上层索引/哈希表必须在 `append`
  /// 返回后才允许发布该地址，读者据此避免观察到半截记录（对标 C# 记录先写后发布协议）。
  pub fn append(&self, key: &[u8], val: &[u8], prev_addr: u64, is_tombstone: bool) -> Result<u64> {
    let p = RecParams {
      prev_addr,
      key,
      val,
      is_tombstone,
    };
    let rec_size = self.validate_append_args(&p)?;

    let page_bits = self.config.page_bits();
    let page_mask = self.config.page_mask();
    let page_size = self.config.page_size;

    loop {
      let tail = self.addresses.tail_address.load(Ordering::Acquire);
      let curr_page = tail >> page_bits;
      let offset = (tail & page_mask) as usize;
      let remaining = page_size - offset;

      // 若当前页尚未加载就绪（例如刚跨入新页或环形槽位需回绕重用），获取锁进行安全初始化
      if !self.buffer.is_page_loaded(curr_page) {
        let _lock = self.page_turn_lock.lock();
        if !self.buffer.is_page_loaded(curr_page) {
          self.ensure_page_ready(curr_page)?;
          // tail 滞留页中且槽位失效仅在恢复预热缺失时出现；offset 以下的磁盘内容
          // 由 head 边界路由至 Device 读取，此处仅需保证 [offset, 页尾) 干净可写
          self.buffer.clear_page_from_offset(curr_page, offset);
        }
        continue;
      }

      if remaining >= rec_size {
        // 页内快速无锁分配路径：通过 CAS 原子瓜分独占切片
        let new_tail = tail + rec_size as u64;
        if self
          .addresses
          .tail_address
          .compare_exchange_weak(tail, new_tail, Ordering::AcqRel, Ordering::Acquire)
          .is_ok()
        {
          // SAFETY: 当前线程已通过 CAS 独占抢占了 [offset, offset + rec_size) 物理空间，
          // 该页槽位在初始或换页时已完成置零与槽位标定，多线程在各自互不相交的切片中并发写入。
          unsafe {
            self.encode_at(curr_page, offset, rec_size, &p)?;
          }

          // ReadOnlyAddress 推进短路：未跨页的追加不触发计算（RO 只在页边界事件推进，
          // 对齐 C# Tsavorite 换页驱动的 ReadOnly 滑动，热路径零 f64 与冗余原子操作；
          // RO 滞后仅影响原位更新机会窗口，语义保守安全，checkpoint 的 shift_read_only_to_tail 兜底）
          return Ok(tail);
        }

        // CAS 竞争失败，自旋重试
        spin_loop();
        continue;
      }

      // 页内空间不足，进入跨页慢路径（仅在换页时获取轻量 page_turn_lock，严格对齐 Garnet HandlePageOverflow）
      let next_page = curr_page + 1;

      // 1. 校验下一页环形槽位可安全复用（旧页须已安全落盘且读者全部退出）；
      //    CAS 协议下 tail 决不越过页界，下一页绝无并发写入者，锁外预清零绝无竞争。
      self.ensure_page_ready(next_page)?;
      if !self.buffer.is_page_loaded(next_page) {
        // P2-4: 64KB memset 在全局换页锁窗口外执行（对标 C# allocate-ahead 不变量），
        // 使临界区内只剩槽位标定
        self.buffer.preclear_page(next_page);
      }

      let _lock = self.page_turn_lock.lock();
      let cur_tail = self.addresses.tail_address.load(Ordering::Acquire);
      if cur_tail != tail {
        // 等锁期间已有其他线程完成换页，释放锁后重试
        continue;
      }

      trace!(
        "触发换页: curr_page={curr_page}, offset={offset}, remaining={remaining}, req_size={rec_size}"
      );

      // 2. 槽位标定（临界区内仅页号发布与罕见补零；预清零已在锁外完成），
      //    Release 页号发布先于 tail 发布，读者经 tail（Acquire）观察到记录时页必已就绪
      let next_page_start = self.config.page_start_address(next_page);
      self.buffer.seal_page(next_page);

      // 3. 原子 CAS 推进 tail_address，一举发布新页空间（含本记录预留的 [0, rec_size)）
      let new_tail = next_page_start + rec_size as u64;
      if self
        .addresses
        .tail_address
        .compare_exchange(cur_tail, new_tail, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
      {
        // 等锁/标定窗口内 tail 已被并发推进（如其他线程恰好填满旧页边界）：
        // 丢弃本次换页重载 tail 重试。槽位虽已 seal 但 tail 未进入新页，
        // 读者按 tail 钳制绝不会触达，后续换页者复用已 seal 槽位零额外成本
        continue;
      }

      // 4. CAS 成功独占换页后才落笔：先封印旧页剩余空间打 Pad 标记，再编码新页首部记录。
      //    与页内快路径「CAS → 编码」及 C# 「先分配（发布 tail）后写」协议严格一致——
      //    若在 CAS 前预写，CAS 竞争失败（如并发者恰好填满旧页边界）会遗留孤儿字节，
      //    胜者发布 tail 后扫描器可能把孤儿（或其被部分覆写的撕裂残片）误读为已发布记录；
      //    CAS 后写入则未编码区间必为整页零（读者按 Pad 跳过），绝不产生脏数据窗口
      if remaining > 0 {
        // SAFETY: 本线程已成功 CAS 独占跨越当前页边界，旧页 [offset, page_size) 剩余空间排他归本线程封印
        unsafe { self.write_pad_tail(curr_page, offset, remaining) };
      }
      // SAFETY: [0, rec_size) 已由本线程 CAS 独占预留，新页槽位已 seal 置零，
      // 后续分配者只能自 new_tail 起瓜分，切片互不相交
      unsafe { self.encode_at(next_page, 0, rec_size, &p)? };

      // 5. 检查并自动推进 ReadOnlyAddress（换页事件触发，定点化计算零 f64）
      self.maybe_advance_read_only(new_tail);
      return Ok(next_page_start);
    }
  }

  /// 基于当前 HeadAddress 与 mutable_fraction 尝试自动推进 ReadOnlyAddress（对标 C# 页界滑动的只读边界估算）
  #[inline]
  fn maybe_advance_read_only(&self, new_tail: u64) {
    let desired_ro = self
      .config
      .calculate_read_only_address(self.addresses.head(), new_tail);
    if desired_ro > self.addresses.read_only() {
      self.shift_read_only_address(desired_ro);
    }
  }
}
