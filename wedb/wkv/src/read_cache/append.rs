//! 只读缓存记录追加：CAS 预留 + 页内编码 + 哈希索引挂载
//!
//! 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToReadCache.cs:TryCopyToReadCache

use std::{
  slice::from_raw_parts_mut,
  sync::atomic::{
    AtomicU64,
    Ordering::{AcqRel, Acquire, Release},
  },
};

use wbase::{addr::with_read_cache, backoff::Backoff};
use windex::HashIndex;
use wrecord::{RecordHeader, encode_to_slice, record_size};

use super::{INFLIGHT_CLOSED, INFLIGHT_COUNT_MASK, ReadCache};

impl ReadCache {
  /// 向 ReadCache 追加一条只读缓存记录并 CAS 挂载哈希索引（严格对标
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToReadCache.cs:TryCopyToReadCache）
  ///
  /// 返回打上 `READ_CACHE_BIT` 的虚拟地址（记录已编码且已挂载）；若未启用、
  /// 单记录超出一页大小或索引挂载失败（并发写已 detach）则返回 None——
  /// 挂载失败时记录被原子作废（对标 C# SetNewRecordInvalid），读取方按未命中
  /// 降级主日志。
  ///
  /// # 回写窗撕裂闭环（原 r1 观察项，已按槽位两阶段关闭协议修复）
  ///
  /// CAS 预留 `[curr_tail, new_tail)` 切片与随后的字节编码之间存在无锁窗口。
  /// 原实现下极端停滞（SIGSTOP / VM 暂停恰好落在窗口内且持续超过整环回绕周期）
  /// 时，唤醒后的编码会落入已被换装复用的页槽污损新近记录，且 cleanse_page 扫描
  /// 遇零切片提前 break 截断清洗、令页尾已完成记录的哈希索引悬垂断链。
  /// 现以 [`Self::page_inflight`] 状态字闭环：编码与索引挂载全程持在途注册，
  /// 换页侧 `fetch_or(CLOSED)` 两阶段关闭被复用槽——置位后新注册必被拒绝、
  /// 存量注册计入计数，等待清零即保证页上全部编码 + 挂载完成，与 C# epoch-gated
  /// 页清除（AllocatorBase.cs 的 OnPagesClosed 排空）不变式一致。发布顺序收敛为
  /// 关闭 → 清洗 → 清页 → tail 发布，滞留编码者的切片恒落在仍有效的旧代页内。
  pub fn append(
    &self,
    key: &[u8],
    val: &[u8],
    prev_main_addr: u64,
    index: &HashIndex,
  ) -> Option<u64> {
    if !self.is_enabled {
      return None;
    }

    // 记录对齐逻辑尺寸（含隐式对齐填充，与主日志记录头 8 字节对齐不变式一致）
    let rec_size = record_size(key.len(), val.len());
    if rec_size > self.page_size {
      return None; // 单记录超出页容量，直接跳过缓存
    }

    let mut backoff = Backoff::new();
    loop {
      let curr_tail = self.tail_address.load(Acquire);
      let page_offset = curr_tail & self.page_mask;
      let remaining = (self.page_size as u64) - page_offset;

      if (rec_size as u64) <= remaining {
        let slot = self.buffer.page_idx(curr_tail >> self.page_shift);
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
            let dest = from_raw_parts_mut(page_ptr.add(page_offset as usize), rec_size);
            if encode_to_slice(dest, prev_main_addr, key, val, false).is_err() {
              // 编码失败（非法键长/地址超界等）：切片作废，退订后按未命中降级
              self.page_inflight[slot].fetch_sub(1, Release);
              return None;
            }
            // 编码完成后立即 CAS 挂载哈希索引（严格对标 TryCopyToReadCache 的
            // hei.TryCAS 线性化点：挂载是读取方可见性的唯一入口）。退订先于挂载
            // 会重开「清洗早于挂链」乱序窗，故挂载必须在在途计数覆盖内完成
            if !index.update_address(key, prev_main_addr, rc_addr) {
              // 挂载失败（并发写已 detach 或条目不存在）：对标 C# SetNewRecordInvalid
              // 原子置 SEALED 位作废本记录——cleanse 按关闭记录跳过（未挂载记录
              // 无需哈希链恢复），读侧按未命中降级主日志
              RecordHeader::set_invalid_atomic(
                &*page_ptr.add(page_offset as usize).cast::<AtomicU64>(),
              );
              self.page_inflight[slot].fetch_sub(1, Release);
              return None;
            }
          }

          self.page_inflight[slot].fetch_sub(1, Release);

          // 推进 head_address 滑动窗口下界
          let min_head = new_tail.saturating_sub(self.capacity);
          self.head_address.fetch_max(min_head, Release);

          return Some(rc_addr);
        }

        self.page_inflight[slot].fetch_sub(1, Release);
        // CAS 冲突自适应退避，降低 CPU 流水线惩罚与总线锁颠簸
        backoff.snooze();
      } else {
        // 页内剩余空间不足，获取轻量换页互斥锁跳至下一页开头
        let _lock = self.turn_lock.lock();
        let curr_tail2 = self.tail_address.load(Acquire);
        let page_offset2 = curr_tail2 & self.page_mask;
        let remaining2 = (self.page_size as u64) - page_offset2;

        if (rec_size as u64) <= remaining2 {
          // 并发换页方已推进 tail，本页空间已足，退锁按页内路径重试
          continue;
        }
        let next_page_start = curr_tail2 + remaining2;
        let next_page_id = next_page_start >> self.page_shift;
        let slot = self.buffer.page_idx(next_page_id);

        // 关键顺序保证：在复用清空旧槽位前，先将 head_address 推进，杜绝读取方进入即将被覆写的旧页
        let min_head = next_page_start.saturating_sub(self.capacity);
        self.head_address.fetch_max(min_head, Release);

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

        // 若发生环形回绕，在清空旧槽位前扫描被驱逐的旧页，原子更新哈希索引恢复指向主日志地址
        if next_page_id >= self.num_pages as u64 {
          self.cleanse_page(next_page_id - (self.num_pages as u64), index);
          // 驱逐清洗完成后发布 ClosedUntilAddress 高水位（严格对标
          // AllocatorBase.cs:OnPagesClosedWorker 的 MonotonicUpdate(ref ClosedUntilAddress, end)），
          // 唤醒读侧 SpinWaitUntilRecordIsClosed 等待者回链头重探
          self
            .closed_until_address
            .fetch_max(next_page_start, Release);
        }

        self.buffer.clear_page(next_page_id);
        // 最后发布 tail 再重置状态字：tail 发布后新快照全部落在新页（新注册者
        // 暂见 CLOSED 退订重试，重置后正常注册），被驱逐页自此不再有任何切片抢占
        self.tail_address.store(next_page_start, Release);
        self.page_inflight[slot].store(0, Release);
      }
    }
  }
}
