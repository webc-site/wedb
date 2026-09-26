//! 页关闭清洗：环形回绕驱逐前沿哈希链链式摘除被驱逐段并缝合高位记录
//!
//! 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:ReadCacheEvict
//! （ReadCacheEvictChain）
//!
//! 对标口径：C# CASRecordIntoChain 的锁免设计下，写侧 CAS 即原子脱钩 ReadCache
//! 前缀，被取代的孤儿缓存记录不在写侧作废，统一由页关闭清洗回收（仍指向槽位的
//! 记录经槽位 CAS 恢复主日志地址，已脱钩槽位 CAS 自然落败跳过）；本文件
//! 承接同一单点回收机制。挂载失败帧的即时作废由 `read_cache/append.rs` 的
//! `set_invalid_atomic` 承担（对标 C# BlockAllocate.cs:TryAllocateRecordReadCache
//! 的败帧 ReadCacheAbandonRecord）。

use std::sync::atomic::{AtomicU64, Ordering::Acquire};

use wbase::{
  addr::{ADDRESS_MASK, is_read_cache, to_absolute},
  backoff::Backoff,
};
use whlog::for_each_record_in_page;
use windex::HashIndex;
use wrecord::{HEADER_SIZE, RecordHeader};

use super::ReadCache;

impl ReadCache {
  /// 覆写旧页前扫描其中的 ReadCache 记录，沿哈希链链式摘除被驱逐段（严格对标
  /// Garnet ReadCacheEvict：页内每条记录即 C# 驱逐区间的逐条锚定入口）
  ///
  /// C# 链级驱逐内核 ReadCacheEvictChain 由 [`Self::evict_chain`] 承接：哈希槽位
  /// 恒指向链头（Tag 碰撞键共享同一槽位），被驱逐记录多在链深处，仅恢复槽位不足以
  /// 摘除——高位未驱逐记录的 prev_address 必须原地缝合，否则读者顺链越过
  /// head_address 触发 PageView::Gone，构成 RETRY_LATER 活锁。
  ///
  /// 页内记录链走查复用 whlog 单点内核 [`for_each_record_in_page`]（零头/pad/
  /// 不可解码/越页界终止与按物理尺寸步进不再本地复刻），本函数只提供
  /// 哈希链恢复语义闭包。
  pub(super) fn cleanse_page(&self, page_id: u64, index: &HashIndex) {
    let page_start_addr = self.buffer.page_start_address(page_id);
    let guard = self.buffer.read_page(page_id);
    for_each_record_in_page(&guard, 0, |header, _offset, key, _val| {
      // 已作废记录（append 挂载失败即时作废 / 前序闭包已摘除）：对标 C# ReadCacheEvict 的
      // kTempInvalidAddress 跳过语义——未挂载/已脱链记录无需哈希链恢复，推进下一条
      // 而非截断扫描
      if header.is_closed() {
        return true;
      }

      self.evict_chain(&guard, page_start_addr, index, key);
      true
    });
  }

  /// 单键哈希链的驱逐摘除与缝合内核（严格对标 ReadCache.cs:ReadCacheEvictChain）
  ///
  /// 自 [`HashIndex::find_tag_entry_by_hash_with_min_addr`] 定位的槽位链头沿
  /// previousAddress 链下探（`min_valid_addr` 传 0 关闭截断清退，纯查找对标 C#
  /// ReadCacheEvict 的 FindTag）：
  ///
  /// - 高位记录（地址 >= 驱逐上界，驻留更新页）保留在链，仅记为缝合锚继续前瞻
  ///   （对标 nextPhysicalAddress 臂）；
  /// - 被驱逐记录（本页）在存在高位锚时经 [`RecordHeader::try_update_address`]
  ///   原子缝合锚记录的 prev_address（对标 nextri.TryUpdateAddress），否则以
  ///   定位句柄的定点槽位 CAS 直接恢复槽位至其前驱（对标 hei.TryCAS；前驱
  ///   为 0 的链尾经 `try_elide` 摘为空槽——0 为空槽哨兵，`try_cas` 依契约拒收）。
  ///   摘除成功后 `set_invalid_atomic` 标记本记录脱链（对标
  ///   PreviousAddress = kTempInvalidAddress）；
  /// - 槽位 CAS 落败按 C# SetToCurrent 重读槽位续链，与并发挂载/脱钩写自然收敛。
  ///
  /// # 并发与裸指针原子访问
  ///
  /// 链上高位记录驻留未换装页槽（环形窗口不变式：被驱逐页与 tail 相距整环，
  /// 高位地址恒 >= 驱逐上界 > head_address），其 prev_address 在挂链发布后唯一
  /// 写者即本清洗（持 turn_lock 单线程）；被驱逐页已过两阶段关闭且持页读锁。
  /// 头字一律以原子原语读写，与读侧裸读并存为全仓既有原子口径（对标 C# 非
  /// volatile 字段上的 Interlocked）。
  fn evict_chain(&self, guard: &[u8], page_start_addr: u64, index: &HashIndex, key: &[u8]) {
    let Some(mut hei) = index.find_tag_entry_by_hash_with_min_addr(HashIndex::hash_key(key), 0)
    else {
      return;
    };
    let evict_upper = page_start_addr + self.page_size as u64;
    let mut backoff = Backoff::new();
    // 高位缝合锚：链头段首个未驱逐 ReadCache 记录（对标 nextPhysicalAddress）
    let mut next_rc: Option<u64> = None;
    let mut entry = hei.address();
    while is_read_cache(entry) {
      let entry_abs = to_absolute(entry);
      if entry_abs >= evict_upper {
        // 前瞻：高位记录保留，记锚并沿其 prev 下探（对标 C# 前瞻分支）
        let Some(prev) = self.read_prev_atomic(entry_abs) else {
          return;
        };
        next_rc = Some(entry);
        entry = prev;
        continue;
      }
      if entry_abs < page_start_addr {
        // 比本页更旧的地址：前轮清洗已摘链，链上不可达，防御终止
        return;
      }
      let offset = (entry_abs - page_start_addr) as usize;
      let Some(rec) = RecordHeader::decode_opt(&guard[offset..]) else {
        return;
      };
      let rec_prev = rec.address();
      if let Some(next) = next_rc {
        // 缝合高位锚的 prev_address：被驱逐记录地址 → 其前驱（原子 CAS，
        // 对标 nextri.TryUpdateAddress）；落败重走本条（C# 同款：锚 prev 已被
        // 并发推进时下一轮以新链况收敛，清洗单线程下实际恒成功）
        if self.patch_prev_atomic(next, entry, rec_prev) {
          self.seal_atomic(entry_abs);
          entry = rec_prev;
          continue;
        }
        // 缝合落败：退避后重走本条（C# 同款重试语义；清洗持 turn_lock 单线程
        // 下实际恒成功，退避仅为防不可能态自旋兜底）
        backoff.snooze();
        continue;
      }
      // 槽位直指被驱逐记录：hei.TryCAS 恢复其前驱，整段旧链改挂槽位直通
      let unlinked = if rec_prev == 0 {
        hei.try_elide()
      } else {
        hei.try_cas(rec_prev)
      };
      if unlinked {
        self.seal_atomic(entry_abs);
      } else {
        // 并发写已抢占槽位（detach / 并发晋升新挂载）：SetToCurrent 重读续链
        hei.set_to_current();
      }
      entry = hei.address();
    }
  }

  /// 裸指针取页内记录头字原子视图（清洗域单点，安全性论证见 [`Self::evict_chain`]）
  #[inline]
  fn record_word_at(&self, abs_addr: u64) -> &AtomicU64 {
    let ptr = unsafe {
      self
        .buffer
        .raw_page_ptr_mut(self.buffer.slot_for_address(abs_addr))
        .add(self.buffer.offset_in_page(abs_addr))
    };
    // SAFETY: 记录 8 字节对齐不变式（RECORD_ALIGNMENT）保证头字原子访问合法
    unsafe { &*ptr.cast::<AtomicU64>() }
  }

  /// 原子读 RC 记录前驱地址（对标 C# 走查臂的 recordInfo.PreviousAddress 直读）
  #[inline]
  fn read_prev_atomic(&self, abs_addr: u64) -> Option<u64> {
    (self.buffer.offset_in_page(abs_addr) + HEADER_SIZE <= self.page_size)
      .then(|| self.record_word_at(abs_addr).load(Acquire) & ADDRESS_MASK)
  }

  /// 原子缝合 RC 记录的 prev_address（严格对标 RecordInfo.cs:TryUpdateAddress）
  #[inline]
  fn patch_prev_atomic(&self, abs_addr: u64, expect_prev: u64, new_prev: u64) -> bool {
    RecordHeader::try_update_address(self.record_word_at(abs_addr), expect_prev, new_prev)
  }

  /// 原子密封脱链记录（对标 C# recordInfo.PreviousAddress = kTempInvalidAddress：
  /// 记录不再在链，后续清洗与走查按关闭记录跳过）
  #[inline]
  fn seal_atomic(&self, abs_addr: u64) {
    RecordHeader::set_invalid_atomic(self.record_word_at(abs_addr));
  }
}
