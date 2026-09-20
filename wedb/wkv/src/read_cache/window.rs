//! 地址窗口水位与读侧链路：tail/head/closed_until 水位读取、环形窗口判定、
//! 零拷贝直读、链路跳读与驱逐等待协议
//!
//! 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:FindInReadCache
//! ReadCache.cs:SkipReadCache、ReadCache.cs:ReadCacheNeedToWaitForEviction
//! 与 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs 地址窗口水位域（ClosedUntilAddress）

use std::{sync::atomic::Ordering::Acquire, thread::yield_now};

use wbase::addr::{is_read_cache, to_absolute};
use wrecord::{HEADER_SIZE, RecordHeader};

use super::{RcVisit, ReadCache};

impl ReadCache {
  /// 获取当前已分配的尾部逻辑偏移
  #[inline(always)]
  pub fn tail_address(&self) -> u64 {
    self.tail_address.load(Acquire)
  }

  /// 获取当前有效窗口的头部逻辑偏移
  #[inline(always)]
  pub fn head_address(&self) -> u64 {
    self.head_address.load(Acquire)
  }

  /// 获取驱逐清洗完成高水位地址（严格对标 AllocatorBase.cs:ClosedUntilAddress）
  #[inline(always)]
  pub fn closed_until_address(&self) -> u64 {
    self.closed_until_address.load(Acquire)
  }

  /// 零拷贝直读单条 ReadCache 记录并三态披露续链前驱（对标 FindInReadCache 每跳的
  /// 单条判读：LogRecord.GetInfo + readcache.CreateLogRecord + KeysEqual，兼
  /// AllocatorBase 无锁指针直读；整链走查单点在
  /// `session/raw/read.rs:StoreSession::find_in_read_cache`）
  ///
  /// 闭包仅在可读且未作废的记录上执行；返回值三态口径见 [`RcVisit`]——
  /// closed 作废记录不比对键、携 prev 续链（对标 C#「非 Invalid 才比对、
  /// 无条件沿 PreviousAddress 继续」），滑窗/换装竞态返回 [`RcVisit::Gone`]
  /// 由调用方回链头重探，绝不折叠成链终止产出假 NOTFOUND
  pub fn with_record<R>(
    &self,
    tagged_addr: u64,
    f: impl FnOnce(&[u8], &[u8]) -> Option<R>,
  ) -> RcVisit<R> {
    if !self.is_enabled || !is_read_cache(tagged_addr) {
      return RcVisit::Next(0);
    }

    let abs_addr = to_absolute(tagged_addr);
    let head = self.head_address.load(Acquire);
    let tail = self.tail_address.load(Acquire);

    // 检查是否处于当前有效环形内存窗口内；滑出窗口不可判读，非链终止
    if abs_addr < head || abs_addr >= tail {
      return RcVisit::Gone;
    }

    let page_id = self.buffer.page_of_address(abs_addr);
    let offset = self.buffer.offset_in_page(abs_addr);
    if offset + HEADER_SIZE > self.page_size {
      return RcVisit::Gone;
    }

    // 1. 无锁快速直读：利用 page_ids 双重校验规避 RwLock 原子计数器颠簸。
    //    解析后追加 seqlock 式读后复核（page_ids + head 双校验）：HybridLog 只读区
    //    裸读有 epoch 排空兜底（页驱逐前必经 safe_head 纪元屏障），而本缓冲的环形
    //    换装为纯锁协同（clear_page 写锁不等待纪元），skip_read_cache 等调用方
    //    （wcpr 检查点快照解析）可无 epoch 保护运行——try_read_page_unlocked 的
    //    二次校验与解析之间存在窗口，clear_page 可在该窗口内清零/换装页字节。
    //    复核通过即保证解析期间字节恒为原页内容：clear 必先翻 page_ids（Release），
    //    64 位逻辑页号单调不复用无 ABA，复核仍见本页号 ⇒ 清零尚未发生 ⇒ 解析字节
    //    完整。复核失败转锁内路径重读（页读锁与 clear 写锁互斥）。
    if let Some(page_slice) = unsafe { self.buffer.try_read_page_unlocked(page_id) }
      && abs_addr >= self.head_address.load(Acquire)
      && let Some(entry) = classify_record_at(page_slice, offset, self.page_size - offset)
      && self.buffer.is_page_loaded(page_id)
      && abs_addr >= self.head_address.load(Acquire)
    {
      return match entry {
        RcEntry::Live { key, val, prev } => f(key, val).map_or(RcVisit::Next(prev), RcVisit::Found),
        RcEntry::Invalid { prev } => RcVisit::Next(prev),
        RcEntry::Tail => RcVisit::Next(0),
      };
    }

    // 2. 慢路径安全回退：在换页临界区获取页读锁保护。
    //    锁内双重校验（对齐 HybridLog::probe_resident 的 Locked 路径口径）：head 已推进
    //    （记录滑出窗口）或槽位已换装承载其他逻辑页（加锁间隙环形回绕复用）时按
    //    不可判读返回 Gone（区别于链尾，杜绝瞬态竞态折叠成假 NOTFOUND）；
    //    页读锁下页字节稳定（clear_page 须取写锁互斥），解析失败即真实零头/松弛区，
    //    按链尾口径终止
    let page_guard = self.buffer.read_page(page_id);
    if abs_addr < self.head_address.load(Acquire) || !self.buffer.is_page_loaded(page_id) {
      return RcVisit::Gone;
    }
    match classify_record_at(&page_guard, offset, self.page_size - offset) {
      Some(RcEntry::Live { key, val, prev }) => {
        f(key, val).map_or(RcVisit::Next(prev), RcVisit::Found)
      }
      Some(RcEntry::Invalid { prev }) => RcVisit::Next(prev),
      Some(RcEntry::Tail) | None => RcVisit::Next(0),
    }
  }

  /// 读侧驱逐等待协议入口（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:ReadCacheNeedToWaitForEviction）
  ///
  /// 判定 RC 记录地址是否已滑出环形窗口（abs < head_address）：是则自旋等待驱逐方
  /// 完成该页哈希链清洗并发布 ClosedUntilAddress（`spin_wait_until_record_is_closed`），
  /// 返回 true 通知调用方按 `UpdateRecordSourceToCurrentHashEntry` 语义回链头重探
  /// （RestartChain）；未启用 / 非 RC 地址 / 窗口内一律返回 false。
  ///
  /// `refresh` 为调用方纪元句柄的 ProtectAndDrain 语义闭包（对标 epoch.ProtectAndDrain）。
  /// 调用方自旋期间可持有哈希桶共享 latch：驱逐方清洗为纯 CAS（无独占 latch），
  /// 进度不受阻塞，等待有界。
  #[inline]
  pub fn need_to_wait_for_eviction(&self, tagged_addr: u64, refresh: impl FnMut()) -> bool {
    if !self.is_enabled || !is_read_cache(tagged_addr) {
      return false;
    }
    let abs_addr = to_absolute(tagged_addr);
    if abs_addr >= self.head_address.load(Acquire) {
      return false;
    }
    self.spin_wait_until_record_is_closed(abs_addr, refresh);
    true
  }

  /// 自旋等待记录被驱逐方关闭（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/EpochOperations.cs:SpinWaitUntilRecordIsClosed）
  ///
  /// 每轮先执行 `refresh`（ProtectAndDrain 语义）再让出 CPU，随后检查
  /// `abs < closed_until_address`（ClosedUntilAddress 为高水位：等于该值的记录
  /// 尚未关闭）。调用方已保证 `abs < head_address`，故至少执行一轮 refresh——
  /// 若立即返回，调用方回链头重探时可能依旧看到 abs < head（下一次换页清洗尚未
  /// 获得推进机会，且本线程纪元落后于排空队列），构成活锁。
  fn spin_wait_until_record_is_closed(&self, abs_addr: u64, mut refresh: impl FnMut()) {
    loop {
      refresh();
      yield_now();

      // ClosedUntilAddress 高水位语义：== 表示尚未关闭，必须严格小于
      if abs_addr < self.closed_until_address.load(Acquire) {
        break;
      }
    }
  }

  /// 获取 ReadCache 记录的前驱地址（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:SkipReadCache）
  #[inline]
  pub fn prev_address_of(&self, tagged_addr: u64) -> Option<u64> {
    if !self.is_enabled || !is_read_cache(tagged_addr) {
      return None;
    }
    let abs_addr = to_absolute(tagged_addr);
    let head = self.head_address.load(Acquire);
    let tail = self.tail_address.load(Acquire);
    if abs_addr < head || abs_addr >= tail {
      return None;
    }
    let page_id = self.buffer.page_of_address(abs_addr);
    let offset = self.buffer.offset_in_page(abs_addr);
    if offset + HEADER_SIZE > self.page_size {
      return None;
    }
    if let Some(page_slice) = unsafe { self.buffer.try_read_page_unlocked(page_id) }
      && abs_addr >= self.head_address.load(Acquire)
      && let Some(header) = RecordHeader::decode_opt(&page_slice[offset..])
      && !header.is_pad()
      && !header.is_null()
      && self.buffer.is_page_loaded(page_id)
      && abs_addr >= self.head_address.load(Acquire)
    {
      return Some(header.address());
    }
    let page_guard = self.buffer.read_page(page_id);
    if abs_addr < self.head_address.load(Acquire) || !self.buffer.is_page_loaded(page_id) {
      return None;
    }
    let header = RecordHeader::decode_opt(&page_guard[offset..])?;
    // 零头（换页后页尾松弛区）与 pad 同语义：前驱不存在，按断链口径返回
    if header.is_pad() || header.is_null() {
      return None;
    }
    Some(header.address())
  }

  /// 顺链跳过所有 ReadCache 记录，获取底层的首个主日志逻辑地址（严格对标 Garnet SkipReadCache）
  ///
  /// 无跳数上限（对标 C#：地址沿 prev 严格单调递减天然无环）。返回 None 表示走查
  /// 途中触及滑出窗口/不可判读记录（对标 C# SkipReadCache 的
  /// ReadCacheNeedToWaitForEviction → RestartChain 重启窗）：调用方须回链头重探，
  /// 不得按断链降级；Some(0) 保留链尽语义
  #[inline]
  pub fn skip_read_cache(&self, mut addr: u64) -> Option<u64> {
    while is_read_cache(addr) {
      {
        let prev = self.prev_address_of(addr)?;
        addr = prev
      }
    }
    Some(addr)
  }

  /// 宿主端口地址折算单点（`wcpr::CprStore::skip_read_cache` 与
  /// `wcompact::CompactStore::skip_read_cache` 共用，杜绝两端逐字复制判定体）
  ///
  /// 端口契约为 `u64` 无 Option：走查触到滑窗/不可判读（[`Self::skip_read_cache`]
  /// 返回 None，C# 侧对应 ReadCacheNeedToWaitForEviction → RestartChain 重启窗）
  /// 折成 0 断链哨兵，两端各自另有复核——检查点面 resolve_slot 断链重读 +
  /// cleanse_page 承诺页复用前恢复槽位至主日志地址（快照口径保守无损），
  /// 紧缩面把 0 候选标为失效陈旧槽位并有记录级复核兜底。
  /// ReadCache 关闭时恒 0：无 RC 标记位可剥，两端消费者均只在
  /// `is_read_cache(addr)` 为真时才调入本口（关闭态该判据恒假，分支不可达，
  /// 保留作口径收口）
  #[inline]
  pub(crate) fn skip_read_cache_addr(&self, addr: u64) -> u64 {
    if self.is_enabled {
      self.skip_read_cache(addr).unwrap_or(0)
    } else {
      0
    }
  }
}

/// 页内 offset 处记录的走查分类结果（对标 C# FindInReadCache 消费的
/// LogRecord.GetInfo 三态：Valid / Invalid(closed) / 页尾非记录区）
enum RcEntry<'a> {
  /// 可读有效记录：键值切片 + 前驱地址
  Live {
    key: &'a [u8],
    val: &'a [u8],
    prev: u64,
  },
  /// 已作废（closed）记录：不比对键，携前驱续链（对标 C# 走查跳过 Invalid）
  Invalid { prev: u64 },
  /// 链尾：pad/零头/尺寸越界（页尾松弛区，正常走查不可达链中）
  Tail,
}

/// 页内 offset 处的记录走查分类（头部尚不可解码返回 None，交调用方降级）
#[inline(always)]
fn classify_record_at(
  page_slice: &[u8],
  offset: usize,
  remaining_in_page: usize,
) -> Option<RcEntry<'_>> {
  if remaining_in_page < HEADER_SIZE {
    return Some(RcEntry::Tail);
  }
  let header = RecordHeader::decode_opt(&page_slice[offset..])?;

  // 零头/填充/墓碑按链尾（对标换页后页尾松弛区恒为零填充的口径）；
  // closed 作废记录携 prev 续链，绝不当链终止（本条修复核心）
  if header.is_pad() || header.is_null() || header.is_tombstone() {
    return Some(RcEntry::Tail);
  }
  if header.is_closed() {
    return Some(RcEntry::Invalid {
      prev: header.address(),
    });
  }

  let rec_size = header.checked_record_size()?;
  if rec_size > remaining_in_page {
    return Some(RcEntry::Tail);
  }

  let key_start = offset + HEADER_SIZE;
  let key_end = key_start + header.key_len() as usize;
  let val_end = key_end + header.val_len() as usize;

  Some(RcEntry::Live {
    key: &page_slice[key_start..key_end],
    val: &page_slice[key_end..val_end],
    prev: header.address(),
  })
}
