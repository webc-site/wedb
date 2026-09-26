//! 地址窗口水位与读侧链路：tail/head/closed_until 水位读取、环形窗口判定、
//! 零拷贝直读、链路跳读与驱逐等待协议
//!
//! 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:FindInReadCache
//! ReadCache.cs:SkipReadCache、ReadCache.cs:ReadCacheNeedToWaitForEviction
//! 与 libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs 地址窗口水位域（ClosedUntilAddress）

use std::{ops::Range, sync::atomic::Ordering::Acquire, thread::yield_now};

use parking_lot::RwLockReadGuard;
use wbase::{
  addr::{is_read_cache, to_absolute},
  pool::AlignedBuf,
};
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

  /// 获取纪元排空完成高水位地址（严格对标 AllocatorBase.cs:SafeHeadAddress）
  ///
  /// 回绕换页的旧页关闭序列（`read_cache/append.rs:close_pending_page`，经
  /// `pump_close_barrier` 注册的纪元延迟动作）先推进该水位再清零/换装槽位：
  /// 持 Participant/EpochGuard 的在途无锁直读读者未退出前，`PageView::Fast`
  /// 借出的裸切片所指物理页恒不被并发清空或覆写
  #[inline(always)]
  pub fn safe_head_address(&self) -> u64 {
    self.safe_head_address.load(Acquire)
  }

  /// 零拷贝直读单条 ReadCache 记录并三态披露续链前驱（对标 FindInReadCache 每跳的
  /// 单条判读：LogRecord.GetInfo + readcache.CreateLogRecord + KeysEqual，兼
  /// AllocatorBase 无锁指针直读；整链走查单点在
  /// `session/raw/read.rs:StoreSession::find_in_read_cache`）
  ///
  /// 取页与校验共用 [`Self::page_view`] 单内核，本臂只做分类：闭包消费经
  /// [`disclose`] 单点，仅在全部复核通过后执行
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
    match self.page_view(tagged_addr, classify_record_at) {
      // 入口短路（未启用/非 RC 地址）与锁内真实零头/松弛区均按链尾口径披露
      PageView::NotRc | PageView::Unparsable => RcVisit::Next(0),
      PageView::Gone => RcVisit::Gone,
      PageView::Fast(entry, page) => disclose(entry, page, f),
      PageView::Slow(entry, guard) => disclose(entry, &guard, f),
    }
  }

  /// 页内记录视图取页内核（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs
  /// 中 FindInReadCache 与 SkipReadCache 两臂共用的单点记录读取 InternalRead：取页与校验只有一份，
  /// 两臂仅拿到记录视图后的分类不同）
  ///
  /// 本模块 `try_read_page_unlocked` 无锁快路径与 `read_page` 页读锁慢路径的唯一调用点：
  /// 窗口/offset 前置守卫 → 快路径 page_ids 首校验 + head 前置校验后投机调用 `classify`
  /// （必须为纯解码、零副作用闭包，允许白跑）→ seqlock 式读后双重复核（page_ids + head）→
  /// 任一环节未落定即降级页读锁慢路径（锁内 head/page_ids 双校验后再次 `classify`，
  /// 失败归 [`PageView::Unparsable`]）。校验序列逐字保持单一事实源，杜绝两臂各写一份时
  /// 校验口径分叉成静默读脏风险。
  ///
  /// 快路径复核安全性：本缓冲回绕换装的旧页关闭序列已整体挂入全局纪元延迟排空
  /// 队列（`read_cache/append.rs:pump_close_barrier` / `close_pending_page`，对标
  /// AllocatorBase.cs:ShiftHeadAddress 的 `epoch.BumpCurrentEpoch(() => OnPagesClosed)`
  /// 与 readcache.md「Memory reclamation and epoch protection」）：调用方读链全程持
  /// Participant/EpochGuard（`session/raw/read.rs:find_in_read_cache` 与紧缩/复制臂
  /// 同口径），clear_page 的 `fill(0)` 与新代 `encode_to_slice` 覆写只可能在全部
  /// 注册前受保护读者刷新退出之后发生，故借出至 [`disclose`] 闭包内的裸切片在纪元
  /// 窗口内恒稳定。本函数的 page_ids/head 双重复核继续兜住正向首轮内联换装与
  /// `try_read_page_unlocked` 首校验至解析之间的窗口（clear 必先翻 page_ids
  /// （Release），64 位逻辑页号单调不复用无 ABA，复核仍见本页号 ⇒ 清零尚未发生 ⇒
  /// 解析字节完整），复核失败转锁内路径重读（页读锁与 clear 写锁互斥）。
  ///
  /// 慢路径锁内双重校验（对齐 HybridLog::probe_resident 的 Locked 路径口径）：head 已推进
  /// （记录滑出窗口）或槽位已换装承载其他逻辑页（加锁间隙环形回绕复用）时按不可判读返回
  /// [`PageView::Gone`]（区别于链终止，杜绝瞬态竞态折叠成假 NOTFOUND）；页读锁下页字节稳定
  /// （clear_page 须取写锁互斥），解析失败即真实零头/松弛区，归 [`PageView::Unparsable`]。
  fn page_view<P>(
    &self,
    tagged_addr: u64,
    classify: impl Fn(&[u8], usize, usize) -> Option<P>,
  ) -> PageView<'_, P> {
    if !self.is_enabled || !is_read_cache(tagged_addr) {
      return PageView::NotRc;
    }

    let abs_addr = to_absolute(tagged_addr);
    let head = self.head_address.load(Acquire);
    let tail = self.tail_address.load(Acquire);

    // 检查是否处于当前有效环形内存窗口内；滑出窗口不可判读，非链终止
    if abs_addr < head || abs_addr >= tail {
      return PageView::Gone;
    }

    let page_id = self.buffer.page_of_address(abs_addr);
    let offset = self.buffer.offset_in_page(abs_addr);
    if offset + HEADER_SIZE > self.page_size {
      return PageView::Gone;
    }

    // 1. 无锁快速直读：利用 page_ids 双重校验规避 RwLock 原子计数器颠簸
    if let Some(page_slice) = unsafe { self.buffer.try_read_page_unlocked(page_id) }
      && abs_addr >= self.head_address.load(Acquire)
      && let Some(view) = classify(page_slice, offset, self.page_size - offset)
      && self.buffer.is_page_loaded(page_id)
      && abs_addr >= self.head_address.load(Acquire)
    {
      return PageView::Fast(view, page_slice);
    }

    // 2. 慢路径安全回退：在换页临界区获取页读锁保护
    let page_guard = self.buffer.read_page(page_id);
    if abs_addr < self.head_address.load(Acquire) || !self.buffer.is_page_loaded(page_id) {
      return PageView::Gone;
    }
    match classify(&page_guard, offset, self.page_size - offset) {
      Some(view) => PageView::Slow(view, page_guard),
      None => PageView::Unparsable,
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

  /// 走查单步分类内核（[`Self::prev_address_of`] 与两条跳读臂的唯一判读口）
  ///
  /// 取页与校验共用 [`Self::page_view`] 单内核，本臂只做分类：解码头部、剥除 pad/零头
  /// 后携前驱地址；不可判读两态严格分家——慢路径页读锁下定谳的真实零头/残损归
  /// [`Step::Tail`]（链尾），滑窗/换装竞态归 [`Step::Evicted`]（瞬态，供走查臂回链头
  /// 重探），杜绝把驱逐过渡态折叠成链终止
  #[inline(always)]
  fn walk_step(&self, tagged_addr: u64) -> Step {
    match self.page_view(tagged_addr, |page, offset, _| {
      let header = RecordHeader::decode_opt(&page[offset..])?;
      // 零头（换页后页尾松弛区）与 pad 同语义：前驱不存在，视图不可解析
      (!header.is_pad() && !header.is_null()).then_some(header.address())
    }) {
      PageView::Fast(prev, _) | PageView::Slow(prev, _) => Step::Next(prev),
      PageView::Unparsable | PageView::NotRc => Step::Tail,
      PageView::Gone => Step::Evicted,
    }
  }

  /// 获取 ReadCache 记录的前驱地址（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:SkipReadCache）
  #[inline]
  pub fn prev_address_of(&self, tagged_addr: u64) -> Option<u64> {
    match self.walk_step(tagged_addr) {
      Step::Next(prev) => Some(prev),
      Step::Tail | Step::Evicted => None,
    }
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
      let Step::Next(prev) = self.walk_step(addr) else {
        return None;
      };
      addr = prev;
    }
    Some(addr)
  }

  /// 顺链跳过所有 ReadCache 记录 + 驱逐等待的走查单口（严格对标
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:SkipReadCache
  /// 的 `RestartChain` 环：每步先判当前位置是否滑窗、命中即就地等待清洗后回链头重探）
  ///
  /// 与 [`Self::skip_read_cache`] 的唯一差异是不可判读形态的处置，且恒有返回值：
  /// - 滑窗驱逐过渡态：以**当前走查位置**（而非链头）经 [`Self::need_to_wait_for_eviction`]
  ///   自旋等待驱逐方清洗该页并发布 ClosedUntilAddress（对标 C# :105 判定
  ///   `AbsoluteAddress(recSrc.LatestLogicalAddress)`），落定后经 `head` 闭包回链头
  ///   重探（对标 :111 `UpdateRecordSourceToCurrentHashEntry` + :138 `goto RestartChain`），
  ///   循环直至解析落定——链中段记录滑出而槽头尚在窗内是驱逐进行中的常态形态，
  ///   锚链头判定会把该形态误判为「非驱逐窗」，故等待必须逐位置锚定；
  /// - 页读锁下定谳的真实零头/残损（[`Step::Tail`]）按链尽返回 0；
  /// - 快路径复核竞态 [`Step::Evicted`]（未滑窗，无需等待）直接回链头重探。
  ///
  /// `head` 闭包契约：**重读该哈希项当前的地址值**（低 48 位，可含 ReadCache 位）。
  /// 清洗方以槽位 CAS 把易失前缀换指主日志地址，重走旧链头地址会命中已清零页误判
  /// 链尽，故重读为正确性必需而非锦上添花；持不到槽位句柄的调用方以该键的索引候选
  /// 链重探等价复现（见 wkv 会话面端口体）。
  ///
  /// `refresh` 沿用 [`Self::need_to_wait_for_eviction`] 的调用方纪元口径（快照面宿主
  /// 纪元 drain、会话面 `Participant::refresh`）。RC 未启用与非 RC 地址恒等透传零开销
  /// （未启用直返 `head()`，非 RC 不进走查环）。
  #[inline]
  pub fn skip_read_cache_with_wait(
    &self,
    head: impl Fn() -> u64,
    mut refresh: impl FnMut(),
  ) -> u64 {
    if !self.is_enabled {
      return head();
    }
    loop {
      let mut addr = head();
      let mut restart = false;
      while is_read_cache(addr) {
        // 每步先按当前位置判驱逐窗：命中即等待落定并回链头重探（C# :135-139）
        if self.need_to_wait_for_eviction(addr, &mut refresh) {
          restart = true;
          break;
        }
        match self.walk_step(addr) {
          Step::Next(prev) => addr = prev,
          // 链尽：RC 专属记录无主日志对应
          Step::Tail => return 0,
          // 未滑窗的取页竞态：不等待，直接回链头重读重探
          Step::Evicted => {
            restart = true;
            break;
          }
        }
      }
      if !restart {
        return addr;
      }
    }
  }
}

/// [`ReadCache::walk_step`] 的走查单步分类
enum Step {
  /// 可判读且非链尾：携前驱地址续链
  Next(u64),
  /// 链尽：页读锁下定谳的真实零头/残损/pad（或 RC 未启用的入口短路）
  Tail,
  /// 瞬态不可判读：滑出窗口 / offset 越界 / 锁内 head·page_ids 双重校验失败，
  /// 须回链头重探，绝不按断链降级
  Evicted,
}

/// [`ReadCache::page_view`] 的取页产物：页视图借用 + 分类闭包输出
enum PageView<'a, P> {
  /// 入口短路：ReadCache 未启用或地址无 RC 标记
  NotRc,
  /// 不可判读：滑出窗口 / offset 越界 / 锁内 head·page_ids 双重校验失败
  /// （瞬态竞态，供走查臂回链头重探，区别于链终止）
  Gone,
  /// 快慢两路分类均不可解析：慢路径页读锁下解析失败即真实零头/残损头部
  /// （页尾松弛区，按链尾口径消费）
  Unparsable,
  /// 无锁快路径读后双重复核全通过：分类产物 + 裸页切片（安全性论证见
  /// [`ReadCache::page_view`]）
  Fast(P, &'a [u8]),
  /// 页读锁慢路径校验通过：分类产物 + 存活读锁守卫（守卫期间页字节稳定）
  Slow(P, RwLockReadGuard<'a, AlignedBuf>),
}

/// 页内 offset 处记录的走查分类结果（对标 C# FindInReadCache 消费的
/// LogRecord.GetInfo 三态：Valid / Invalid(closed) / 页尾非记录区）。
/// 键值以免借用的页内字节区间表达，切片消费延到全部复核通过后单点
/// [`disclose`]，投机解析绝不触碰调用方闭包
enum RcEntry {
  /// 可读有效记录：键值页内区间 + 前驱地址
  Live {
    key: Range<usize>,
    val: Range<usize>,
    prev: u64,
  },
  /// 已作废（closed）记录：不比对键，携前驱续链（对标 C# 走查跳过 Invalid）
  Invalid { prev: u64 },
  /// 链尾：pad/零头/尺寸越界（页尾松弛区，正常走查不可达链中）
  Tail,
}

/// 页内 offset 处的记录走查分类（头部尚不可解码返回 None，交取页内核降级）
#[inline(always)]
fn classify_record_at(
  page_slice: &[u8],
  offset: usize,
  remaining_in_page: usize,
) -> Option<RcEntry> {
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
    key: key_start..key_end,
    val: key_end..val_end,
    prev: header.address(),
  })
}

/// RcEntry 三态消费单点：仅在全部取页校验通过、页字节稳定后调用，
/// 闭包 f 至多执行恰好一次（Live 且未作废才比对键值）
fn disclose<R>(
  entry: RcEntry,
  page: &[u8],
  f: impl FnOnce(&[u8], &[u8]) -> Option<R>,
) -> RcVisit<R> {
  match entry {
    RcEntry::Live { key, val, prev } => {
      f(&page[key], &page[val]).map_or(RcVisit::Next(prev), RcVisit::Found)
    }
    RcEntry::Invalid { prev } => RcVisit::Next(prev),
    RcEntry::Tail => RcVisit::Next(0),
  }
}
