use std::{
  hint::spin_loop,
  slice::from_raw_parts_mut,
  sync::atomic::{
    AtomicU64,
    Ordering::{AcqRel, Acquire, Release},
  },
  thread::yield_now,
};

use itoa::Buffer;
use parking_lot::Mutex;
use wbase::{
  addr::{self},
  align::CachePadded,
};
use whlog::{CircularPageBuffer, HybridLogConfig};
use windex::HashIndex;
use wrecord::{HEADER_SIZE, RecordHeader, encode_to_slice, record_size};

use crate::error::{Error, Result};

/// 判断给定逻辑地址是否属于 ReadCache 独立只读内存日志
#[inline(always)]
pub const fn is_read_cache_addr(addr: u64) -> bool {
  addr::is_read_cache(addr)
}

/// 还原去除了 ReadCache 标记位的绝对逻辑地址
#[inline(always)]
pub const fn absolute_address(addr: u64) -> u64 {
  addr::to_absolute(addr)
}

/// 为本地逻辑地址打上 ReadCache 标志位
#[inline(always)]
pub const fn tag_read_cache_addr(addr: u64) -> u64 {
  addr::with_read_cache(addr)
}

/// Microsoft Garnet 官方架构对标的独立只读非脏页内存日志（ReadCache）
///
/// 严格对齐 Garnet `ReadCache.cs` + `TryCopyToReadCache.cs`：
/// 1. 纯 DRAM 环形日志分配器，无任何物理磁盘持久化开销，零写放大；
/// 2. 磁盘冷数据命中回填后，挂载为哈希链首部前缀，加速后续高频读请求纳秒级命中；
/// 3. 主日志执行写操作（Upsert / RMW / Delete）时通过单次 CAS 原子脱钩整条 ReadCache 链；
/// 4. 环形覆盖自然淘汰旧页，在复用前执行 CleanseHashChain 原子解构恢复主日志链接，杜绝悬垂指针与数据丢失。
pub struct ReadCache {
  /// 环形页内存池
  buffer: CircularPageBuffer,
  /// 页面大小（字节，2 的幂）
  pub page_size: usize,
  /// 缓冲页总数（2 的幂）
  pub num_pages: usize,
  /// 页面位移量（用于取代 64 位整数除法）
  page_shift: u32,
  /// 页面偏移掩码（用于取代 64 位整数取模）
  page_mask: u64,
  /// 环形缓冲区总容量字节数
  capacity: u64,
  /// 活跃尾部分配地址（写热点，独占 64 字节缓存行）
  tail_address: CachePadded<AtomicU64>,
  /// 有效起始地址（滑动窗口下界，读热点，独占 64 字节缓存行）
  head_address: CachePadded<AtomicU64>,
  /// 驱逐清洗完成高水位地址（严格对标 AllocatorBase.cs:ClosedUntilAddress）
  ///
  /// 换页路径在 cleanse_page 恢复哈希链后按 MonotonicUpdate 口径单调推进；
  /// 读侧 SpinWaitUntilRecordIsClosed 以 `abs < closed_until_address` 为关闭判据
  /// （等于该值表示记录尚未关闭）。低频标记位，不加缓存行填充
  closed_until_address: AtomicU64,
  /// 换页保护互斥锁
  turn_lock: Mutex<()>,
  /// 是否启用 ReadCache
  pub is_enabled: bool,
}

impl ReadCache {
  /// 创建新的 ReadCache 实例
  pub fn new(page_size: usize, num_pages: usize, is_enabled: bool) -> Result<Self> {
    if !page_size.is_power_of_two() || page_size == 0 {
      let mut msg = String::from("ReadCache page_size 必须为非零且为 2 的幂，当前为 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(page_size));
      return Err(Error::InvalidConfig(msg));
    }
    if !num_pages.is_power_of_two() || num_pages == 0 {
      let mut msg = String::from("ReadCache num_pages 必须为非零且为 2 的幂，当前为 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(num_pages));
      return Err(Error::InvalidConfig(msg));
    }

    let dummy_config = HybridLogConfig {
      page_size,
      num_pages,
      mutable_fraction: 1.0,
      ro_lag_num: whlog::ro_lag_num_from_fraction(1.0),
      initial_address: 0,
    };
    let buffer = CircularPageBuffer::new(&dummy_config)?;
    buffer.clear_page(0);

    let page_shift = page_size.trailing_zeros();
    let page_mask = (page_size - 1) as u64;
    let capacity = (num_pages * page_size) as u64;

    Ok(Self {
      buffer,
      page_size,
      num_pages,
      page_shift,
      page_mask,
      capacity,
      tail_address: CachePadded(AtomicU64::new(0)),
      head_address: CachePadded(AtomicU64::new(0)),
      closed_until_address: AtomicU64::new(0),
      turn_lock: Mutex::new(()),
      is_enabled,
    })
  }

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

  /// 向 ReadCache 追加一条只读缓存记录（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToReadCache.cs:TryCopyToReadCache）
  ///
  /// 返回打上 `READ_CACHE_BIT` 的虚拟地址；若未启用或单记录超出一页大小则返回 None
  ///
  /// # 撕裂写窗口（r1 观察项：维持观察，文档记档）
  ///
  /// CAS 预留 `[curr_tail, new_tail)` 切片与随后的字节编码之间存在无锁窗口。
  /// 极端停滞（SIGSTOP / VM 暂停恰好落在窗口内，且持续超过整个环形缓冲的回绕
  /// 周期）时，唤醒后的编码会落入已被换装复用的页槽，污损新近记录。安全性边界：
  /// 哈希索引只可能指向**已完成编码**的记录（`append` 返回 Some 后调用方才 CAS
  /// 挂载），预留未写的切片对读取方仅表现为零头解析失败（按 miss 降级主日志）；
  /// 被污损新记录的读取方经键精确比对 + 记录头校验兜底，最坏退化为一次缓存
  /// miss / 陈旧值命中，无持久化影响。彻底闭合需在 `CircularPageBuffer` 层引入
  /// 跨 crate 的 seqlock 版本字协议（预留-编码-发布三段式），代价是热路径多一次
  /// 发布写；鉴于触发条件要求单线程停滞横跨整环回绕（微秒级窗口对 num_pages ×
  /// page_size 的写入量），维持观察，待 on_flush 管线接线时一并评估。
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

    loop {
      let curr_tail = self.tail_address.load(Acquire);
      let page_offset = curr_tail & self.page_mask;
      let remaining = (self.page_size as u64) - page_offset;

      if (rec_size as u64) <= remaining {
        // 页内剩余空间充足，CAS 抢占独占物理切片
        let new_tail = curr_tail + rec_size as u64;
        if self
          .tail_address
          .compare_exchange_weak(curr_tail, new_tail, AcqRel, Acquire)
          .is_ok()
        {
          let page_id = curr_tail >> self.page_shift;
          let slot = self.buffer.page_idx(page_id);
          // SAFETY: 当前线程通过 CAS 独占抢占 [page_offset, page_offset + rec_size) 内存切片，
          // 该页在初始或换页时已置零，多线程在互不重叠的切片内并发编码，完全零锁。
          unsafe {
            let page_ptr = self.buffer.raw_page_ptr_mut(slot);
            let dest = from_raw_parts_mut(page_ptr.add(page_offset as usize), rec_size);
            encode_to_slice(dest, prev_main_addr, key, val, false).ok()?;
          }

          // 推进 head_address 滑动窗口下界
          let min_head = new_tail.saturating_sub(self.capacity);
          self.head_address.fetch_max(min_head, Release);

          return Some(tag_read_cache_addr(curr_tail));
        }

        // CAS 冲突自旋提示，降低 CPU 流水线惩罚与总线锁颠簸
        spin_loop();
      } else {
        // 页内剩余空间不足，获取轻量换页互斥锁填充 Pad 并跳至下一页开头
        let _lock = self.turn_lock.lock();
        let curr_tail2 = self.tail_address.load(Acquire);
        let page_offset2 = curr_tail2 & self.page_mask;
        let remaining2 = (self.page_size as u64) - page_offset2;

        if (rec_size as u64) > remaining2 {
          let page_id = curr_tail2 >> self.page_shift;
          if remaining2 >= (HEADER_SIZE as u64) {
            let slot = self.buffer.page_idx(page_id);
            unsafe {
              let page_ptr = self.buffer.raw_page_ptr_mut(slot);
              let pad_dest =
                from_raw_parts_mut(page_ptr.add(page_offset2 as usize), remaining2 as usize);
              let pad_header = RecordHeader::pad(remaining2 as usize);
              pad_dest[..HEADER_SIZE].copy_from_slice(&pad_header.to_bytes());
            }
          }
          let next_page_start = curr_tail2 + remaining2;
          let next_page_id = next_page_start >> self.page_shift;

          // 关键顺序保证：在复用清空旧槽位前，先将 head_address 推进，杜绝读取方进入即将被覆写的旧页
          let min_head = next_page_start.saturating_sub(self.capacity);
          self.head_address.fetch_max(min_head, Release);

          // 若发生环形回绕，在 clear_page 之前先扫描被驱逐的旧页，原子更新哈希索引恢复指向主日志地址
          if next_page_id >= self.num_pages as u64 {
            let evicted_page_id = next_page_id - (self.num_pages as u64);
            self.cleanse_page(evicted_page_id, index);
            // 驱逐清洗完成后发布 ClosedUntilAddress 高水位（严格对标
            // AllocatorBase.cs:OnPagesClosedWorker 的 MonotonicUpdate(ref ClosedUntilAddress, end)），
            // 唤醒读侧 SpinWaitUntilRecordIsClosed 等待者回链头重探
            self
              .closed_until_address
              .fetch_max(next_page_start, Release);
          }

          self.buffer.clear_page(next_page_id);
          self.tail_address.store(next_page_start, Release);
        }
      }
    }
  }

  /// 覆写旧页前扫描其中的 ReadCache 记录，将仍指向这些记录的哈希索引槽位原子恢复至主日志地址（严格对标 Garnet CleanseHashChain）
  fn cleanse_page(&self, page_id: u64, index: &HashIndex) {
    let page_start_addr = page_id << self.page_shift;
    let guard = self.buffer.read_page(page_id);
    let mut offset = 0;

    while offset + HEADER_SIZE <= self.page_size {
      let slice = &guard[offset..];
      let Some(header) = RecordHeader::decode_opt(slice) else {
        break;
      };

      if header.is_pad() || header.is_null() {
        break;
      }

      let Some(rec_size) = header.checked_physical_size() else {
        break;
      };
      if offset + rec_size > self.page_size {
        break;
      }

      let key_start = HEADER_SIZE;
      let key_end = key_start + header.key_len() as usize;
      let key = &slice[key_start..key_end];
      let rc_addr = tag_read_cache_addr(page_start_addr + offset as u64);
      let prev_addr = header.address();

      if prev_addr == 0 {
        index.delete(key, rc_addr);
      } else {
        index.update_address(key, rc_addr, prev_addr);
      }

      offset += rec_size;
    }
  }

  /// 零拷贝直读 ReadCache 记录（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:FindInReadCache 与 AllocatorBase 无锁指针直读）
  pub fn with_record<R>(
    &self,
    tagged_addr: u64,
    f: impl FnOnce(&[u8], &[u8], u64) -> R,
  ) -> Option<R> {
    if !self.is_enabled || !is_read_cache_addr(tagged_addr) {
      return None;
    }

    let abs_addr = absolute_address(tagged_addr);
    let head = self.head_address.load(Acquire);
    let tail = self.tail_address.load(Acquire);

    // 检查是否处于当前有效环形内存窗口内
    if abs_addr < head || abs_addr >= tail {
      return None;
    }

    let page_id = abs_addr >> self.page_shift;
    let offset = (abs_addr & self.page_mask) as usize;
    if offset + HEADER_SIZE > self.page_size {
      return None;
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
      && let Some((key, val, prev_addr)) =
        Self::parse_record_at(page_slice, offset, self.page_size - offset)
      && self.buffer.is_page_loaded(page_id)
      && abs_addr >= self.head_address.load(Acquire)
    {
      return Some(f(key, val, prev_addr));
    }

    // 2. 慢路径安全回退：在换页临界区获取页读锁保护。
    //    锁内双重校验（对齐 HybridLog::probe_resident 的 Locked 路径口径）：head 已推进
    //    （记录滑出窗口）或槽位已换装承载其他逻辑页（加锁间隙环形回绕复用）时按未命中
    //    返回，杜绝把新页数据按旧偏移误解析——head 推进先于 clear_page 的顺序保证
    //    二者至少其一必然命中
    let page_guard = self.buffer.read_page(page_id);
    if abs_addr < self.head_address.load(Acquire) || !self.buffer.is_page_loaded(page_id) {
      return None;
    }
    // 页读锁下页字节稳定（clear_page 须取写锁互斥），单次解析即可信
    let (key, val, prev_addr) =
      Self::parse_record_at(&page_guard, offset, self.page_size - offset)?;
    Some(f(key, val, prev_addr))
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
    if !self.is_enabled || !is_read_cache_addr(tagged_addr) {
      return false;
    }
    let abs_addr = absolute_address(tagged_addr);
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

  /// 解析页内 offset 处的记录为 `(key, val, prev_addr)` 零拷贝切片（校验失败返回 None）
  #[inline(always)]
  fn parse_record_at(
    page_slice: &[u8],
    offset: usize,
    remaining_in_page: usize,
  ) -> Option<(&[u8], &[u8], u64)> {
    if remaining_in_page < HEADER_SIZE {
      return None;
    }
    let header = RecordHeader::decode_opt(&page_slice[offset..])?;

    if header.is_pad() || header.is_tombstone() {
      return None;
    }

    let rec_size = header.checked_record_size()?;
    if rec_size > remaining_in_page {
      return None;
    }

    let key_start = offset + HEADER_SIZE;
    let key_end = key_start + header.key_len() as usize;
    let val_end = key_end + header.val_len() as usize;

    let key = &page_slice[key_start..key_end];
    let val = &page_slice[key_end..val_end];
    let prev_addr = header.address();

    Some((key, val, prev_addr))
  }

  /// 顺链跳过所有 ReadCache 记录，获取底层的首个主日志逻辑地址（严格对标 Garnet SkipReadCache）
  #[inline]
  pub fn skip_read_cache(&self, mut addr: u64) -> u64 {
    let mut spins = 0;
    while is_read_cache_addr(addr) && spins < 32 {
      spins += 1;
      match self.with_record(addr, |_, _, prev| prev) {
        Some(prev) => addr = prev,
        None => return 0, // 缓存记录已滑出窗口或已失效，断链返回 0 杜绝将物理偏移当主日志地址
      }
    }
    if is_read_cache_addr(addr) {
      0 // 超过最大跃点数或环路，安全返回 0
    } else {
      addr
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

  use super::*;

  /// 门控：非 RC 地址 / 窗口内 / 未启用一律不等待（对标 ReadCacheNeedToWaitForEviction 快路径）
  #[test]
  fn gate_is_noop_for_non_rc_in_window_and_disabled() -> Result<()> {
    let rc = ReadCache::new(4096, 4, true)?;
    let index = HashIndex::new(16)?;
    let rc_addr = rc.append(b"k", b"v", 0, &index).expect("append 应成功");

    assert!(!rc.need_to_wait_for_eviction(123, || ()));
    assert!(!rc.need_to_wait_for_eviction(rc_addr, || ()));

    let off = ReadCache::new(4096, 4, false)?;
    assert!(!off.need_to_wait_for_eviction(tag_read_cache_addr(0), || ()));
    Ok(())
  }

  /// 等待协议：ClosedUntilAddress 未发布时至少续转一轮（C# 首轮 ProtectAndDrain
  /// 强制语义），驱逐方发布高水位后立即退出
  #[test]
  fn wait_blocks_until_closed_until_published() -> Result<()> {
    let rc = ReadCache::new(4096, 4, true)?;
    let index = HashIndex::new(16)?;
    let rc_addr = rc.append(b"k", b"v", 0, &index).expect("append 应成功");
    let abs_addr = absolute_address(rc_addr);

    // 模拟驱逐方完成 head 推进、cleanse 尚未发布 ClosedUntilAddress 的窗口
    rc.head_address.fetch_max(abs_addr + 1, Release);

    let rounds = AtomicU32::new(0);
    let waited = rc.need_to_wait_for_eviction(rc_addr, || {
      let n = rounds.fetch_add(1, Relaxed) + 1;
      if n == 2 {
        // 第二轮模拟驱逐方完成清洗：发布越过记录地址的高水位
        rc.closed_until_address.fetch_max(abs_addr + 1, Release);
      }
    });

    assert!(waited);
    assert_eq!(rounds.load(Relaxed), 2);
    Ok(())
  }

  /// 真实环形回绕驱逐：cleanse_page 完成后 ClosedUntilAddress 单调发布到被驱逐页末尾，
  /// 被驱逐页旧地址门控触发且仅一轮 refresh 即返回（回链头重探）
  #[test]
  fn page_turn_publishes_closed_until() -> Result<()> {
    let rc = ReadCache::new(4096, 2, true)?;
    let index = HashIndex::new(16)?;

    let mut first_rc_addr = None;
    for i in 0..4096u32 {
      let key = format!("k{i}");
      if let Some(addr) = rc.append(key.as_bytes(), b"v", 0, &index)
        && first_rc_addr.is_none()
      {
        first_rc_addr = Some(addr);
      }
      if rc.closed_until_address() > 0 {
        break;
      }
    }
    assert!(
      rc.closed_until_address() > 0,
      "环形回绕应发布 ClosedUntilAddress"
    );

    let evicted = first_rc_addr.expect("首条 RC 记录应存在");
    let rounds = AtomicU32::new(0);
    assert!(rc.need_to_wait_for_eviction(evicted, || {
      rounds.fetch_add(1, Relaxed);
    }));
    assert_eq!(rounds.load(Relaxed), 1);
    Ok(())
  }
}
