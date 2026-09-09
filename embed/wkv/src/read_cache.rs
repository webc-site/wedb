use std::{
  hint::spin_loop,
  slice::from_raw_parts_mut,
  sync::atomic::{
    AtomicU64,
    Ordering::{AcqRel, Acquire, Release},
  },
};

/// 48 位绝对地址掩码（第 0..47 位，去除了第 47 位的 ReadCache 标志）
pub use addr::ABSOLUTE_ADDRESS_MASK;
/// ReadCache 虚拟地址指示位掩码（第 47 位，严格对标 Garnet LogAddress.kIsReadCacheBitMask）
pub use addr::READ_CACHE_BIT;
use itoa::Buffer;
use parking_lot::Mutex;
use wbase::{
  addr::{self},
  align::CachePadded,
};
use whlog::{CircularPageBuffer, HybridLogConfig};
use windex::HashIndex;
use wrecord::{HEADER_SIZE, RecordHeader, encode_to_slice};

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

  /// 向 ReadCache 追加一条只读缓存记录（严格对标 Garnet TryCopyToReadCache）
  ///
  /// 返回打上 `READ_CACHE_BIT` 的虚拟地址；若未启用或单记录超出一页大小则返回 None
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

    let rec_size = HEADER_SIZE.checked_add(key.len())?.checked_add(val.len())?;
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

      if header.is_pad() || header.key_len == 0 {
        break;
      }

      let Some(rec_size) = header.checked_record_size() else {
        break;
      };
      if offset + rec_size > self.page_size {
        break;
      }

      let key_start = HEADER_SIZE;
      let key_end = key_start + header.key_len as usize;
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

  /// 零拷贝直读 ReadCache 记录（严格对标 Garnet FindInReadCache 与 AllocatorBase 无锁指针直读）
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

    // 1. 无锁快速直读：利用 page_ids 双重校验彻底规避 RwLock 原子计数器颠簸
    if let Some(page_slice) = unsafe { self.buffer.try_read_page_unlocked(page_id) }
      && abs_addr >= self.head_address.load(Acquire)
    {
      return Self::parse_record_and_call(&page_slice[offset..], self.page_size - offset, f);
    }

    // 2. 慢路径安全回退：在换页临界区获取页读锁保护
    let page_guard = self.buffer.read_page(page_id);
    if abs_addr < self.head_address.load(Acquire) {
      return None;
    }
    Self::parse_record_and_call(&page_guard[offset..], self.page_size - offset, f)
  }

  #[inline(always)]
  fn parse_record_and_call<R>(
    page_slice: &[u8],
    remaining_in_page: usize,
    f: impl FnOnce(&[u8], &[u8], u64) -> R,
  ) -> Option<R> {
    if remaining_in_page < HEADER_SIZE {
      return None;
    }
    let header = RecordHeader::decode_opt(page_slice)?;

    if header.is_pad() || header.is_tombstone() {
      return None;
    }

    let rec_size = header.checked_record_size()?;
    if rec_size > remaining_in_page {
      return None;
    }

    let key_start = HEADER_SIZE;
    let key_end = key_start + header.key_len as usize;
    let val_end = key_end + header.val_len as usize;

    let key = &page_slice[key_start..key_end];
    let val = &page_slice[key_end..val_end];
    let prev_addr = header.address();

    Some(f(key, val, prev_addr))
  }

  /// 顺链跳过所有 ReadCache 记录，获取底层的首个主日志逻辑地址（严格对标 Garnet SkipReadCache）
  #[inline]
  pub fn skip_read_cache(&self, mut addr: u64) -> u64 {
    let mut spins = 0;
    while is_read_cache_addr(addr) && spins < 32 {
      spins += 1;
      match self.with_record(addr, |_k, _v, prev| prev) {
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
