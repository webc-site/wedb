use std::{
  slice::from_raw_parts,
  sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use wram::AlignedBuf;

use crate::{
  config::{HybridLogConfig, SECTOR_ALIGNMENT},
  error::Result,
};

/// 页面槽位无效标记
const INVALID_PAGE_ID: u64 = u64::MAX;

/// 页面槽位已预清零标记（换页 owner 锁外预清零完成后写入，owner 标定时据此跳过重复 memset）
const CLAIMED_PAGE_ID: u64 = u64::MAX - 1;

/// 环形页缓冲池
///
/// 采用定长环形队列维护驻留在内存中的扇区对齐页面（AlignedBuf）。
/// 每个页均由独立的读写锁 `parking_lot::RwLock` 保护，实现细粒度并发读写。
pub struct CircularPageBuffer {
  /// 环形页缓冲区数组
  pub pages: Box<[RwLock<AlignedBuf>]>,
  /// 各槽位物理页首地址缓存（严格对标 C# Garnet AllocatorBase.cs values 裸指针数组，消除多级指针寻址）
  raw_pages: Box<[*mut u8]>,
  /// 各槽位当前承载的逻辑页号
  page_ids: Box<[AtomicU64]>,
  /// 单页容量（字节数）
  pub page_size: usize,
  /// 单页地址偏移位数
  pub page_bits: u32,
  /// 页内偏移掩码
  pub page_mask: u64,
  /// 环形页槽位总数
  pub num_pages: usize,
  /// 槽位索引掩码
  pub num_pages_mask: usize,
}

unsafe impl Send for CircularPageBuffer {}
unsafe impl Sync for CircularPageBuffer {}

impl CircularPageBuffer {
  /// 根据配置创建并初始化环形页缓冲池
  pub fn new(config: &HybridLogConfig) -> Result<Self> {
    let mut pages_vec = Vec::with_capacity(config.num_pages);
    let mut raw_pages_vec = Vec::with_capacity(config.num_pages);
    let mut page_ids_vec = Vec::with_capacity(config.num_pages);

    for _ in 0..config.num_pages {
      let mut buf = AlignedBuf::zeroed(config.page_size, SECTOR_ALIGNMENT)?;
      raw_pages_vec.push(buf.as_mut_buf_ptr());
      pages_vec.push(RwLock::new(buf));
      page_ids_vec.push(AtomicU64::new(INVALID_PAGE_ID));
    }

    Ok(Self {
      pages: pages_vec.into_boxed_slice(),
      raw_pages: raw_pages_vec.into_boxed_slice(),
      page_ids: page_ids_vec.into_boxed_slice(),
      page_size: config.page_size,
      page_bits: config.page_bits(),
      page_mask: config.page_mask(),
      num_pages: config.num_pages,
      num_pages_mask: config.num_pages_mask(),
    })
  }

  /// 根据逻辑页号计算环形槽位索引
  #[inline]
  pub const fn page_idx(&self, page_id: u64) -> usize {
    (page_id as usize) & self.num_pages_mask
  }

  /// 根据逻辑地址计算环形槽位索引
  #[inline]
  pub const fn slot_for_address(&self, addr: u64) -> usize {
    ((addr >> self.page_bits) as usize) & self.num_pages_mask
  }

  /// 根据逻辑地址计算页内偏移
  #[inline]
  pub const fn offset_in_page(&self, addr: u64) -> usize {
    (addr & self.page_mask) as usize
  }

  /// 获取指定逻辑页的读锁
  #[inline]
  pub fn read_page(&self, page_id: u64) -> RwLockReadGuard<'_, AlignedBuf> {
    let slot = self.page_idx(page_id);
    // SAFETY: slot 经过掩码计算约束在 [0, num_pages) 内
    unsafe { self.pages.get_unchecked(slot).read() }
  }

  /// 获取指定逻辑页的写锁
  #[inline]
  pub fn write_page(&self, page_id: u64) -> RwLockWriteGuard<'_, AlignedBuf> {
    let slot = self.page_idx(page_id);
    // SAFETY: slot 经过掩码计算约束在 [0, num_pages) 内
    unsafe { self.pages.get_unchecked(slot).write() }
  }

  /// 重置并初始化指定逻辑页（通常在换页分配或覆写新页时调用）
  #[inline]
  pub fn clear_page(&self, page_id: u64) {
    self.clear_page_from_offset(page_id, 0);
  }

  /// 加载页面数据至指定逻辑页槽位（对标 Garnet Recovery AsyncReadPagesForRecovery）
  pub fn load_page(&self, page_id: u64, data: &[u8]) {
    let slot = self.page_idx(page_id);
    unsafe {
      self
        .page_ids
        .get_unchecked(slot)
        .store(INVALID_PAGE_ID, Ordering::Release);
      let mut guard = self.pages.get_unchecked(slot).write();
      let copy_len = data.len().min(self.page_size);
      guard[..copy_len].copy_from_slice(&data[..copy_len]);
      if copy_len < self.page_size {
        guard[copy_len..].fill(0);
      }
      self
        .page_ids
        .get_unchecked(slot)
        .store(page_id, Ordering::Release);
    }
  }

  /// 清空指定逻辑页从 offset 起至页尾的内容（严格对标 C# Garnet ClearPage(pageIndex, offset)）
  ///
  /// 不递增标定代数：调用方（接管初始化）不回退 tail，在途预留依旧连续有效，
  /// 误递增会使等待线程误判预留作废而在已发布 tail 之下遗留永久零洞。
  pub fn clear_page_from_offset(&self, page_id: u64, offset: usize) {
    let slot = self.page_idx(page_id);
    unsafe {
      self
        .page_ids
        .get_unchecked(slot)
        .store(INVALID_PAGE_ID, Ordering::Release);
      let mut guard = self.pages.get_unchecked(slot).write();
      if offset < self.page_size {
        guard[offset..].fill(0);
      }
      self
        .page_ids
        .get_unchecked(slot)
        .store(page_id, Ordering::Release);
    }
  }

  /// 换页预清零：在全局换页锁窗口外执行整页 memset（对标 C# allocate-ahead 不变量）
  ///
  /// 仅当槽位仍承载上一轮同槽旧页（或为空）时才清零，并以 [`CLAIMED_PAGE_ID`] 标记"已清零待标定"；
  /// 槽位写锁保证与并发预清零者、接管初始化者互斥，[64KB memset] 代价完全移出换页临界区。
  pub fn preclear_page(&self, page_id: u64) {
    let slot = self.page_idx(page_id);
    unsafe {
      let mut guard = self.pages.get_unchecked(slot).write();
      let cur = self.page_ids.get_unchecked(slot).load(Ordering::Relaxed);
      if cur == INVALID_PAGE_ID || cur == page_id.wrapping_sub(self.num_pages as u64) {
        guard.fill(0);
        self
          .page_ids
          .get_unchecked(slot)
          .store(CLAIMED_PAGE_ID, Ordering::Release);
      }
    }
  }

  /// 换页发布前的槽位标定（须在全局换页锁内调用）：临界区内仅剩页号发布与罕见补零
  ///
  /// - 若槽位已被预清零（CLAIM 标记）或已标定，仅以 Release 发布页号；
  /// - 否则（无人预清零的罕见路径）在临界区内补零后发布。
  ///
  /// Release 页号发布先于 tail 发布，读者经 tail（Acquire）观察到记录时必然看见整页零字节。
  pub fn seal_page(&self, page_id: u64) {
    let slot = self.page_idx(page_id);
    unsafe {
      let mut guard = self.pages.get_unchecked(slot).write();
      let cur = self.page_ids.get_unchecked(slot).load(Ordering::Relaxed);
      if cur != page_id {
        if cur != CLAIMED_PAGE_ID {
          guard.fill(0);
        }
        self
          .page_ids
          .get_unchecked(slot)
          .store(page_id, Ordering::Release);
      }
    }
  }

  /// 检查指定槽位是否当前承载该逻辑页
  #[inline]
  pub fn is_page_loaded(&self, page_id: u64) -> bool {
    let slot = self.page_idx(page_id);
    // SAFETY: slot 经过掩码计算约束在 [0, num_pages) 内
    unsafe { self.page_ids.get_unchecked(slot).load(Ordering::Acquire) == page_id }
  }

  /// 标记槽位承载的逻辑页号
  #[inline]
  pub fn set_page_id(&self, page_id: u64) {
    let slot = self.page_idx(page_id);
    // SAFETY: slot 经过掩码计算约束在 [0, num_pages) 内
    unsafe {
      self
        .page_ids
        .get_unchecked(slot)
        .store(page_id, Ordering::Release)
    };
  }

  /// 在 Epoch 保护下直接获取槽位物理页的切片引用，完全绕过 RwLock 的原子计数器开销
  ///
  /// # Safety
  /// 调用方必须确保当前线程处于 `LightEpoch` 保护下，且目标页在生命周期内未被驱逐或正在被写入清空，
  /// 同时保证 `slot < self.num_pages`。
  #[inline(always)]
  pub unsafe fn page_slice_unchecked(&self, slot: usize) -> &[u8] {
    debug_assert!(slot < self.num_pages);
    // SAFETY: slot 经过掩码计算约束在 [0, num_pages) 内，
    // 调用方保证当前处于 LightEpoch 保护下且页未被清空，
    // raw_pages 指向稳定分配且已置零初始化的 AlignedBuf 底层堆内存。
    unsafe {
      let ptr = *self.raw_pages.get_unchecked(slot);
      from_raw_parts(ptr, self.page_size)
    }
  }

  /// 尝试无锁直读指定逻辑页的切片数据（严格对标 C# Garnet AllocatorBase.cs 纯指针无锁直读）
  ///
  /// 结合 double-check 逻辑页号 `page_ids` 验证有效性，
  /// 完全绕过 `RwLock` 读锁的原子计数器（`fetch_add`/`fetch_sub`）开销，杜绝多线程热页读取下的 CPU 缓存行颠簸。
  ///
  /// # Safety
  /// 调用方必须确保当前线程处于 `LightEpoch`（如 `EpochGuard` 或 `Participant`）保护下，
  /// 且读取的数据处于不可变区（`addr < read_only_address`）或调用方能保证并发安全性。
  #[inline]
  pub unsafe fn try_read_page_unlocked(&self, page_id: u64) -> Option<&[u8]> {
    let slot = self.page_idx(page_id);
    // SAFETY: slot 经过掩码计算约束在 [0, num_pages) 内
    let page_id_atomic = unsafe { self.page_ids.get_unchecked(slot) };
    if page_id_atomic.load(Ordering::Acquire) != page_id {
      return None;
    }
    // SAFETY: 在已校验槽位承载目标页的前提下获取物理页切片，
    // 并在获取后通过二次 Acquire 读取校验，确保读取期间槽位未被并发清空或复用。
    let slice = unsafe { self.page_slice_unchecked(slot) };
    if page_id_atomic.load(Ordering::Acquire) != page_id {
      return None;
    }
    Some(slice)
  }

  /// 根据逻辑地址计算物理内存裸指针（严格对标 C# Garnet `GetPhysicalAddress`）
  ///
  /// 汇编级单条偏移指令优化：直接通过 `raw_pages[slot] + offset` 完成物理指针寻址，
  /// 消除原先经由 `RwLock::data_ptr` 与 `AlignedBuf` 的多重间接寻址开销。
  ///
  /// # Safety
  /// 调用方必须确保处于 `LightEpoch` 保护下且该逻辑地址驻留在内存中。
  #[inline(always)]
  pub unsafe fn get_physical_address(&self, addr: u64) -> *const u8 {
    let slot = self.slot_for_address(addr);
    let offset = self.offset_in_page(addr);
    debug_assert!(slot < self.num_pages);
    // SAFETY: slot 经 slot_for_address 掩码运算必然 < num_pages，
    // 调用方确保 addr 处于驻留内存区间且在 LightEpoch 保护下，内存页不会被释放。
    unsafe {
      let ptr = *self.raw_pages.get_unchecked(slot);
      ptr.add(offset)
    }
  }

  /// 获取槽位物理页的可变首地址裸指针（严格对标 C# Garnet AllocatorBase.cs 写入物理切片）
  ///
  /// # Safety
  /// 调用方必须确保对切片的写入互不重叠或有并发同步保护。
  #[inline(always)]
  pub unsafe fn raw_page_ptr_mut(&self, slot: usize) -> *mut u8 {
    debug_assert!(slot < self.num_pages);
    unsafe { *self.raw_pages.get_unchecked(slot) }
  }
}
