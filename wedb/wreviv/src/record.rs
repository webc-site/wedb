use std::{
  fmt,
  sync::atomic::{AtomicU64, Ordering},
};

use wbase::addr::{ADDRESS_BITS, ADDRESS_MASK, INVALID_ADDRESS};

/// 槽位写入状态机（闭环三态）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetStatus {
  /// 成功存入空槽位
  InsertedEmpty,
  /// 成功覆盖已过期的槽位
  ReplacedExpired,
  /// 未能写入：槽位已被有效记录占用、分桶已满、单次 CompareExchange 竞争失败，或参数非法（地址越界 / 尺寸为 0 / 尺寸超限）被防御性拒绝
  Occupied,
}

/// 64 位紧凑无锁空闲记录槽位（对标 C# Tsavorite FreeRecord.cs）
///
/// 内存排布（小端 64 位整型）：
/// - `[0..48)`：48 位逻辑/物理地址（address，与 wrecord RecordHeader 完全对齐，最大寻址 256TB）
/// - `[48..64)`：16 位记录尺寸（size，最大内联尺寸 65535B = 64KB - 1）
///
/// 并发模型裁决（对标 C# `logRecord.InfoRef.TrySeal(invalidate: true)` 前置协议）：
/// - 本槽位通过单字 64 位 CAS CompareExchange 实现无锁原子存取与置空，结合地址的单调物理分配消除链表 ABA 隐患；
/// - 投入复活池前，调用方需先通过 `wrecord::RecordHeader::try_seal`（或 `wedb_hlog` 提供的类似入口）对记录头进行原子密封，避免并发读者读到非法中间态；
/// - 复活覆写时，原位更新操作会自动解除记录的密封位。
#[derive(Debug, Default)]
#[repr(transparent)]
pub struct FreeRecord(pub AtomicU64);

impl FreeRecord {
  /// 地址位长度（48位，对齐 wrecord 与 wbase::addr）
  pub const ADDRESS_BITS: u32 = ADDRESS_BITS;
  /// 地址掩码（低 48 位: 0x0000_FFFF_FFFF_FFFF）
  pub const ADDRESS_MASK: u64 = ADDRESS_MASK;

  /// 尺寸位长度（16位）
  pub const SIZE_BITS: u32 = 16;
  /// 尺寸位偏移量（48位）
  pub const SIZE_SHIFT: u32 = Self::ADDRESS_BITS;
  /// 尺寸掩码（由 SIZE_BITS 编译期推导，0xFFFF）
  pub const SIZE_MASK: u64 = (1 << Self::SIZE_BITS) - 1;
  /// 最大支持的内联尺寸（65535 字节；对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:kSizeMask）
  pub const MAX_INLINE_SIZE: u32 = Self::SIZE_MASK as u32;

  /// 空槽位原始值（0L，对齐 Tsavorite LogAddress.kInvalidAddress）
  pub const EMPTY_WORD: u64 = INVALID_ADDRESS;

  /// 构造一个空槽位（未分配状态）
  #[inline]
  pub const fn empty() -> Self {
    Self(AtomicU64::new(Self::EMPTY_WORD))
  }

  /// 将 48 位地址与 16 位尺寸打包为 64 位无符号整数
  ///
  /// # 前置条件（违反即缺陷，绝不允许静默截断）
  ///
  /// - `size <= MAX_INLINE_SIZE`：debug 构建直接断言失败；release 构建虽按 16 位
  ///   掩码圆整，但禁止依赖该行为——调用方须先行校验（入口 [`Self::set`] 已防御，
  ///   分桶构造 [`crate::FreeRecordBin`] 亦钳位）
  /// - `address` 超出 48 位按掩码取址属规范行为（48 位地址空间的定义性截断，
  ///   与 wrecord RecordHeader 口径一致）
  #[inline]
  pub const fn pack(address: u64, size: u32) -> u64 {
    debug_assert!(
      size <= Self::MAX_INLINE_SIZE,
      "FreeRecord::pack 尺寸超出 16 位内联上限，静默截断被禁止"
    );
    ((size as u64 & Self::SIZE_MASK) << Self::SIZE_SHIFT) | (address & Self::ADDRESS_MASK)
  }

  /// 从 64 位无符号整数解包出 48 位地址与 16 位尺寸
  #[inline(always)]
  pub const fn unpack(raw: u64) -> (u64, u32) {
    (Self::raw_address(raw), Self::raw_size(raw))
  }

  /// 从 64 位原生数值中快速提取 48 位地址（const fn，零右移）
  #[inline(always)]
  pub const fn raw_address(raw: u64) -> u64 {
    raw & Self::ADDRESS_MASK
  }

  /// 从 64 位原生数值中快速提取 16 位尺寸（const fn）
  #[inline(always)]
  pub const fn raw_size(raw: u64) -> u32 {
    (raw >> Self::SIZE_SHIFT) as u32
  }

  /// 获取底层的 u64 原生数值（Acquire 序）
  #[inline]
  pub fn raw(&self) -> u64 {
    self.0.load(Ordering::Acquire)
  }

  /// 解包获取当前地址（Acquire 序）
  #[inline]
  pub fn address(&self) -> u64 {
    Self::raw_address(self.raw())
  }

  /// 解包获取当前记录尺寸（Acquire 序）
  #[inline]
  pub fn size(&self) -> u32 {
    Self::raw_size(self.raw())
  }

  /// 解包获取当前地址与尺寸（Acquire 序）
  #[inline]
  pub fn get(&self) -> (u64, u32) {
    Self::unpack(self.raw())
  }

  /// 判断当前槽位是否为空（Acquire 序）
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.raw() == Self::EMPTY_WORD
  }

  /// 尝试将记录归还入该槽位（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:Set）
  ///
  /// - `SetStatus::InsertedEmpty`：成功写入原为空的槽位
  /// - `SetStatus::ReplacedExpired`：成功覆盖已过期的槽位
  /// - `SetStatus::Occupied`：槽位已被有效记录占用、单次 CAS 竞争失败，或因入参非法（地址为 0、低于 min_address、超过 48 位，尺寸为 0 或超限）被防御性拒绝
  ///
  /// 与 C# 同形：一次 `load` 取快照（对位 `var oldRecord = this`）+ 一次
  /// `compare_exchange` 提交（对位 `Interlocked.CompareExchange`），竞争失败即刻返回
  /// `Occupied`，绝不在同一槽位自旋重试。
  /// 有界性来源是调用侧的分桶扫描：`FreeRecordBin::put` 逐槽 `Occupied => continue`、
  /// `FreeRecordPool::put` 逐级分桶 continue，即 C# 失败后换下一槽位的语义。
  #[inline]
  pub fn set(&self, address: u64, size: u32, min_address: u64) -> SetStatus {
    // 边界安全防御：地址为 0、低于 min_address、超过 48 位，或尺寸为 0、超过 16 位内联尺寸
    if address < min_address
      || address == 0
      || address > Self::ADDRESS_MASK
      || size == 0
      || size > Self::MAX_INLINE_SIZE
    {
      return SetStatus::Occupied;
    }

    // 单次快照：非空且槽内地址仍有效（>= min_address）即占用，直接让调用方换槽
    let current = self.0.load(Ordering::Acquire);
    let is_empty = current == Self::EMPTY_WORD;
    if !is_empty && Self::raw_address(current) >= min_address {
      return SetStatus::Occupied;
    }

    // 单次强 CAS：失败（含并发投入/取走同一槽位）一律按 Occupied 上报，不回读、不重试
    let new_val = Self::pack(address, size);
    if self
      .0
      .compare_exchange(current, new_val, Ordering::AcqRel, Ordering::Acquire)
      .is_err()
    {
      return SetStatus::Occupied;
    }
    if is_empty {
      SetStatus::InsertedEmpty
    } else {
      SetStatus::ReplacedExpired
    }
  }

  /// 精准原子取出：若当前原生值等于预期值，将其 CAS 置为 EMPTY_WORD
  #[inline]
  pub fn try_take_exact(&self, expected_raw: u64) -> bool {
    self
      .0
      .compare_exchange(
        expected_raw,
        Self::EMPTY_WORD,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_ok()
  }

  /// 主动清理：若当前槽位非空且低于 min_address，尝试原子置零淘汰
  #[inline]
  pub fn try_purge_below(&self, min_address: u64) -> bool {
    let mut current = self.0.load(Ordering::Acquire);
    loop {
      if current == Self::EMPTY_WORD {
        return false;
      }
      let addr = Self::raw_address(current);
      if addr >= min_address {
        return false;
      }
      match self.0.compare_exchange_weak(
        current,
        Self::EMPTY_WORD,
        Ordering::Release,
        Ordering::Acquire,
      ) {
        Ok(_) => return true,
        Err(actual) => current = actual,
      }
    }
  }

  /// 原子置零清空槽位
  #[inline]
  pub fn clear(&self) {
    self.0.store(Self::EMPTY_WORD, Ordering::Release);
  }
}

impl fmt::Display for FreeRecord {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let raw = self.raw();
    if raw == Self::EMPTY_WORD {
      write!(f, "FreeRecord(empty)")
    } else {
      let (addr, size) = Self::unpack(raw);
      write!(f, "FreeRecord(addr: {addr:#x}, size: {size})")
    }
  }
}

const _: () = {
  assert!(FreeRecord::ADDRESS_BITS == 48);
  assert!(FreeRecord::SIZE_BITS == 16);
  assert!(FreeRecord::ADDRESS_BITS + FreeRecord::SIZE_BITS == 64);
  assert!(FreeRecord::ADDRESS_MASK == (1u64 << FreeRecord::ADDRESS_BITS) - 1);
  assert!(FreeRecord::SIZE_MASK == (1u64 << FreeRecord::SIZE_BITS) - 1);
  assert!(FreeRecord::MAX_INLINE_SIZE == 65535);
  let p = FreeRecord::pack(0x1234_5678, 100);
  let (a, s) = FreeRecord::unpack(p);
  assert!(a == 0x1234_5678);
  assert!(s == 100);
};
