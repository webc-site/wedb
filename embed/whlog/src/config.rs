use crate::error::{Error, Result};

/// 默认单页容量（64KB）
pub const DEFAULT_PAGE_SIZE: usize = 64 * 1024;

/// 默认环形缓冲区页数（16 页）
pub const DEFAULT_NUM_PAGES: usize = 16;

/// 默认可变区比例（0.5）
pub const DEFAULT_MUTABLE_FRACTION: f64 = 0.5;

/// 默认起始有效逻辑地址（64 字节，前 64 字节保留为特殊标记）
pub const DEFAULT_INITIAL_ADDRESS: u64 = 64;

/// 扇区基准对齐大小（4096 字节）
pub const SECTOR_ALIGNMENT: usize = 4096;

/// 单页容量的合法上界（Pad 填充标记头的 `val_len` 为 u32 字段，
/// 页容量超出会使剩余空间长度编码静默截断；对标 libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:pageSize 为 int 的约束）
const MAX_PAGE_SIZE: usize = (u32::MAX as usize) + 1;

/// ReadOnlyAddress 滞后比例定点化的小数位宽（20 位定点，分辨率约百万分之一）
pub const RO_LAG_BITS: u32 = 20;

/// ReadOnlyAddress 滞后比例定点化分母（2^20）
pub const RO_LAG_DENOM: u64 = 1 << RO_LAG_BITS;

/// 将可变区比例换算为定点滞后分子：`ro_offset = (memory_span * ro_lag_num) >> RO_LAG_BITS`
///
/// 构造期预计算，热路径（换页触发的 ReadOnlyAddress 推进）零 f64 乘除。
#[inline]
pub fn ro_lag_num_from_fraction(mutable_fraction: f64) -> u64 {
  ((1.0 - mutable_fraction) * RO_LAG_DENOM as f64) as u64
}

/// HybridLog 混合日志核心配置
#[derive(Debug, Clone, PartialEq)]
pub struct HybridLogConfig {
  /// 单页容量（字节数，必须为 2 的幂且是 4096 的整数倍）
  pub page_size: usize,
  /// 环形缓冲区页数（必须为 2 的幂）
  pub num_pages: usize,
  /// 内存中可变区所占比例（范围为 (0.0, 1.0]）
  pub mutable_fraction: f64,
  /// 初始 TailAddress（必须 >= 64）
  pub initial_address: u64,
  /// 只读区滞后比例定点分子（构造期由 mutable_fraction 预计算，热路径零 f64）
  pub ro_lag_num: u64,
}

impl Default for HybridLogConfig {
  fn default() -> Self {
    Self {
      page_size: DEFAULT_PAGE_SIZE,
      num_pages: DEFAULT_NUM_PAGES,
      mutable_fraction: DEFAULT_MUTABLE_FRACTION,
      initial_address: DEFAULT_INITIAL_ADDRESS,
      ro_lag_num: ro_lag_num_from_fraction(DEFAULT_MUTABLE_FRACTION),
    }
  }
}

impl HybridLogConfig {
  /// 默认配置常量
  pub const DEFAULT: Self = Self {
    page_size: DEFAULT_PAGE_SIZE,
    num_pages: DEFAULT_NUM_PAGES,
    mutable_fraction: DEFAULT_MUTABLE_FRACTION,
    initial_address: DEFAULT_INITIAL_ADDRESS,
    ro_lag_num: RO_LAG_DENOM / 2,
  };
  /// 创建并校验配置项
  pub fn new(page_size: usize, num_pages: usize, mutable_fraction: f64) -> Result<Self> {
    Self::with_initial_address(
      page_size,
      num_pages,
      mutable_fraction,
      DEFAULT_INITIAL_ADDRESS,
    )
  }

  /// 创建并指定初始地址
  pub fn with_initial_address(
    page_size: usize,
    num_pages: usize,
    mutable_fraction: f64,
    initial_address: u64,
  ) -> Result<Self> {
    let mut ibuf = itoa::Buffer::new();
    if !page_size.is_power_of_two() {
      let mut msg = String::from("page_size 必须为 2 的幂，当前为 ");
      msg.push_str(ibuf.format(page_size));
      return Err(Error::InvalidConfig(msg));
    }
    if !page_size.is_multiple_of(SECTOR_ALIGNMENT) {
      let mut msg = String::from("page_size 必须是 ");
      msg.push_str(ibuf.format(SECTOR_ALIGNMENT));
      msg.push_str(" 的整数倍，当前为 ");
      msg.push_str(ibuf.format(page_size));
      return Err(Error::InvalidConfig(msg));
    }
    if page_size > MAX_PAGE_SIZE {
      let mut msg = String::from("page_size 超过上界 ");
      msg.push_str(ibuf.format(MAX_PAGE_SIZE));
      msg.push_str("，当前为 ");
      msg.push_str(ibuf.format(page_size));
      return Err(Error::InvalidConfig(msg));
    }
    if !num_pages.is_power_of_two() {
      let mut msg = String::from("num_pages 必须为非零且为 2 的幂，当前为 ");
      msg.push_str(ibuf.format(num_pages));
      return Err(Error::InvalidConfig(msg));
    }
    if !(mutable_fraction > 0.0 && mutable_fraction <= 1.0) {
      let mut fbuf = zmij::Buffer::new();
      let mut msg = String::from("mutable_fraction 必须在 (0.0, 1.0] 区间内，当前为 ");
      msg.push_str(fbuf.format(mutable_fraction));
      return Err(Error::InvalidConfig(msg));
    }
    if initial_address < DEFAULT_INITIAL_ADDRESS {
      let mut msg = String::from("initial_address 必须 >= ");
      msg.push_str(ibuf.format(DEFAULT_INITIAL_ADDRESS));
      msg.push_str("，当前为 ");
      msg.push_str(ibuf.format(initial_address));
      return Err(Error::InvalidConfig(msg));
    }

    Ok(Self {
      page_size,
      num_pages,
      mutable_fraction,
      initial_address,
      ro_lag_num: ro_lag_num_from_fraction(mutable_fraction),
    })
  }

  /// 单页地址偏移所占位数（例如 64KB 为 16 位）
  #[inline]
  /// garnet相对路径:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:LogPageSizeBits
  pub const fn page_bits(&self) -> u32 {
    self.page_size.trailing_zeros()
  }

  /// 页内偏移掩码（例如 64KB 为 0xFFFF）
  #[inline]
  pub const fn page_mask(&self) -> u64 {
    (self.page_size - 1) as u64
  }

  /// 环形页槽位掩码（例如 16 页为 0b1111 = 15）
  #[inline]
  pub const fn num_pages_mask(&self) -> usize {
    self.num_pages - 1
  }

  /// 内存环形缓冲区总字节容量
  #[inline]
  pub const fn total_buffer_size(&self) -> usize {
    self.page_size * self.num_pages
  }

  /// 根据逻辑地址计算所在逻辑页号
  #[inline]
  /// garnet相对路径:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetPageIndexForAddress

  /// garnet相对路径:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetPageSize
  #[inline]
  pub const fn page_size(&self) -> usize {
    1 << self.page_bits()
  }

  /// garnet相对路径:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetAddressOfStartOfPageOfAddress
  #[inline]
  pub const fn get_address_of_start_of_page(&self, addr: u64) -> u64 {
    addr & !self.page_mask()
  }

  /// garnet相对路径:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetFirstValidLogicalAddressOnPage
  #[inline]
  pub const fn get_first_valid_logical_address_on_page(&self, page_id: u64) -> u64 {
    let mut addr = page_id << self.page_bits();
    if addr == 0 {
      // Address 0 is invalid
      addr = 1;
    }
    addr
  }

  /// garnet相对路径:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetPageIndexForAddress
  pub const fn page_id(&self, addr: u64) -> u64 {
    addr >> self.page_bits()
  }

  /// 根据逻辑地址计算其在页内的字节偏移
  #[inline]
  /// garnet相对路径:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:GetOffsetOnPage
  pub const fn page_offset(&self, addr: u64) -> usize {
    (addr & self.page_mask()) as usize
  }

  /// 根据逻辑页号计算该页起始逻辑地址
  #[inline]
  pub const fn page_start_address(&self, page_id: u64) -> u64 {
    page_id << self.page_bits()
  }

  /// 根据逻辑地址计算在环形缓冲区中的页槽位索引
  #[inline]
  pub const fn page_slot(&self, addr: u64) -> usize {
    ((addr >> self.page_bits()) as usize) & self.num_pages_mask()
  }

  /// 仿照 Tsavorite 算法，基于当前 HeadAddress、TailAddress 与 mutable_fraction
  /// 计算推荐的 ReadOnlyAddress（向低位页边界对齐）
  ///
  /// 滞后比例已定点化（`ro_lag_num`，2^20 分母），热路径零 f64 乘除；
  /// u128 中间乘法杜绝大跨度地址空间的乘法溢出。
  /// garnet相对路径:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:CalculateReadOnlyAddress
  pub const fn calculate_read_only_address(&self, head: u64, tail: u64) -> u64 {
    if head >= tail {
      return tail;
    }
    // 当所有数据仍在第一页内时，只读区不移动，保持在 head
    if tail <= self.page_size as u64 {
      return head;
    }

    let memory_span = tail - head;
    // 内存中低段 (1 - mutable_fraction) 归为只读区
    let ro_offset = ((memory_span as u128 * self.ro_lag_num as u128) >> RO_LAG_BITS) as u64;
    let target = head + ro_offset;

    // 向下对齐到 page_size 边界
    let aligned_ro = target & !(self.page_mask());

    if aligned_ro <= head {
      let head_page = self.page_id(head);
      let tail_page = self.page_id(tail);
      if (head & self.page_mask()) <= DEFAULT_INITIAL_ADDRESS || head_page == tail_page {
        head
      } else {
        self.page_start_address(head_page + 1)
      }
    } else if aligned_ro < tail {
      aligned_ro
    } else {
      tail
    }
  }
}
