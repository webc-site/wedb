use std::{
  fmt,
  hint::unreachable_unchecked,
  sync::atomic::{AtomicU64, Ordering},
};

use crate::{
  Error, Result,
  bin::{BEST_FIT_SCAN_ALL, FreeRecordBin},
  record::{FreeRecord, SetStatus},
};

/// 默认分桶尺寸阶梯（对标 C# PowerOf2BinsRevivificationSettings：以 2 的幂次递增，起点
/// `RevivificationBin.MinRecordSize` = 16，与 wrecord `HEADER_SIZE` = 16 的最小记录一致，
/// 直至最大内联尺寸 65535B；缺 16/32 档会使最小记录落入 64B 桶，内部碎片最高达 75%）
pub const DEFAULT_BIN_SIZES: [u32; 13] = [
  16,
  32,
  64,
  128,
  256,
  512,
  1024,
  2048,
  4096,
  8192,
  16384,
  32768,
  FreeRecord::MAX_INLINE_SIZE,
];

const _: () = {
  let mut i = 0;
  let mut prev = 0;
  while i < DEFAULT_BIN_SIZES.len() {
    let s = DEFAULT_BIN_SIZES[i];
    assert!(s > prev);
    assert!(s <= FreeRecord::MAX_INLINE_SIZE);
    prev = s;
    i += 1;
  }
};

/// 每个分桶默认槽位数（对标 Garnet DefaultRecordsPerBin = 256）
pub const DEFAULT_BIN_CAPACITY: usize = 256;

/// 槽位复活分配结果，协同上层系统（如 wedb_hlog / wedb_store）的松弛填充（filler_bytes）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RevivAllocation {
  /// 复活槽位的逻辑地址
  pub address: u64,
  /// 槽位实际物理分配尺寸
  pub actual_size: u32,
  /// 申请记录所需尺寸
  pub required_size: u32,
  /// 内部碎片松弛填充字节（filler_bytes = actual_size - required_size）
  pub filler_bytes: u32,
}

impl RevivAllocation {
  /// 构造复活分配结果
  #[inline]
  pub const fn new(address: u64, actual_size: u32, required_size: u32) -> Self {
    Self {
      address,
      actual_size,
      required_size,
      filler_bytes: actual_size.saturating_sub(required_size),
    }
  }

  /// 是否为零浪费的精确匹配
  #[inline]
  pub const fn is_exact(&self) -> bool {
    self.filler_bytes == 0
  }
}

impl fmt::Display for RevivAllocation {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "RevivAllocation(addr: {:#x}, size: {} [need: {}, filler: {}])",
      self.address, self.actual_size, self.required_size, self.filler_bytes
    )
  }
}

/// 槽位复活回收池统计指标
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RevivStats {
  /// 总投入调用次数
  pub put_count: u64,
  /// 总取出调用次数
  pub take_count: u64,
  /// 成功复活命中次数
  pub hit_count: u64,
  /// 丢弃槽位次数（包括边界失效过滤、分桶溢出淘汰、取出时就地废弃、覆盖替换的过期槽位）
  pub drop_count: u64,
}

impl RevivStats {
  /// 获取复活命中率（0.0 ~ 1.0）
  #[inline]
  pub fn hit_rate(&self) -> f64 {
    if self.take_count == 0 {
      0.0
    } else {
      self.hit_count as f64 / self.take_count as f64
    }
  }

  /// 获取复活申请失败次数
  #[inline]
  pub const fn failed_takes(&self) -> u64 {
    self.take_count.saturating_sub(self.hit_count)
  }
}

impl fmt::Display for RevivStats {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "RevivStats(puts: {}, takes: {}, hits: {} [{:.1}%], drops: {})",
      self.put_count,
      self.take_count,
      self.hit_count,
      self.hit_rate() * 100.0,
      self.drop_count
    )
  }
}

/// 内存与日志槽位复活回收池（Layer 4）
///
/// 仿写 Microsoft Garnet Tsavorite 的 FreeRecordPool：
/// - 多尺寸分级分桶管理空闲槽位
/// - 无锁 CAS 并发存取
/// - 支持就地废弃低于 min_address（只读区/截断区）的失效记录
/// - 自动统计指标 (put_count, take_count, hit_count, drop_count)
/// - `min_address` 单调性契约：由上层随日志推进单调传入（如 read_only_address）；非单调传入
///   不会破坏池不变量（active_count 仍由成功 CAS 背书），仅放宽当次过滤或收窄清理范围，
///   已清零槽位不可因 min_address 回退而复活
#[derive(Debug)]
pub struct FreeRecordPool {
  /// 多尺寸分级分桶列表（按 max_size 升序排列）
  pub bins: Box<[FreeRecordBin]>,
  /// 投入槽位调用计数
  pub put_count: AtomicU64,
  /// 申请复活调用计数
  pub take_count: AtomicU64,
  /// 成功复活命中计数
  pub hit_count: AtomicU64,
  /// 丢弃或失效槽位计数
  pub drop_count: AtomicU64,
}

impl FreeRecordPool {
  /// 创建使用默认分桶尺寸与默认容量的复活池
  ///
  /// 常量 `DEFAULT_BIN_SIZES` / `DEFAULT_BIN_CAPACITY` 的合法性已由编译期断言与
  /// [`Self::with_bin_sizes_and_scan_limit`] 的校验规则保证，此处 expect 绝不触发。
  pub fn new() -> Self {
    match Self::with_capacity(DEFAULT_BIN_CAPACITY) {
      Ok(s) => s,
      Err(_) => unsafe { unreachable_unchecked() },
    }
  }

  /// 创建使用自定义容量（每个分桶）的复活池
  pub fn with_capacity(bin_capacity: usize) -> Result<Self> {
    Self::with_bin_sizes(&DEFAULT_BIN_SIZES, bin_capacity)
  }

  /// 创建使用自定义分桶尺寸阶梯与容量的复活池（默认全局最优适配）
  #[inline]
  pub fn with_bin_sizes(bin_sizes: &[u32], bin_capacity: usize) -> Result<Self> {
    Self::with_bin_sizes_and_scan_limit(bin_sizes, bin_capacity, BEST_FIT_SCAN_ALL)
  }

  /// 创建使用自定义分桶尺寸阶梯、容量与最优适配扫描上限的复活池
  pub fn with_bin_sizes_and_scan_limit(
    bin_sizes: &[u32],
    bin_capacity: usize,
    best_fit_scan_limit: usize,
  ) -> Result<Self> {
    if bin_sizes.is_empty() {
      return Err(Error::EmptyBinSizes);
    }
    if bin_capacity == 0 {
      return Err(Error::InvalidCapacity);
    }
    let mut prev = 0;
    for &size in bin_sizes {
      if size == 0 || size <= prev {
        return Err(Error::UnsortedBinSizes);
      }
      if size > FreeRecord::MAX_INLINE_SIZE {
        return Err(Error::SizeOverflow(size));
      }
      prev = size;
    }
    let bins = bin_sizes
      .iter()
      .map(|&s| FreeRecordBin::with_scan_limit(s, bin_capacity, best_fit_scan_limit))
      .collect::<Box<[_]>>();
    Ok(Self {
      bins,
      put_count: AtomicU64::new(0),
      take_count: AtomicU64::new(0),
      hit_count: AtomicU64::new(0),
      drop_count: AtomicU64::new(0),
    })
  }

  /// 将删除或缩减释放的记录槽位归还入池（对标 C# FreeRecordPool.TryAdd）
  ///
  /// 前置条件：调用方（wedb_hlog / wedb_store 层）须已将该地址的记录墓碑化或确认其已失效
  /// （对标 C# TryAdd 前置的 `InfoRef.TrySeal(invalidate: true)`，见 [`FreeRecord`] 并发模型说明）。
  ///
  /// - 若地址低于 min_address、地址无效或尺寸为 0 / 溢出，则拒绝存入并计入 drop_count
  /// - 若目标分桶已满，无法容纳，则返回 false 并计入 drop_count
  /// - 若成功覆盖替换了已过期槽位，返回 true 且过期槽位计入 drop_count
  #[inline]
  pub fn put(&self, address: u64, size: u32, min_address: u64) -> bool {
    self.put_count.fetch_add(1, Ordering::Relaxed);

    if address < min_address
      || address == 0
      || address > FreeRecord::ADDRESS_MASK
      || size == 0
      || size > FreeRecord::MAX_INLINE_SIZE
    {
      self.drop_count.fetch_add(1, Ordering::Relaxed);
      return false;
    }

    let Some(bin_idx) = self.find_bin_index(size) else {
      self.drop_count.fetch_add(1, Ordering::Relaxed);
      return false;
    };

    // SAFETY: find_bin_index 保证 bin_idx < self.bins.len()，且 self.bins 长度固定不变
    let bin = unsafe { self.bins.get_unchecked(bin_idx) };
    match bin.put(address, size, min_address) {
      SetStatus::InsertedEmpty => true,
      SetStatus::ReplacedExpired => {
        // 被覆盖的过期槽位计入丢弃统计
        self.drop_count.fetch_add(1, Ordering::Relaxed);
        true
      }
      SetStatus::Occupied => {
        self.drop_count.fetch_add(1, Ordering::Relaxed);
        false
      }
    }
  }

  /// 申请复活可用槽位并指定最大向上跨桶搜索数量（0 表示仅搜索目标尺寸所属桶，对应 Garnet numberOfBinsToSearch）
  #[inline]
  pub fn take_with_max_bins(
    &self,
    required_size: u32,
    min_address: u64,
    max_bins_to_search: usize,
  ) -> Option<(u64, u32)> {
    self.take_count.fetch_add(1, Ordering::Relaxed);

    if required_size == 0 || required_size > FreeRecord::MAX_INLINE_SIZE {
      return None;
    }

    let start_bin = self.find_bin_index(required_size)?;
    let search_end = (start_bin + 1)
      .saturating_add(max_bins_to_search)
      .min(self.bins.len());

    // SAFETY: start_bin < self.bins.len() (来自 find_bin_index)，search_end 已被 min 钳位至 bins.len()
    let search_bins = unsafe { self.bins.get_unchecked(start_bin..search_end) };
    for bin in search_bins {
      let (res, purged) = bin.take_best_fit(required_size, min_address);
      if purged > 0 {
        self.drop_count.fetch_add(purged as u64, Ordering::Relaxed);
      }
      if let Some((addr, size)) = res {
        self.hit_count.fetch_add(1, Ordering::Relaxed);
        return Some((addr, size));
      }
    }

    None
  }

  /// 查找最适配的空闲槽位并原子取出（向上跨所有可用分桶搜索）
  ///
  /// - 从满足 `bin.max_size >= required_size` 的最小分桶开始向上逐级分桶查找
  /// - 扫描中遇到的低于 min_address（如已滑入只读区或已截断）的槽位自动就地原子清零废弃并计入 drop_count
  /// - 成功取出返回 `Some((address, actual_size))` 并递增 hit_count
  /// - 无可用槽位返回 `None`
  /// - 争抢回退语义（非错误）：极端 CAS 争抢下分桶内有限轮重试可能耗尽，即便桶中
  ///   仍有满足尺寸的候选也可能返回 None——调用方（wedb_hlog 等）据此回退追加
  ///   分配新地址，属正确的性能取舍而非故障。对照 C# Revivification 的 per-worker
  ///   混合策略：C# TryTake 失败同样立即走日志分配器，回退追加是两侧一致的正确语义
  #[inline]
  pub fn take(&self, required_size: u32, min_address: u64) -> Option<(u64, u32)> {
    self.take_with_max_bins(required_size, min_address, usize::MAX)
  }

  /// 查找最适配的空闲槽位并返回包含松弛填充信息的结果结构
  #[inline]
  pub fn take_allocation(&self, required_size: u32, min_address: u64) -> Option<RevivAllocation> {
    self
      .take(required_size, min_address)
      .map(|(addr, actual_size)| RevivAllocation::new(addr, actual_size, required_size))
  }

  /// 主动清理已滑入冷区或只读区的失效槽位
  ///
  /// 返回被清理废弃的槽位总数，并将其累计计入 drop_count
  #[inline]
  pub fn purge_below(&self, min_address: u64) -> usize {
    let total_purged: usize = self
      .bins
      .iter()
      .map(|bin| bin.purge_below(min_address))
      .sum();
    if total_purged > 0 {
      self
        .drop_count
        .fetch_add(total_purged as u64, Ordering::Relaxed);
    }
    total_purged
  }

  /// 获取统计指标快照
  #[inline]
  pub fn stats(&self) -> RevivStats {
    RevivStats {
      put_count: self.put_count.load(Ordering::Relaxed),
      take_count: self.take_count.load(Ordering::Relaxed),
      hit_count: self.hit_count.load(Ordering::Relaxed),
      drop_count: self.drop_count.load(Ordering::Relaxed),
    }
  }

  /// 获取累计归还投入次数
  #[inline]
  pub fn put_count(&self) -> u64 {
    self.put_count.load(Ordering::Relaxed)
  }

  /// 获取累计申请复活次数
  #[inline]
  pub fn take_count(&self) -> u64 {
    self.take_count.load(Ordering::Relaxed)
  }

  /// 获取累计成功复活命中次数
  #[inline]
  pub fn hit_count(&self) -> u64 {
    self.hit_count.load(Ordering::Relaxed)
  }

  /// 获取累计失效或溢出丢弃槽位次数
  #[inline]
  pub fn drop_count(&self) -> u64 {
    self.drop_count.load(Ordering::Relaxed)
  }

  /// 重置统计指标
  #[inline]
  pub fn reset_stats(&self) {
    self.put_count.store(0, Ordering::Relaxed);
    self.take_count.store(0, Ordering::Relaxed);
    self.hit_count.store(0, Ordering::Relaxed);
    self.drop_count.store(0, Ordering::Relaxed);
  }

  /// 根据记录尺寸查找首个匹配分桶的索引（利用单调性二分查找，O(log N)）
  #[inline]
  pub fn find_bin_index(&self, size: u32) -> Option<usize> {
    let idx = self.bins.partition_point(|bin| bin.max_size < size);
    (idx < self.bins.len()).then_some(idx)
  }

  /// 获取分桶总数
  #[inline]
  pub fn bin_count(&self) -> usize {
    self.bins.len()
  }

  /// 获取当前所有分桶中活跃槽位总数（O(B) 复杂度，仅汇总各分桶活跃计数）
  #[inline]
  pub fn total_active_records(&self) -> usize {
    self.bins.iter().map(|b| b.len()).sum()
  }

  /// 判断回收池是否为空（O(B) 复杂度，短路快速判断）
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.bins.iter().all(|b| b.is_empty())
  }

  /// 清空所有分桶
  #[inline]
  pub fn clear(&self) {
    for bin in &self.bins {
      bin.clear();
    }
  }
}

impl Default for FreeRecordPool {
  fn default() -> Self {
    Self::new()
  }
}
