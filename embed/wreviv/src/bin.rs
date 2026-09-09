use std::{
  fmt,
  sync::atomic::{AtomicIsize, AtomicUsize, Ordering},
};

use crate::record::{FreeRecord, SetStatus};

/// 首次适配（First-Fit）：取首个满足尺寸的槽位（对标 C# RevivificationBin.UseFirstFit = 0）
pub const USE_FIRST_FIT: usize = 0;
/// 全局最优适配（Best-Fit Scan All）：扫描全桶寻找最小浪费槽位（对标 C# RevivificationBin.BestFitScanAll）
pub const BEST_FIT_SCAN_ALL: usize = usize::MAX;

/// 定长分桶（管理特定尺寸范围的空闲槽位，支持原子 CAS 存取，防止并发锁争用）
///
/// 对标 C# Tsavorite FreeRecordBin：C# 依据 [prevBinRecordSize, RecordSize] 尺寸区间划分
/// segment 并以 GetSegmentStart 定位插入起点；本实现为既定 Rust 化决策，采用单一扁平槽位数组，
/// 依赖 best_fit_scan_limit 全桶扫描保证 Best-Fit 质量，put 以轮询游标分散写热点。
pub struct FreeRecordBin {
  /// 定长槽位数组
  pub slots: Box<[FreeRecord]>,
  /// 当前分桶中活跃记录总数（与真实非空槽位数保证最终一致）
  pub active_count: AtomicIsize,
  /// 并发插入游标（轮询分布，降低单槽位 CAS 竞争）
  pub cursor: AtomicUsize,
  /// 最优适配扫描上限（0 表示首次适配 First-Fit，usize::MAX 表示扫描全桶）
  pub best_fit_scan_limit: usize,
  /// 本分桶允许管理的最大记录尺寸
  pub max_size: u32,
}

impl fmt::Debug for FreeRecordBin {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("FreeRecordBin")
      .field("max_size", &self.max_size)
      .field("capacity", &self.slots.len())
      .field("active_count", &self.active_count.load(Ordering::Acquire))
      .field("best_fit_scan_limit", &self.best_fit_scan_limit)
      .finish()
  }
}

impl FreeRecordBin {
  /// First-Fit 全桶扫描轮数上限（与 C# TryTakeFirstFit 以 recordCount 折半递减至
  /// MinRecordsPerBin 的多轮重试同构——容量 256 时约 5 轮；此处取固定小常数，
  /// 极端 CAS 争抢下快速放弃，由调用方外层重试保证活性）
  const TAKE_RETRY_ROUNDS: usize = 4;

  /// 构造指定容量与扫描上限的分桶
  pub fn with_scan_limit(max_size: u32, capacity: usize, best_fit_scan_limit: usize) -> Self {
    let slots = (0..capacity)
      .map(|_| FreeRecord::empty())
      .collect::<Box<[_]>>();
    let max_size = max_size.min(FreeRecord::MAX_INLINE_SIZE);
    let best_fit_scan_limit = best_fit_scan_limit.min(capacity);
    Self {
      slots,
      active_count: AtomicIsize::new(0),
      cursor: AtomicUsize::new(0),
      best_fit_scan_limit,
      max_size,
    }
  }

  /// 构造指定容量的分桶（默认全局最优适配 BEST_FIT_SCAN_ALL）
  ///
  /// 注：C# RevivificationBin.BestFitScanLimit 默认 UseFirstFit，本实现为既定决策改用全桶最优适配。
  pub fn new(max_size: u32, capacity: usize) -> Self {
    Self::with_scan_limit(max_size, capacity, BEST_FIT_SCAN_ALL)
  }

  /// 获取当前活跃槽位数近似值（Acquire 序同步）
  #[inline]
  pub fn len(&self) -> usize {
    self.active_count.load(Ordering::Acquire).max(0) as usize
  }

  /// 获取最大记录尺寸
  #[inline]
  pub const fn max_size(&self) -> u32 {
    self.max_size
  }

  /// 获取分桶总容量
  #[inline]
  pub fn capacity(&self) -> usize {
    self.slots.len()
  }

  /// 获取最优适配扫描上限
  #[inline]
  pub const fn best_fit_scan_limit(&self) -> usize {
    self.best_fit_scan_limit
  }

  /// 判断分桶是否为空（Acquire 序同步）
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.active_count.load(Ordering::Acquire) <= 0
  }

  /// 递减活跃槽位计数（AcqRel 序；仅限内部配对成功 CAS 的增减路径调用，防止外部造成下溢）
  #[inline]
  pub(crate) fn dec_count(&self) {
    self.active_count.fetch_sub(1, Ordering::AcqRel);
  }

  /// 归还槽位入桶
  ///
  /// - `SetStatus::InsertedEmpty`：落入空槽位
  /// - `SetStatus::ReplacedExpired`：覆盖替换了已过期槽位（该过期槽位由调用方计入丢弃统计）
  /// - `SetStatus::Occupied`：参数非法，或分桶已满（全部槽位均被有效记录占用）
  #[inline]
  pub fn put(&self, address: u64, size: u32, min_address: u64) -> SetStatus {
    // 边界安全防御：低于 min_address、地址无效或超出 48 位、尺寸为 0 或超过本桶上限
    if address < min_address
      || address == 0
      || address > FreeRecord::ADDRESS_MASK
      || size == 0
      || size > self.max_size
    {
      return SetStatus::Occupied;
    }

    let len = self.slots.len();
    if len == 0 {
      return SetStatus::Occupied;
    }
    let start = self.cursor.fetch_add(1, Ordering::Relaxed) % len;
    // SAFETY: len > 0 且 start = cursor % len < len
    let (left, right) = unsafe { self.slots.split_at_unchecked(start) };
    for slot in right.iter().chain(left) {
      match slot.set(address, size, min_address) {
        SetStatus::InsertedEmpty => {
          self.active_count.fetch_add(1, Ordering::Release);
          return SetStatus::InsertedEmpty;
        }
        SetStatus::ReplacedExpired => return SetStatus::ReplacedExpired,
        SetStatus::Occupied => continue,
      }
    }
    SetStatus::Occupied
  }

  /// 首次适配（First-Fit）原子取出（对应 C# TryTakeFirstFit）
  ///
  /// 线性扫描槽位，发现首个尺寸满足且未过期的槽位立即尝试原子取出。
  /// 高并发竞争 CAS 失败时继续扫描下一个候选槽位，避免过早失败。
  #[inline]
  pub fn take_first_fit(
    &self,
    required_size: u32,
    min_address: u64,
  ) -> (Option<(u64, u32)>, usize) {
    if self.is_empty() || self.slots.is_empty() {
      return (None, 0);
    }

    let mut purged = 0;

    for _ in 0..Self::TAKE_RETRY_ROUNDS {
      let mut found_candidate = false;
      for slot in &self.slots {
        let current = slot.raw();
        if current == FreeRecord::EMPTY_WORD {
          continue;
        }
        let addr = FreeRecord::raw_address(current);
        if addr < min_address {
          if slot.try_take_exact(current) {
            self.dec_count();
            purged += 1;
          }
        } else {
          let size = FreeRecord::raw_size(current);
          if size >= required_size {
            found_candidate = true;
            // 首次适配：立即原子取出
            if slot.try_take_exact(current) {
              self.dec_count();
              return (Some((addr, size)), purged);
            }
          }
        }
      }

      if !found_candidate {
        break;
      }
    }

    (None, purged)
  }

  /// 带扫描上限的最优适配原子取出（对标 C# TryTakeBestFit）
  ///
  /// 扫描分桶内槽位：
  /// - 遇到低于 `min_address` 的失效槽位，就地原子 CAS 清零淘汰并计入 `purged`
  /// - 遇到尺寸完全相同的槽位（精确匹配），立即尝试原子取出
  /// - 记录首个满足尺寸的候选位置 `first_fit_idx`，向后最多扫描 `scan_limit` 个槽位
  /// - 选取浪费最小的槽位（Best-Fit）原子取出
  /// - 若最优候选在 CAS 时争抢失败，按 C# 算法折半扫描上限重试；若上限 <= 1 则回退为 First-Fit
  ///
  /// 返回 `(Option<(address, size)>, purged_count)`
  pub fn take_best_fit_with_limit(
    &self,
    required_size: u32,
    min_address: u64,
    scan_limit: usize,
  ) -> (Option<(u64, u32)>, usize) {
    if self.is_empty() || self.slots.is_empty() {
      return (None, 0);
    }
    if scan_limit == USE_FIRST_FIT {
      return self.take_first_fit(required_size, min_address);
    }

    let mut purged = 0;
    let mut local_scan_limit = scan_limit;

    loop {
      let mut best_idx = None;
      let mut best_size = u32::MAX;
      let mut best_raw = 0;
      let mut first_fit_idx = None;

      for (i, slot) in self.slots.iter().enumerate() {
        let current = slot.raw();
        if current != FreeRecord::EMPTY_WORD {
          let addr = FreeRecord::raw_address(current);
          if addr < min_address {
            // 已滑入冷区或截断区，就地原子清零淘汰
            if slot.try_take_exact(current) {
              self.dec_count();
              purged += 1;
            }
          } else {
            let size = FreeRecord::raw_size(current);
            if size >= required_size {
              if first_fit_idx.is_none() {
                first_fit_idx = Some(i);
              }

              // 精确匹配：零浪费，作为最优候选立即停止扫描（对标 C# TryPeek 返回 exact match 逻辑）
              if size == required_size {
                best_idx = Some(i);
                best_raw = current;
                break;
              }

              // 寻找最优适配（内部碎片最小）
              if size < best_size {
                best_size = size;
                best_idx = Some(i);
                best_raw = current;
              }
            }
          }
        }

        // 处理完当前槽位后，检查是否已达到向后扫描上限（严格对标 C# ii - firstFitIndex >= localBestFitScanLimit）
        if let Some(ff_idx) = first_fit_idx
          && i.saturating_sub(ff_idx) >= local_scan_limit
        {
          break;
        }
      }

      if let Some(idx) = best_idx {
        // SAFETY: best_idx 来自枚举 self.slots 的有效索引，必定在 bounds 范围内
        if unsafe { self.slots.get_unchecked(idx) }.try_take_exact(best_raw) {
          self.dec_count();
          return (Some(FreeRecord::unpack(best_raw)), purged);
        }

        // 找到了候选但 CAS 争抢失败：按 C# 算法逐步折半扫描上限并重试
        local_scan_limit /= 2;
        if local_scan_limit <= 1 {
          let (res, p) = self.take_first_fit(required_size, min_address);
          return (res, purged + p);
        }
      } else {
        // 无可用候选
        break;
      }
    }

    (None, purged)
  }

  /// 最优适配（Best Fit）原子取出（使用分桶配置的 best_fit_scan_limit）
  #[inline]
  pub fn take_best_fit(&self, required_size: u32, min_address: u64) -> (Option<(u64, u32)>, usize) {
    self.take_best_fit_with_limit(required_size, min_address, self.best_fit_scan_limit)
  }

  /// 主动清理低于 min_address 的失效槽位
  #[inline]
  pub fn purge_below(&self, min_address: u64) -> usize {
    if self.is_empty() {
      return 0;
    }
    let mut purged = 0;
    for slot in &self.slots {
      if slot.try_purge_below(min_address) {
        self.dec_count();
        purged += 1;
      }
    }
    purged
  }

  /// 清空分桶内所有槽位（维护操作，须独占调用，不可与其他存取并发）
  #[inline]
  pub fn clear(&self) {
    for slot in &self.slots {
      slot.clear();
    }
    self.active_count.store(0, Ordering::Release);
  }
}
