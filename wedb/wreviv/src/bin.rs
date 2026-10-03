use std::{
  fmt,
  sync::atomic::{AtomicIsize, AtomicUsize, Ordering},
};

use crate::{
  RecordSizeReader,
  record::{FreeRecord, SetStatus},
};

/// 首次适配（First-Fit）：取首个满足尺寸的槽位（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationSettings.cs:UseFirstFit = 0）
pub const USE_FIRST_FIT: usize = 0;
/// 全局最优适配（Best-Fit Scan All）：扫描全桶寻找最小浪费槽位（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationSettings.cs:BestFitScanAll）
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
  ///
  /// 对标 C# `FreeRecordBin(ref RevivificationBin binDef, prevBinRecordSize)`：从配置
  /// （RecordSize / NumberOfRecords / BestFitScanLimit）构造的唯一入口。
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

  /// 构造 oversize 专用分桶（对标 C# `PowerOf2BinsRevivificationSettings` 预设恒附的
  /// 单一尾桶 `RevivificationBin { RecordSize = MaxRecordSize }`：max_size 为页容量级
  /// 硬上界，越过 16 位内联词顶值，不做构造期钳位；入池仅存地址、取出经复读口
  /// 推导真实尺寸，扫描策略固定全桶 Best-Fit）
  pub fn new_oversize(max_size: u32, capacity: usize) -> Self {
    let slots = (0..capacity)
      .map(|_| FreeRecord::empty())
      .collect::<Box<[_]>>();
    Self {
      slots,
      active_count: AtomicIsize::new(0),
      cursor: AtomicUsize::new(0),
      best_fit_scan_limit: capacity,
      max_size,
    }
  }

  /// 是否为 oversize 专用分桶（桶上界越过 16 位内联词顶值即 oversize 形，
  /// 对标 C# TryTake 的 `oversize: sizeIndex[index] > MaxInlineRecordSize`
  /// 按桶上界判别同构）
  #[inline]
  pub fn is_oversize(&self) -> bool {
    self.max_size > FreeRecord::MAX_INLINE_SIZE
  }

  /// 获取当前活跃槽位数近似值（Acquire 序同步）
  #[inline]
  pub fn len(&self) -> usize {
    self.active_count.load(Ordering::Acquire).max(0) as usize
  }

  /// 判断分桶是否为空（Acquire 序同步）
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.active_count.load(Ordering::Acquire) <= 0
  }

  /// 递减活跃槽位计数（AcqRel 序；仅限本分桶内配对成功 CAS 的增减路径调用，防止外部造成下溢）
  #[inline]
  fn dec_count(&self) {
    self.active_count.fetch_sub(1, Ordering::AcqRel);
  }

  /// 归还槽位入桶
  ///
  /// - `SetStatus::InsertedEmpty`：落入空槽位
  /// - `SetStatus::ReplacedExpired`：覆盖替换了已过期槽位（该过期槽位由调用方计入丢弃统计）
  /// - `SetStatus::Occupied`：参数非法，或分桶已满（全部槽位均被有效记录占用）
  ///
  /// oversize 专用桶形态分支：入池仅存地址（16 位尺寸位段不承载、恒零，真实
  /// 尺寸由取出侧复读口经 hlog 记录头推导），尺寸档位只作本桶上界判定，地址
  /// 档位走 [`FreeRecord::validate_addr`]；内联桶判定口径不变（[`FreeRecord::validate`]
  /// 单点 + 本桶上限）
  #[inline]
  pub fn put(&self, address: u64, size: u32, min_address: u64) -> SetStatus {
    let ok = if self.is_oversize() {
      FreeRecord::validate_addr(address, min_address) && size != 0 && size <= self.max_size
    } else {
      // 边界安全防御单点转调（判定口径见 [`FreeRecord::validate`]）+ 本桶上限
      // 桶内判定保留（max_size ≤ MAX_INLINE_SIZE，构造期钳位）
      FreeRecord::validate(address, size, min_address) && size <= self.max_size
    };
    if !ok {
      return SetStatus::Occupied;
    }

    let len = self.slots.len();
    if len == 0 {
      return SetStatus::Occupied;
    }
    let start = self.cursor.fetch_add(1, Ordering::Relaxed) % len;
    // SAFETY: len > 0 且 start = cursor % len < len
    let (left, right) = unsafe { self.slots.split_at_unchecked(start) };
    // oversize 桶形态分支提升到槽位循环外：形态对单一桶恒定（构造期定死），
    // 逐槽重判为纯重复开销
    let oversize = self.is_oversize();
    for slot in right.iter().chain(left) {
      let status = if oversize {
        slot.set_addr(address, min_address)
      } else {
        slot.set(address, size, min_address)
      };
      match status {
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

  /// 槽位真实尺寸解析单点：内联桶直读槽位 16 位尺寸字；oversize 复读臂经 hlog
  /// 尺寸复读闭包取回（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryPeek 的
  /// `oversize ? GetRecordSize(...) : oldRecord.Size` 分支）；复读不可判（页驱逐/
  /// 头部残片）按尺寸 0 计——仅作尺寸不足不纳裁量，绝非清退对象
  #[inline]
  fn size_at(current: u64, read: Option<&RecordSizeReader>) -> u32 {
    match read {
      Some(f) => f(FreeRecord::raw_address(current)).unwrap_or(0),
      None => FreeRecord::raw_size(current),
    }
  }

  /// 首次适配（First-Fit）原子取出（对应 C# TryTakeFirstFit）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryTakeFirstFit
  ///
  /// 双下界口径（对标 C# FreeRecord.TryTake 低于 minAddress 仅返回 false 绝不清退）：
  /// - `addr < min_reviv_addr`：已滑入只读/截断区，全局永久失效，就地 CAS 清零淘汰并计入 `purged`；
  /// - `addr < min_eligible_addr`：仅暂不满足本次申请下界（如低于目标哈希链首地址），
  ///   直接跳过 continue，**严禁就地清零淘汰**——该槽位对其他哈希链仍完全有效；
  /// - 其余槽位尺寸满足即作为候选立即尝试原子取出。
  ///   高并发竞争 CAS 失败时继续扫描下一个候选槽位，避免过早失败。
  ///
  /// 与 C# 差异：C# `FreeRecord.TryTake` 对过期槽位一律仅跳过不清零（仅 Best-Fit 路径的
  /// `TryPeek` 就地清零）；本实现对低于 `min_reviv_addr` 的槽位统一就地 CAS 清零并递减
  /// 活跃计数——本实现以 active_count 取代 C# CheckEmptyWorker 的 isEmpty 标志，计数与
  /// 真实非空槽位严格一致是空桶快路径正确性的前提。
  #[inline]
  pub fn take_first_fit(
    &self,
    required_size: u32,
    min_reviv_addr: u64,
    min_eligible_addr: u64,
  ) -> (Option<(u64, u32)>, usize) {
    self.first_fit(required_size, min_reviv_addr, min_eligible_addr, None)
  }

  /// First-Fit 扫描本体（`read` 为 Some 即 oversize 复读臂形：候选尺寸经复读
  /// 口推导后再判纳，槽位词内尺寸位段不参与裁量）
  fn first_fit(
    &self,
    required_size: u32,
    min_reviv_addr: u64,
    min_eligible_addr: u64,
    read: Option<&RecordSizeReader>,
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
        if addr < min_reviv_addr {
          // 全局失效水位之下：永久淘汰清零
          if slot.try_take_exact(current) {
            self.dec_count();
            purged += 1;
          }
        } else if addr < min_eligible_addr {
          // 低于本次申请下界：跳过让位，绝非淘汰对象
          continue;
        } else {
          let size = Self::size_at(current, read);
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

  /// 带扫描上限的最优适配原子取出本体（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryTakeBestFit）
  ///
  /// 双下界语义与 [`Self::first_fit`] 完全一致：
  /// - `addr < min_reviv_addr`：已滑入只读/截断区，就地原子 CAS 清零淘汰并计入 `purged`
  /// - `addr < min_eligible_addr`：暂不满足本次申请下界（如低于目标哈希链首地址），
  ///   直接跳过，严禁就地清零淘汰（槽位对其他链仍有效，也绝不参与适配竞选）
  /// - 其余槽位（候选尺寸一律经 [`Self::size_at`] 单点解析——oversize 复读臂形走
  ///   hlog 复读闭包，内联桶形直读槽位尺寸字，与 C# `TryPeek` 的
  ///   `oversize ? GetRecordSize(...) : Size` 同构）：
  ///   - 尺寸完全相同（精确匹配）立即尝试原子取出
  ///   - 记录首个满足尺寸的候选位置 `first_fit_idx`，向后最多扫描 `scan_limit` 个槽位
  ///   - 选取浪费最小的槽位（Best-Fit）原子取出
  /// - 若最优候选在 CAS 时争抢失败，按 C# 算法折半扫描上限重试；若上限 <= 1 则回退为 First-Fit
  ///
  /// 返回 `(Option<(address, actual_size)>, purged_count)`
  fn best_fit(
    &self,
    required_size: u32,
    min_reviv_addr: u64,
    min_eligible_addr: u64,
    scan_limit: usize,
    read: Option<&RecordSizeReader>,
  ) -> (Option<(u64, u32)>, usize) {
    if self.is_empty() || self.slots.is_empty() {
      return (None, 0);
    }
    if scan_limit == USE_FIRST_FIT {
      return self.first_fit(required_size, min_reviv_addr, min_eligible_addr, read);
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
          if addr < min_reviv_addr {
            // 已滑入冷区或截断区，全局永久失效，就地原子清零淘汰
            if slot.try_take_exact(current) {
              self.dec_count();
              purged += 1;
            }
          } else if addr < min_eligible_addr {
            // 低于本次申请下界：跳过让位，绝非淘汰对象
          } else {
            let size = Self::size_at(current, read);
            if size >= required_size {
              if first_fit_idx.is_none() {
                first_fit_idx = Some(i);
              }

              // 精确匹配：零浪费，作为最优候选立即停止扫描（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryPeek 返回 exact match 逻辑）
              if size == required_size {
                best_idx = Some(i);
                best_size = size;
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
          return (Some((FreeRecord::raw_address(best_raw), best_size)), purged);
        }

        // 找到了候选但 CAS 争抢失败：按 C# 算法逐步折半扫描上限并重试
        local_scan_limit /= 2;
        if local_scan_limit <= 1 {
          let (res, p) = self.first_fit(required_size, min_reviv_addr, min_eligible_addr, read);
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
  pub fn take_best_fit(
    &self,
    required_size: u32,
    min_reviv_addr: u64,
    min_eligible_addr: u64,
  ) -> (Option<(u64, u32)>, usize) {
    self.best_fit(
      required_size,
      min_reviv_addr,
      min_eligible_addr,
      self.best_fit_scan_limit,
      None,
    )
  }

  /// oversize 专用桶的最优适配原子取出（对标 C# TryTakeBestFit/TryTakeFirstFit 携
  /// `oversize: true` 全链贯穿：候选真实尺寸经 `read` 复读口从 hlog 记录头推导，
  /// 判纳后才 CAS 置空；C# `TryTakeOversize` 在 CAS 败后自旋重读，rust 沿本池既定
  /// 单次 CAS 让位口径（[`FreeRecord::set`] 文档），竞争败即换下一候选）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryTakeOversize
  pub fn take_oversize(
    &self,
    required_size: u32,
    min_reviv_addr: u64,
    min_eligible_addr: u64,
    read: &RecordSizeReader,
  ) -> (Option<(u64, u32)>, usize) {
    self.best_fit(
      required_size,
      min_reviv_addr,
      min_eligible_addr,
      self.best_fit_scan_limit,
      Some(read),
    )
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
