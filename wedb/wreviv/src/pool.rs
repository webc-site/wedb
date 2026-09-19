use std::{
  fmt,
  hint::unreachable_unchecked,
  sync::atomic::{AtomicI64, AtomicU64, Ordering},
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

/// 每个分桶默认槽位数（对标 Garnet DefaultRecordsPerBin = 256；生产唯一预设，不对外配置）
const DEFAULT_BIN_CAPACITY: usize = 256;

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
  /// 复活暂停挂起计数（0 = 启用；负数 = 未启用或暂停；正数 = 暂停后已超额恢复）
  ///
  /// 单字段承载「是否启用 + 是否暂停」两义，与 C# 同形：`revivSuspendCount` 初值 -1
  /// （未启用恒假），仅当 EnableRevivification 时置 0（RevivificationManager.cs:24/:43），
  /// 暂停再递减；故 [`Self::is_enabled`] 即 C# `RevivificationManager.IsEnabled`，
  /// 上层复活臂（链内原地复活、池取）只判此一谓词，不再并列配置开关。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationManager.cs:revivSuspendCount
  reviv_suspend_count: AtomicI64,
}

/// 未启用态挂起计数（对标 C# `revivSuspendCount = -1`：负数使 [`FreeRecordPool::is_enabled`] 恒假）
const SUSPEND_DISABLED: i64 = -1;

impl FreeRecordPool {
  /// 创建默认分桶阶梯的复活池（生产唯一构造入口，wkv store 层使用）
  ///
  /// 等价 C# `RevivificationSettings.PowerOf2Bins` 预设：以 2 的幂次分桶 + 每桶
  /// DefaultRecordsPerBin = 256 + 全桶最优适配。常量 `DEFAULT_BIN_SIZES` /
  /// `DEFAULT_BIN_CAPACITY` 的合法性已由编译期断言与 [`Self::build`] 的校验规则
  /// 保证，此处 Err 分支绝不触发。
  ///
  /// `enable_revivification` 对标 C# `RevivificationSettings.EnableRevivification`
  /// （`--reviv`，RevivificationManager.cs:38-43）：为假时挂起计数停在
  /// [`SUSPEND_DISABLED`]，[`Self::is_enabled`] 恒假，链内原地复活与池取一并关闭
  /// （上层不再并列第二道开关）。
  pub fn new(enable_revivification: bool) -> Self {
    match Self::build(
      &DEFAULT_BIN_SIZES,
      DEFAULT_BIN_CAPACITY,
      BEST_FIT_SCAN_ALL,
      enable_revivification,
    ) {
      Ok(pool) => pool,
      // SAFETY: 编译期断言保证 DEFAULT_BIN_SIZES 严格递增且不超 MAX_INLINE_SIZE，
      // DEFAULT_BIN_CAPACITY = 256 > 0，Err 分支不可达
      Err(_) => unsafe { unreachable_unchecked() },
    }
  }

  /// 创建使用自定义分桶尺寸阶梯、容量与最优适配扫描上限的复活池（启用态）
  ///
  /// 对标 C# 唯一构造入口 `FreeRecordPool(store, RevivificationSettings)` 的完整配置面
  /// （`FreeRecordBins` / `BestFitScanLimit`）；wedb 生产固定 PowerOf2Bins 预设（[`Self::new`]），
  /// 本构造仅供对标 C# RevivificationTests 的集成测试构造夹具（单桶、小容量、First-Fit 等）。
  pub fn with_bin_sizes_and_scan_limit(
    bin_sizes: &[u32],
    bin_capacity: usize,
    best_fit_scan_limit: usize,
  ) -> Result<Self> {
    Self::build(bin_sizes, bin_capacity, best_fit_scan_limit, true)
  }

  /// 分桶装配与校验本体（两个构造入口的单点）
  fn build(
    bin_sizes: &[u32],
    bin_capacity: usize,
    best_fit_scan_limit: usize,
    enable_revivification: bool,
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
      reviv_suspend_count: AtomicI64::new(if enable_revivification {
        0
      } else {
        SUSPEND_DISABLED
      }),
    })
  }

  /// 暂停复活：挂起计数递减，[`Self::is_enabled`] 转为 false，take 不再外借槽位
  ///
  /// 可重入（嵌套暂停需等量 resume 才恢复），供上层在检查点封印、迁移搬迁等
  /// 维护窗口冻结复活分配；已置位期间在途的 take 不受影响（无全局栅栏语义，
  /// 强同步暂停需配合上层 epoch 排空，对标 C# Tsavorite.PauseRevivification
  /// 的 BumpCurrentEpoch + 事件等待协议，由调用方按需组合）。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationManager.cs:PauseRevivification
  #[inline]
  pub fn pause(&self) {
    self.reviv_suspend_count.fetch_sub(1, Ordering::AcqRel);
  }

  /// 恢复复活：挂起计数递增，归零后 [`Self::is_enabled`] 重新为 true
  ///
  /// 必须与 [`Self::pause`] 严格配对（C# 同此约定，其唯一调用点是迁移驱动的
  /// Pause/Resume 一对；wedb 侧由 `RevivPauseGuard` 的 RAII 绑定保证）：
  /// 未启用态计数为 [`SUSPEND_DISABLED`]，孤调本方法会把它推到 0 而凭空开启复活。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationManager.cs:ResumeRevivification
  #[inline]
  pub fn resume(&self) {
    self.reviv_suspend_count.fetch_add(1, Ordering::AcqRel);
  }

  /// 复活是否处于启用状态（挂起计数为 0：既开启 `--reviv` 又未处于暂停窗口）
  ///
  /// 单谓词等价 C# `RevivificationManager.IsEnabled`，是上层所有复活臂（链内原地复活、
  /// 池取）的唯一门；功能未启用与迁移暂停在此合一，调用方勿再并列 `enable_revivification`。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationManager.cs:IsEnabled
  #[inline]
  pub fn is_enabled(&self) -> bool {
    self.reviv_suspend_count.load(Ordering::Acquire) == 0
  }

  /// 将删除或缩减释放的记录槽位归还入池（按尺寸定位分桶直投，不做相邻空洞合并）
  ///
  /// 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryAdd
  /// → TryAddToBin → FreeRecordBin.TryAdd
  ///
  /// 前置条件：调用方（wedb_hlog / wedb_store 层）须已将该地址的记录墓碑化或确认其已失效
  /// （对标 C# TryAdd 前置的 `InfoRef.TrySeal(invalidate: true)`，见 [`FreeRecord`] 并发模型说明）。
  ///
  /// 与 C# 差异：目标分桶槽位耗尽时依次向上尝试更大分桶（C# TryAddToBin 满则弃），以可控的
  /// 跨桶尝试换取空间留存；相邻空洞合并不实现——C# 复活池从无该机制（其 TryAdd 单桶轮转直投），
  /// 碎片收敛交由 [`Self::take`] 的 Best-Fit 尺寸匹配与上层日志紧缩承担。
  #[inline]
  pub fn put(&self, address: u64, size: u32, min_address: u64) -> bool {
    self.put_count.fetch_add(1, Ordering::Relaxed);

    // 边界防御：低于只读/截断水位、无效地址、超出 48 位地址域或尺寸非法，一律丢弃
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
    for bin in &self.bins[bin_idx..] {
      match bin.put(address, size, min_address) {
        SetStatus::InsertedEmpty => return true,
        SetStatus::ReplacedExpired => {
          self.drop_count.fetch_add(1, Ordering::Relaxed);
          return true;
        }
        SetStatus::Occupied => continue,
      }
    }
    self.drop_count.fetch_add(1, Ordering::Relaxed);
    false
  }

  /// 查找最适配的空闲槽位并原子取出（向上跨所有可用分桶搜索）
  ///
  /// 与 C# 默认值差异：Garnet `RevivificationSettings.NumberOfBinsToSearch` 默认 0
  /// （仅检索目标尺寸所属桶，构造时固化、非调用参数）；本实现固定向上检索全部
  /// 更高级分桶，以可控的内部碎片代价换取更高复活命中率。
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
    // 暂停门控（对标 C# 调用侧 `RevivificationManager.IsEnabled` 检查，内联于此保证
    // 池语义自洽：暂停期间任何调用方都取不到槽位，计数亦不虚增）
    if !self.is_enabled() {
      return None;
    }
    self.take_count.fetch_add(1, Ordering::Relaxed);

    if required_size == 0 || required_size > FreeRecord::MAX_INLINE_SIZE {
      return None;
    }

    let start_bin = self.find_bin_index(required_size)?;

    // SAFETY: start_bin < self.bins.len()（来自 find_bin_index），全桶搜索直接取尾部开区间
    let search_bins = unsafe { self.bins.get_unchecked(start_bin..) };
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

  /// 复位统计账目：四计数归零，不动任何分桶槽位
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationStats.cs:Reset
  ///
  /// INFO RESETSTAT 的 reviv 臂终点（C# 链
  /// libs/server/Metrics/GarnetServerMonitor.cs:211 →
  /// libs/server/StoreWrapper.cs:646 →
  /// libs/server/Databases/SingleDatabaseManager.cs:301 →
  /// libs/storage/Tsavorite/cs/src/core/ClientSession/ManageClientSessions.cs:91）。
  /// 与 [`Self::clear`] 职责分立：clear 只清槽（账目原样保留，供清池后继续
  /// 观察累计），reset_stats 只清账（可复活槽位原样保留，不因复位丢命中能力）。
  #[inline]
  pub fn reset_stats(&self) {
    self.put_count.store(0, Ordering::Relaxed);
    self.take_count.store(0, Ordering::Relaxed);
    self.hit_count.store(0, Ordering::Relaxed);
    self.drop_count.store(0, Ordering::Relaxed);
  }

  /// 根据记录尺寸查找首个匹配分桶的索引（利用单调性二分查找，O(log N)）
  ///
  /// 对标 C# `FreeRecordPool.GetBinIndex`；除 put / take 内部使用外，
  /// 亦由对标 C# BinSelectionTest 的集成测试直接验证分桶阶梯边界。
  #[inline]
  pub fn find_bin_index(&self, size: u32) -> Option<usize> {
    let idx = self.bins.partition_point(|bin| bin.max_size < size);
    (idx < self.bins.len()).then_some(idx)
  }

  /// 判断回收池是否为空（O(B) 复杂度，短路快速判断）
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.bins.iter().all(|b| b.is_empty())
  }

  /// 清空所有分桶（只清槽不动账目，计数复位见 [`Self::reset_stats`]）
  #[inline]
  pub fn clear(&self) {
    for bin in &self.bins {
      bin.clear();
    }
  }
}
