use std::{
  fmt,
  hint::unreachable_unchecked,
  sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64, Ordering},
  },
};

use crate::{
  Error, RecordSizeReader, Result,
  bin::{BEST_FIT_SCAN_ALL, FreeRecordBin},
  record::{FreeRecord, SetStatus},
};

/// 记录跨度 8 字节整词对齐（转发 `FreeRecord::RECORD_ALIGNMENT`，全仓与 wrecord 恒等值）
const RECORD_ALIGNMENT: u32 = FreeRecord::RECORD_ALIGNMENT;

/// 最大分桶的整词对齐上限（65535 非 8 的倍数，直接作末桶 max_size 会容许切出非整词
/// Pad 跨度，破坏全仓记录 8 字节整词对齐不变式；向下取整到 65528）
const MAX_ALIGNED_BIN_SIZE: u32 =
  FreeRecord::MAX_INLINE_SIZE - FreeRecord::MAX_INLINE_SIZE % RECORD_ALIGNMENT;

/// 默认分桶尺寸阶梯（对标 C# PowerOf2BinsRevivificationSettings：以 2 的幂次递增，起点
/// `RevivificationBin.MinRecordSize` = 16，与 wrecord `HEADER_SIZE` = 16 的最小记录一致，
/// 直至整词对齐的最大内联尺寸 65528B；缺 16/32 档会使最小记录落入 64B 桶，内部碎片最高达 75%。
/// C# 阶梯全为 2 的幂天然 8 字节对齐，本阶梯末位同样以 [`MAX_ALIGNED_BIN_SIZE`] 保证对齐）
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
  MAX_ALIGNED_BIN_SIZE,
];

/// 受控向上跨桶窗档数（对标 C# `RevivificationSettings.NumberOfBinsToSearch`，
/// RevivificationSettings.cs:48，构造时固化、非调用参数；C# 默认 0 仅检索当期桶，
/// C# TryTake 开启后也只是逐桶受控探查相邻桶，FreeRecordPool.cs:556-560。
/// wedb 固化为 1：当期桶落空/满载时至多再及紧邻上一档，杜绝微小记录掠夺
/// 32KB/64KB 大槽位，同时保留一档命中缓冲；不对外暴露配置开关。
/// put 溢出级联与 take 检索共用此档数经 [`Self::search_window`] 单点成窗，
/// 两窗恒同构——入池级联落点必在同尺寸取池可达窗内，杜绝越窗僵尸槽）
/// 命中落空时的受控向上续探档数；窗口触及阶梯末桶仍落空时另续探一档
/// oversize 臂（臂在阶梯外独立挂接，见 take_first_fit 续探段）
const BINS_TO_SEARCH: usize = 1;

const _: () = {
  let mut i = 0;
  let mut prev = 0;
  while i < DEFAULT_BIN_SIZES.len() {
    let s = DEFAULT_BIN_SIZES[i];
    assert!(s > prev);
    assert!(s <= FreeRecord::MAX_INLINE_SIZE);
    // 全桶尺寸必须 8 字节整词对齐：否则以非对齐尺寸切出 Pad，Pad 跨度不是整词倍数，
    // 后续扫描跳步偏移失准并引发未对齐内存访问风险
    assert!(s.is_multiple_of(RECORD_ALIGNMENT));
    prev = s;
    i += 1;
  }
};

/// 每个分桶默认槽位数（对标 Garnet DefaultRecordsPerBin = 256；生产唯一预设，不对外配置）
const DEFAULT_BIN_CAPACITY: usize = 256;

/// oversize 复活臂（对标 C# `PowerOf2BinsRevivificationSettings` 预设恒附的单一
/// 尾桶 `RevivificationBin { RecordSize = MaxRecordSize, NumberOfRecords =
/// DefaultRecordsPerBin }`，C# FreeRecordPool.cs:TryPeek/:TryTakeOversize 的
/// `GetRecordSize` 复读臂由此承接）：管理超 16 位内联词顶值、仍单页可纳的
/// 失效槽位——入池仅存地址，取出经 `read_size` 复读口推导真实块尺寸再判纳
pub struct Oversize {
  /// 专用尾桶（max_size = 单记录硬上界即单页容量，恒 > MAX_INLINE_SIZE）
  pub bin: FreeRecordBin,
  /// 记录头尺寸复读口（wkv store 层从 whlog 读尺寸单点注入，只读闭包）
  read_size: Arc<RecordSizeReader>,
}

impl fmt::Debug for Oversize {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Oversize")
      .field("bin", &self.bin)
      .field("read_size", &"record_size_reader")
      .finish()
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
pub struct FreeRecordPool {
  /// 多尺寸分级分桶列表（按 max_size 升序排列）
  pub bins: Box<[FreeRecordBin]>,
  /// oversize 复活臂（[`Self::with_oversize`] 挂接；None = 无该臂，出阶梯尺寸弃归，
  /// 对标 C# 未配置 oversize 桶形态）
  pub oversize: Option<Oversize>,
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
  /// 分桶阶梯与每桶容量对标 C# `RevivificationSettings.PowerOf2Bins` 预设：
  /// 以 2 的幂次分桶 + 每桶 DefaultRecordsPerBin = 256。
  /// 注：C# 构造未显式设定 BestFitScanLimit，停留在默认值 UseFirstFit(0)（即 First-Fit，
  /// RevivificationSettings.cs:181）；rust 采用 BEST_FIT_SCAN_ALL 全桶最优适配
  /// 属 §111c 几何收形内裁，禁以「对齐 C#」名义回摆。
  /// 常量 `DEFAULT_BIN_SIZES` / `DEFAULT_BIN_CAPACITY` 的合法性已由编译期断言与
  /// [`Self::build`] 的校验规则保证，此处 Err 分支绝不触发。
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
      // SAFETY: 编译期断言保证 DEFAULT_BIN_SIZES 严格递增、8 字节整词对齐且不超
      // MAX_INLINE_SIZE，DEFAULT_BIN_CAPACITY = 256 > 0，Err 分支不可达
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

  /// 挂接 oversize 复活臂（wkv store 层生产装配必挂；对标 C# `PowerOf2Bins` 预设
  /// 恒附的单一尾桶，`max_size` 即单记录硬上界——页容量，
  /// `read_size` 为记录头真实尺寸复读口）
  ///
  /// 挂接后超内联阶梯尺寸（> 末桶 `MAX_ALIGNED_BIN_SIZE`）的失效槽位入臂存址、
  /// 取出经复读口判纳；`max_size` 须越过 16 位内联词顶值方有臂形意义
  /// （等值配置下 65529~65535 非整词尺寸本不可达，65536 恰为 64KB 整页记录
  /// 唯一可达越界档）。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationSettings.cs:PowerOf2Bins
  #[must_use]
  pub fn with_oversize(
    self,
    max_size: u32,
    read_size: impl Fn(u64) -> Option<u32> + Send + Sync + 'static,
  ) -> Self {
    debug_assert!(
      max_size > FreeRecord::MAX_INLINE_SIZE,
      "oversize 臂上界必须越过 16 位内联词顶值"
    );
    let bin = FreeRecordBin::new_oversize(max_size, DEFAULT_BIN_CAPACITY);
    Self {
      oversize: Some(Oversize {
        bin,
        read_size: Arc::new(read_size),
      }),
      ..self
    }
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
      oversize: None,
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
  /// 管理器入口同挂此处（rust 折叠为同一单点）：
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationManager.cs:TryAdd
  ///
  /// 前置条件：调用方（wedb_hlog / wedb_store 层）须已将该地址的记录墓碑化或确认其已失效
  /// （对标 C# TryAdd 前置的 `InfoRef.TrySeal(invalidate: true)`，见 [`FreeRecord`] 并发模型说明）。
  ///
  /// 与 C# 差异：目标分桶槽位耗尽时受控向上尝试至多 [`BINS_TO_SEARCH`] 档分桶
  /// （经 [`Self::search_window`] 与 [`Self::take`] 共用同一窗界，越窗即弃计入 `drop_count`；
  /// 对标 C# TryAddToBin 满则弃），以受控的相邻档缓冲换取空间留存，同时两窗同构
  /// 杜绝越窗僵尸槽；相邻空洞合并不实现——C# 复活池从无该机制（其 TryAdd 单桶轮转直投），
  /// 碎片收敛交由 [`Self::take`] 的 Best-Fit 尺寸匹配与上层日志紧缩承担。
  #[inline]
  pub fn put(&self, address: u64, size: u32, min_address: u64) -> bool {
    self.put_count.fetch_add(1, Ordering::Relaxed);

    // 越出 16 位内联词顶值的尺寸带：oversize 臂存址专用形（C# oversize 尾桶），
    // 无臂一律弃归
    if size > FreeRecord::MAX_INLINE_SIZE {
      return self.put_oversize(address, size, min_address);
    }

    // 边界防御单点转调（判定口径见 [`FreeRecord::validate`]），非法一律丢弃
    if !FreeRecord::validate(address, size, min_address) {
      self.drop_count.fetch_add(1, Ordering::Relaxed);
      return false;
    }
    let Some(bin_idx) = self.find_bin_index(size) else {
      // 65529~65535 出阶梯拒收（整词对齐不变式下恒不可达）
      self.drop_count.fetch_add(1, Ordering::Relaxed);
      return false;
    };
    for bin in self.search_window(bin_idx) {
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

  /// oversize 臂归池（对标 C# FreeRecordPool.cs:TryAddToBin 单桶直投满则弃：
  /// 臂即阶梯末档，无更大桶可上爬；入臂仅存地址，真实尺寸取出时复读推导）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryAddToBin
  fn put_oversize(&self, address: u64, size: u32, min_address: u64) -> bool {
    let Some(arm) = &self.oversize else {
      self.drop_count.fetch_add(1, Ordering::Relaxed);
      return false;
    };
    match arm.bin.put(address, size, min_address) {
      SetStatus::InsertedEmpty => true,
      SetStatus::ReplacedExpired => {
        self.drop_count.fetch_add(1, Ordering::Relaxed);
        true
      }
      SetStatus::Occupied => {
        self.drop_count.fetch_add(1, Ordering::Relaxed);
        false
      }
    }
  }

  /// 查找最适配的空闲槽位并原子取出（当期分桶 + 受控向上探查至多 [`BINS_TO_SEARCH`] 档）
  ///
  /// 管理器入口与池实现两层在 rust 折叠为同一单点：
  /// - libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationManager.cs:TryTake
  /// - libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/FreeRecordPool.cs:TryTake
  ///
  /// 跨桶检索对标 C# TryTake（FreeRecordPool.cs:549-561）：当期桶落空后至多受控探查
  /// `NumberOfBinsToSearch` 个相邻更大桶（C# 默认 0、开启亦仅逐桶探查；本实现固化 1，
  /// 杜绝微小记录无限制向上掠夺 32KB/64KB 大槽位造成大空间耗竭）。
  ///
  /// 双下界（对标 C# BlockAllocate.cs:59-62 的 minRevivAddress 链首抬升 +
  /// FreeRecordPool.cs:TryTake 低于 minAddress 仅跳过不清退）：
  /// - `min_reviv_addr`：全局可变区复活水位，扫描中遇到低于此线的槽位已滑入只读/截断区，
  ///   就地原子清零废弃并计入 drop_count（永久淘汰）
  /// - `min_eligible_addr`：本次申请下界（如目标哈希链首地址 + 1），低于此线但高于
  ///   `min_reviv_addr` 的槽位仅跳过让位（continue），绝不参与本次提取、更绝不误淘汰
  ///   ——槽位对其他哈希链仍完全有效；调用方无需单次下界时传入 ≤ `min_reviv_addr`
  ///   的值即可，内部以 `min_reviv_addr` 兜底钳位
  /// - 成功取出返回 `Some((address, actual_size))` 并递增 hit_count
  /// - 无可用槽位返回 `None`
  /// - 争抢回退语义（非错误）：极端 CAS 争抢下分桶内有限轮重试可能耗尽，即便桶中
  ///   仍有满足尺寸的候选也可能返回 None——调用方（wedb_hlog 等）据此回退追加
  ///   分配新地址，属正确的性能取舍而非故障。对照 C# Revivification 的 per-worker
  ///   混合策略：C# TryTake 失败同样立即走日志分配器，回退追加是两侧一致的正确语义
  #[inline]
  pub fn take(
    &self,
    required_size: u32,
    min_reviv_addr: u64,
    min_eligible_addr: u64,
  ) -> Option<(u64, u32)> {
    // 暂停门控（对标 C# 调用侧 `RevivificationManager.IsEnabled` 检查，内联于此保证
    // 池语义自洽：暂停期间任何调用方都取不到槽位，计数亦不虚增）
    if !self.is_enabled() {
      return None;
    }
    self.take_count.fetch_add(1, Ordering::Relaxed);

    if required_size == 0 {
      return None;
    }

    // 单次申请下界不得低于全局复活水位，否则 eligible 档形同虚设（防御钳位）
    let min_eligible_addr = min_eligible_addr.max(min_reviv_addr);

    let Some(start_bin) = self.find_bin_index(required_size) else {
      // 越出内联阶梯的尺寸带：oversize 臂复读判纳（无臂即拒）
      return self.take_oversize(required_size, min_reviv_addr, min_eligible_addr);
    };

    for bin in self.search_window(start_bin) {
      let (res, purged) = bin.take_best_fit(required_size, min_reviv_addr, min_eligible_addr);
      if purged > 0 {
        self.drop_count.fetch_add(purged as u64, Ordering::Relaxed);
      }
      if let Some((addr, size)) = res {
        self.hit_count.fetch_add(1, Ordering::Relaxed);
        return Some((addr, size));
      }
    }

    // 检索窗伸及阶梯末桶仍落空：相邻下一档即 oversize 臂，受控续探一档
    // （C# oversize 尾桶同列 bins 阶梯、相邻档探查天然覆盖之同构）。
    // 不变式：窗触及阶梯末桶（start + 1 + BINS_TO_SEARCH >= len）仍落空，
    // oversize 臂必被续探恰一次（臂在阶梯外独立挂接，窗内桶不重复探）
    if start_bin + 1 + BINS_TO_SEARCH >= self.bins.len()
      && let Some(hit) = self.take_oversize(required_size, min_reviv_addr, min_eligible_addr)
    {
      return Some(hit);
    }

    None
  }

  /// oversize 臂取出调度：命中阶梯外尺寸带（或末桶落空续探一档）后转调臂桶，
  /// 候选真实尺寸经 `read_size` 复读口从记录头推导、判纳后才 CAS 置空
  /// （机械本体与其 TryTakeOversize 锚点见 [`FreeRecordBin::take_oversize`]；
  /// 复读不可判槽位仅尺寸不足跳过，绝不误清退）
  fn take_oversize(
    &self,
    required_size: u32,
    min_reviv_addr: u64,
    min_eligible_addr: u64,
  ) -> Option<(u64, u32)> {
    let arm = self.oversize.as_ref()?;
    if required_size > arm.bin.max_size {
      return None;
    }
    let (res, purged) = arm.bin.take_oversize(
      required_size,
      min_reviv_addr,
      min_eligible_addr,
      &*arm.read_size,
    );
    if purged > 0 {
      self.drop_count.fetch_add(purged as u64, Ordering::Relaxed);
    }
    if let Some(slot) = res {
      self.hit_count.fetch_add(1, Ordering::Relaxed);
      return Some(slot);
    }
    None
  }

  /// 主动清理已滑入冷区或只读区的失效槽位
  ///
  /// 返回被清理废弃的槽位总数，并将其累计计入 drop_count
  #[inline]
  pub fn purge_below(&self, min_address: u64) -> usize {
    let mut total_purged: usize = self
      .bins
      .iter()
      .map(|bin| bin.purge_below(min_address))
      .sum();
    if let Some(arm) = &self.oversize {
      total_purged += arm.bin.purge_below(min_address);
    }
    if total_purged > 0 {
      self
        .drop_count
        .fetch_add(total_purged as u64, Ordering::Relaxed);
    }
    total_purged
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

  /// put 溢出级联与 take 跨桶检索共用的受控窗口 `[start_bin, start_bin + 1 + BINS_TO_SEARCH)`
  /// （窗界单点，两窗同构）
  ///
  /// 入池级联落点必在同尺寸取池可达窗内，杜绝距当期桶 ≥2 档、同尺寸 take 永远盖不到
  /// 的越窗僵尸槽；窗外不落即弃（put 走 drop_count 弃路径，语义对齐 C# TryAddToBin
  /// 满则弃，FreeRecordPool.cs:518/:538 failedAdds——C# 单桶直投与逐桶受控检索天然
  /// 两窗对称，本实现以同一窗界复刻该对称性）。
  ///
  /// 前置契约：`start_bin` 来自 [`Self::find_bin_index`]，恒 < `self.bins.len()`。
  #[inline]
  fn search_window(&self, start_bin: usize) -> &[FreeRecordBin] {
    // SAFETY: start_bin < bins.len()（find_bin_index 背书），end 已对 bins.len() 取
    // min，区间端点必在界内
    unsafe {
      let end = (start_bin + 1 + BINS_TO_SEARCH).min(self.bins.len());
      self.bins.get_unchecked(start_bin..end)
    }
  }

  /// 判断回收池是否为空（O(B) 复杂度，短路快速判断，含 oversize 臂）
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.bins.iter().all(|b| b.is_empty())
      && self.oversize.as_ref().is_none_or(|arm| arm.bin.is_empty())
  }

  /// 清空所有分桶（只清槽不动账目，计数复位见 [`Self::reset_stats`]，含 oversize 臂）
  #[inline]
  pub fn clear(&self) {
    for bin in &self.bins {
      bin.clear();
    }
    if let Some(arm) = &self.oversize {
      arm.bin.clear();
    }
  }
}
