use std::{mem::size_of, path::PathBuf};

use itoa::Buffer;
use wbase::align::{DEFAULT_SECTOR_SIZE, prev_power_of2};
use wconf::LogCompactionType;
use wdev::{detect_cpu_cores, detect_system_memory};
use whlog::{
  DEFAULT_MUTABLE_FRACTION, DEFAULT_NUM_PAGES, DEFAULT_PAGE_SIZE, DEFAULT_SERVER_PAGE_SIZE,
  HybridLogConfig,
};
use windex::HashBucket;
use zmij::Buffer as FmtBuffer;

use crate::error::{Error, Result};

/// 默认哈希索引主桶数基线（65536）
pub const DEFAULT_INDEX_SIZE: usize = 65536;

/// 单个哈希桶字节数（64B Cacheline 对齐，编译期取自 HashBucket 实际布局）
pub const INDEX_BUCKET_BYTES: usize = size_of::<HashBucket>();

/// 每个哈希桶的数据槽位数（第 8 槽为溢出指针与锁位，不放数据）
pub const INDEX_BUCKET_DATA_SLOTS: usize = HashBucket::DATA_ENTRIES;

/// 默认复活区间比例（1.0 = 整个可变区均可复活）
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationSettings.cs:DefaultRevivifiableFraction
pub const DEFAULT_REVIVIFIABLE_FRACTION: f64 = 1.0;

/// 默认最大并发纪元会话数基线（128）
pub const DEFAULT_MAX_SESSIONS: usize = 128;

/// 最小自适应物理内存预算（256 MB）
pub const MIN_MEMORY_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// 默认最大自动自适应内存预算（32 GB，防止默认独占全部宿主内存）
pub const MAX_DEFAULT_MEMORY_BUDGET_BYTES: u64 = 32 * 1024 * 1024 * 1024;

/// 默认物理内存占比百分比（25%）
pub const DEFAULT_MEMORY_PERCENT: u64 = 25;

/// 每个 CPU 物理/逻辑核心默认派生并发会话数
const SESSIONS_PER_CORE: usize = 16;

/// 最小并发会话下限（128）
const MIN_SESSIONS: usize = 128;

/// 最大并发会话上限（1024）
const MAX_SESSIONS: usize = 1024;

/// 最小自适应哈希桶数量（65536）
pub const MIN_INDEX_SIZE: usize = 65536;

/// 最大自适应哈希桶数量（16,777,216，即 16M 桶，对应 1GB 索引内存，可容纳超 1 亿条记录）
pub const MAX_INDEX_SIZE: usize = 16_777_216;

/// 最小自适应混合日志页数下限（16 页）
const MIN_NUM_PAGES: usize = 16;

/// 最大自适应混合日志页数上限（1,048,576 页；页容量随预算自适应后的页数守护上界）
const MAX_NUM_PAGES: usize = 1_048_576;

/// 页容量规划除数：页容量 ≤ 预算 / 64，保证页数下限 16 页的环形缓冲
/// 至多占预算的 1/4（25%，低于 37.5% 索引 + 62.5% 日志配比下的日志份额），
/// 小预算宿主绝不被大页顶穿
const PAGE_SIZE_PLANNING_DIVISOR: u64 = 64;

/// ReadCache 默认内存页数（64 页，2 的幂满足容量校验；仅启用时物化整环内存）
///
/// 对齐 Garnet 默认为读多写少负载提供充足缓存窗口：过小（如 8 页）会在热键集
/// 略大时触发滑动窗口高频换页驱逐，导致冷读反复击穿到磁盘。
const DEFAULT_READ_CACHE_NUM_PAGES: usize = 64;

/// 内置 GC 默认紧缩触发阈值段数（32，对标 C# GarnetServerOptions.CompactionMaxSegments
/// 默认值 32，与 wconf 运行时槽位默认一致）
pub const DEFAULT_GC_MAX_SEGMENTS: usize = 32;

/// 内置 GC 默认单轮过期扫描物理删除键数上限（256）
pub const DEFAULT_GC_MAX_BATCH_DELETES: usize = 256;

/// 内置 GC 默认死亡虚拟 ID 队列高水位（1024；积压超过即熔断加速紧缩）
pub const DEFAULT_GC_DEAD_HIGH_WATERMARK: usize = 1024;

/// 内置 GC 默认死亡虚拟 ID 队列低水位（256；回落至此退出加速，迟滞死区防振荡）
pub const DEFAULT_GC_DEAD_LOW_WATERMARK: usize = 256;

/// 内置 GC 后台管理器配置（对标 Garnet ExpiredKeyDeletionTask + CompactionTask）
///
/// 内置存储引擎 GC 配置（主动过期扫描 + 日志紧缩调度）
///
/// 本配置驱动的 [`crate::gc::GcManager`] 随引擎内置启动，同时承担 key 级 TTL 主动
/// 过期扫描与内置日志紧缩调度，覆盖无服务端进程的纯嵌入式场景；紧缩不设独立周期
/// 旋钮（判定节奏单点见 `crate::gc` 模块注释「换号物理回收四面」段），本结构体只
/// 给阈值与档位（[`GcConfig::compaction_max_segments`] / [`GcConfig::compaction_type`]）。
///
/// 默认配置对标 Garnet：`ExpiredKeyDeletionScanFrequencySecs = -1`，
/// 默认关闭后台周期任务（enabled: false, scan_interval_ms: 0），
/// 由读请求的惰性过期（Passive Expiration）进行淘汰，并支持客户端随时发送 `EXPDELSCAN` 手动扫描。
///
/// 运行态语义：打开引擎后本配置快照进 `WedbStore` 的共享配置句柄，GC 驱动循环
/// **每轮重读**（对标 Garnet 每轮读取 RuntimeServerConfig），经
/// `WedbStore::update_gc_config` 热更新间隔/阈值/开关，下一轮即生效，无需重启。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcConfig {
  /// 是否启用内置 GC（默认 false：对标 Garnet 默认 -1 禁用后台主动扫描）
  pub enabled: bool,
  /// 主动过期扫描间隔毫秒（默认 0：禁用后台定时循环；大于 0 时启用主动增量扫描）
  pub scan_interval_ms: u64,
  /// 紧缩触发阈值段数：`read_only - begin > 本值 × segment_size` 时触发紧缩
  /// （默认 32；0 = 永不紧缩。segment_size 取设备分段大小，无分段设备回退 hlog page_size）
  pub compaction_max_segments: usize,
  /// 日志紧缩档位（默认 None，对标 C# GarnetServerOptions.CompactionType /
  /// RuntimeServerConfig COMPACTION_TYPE 每轮现取；CONFIG SET compaction-type 经
  /// 调停消息投影进本字段）：
  /// - None：关闭常规阈值紧缩（C# DoCompactionAsync 首行判 None 短路的对译）；
  ///   死亡虚拟 ID 账本积压熔断态仍以 Lookup 档旁路推进——换号物理回收是
  ///   doc/zh/db.md「偏序 GC 屏障 + 高低水位熔断」承诺的安全机制，不受旋钮关闭，
  ///   且共享单日志内混有他库活记录，必须以活性校验档执行；
  /// - Shift：不搬记录直接推进 begin（数据丢弃档，对标 C#
  ///   `ShiftBeginAddress(untilAddress, true, …)`；回退段数钳制 max-1，until 恒低于
  ///   只读线至少一段）。wedb 移位经设备截断无条件物理回收历史段，C#
  ///   `truncateLog` 参数面无对应；
  /// - Lookup / Scan：活性校验紧缩（转发 `wcompact::CompactionType` 同名档）。
  ///
  /// C# 的 CompactionForceDelete 不移植：wedb 紧缩/移位本就物理回收段文件，
  /// 「紧缩后 commit AOF + Truncate 才真正删文件」的次序无对位需求。
  pub compaction_type: LogCompactionType,
  /// 单轮过期扫描物理删除键数上限（默认 256，下限钳制 1）
  pub max_batch_deletes: usize,
  /// 单轮冷区过期扫描记录数上限（默认 4096，下限钳制 1）；游标跨轮继续，覆盖冷区。
  /// 热区窗口（read_only 之上）每轮全量扫描不受此限——纯内存指针行走，对标 Garnet
  /// 每 tick 全窗口扫描，窗口大小由内存缓冲区天然约束
  pub max_scan_records: usize,
  /// 数据库级垃圾回收的延时等待秒数（默认 24 小时 = 86400 秒）。
  /// 逾期旧虚库 ID 及对应废弃记录方可安全从物理层面抹除，严防幽灵读取。
  ///
  /// 到期判定口径：登记时按本值算出 `expired_at` 入死亡账本小根堆，内置 GC
  /// 轮次（`GcManager::sweep_vdb`）每轮只弹「已到期且 begin 已越过其
  /// tail_address」的前缀，成本与到期量成正比——故库级回收不设第二个周期
  /// 旋钮，节奏统一由本结构体的 `scan_interval_ms` 单点承载（C# 无虚库换号，
  /// 无对应物）
  pub db_gc_reclaim_delay_secs: u64,
  /// 死亡虚拟 ID 队列高水位（默认 1024；0 = 熔断关闭，行为同无水位判定）。
  ///
  /// 积压条目数（`vdb.gc_dead` 长度）超过本值时置位熔断：常规 `None` 档亦旁路
  /// 短路以 Lookup 活性校验档推进，且单轮回退段数提升至
  /// `compaction_max_segments` 全速紧缩，快速推进 begin 越过死亡条目的
  /// `tail_address`，防止换号旧垃圾积压拖垮磁盘占用（承诺见 doc/zh/db.md
  /// 「高低水位熔断」段）
  pub gc_dead_high_watermark: usize,
  /// 死亡虚拟 ID 队列低水位（默认 256，迟滞退出水位）。
  ///
  /// 熔断置位后积压回落至本值以下（<=）退出加速；两水位之间为迟滞死区，
  /// 维持原态防临界振荡。本值大于高水位时按高水位钳制，杜绝倒挂失效
  pub gc_dead_low_watermark: usize,
  /// 租户路由快照空闲析构期限秒数（默认 300；0 = 引用归零即期可析构）。
  ///
  /// 会话解绑使租户路由引用归零时登记期限，内置 GC 轮次到期摘除快照释放
  /// 内存（连接断开 + 长期空闲双触发）；析构后访问经磁盘点查装载回建，
  /// 映射权威在磁盘 DbMeta，析构绝不变更任何映射（承诺见 doc/zh/db.md
  /// 「冷租户按需加载与零全局常驻内存」段）
  pub route_idle_evict_secs: u64,
}

impl Default for GcConfig {
  fn default() -> Self {
    Self {
      enabled: false,
      scan_interval_ms: 0,
      compaction_max_segments: DEFAULT_GC_MAX_SEGMENTS,
      compaction_type: LogCompactionType::None,
      max_batch_deletes: DEFAULT_GC_MAX_BATCH_DELETES,
      max_scan_records: 4096,
      db_gc_reclaim_delay_secs: 86400,
      gc_dead_high_watermark: DEFAULT_GC_DEAD_HIGH_WATERMARK,
      gc_dead_low_watermark: DEFAULT_GC_DEAD_LOW_WATERMARK,
      route_idle_evict_secs: 300,
    }
  }
}

/// 顶层存储引擎配置
#[derive(Debug, Clone, PartialEq)]
pub struct StoreConfig {
  /// 哈希索引主桶数（必须为 2 的幂且大于 0）
  ///
  /// # 容量规划契约（初始容量配置，运行期支持在线动态扩容）
  ///
  /// - 布局：每桶 64B（8 槽 × 8B，其中 7 个数据槽 + 1 个溢出指针/锁槽），
  ///   索引内存成本 = `index_size × 64B`；可用 [`Self::recommended_index_size`]
  ///   按预期键数推导建议值。
  /// - 初始容量：打开/恢复时按本值一次性建表。运行期支持通过在线动态扩容状态机
  ///   （`grow_index` / `SplitIndex`）平滑翻倍扩容。
  /// - 超限行为：主桶满后碰撞链经溢出桶增长（写入延迟渐进劣化，不丢数据）；
  ///   溢出桶池（全局 4,194,304 桶上限）耗尽后写入显式报错
  ///   `windex::Error::OverflowPoolExhausted`，拒绝服务而非静默失败。
  /// - 恢复：检查点恢复路径的容量完全由持久化 StoreMeta.index_size 决定，
  ///   索引快照与元数据严格相等校验（wcpr），绝不静默缩表。
  pub index_size: usize,
  /// 混合日志单页大小（必须为 2 的幂且为扇区大小的整数倍）
  pub page_size: usize,
  /// 环形缓冲区页数（必须为 2 的幂且大于 0）
  pub num_pages: usize,
  /// 内存中可变区所占比例（范围 (0.0, 1.0]）
  pub mutable_fraction: f64,
  /// 最大并发客户端会话数（LightEpoch 参与者容量，必须大于 0）
  pub max_sessions: usize,
  /// 基于磁盘的 RangeIndex 根目录路径（若为 None 则自动生成系统临时目录下的独立路径）
  pub range_index_dir: Option<PathBuf>,
  /// 是否启用空间复活回收池与链内原地复活（严格对标 C# Garnet --reviv 与 RevivificationSettings）
  pub enable_revivification: bool,
  /// 复活区间比例（可变区中允许复活的最靠后比例，默认 1.0 = 全可变区）
  ///
  /// 非默认值时必须落在 (0.0, mutable_fraction] 区间（对标
  /// RevivificationSettings.Verify：负值/零拒绝，超出可变区比例拒绝）；分配复活
  /// 槽位时下限按 `tail - (tail - read_only) × 本值` 推导，防止复活写紧贴只读区
  /// 边界、被并发只读线推进追尾。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationSettings.cs:RevivifiableFraction
  pub revivifiable_fraction: f64,
  /// 是否启用 ReadCache 独立只读非脏页内存日志系统（严格对标 Garnet ReadCacheEnabled）
  ///
  /// 默认 false（对标 C# GarnetServerOptions EnableReadCache 默认 false）。
  /// 生产命令行经 wconf `hlog.read_cache` 开关暴露（回写窗撕裂隐患已按槽位
  /// 两阶段关闭协议闭环，见 read_cache/append.rs `append` 文档）。其余启用方 =
  /// `StoreConfig::with_read_cache` 经 `wnode::open_node_with_config` 嵌入式
  /// 注入，或检查点恢复面按 StoreMeta 自动复原（store/cpr_host.rs `from_recovered`）。
  /// 驱动 ReadCache 引擎本体（read_cache/ 目录）与冷读回填/promotion 链（raw/read.rs）
  pub enable_read_cache: bool,
  /// ReadCache 内存页数（必须为 2 的幂；默认 64 页，随 page_size 线性伸缩。
  /// 仅 `enable_read_cache` 为真时才物化整环内存，禁用态零预算占位）
  pub read_cache_num_pages: usize,
  /// 冷区/磁盘读取成功后是否将记录复制晋升到日志 Tail（对标 C#
  /// Options.cs:126-128 CopyReadsToTail 经 GarnetServerOptions.cs:899-900 投影进
  /// kvSettings.ReadCopyOptions——store 级配置而非会话私有；默认 false）
  ///
  /// 本字段为冷读晋升门的唯一真源：生产投影单点在 wconf hlog 段经 wnode
  /// `service.rs::apply_hlog_overrides` 写入；会话侧 `copy_reads_to_tail` 位
  /// 仅在装配期从本配置取初值（`StoreSession::new`），不构成第二套真源
  pub copy_reads_to_tail: bool,
  /// 内置 GC 后台管理器配置（过期扫描 + 紧缩调度，随引擎内置自动启动；
  /// 打开后运行态经 `WedbStore::update_gc_config` 热更新，每轮生效）
  pub gc: GcConfig,
}

impl Default for StoreConfig {
  fn default() -> Self {
    Self::minimal()
  }
}

/// 最小自适应物理内存预算基线（5 MB，保证最小索引与最小环形缓冲）
pub const MIN_ADAPTIVE_BUDGET_BYTES: u64 =
  (MIN_INDEX_SIZE * INDEX_BUCKET_BYTES) as u64 + (MIN_NUM_PAGES * DEFAULT_PAGE_SIZE) as u64;

impl StoreConfig {
  /// 自动探测当前宿主机物理硬件（可用内存与 CPU 核心数），推导最优配置
  #[must_use]
  pub fn auto() -> Self {
    let sys_mem = detect_system_memory();
    let budget = (sys_mem * DEFAULT_MEMORY_PERCENT / 100)
      .clamp(MIN_MEMORY_BUDGET_BYTES, MAX_DEFAULT_MEMORY_BUDGET_BYTES);
    Self::auto_with_budget(budget)
  }

  /// 根据指定的物理内存预算（字节），自适应推导最佳的日志页数、哈希索引容量与并发会话数
  #[must_use]
  pub fn auto_with_budget(memory_bytes: u64) -> Self {
    Self::from_memory_budget_with_keys(memory_bytes, None)
  }

  /// 根据目标内存预算与预期键数量自适应推导最优细节参数
  ///
  /// 规划保证：
  /// 1. 严格不超标：`index_bytes + hlog_bytes <= memory_bytes`，杜绝盲目向上取整导致溢出；
  /// 2. 向下兼容任意低内存约束（如 32MB、64MB、128MB、256MB、1GB 等），不设 256MB 硬编码壁垒；
  /// 3. 若提供 `expected_keys`，按数据规模自适应权衡哈希桶与日志缓冲区；否则按 37.5% 索引 + 62.5% 日志配比；
  /// 4. 页容量随预算自适应（`budget / 64` 向下取 2 的幂，钳制 `[64KB, 16MB]`）：
  ///    生产大机（预算 ≥ 1GB）推导 16MB 页，单页内联承载 C#
  ///    DefaultMaxInlineValueSize = 1MB 的大值记录（对标 GarnetServerOptions
  ///    PageSize = "16m"）；whlog 为常驻整页分配模型，小预算宿主按比例收缩页容量，
  ///    内存占用与旧版 64KB 固定页完全同量级
  #[must_use]
  pub fn from_memory_budget_with_keys(memory_bytes: u64, expected_keys: Option<u64>) -> Self {
    let budget = memory_bytes.max(MIN_ADAPTIVE_BUDGET_BYTES);
    let page_size = (prev_power_of2(budget / PAGE_SIZE_PLANNING_DIVISOR) as usize)
      .clamp(DEFAULT_PAGE_SIZE, DEFAULT_SERVER_PAGE_SIZE);

    // 1. 哈希索引桶数规划：
    // 保证日志环形缓冲至少留足 MIN_NUM_PAGES (16 * 64KB = 1MB)
    let max_index_bytes = budget.saturating_sub((MIN_NUM_PAGES * page_size) as u64);
    // 索引最多不超过总预算的 50%（或小内存下至少允许 MIN_INDEX_SIZE）
    let capped_index_bytes = (budget / 2)
      .min(max_index_bytes)
      .max((MIN_INDEX_SIZE * INDEX_BUCKET_BYTES) as u64);
    let max_buckets = ((capped_index_bytes / INDEX_BUCKET_BYTES as u64) as usize)
      .clamp(MIN_INDEX_SIZE, MAX_INDEX_SIZE);
    // 向下取 2 的幂，绝不超预算
    let max_index_size = prev_power_of2(max_buckets as u64) as usize;

    let index_size = if let Some(keys) = expected_keys {
      let rec = Self::recommended_index_size(keys);
      rec
        .min(max_index_size)
        .clamp(MIN_INDEX_SIZE, MAX_INDEX_SIZE)
    } else {
      let default_index_bytes = budget * 3 / 8; // 37.5%
      let default_buckets = ((default_index_bytes / INDEX_BUCKET_BYTES as u64) as usize)
        .clamp(MIN_INDEX_SIZE, MAX_INDEX_SIZE);
      (prev_power_of2(default_buckets as u64) as usize)
        .min(max_index_size)
        .clamp(MIN_INDEX_SIZE, MAX_INDEX_SIZE)
    };

    let index_bytes = (index_size * INDEX_BUCKET_BYTES) as u64;

    // 2. 日志环形缓冲页数规划：严格向下对齐，保证总和 <= budget
    let log_budget = budget.saturating_sub(index_bytes);
    let raw_pages = (log_budget / page_size as u64) as usize;
    let num_pages = if raw_pages >= MIN_NUM_PAGES {
      (prev_power_of2(raw_pages as u64) as usize).clamp(MIN_NUM_PAGES, MAX_NUM_PAGES)
    } else {
      MIN_NUM_PAGES
    };

    let cores = detect_cpu_cores();
    let max_sessions = (cores.saturating_mul(SESSIONS_PER_CORE))
      .next_power_of_two()
      .clamp(MIN_SESSIONS, MAX_SESSIONS);

    // 自适应推导量：index/page/页数/会话数；其余（可变区占比与默认尾）
    // 一律继承 [`Self::minimal`] 单点基线
    Self {
      index_size,
      page_size,
      num_pages,
      max_sessions,
      ..Self::minimal()
    }
  }

  /// 构造极简静态微型配置（对应轻量 1MB 环形缓冲与 128 会话基线，常用于微型嵌入式或特定单元测试）
  ///
  /// 本函数是 [`StoreConfig`] 默认值的单点真源：`Default`、[`Self::new`]、
  /// [`Self::from_memory_budget_with_keys`] 均以函数式更新 `..Self::minimal()`
  /// 继承本处定义的字段默认尾（`mutable_fraction` / `max_sessions` /
  /// `range_index_dir` / `enable_revivification` / `revivifiable_fraction` /
  /// `enable_read_cache` / `read_cache_num_pages` / `copy_reads_to_tail` / `gc`），
  /// 新增配置字段只在此处落点
  #[must_use]
  pub fn minimal() -> Self {
    Self {
      index_size: DEFAULT_INDEX_SIZE,
      page_size: DEFAULT_PAGE_SIZE,
      num_pages: DEFAULT_NUM_PAGES,
      mutable_fraction: DEFAULT_MUTABLE_FRACTION,
      max_sessions: DEFAULT_MAX_SESSIONS,
      range_index_dir: None,
      enable_revivification: false,
      revivifiable_fraction: DEFAULT_REVIVIFIABLE_FRACTION,
      enable_read_cache: false,
      read_cache_num_pages: DEFAULT_READ_CACHE_NUM_PAGES,
      copy_reads_to_tail: false,
      gc: GcConfig::default(),
    }
  }

  /// 按预期键数推导建议 `index_size`（50% 负载因子上限，向上取 2 的幂）
  ///
  /// 估算式：`index_size = next_power_of_two(expected_keys × 2 / 7)`——每桶 7 个
  /// 数据槽，预留一半余量保持低碰撞链；结果钳制在
  /// `[MIN_INDEX_SIZE, MAX_INDEX_SIZE]` 区间。建议再按业务增长预留倍数放大。
  #[must_use]
  pub fn recommended_index_size(expected_keys: u64) -> usize {
    let buckets = (expected_keys.saturating_mul(2))
      .div_ceil(INDEX_BUCKET_DATA_SLOTS as u64)
      .min(MAX_INDEX_SIZE as u64);
    usize::try_from(buckets).map_or(MAX_INDEX_SIZE, |b| {
      b.next_power_of_two().clamp(MIN_INDEX_SIZE, MAX_INDEX_SIZE)
    })
  }

  /// 校验配置合法性（索引/日志/页数/占比/读缓存容量）
  ///
  /// 打开与恢复组件装配入口均须调用：`StoreConfig` 字段公开，调用方可绕过
  /// [`Self::new`] 手搓结构体，校验必须在资源分配前拦截非法容量。
  pub fn validate(&self) -> Result<()> {
    if self.index_size == 0 || !self.index_size.is_power_of_two() {
      // 报错自带容量规划契约：当前值、定容与超限语义、就近建议值（预期键数
      // 无法在此获知，给出去就近 2 的幂并提示按公式预留 50% 负载余量）
      let mut buf = Buffer::new();
      let mut msg = String::from("index_size 必须为非零且为 2 的幂，当前为 ");
      msg.push_str(buf.format(self.index_size));
      msg.push_str("；索引打开时按此值定容，运行期支持在线动态扩容（grow_index 状态机平滑翻倍，每桶 64B、7 数据槽/桶），容量不足将先拉长碰撞链劣化写入延迟、直至溢出桶池耗尽（OverflowPoolExhausted）显式拒绝写入；建议按预期键数 K 取 next_power_of_two(K×2/7) 预留 50% 负载余量，当前值就近建议 ");
      let suggest = if self.index_size == 0 {
        MIN_INDEX_SIZE
      } else {
        self.index_size.next_power_of_two()
      };
      msg.push_str(buf.format(suggest));
      msg.push_str(" 桶");
      return Err(Error::InvalidConfig(msg));
    }
    if !self.page_size.is_power_of_two() {
      let mut msg = String::from("page_size 必须为 2 的幂，当前为 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(self.page_size));
      return Err(Error::InvalidConfig(msg));
    }
    if !self.page_size.is_multiple_of(DEFAULT_SECTOR_SIZE) {
      let mut msg = String::from("page_size 必须是 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(DEFAULT_SECTOR_SIZE));
      msg.push_str(" 的整数倍，当前为 ");
      msg.push_str(buf.format(self.page_size));
      return Err(Error::InvalidConfig(msg));
    }
    if !self.num_pages.is_power_of_two() || self.num_pages == 0 {
      let mut msg = String::from("num_pages 必须为非零且为 2 的幂，当前为 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(self.num_pages));
      return Err(Error::InvalidConfig(msg));
    }
    if self.mutable_fraction <= 0.0 || self.mutable_fraction > 1.0 {
      let mut fmt_buf = FmtBuffer::new();
      let mut msg = String::from("mutable_fraction 必须在 (0.0, 1.0] 区间内，当前为 ");
      msg.push_str(fmt_buf.format(self.mutable_fraction));
      return Err(Error::InvalidConfig(msg));
    }
    // 对标 RevivificationSettings.Verify：非默认比例时必须为正且不超出可变区占比
    if self.revivifiable_fraction != DEFAULT_REVIVIFIABLE_FRACTION
      && (self.revivifiable_fraction <= 0.0 || self.revivifiable_fraction > self.mutable_fraction)
    {
      let mut fmt_buf = FmtBuffer::new();
      let mut msg = String::from(
        "revivifiable_fraction 必须为 1.0 或落在 (0.0, mutable_fraction] 区间内，当前为 ",
      );
      msg.push_str(fmt_buf.format(self.revivifiable_fraction));
      return Err(Error::InvalidConfig(msg));
    }
    if !self.read_cache_num_pages.is_power_of_two() || self.read_cache_num_pages == 0 {
      let mut msg = String::from("read_cache_num_pages 必须为非零且为 2 的幂，当前为 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(self.read_cache_num_pages));
      return Err(Error::InvalidConfig(msg));
    }
    if self.max_sessions == 0 {
      return Err(Error::InvalidConfig("max_sessions 必须大于 0".into()));
    }
    Ok(())
  }

  /// 创建并校验顶层存储引擎配置
  pub fn new(
    index_size: usize,
    page_size: usize,
    num_pages: usize,
    mutable_fraction: f64,
  ) -> Result<Self> {
    // 入参四项之外的会话数与默认尾全部继承 [`Self::minimal`]
    let config = Self {
      index_size,
      page_size,
      num_pages,
      mutable_fraction,
      ..Self::minimal()
    };
    config.validate()?;
    Ok(config)
  }

  /// 配置并发会话上限（必须大于 0）
  pub fn with_max_sessions(mut self, max_sessions: usize) -> Result<Self> {
    self.max_sessions = max_sessions;
    self.validate()?;
    Ok(self)
  }

  /// 启用或禁用空间复活回收池
  pub fn with_revivification(mut self, enable: bool) -> Self {
    self.enable_revivification = enable;
    self
  }

  /// 配置复活区间比例（必须为 1.0 或落在 (0.0, mutable_fraction] 区间）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Revivification/RevivificationSettings.cs:Verify
  pub fn with_revivifiable_fraction(mut self, fraction: f64) -> Result<Self> {
    self.revivifiable_fraction = fraction;
    self.validate()?;
    Ok(self)
  }

  /// 启用或禁用 ReadCache 独立读缓存
  pub fn with_read_cache(mut self, enable: bool) -> Self {
    self.enable_read_cache = enable;
    self
  }

  /// 启用或禁用冷读复制晋升 Tail（本字段真源在 StoreConfig，会话装配期取初值；
  /// 对标 C# GarnetServerOptions.cs:899-900 CopyReadsToTail →
  /// kvSettings.ReadCopyOptions 的 store 级投影形态）
  pub fn with_copy_reads_to_tail(mut self, enable: bool) -> Self {
    self.copy_reads_to_tail = enable;
    self
  }

  /// 配置 ReadCache 内存页数（必须为 2 的幂且大于 0）
  pub fn with_read_cache_pages(mut self, num_pages: usize) -> Result<Self> {
    self.read_cache_num_pages = num_pages;
    self.validate()?;
    Ok(self)
  }

  /// 自定义基于磁盘的 RangeIndex 根目录路径
  pub fn with_range_index_dir(mut self, path: impl Into<PathBuf>) -> Self {
    self.range_index_dir = Some(path.into());
    self
  }

  /// 转换为 HybridLog 核心配置
  pub fn to_hlog_config(&self) -> Result<HybridLogConfig> {
    HybridLogConfig::new(self.page_size, self.num_pages, self.mutable_fraction).map_err(Into::into)
  }
}
