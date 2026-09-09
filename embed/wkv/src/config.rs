use std::{mem::size_of, path::PathBuf};

use itoa::Buffer;
use wdev::{detect_cpu_cores, detect_system_memory};
use whlog::{
  DEFAULT_MUTABLE_FRACTION, DEFAULT_NUM_PAGES, DEFAULT_PAGE_SIZE, HybridLogConfig, SECTOR_ALIGNMENT,
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

/// 默认最大并发纪元会话数基线（128）
pub const DEFAULT_MAX_SESSIONS: usize = 128;

/// 最小自适应物理内存预算（256 MB）
pub const MIN_MEMORY_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// 默认最大自动自适应内存预算（32 GB，防止默认独占全部宿主内存）
pub const MAX_DEFAULT_MEMORY_BUDGET_BYTES: u64 = 32 * 1024 * 1024 * 1024;

/// 默认物理内存占比百分比（25%）
pub const DEFAULT_MEMORY_PERCENT: u64 = 25;

/// 每个 CPU 物理/逻辑核心默认派生并发会话数
pub const SESSIONS_PER_CORE: usize = 16;

/// 最小并发会话下限（128）
pub const MIN_SESSIONS: usize = 128;

/// 最大并发会话上限（1024）
pub const MAX_SESSIONS: usize = 1024;

/// 最小自适应哈希桶数量（65536）
pub const MIN_INDEX_SIZE: usize = 65536;

/// 最大自适应哈希桶数量（16,777,216，即 16M 桶，对应 1GB 索引内存，可容纳超 1 亿条记录）
pub const MAX_INDEX_SIZE: usize = 16_777_216;

/// 最小自适应混合日志页数下限（16 页）
pub const MIN_NUM_PAGES: usize = 16;

/// 最大自适应混合日志页数上限（1,048,576 页，按 64KB 页面对应 64GB 日志缓冲）
pub const MAX_NUM_PAGES: usize = 1_048_576;

/// ReadCache 默认内存页数（64 页 × 64KB = 4MB DRAM 只读缓存预算，2 的幂满足容量校验）
///
/// 对齐 Garnet 默认为读多写少负载提供充足缓存窗口：过小（如 8 页 = 512KB）会在热键集
/// 略大时触发滑动窗口高频换页驱逐，导致冷读反复击穿到磁盘。
pub const DEFAULT_READ_CACHE_NUM_PAGES: usize = 64;

/// 内置 GC 默认主动过期扫描间隔毫秒（5 秒）
pub const DEFAULT_GC_SCAN_INTERVAL_MS: u64 = 5_000;

/// 内置 GC 默认日志紧缩判定间隔毫秒（60 秒）
///
/// Garnet CompactionTask 频率在 wedb 由 [`GcConfig::compaction_interval_ms`]
/// 统一承担：不再单设 StoreConfig 级周期紧缩配置（原 `compaction_freq_secs` 已删），
/// 避免两条调度链对同一日志各自判定紧缩时机。
pub const DEFAULT_GC_COMPACTION_INTERVAL_MS: u64 = 60_000;

/// 内置 GC 默认紧缩触发阈值段数（8，对标 C# Garnet CompactionMaxSegments 默认值）
pub const DEFAULT_GC_MAX_SEGMENTS: usize = 8;

/// 内置 GC 默认单轮紧缩回退段数（1，对标 C# Garnet CompactionNumSegmentsToCompact 默认值）
pub const DEFAULT_GC_NUM_SEGMENTS: usize = 1;

/// 内置 GC 默认单轮过期扫描物理删除键数上限（256）
pub const DEFAULT_GC_MAX_BATCH_DELETES: usize = 256;

/// 单轮主动过期扫描记录数上限
pub const DEFAULT_GC_MAX_SCAN_RECORDS: usize = 4096;

/// 内置 GC 后台管理器配置（对标 Garnet ExpiredKeyDeletionTask + CompactionTask）
///
/// 内置存储引擎 GC 配置（主动过期扫描 + 日志紧缩调度）
///
/// 本配置驱动的 [`crate::gc::GcManager`] 随引擎内置启动，同时承担 key 级 TTL 主动
/// 过期扫描与内置日志紧缩调度，覆盖无服务端进程的纯嵌入式场景；Garnet CompactionTask
/// 频率在 wedb 由 [`GcConfig::compaction_interval_ms`] 统一承担（引擎仅此一条紧缩
/// 调度链，避免双轨配置对同一日志各自判定时机）。
///
/// 默认配置对标 Garnet：`ExpiredKeyDeletionScanFrequencySecs = -1`，
/// 默认关闭后台周期任务（enabled: false, scan_interval_ms: 0, compaction_interval_ms: 0），
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
  /// 日志紧缩判定间隔毫秒（默认 0：禁用后台定时判定）
  pub compaction_interval_ms: u64,
  /// 紧缩触发阈值段数：`read_only - begin > 本值 × segment_size` 时触发紧缩
  /// （默认 8；0 = 永不紧缩。segment_size 取设备分段大小，无分段设备回退 hlog page_size）
  pub compaction_max_segments: usize,
  /// 单轮紧缩的日志回退段数（默认 1）：`until = read_only - segment_size × (max - 本值)`，
  /// 对齐 DatabaseManagerBase.cs:425；超过阈值段数时按阈值钳制
  pub compaction_num_segments: usize,
  /// 单轮过期扫描物理删除键数上限（默认 256，下限钳制 1）
  pub max_batch_deletes: usize,
  /// 单轮冷区过期扫描记录数上限（默认 4096，下限钳制 1）；游标跨轮继续，覆盖冷区。
  /// 热区窗口（read_only 之上）每轮全量扫描不受此限——纯内存指针行走，对标 Garnet
  /// 每 tick 全窗口扫描，窗口大小由内存缓冲区天然约束
  pub max_scan_records: usize,
}

impl GcConfig {
  /// 开箱即用的生产推荐配置（`default()` 全关对标 Garnet 默认 -1，本构造一行开启）
  ///
  /// enabled + 主动过期扫描 5s（[`DEFAULT_GC_SCAN_INTERVAL_MS`]）+ 紧缩判定 60s
  /// （[`DEFAULT_GC_COMPACTION_INTERVAL_MS`]），其余参数取默认。
  #[must_use]
  pub fn tuned() -> Self {
    Self {
      enabled: true,
      scan_interval_ms: DEFAULT_GC_SCAN_INTERVAL_MS,
      compaction_interval_ms: DEFAULT_GC_COMPACTION_INTERVAL_MS,
      ..Self::default()
    }
  }
}

impl Default for GcConfig {
  fn default() -> Self {
    Self {
      enabled: false,
      scan_interval_ms: 0,
      compaction_interval_ms: 0,
      compaction_max_segments: DEFAULT_GC_MAX_SEGMENTS,
      compaction_num_segments: DEFAULT_GC_NUM_SEGMENTS,
      max_batch_deletes: DEFAULT_GC_MAX_BATCH_DELETES,
      max_scan_records: DEFAULT_GC_MAX_SCAN_RECORDS,
    }
  }
}

/// 顶层存储引擎配置
#[derive(Debug, Clone, PartialEq)]
pub struct StoreConfig {
  /// 哈希索引主桶数（必须为 2 的幂且大于 0）
  ///
  /// # 容量规划契约（打开时定容，运行期无在线扩容）
  ///
  /// - 布局：每桶 64B（8 槽 × 8B，其中 7 个数据槽 + 1 个溢出指针/锁槽），
  ///   索引内存成本 = `index_size × 64B`；可用 [`Self::recommended_index_size`]
  ///   按预期键数推导建议值。
  /// - 定容：打开/恢复时按本值一次性建表，构造后终身不可变（无在线 rehash）。
  ///   键规模增长前须按新容量重建索引。
  /// - 超限行为：主桶满后碰撞链经溢出桶增长（写入延迟渐进劣化，不丢数据）；
  ///   溢出桶池（全局 4,194,304 桶上限）耗尽后写入显式报错
  ///   `windex::Error::OverflowPoolExhausted`，拒绝服务而非静默失败。
  /// - 恢复：检查点恢复路径的容量完全由持久化 StoreMeta.index_size 决定，
  ///   索引快照与元数据严格相等校验（wcpr），绝不静默缩表。
  pub index_size: usize,
  /// 混合日志单页大小（必须为 2 的幂且为 4096 的整数倍）
  pub page_size: usize,
  /// 环形缓冲区页数（必须为 2 的幂且大于 0）
  pub num_pages: usize,
  /// 内存中可变区所占比例（范围 (0.0, 1.0]）
  pub mutable_fraction: f64,
  /// 最大并发客户端会话数（LightEpoch 参与者容量，必须大于 0）
  pub max_sessions: usize,
  /// 基于磁盘的 BfTree 有序索引文件路径（若为 None 则自动生成系统临时目录下的独立磁盘文件）
  pub bftree_path: Option<PathBuf>,
  /// 基于磁盘的 RangeIndex 根目录路径（若为 None 则自动生成系统临时目录下的独立路径）
  pub range_index_dir: Option<PathBuf>,
  /// 是否启用空间复活回收池与链内原地复活（严格对标 C# Garnet --reviv 与 RevivificationSettings）
  pub enable_revivification: bool,
  /// 是否启用 ReadCache 独立只读非脏页内存日志系统（严格对标 Garnet ReadCacheEnabled）
  pub enable_read_cache: bool,
  /// ReadCache 内存页数（必须为 2 的幂；默认 64 页，按 64KB 页面对应 4MB DRAM 预算，随 page_size 线性伸缩）
  pub read_cache_num_pages: usize,
  /// 内置 GC 后台管理器配置（过期扫描 + 紧缩调度，随引擎内置自动启动；
  /// 打开后运行态经 `WedbStore::update_gc_config` 热更新，每轮生效）
  pub gc: GcConfig,
}

impl Default for StoreConfig {
  fn default() -> Self {
    Self::minimal()
  }
}

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
    let budget = memory_bytes.max(MIN_MEMORY_BUDGET_BYTES);
    let page_size = DEFAULT_PAGE_SIZE;
    // 将总预算按约 3/4 分配给日志环形缓冲区，1/4 留给哈希索引与对齐缓冲池
    let log_budget = budget * 3 / 4;
    let target_pages =
      ((log_budget / (page_size as u64)) as usize).clamp(MIN_NUM_PAGES, MAX_NUM_PAGES);
    let num_pages = target_pages.next_power_of_two();

    // 索引桶数自适应：每个桶 64 字节，分配约 1/8 预算作为索引，并约束在合理边界
    let index_budget = budget / 8;
    let target_buckets = (index_budget / 64) as usize;
    let index_size = target_buckets
      .next_power_of_two()
      .clamp(MIN_INDEX_SIZE, MAX_INDEX_SIZE);

    let cores = detect_cpu_cores();
    let max_sessions = (cores * SESSIONS_PER_CORE)
      .next_power_of_two()
      .clamp(MIN_SESSIONS, MAX_SESSIONS);

    Self {
      index_size,
      page_size,
      num_pages,
      mutable_fraction: DEFAULT_MUTABLE_FRACTION,
      max_sessions,
      bftree_path: None,
      range_index_dir: None,
      enable_revivification: false,
      enable_read_cache: false,
      read_cache_num_pages: DEFAULT_READ_CACHE_NUM_PAGES,
      gc: GcConfig::default(),
    }
  }

  /// 构造极简静态微型配置（对应轻量 1MB 环形缓冲与 128 会话基线，常用于微型嵌入式或特定单元测试）
  #[must_use]
  pub fn minimal() -> Self {
    Self {
      index_size: DEFAULT_INDEX_SIZE,
      page_size: DEFAULT_PAGE_SIZE,
      num_pages: DEFAULT_NUM_PAGES,
      mutable_fraction: DEFAULT_MUTABLE_FRACTION,
      max_sessions: DEFAULT_MAX_SESSIONS,
      bftree_path: None,
      range_index_dir: None,
      enable_revivification: false,
      enable_read_cache: false,
      read_cache_num_pages: DEFAULT_READ_CACHE_NUM_PAGES,
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
      msg.push_str("；索引打开时按此值定容且运行期无在线扩容（每桶 64B、7 数据槽/桶），容量不足将拉长碰撞链直至溢出桶池耗尽（OverflowPoolExhausted）显式拒绝写入；建议按预期键数 K 取 next_power_of_two(K×2/7) 预留 50% 负载余量，当前值就近建议 ");
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
    if !self.page_size.is_multiple_of(SECTOR_ALIGNMENT) {
      let mut msg = String::from("page_size 必须是 ");
      let mut buf = Buffer::new();
      msg.push_str(buf.format(SECTOR_ALIGNMENT));
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
    let config = Self {
      index_size,
      page_size,
      num_pages,
      mutable_fraction,
      max_sessions: DEFAULT_MAX_SESSIONS,
      bftree_path: None,
      range_index_dir: None,
      enable_revivification: false,
      enable_read_cache: false,
      read_cache_num_pages: DEFAULT_READ_CACHE_NUM_PAGES,
      gc: GcConfig::default(),
    };
    config.validate()?;
    Ok(config)
  }

  /// 自定义最大并发客户端会话容量
  pub fn with_max_sessions(mut self, max_sessions: usize) -> Result<Self> {
    self.max_sessions = max_sessions;
    self.validate()?;
    Ok(self)
  }

  /// 设置是否启用空间复活回收池（严格对标 C# Garnet --reviv 选项）
  pub fn with_revivification(mut self, enable: bool) -> Self {
    self.enable_revivification = enable;
    self
  }

  /// 设置是否启用 ReadCache 独立只读内存日志（严格对标 C# Garnet ReadCacheEnabled）
  pub fn with_read_cache(mut self, enable: bool) -> Self {
    self.enable_read_cache = enable;
    self
  }

  /// 配置 ReadCache 内存页数（必须为 2 的幂且大于 0）
  pub fn with_read_cache_pages(mut self, num_pages: usize) -> Result<Self> {
    self.read_cache_num_pages = num_pages;
    self.validate()?;
    Ok(self)
  }

  /// 自定义基于磁盘的 BfTree 有序索引文件路径
  pub fn with_bftree_path(mut self, path: impl Into<PathBuf>) -> Self {
    self.bftree_path = Some(path.into());
    self
  }

  /// 自定义基于磁盘的 RangeIndex 根目录路径
  pub fn with_range_index_dir(mut self, path: impl Into<PathBuf>) -> Self {
    self.range_index_dir = Some(path.into());
    self
  }

  /// 自定义内置 GC 配置
  pub fn with_gc(mut self, gc: GcConfig) -> Self {
    self.gc = gc;
    self
  }

  /// 转换为 HybridLog 核心配置
  pub fn to_hlog_config(&self) -> Result<HybridLogConfig> {
    HybridLogConfig::new(self.page_size, self.num_pages, self.mutable_fraction).map_err(Into::into)
  }
}
