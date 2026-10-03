//! 主存混合日志（hlog）配置段：结构与显式项校验投影
//!
//! 对标 C# libs/server/Config/ 目录的存储日志选项与 GarnetServerOptions 主存
//! 日志段（PageSize = "16m"、LogMemorySize = "16g"）。

use wbase::cfg;

use crate::{node_options::NodeOptionsError, size::validated_page_size_bits};

/// 生产默认主存日志页容量字节：取值下沉基座 [`wbase::cfg::DEFAULT_HLOG_PAGE_SIZE`]
/// （对标 C# ServerOptions.cs:46 PageSize = "16m"），本处仅按配置层既有对外
/// 口径保留同名常量，杜绝第二套定义（票
/// task/todo/whlog-waof-wconf-inline-dep-reverse-layering.md）。
pub const DEFAULT_HLOG_PAGE_SIZE: usize = cfg::DEFAULT_HLOG_PAGE_SIZE;

/// 主存混合日志（hlog）配置段（对标 libs/server/Servers/GarnetServerOptions.cs
/// 主存日志选项：PageSize = "16m"、LogMemorySize = "16g"；MutablePercent C# 缺省
/// 为 90（Servers/ServerOptions.cs:77、host/defaults.conf:64），rust 引擎缺省比例
/// 0.5 与之系未裁决分叉，见 doc/zh/deviations.md §93 留槽，严禁按任一方径改）
///
/// 全部字段可缺省：None 项不覆盖装配基线，由 `StoreConfig::auto()` 的内存预算
/// 规划器推导（大机预算 ≥ 1GB 时推导 [`DEFAULT_HLOG_PAGE_SIZE`] 页）。
/// TOML 形态：
///
/// ```toml
/// [hlog]
/// page_size = 16777216
/// memory_size = 4294967296
/// mutable_percent = 50
/// ```
pub mod usize_u64 {
  use toml_spanner::{Arena, Context, Failed, Item, ToTomlError};

  pub fn to_toml<'a>(value: &usize, _: &'a Arena) -> Result<Item<'a>, ToTomlError> {
    Ok(Item::from(*value as i128))
  }

  pub fn from_toml<'de>(ctx: &mut Context<'de>, item: &Item<'de>) -> Result<usize, Failed> {
    let Some(i) = item.as_i64() else {
      return Err(ctx.report_expected_but_found(&"an integer", item));
    };
    if i < 0 {
      return Err(ctx.report_custom_error("expected non-negative integer", item));
    }
    Ok(i as usize)
  }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, clap::Args, toml_spanner::Toml)]
#[toml(FromToml, ToToml, ignore_unknown_fields)]
pub struct HlogOptions {
  /// 主存日志单页容量字节（必须为 2 的幂且为扇区大小整数倍，且不低于
  /// [`crate::size::MIN_PAGE_SIZE_BYTES`]；未配置时由内存预算规划器推导，对标 C#
  /// GarnetServerOptions.cs PageSize = "16m"）。
  ///
  /// 页容量决定单条内联记录上限：值 ≤ 页容量 - 记录头 - 键 即可内联存储
  /// （16MB 页可承载 C# DefaultMaxInlineValueSize = 1MB 基线的大值）。
  #[arg(id = "hlog_page_size", long = "hlog-page-size")]
  #[toml(with = usize_u64)]
  pub page_size: Option<usize>,

  /// 主存日志内存环形缓冲预算字节（未配置时由内存预算规划器推导；
  /// 对标 C# GarnetServerOptions.cs LogMemorySize = "16g" 的 pageCount 推导：
  /// `num_pages = next_power_of_2(memory_size / page_size)`）
  #[arg(id = "hlog_memory_size", long = "hlog-memory-size")]
  #[toml(with = usize_u64)]
  pub memory_size: Option<usize>,

  /// 内存可变区百分比（10..=95，对标 C# GarnetServerOptions.cs:747-748 的
  /// GetSettings 区间校验；C# 缺省值为 90（ServerOptions.cs:77），未配置时取
  /// rust 引擎默认比例 0.5——两者差异系未裁决分叉，见 doc/zh/deviations.md §93
  /// 留槽，严禁按 90 或 50 任何一方径改行为）
  #[arg(id = "hlog_mutable_percent", long = "hlog-mutable-percent")]
  pub mutable_percent: Option<u8>,

  /// 是否启用 ReadCache 独立只读非脏页内存日志（对标 C# GarnetServerOptions.cs:582
  /// EnableReadCache，默认 false）
  #[arg(id = "read_cache", long = "read-cache", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub read_cache: bool,

  /// ReadCache 内存预算字节（仅 `read_cache` 开启时参与页数推导：预算 / 主日志
  /// 页容量向下取 2 的幂；对标 C# GarnetServerOptions.cs:587
  /// ReadCacheMemorySize = "1g"。未配置时取 [`DEFAULT_READ_CACHE_MEMORY_SIZE`]）
  #[arg(id = "read_cache_memory_size", long = "read-cache-memory-size")]
  #[toml(with = usize_u64)]
  pub read_cache_memory_size: Option<usize>,

  /// 升阶树页缓存全局总预算字节（长驻升阶树页环与并发升阶 scratch 环共用的
  /// 字节总闸；C#RangeIndexManager 无预算机制、CacheSizeTracker 只跟主日志与
  /// 读缓存，本闸为本仓分层架构自研组件，观测面对标 CacheSizeTracker 的
  /// TargetSize 高水位语义，见 doc/zh/collection.md。0 = 不设限，未配置时取
  /// wkv `DEFAULT_TREE_CACHE_BUDGET_BYTES`（wkv/src/config.rs）。启动期一次性注入 RangeIndexManager，
  /// 不做热更）
  #[arg(id = "tree_cache_budget", long = "tree-cache-budget")]
  #[toml(with = usize_u64)]
  pub tree_cache_budget: Option<usize>,

  /// 是否启用空间复活回收池与链内原地复活（对标 C# Options.cs:564-567
  /// EnableRevivification，命令行 `--reviv`，默认 false）
  #[arg(long = "reviv", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub reviv: bool,

  /// 复活区间比例（对标 C# Options.cs:558-561 RevivifiableFraction，命令行
  /// `--reviv-fraction`，DoubleRangeValidation(0, 1)；None = 未配置，不覆盖
  /// 引擎默认值）。区间校验单点在 wkv `StoreConfig::validate`
  /// （(0, mutable_fraction]），本处不复校验，避免第二套真源
  #[arg(long = "reviv-fraction")]
  pub reviv_fraction: Option<f64>,

  /// 冷区/磁盘读取成功后是否将记录复制晋升到日志 Tail（对标 C#
  /// Options.cs:126-128 CopyReadsToTail，命令行 `--copy-reads-to-tail`，
  /// 默认 false；C# 经 GarnetServerOptions.cs:899-900 投影进
  /// kvSettings.ReadCopyOptions，store 级而非会话私有）
  #[arg(long = "copy-reads-to-tail", default_value_t = false, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
  #[toml(default)]
  pub copy_reads_to_tail: bool,
}

/// 默认 ReadCache 内存预算字节（1GB，对标 C# GarnetServerOptions.cs
/// ReadCacheMemorySize = "1g" 默认值）
pub const DEFAULT_READ_CACHE_MEMORY_SIZE: usize = 1024 * 1024 * 1024;

/// hlog 配置段覆盖项投影（None 项原样透传装配基线）
pub struct HlogProjection {
  /// 主存日志单页容量字节
  pub page_size: Option<usize>,
  /// 主存日志内存环形缓冲预算字节
  pub memory_size: Option<usize>,
  /// 内存可变区比例（(0, 1]）
  pub mutable_fraction: Option<f64>,
  /// 是否启用 ReadCache 独立读缓存
  pub read_cache: bool,
  /// ReadCache 内存预算字节
  pub read_cache_memory_size: Option<usize>,
  /// 升阶树页缓存全局总预算字节（0 = 不设限）
  pub tree_cache_budget: Option<usize>,
  /// 是否启用空间复活回收池（对标 C# Options.cs:564-567 reviv）
  pub reviv: bool,
  /// 复活区间比例（对标 C# Options.cs:559 reviv-fraction；区间校验单点在
  /// wkv `StoreConfig::validate`，本处只透传）
  pub reviv_fraction: Option<f64>,
  /// 冷读复制晋升 Tail（对标 C# Options.cs:128 CopyReadsToTail）
  pub copy_reads_to_tail: bool,
}

/// hlog 页容量扇区对齐字节（4KB 高级格式扇区，与 wkv `StoreConfig::validate`
/// 的 `wbase::DEFAULT_SECTOR_SIZE` 校验口径一致）
pub(crate) const HLOG_PAGE_SIZE_SECTOR_BYTES: usize = 4096;

/// hlog 页容量配置属性名（校验文案点名用，与命令行长线名同字面）
const HLOG_PAGE_SIZE_PROP: &str = "hlog-page-size";

impl HlogOptions {
  /// 校验显式配置项合法性（页容量下限走 [`crate::size::validated_page_size_bits`] 校验核，
  /// 其余对标 C# GarnetServerOptions.GetSettings 装配期校验）
  ///
  /// 返回投影（百分比已换算为 (0, 1] 比例，C# MutableFraction =
  /// MutablePercent / 100），None 项原样透传。
  pub fn validated(&self) -> Result<HlogProjection, NodeOptionsError> {
    // 页容量投影单点（主存日志与 read cache 共用本页容量，对标 C# PageSizeBits /
    // ReadCachePageSizeBits 同调 ValidatedPageSizeBits）：取幂 + MIN_PAGE_SIZE_BYTES
    // 下限由校验核裁决；引擎侧另须页容量恰为 2 的幂且扇区对齐，故核有折损即非法。
    if let Some(p) = self.page_size {
      let bits = validated_page_size_bits(p as i64, HLOG_PAGE_SIZE_PROP)
        .map_err(|e| NodeOptionsError::Hlog(e.to_string()))?;
      if 1u64 << bits != p as u64 || !p.is_multiple_of(HLOG_PAGE_SIZE_SECTOR_BYTES) {
        return Err(NodeOptionsError::Hlog(format!(
          "hlog page_size 必须为 2 的幂且为 {HLOG_PAGE_SIZE_SECTOR_BYTES} 的整数倍，当前为 {p}"
        )));
      }
    }
    if self.memory_size.is_some_and(|m| m == 0) {
      return Err(NodeOptionsError::Hlog("hlog memory_size 必须大于 0".into()));
    }
    // MutablePercent is < 10 or > 95 → throw（GarnetServerOptions.GetSettings）
    if let Some(pct) = self.mutable_percent
      && !(10..=95).contains(&pct)
    {
      return Err(NodeOptionsError::Hlog(format!(
        "MutablePercent must be between 10 and 95, 当前为 {pct}"
      )));
    }
    if self.read_cache_memory_size.is_some_and(|m| m == 0) {
      return Err(NodeOptionsError::Hlog(
        "hlog read_cache_memory_size 必须大于 0".into(),
      ));
    }
    Ok(HlogProjection {
      page_size: self.page_size,
      memory_size: self.memory_size,
      mutable_fraction: self.mutable_percent.map(|p| f64::from(p) / 100.0),
      read_cache: self.read_cache,
      read_cache_memory_size: self.read_cache_memory_size,
      // 0 = 不设限为合法显式值，无区间校验
      tree_cache_budget: self.tree_cache_budget,
      // reviv 三旋钮纯透传：reviv_fraction 的区间校验单点在 wkv
      // StoreConfig::validate（(0, mutable_fraction]），此处不复校，避免第二套真源
      reviv: self.reviv,
      reviv_fraction: self.reviv_fraction,
      copy_reads_to_tail: self.copy_reads_to_tail,
    })
  }
}
