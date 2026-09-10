//! Garnet 服务器选项（对标 libs/server/Servers/GarnetServerOptions.cs）
//!
//! 承接路径派生（检查点/AOF 目录）、内存尺寸字符串解析、尺寸幂等归约
//! （MemorySizeBits 上取 2 的幂、AOF 系下取 2 的幂）、内联键值上限校验、
//! AOF 设置装配与 AOF 设备定位。Tsavorite 的 KVSettings/TsavoriteLogSettings/
//! INamedDeviceFactory 为 .NET 框架面，Rust 侧以域内设置结构 + 路径承接。

use std::{
  ops::RangeInclusive,
  path::{Path, PathBuf},
};

/// 主日志页大小的最小字节数（对齐 ServerOptions.MinPageSizeBytes：最坏内联
/// 记录约 490B + 64B 页头，512B 为可容纳的最小 2 的幂页大小）。
pub const MIN_PAGE_SIZE_BYTES: i64 = 512;

/// MaxInlineKeySize 未设置时的默认值（Tsavorite 上限 1022 字节）。
pub const DEFAULT_MAX_INLINE_KEY_SIZE: i64 = 1022;

/// MaxInlineValueSize 的绝对上限（RecordDataHeader 值长度字段的内联极限）。
const MAX_INLINE_VALUE_LIMIT: i64 = 0xFFFFFE;

/// MaxInlineValueSize 未设置时的默认值（1m）。
pub const DEFAULT_MAX_INLINE_VALUE_SIZE: i64 = 1 << 20;

/// PubSubPageSize 未设置时的默认值（4k）。
pub const DEFAULT_PUB_SUB_PAGE_SIZE: &str = "4k";

/// IndexMemorySize 未设置时的默认值（128m）。
pub const DEFAULT_INDEX_MEMORY_SIZE: &str = "128m";

/// MutablePercent 的合法区间（C# GetSettings 入口校验 [10, 95]）。
pub const MUTABLE_PERCENT_RANGE: RangeInclusive<i32> = 10..=95;

/// InitialIORecordSize 未设置的哨兵（对齐 KVSettings.UseDefaultInitialIORecordSize 语义）。
pub const USE_DEFAULT_INITIAL_IO_RECORD_SIZE: i64 = 0;

/// 选项解析错误。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OptionsError {
  /// 尺寸字符串无法解析。
  #[error("Unable to parse size value '{0}'. Expected a memory size string (e.g. '1k', '128').")]
  BadSize(String),
  /// 数值越界。
  #[error("Value '{0}' ({1} bytes) is outside the allowed range")]
  OutOfRange(String, i64),
  /// 数值必须为正。
  #[error("Value '{0}' ({1} bytes) must be positive.")]
  NotPositive(String, i64),
  /// 数值超过页大小。
  #[error("Value '{0}' ({1} bytes) exceeds the page size ({2} bytes).")]
  ExceedsPageSize(String, i64, i64),
  /// 数值超过页大小一半（保证至少一条记录落页）。
  #[error("Value '{0}' ({1} bytes) is greater than half the page size ({2} bytes).")]
  ExceedsHalfPage(String, i64, i64),
  /// 页大小低于最小值。
  #[error("Page size '{0}' (effective {1} bytes) must be at least {2} bytes")]
  PageSizeTooSmall(String, i64, i64),
  /// 索引尺寸无效（PreviousPowerOf2 后须在 [64, 2^37] 内）。
  #[error("Invalid {0}")]
  InvalidIndexSize(String),
  /// MutablePercent 越界（C# GetSettings 入口校验 [10, 95]）。
  #[error("MutablePercent must be between 10 and 95")]
  MutablePercentOutOfRange(i32),
  /// 组合非法（null AOF 设备 + 集群）。
  #[error(
    "Cannot use null device for AOF when cluster is enabled and you are not using main memory replication"
  )]
  NullDeviceWithCluster,
  /// 配置项未设置。
  #[error("Configuration value is not set")]
  NotSet,
}

/// AOF 子日志设置（TsavoriteLogSettings 的域内承接）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AofLogSettings {
  /// 子日志目录（相对 AOF 基目录）。
  pub directory: String,
  /// 日志文件名。
  pub log_file: String,
  /// 内存位数。
  pub memory_size_bits: i32,
  /// 页大小位数。
  pub page_size_bits: i32,
  /// 段大小位数。
  pub segment_size_bits: i32,
}

/// 存储设置（KVSettings 的域内承接）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreSettings {
  /// 哈希索引字节数（C# KVSettings.IndexSize = cachelines * 64）。
  pub index_size: i64,
  /// 内存位数。
  pub memory_size_bits: i32,
  /// 页大小位数。
  pub page_size_bits: i32,
  /// 段大小位数。
  pub segment_size_bits: i32,
  /// 内联键上限（字节）。
  pub max_inline_key_size: i32,
  /// 内联值上限（字节）。
  pub max_inline_value_size: i32,
  /// 初始 IO 记录大小（字节；0 = 使用默认）。
  pub initial_io_record_size: i32,
  /// 日志设备根目录。
  pub log_directory: Option<String>,
}

/// Garnet 服务器选项（Rust 侧承载的字段子集，默认值对齐 C# 字段初始化器）。
#[derive(Debug, Clone)]
pub struct GarnetServerOptions {
  /// 检查点根目录（CheckpointDir；可空）。
  pub checkpoint_dir: Option<String>,
  /// 日志根目录（LogDir；可空）。
  pub log_dir: Option<String>,
  /// 主日志内存尺寸串（默认 "16g"）。
  pub log_memory_size: String,
  /// 页尺寸串（默认 "32m"）。
  pub page_size: String,
  /// 日志初始页数（0 = 按 LogMemorySize / PageSize 推算）。
  pub page_count: i32,
  /// 哈希索引尺寸串（默认 "128m"）。
  pub index_memory_size: String,
  /// 日志内存中可变区占比（默认 90，合法区间 [10, 95]）。
  pub mutable_percent: i32,
  /// 读缓存页尺寸串（默认 "32m"）。
  pub read_cache_page_size: String,
  /// 段尺寸串（默认 "1g"）。
  pub segment_size: String,
  /// 内联键尺寸串（可空）。
  pub max_inline_key_size: Option<String>,
  /// 内联值尺寸串（可空）。
  pub max_inline_value_size: Option<String>,
  /// 初始 IO 记录尺寸串（可空）。
  pub initial_io_record_size: Option<String>,
  /// AOF 内存尺寸串（默认 "128m"）。
  pub aof_memory_size: String,
  /// AOF 页尺寸串（默认 "32m"）。
  pub aof_page_size: String,
  /// AOF 段尺寸串（默认 "1g"）。
  pub aof_segment_size: String,
  /// pub/sub 日志页尺寸串（默认 "4k"）。
  pub pub_sub_page_size: String,
  /// AOF 大小上限串（默认空 = 不限制）。
  pub aof_size_limit: String,
  /// 缓冲池预算串。
  pub buffer_pool_memory_budget: String,
  /// 磁盘less 同步全量同步 AOF 阈值串（可空，回落 AofMemorySize）。
  pub replica_diskless_sync_full_sync_aof_threshold: Option<String>,
  /// 是否使用 AOF 空设备。
  pub use_aof_null_device: bool,
  /// 是否启用集群。
  pub enable_cluster: bool,
  /// 是否快速截断 AOF。
  pub fast_aof_truncate: bool,
  /// 物理 AOF 子日志数（默认 1）。
  pub aof_physical_sublog_count: i32,
}

impl Default for GarnetServerOptions {
  fn default() -> Self {
    Self {
      checkpoint_dir: None,
      log_dir: None,
      log_memory_size: "16g".to_string(),
      page_size: "32m".to_string(),
      page_count: 0,
      index_memory_size: DEFAULT_INDEX_MEMORY_SIZE.to_string(),
      mutable_percent: 90,
      read_cache_page_size: "32m".to_string(),
      segment_size: "1g".to_string(),
      max_inline_key_size: None,
      max_inline_value_size: None,
      initial_io_record_size: None,
      aof_memory_size: "128m".to_string(),
      aof_page_size: "32m".to_string(),
      aof_segment_size: "1g".to_string(),
      pub_sub_page_size: DEFAULT_PUB_SUB_PAGE_SIZE.to_string(),
      aof_size_limit: String::new(),
      buffer_pool_memory_budget: String::new(),
      replica_diskless_sync_full_sync_aof_threshold: None,
      use_aof_null_device: false,
      enable_cluster: false,
      fast_aof_truncate: false,
      aof_physical_sublog_count: 1,
    }
  }
}

pub use super::server_options::{
  next_power_of_2, parse_size, parse_size_bytes, pretty_size, previous_power_of_2,
  try_parse_size, try_parse_size_bytes,
};

/// 位数的 log2（输入保证为 2 的幂且 > 0）。
fn log2_exact(v: i64) -> i32 {
  63 - v.leading_zeros() as i32
}

impl GarnetServerOptions {
  /// 检查点基目录（对齐 CheckpointBaseDirectory：CheckpointDir ?? LogDir ?? ""）。
  pub fn checkpoint_base_directory(&self) -> String {
    self
      .checkpoint_dir
      .clone()
      .or_else(|| self.log_dir.clone())
      .unwrap_or_default()
  }

  /// 存储检查点基目录（对齐 StoreCheckpointBaseDirectory：base/Store）。
  pub fn store_checkpoint_base_directory(&self) -> PathBuf {
    Path::new(&self.checkpoint_base_directory()).join("Store")
  }

  /// libs/server/Servers/GarnetServerOptions.cs:GetCheckpointDirectoryName
  ///
  /// 数据库检查点目录名：`checkpoints` 或 `checkpoints_{dbId}`。
  pub fn get_checkpoint_directory_name(db_id: i32) -> String {
    if db_id == 0 {
      "checkpoints".to_string()
    } else {
      format!("checkpoints_{db_id}")
    }
  }

  /// libs/server/Servers/GarnetServerOptions.cs:GetStoreCheckpointDirectory
  pub fn get_store_checkpoint_directory(&self, db_id: i32) -> PathBuf {
    self
      .store_checkpoint_base_directory()
      .join(Self::get_checkpoint_directory_name(db_id))
  }

  /// AOF 基目录（对齐 AppendOnlyFileBaseDirectory：CheckpointDir ?? ""）。
  pub fn append_only_file_base_directory(&self) -> String {
    self.checkpoint_dir.clone().unwrap_or_default()
  }

  /// libs/server/Servers/GarnetServerOptions.cs:GetAppendOnlyFileDirectoryName
  ///
  /// 数据库 AOF 目录名：`AOF` 或 `AOF_{dbId}`。
  pub fn get_append_only_file_directory_name(db_id: i32) -> String {
    if db_id == 0 {
      "AOF".to_string()
    } else {
      format!("AOF_{db_id}")
    }
  }

  /// libs/server/Servers/GarnetServerOptions.cs:GetAppendOnlyFileDirectory
  pub fn get_append_only_file_directory(&self, db_id: i32) -> PathBuf {
    Path::new(&self.append_only_file_base_directory())
      .join(Self::get_append_only_file_directory_name(db_id))
  }

  /// libs/server/Servers/GarnetServerOptions.cs:GetSettings
  ///
  /// 装配存储设置（对齐 C# GetSettings 内的 KVSettings 装配：
  /// MutablePercent 区间与索引尺寸校验在前，页尺寸经
  /// [`Self::page_size_bits`] 强制最小页约束；Tsavorite 实例与设备工厂
  /// 为框架面，由存储域承接）。
  pub fn get_settings(&self) -> Result<StoreSettings, OptionsError> {
    if !MUTABLE_PERCENT_RANGE.contains(&self.mutable_percent) {
      return Err(OptionsError::MutablePercentOutOfRange(self.mutable_percent));
    }
    // C# IndexSize = IndexSizeCachelines("hash index size", IndexMemorySize) * 64
    let index_size = self.index_size_cachelines()? * 64;

    let page_size_bits = self.page_size_bits()?;
    let page_size = 1i64 << page_size_bits;
    let segment_size = try_parse_size(&self.segment_size)
      .ok_or_else(|| OptionsError::BadSize(self.segment_size.clone()))?;
    let memory_size = try_parse_size(&self.log_memory_size)
      .ok_or_else(|| OptionsError::BadSize(self.log_memory_size.clone()))?;

    Ok(StoreSettings {
      index_size,
      memory_size_bits: Self::memory_size_bits(memory_size),
      page_size_bits,
      segment_size_bits: log2_exact(previous_power_of_2(segment_size)),
      max_inline_key_size: self.max_inline_key_size_bytes()?,
      max_inline_value_size: self.max_inline_value_size_bytes(page_size)?,
      initial_io_record_size: self.get_initial_io_record_size_bytes(page_size)?,
      log_directory: self.log_dir.clone(),
    })
  }

  /// libs/server/Servers/GarnetServerOptions.cs:MemorySizeBits
  ///
  /// 内存尺寸 → 位数（上取 2 的幂后取 log2）。
  pub fn memory_size_bits(memory_size: i64) -> i32 {
    let adjusted = next_power_of_2(memory_size);
    log2_exact(adjusted)
  }

  /// 主日志页尺寸位数（下取 2 的幂并强制最小页大小）。
  pub fn page_size_bits(&self) -> Result<i32, OptionsError> {
    self.validated_page_size_bits(&self.page_size)
  }

  /// libs/server/Servers/GarnetServerOptions.cs:ReadCachePageSizeBits
  ///
  /// 读缓存页大小位数（强制最小页大小）。
  pub fn read_cache_page_size_bits(&self) -> Result<i32, OptionsError> {
    self.validated_page_size_bits(&self.read_cache_page_size)
  }

  /// 页尺寸串 → 位数（下取 2 的幂并校验最小页大小）。
  fn validated_page_size_bits(&self, value: &str) -> Result<i32, OptionsError> {
    let size = try_parse_size(value).ok_or_else(|| OptionsError::BadSize(value.to_string()))?;
    let adjusted = previous_power_of_2(size);
    if adjusted < MIN_PAGE_SIZE_BYTES {
      return Err(OptionsError::PageSizeTooSmall(
        value.to_string(),
        adjusted,
        MIN_PAGE_SIZE_BYTES,
      ));
    }
    Ok(log2_exact(adjusted))
  }

  /// pub/sub 日志页大小（字节，下取 2 的幂）。
  pub fn pub_sub_page_size_bytes(&self) -> i64 {
    previous_power_of_2(parse_size(&self.pub_sub_page_size).0)
  }

  /// 主日志段尺寸位数（下取 2 的幂；C# isObj 分支的对象日志段尺寸为
  /// 对象存储域字段，此处仅承载主日志）。
  pub fn segment_size_bits(&self) -> Result<i32, OptionsError> {
    let size = try_parse_size(&self.segment_size)
      .ok_or_else(|| OptionsError::BadSize(self.segment_size.clone()))?;
    Ok(log2_exact(previous_power_of_2(size)))
  }

  /// 哈希索引缓存行数（下取 2 的幂后须落在 [64, 2^37]，每行 64 字节）。
  pub fn index_size_cachelines(&self) -> Result<i64, OptionsError> {
    const MIN_ADJUSTED: i64 = 64;
    const MAX_ADJUSTED: i64 = 1 << 37;

    let size = parse_size(&self.index_memory_size).0;
    let adjusted = previous_power_of_2(size);
    if !(MIN_ADJUSTED..=MAX_ADJUSTED).contains(&adjusted) {
      return Err(OptionsError::InvalidIndexSize(
        self.index_memory_size.clone(),
      ));
    }
    Ok(adjusted / 64)
  }

  /// libs/server/Servers/GarnetServerOptions.cs:MaxInlineKeySizeBytes
  ///
  /// 内联键上限（默认 1022；范围 [0, 1022]）。
  pub fn max_inline_key_size_bytes(&self) -> Result<i32, OptionsError> {
    const MIN_BYTES: i64 = 0;
    const MAX_BYTES: i64 = 1022;

    let Some(raw) = self
      .max_inline_key_size
      .as_deref()
      .filter(|s| !s.is_empty())
    else {
      return Ok(i32::try_from(DEFAULT_MAX_INLINE_KEY_SIZE).unwrap_or(i32::MAX));
    };
    let size = try_parse_size(raw).ok_or_else(|| OptionsError::BadSize(raw.to_string()))?;
    if !(MIN_BYTES..=MAX_BYTES).contains(&size) {
      return Err(OptionsError::OutOfRange(raw.to_string(), size));
    }
    Ok(size as i32)
  }

  /// libs/server/Servers/GarnetServerOptions.cs:MaxInlineValueSizeBytes
  ///
  /// 内联值上限：未设置时默认 min(页/2, 1m)；否则不得超过页/2 与绝对上限。
  pub fn max_inline_value_size_bytes(&self, page_size: i64) -> Result<i32, OptionsError> {
    const MIN_BYTES: i64 = 0;

    let Some(raw) = self
      .max_inline_value_size
      .as_deref()
      .filter(|s| !s.is_empty())
    else {
      let default = (page_size / 2).min(DEFAULT_MAX_INLINE_VALUE_SIZE);
      return Ok(default as i32);
    };
    let size = try_parse_size(raw).ok_or_else(|| OptionsError::BadSize(raw.to_string()))?;
    if !(MIN_BYTES..=MAX_INLINE_VALUE_LIMIT).contains(&size) {
      return Err(OptionsError::OutOfRange(raw.to_string(), size));
    }
    if size > page_size / 2 {
      return Err(OptionsError::ExceedsHalfPage(
        raw.to_string(),
        size,
        page_size / 2,
      ));
    }
    Ok(size as i32)
  }

  /// libs/server/Servers/GarnetServerOptions.cs:GetInitialIORecordSizeBytes
  ///
  /// 初始 IO 记录大小；未设置返回哨兵 0（使用默认）；须为正且不超过页大小。
  pub fn get_initial_io_record_size_bytes(&self, page_size: i64) -> Result<i32, OptionsError> {
    let Some(raw) = self
      .initial_io_record_size
      .as_deref()
      .filter(|s| !s.is_empty())
    else {
      return Ok(i32::try_from(USE_DEFAULT_INITIAL_IO_RECORD_SIZE).unwrap_or(0));
    };
    let size = try_parse_size(raw).ok_or_else(|| OptionsError::BadSize(raw.to_string()))?;
    if size <= 0 {
      return Err(OptionsError::NotPositive(raw.to_string(), size));
    }
    if size > page_size {
      return Err(OptionsError::ExceedsPageSize(
        raw.to_string(),
        size,
        page_size,
      ));
    }
    Ok(size as i32)
  }

  /// libs/server/Servers/GarnetServerOptions.cs:GetAofSettings
  ///
  /// 装配 AOF 设置：每物理子日志一份（目录/文件名/内存页段位数）。
  pub fn get_aof_settings(&self, db_id: i32) -> Result<Vec<AofLogSettings>, OptionsError> {
    let memory_size_bits = self.aof_memory_size_bits()?;
    let page_size_bits = self.aof_page_size_bits()?;
    let segment_size_bits = self.aof_segment_size_bits()?;
    let dir_name = Self::get_append_only_file_directory_name(db_id);

    let sublogs = self.aof_physical_sublog_count.max(1);
    Ok(
      (0..sublogs)
        .map(|i| {
          let log_file = if sublogs == 1 {
            "aof.log".to_string()
          } else {
            format!("aof.{i}.log")
          };
          AofLogSettings {
            directory: dir_name.clone(),
            log_file,
            memory_size_bits,
            page_size_bits,
            segment_size_bits,
          }
        })
        .collect(),
    )
  }

  /// libs/server/Servers/GarnetServerOptions.cs:GetInitializedDeviceFactory
  ///
  /// 初始化设备工厂（Tsavorite 设备为框架面；此处返回规范化的设备根路径，
  /// 由 wdev 设备域以该路径创建真实工厂）。
  pub fn get_initialized_device_factory(&self, base_name: &str) -> PathBuf {
    PathBuf::from(base_name)
  }

  /// libs/server/Servers/GarnetServerOptions.cs:AofMemorySizeBits
  ///
  /// AOF 内存位数（下取 2 的幂）。
  pub fn aof_memory_size_bits(&self) -> Result<i32, OptionsError> {
    let size = try_parse_size(&self.aof_memory_size)
      .ok_or_else(|| OptionsError::BadSize(self.aof_memory_size.clone()))?;
    Ok(log2_exact(previous_power_of_2(size)))
  }

  /// libs/server/Servers/GarnetServerOptions.cs:GetBufferPoolMemoryBudgetBytes
  pub fn get_buffer_pool_memory_budget_bytes(&self) -> i64 {
    parse_size(&self.buffer_pool_memory_budget).0
  }

  /// libs/server/Servers/GarnetServerOptions.cs:AofPageSizeBits
  pub fn aof_page_size_bits(&self) -> Result<i32, OptionsError> {
    let size = try_parse_size(&self.aof_page_size)
      .ok_or_else(|| OptionsError::BadSize(self.aof_page_size.clone()))?;
    Ok(log2_exact(previous_power_of_2(size)))
  }

  /// libs/server/Servers/GarnetServerOptions.cs:AofSegmentSizeBits
  pub fn aof_segment_size_bits(&self) -> Result<i32, OptionsError> {
    let size = try_parse_size(&self.aof_segment_size)
      .ok_or_else(|| OptionsError::BadSize(self.aof_segment_size.clone()))?;
    Ok(log2_exact(previous_power_of_2(size)))
  }

  /// libs/server/Servers/GarnetServerOptions.cs:AofSizeLimitSizeBits
  ///
  /// 尺寸未设置（空串）时返回 0，表示不限制。
  pub fn aof_size_limit_size_bits(&self) -> Result<i32, OptionsError> {
    if self.aof_size_limit.is_empty() {
      return Ok(0);
    }
    let size = try_parse_size(&self.aof_size_limit)
      .ok_or_else(|| OptionsError::BadSize(self.aof_size_limit.clone()))?;
    Ok(log2_exact(previous_power_of_2(size)))
  }

  /// libs/server/Servers/GarnetServerOptions.cs:ReplicaDisklessSyncFullSyncAofThresholdValue
  ///
  /// 阈值未设置时回落 AOF 内存尺寸。
  pub fn replica_diskless_sync_full_sync_aof_threshold_value(&self) -> i64 {
    let raw = self
      .replica_diskless_sync_full_sync_aof_threshold
      .as_deref()
      .filter(|s| !s.is_empty())
      .unwrap_or(&self.aof_memory_size);
    parse_size(raw).0
  }

  /// libs/server/Servers/GarnetServerOptions.cs:GetAofDevice
  ///
  /// AOF 设备定位：null 设备校验 + AOF 日志文件路径装配
  /// （设备实例由 wdev 域以该路径创建）。
  pub fn get_aof_device(
    &self,
    db_id: i32,
    sub_log_idx: Option<i32>,
  ) -> Result<PathBuf, OptionsError> {
    if self.use_aof_null_device && self.enable_cluster && !self.fast_aof_truncate {
      return Err(OptionsError::NullDeviceWithCluster);
    }
    if self.use_aof_null_device {
      // NullDevice：以空路径表达
      return Ok(PathBuf::new());
    }

    let dir = self.get_append_only_file_directory(db_id);
    match sub_log_idx {
      None | Some(-1) => Ok(dir.join("aof.log")),
      Some(idx) => Ok(dir.join(format!("aof.{idx}.log"))),
    }
  }

  /// AOF 自动提交（CommitFrequencyMs == 0，对齐 AofAutoCommit）。
  pub fn aof_auto_commit(&self, commit_frequency_ms: i32) -> bool {
    commit_frequency_ms == 0
  }

  /// 多日志判定（对齐 MultiLogEnabled）。
  pub fn multi_log_enabled(&self, aof_replay_task_count: i32) -> bool {
    self.aof_physical_sublog_count > 1 || aof_replay_task_count > 1
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn size_parsing_matches_csharp() {
    // 纯数字
    assert_eq!(parse_size("128"), (128, 3));
    assert_eq!(parse_size("0"), (0, 1));
    // 后缀（大小写不敏感，容忍尾随 b）
    assert_eq!(parse_size("4k"), (4 * 1024, 2));
    assert_eq!(parse_size("4Kb"), (4 * 1024, 3));
    assert_eq!(parse_size("32m"), (32 * 1024 * 1024, 3));
    assert_eq!(parse_size("1g"), (1024 * 1024 * 1024, 2));
    assert_eq!(parse_size("2T"), (2i64 * 1024 * 1024 * 1024 * 1024, 2));
    assert_eq!(parse_size("1p"), (1024i64.pow(5), 2));
    // 未知字符跳过续扫（C# 无 break 语义）：前导垃圾不计入消费数
    assert_eq!(parse_size("x16"), (16, 2));
    assert_eq!(parse_size(" 16"), (16, 2));
    // TryParseSize 需全量消费：垃圾字符必致失败
    assert_eq!(try_parse_size("x16"), None);
    assert_eq!(try_parse_size("16x"), None);
    assert_eq!(try_parse_size("16"), Some(16));
    // C# 语义：空串合法，尺寸 0
    assert_eq!(try_parse_size(""), Some(0));
    // 组合后缀
    assert_eq!(parse_size("16m"), (16 * 1024 * 1024, 3));
  }

  #[test]
  fn byte_slice_parsing_matches_str() {
    assert_eq!(parse_size_bytes(b"32m"), (32 * 1024 * 1024, 3));
    assert_eq!(try_parse_size_bytes(b"4kb"), Some(4 * 1024));
    assert_eq!(try_parse_size_bytes(b"4kbx"), None);
  }

  #[test]
  fn pretty_size_matches_csharp() {
    // 位数 > 3 时向小单位归一
    assert_eq!(pretty_size(16 * 1024 * 1024 * 1024), "16g");
    assert_eq!(pretty_size(32 * 1024 * 1024), "32m");
    assert_eq!(pretty_size(4 * 1024), "4k");
    assert_eq!(pretty_size(512), "512");
    // 归一后产生小数：1000 → 0.9765625k（C# 同式）
    assert_eq!(pretty_size(1000), "0.9765625k");
  }

  #[test]
  fn index_cachelines_and_pub_sub_page() {
    let opts = GarnetServerOptions::default();
    // 128m → 下取 2 的幂 128m → cachelines = 128m / 64
    let cachelines = opts.index_size_cachelines().unwrap();
    assert_eq!(cachelines * 64, 128 * 1024 * 1024);
    // 越界：低于 64
    let mut small = opts.clone();
    small.index_memory_size = "32".to_string();
    assert!(matches!(
      small.index_size_cachelines(),
      Err(OptionsError::InvalidIndexSize(_))
    ));
    // pub/sub 页：4k 下取 2 的幂
    assert_eq!(opts.pub_sub_page_size_bytes(), 4 * 1024);
    let mut odd = opts.clone();
    odd.pub_sub_page_size = "3000".to_string();
    assert_eq!(odd.pub_sub_page_size_bytes(), 2048);
  }

  #[test]
  fn power_of_2_helpers() {
    assert_eq!(previous_power_of_2(1024), 1024);
    assert_eq!(previous_power_of_2(1000), 512);
    assert_eq!(previous_power_of_2(1), 1);
    assert_eq!(previous_power_of_2(3), 2);
    assert_eq!(next_power_of_2(1000), 1024);
    assert_eq!(next_power_of_2(1024), 1024);
    assert_eq!(next_power_of_2(1), 1);
  }

  #[test]
  fn directory_names_and_paths() {
    let opts = GarnetServerOptions {
      checkpoint_dir: Some("/data".to_string()),
      ..GarnetServerOptions::default()
    };

    // 目录名
    assert_eq!(
      GarnetServerOptions::get_checkpoint_directory_name(0),
      "checkpoints"
    );
    assert_eq!(
      GarnetServerOptions::get_checkpoint_directory_name(3),
      "checkpoints_3"
    );
    assert_eq!(
      GarnetServerOptions::get_append_only_file_directory_name(0),
      "AOF"
    );
    assert_eq!(
      GarnetServerOptions::get_append_only_file_directory_name(2),
      "AOF_2"
    );

    // 路径组合
    let cp = opts.get_store_checkpoint_directory(1);
    assert_eq!(cp, PathBuf::from("/data/Store/checkpoints_1"));
    let aof = opts.get_append_only_file_directory(0);
    assert_eq!(aof, PathBuf::from("/data/AOF"));

    // CheckpointDir 缺省回落空串
    let bare = GarnetServerOptions::default();
    assert_eq!(
      bare.get_store_checkpoint_directory(0),
      PathBuf::from("Store/checkpoints")
    );

    // 设备工厂
    assert_eq!(
      opts.get_initialized_device_factory("/data/Store"),
      PathBuf::from("/data/Store")
    );
  }

  #[test]
  fn size_bits_semantics() {
    // MemorySizeBits 上取 2 的幂
    assert_eq!(
      GarnetServerOptions::memory_size_bits(16 * 1024 * 1024 * 1024),
      34
    );
    assert_eq!(GarnetServerOptions::memory_size_bits(1000), 10);

    let opts = GarnetServerOptions::default();
    // AOF 系下取 2 的幂：默认 128m → 27 位，32m → 25，1g → 30
    assert_eq!(opts.aof_memory_size_bits().unwrap(), 27);
    assert_eq!(opts.aof_page_size_bits().unwrap(), 25);
    assert_eq!(opts.aof_segment_size_bits().unwrap(), 30);
    // 大小上限未设置 → 0
    assert_eq!(opts.aof_size_limit_size_bits().unwrap(), 0);
    // 缓冲池预算
    let mut budget = opts.clone();
    budget.buffer_pool_memory_budget = "1g".to_string();
    assert_eq!(
      budget.get_buffer_pool_memory_budget_bytes(),
      1024 * 1024 * 1024
    );

    // 阈值回落 AOF 内存
    assert_eq!(
      opts.replica_diskless_sync_full_sync_aof_threshold_value(),
      128 * 1024 * 1024
    );
    let mut with_threshold = opts.clone();
    with_threshold.replica_diskless_sync_full_sync_aof_threshold = Some("2g".to_string());
    assert_eq!(
      with_threshold.replica_diskless_sync_full_sync_aof_threshold_value(),
      2 * 1024 * 1024 * 1024
    );
  }

  #[test]
  fn inline_key_value_validation() {
    let opts = GarnetServerOptions::default();

    // 键：默认 1022
    assert_eq!(opts.max_inline_key_size_bytes().unwrap(), 1022);
    let mut over = opts.clone();
    over.max_inline_key_size = Some("1023".to_string());
    assert!(over.max_inline_key_size_bytes().is_err());
    let mut ok = opts.clone();
    ok.max_inline_key_size = Some("128".to_string());
    assert_eq!(ok.max_inline_key_size_bytes().unwrap(), 128);
    // 非法串
    let mut bad = opts.clone();
    bad.max_inline_key_size = Some("abc".to_string());
    assert_eq!(
      bad.max_inline_key_size_bytes().unwrap_err(),
      OptionsError::BadSize("abc".to_string())
    );

    // 值：默认 min(页/2, 1m)
    let page_size: i64 = 32 * 1024 * 1024;
    assert_eq!(
      opts.max_inline_value_size_bytes(page_size).unwrap(),
      i32::try_from(DEFAULT_MAX_INLINE_VALUE_SIZE).unwrap()
    );
    // 小页时取页/2
    assert_eq!(opts.max_inline_value_size_bytes(1024).unwrap(), 512);
    // 超过页/2 报错
    let mut big = opts.clone();
    big.max_inline_value_size = Some("1m".to_string());
    let err = big.max_inline_value_size_bytes(1024).unwrap_err();
    assert!(matches!(
      err,
      OptionsError::ExceedsHalfPage(_, 1_048_576, 512)
    ));
    // 绝对上限
    let mut huge = opts.clone();
    huge.max_inline_value_size = Some("0xfffffe".to_string());
    // 0xfffffe 含非数字字符 → 无法解析（十六进制不在语法内）
    assert_eq!(
      huge.max_inline_value_size_bytes(page_size).unwrap_err(),
      OptionsError::BadSize("0xfffffe".to_string())
    );
  }

  #[test]
  fn initial_io_record_and_page_validation() {
    let opts = GarnetServerOptions::default();
    // 未设置 → 哨兵 0
    assert_eq!(opts.get_initial_io_record_size_bytes(512).unwrap(), 0);

    let mut set = opts.clone();
    set.initial_io_record_size = Some("512".to_string());
    assert_eq!(set.get_initial_io_record_size_bytes(1024).unwrap(), 512);
    // 负值/零
    let mut zero = opts.clone();
    zero.initial_io_record_size = Some("0".to_string());
    assert!(matches!(
      zero.get_initial_io_record_size_bytes(1024).unwrap_err(),
      OptionsError::NotPositive(_, 0)
    ));
    // 超页
    let mut big = opts.clone();
    big.initial_io_record_size = Some("2k".to_string());
    assert!(matches!(
      big.get_initial_io_record_size_bytes(1024).unwrap_err(),
      OptionsError::ExceedsPageSize(_, 2048, 1024)
    ));

    // 页大小下限
    let mut small = opts.clone();
    small.read_cache_page_size = "128".to_string();
    assert!(matches!(
      small.read_cache_page_size_bits().unwrap_err(),
      OptionsError::PageSizeTooSmall(_, 128, 512)
    ));
    let mut fine = opts.clone();
    fine.read_cache_page_size = "4k".to_string();
    assert_eq!(fine.read_cache_page_size_bits().unwrap(), 12);
  }

  #[test]
  fn aof_settings_and_device() {
    let opts = GarnetServerOptions::default();

    // 单子日志
    let settings = opts.get_aof_settings(0).unwrap();
    assert_eq!(settings.len(), 1);
    assert_eq!(settings[0].directory, "AOF");
    assert_eq!(settings[0].log_file, "aof.log");
    assert_eq!(settings[0].memory_size_bits, 27);

    // dbId 体现于目录名
    let settings = opts.get_aof_settings(2).unwrap();
    assert_eq!(settings[0].directory, "AOF_2");

    // 多子日志
    let mut multi = opts.clone();
    multi.aof_physical_sublog_count = 2;
    let settings = multi.get_aof_settings(0).unwrap();
    assert_eq!(settings.len(), 2);
    assert_eq!(settings[1].log_file, "aof.1.log");
    assert!(multi.multi_log_enabled(1));

    // 设备：正常路径 / 子日志路径 / null 设备冲突
    assert_eq!(
      opts.get_aof_device(0, None).unwrap(),
      PathBuf::from("AOF/aof.log")
    );
    assert_eq!(
      opts.get_aof_device(1, Some(2)).unwrap(),
      PathBuf::from("AOF_1/aof.2.log")
    );
    let mut null_dev = opts.clone();
    null_dev.use_aof_null_device = true;
    null_dev.enable_cluster = true;
    assert_eq!(
      null_dev.get_aof_device(0, None).unwrap_err(),
      OptionsError::NullDeviceWithCluster
    );
    null_dev.fast_aof_truncate = true;
    assert_eq!(null_dev.get_aof_device(0, None).unwrap(), PathBuf::new());

    // AOF 自动提交
    assert!(opts.aof_auto_commit(0));
    assert!(!opts.aof_auto_commit(100));
  }

  #[test]
  fn get_settings_assembles_defaults() {
    let opts = GarnetServerOptions::default();
    let settings = opts.get_settings().unwrap();
    // 16g → 34 位；32m 页 → 25 位；1g 段 → 30 位；索引 128m
    assert_eq!(settings.index_size, 128 * 1024 * 1024);
    assert_eq!(settings.memory_size_bits, 34);
    assert_eq!(settings.page_size_bits, 25);
    assert_eq!(settings.segment_size_bits, 30);
    // 内联值默认 min(页/2, 1m) = 1m
    assert_eq!(
      settings.max_inline_value_size,
      i32::try_from(DEFAULT_MAX_INLINE_VALUE_SIZE).unwrap()
    );
    assert_eq!(settings.max_inline_key_size, 1022);
    assert_eq!(settings.initial_io_record_size, 0);
    assert_eq!(settings.log_directory, None);
  }

  #[test]
  fn get_settings_rejects_bad_mutable_percent_and_small_page() {
    // C# GetSettings 入口：MutablePercent 须在 [10, 95]
    let opts = GarnetServerOptions {
      mutable_percent: 5,
      ..GarnetServerOptions::default()
    };
    assert!(matches!(
      opts.get_settings(),
      Err(OptionsError::MutablePercentOutOfRange(5))
    ));
    // 页大小低于 MinPageSizeBytes(512) 被 PageSizeBits 校验拒绝
    let small_page = GarnetServerOptions {
      page_size: "256".to_string(),
      ..GarnetServerOptions::default()
    };
    assert!(matches!(
      small_page.get_settings(),
      Err(OptionsError::PageSizeTooSmall(_, 256, 512))
    ));
  }
}
