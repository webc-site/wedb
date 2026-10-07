//! NodeArgs 派生视图与投影：网络端点、运行时选项投影、尺寸字节折算
//!
//! 对标 C# GarnetServerOptions.cs 装配段与 Options.cs:GetServerOptions 的
//! 逐字段投影（端点组装对标 libs/common/Format.cs:TryParseAddressList）。

use std::net::Ipv6Addr;

use log::LevelFilter;
use wbase::endpoint::uds_path;

use crate::{
  node_options::{
    DEFAULT_BIND, DEFAULT_BIND_ANY, INDEX_MAX_SIZE_MAX_BYTES, INDEX_MAX_SIZE_MIN_BYTES,
    INFINITE_SYNC_TIMEOUT_SECS, NodeArgs, NodeOptionsError, try_parse_log_level,
  },
  runtime_server_options::RuntimeServerOptions,
  size::{previous_power_of_2, try_parse_size},
};

impl NodeArgs {
  /// 解析最低日志级别（serverSettings.LogLevel；缺省 Warning 对标
  /// defaults.conf:280）
  ///
  /// libs/host/Configuration/TypeConverters.cs:RedisLogLevelTypeConverter 解析投影
  pub fn minimum_log_level(&self) -> LevelFilter {
    match self.log_level.as_deref() {
      Some(s) => try_parse_log_level(s).unwrap_or(LevelFilter::Warn),
      // 未配置缺省 Warning（defaults.conf:280 LogLevel；C# GarnetServerOptions.cs:302
      // 库级字段 Error 亦非 Information，取 conf 生效侧）
      None => LevelFilter::Warn,
    }
  }

  /// 生成网络端点定义列表（bind 多地址拆分唯一落点）
  ///
  /// 对标 libs/common/Format.cs:TryParseAddressList：bind 未指定或全空白时按
  /// protected-mode 回退（保护→回环 / 非保护→全接口；C# defaultBindLoopBack 与
  /// defaultBindAny 产出双端点）；否则按逗号与空格切分多地址（TrimEntries |
  /// RemoveEmptyEntries 语义，Format.cs:64），逐地址与 port 组合成端点。
  /// 若 bind 条目命中 UDS 路径形态（uds_path），显式报错拒启（对标 C#
  /// OptionsValidators.cs:373 拒绝非 TCP 地址），UDS 仅允许经 unixsocket 独立选项配置。
  /// unixsocket 尾部追加（对标 Options.cs:813-814）。
  /// 全空白条目被剔除后可能产出空列表，对应 C# Options.cs:796
  /// `endpoints.Length == 0` 的拒启臂，由消费侧 GarnetServer::new 校验。
  pub fn endpoints(&self) -> Result<Vec<String>, NodeOptionsError> {
    let raw = self.bind.as_deref().unwrap_or_default().trim();
    let bind = if raw.is_empty() {
      if self.protected_mode {
        DEFAULT_BIND
      } else {
        DEFAULT_BIND_ANY
      }
    } else {
      raw
    };
    let mut eps = Vec::new();
    for a in bind
      .split([',', ' '])
      .map(str::trim)
      .filter(|a| !a.is_empty())
    {
      if uds_path(a).is_some() {
        return Err(NodeOptionsError::InvalidAddress(a.to_string()));
      }
      eps.push(format_bind_endpoint(a, self.port));
    }
    if let Some(ref u) = self.unixsocket {
      eps.push(format!("unix:{u}"));
    }
    Ok(eps)
  }

  /// UDS 套接字文件权限模式位（八进制数字字面量折算真实位值：600 → 0o600，
  /// 对标 Options.cs:816-817 Convert.ToInt32(_, 8) 转 UnixFileMode；None 或
  /// 值 0 返回 None = 不设置，对标 GarnetServerTcp.cs:149
  /// `unixSocketPermission != default` 跳过臂；数字位有效性由 validate 单点
  /// 拒启保证，此处纯算术折算无二次校验）
  ///
  /// libs/host/Configuration/Options.cs:816-817
  #[must_use]
  pub fn unix_socket_mode(&self) -> Option<u32> {
    let perm = self.unixsocket_perm.filter(|p| *p != 0)?;
    // 逐位权展开即八进制折算（各位 ≤7 已由 validate 保证）
    Some((perm / 100) as u32 * 64 + (perm / 10 % 10) as u32 * 8 + (perm % 10) as u32)
  }

  /// 投影运行时服务选项（对标 C# Options.GetServerOptions 的服务选项装配段；
  /// RuntimeServerOptions 为 RuntimeServerConfig 播种的运行时单一真源）
  ///
  /// libs/host/Configuration/Options.cs:GetServerOptions
  pub fn runtime_server_options(&self) -> RuntimeServerOptions {
    let mut opts = RuntimeServerOptions::default();
    if let Some(ms) = self.aof_commit_ms {
      opts.commit_frequency_ms = ms;
    }
    opts.slow_log_threshold = self.slow_log_threshold;
    opts.slow_log_max_entries = self.slow_log_max_entries;
    opts.max_databases = self.max_databases;
    opts.object_scan_count_limit = self.object_scan_count_limit;
    opts.expired_object_collection_frequency_secs = self.expired_object_collection_frequency_secs;
    // C# Options.cs:1033 → GarnetServerOptions.ExpiredKeyDeletionScanFrequencySecs
    // → RuntimeServerConfig.cs:264 槽位播种 → StoreWrapper.cs:994-999
    // TryStartExpiredKeyDeletionTask 的启动期唯一写入路径
    opts.expired_key_deletion_scan_frequency_secs = self.expired_key_deletion_scan_frequency_secs;
    // C# Options.cs:934 WaitForCommit → GarnetServerOptions.WaitForCommit 投影
    opts.wait_for_commit = self.aof_commit_wait;
    // C# Options.cs:991-992 FastAofTruncate / OnDemandCheckpoint 投影（rust 不接
    // --main-memory-replication 弃用别名，GetFastAofTruncate 即直取本值）
    opts.fast_aof_truncate = self.fast_aof_truncate;
    opts.on_demand_checkpoint = self.on_demand_checkpoint;

    // <=0 折无限超时哨兵秒（对标 Options.cs:995 `<=0 ? InfiniteTimeSpan`）：
    // 直强转会落 Duration::from_secs(0)，副本一致读在等待回放推进处立即超时，
    // 语义完全反转；负值同臂归哨兵。哨兵字面值单源
    // [`INFINITE_SYNC_TIMEOUT_SECS`]，消费侧据此折 None（不挂计时器）
    opts.replica_sync_timeout_secs = if self.replica_sync_timeout_secs <= 0 {
      INFINITE_SYNC_TIMEOUT_SECS
    } else {
      self.replica_sync_timeout_secs as u64
    };
    opts.replica_attach_timeout_secs = self.replica_attach_timeout_secs;
    opts.replica_sync_delay_ms = self.replica_sync_delay_ms;
    opts.cluster_replication_reestablishment_timeout =
      self.cluster_replication_reestablishment_timeout;
    opts.aof_tail_witness_freq_ms = self.aof_tail_witness_freq_ms;
    opts.aof_sync_max_lag_bytes = self.aof_sync_max_lag_bytes;
    opts.vector_set_quantization_task_count = self.vector_set_quantization_task_count;

    // —— 五旋钮启动段承接（对标 C# Options.cs GetServerOptions 逐字段直取投影；
    // 缺省即引同一 DEFAULT_* 常量，故未显式给值时投影等于 default()，零分叉）——
    // C# Options.cs:939/:941 CompactionType / CompactionMaxSegments → 槽 16/15 播种
    // → wnode service 每轮现取回灌 GcConfig，wkv gc 按档分派
    opts.compaction_type = self.compaction_type;
    opts.compaction_max_segments = self.compaction_max_segments;
    // C# Options.cs:987 EnableScatterGatherGet → 槽 19 播种 → get.rs 会话级现取
    opts.enable_scatter_gather_get = self.enable_scatter_gather_get;
    // C# Options.cs:989/:994 AofReplayMaxLagBytes / ReplicaDisklessSyncDelay →
    // 槽 9/12 播种 + boot.rs 直读注入 ClusterProvider 推流门限与攒批开窗等待
    opts.aof_replay_max_lag_bytes = self.aof_replay_max_lag_bytes;
    opts.replica_diskless_sync_delay = self.replica_diskless_sync_delay;

    // —— 只读回显字段（CONFIG GET/INFO 经格式器直读，C# Options.cs:909-910、:921、
    // :935、:1030 逐字段投影进 GarnetServerOptions 的同源装配） ——
    // rust `checkpoint_base_dir()` ↔ C# CheckpointBaseDirectory（GarnetServerOptions.cs:625
    // CheckpointDir ?? LogDir 回落根；显式 --checkpoint-dir 优先，缺省回落单根 dir）
    opts.checkpoint_base_directory = self.checkpoint_base_dir().display().to_string();
    // rust `wal_dir()` ↔ C# LogDir（Options.cs:909 物理日志设备根；缺省 <dir>/wal
    // 恒有目录，口径单点取本结构 wal_dir()，路径落串统一 display 写法）
    opts.log_dir = Some(self.wal_dir().display().to_string());
    // C# Options.cs:1030 UnixSocketPath：与绑定侧 endpoints() 同读 unixsocket，
    // 此处仅供展示回显，不另起绑定链
    opts.unix_socket_path = self.unixsocket.clone();
    // C# Options.cs:921 EnableAOF：APPENDONLY 只读格式器直读源
    opts.enable_aof = self.aof;
    // C# Options.cs:935 AofSizeLimit 原样字符串入展示面；行为侧字节折算另走
    // aof_size_limit_bytes() 单点，不在展示侧解析
    opts.aof_size_limit = self.aof_size_limit.clone();
    // C# Options.cs:924-926 AofMemorySize / AofPageSize / AofSegmentSize 原样
    // 字符串投影：Some 即覆盖、None 保留 RuntimeServerOptions::default() 的
    // "128m"/"32m"/"1g"（缺省唯一真源，杜绝第二套缺省常量）；字节折算与组合
    // 体检统一在消费侧 wnode AofSettings::from_options，装配链不再二次解析
    opts.aof_memory_size = self.aof_memory_size.clone().or(opts.aof_memory_size);
    opts.aof_page_size = self.aof_page_size.clone().or(opts.aof_page_size);
    opts.aof_segment_size = self.aof_segment_size.clone().or(opts.aof_segment_size);
    // AofSizeLimitEnforceFrequencySecs 播种运行期槽位（C# 检查周期任务每轮
    // runtimeConfig.GetInt 现取的唯一真值链）：越 i32 契约上界已由
    // NodeArgs::validate 入口拒启，此处 .min 收窄纯 u64→i32 类型适配，
    // 无静默饱和活臂
    opts.aof_size_limit_enforce_frequency_secs = self
      .aof_size_limit_enforce_frequency_secs
      .min(i32::MAX as u64) as i32;
    opts
  }

  /// AOF 体积限额字节（配置尺寸向下取 2 的幂；未配置或解析失败返回 None）
  ///
  /// libs/server/Servers/GarnetServerOptions.cs:AofSizeLimitSizeBits
  ///（`1L << bits` 等价 PreviousPowerOf2(size)）
  #[must_use]
  pub fn aof_size_limit_bytes(&self) -> Option<u64> {
    let raw = self.aof_size_limit.as_deref().filter(|s| !s.is_empty())?;
    let size = try_parse_size(raw)?;
    let adjusted = previous_power_of_2(size);
    Some(adjusted as u64)
  }

  /// 哈希索引内存上限桶数（尺寸向下取 2 的幂再按 64B/桶折算）
  ///
  /// libs/server/Servers/ServerOptions.cs:IndexSizeCachelines
  ///（adjustedSize / 64，每 cache line 64B 恰为一桶；越出 `[<64, >1<<37]`
  /// 双界即 None，与 C# throw 同口径。拒启单点在 [`NodeArgs::validate`]，
  /// 本处为投影侧兜底：直接经本口取值的调用链不吃 validate 时也不越闸）
  #[must_use]
  pub fn index_max_size_buckets(&self) -> Option<usize> {
    let raw = self.index_max_size.as_deref().filter(|s| !s.is_empty())?;
    let size = try_parse_size(raw)?;
    if size < INDEX_MAX_SIZE_MIN_BYTES {
      return None;
    }
    let adjusted = previous_power_of_2(size);
    if !(INDEX_MAX_SIZE_MIN_BYTES..=INDEX_MAX_SIZE_MAX_BYTES).contains(&adjusted) {
      return None;
    }
    Some((adjusted / INDEX_MAX_SIZE_MIN_BYTES) as usize)
  }

  /// Lua 脚本内存限额字节（尺寸串整体解析为 i64 字节量；未配置或解析失败 None）
  ///
  /// 对标 C# Options.cs:647 LuaScriptMemoryLimit 经 LuaOptions.cs:GetMemoryLimitBytes
  /// 的 `ParseSize` 折算臂。[1K, 2GB] 值域闸与 Native 忽略判定复用 wlua
  /// `LuaOptions::get_memory_limit_bytes` 单点，本层不复刻；Native 档的限额由
  /// [`NodeArgs::validate`] 的 ForbiddenWithOption 前置拒启，本处仅对合法形态
  /// 产出字节量供 attach 单点投影
  #[must_use]
  pub fn lua_memory_limit_bytes(&self) -> Option<i64> {
    let raw = self
      .lua_script_memory_limit
      .as_deref()
      .filter(|s| !s.is_empty())?;
    try_parse_size(raw)
  }
}

/// 将单条 bind 监听地址与端口组装为端点字符串
///
/// IPv6 地址若未包含方括号包裹，自动包裹方括号（对标 C# Format.cs 的 TryParseAddressList 方法），
/// 确保可被 `SocketAddr` / `ServerEndpoint` 正常解析
#[inline]
pub fn format_bind_endpoint(addr: &str, port: u16) -> String {
  let trimmed = addr.trim();
  if trimmed.starts_with('[') && trimmed.ends_with(']') {
    format!("{trimmed}:{port}")
  } else if trimmed.parse::<Ipv6Addr>().is_ok() {
    format!("[{trimmed}]:{port}")
  } else {
    format!("{trimmed}:{port}")
  }
}
