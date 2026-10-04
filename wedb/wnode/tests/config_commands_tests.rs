//! CONFIG GET/SET/REWRITE 命令层单元测试（自 src/resp/config_commands.rs 内联
//! mod tests 迁出；size 解析与 2 的幂辅助断言随主体归位并入 wconf 测试）
//!
//! 对标 libs/server/ServerConfig.cs 的 GetConfig/NetworkCONFIG_* 与
//! RuntimeServerConfig 交互面：全部经 crate 公开口
//! （ServerConfig::get_config / network_config_*、ConfigSetHost、
//! ClusterProvider 抽象面）装配，无提权后门。

use std::sync::Arc;

use wbase::cfg::LogCompactionType;
use wconf::{RuntimeServerConfig, RuntimeServerOptions, ServerConfigType};
use wdev::SegmentedDevice;
use wnode::{
  ClusterProvider, ClusterProviderHandle,
  resp::config_commands::{ConfigSetHost, ServerConfig},
};

/// 无持有方的默认运行时配置（测试夹具）
fn config() -> RuntimeServerConfig {
  RuntimeServerConfig::new(RuntimeServerOptions::default())
}

#[test]
fn get_config_parses_table_and_special_params() {
  assert_eq!(ServerConfig::get_config(b"*"), ServerConfigType::All);
  assert_eq!(ServerConfig::get_config(b"*x"), ServerConfigType::None);
  assert_eq!(
    ServerConfig::get_config(b"slave-read-only"),
    ServerConfigType::SlaveReadOnly
  );
  assert_eq!(
    ServerConfig::get_config(b"SLAVE-READ-ONLY"),
    ServerConfigType::SlaveReadOnly
  );
  // 运行时表命中（大小写不敏感 + 别名）
  assert_eq!(
    ServerConfig::get_config(b"cluster-node-timeout"),
    ServerConfigType::ClusterNodeTimeout
  );
  assert_eq!(
    ServerConfig::get_config(b"CLUSTER-TIMEOUT"),
    ServerConfigType::ClusterNodeTimeout
  );
  assert_eq!(
    ServerConfig::get_config(b"Slowlog-Log-Slower-Than"),
    ServerConfigType::SlowlogLogSlowerThan
  );
  // 表未命中 → NONE
  assert_eq!(
    ServerConfig::get_config(b"maxmemory"),
    ServerConfigType::None
  );
}

#[test]
fn config_get_runtime_params_and_star() {
  let mut s = ServerConfig;
  let rc = config();

  // 零参 → wrong args
  let mut out = Vec::new();
  s.network_config_get(&[], &rc, 2, &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'CONFIG|GET' command\r\n"
  );

  // 未知参数 → 空列表
  let mut out = Vec::new();
  s.network_config_get(&[b"maxmemory"], &rc, 2, &mut out)
    .unwrap();
  assert_eq!(out, b"*0\r\n");

  // slave-read-only → 单条 map（RESP2 双倍数组），固定回 yes
  let mut out = Vec::new();
  s.network_config_get(&[b"slave-read-only"], &rc, 2, &mut out)
    .unwrap();
  assert_eq!(out, b"*2\r\n$15\r\nslave-read-only\r\n$3\r\nyes\r\n");

  // 运行时表参数：CONFIG GET cluster-node-timeout → 默认 60
  let mut out = Vec::new();
  s.network_config_get(&[b"cluster-node-timeout"], &rc, 2, &mut out)
    .unwrap();
  assert_eq!(out, b"*2\r\n$20\r\ncluster-node-timeout\r\n$2\r\n60\r\n");

  // "*" → 全表 + 会话级；重复参数去重（C# HashSet 语义）
  let mut out = Vec::new();
  s.network_config_get(
    &[b"cluster-node-timeout", b"CLUSTER-NODE-TIMEOUT"],
    &rc,
    2,
    &mut out,
  )
  .unwrap();
  assert_eq!(out, b"*2\r\n$20\r\ncluster-node-timeout\r\n$2\r\n60\r\n");

  // SET 后 GET 反映新值
  rc.try_set(ServerConfigType::ClusterNodeTimeout, "120")
    .unwrap();
  let mut out = Vec::new();
  s.network_config_get(&[b"cluster-timeout"], &rc, 2, &mut out)
    .unwrap();
  assert_eq!(out, b"*2\r\n$20\r\ncluster-node-timeout\r\n$3\r\n120\r\n");

  // "*" 输出含只读回落参数与运行时参数
  let mut out = Vec::new();
  s.network_config_get(&[b"*"], &rc, 2, &mut out).unwrap();
  let text = String::from_utf8_lossy(&out);
  assert!(text.contains("slave-read-only"), "{text}");
  assert!(text.contains("cluster-node-timeout"), "{text}");
  assert!(text.contains("aof-commit-freq"), "{text}");
  assert!(text.contains("databases"), "{text}");
}

#[test]
fn config_get_writes_resp3_map_header() {
  // C# ServerConfig.cs:69 WriteMapLength：RESP3 客户端写 %N 头
  let mut s = ServerConfig;
  let rc = config();

  // RESP3：单参数 → %1（一对名值）+ 名值两 bulk
  let mut out = Vec::new();
  s.network_config_get(&[b"cluster-node-timeout"], &rc, 3, &mut out)
    .unwrap();
  assert_eq!(out, b"%1\r\n$20\r\ncluster-node-timeout\r\n$2\r\n60\r\n");

  // RESP3：slave-read-only 单条，固定 yes
  let mut out = Vec::new();
  s.network_config_get(&[b"slave-read-only"], &rc, 3, &mut out)
    .unwrap();
  assert_eq!(out, b"%1\r\n$15\r\nslave-read-only\r\n$3\r\nyes\r\n");

  // 零命中两协议同回空数组（C# RESP_EMPTYLIST）
  let mut out = Vec::new();
  s.network_config_get(&[b"maxmemory"], &rc, 3, &mut out)
    .unwrap();
  assert_eq!(out, b"*0\r\n");
}

/// 无宿主注入的默认 CONFIG SET 面（单机、无 AOF、auto-grow 关）
fn bare_host(rc: &RuntimeServerConfig) -> ConfigSetHost<'_> {
  ConfigSetHost {
    runtime_config: rc,
    primary_tasks: None,
    cluster: None,
    aof: None,
    index_auto_grow_active: false,
    #[cfg(feature = "tls")]
    tls_config: None,
  }
}

#[test]
fn config_set_validation_and_disabled_subsystems() {
  let mut s = ServerConfig;
  let rc = config();
  let host = bare_host(&rc);

  // 奇数参数
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"a"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'CONFIG|SET' command\r\n"
  );

  // 未知键 → 首例进错误
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"maxmemory", b"1g"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR Unknown option or number of arguments for CONFIG SET - 'maxmemory'\r\n"
  );

  // TLS 禁用
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"cert-file-name", b"a.pem"], None, &host, &mut out)
    .unwrap();
  assert_eq!(out, b"-ERR TLS is disabled.\r\n");

  // 集群禁用
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"cluster-password", b"p"], None, &host, &mut out)
    .unwrap();
  assert_eq!(out, b"-ERR Cluster is disabled.\r\n");

  // 多错误以 "; " 连接且后续条剥离 "ERR "（ErrorMsgBuilder 语义的线上级覆盖：
  // 首条原样 + 模板 {0} 替换同由本组断言承载）
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(
    &[b"cluster-username", b"u", b"cert-password", b"p"],
    None,
    &host,
    &mut out,
  )
  .unwrap();
  assert_eq!(out, b"-ERR Cluster is disabled.; TLS is disabled.\r\n");

  // 尺寸格式非法
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"memory", b"abc"], None, &host, &mut out)
    .unwrap();
  assert_eq!(out, b"-ERR Incorrect size format in (option: 'memory')\r\n");

  // tracker 未运行（含格式合法场景）
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"memory", b"1g"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR Cannot adjust main log memory size configuration when size tracker is not running (option: 'memory')\r\n"
  );

  // readcache-memory 混大小写形：C# allowNonAlphabeticChars: false 下 '-'
  // 常量位恒拒，不进读缓存臂 → unknown option（对标 ServerConfig.cs:159 短路）
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"READCACHE-MEMORY", b"1g"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR Unknown option or number of arguments for CONFIG SET - 'READCACHE-MEMORY'\r\n"
  );

  // 精确小写形命中读缓存臂：tracker 未运行错误模板用 readcache 选项名
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"readcache-memory", b"1g"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR Cannot adjust readcache memory size configuration when size tracker is not running (option: 'readcache-memory')\r\n"
  );

  // 混批：unknown option 短路整批，合法动态项 sg-get 不落槽（默认 true 保持）
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(
    &[b"READCACHE-MEMORY", b"1g", b"sg-get", b"no"],
    None,
    &host,
    &mut out,
  )
  .unwrap();
  assert_eq!(
    out,
    b"-ERR Unknown option or number of arguments for CONFIG SET - 'READCACHE-MEMORY'\r\n"
  );
  assert!(rc.get_bool(ServerConfigType::SgGet));

  // 纯字母常量 false 口径折叠不变量：memory/index 大小写形仍命中
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"MEMORY", b"abc"], None, &host, &mut out)
    .unwrap();
  assert_eq!(out, b"-ERR Incorrect size format in (option: 'memory')\r\n");
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"Index", b"1000"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR Index size must be a power of 2 (option: 'index')\r\n"
  );

  // cert-/cluster- 四臂 true 口径：大小写形仍命中
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"CERT-PASSWORD", b"p"], None, &host, &mut out)
    .unwrap();
  assert_eq!(out, b"-ERR TLS is disabled.\r\n");
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"Cluster-Username", b"u"], None, &host, &mut out)
    .unwrap();
  assert_eq!(out, b"-ERR Cluster is disabled.\r\n");

  // index 非 2 的幂
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"index", b"1000"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR Index size must be a power of 2 (option: 'index')\r\n"
  );

  // index 2 的幂 → 增长通道缺席错误
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"index", b"1024"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR failed to grow index size beyond current size (option: 'index')\r\n"
  );
}

/// 记录凭据更新的测试桩（C# ClusterProvider.authContainer 消费面）
#[derive(Default)]
struct RecordingClusterProvider {
  auth: parking_lot::RwLock<(Option<String>, Option<String>)>,
}

impl ClusterProvider for RecordingClusterProvider {
  fn is_cluster_enabled(&self) -> bool {
    true
  }

  fn update_cluster_auth(&self, username: Option<String>, password: Option<String>) {
    let old_user = self.auth.read().0.clone();
    *self.auth.write() = (username.or(old_user), password);
  }
}

/// CONFIG SET 整数值首尾空白宽收（对齐 C# RuntimeServerConfig.TrySet 的
/// NumberStyles.Integer：AllowLeadingWhite|AllowTrailingWhite）：INT32/INT64
/// 两臂解析前裁首尾 ASCII 空白，" 100"/"100 " 同帧 +OK 且 GET 回显裁后原值；
/// 纯空白 trim 后解析失败双侧同帧 ERR；千分位等 NumberStyles 全集形仍拒。
/// 防两整数臂单边回改。
#[test]
fn config_set_integer_value_whitespace_widely_accepted() {
  let mut s = ServerConfig;
  let rc = config();
  let host = bare_host(&rc);

  // INT32 臂：slowlog-log-slower-than 前导空白 → +OK，GET 回显裁后原值
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(
    &[b"slowlog-log-slower-than", b" 100"],
    None,
    &host,
    &mut out,
  )
  .unwrap();
  assert_eq!(out, b"+OK\r\n");
  let mut out = Vec::new();
  s.network_config_get(&[b"slowlog-log-slower-than"], &rc, 2, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"*2\r\n$23\r\nslowlog-log-slower-than\r\n$3\r\n100\r\n"
  );

  // INT32 臂尾空白同形；INT64 臂 aof-sync-max-lag-bytes 前导+尾空白 → +OK 回显
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(
    &[b"slowlog-log-slower-than", b"200 "],
    None,
    &host,
    &mut out,
  )
  .unwrap();
  assert_eq!(out, b"+OK\r\n");
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(
    &[b"aof-sync-max-lag-bytes", b" 2097152 "],
    None,
    &host,
    &mut out,
  )
  .unwrap();
  assert_eq!(out, b"+OK\r\n");
  let mut out = Vec::new();
  s.network_config_get(&[b"aof-sync-max-lag-bytes"], &rc, 2, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"*2\r\n$22\r\naof-sync-max-lag-bytes\r\n$7\r\n2097152\r\n"
  );

  // 纯空白：trim 后空串解析失败，双侧同帧 ERR InvalidInteger
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(
    &[b"slowlog-log-slower-than", b" "],
    None,
    &host,
    &mut out,
  )
  .unwrap();
  assert_eq!(
    out,
    b"-ERR Invalid value for 'slowlog-log-slower-than': expected an integer.\r\n"
  );

  // 千分位形 NumberStyles 全集不含，裁空白后仍拒（仅空白折叠的分界锁形）
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(
    &[b"aof-sync-max-lag-bytes", b"1,024"],
    None,
    &host,
    &mut out,
  )
  .unwrap();
  assert_eq!(
    out,
    b"-ERR Invalid value for 'aof-sync-max-lag-bytes': expected an integer.\r\n"
  );
}

/// CONFIG SET cluster-username/cluster-password：集群提供者在场经
/// update_cluster_auth 生效回 +OK（C# ServerConfig.cs:167-171），
/// 只给 password 沿用旧用户名（C# ClusterProvider.cs:106）
#[test]
fn config_set_cluster_credentials_with_provider() {
  let mut s = ServerConfig;
  let rc = config();
  let provider = Arc::new(RecordingClusterProvider::default());
  let handle: ClusterProviderHandle = provider.clone();
  let host = ConfigSetHost {
    cluster: Some(&handle),
    ..bare_host(&rc)
  };

  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"cluster-username", b"u"], None, &host, &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(*provider.auth.read(), (Some("u".to_owned()), None));

  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(
    &[b"cluster-username", b"v", b"cluster-password", b"p"],
    None,
    &host,
    &mut out,
  )
  .unwrap();
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    *provider.auth.read(),
    (Some("v".to_owned()), Some("p".to_owned()))
  );

  // password-only：username 缺席 → 复用旧值 v（C# null 用户名告警臂）
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"cluster-password", b"q"], None, &host, &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    *provider.auth.read(),
    (Some("v".to_owned()), Some("q".to_owned()))
  );
}

/// 案二：auto-grow 运行门 —— `--index-max-size` 配置使后台 IndexAutoGrowTask
/// 常驻，人工 `CONFIG SET index` 必须短路拒绝 GenericErrIndexSizeAutoGrow，
/// 杜绝与自动扩容并发同入 grow_index_blocking 双写竞态（对标 C#
/// ServerConfig.cs:280 AdjustedIndexMaxCacheLines > 0）
#[test]
fn config_set_index_rejected_when_auto_grow_active() {
  let mut s = ServerConfig;
  let rc = config();
  // 门开：index_auto_grow_active = true（等价 C# AdjustedIndexMaxCacheLines > 0）
  let host = ConfigSetHost {
    index_auto_grow_active: true,
    ..bare_host(&rc)
  };

  // 合法 2 的幂尺寸在门前，命中 auto-grow 拒绝帧（store=None 亦先于 store 判定）
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"index", b"2g"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR Cannot adjust index size when auto-grow task is running (option: 'index')\r\n"
  );

  // 门关（false）：2 的幂尺寸放行到增长通道（store=None → 增长缺席错误），
  // 证明门仅在 auto-grow 在跑时生效，不误伤常规路径
  let host_off = bare_host(&rc);
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"index", b"2g"], None, &host_off, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR failed to grow index size beyond current size (option: 'index')\r\n"
  );
}

#[test]
fn config_set_dynamic_runtime_values() {
  let mut s = ServerConfig;
  let rc = config();
  let host = bare_host(&rc);

  // 运行时参数：合法值落槽并回 OK
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(
    &[b"slowlog-log-slower-than", b"5000"],
    None,
    &host,
    &mut out,
  )
  .unwrap();
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    rc.get_microseconds(ServerConfigType::SlowlogLogSlowerThan),
    5000
  );

  // 布尔值
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"sg-get", b"no"], None, &host, &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");
  assert!(!rc.get_bool(ServerConfigType::SgGet));

  // 枚举值
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"compaction-type", b"Lookup"], None, &host, &mut out)
    .unwrap();
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    rc.get_enum(ServerConfigType::CompactionType),
    Ok(LogCompactionType::Lookup)
  );

  // 非法值：TrySet 错误进累积器，槽位保持
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"sg-get", b"maybe"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR Invalid value for 'sg-get': expected 'yes' or 'no'.\r\n"
  );

  // 只读参数拒绝
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(&[b"databases", b"8"], None, &host, &mut out)
    .unwrap();
  assert_eq!(
    out,
    b"-ERR Option 'databases' is read-only and cannot be set at runtime.\r\n"
  );

  // 多对混合：合法 + 非法各一，错误累积、合法仍生效
  let mut out = Vec::new();
  s.network_config_set::<SegmentedDevice>(
    &[
      b"object-scan-count-limit",
      b"64",
      b"cluster-node-timeout",
      b"not-a-number",
    ],
    None,
    &host,
    &mut out,
  )
  .unwrap();
  assert_eq!(
    out,
    b"-ERR Invalid value for 'cluster-node-timeout': expected an integer.\r\n"
  );
  assert_eq!(rc.get_int(ServerConfigType::ObjectScanCountLimit), 64);
}

#[test]
fn config_rewrite_ok_without_cluster() {
  let mut s = ServerConfig;

  let mut out = Vec::new();
  s.network_config_rewrite(&[], &mut out).unwrap();
  assert_eq!(out, b"+OK\r\n");

  // 带参 → wrong args
  let mut out = Vec::new();
  s.network_config_rewrite(&[b"extra"], &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'CONFIG|REWRITE' command\r\n"
  );
}
