//! CONFIG GET/SET/REWRITE（对标 libs/server/ServerConfig.cs 与 RuntimeServerConfig 交互）

use std::sync::Arc;

use compio::runtime::spawn;
use wconf::{
  RuntimeServerConfig, ServerConfigType,
  size::{previous_power_of_2, try_parse_size_bytes},
};
use wdev::Device;
use wkv::{WedbStore, store::grow_index_blocking};
use wresp::{
  Result,
  cmd_strings::{
    self as cs, CERT_FILE_NAME, CERT_PASSWORD, CLUSTER_PASSWORD, CLUSTER_USERNAME,
    GENERIC_ERR_INCORRECT_SIZE_FORMAT, GENERIC_ERR_INDEX_SIZE_GROW_FAILED,
    GENERIC_ERR_INDEX_SIZE_POWER_OF_TWO, GENERIC_ERR_INDEX_SIZE_SMALLER_THAN_CURRENT,
    GENERIC_ERR_MAIN_LOG_MEMORY_SIZE_TRACKER_NOT_RUNNING,
    GENERIC_ERR_READ_CACHE_MEMORY_SIZE_TRACKER_NOT_RUNNING, INDEX, MAIN_LOG_MEMORY,
    READ_CACHE_MEMORY, abort_with_wrong_number_of_arguments, write_error_raw, write_map_len,
    write_raw,
  },
  ext::{RespSliceExt, RespVecExt},
};

use crate::{
  aof::garnet_append_only_file::GarnetAppendOnlyFile, cluster_provider::ClusterProviderHandle,
  config_owner::apply_config_reconcile, primary_tasks::PrimaryTasks,
};

/// CONFIG SET 错误消息累积器（对标 C# StringBuilder + AppendError 语义：
/// 首条原样，后续条剥离 "ERR " 前缀以 "; " 连接）
struct ErrorMsgBuilder {
  msg: String,
}

impl ErrorMsgBuilder {
  const fn new() -> Self {
    Self { msg: String::new() }
  }

  /// libs/server/ServerConfig.cs:AppendError
  fn append(&mut self, error: &str) {
    if self.msg.is_empty() {
      self.msg = error.to_string();
    } else {
      // C# error.Substring(4) 剥离 "ERR "
      let tail = error.strip_prefix("ERR ").unwrap_or(error);
      self.msg.push_str("; ");
      self.msg.push_str(tail);
    }
  }

  /// libs/server/ServerConfig.cs:AppendErrorWithTemplate
  ///
  /// 模板占位符 {0} 以 ASCII 选项名替换
  fn append_with_template(&mut self, template: &str, option: &[u8]) {
    let error = template.replace("{0}", option.as_str_safe());
    self.append(&error);
  }

  const fn is_empty(&self) -> bool {
    self.msg.is_empty()
  }
}

/// 大小写不敏感的连字键名等价比较（对标 EqualsLowerCaseSpanIgnoringCase
/// allowNonAlphabeticChars: false：字母折叠，'-' 等非字母按原样比较）
fn eq_ignore_case_key(a: &[u8], b: &[u8]) -> bool {
  a.eq_ignore_ascii_case(b)
}

/// C# Encoding.ASCII.GetString 的域内承接：非 UTF-8 字节按替换字符落地
/// （配置值仅接受 ASCII 数值/布尔/枚举字面量，替换后的解析失败路径与
/// C# 的 '?' 替换一致地以错误收场）
fn string_lossy(value: &[u8]) -> String {
  String::from_utf8_lossy(value).into_owned()
}

pub struct ServerConfig;

/// CONFIG SET 的宿主注入面（C# NetworkCONFIG_SET 经 storeWrapper 触达的
/// 命令域半边：运行时配置表、主端调停任务、AOF 门面、集群提供者；散参聚合，
/// 免 too_many_arguments）
pub struct ConfigSetHost<'a> {
  /// 运行时配置表（会话共享单例，C# storeWrapper.runtimeConfig）
  pub runtime_config: &'a RuntimeServerConfig,
  /// 主端后台任务集（C# storeWrapper 的 primaryTaskStore 侧）
  pub primary_tasks: Option<&'a Arc<PrimaryTasks>>,
  /// AOF 追加日志门面（C# storeWrapper.appendOnlyFile；aof-sync-max-lag-bytes
  /// 背压预算调停的推送位，None = 无 AOF 形态）
  pub aof: Option<&'a Arc<GarnetAppendOnlyFile>>,
  /// 集群提供者句柄（C# storeWrapper.clusterProvider，单机为 None）
  pub cluster: Option<&'a ClusterProviderHandle>,
}

impl ServerConfig {
  /// libs/server/ServerConfig.cs:GetConfig
  ///
  /// 参数名 → 配置类型；星号与会话级 slave-read-only 特判，其余一律经
  /// 运行时配置表解析（含 cluster-timeout 别名，ASCII 大小写不敏感）。
  pub fn get_config(parameter: &[u8]) -> ServerConfigType {
    // C# 原地大写后比较；此处直接大小写不敏感等价
    if parameter.eq_ignore_ascii_case(b"*") {
      return ServerConfigType::All;
    }

    // slave-read-only 是固定兼容配置项，由 CONFIG GET 处理器直接处理，不入运行时配置表
    if parameter.eq_ignore_ascii_case(b"SLAVE-READ-ONLY") {
      return ServerConfigType::SlaveReadOnly;
    }

    // 其余参数经运行时配置表解析；表未命中 → NONE
    RuntimeServerConfig::try_get_type(parameter).unwrap_or(ServerConfigType::None)
  }
}

impl ServerConfig {
  /// libs/server/ServerConfig.cs:NetworkCONFIG_GET
  pub fn network_config_get(
    &mut self,
    parse_state: &[&[u8]],
    runtime_config: &RuntimeServerConfig,
    resp_protocol_version: u8,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "CONFIG|GET");
      return Ok(true);
    }

    // 去重保序收集（C# HashSet 语义）；NONE 参数静默跳过
    let mut parameters: Vec<ServerConfigType> = Vec::new();
    let mut return_all = false;
    for parameter in parse_state {
      let server_config_type = ServerConfig::get_config(parameter);

      if return_all {
        continue;
      }
      if server_config_type == ServerConfigType::All {
        // 运行时全表 + slave-read-only
        parameters = RuntimeServerConfig::runtime_types().to_vec();
        parameters.push(ServerConfigType::SlaveReadOnly);
        return_all = true;
        continue;
      }

      if server_config_type == ServerConfigType::None || parameters.contains(&server_config_type) {
        continue;
      }

      parameters.push(server_config_type);
    }

    if parameters.is_empty() {
      // C# 空匹配回 RESP_EMPTYLIST
      write_raw(output, cs::RESP_EMPTYLIST);
      return Ok(true);
    }

    // 命中参数对按协议版本写 map 头（C# WriteMapLength：RESP3 %N、RESP2 双倍
    // 数组）；slave-read-only 固定返回 "yes"（Redis 标准兼容常量）
    write_map_len(output, parameters.len(), resp_protocol_version);
    for config_type in parameters {
      if config_type == ServerConfigType::SlaveReadOnly {
        output.write_resp_bulk_string(b"slave-read-only");
        output.write_resp_bulk_string(b"yes");
      } else {
        output.write_resp_bulk_string(RuntimeServerConfig::name(config_type).as_bytes());
        output.write_resp_bulk_string(runtime_config.resp_format(config_type).as_bytes());
      }
    }
    Ok(true)
  }

  /// libs/server/ServerConfig.cs:NetworkCONFIG_REWRITE
  pub fn network_config_rewrite(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "CONFIG|REWRITE");
      return Ok(true);
    }

    // C# 段首 `storeWrapper.clusterProvider?.FlushConfig()`（ServerConfig.cs:
    // 105）的拓扑刷盘由会话分派层经集群会话切面承接（RespServerSession::
    // network_config_rewrite），本实现仅回 OK
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }

  /// libs/server/ServerConfig.cs:NetworkCONFIG_SET
  pub fn network_config_set<D>(
    &mut self,
    parse_state: &[&[u8]],
    store: Option<&Arc<WedbStore<D>>>,
    host: &ConfigSetHost<'_>,
    output: &mut Vec<u8>,
  ) -> Result<bool>
  where
    D: Device + 'static,
  {
    if parse_state.is_empty() || !parse_state.len().is_multiple_of(2) {
      abort_with_wrong_number_of_arguments(output, "CONFIG|SET");
      return Ok(true);
    }

    let mut cert_file_name = false;
    let mut cert_password = false;
    // C# clusterUsername/clusterPassword 捕获 Encoding.ASCII.GetString 的值，
    // 键重复出项时末次覆盖（else-if 链逐对覆写同式）
    let mut cluster_username: Option<String> = None;
    let mut cluster_password: Option<String> = None;
    let mut main_log_memory_size: Option<Vec<u8>> = None;
    let mut read_cache_memory_size: Option<Vec<u8>> = None;
    let mut index: Option<Vec<u8>> = None;
    // 运行时表命中的动态设置对（C# dynamicSets：List<(type, value)>）
    let mut dynamic_sets: Vec<(ServerConfigType, Vec<u8>)> = Vec::new();

    let mut unknown_option = false;
    let mut unknown_key: &[u8] = &[];

    let mut c = 0usize;
    while c < parse_state.len() {
      let key = parse_state[c];
      let value = parse_state[c + 1];

      if eq_ignore_case_key(key, MAIN_LOG_MEMORY) {
        main_log_memory_size = Some(value.to_vec());
      } else if eq_ignore_case_key(key, READ_CACHE_MEMORY) {
        read_cache_memory_size = Some(value.to_vec());
      } else if eq_ignore_case_key(key, INDEX) {
        index = Some(value.to_vec());
      } else if eq_ignore_case_key(key, CERT_FILE_NAME) {
        cert_file_name = true;
      } else if eq_ignore_case_key(key, CERT_PASSWORD) {
        cert_password = true;
      } else if eq_ignore_case_key(key, CLUSTER_USERNAME) {
        cluster_username = Some(string_lossy(value));
      } else if eq_ignore_case_key(key, CLUSTER_PASSWORD) {
        cluster_password = Some(string_lossy(value));
      } else if let Some(config_type) = RuntimeServerConfig::try_get_type(key) {
        dynamic_sets.push((config_type, value.to_vec()));
      } else if !unknown_option {
        // 首个未知键进错误（C# unknownOption 同式）
        unknown_option = true;
        unknown_key = key;
      }
      c += 2;
    }

    let mut error_msg = ErrorMsgBuilder::new();

    if unknown_option {
      error_msg.append(
        &cs::GENERIC_ERR_UNKNOWN_OPTION_CONFIG_SET.replace("{0}", unknown_key.as_str_safe()),
      );
    } else {
      // 集群凭据（C# ServerConfig.cs:163-171）：提供者在场则 UpdateClusterAuth
      // 生效不追加错误（回 +OK），无集群才 append "ERR Cluster is disabled."；
      // 只给 password 未给 username 时 C# 记 warning 并沿用旧用户名
      if cluster_username.is_some() || cluster_password.is_some() {
        if cluster_username.is_none() {
          log::warn!(
            "Cluster username is not provided, will use new password with existing username"
          );
        }
        match host.cluster.filter(|p| p.is_cluster_enabled()) {
          Some(provider) => provider.update_cluster_auth(cluster_username, cluster_password),
          None => error_msg.append("ERR Cluster is disabled."),
        }
      }

      // TLS 证书：C# 无 TLS 选项时 append "ERR TLS is disabled."
      if cert_file_name || cert_password {
        error_msg.append("ERR TLS is disabled.");
      }

      if let Some(size) = &main_log_memory_size {
        self.handle_memory_size_change(size, false, &mut error_msg);
      }
      if let Some(size) = &read_cache_memory_size {
        self.handle_memory_size_change(size, true, &mut error_msg);
      }
      if let Some(index_size) = &index {
        self.handle_index_size_change(index_size, store, &mut error_msg);
      }

      // 动态设置：逐条校验落槽（C# TrySet 错误原样 AppendError 累积）；
      // 落槽产出的调停消息就地执行（对标 C# owner 经 StoreWrapper 触达
      // ReconcilePrimaryTask 家族——同一调用栈内完成，应答前生效）
      for (config_type, value) in dynamic_sets {
        match host
          .runtime_config
          .try_set(config_type, &string_lossy(&value))
        {
          // 无存储域会话（纯协议层 mock 形态）时调停无目标：仅落槽位，
          // 对齐 C# RuntimeServerConfig 的 null owner 构造
          Ok(Some(msg)) => {
            if let Some(store) = store {
              apply_config_reconcile(host.primary_tasks, store, host.aof, host.cluster, msg);
            }
          }
          Ok(None) => {}
          Err(error) => error_msg.append(&error.to_string()),
        }
      }
    }

    if error_msg.is_empty() {
      write_raw(output, cs::RESP_OK);
    } else {
      write_error_raw(output, &error_msg.msg);
    }
    Ok(true)
  }

  /// libs/server/ServerConfig.cs:HandleMemorySizeChange
  ///
  /// 尺寸格式校验后，C# 依次比较已配置内存、缓冲上限、size tracker 状态；rust
  /// 无服务器选项与 size tracker（对应域未建成），格式合法即落
  /// "tracker 未运行"错误——与 C# sizeTracker 为 null 的行为一致
  fn handle_memory_size_change(
    &mut self,
    memory_size: &[u8],
    is_read_cache: bool,
    error_msg: &mut ErrorMsgBuilder,
  ) {
    let Some(_new_memory_size) = try_parse_size_bytes(memory_size) else {
      error_msg.append_with_template(GENERIC_ERR_INCORRECT_SIZE_FORMAT, MAIN_LOG_MEMORY);
      return;
    };

    // C# 同尺寸短路（GenericErrMemorySizeGreaterThanBuffer 上限拒绝同理）依赖
    // 已配置的 LogMemorySize（服务器选项未接）；直落 tracker 检查
    if is_read_cache {
      error_msg.append_with_template(
        GENERIC_ERR_READ_CACHE_MEMORY_SIZE_TRACKER_NOT_RUNNING,
        READ_CACHE_MEMORY,
      );
    } else {
      error_msg.append_with_template(
        GENERIC_ERR_MAIN_LOG_MEMORY_SIZE_TRACKER_NOT_RUNNING,
        MAIN_LOG_MEMORY,
      );
    }
  }

  /// libs/server/ServerConfig.cs:HandleIndexSizeChangeAsync
  ///
  /// 尺寸格式与 2 的幂校验后三态分臂，对齐 C# ResizeAndReprocess 语义：
  /// 等值目标桶数直接成功续行走 +OK；缩小回 GenericErrIndexSizeSmallerThanCurrent
  /// 专用文案；仅扩容走后台在线逐级翻倍（C# GrowIndexAsync）。
  /// C# 扩容前的 auto-grow 运行门（AdjustedIndexMaxCacheLines>0 →
  /// GenericErrIndexSizeAutoGrow）在 rust 不可达：本仓无 index 自动增长选项，
  /// 该服务器选项未接，故不设该门亦无对位常量。
  fn handle_index_size_change<D: Device + 'static>(
    &mut self,
    index_size: &[u8],
    store: Option<&Arc<WedbStore<D>>>,
    error_msg: &mut ErrorMsgBuilder,
  ) {
    let Some(new_index_size) = try_parse_size_bytes(index_size) else {
      error_msg.append_with_template(GENERIC_ERR_INCORRECT_SIZE_FORMAT, INDEX);
      return;
    };

    // 2 的幂校验
    let adj_new_index_size = previous_power_of_2(new_index_size);
    if adj_new_index_size != new_index_size {
      error_msg.append_with_template(GENERIC_ERR_INDEX_SIZE_POWER_OF_TWO, INDEX);
      return;
    }

    let target_buckets = (adj_new_index_size / 64) as usize;
    if let Some(store) = store {
      let curr_buckets = store.active_index().size;
      // C# 等值短路：目标桶数 == 当前索引尺寸 → 直接返回（CONFIG SET 成功回 +OK）
      if target_buckets == curr_buckets {
        return;
      }
      // C# 缩小态专用文案，非扩容失败
      if target_buckets < curr_buckets {
        error_msg.append_with_template(GENERIC_ERR_INDEX_SIZE_SMALLER_THAN_CURRENT, INDEX);
        return;
      }
      // C# HandleIndexSizeChangeAsync 经 BlockingWait 阻塞连接线程等待扩容完成；
      // thread-per-core 下 reactor 严禁阻塞（扩容含纪元排空忙等与全量分块迁移
      // 自旋，大索引秒级），移交 compio 阻塞线程后台执行，逐次翻倍直至目标桶数，
      // 结果经日志留痕（C# 应答内报 ERR 的同步语义不可 1:1 保留）
      let store = Arc::clone(store);
      let target = target_buckets;
      spawn(async move {
        while store.active_index().size < target {
          match grow_index_blocking(Arc::clone(&store)).await {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => {
              log::error!("CONFIG SET index 在线扩容至 {target} 桶失败: {e}");
              break;
            }
          }
        }
      })
      .detach();
    } else {
      error_msg.append_with_template(GENERIC_ERR_INDEX_SIZE_GROW_FAILED, INDEX);
    }
  }
}

#[cfg(test)]
mod tests {
  use wconf::{LogCompactionType, RuntimeServerOptions};
  use wdev::SegmentedDevice;

  use super::*;
  use crate::cluster_provider::ClusterProvider;

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
  fn parse_size_matches_csharp_semantics() {
    // 纯数字 / 单后缀 / 双后缀 / 忽略大小写
    assert_eq!(try_parse_size_bytes(b"1024"), Some(1024));
    assert_eq!(try_parse_size_bytes(b"1k"), Some(1024));
    assert_eq!(try_parse_size_bytes(b"1kb"), Some(1024));
    assert_eq!(try_parse_size_bytes(b"1K"), Some(1024));
    assert_eq!(try_parse_size_bytes(b"2m"), Some(2 * 1024 * 1024));
    assert_eq!(try_parse_size_bytes(b"1g"), Some(1024 * 1024 * 1024));
    assert_eq!(try_parse_size_bytes(b"1t"), Some(1024i64.pow(4)));
    assert_eq!(try_parse_size_bytes(b"1p"), Some(1024i64.pow(5)));
    // 尾随垃圾 → 消费数不等 → None
    assert_eq!(try_parse_size_bytes(b"1x"), None);
    assert_eq!(try_parse_size_bytes(b"1k2"), None);
    // C# 语义保留：空串 = 合法尺寸 0
    assert_eq!(try_parse_size_bytes(b""), Some(0));
  }

  #[test]
  fn previous_power_of_2_matches() {
    assert_eq!(previous_power_of_2(64), 64);
    assert_eq!(previous_power_of_2(65), 64);
    assert_eq!(previous_power_of_2(1), 1);
    assert_eq!(previous_power_of_2(1 << 40), 1 << 40);
  }

  #[test]
  fn error_builder_joins_with_semicolon() {
    let mut b = ErrorMsgBuilder::new();
    assert!(b.is_empty());
    b.append("ERR Cluster is disabled.");
    b.append_with_template(GENERIC_ERR_INCORRECT_SIZE_FORMAT, MAIN_LOG_MEMORY);
    assert_eq!(
      b.msg,
      "ERR Cluster is disabled.; Incorrect size format in (option: 'memory')"
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

  #[test]
  fn config_set_validation_and_disabled_subsystems() {
    let mut s = ServerConfig;
    let rc = config();
    let host = ConfigSetHost {
      runtime_config: &rc,
      primary_tasks: None,
      cluster: None,
      aof: None,
    };

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

    // 多错误以 "; " 连接且后续条剥离 "ERR "
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

    // readcache-memory：错误模板用 readcache 选项名
    let mut out = Vec::new();
    s.network_config_set::<SegmentedDevice>(&[b"READCACHE-MEMORY", b"1g"], None, &host, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Cannot adjust readcache memory size configuration when size tracker is not running (option: 'readcache-memory')\r\n"
    );

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
      runtime_config: &rc,
      primary_tasks: None,
      cluster: Some(&handle),
      aof: None,
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

  #[test]
  fn config_set_dynamic_runtime_values() {
    let mut s = ServerConfig;
    let rc = config();
    let host = ConfigSetHost {
      runtime_config: &rc,
      primary_tasks: None,
      cluster: None,
      aof: None,
    };

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
    s.network_config_set::<SegmentedDevice>(
      &[b"compaction-type", b"Lookup"],
      None,
      &host,
      &mut out,
    )
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
}
