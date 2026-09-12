//! CONFIG GET/SET/REWRITE（对标 libs/server/ServerConfig.cs 与 RuntimeServerConfig 交互）

use wresp::{
  RespSliceExt, RespVecExt, Result,
  cmd_strings::{
    self as cs, abort_with_wrong_number_of_arguments, write_error_raw, write_map_len_resp2,
    write_raw,
  },
};

use crate::{
  config::{RuntimeServerConfig, ServerConfigType},
  units::{previous_power_of_2, try_parse_size_bytes},
};

/// libs/server/Resp/CmdStrings.cs:GenericErrIncorrectSizeFormat
const ERR_INCORRECT_SIZE_FORMAT: &str = "ERR Incorrect size format in (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:GenericErrMainLogMemorySizeTrackerNotRunning
const ERR_MAIN_LOG_TRACKER_NOT_RUNNING: &str = "ERR Cannot adjust main log memory size configuration when size tracker is not running (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:GenericErrReadCacheMemorySizeTrackerNotRunning
const ERR_READ_CACHE_TRACKER_NOT_RUNNING: &str = "ERR Cannot adjust readcache memory size configuration when size tracker is not running (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:GenericErrIndexSizePowerOfTwo
const ERR_INDEX_SIZE_POWER_OF_TWO: &str = "ERR Index size must be a power of 2 (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:GenericErrIndexSizeGrowFailed
const ERR_INDEX_SIZE_GROW_FAILED: &str =
  "ERR failed to grow index size beyond current size (option: '{0}')";
/// libs/server/Resp/CmdStrings.cs:MainLogMemory
const KEY_MAIN_LOG_MEMORY: &[u8] = b"memory";
/// libs/server/Resp/CmdStrings.cs:ReadCacheMemory
const KEY_READ_CACHE_MEMORY: &[u8] = b"readcache-memory";
/// libs/server/Resp/CmdStrings.cs:Index
const KEY_INDEX: &[u8] = b"index";
/// libs/server/Resp/CmdStrings.cs:CertFileName
const KEY_CERT_FILE_NAME: &[u8] = b"cert-file-name";
/// libs/server/Resp/CmdStrings.cs:CertPassword
const KEY_CERT_PASSWORD: &[u8] = b"cert-password";
/// libs/server/Resp/CmdStrings.cs:ClusterUsername
const KEY_CLUSTER_USERNAME: &[u8] = b"cluster-username";
/// libs/server/Resp/CmdStrings.cs:ClusterPassword
const KEY_CLUSTER_PASSWORD: &[u8] = b"cluster-password";

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

    // RESP2 map 退化为双倍数组；slave-read-only 固定返回 "yes"（Redis 标准兼容常量）
    write_map_len_resp2(output, parameters.len());
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

    // C# storeWrapper.clusterProvider?.FlushConfig() 为可空调用，无集群提供者
    // 时静默跳过后仍回 OK；rust 无集群提供者即同路径
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }

  /// libs/server/ServerConfig.cs:NetworkCONFIG_SET
  pub fn network_config_set(
    &mut self,
    parse_state: &[&[u8]],
    runtime_config: &RuntimeServerConfig,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if parse_state.is_empty() || !parse_state.len().is_multiple_of(2) {
      abort_with_wrong_number_of_arguments(output, "CONFIG|SET");
      return Ok(true);
    }

    let mut cert_file_name = false;
    let mut cert_password = false;
    let mut cluster_username = false;
    let mut cluster_password = false;
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

      if eq_ignore_case_key(key, KEY_MAIN_LOG_MEMORY) {
        main_log_memory_size = Some(value.to_vec());
      } else if eq_ignore_case_key(key, KEY_READ_CACHE_MEMORY) {
        read_cache_memory_size = Some(value.to_vec());
      } else if eq_ignore_case_key(key, KEY_INDEX) {
        index = Some(value.to_vec());
      } else if eq_ignore_case_key(key, KEY_CERT_FILE_NAME) {
        cert_file_name = true;
      } else if eq_ignore_case_key(key, KEY_CERT_PASSWORD) {
        cert_password = true;
      } else if eq_ignore_case_key(key, KEY_CLUSTER_USERNAME) {
        cluster_username = true;
      } else if eq_ignore_case_key(key, KEY_CLUSTER_PASSWORD) {
        cluster_password = true;
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
      // 集群凭据：C# 无集群提供者时 append "ERR Cluster is disabled."
      if cluster_username || cluster_password {
        error_msg.append("ERR Cluster is disabled.");
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
        self.handle_index_size_change(index_size, &mut error_msg);
      }

      // 动态设置：逐条校验落槽（C# TrySet 错误原样 AppendError 累积）
      for (config_type, value) in dynamic_sets {
        if let Err(error) = runtime_config.try_set(config_type, &string_lossy(&value)) {
          error_msg.append(&error.to_string());
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
      error_msg.append_with_template(ERR_INCORRECT_SIZE_FORMAT, KEY_MAIN_LOG_MEMORY);
      return;
    };

    // C# 同尺寸短路（GenericErrMemorySizeGreaterThanBuffer 上限拒绝同理）依赖
    // 已配置的 LogMemorySize（服务器选项未接）；直落 tracker 检查
    if is_read_cache {
      error_msg.append_with_template(ERR_READ_CACHE_TRACKER_NOT_RUNNING, KEY_READ_CACHE_MEMORY);
    } else {
      error_msg.append_with_template(ERR_MAIN_LOG_TRACKER_NOT_RUNNING, KEY_MAIN_LOG_MEMORY);
    }
  }

  /// libs/server/ServerConfig.cs:HandleIndexSizeChangeAsync
  ///
  /// 尺寸格式与 2 的幂校验可同步完成；C# 后续索引增长依赖 store.IndexSize /
  /// GrowIndexAsync（rust wkv 无会话可达入口，见 store.rs），按增长失败错误
  /// 降级——缺口：wkv 需暴露索引在线扩容通道
  fn handle_index_size_change(&mut self, index_size: &[u8], error_msg: &mut ErrorMsgBuilder) {
    let Some(new_index_size) = try_parse_size_bytes(index_size) else {
      error_msg.append_with_template(ERR_INCORRECT_SIZE_FORMAT, KEY_INDEX);
      return;
    };

    // 2 的幂校验
    let adj_new_index_size = previous_power_of_2(new_index_size);
    if adj_new_index_size != new_index_size {
      error_msg.append_with_template(ERR_INDEX_SIZE_POWER_OF_TWO, KEY_INDEX);
      return;
    }

    // C# 自动增长任务检查（GenericErrIndexSizeAutoGrow）：AdjustedIndexMaxCacheLines
    // 默认 0（未启用）→ 不可达

    // 索引增长通道缺席：C# GrowIndexAsync 失败时即此错误
    error_msg.append_with_template(ERR_INDEX_SIZE_GROW_FAILED, KEY_INDEX);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::RuntimeServerOptions;

  /// 无持有方的默认运行时配置（测试夹具）
  fn config() -> RuntimeServerConfig {
    RuntimeServerConfig::new(RuntimeServerOptions::default(), None)
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
    b.append_with_template(ERR_INCORRECT_SIZE_FORMAT, KEY_MAIN_LOG_MEMORY);
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
    s.network_config_get(&[], &rc, &mut out).unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'CONFIG|GET' command\r\n"
    );

    // 未知参数 → 空列表
    let mut out = Vec::new();
    s.network_config_get(&[b"maxmemory"], &rc, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");

    // slave-read-only → 单条 map（RESP2 双倍数组），固定回 yes
    let mut out = Vec::new();
    s.network_config_get(&[b"slave-read-only"], &rc, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$15\r\nslave-read-only\r\n$3\r\nyes\r\n");

    // 运行时表参数：CONFIG GET cluster-node-timeout → 默认 60
    let mut out = Vec::new();
    s.network_config_get(&[b"cluster-node-timeout"], &rc, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$20\r\ncluster-node-timeout\r\n$2\r\n60\r\n");

    // "*" → 全表 + 会话级；重复参数去重（C# HashSet 语义）
    let mut out = Vec::new();
    s.network_config_get(
      &[b"cluster-node-timeout", b"CLUSTER-NODE-TIMEOUT"],
      &rc,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*2\r\n$20\r\ncluster-node-timeout\r\n$2\r\n60\r\n");

    // SET 后 GET 反映新值
    rc.try_set(ServerConfigType::ClusterNodeTimeout, "120")
      .unwrap();
    let mut out = Vec::new();
    s.network_config_get(&[b"cluster-timeout"], &rc, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$20\r\ncluster-node-timeout\r\n$3\r\n120\r\n");

    // "*" 输出含只读回落参数与运行时参数
    let mut out = Vec::new();
    s.network_config_get(&[b"*"], &rc, &mut out).unwrap();
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("slave-read-only"), "{text}");
    assert!(text.contains("cluster-node-timeout"), "{text}");
    assert!(text.contains("aof-commit-freq"), "{text}");
    assert!(text.contains("databases"), "{text}");
  }

  #[test]
  fn config_set_validation_and_disabled_subsystems() {
    let mut s = ServerConfig;
    let rc = config();

    // 奇数参数
    let mut out = Vec::new();
    s.network_config_set(&[b"a"], &rc, &mut out).unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'CONFIG|SET' command\r\n"
    );

    // 未知键 → 首例进错误
    let mut out = Vec::new();
    s.network_config_set(&[b"maxmemory", b"1g"], &rc, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Unknown option or number of arguments for CONFIG SET - 'maxmemory'\r\n"
    );

    // TLS 禁用
    let mut out = Vec::new();
    s.network_config_set(&[b"cert-file-name", b"a.pem"], &rc, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR TLS is disabled.\r\n");

    // 集群禁用
    let mut out = Vec::new();
    s.network_config_set(&[b"cluster-password", b"p"], &rc, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR Cluster is disabled.\r\n");

    // 多错误以 "; " 连接且后续条剥离 "ERR "
    let mut out = Vec::new();
    s.network_config_set(
      &[b"cluster-username", b"u", b"cert-password", b"p"],
      &rc,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"-ERR Cluster is disabled.; TLS is disabled.\r\n");

    // 尺寸格式非法
    let mut out = Vec::new();
    s.network_config_set(&[b"memory", b"abc"], &rc, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR Incorrect size format in (option: 'memory')\r\n");

    // tracker 未运行（含格式合法场景）
    let mut out = Vec::new();
    s.network_config_set(&[b"memory", b"1g"], &rc, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Cannot adjust main log memory size configuration when size tracker is not running (option: 'memory')\r\n"
    );

    // readcache-memory：错误模板用 readcache 选项名
    let mut out = Vec::new();
    s.network_config_set(&[b"READCACHE-MEMORY", b"1g"], &rc, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Cannot adjust readcache memory size configuration when size tracker is not running (option: 'readcache-memory')\r\n"
    );

    // index 非 2 的幂
    let mut out = Vec::new();
    s.network_config_set(&[b"index", b"1000"], &rc, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Index size must be a power of 2 (option: 'index')\r\n"
    );

    // index 2 的幂 → 增长通道缺席错误
    let mut out = Vec::new();
    s.network_config_set(&[b"index", b"1024"], &rc, &mut out)
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

    // 运行时参数：合法值落槽并回 OK
    let mut out = Vec::new();
    s.network_config_set(&[b"slowlog-log-slower-than", b"5000"], &rc, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    assert_eq!(
      rc.get_microseconds(ServerConfigType::SlowlogLogSlowerThan),
      5000
    );

    // 布尔值
    let mut out = Vec::new();
    s.network_config_set(&[b"compaction-force-delete", b"yes"], &rc, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    assert!(rc.get_bool(ServerConfigType::CompactionForceDelete));

    // 枚举值
    let mut out = Vec::new();
    s.network_config_set(&[b"compaction-type", b"Lookup"], &rc, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    // 非法值：TrySet 错误进累积器，槽位保持
    let mut out = Vec::new();
    s.network_config_set(&[b"compaction-force-delete", b"maybe"], &rc, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Invalid value for 'compaction-force-delete': expected 'yes' or 'no'.\r\n"
    );

    // 只读参数拒绝
    let mut out = Vec::new();
    s.network_config_set(&[b"databases", b"8"], &rc, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR Option 'databases' is read-only and cannot be set at runtime.\r\n"
    );

    // 多对混合：合法 + 非法各一，错误累积、合法仍生效
    let mut out = Vec::new();
    s.network_config_set(
      &[
        b"object-scan-count-limit",
        b"64",
        b"cluster-node-timeout",
        b"not-a-number",
      ],
      &rc,
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
