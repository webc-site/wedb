//! CONFIG GET/SET/REWRITE（对标 libs/server/ServerConfig.cs 与 RuntimeServerConfig 交互）

use crate::resp::{
  cmd_strings as cs,
  cmd_strings::{
    abort_with_wrong_number_of_arguments, write_error_raw, write_map_len_resp2, write_raw,
  },
  parser::resp_ext::{RespSliceExt, RespVecExt},
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

/// GetConfig 可达的配置类型结果（C# ServerConfigType 全表依赖
/// RuntimeServerConfig 元数据表；rust 表未建成，仅星号/会话级参数可解析，
/// 其余一律 NONE → CONFIG GET 按空匹配处理，CONFIG SET 按未知键处理）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerConfigType {
  /// "*"：展开全部运行时配置 + 会话级 slave-read-only
  All,
  /// "slave-read-only"：会话级值，不在运行时配置表内
  SlaveReadOnly,
  /// 表中无此参数
  None,
}

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

/// libs/server/Servers/ServerOptions.cs:ParseSize
///
/// 解析 "64g"/"256mb" 风格尺寸字面量；返回 (值, 消费字符数)。溢出按 C# 的
/// 无检查算术回绕语义镜像（wrapping）
fn parse_size(value: &[u8]) -> (i64, usize) {
  const SUFFIXES: &[u8] = b"kmgt";
  let mut result: i64 = 0;
  let mut bytes_read = 0usize;
  let mut i = 0usize;
  while i < value.len() {
    let c = value[i];
    if c.is_ascii_digit() {
      result = result.wrapping_mul(10).wrapping_add((c - b'0') as i64);
      bytes_read += 1;
    } else {
      if let Some(s) = SUFFIXES
        .iter()
        .position(|&suffix| suffix == c.to_ascii_lowercase())
      {
        // 1024^(s+1)；C# 以浮点 Math.Pow 计算，幂次 ≤5 时无精度损失
        result = result.wrapping_mul(1024i64.pow(s as u32 + 1));
        bytes_read += 1;
        if i + 1 < value.len() && (value[i + 1] == b'b' || value[i + 1] == b'B') {
          bytes_read += 1;
        }
        return (result, bytes_read);
      }
      // 首个非数字非已知后缀字符终止扫描（bytesRead 不计入）
      return (result, bytes_read);
    }
    i += 1;
  }
  (result, bytes_read)
}

/// libs/server/Servers/ServerOptions.cs:TryParseSize
fn try_parse_size(value: &[u8]) -> Option<i64> {
  let (size, chars_read) = parse_size(value);
  (chars_read == value.len()).then_some(size)
}

/// libs/server/Servers/ServerOptions.cs:PreviousPowerOf2
const fn previous_power_of_2(mut v: i64) -> i64 {
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  v - (v >> 1)
}

pub struct ServerConfig;

impl ServerConfig {
  /// libs/server/ServerConfig.cs:GetConfig
  ///
  /// 参数名 → 配置类型；运行时配置表（RuntimeServerConfig.TryGetType）未建成，
  /// 除星号与会话级 slave-read-only 外一律 NONE
  pub fn get_config(parameter: &[u8]) -> ServerConfigType {
    // C# 原地大写后比较；此处直接大小写不敏感等价
    if parameter.eq_ignore_ascii_case(b"*") {
      return ServerConfigType::All;
    }

    // slave-read-only 是会话级值（READWRITE/READONLY），由 CONFIG GET 处理器
    // 携带会话解析，不入运行时配置表
    if parameter.eq_ignore_ascii_case(b"SLAVE-READ-ONLY") {
      return ServerConfigType::SlaveReadOnly;
    }

    // 其余参数经运行时配置表解析；表未建成 → NONE
    ServerConfigType::None
  }
}

impl ServerConfig {
  /// libs/server/ServerConfig.cs:NetworkCONFIG_GET
  pub fn network_config_get<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "CONFIG|GET");
      return Ok(true);
    }

    // 去重保序收集（C# HashSet 插入序）；NONE 参数静默跳过
    let mut parameters: Vec<ServerConfigType> = Vec::new();
    let mut return_all = false;
    for parameter in parse_state {
      let server_config_type = ServerConfig::get_config(parameter);

      if return_all {
        continue;
      }
      if server_config_type == ServerConfigType::All {
        // 运行时全表 + 会话级 slave-read-only；表未建成 → 仅 slave-read-only
        parameters.push(ServerConfigType::SlaveReadOnly);
        return_all = true;
        continue;
      }

      if server_config_type == ServerConfigType::None {
        continue;
      }

      parameters.push(server_config_type);
    }

    if parameters.is_empty() {
      // C# 空匹配回 RESP_EMPTYLIST
      write_raw(output, cs::RESP_EMPTYLIST);
      return Ok(true);
    }

    // RESP2 map 退化为双倍数组；slave-read-only 为会话级值：无集群会话即
    // 读写会话 → "no"（C# 同式：clusterSession == null || ReadWriteSession）
    write_map_len_resp2(output, parameters.len());
    for config_type in parameters {
      if config_type == ServerConfigType::SlaveReadOnly {
        output.write_resp_bulk_string(b"slave-read-only");
        output.write_resp_bulk_string(b"no");
      }
    }
    Ok(true)
  }
  /// libs/server/ServerConfig.cs:NetworkCONFIG_REWRITE
  pub fn network_config_rewrite<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
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
  pub fn network_config_set<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
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
      } else if !unknown_option {
        // 运行时配置表未建成：除上列特殊键外全部按未知键记录首例
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
    let Some(_new_memory_size) = try_parse_size(memory_size) else {
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
    let Some(new_index_size) = try_parse_size(index_size) else {
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
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};

  use super::*;

  #[test]
  fn get_config_parses_special_params() {
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
    // 运行时表未建成 → NONE
    assert_eq!(
      ServerConfig::get_config(b"maxmemory"),
      ServerConfigType::None
    );
  }

  #[test]
  fn parse_size_matches_csharp_semantics() {
    // 纯数字 / 单后缀 / 双后缀 / 忽略大小写
    assert_eq!(try_parse_size(b"1024"), Some(1024));
    assert_eq!(try_parse_size(b"1k"), Some(1024));
    assert_eq!(try_parse_size(b"1kb"), Some(1024));
    assert_eq!(try_parse_size(b"1K"), Some(1024));
    assert_eq!(try_parse_size(b"2m"), Some(2 * 1024 * 1024));
    assert_eq!(try_parse_size(b"1g"), Some(1024 * 1024 * 1024));
    assert_eq!(try_parse_size(b"1t"), Some(1024i64.pow(4)));
    // 尾随垃圾 → 消费数不等 → None
    assert_eq!(try_parse_size(b"1x"), None);
    assert_eq!(try_parse_size(b"1k2"), None);
    // C# 语义保留：空串 = 合法尺寸 0
    assert_eq!(try_parse_size(b""), Some(0));
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

  /// 兼容既有测试组织：以下用真实批处理上下文覆盖 CONFIG 族
  #[test]
  fn config_get_slave_read_only_and_unknown() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("config.db")).unwrap());
      let mut config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let mut s = ServerConfig;

      // 零参 → wrong args
      let mut out = Vec::new();
      let _ = s.network_config_get(&[], &batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'CONFIG|GET' command\r\n"
      );

      // 未知参数 → 空列表
      let mut out = Vec::new();
      let _ = s
        .network_config_get(&[b"maxmemory"], &batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*0\r\n");

      // slave-read-only → 单条 map（RESP2 双倍数组）
      let mut out = Vec::new();
      let _ = s
        .network_config_get(&[b"slave-read-only"], &batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*2\r\n$15\r\nslave-read-only\r\n$2\r\nno\r\n");

      // "*" → 全表 + 会话级 → 同单条
      let mut out = Vec::new();
      let _ = s.network_config_get(&[b"*"], &batch, &mut out).unwrap();
      assert_eq!(out, b"*2\r\n$15\r\nslave-read-only\r\n$2\r\nno\r\n");
    });
  }

  #[test]
  fn config_set_validation_and_disabled_subsystems() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("config2.db")).unwrap(),
      );
      let mut config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let mut s = ServerConfig;

      // 奇数参数
      let mut out = Vec::new();
      let _ = s.network_config_set(&[b"a"], &batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR wrong number of arguments for 'CONFIG|SET' command\r\n");

      // 未知键 → 首例进错误
      let mut out = Vec::new();
      let _ = s
        .network_config_set(&[b"maxmemory", b"1g"], &batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR Unknown option or number of arguments for CONFIG SET - 'maxmemory'\r\n"
      );

      // TLS 禁用
      let mut out = Vec::new();
      let _ = s
        .network_config_set(&[b"cert-file-name", b"a.pem"], &batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR TLS is disabled.\r\n");

      // 集群禁用
      let mut out = Vec::new();
      let _ = s
        .network_config_set(&[b"cluster-password", b"p"], &batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Cluster is disabled.\r\n");

      // 多错误以 "; " 连接且后续条剥离 "ERR "
      let mut out = Vec::new();
      let _ = s
        .network_config_set(
          &[b"cluster-username", b"u", b"cert-password", b"p"],
          &batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(out, b"-ERR Cluster is disabled.; TLS is disabled.\r\n");

      // 尺寸格式非法
      let mut out = Vec::new();
      let _ = s
        .network_config_set(&[b"memory", b"abc"], &batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR Incorrect size format in (option: 'memory')\r\n"
      );

      // tracker 未运行（含格式合法场景）
      let mut out = Vec::new();
      let _ = s
        .network_config_set(&[b"memory", b"1g"], &batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR Cannot adjust main log memory size configuration when size tracker is not running (option: 'memory')\r\n"
      );

      // readcache-memory：错误模板用 readcache 选项名
      let mut out = Vec::new();
      let _ = s
        .network_config_set(&[b"READCACHE-MEMORY", b"1g"], &batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR Cannot adjust readcache memory size configuration when size tracker is not running (option: 'readcache-memory')\r\n"
      );

      // index 非 2 的幂
      let mut out = Vec::new();
      let _ = s
        .network_config_set(&[b"index", b"1000"], &batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Index size must be a power of 2 (option: 'index')\r\n");

      // index 2 的幂 → 增长通道缺席错误
      let mut out = Vec::new();
      let _ = s
        .network_config_set(&[b"index", b"1024"], &batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR failed to grow index size beyond current size (option: 'index')\r\n"
      );
    });
  }

  #[test]
  fn config_rewrite_ok_without_cluster() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("config3.db")).unwrap());
      let mut config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let mut s = ServerConfig;

      let mut out = Vec::new();
      let _ = s.network_config_rewrite(&[], &batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");

      let mut out = Vec::new();
      let _ = s.network_config_rewrite(&[b"x"], &batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'CONFIG|REWRITE' command\r\n"
      );
    });
  }
}
