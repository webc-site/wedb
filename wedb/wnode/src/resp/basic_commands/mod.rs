//! 基础命令（对标 libs/server/Resp/BasicCommands.cs 与 AdminCommands.cs）
//!
//! 目录化拆分：[`get`] 字符串读命令、[`set`] 字符串写命令、[`incr`] 数值自增自减、
//! [`ttl`] TTL 与时间戳换算；本文件保留服务端通用/管理命令（PING/ASKING/
//! ECHO/HELLO/COMMAND/CONFIG/FLUSHDB/FLUSHALL/MEMORY/OBJECT/ASYNC）。

use wdev::Device;
use wresp::{
  command::RespCommand,
  resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter},
};

mod get;
mod incr;
mod set;
pub mod slow;
mod ttl;

use std::sync::Arc;

use wbase::{ascii_sanitize, num::strict_i32, time::now_ticks};
use wkv::{StoreResult, is_expired};
use wresp::{
  catalog::{
    SimpleRespKeySpec, extract_keys_and_flags_from_slice, extract_keys_from_slice,
    try_get_resp_command_info_by_name, try_get_resp_commands_info_count,
    try_get_resp_commands_info_ordered, try_get_simple_resp_command_info,
  },
  check_args::{check_arg_count, parse_i32_arg, unpack_args},
  cmd_strings::{self as cs, RESP_ERR_GENERIC, abort_with_error_message, write_raw},
  ext::{RespSliceExt, RespVecExt, is_resp3},
};
use wtxn::TxnState;
use wval::KeyTag;

pub use self::{
  incr::IncrCmd,
  set::{SetCmd, SetOptions},
  ttl::{
    MAX_TIMESPAN_MILLISECONDS, MAX_TIMESPAN_SECONDS, MAX_UNIX_TIME_MILLISECONDS,
    MAX_UNIX_TIME_SECONDS,
  },
};
pub(crate) use self::{
  set::{
    RiWriteGate, apply_set_with_expiry, parse_set_options, ri_write_gate, string_record_fits_page,
  },
  ttl::try_get_absolute_expiry_ticks,
};
use super::{
  acl_store::AclStore,
  config_commands::ServerConfig,
  resp_server_session::RespServerSession,
  vector::vector_manager::{INDEX_SIZE_BYTES, VectorManager},
};
use crate::{
  cluster_session::ClusterSessionFace,
  resp::objects::object_store_utils::envelope_heap_estimate,
  session_parse_state_extensions::try_get_client_name_bytes,
  storage::session::common::{
    TagRead, UserRead, read_envelope_sync, read_tag_sync, read_user_sync,
    ttl_sync::{meta_collection_type_of, read_adjudicated_tag_with_size, ttl_of_sync},
  },
};

/// OBJECT 子命令形态（对标 libs/server/Resp/Parser/RespCommand.cs:OBJECT_*）
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase", prefix = "object|")]
pub enum ObjectSubCmd {
  Encoding,
  Freq,
  Idletime,
  Refcount,
}

impl ObjectSubCmd {
  /// C# 错误文案中的子命令名（NetworkOBJECT 的 subCommandName 映射）
  #[inline]
  pub fn as_str(self) -> &'static str {
    self.into()
  }
}

/// COMMAND GETKEYS[ANDFLAGS] 提取上下文
struct CommandKeysContext<'a> {
  cmd_args: &'a [&'a [u8]],
  key_specs: &'static [SimpleRespKeySpec],
  is_sub_command: bool,
}

impl RespServerSession {
  /// libs/server/Resp/BasicCommands.cs:NetworkPING /
  /// libs/server/Resp/ArrayCommands.cs:NetworkArrayPING
  ///
  /// 零参：订阅会话 + RESP2 回 SUSCRIBE_PONG 整帧（`["pong",""]`，redis-py 等按
  /// 帧形态判 pong），否则回 +PONG；单参回显消息 bulk（不受订阅模式影响）；
  /// 多参报参数错误（C# ProcessBasicCommands 依 Count 分流两实现）
  pub fn network_ping(&self, parse_state: &[&[u8]], output: &mut Vec<u8>) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=1, output, "PING");
    if let Some(msg) = parse_state.first() {
      output.write_resp_bulk_string(msg);
    } else if self.is_subscription_session && self.resp_protocol_version == 2 {
      // C# NetworkPING：isSubscriptionSession && respProtocolVersion==2 写
      // CmdStrings.SUSCRIBE_PONG（两元素数组），否则才 RESP_PONG
      output.extend_from_slice(cs::SUSCRIBE_PONG);
    } else {
      output.extend_from_slice(cs::RESP_PONG);
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkASKING
  ///
  /// 严格对位 C#：无 arity 校验（任意参数恒 +OK），仅 EnableCluster（本仓以
  /// cluster_session 挂载表达，同 NetworkFLUSHALL 入口判定）时置
  /// SessionAsking=2（u8 剩余计数，本命令与下一条各放行一次 ask 重定向）
  pub fn network_asking(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    if self.cluster_session.is_some() {
      self.session_asking = 2;
    }
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkREADONLY
  pub fn network_readonly(&mut self) -> bool {
    self.apply_read_only_session(|c| c.set_read_only_session())
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkREADWRITE
  pub fn network_readwrite(&mut self) -> bool {
    self.apply_read_only_session(|c| c.set_read_write_session())
  }

  /// READONLY/READWRITE 共同体
  ///
  /// 严格对位 C#:无 arity 校验(任意参数恒 +OK,多余参数静默忽略);只读态
  /// 唯一真源在 cluster_session 切面原子(C# ClusterSession.readOnlySession),
  /// 切面缺席(standalone)时仅回 +OK,与 C# clusterSession 为 null 同构。臂别
  /// 仅差切面原子的置位/复位调用,经闭包注入
  fn apply_read_only_session(&mut self, set: impl FnOnce(&dyn ClusterSessionFace)) -> bool {
    if let Some(cluster) = &self.cluster_session {
      set(&**cluster);
    }
    self.output.extend_from_slice(cs::RESP_OK);
    true
  }

  /// FLUSHDB/FLUSHALL 副本只读拦截单源
  ///
  /// FLUSHDB/FLUSHALL 的副本只读门（C# BasicCommands 的 NetworkFLUSHDB/
  /// NetworkFLUSHALL 入口判定：EnableCluster && clusterProvider.IsReplica() &&
  /// !clusterSession.IsInternalWriteSession →
  /// CmdStrings.RESP_ERR_FLUSHALL_READONLY_REPLICA）。
  ///
  /// 集群切面缺席（standalone，C# EnableCluster == false）恒放行。C# 以会话
  /// 标记豁免内部重放会话；rust 回放不经 RESP 分派、直达存储域，豁免在架构层
  /// 天然成立，本门仅拦客户端会话
  fn flush_replica_read_only_gate(&self, output: &mut Vec<u8>) -> bool {
    if self.cluster_session.is_none() {
      return false;
    }
    let is_replica = self
      .cluster_provider
      .as_ref()
      .is_some_and(|p| p.is_replica());
    if is_replica {
      abort_with_error_message(output, cs::RESP_ERR_FLUSHALL_READONLY_REPLICA);
      return true;
    }
    false
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkFLUSHDB
  pub fn network_flushdb(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=2, output, "FLUSHDB");
    if self.flush_replica_read_only_gate(output) {
      return Ok(true);
    }
    self.flush_db("FLUSHDB", parse_state, output)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkFLUSHALL
  pub fn network_flushall(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=3, output, "FLUSHALL");
    if self.flush_replica_read_only_gate(output) {
      return Ok(true);
    }
    // Garnet 单库，FLUSHALL 与 FLUSHDB 共用 FlushDb
    self.flush_db("FLUSHALL", parse_state, output)
  }

  /// libs/server/Resp/BasicCommands.cs:WriteCOMMANDResponse
  pub fn write_command_response(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    if let Some(infos) = try_get_resp_commands_info_ordered(true) {
      if is_resp3(self.resp_protocol_version) {
        let mut writer = RespWriter::<_, Resp3>::new_ref_p(output);
        writer.write_array_length(infos.len());
        for (_name, info) in infos {
          info.to_resp_format(&mut writer);
        }
      } else {
        let mut writer = RespWriter::<_, Resp2>::new_ref(output);
        writer.write_array_length(infos.len());
        for (_name, info) in infos {
          info.to_resp_format(&mut writer);
        }
      }
    } else {
      write_raw(output, cs::RESP_EMPTYLIST);
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_COUNT
  pub fn network_command_count<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "COMMAND|COUNT");
    let count = try_get_resp_commands_info_count(true).unwrap_or(0);
    output.write_resp_int(count as i64);
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_DOCS
  pub fn network_command_docs<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    if is_resp3(self.resp_protocol_version) {
      Self::write_command_docs_p::<Resp3>(count, parse_state, output);
    } else {
      Self::write_command_docs_p::<Resp2>(count, parse_state, output);
    }
    Ok(true)
  }

  fn write_command_docs_p<P: RespProtocol>(
    count: usize,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) {
    let mut writer = RespWriter::<_, P>::new_ref_p(output);
    if count == 0 {
      if let Some(cmds_docs) = super::resp_command_docs::try_get_resp_commands_docs_ordered(true) {
        writer.write_map_length(cmds_docs.len());
        for cmd_docs in cmds_docs {
          cmd_docs.to_resp_format(&mut writer);
        }
      } else {
        writer.write_map_length(0);
      }
    } else {
      let matched_count = parse_state
        .iter()
        .filter(|raw| {
          super::resp_command_docs::try_get_resp_command_docs(raw.as_str_safe(), true, true)
            .is_some()
        })
        .count();
      writer.write_map_length(matched_count);
      for raw in parse_state {
        let name = raw.as_str_safe();
        if let Some(cmd_docs) =
          super::resp_command_docs::try_get_resp_command_docs(name, true, true)
        {
          cmd_docs.to_resp_format(&mut writer);
        }
      }
    }
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_INFO
  pub fn network_command_info<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    if count == 0 {
      // 零参等价无参 COMMAND
      return self.write_command_response(output);
    }
    if is_resp3(self.resp_protocol_version) {
      Self::write_command_info_p::<Resp3>(count, parse_state, output);
    } else {
      Self::write_command_info_p::<Resp2>(count, parse_state, output);
    }
    Ok(true)
  }

  fn write_command_info_p<P: RespProtocol>(
    count: usize,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) {
    let mut writer = RespWriter::<_, P>::new_ref_p(output);
    writer.write_array_length(count);
    for raw in parse_state {
      let name = raw.as_str_safe();
      if let Some(info) = try_get_resp_command_info_by_name(name, true, true) {
        info.to_resp_format(&mut writer);
      } else {
        writer.write_null();
      }
    }
  }

  /// 准备提取命令 Key 的参数切片与规格上下文（消除 GETKEYS / GETKEYSANDFLAGS 重复代码，零多余堆分配）
  fn prepare_command_keys_context<'b>(
    parse_state: &'b [&'b [u8]],
    output: &mut Vec<u8>,
    cmd_name_for_err: &str,
  ) -> Option<CommandKeysContext<'b>> {
    check_arg_count!(parse_state, 1.., output, cmd_name_for_err, return None);
    let cmd_name = parse_state[0].as_str_safe();
    // 命令名解析走 strum EnumString 纯名词法（wresp/src/command.rs），数字形名（如 "8"）拒收；
    // 不对齐 C# Enum.TryParse 数值回退（BasicCommands.cs:2046，"8" 收敛 RespCommand.DEL 取其
    // 键规格回 *1+键），rust 回 RESP_INVALID_COMMAND_SPECIFIED 系定义性行为，
    // 详见 doc/zh/deviations.md §106 宗 a，严禁回改。
    let cmd = RespCommand::from_cs_name(cmd_name);
    let mut simple_info = cmd.and_then(try_get_simple_resp_command_info);

    if let Some(info) = simple_info
      && info.is_parent
      && parse_state.len() >= 2
    {
      // C# 上游 TryGetSimpleCommandInfo（BasicCommands.cs:2046）只按首参单名
      // Enum.TryParse：`COMMAND GETKEYS OBJECT ENCODING k` 在 C# 落 OBJECT parent
      // （KeySpecifications 缺席）报 RESP_COMMAND_HAS_NO_KEY_ARGS；rust 有意组合
      // {parent}_{sub} 名再查子命令规格成功提取键，对齐 Redis 行为。子命令规格
      // 同样缺席的（如 CONFIG GET）两侧同报 no-key-args，无分叉。差分测试
      // command_getkeys_parent_sub_lookup（wnode/tests/resp_tests.rs）钉住两面。
      let sub_name = format!("{}_{}", cmd_name, parse_state[1].as_str_safe());
      if let Some(sub_cmd) = RespCommand::from_cs_name(&sub_name)
        && let Some(sub_info) = try_get_simple_resp_command_info(sub_cmd)
      {
        simple_info = Some(sub_info);
      }
    }

    let Some(simple_info) = simple_info else {
      abort_with_error_message(output, cs::RESP_INVALID_COMMAND_SPECIFIED);
      return None;
    };
    if simple_info.key_specs.is_empty() {
      abort_with_error_message(output, cs::RESP_COMMAND_HAS_NO_KEY_ARGS);
      return None;
    }
    let slice_offset = if simple_info.is_sub_command { 2 } else { 1 };
    let cmd_args = if parse_state.len() >= slice_offset {
      &parse_state[slice_offset..]
    } else {
      &[]
    };
    Some(CommandKeysContext {
      cmd_args,
      key_specs: simple_info.key_specs.as_slice(),
      is_sub_command: simple_info.is_sub_command,
    })
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_GETKEYS
  pub fn network_command_getkeys<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(ctx) = Self::prepare_command_keys_context(parse_state, output, "COMMAND_GETKEYS")
    else {
      return Ok(true);
    };
    let keys = extract_keys_from_slice(ctx.cmd_args, ctx.key_specs, ctx.is_sub_command);
    output.write_resp_array_len(keys.len());
    for key in keys {
      output.write_resp_bulk_string(key);
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_GETKEYSANDFLAGS
  pub fn network_command_getkeysandflags<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(ctx) =
      Self::prepare_command_keys_context(parse_state, output, "COMMAND_GETKEYSANDFLAGS")
    else {
      return Ok(true);
    };
    let pairs = extract_keys_and_flags_from_slice(ctx.cmd_args, ctx.key_specs, ctx.is_sub_command);
    output.write_resp_array_len(pairs.len());
    for (key, flags) in pairs {
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      let count = flags.iter().count();
      // set 帧头版本分派一律经 wresp 单点（对标 C# BasicCommands.cs:1413
      // WriteSetLength → RespServerSessionOutput.cs:238 respProtocolVersion >= 3
      // 分派）：RESP3 `~<len>\r\n`，RESP2 退化数组 `*<len>\r\n`，命令层不自拼帧前缀。
      cs::write_set_len(output, count, self.resp_protocol_version);
      for flag in flags.iter_descriptions() {
        output.write_resp_bulk_string(flag.as_bytes());
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkECHO
  pub fn network_echo(&self, parse_state: &[&[u8]], output: &mut Vec<u8>) -> wresp::Result<bool> {
    let Some([msg]) = unpack_args(parse_state, output, "ECHO") else {
      return Ok(true);
    };
    output.write_resp_bulk_string(msg);
    Ok(true)
  }

  /// libs/server/Resp/AdminCommands.cs:NetworkCONFIG_GET
  ///
  /// CONFIG GET 分派承接（实现位于 wconf::ServerConfig，经会话共享的
  /// runtime_config 实例读取；C# RespServerSession 分派 +
  /// storeWrapper.runtimeConfig 管道形态）
  pub fn network_config_get(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let mut config_server = ServerConfig;
    config_server.network_config_get(
      parse_state,
      self.runtime_config(),
      self.resp_protocol_version,
      output,
    )
  }

  /// libs/server/Resp/AdminCommands.cs:NetworkCONFIG_SET
  ///
  /// CONFIG SET 分派承接（实现位于本 crate resp::config_commands::ServerConfig）；
  /// C# storeWrapper.clusterProvider?.UpdateClusterAuth 的集群凭据臂经会话
  /// 持有的集群提供者句柄注入（单机无句柄 = C# null 分支）
  pub fn network_config_set<D: Device + 'static>(
    &mut self,
    parse_state: &[&[u8]],
    store: Option<&Arc<wkv::WedbStore<D>>>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let mut config_server = ServerConfig;
    let host = super::config_commands::ConfigSetHost {
      runtime_config: self.runtime_config(),
      primary_tasks: self.primary_tasks(),
      aof: self.aof.as_ref(),
      cluster: self.cluster_provider.as_ref(),
      index_auto_grow_active: self.index_auto_grow_active,
      #[cfg(feature = "tls")]
      tls_config: self.tls_config.as_deref(),
    };
    config_server.network_config_set(parse_state, store, &host, output)
  }

  /// libs/server/Resp/AdminCommands.cs:NetworkCONFIG_REWRITE
  ///
  /// CONFIG REWRITE 分派承接（实现位于 wconf::ServerConfig，同上）；C#
  /// ServerConfig.cs:105 `storeWrapper.clusterProvider?.FlushConfig()`
  /// 的拓扑刷盘经集群会话切面中转（无集群切面 no-op = C# 空条件调用符跳过）
  pub fn network_config_rewrite(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty()
      && let Some(cluster) = &self.cluster_session
    {
      cluster.flush_config();
    }
    let mut config_server = ServerConfig;
    config_server.network_config_rewrite(parse_state, output)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkHELLO
  ///
  /// 可携 AUTH 凭据（命名用户经 ACL 存储点查认证），故为 async 臂：由
  /// 泵侧 exec_auth_acl 异步域内联闭环，不经同步分派（漏斗预筛停车）
  pub async fn network_hello<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &AclStore<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 位序文法单源在 parse_hello_args（MULTI 排队臂共用，判据注释在同处）
    let args = match parse_hello_args(parse_state) {
      Ok(args) => args,
      Err(err) => {
        match err {
          HelloParseError::TooManyArgs => cs::abort_with_wrong_number_of_arguments(output, "HELLO"),
          HelloParseError::ProtocolNotInteger => {
            abort_with_error_message(output, cs::RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER)
          }
          HelloParseError::UnsupportedProtocolVersion => {
            abort_with_error_message(output, cs::RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION)
          }
          HelloParseError::SyntaxErrorOption(option) => {
            cs::abort_with_syntax_error_option(output, "HELLO", option)
          }
          HelloParseError::SyntaxErrorUnknown { option } => {
            cs::abort_with_syntax_error_option(output, "HELLO", &ascii_sanitize(option))
          }
          HelloParseError::InvalidClientName => {
            abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_NAME)
          }
        }
        return Ok(true);
      }
    };
    let (auth_username, auth_password) = args.auth.unwrap_or_default();

    self
      .process_hello_command(
        args.protocol_version,
        auth_username,
        auth_password,
        args.client_name,
        store,
        output,
      )
      .await
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkMemoryUsage
  pub fn network_memory_usage<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    check_arg_count!(count == 1 || count == 3, output, "MEMORY|USAGE");

    let key = parse_state[0];
    if count == 3 {
      // 嵌套类型采样对 Garnet 无效，仅为 API 兼容校验语法（C# 同口径注释）
      if !parse_state[1].eq_ignore_ascii_case(b"SAMPLES") {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      let Some(samples) = parse_i32_arg(parse_state[2], output) else {
        return Ok(true);
      };
      if samples < 0 {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
    }

    // 对标 UnifiedStore ReadMethods.cs:HandleMemoryUsage：统计命中记录物理分配
    // 尺寸（wkv 读内核披露，口径 = 记录头 + 键 + 值 + 对齐填充/显式松弛，对标
    // `srcLogRecord.AllocatedSize`）。C# 的 Key/ValueIsOverflow 堆溢出加项与 GC
    // 堆布局推算（RecordInfo.Size + 2*IntPtr.Size + RoundUp(key, 8) +
    // ByteArrayOverhead）不适用于 rust：wrecord 24 位键长/32 位值长全内联无溢出
    // 通道，物理尺寸即真实占用。对象键 = 信封记录物理尺寸 + 内层对象
    // heap_memory_size 记账（标准段 + 扩展段合流单点见 object_store_utils
    // envelope_heap_estimate）
    match read_adjudicated_tag_with_size(store, key, KeyTag::String, |_v, size| size) {
      Ok(StoreResult::Success(size)) => {
        output.write_resp_int(size as i64);
        return Ok(true);
      }
      // String 域确认缺失：探对象信封域（对齐 read_adjudicated_user_sync 双域次序）
      Ok(StoreResult::NotFound) => {}
      // 磁盘候选 / TTL 须异步裁决：转慢路径闭环（exec_slow MemoryUsage）
      Ok(StoreResult::RecordOnDisk) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    match read_adjudicated_tag_with_size(store, key, KeyTag::ObjectEnvelope, |raw, size| {
      size as i64 + envelope_heap_estimate(raw)
    }) {
      Ok(StoreResult::Success(total)) => {
        output.write_resp_int(total);
        return Ok(true);
      }
      Ok(StoreResult::NotFound) => {}
      // 磁盘候选 / TTL 须异步裁决：转慢路径闭环（exec_slow MemoryUsage）。
      // 严禁并入 NotFound 空操作块——冷存信封会击穿至 Meta 域误答 nil，
      // 旁路 slow.rs C::MemoryUsage 臂（对齐 String/Meta 域降级语义）
      Ok(StoreResult::RecordOnDisk) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    // 升阶键（Meta 域命中）：C# 对任意存在键回值（HandleMemoryUsage），统计
    // 口径 = Meta 元记录物理尺寸 + 活跃树常驻页环容量（live_tree_cache_bytes：
    // bf-tree 页环一次性整块分配且树存活期常驻，绝非「冷热换入换出」的动态
    // 量；冷态未打开回 0，不虚报）。C# 无对应并项——树页缓存无运行态披露，
    // 本口径为 rust 分层防 OOM 自定义面的可观测性义务
    match read_adjudicated_tag_with_size(store, key, KeyTag::Meta, |_v, size| {
      size as i64 + store.live_tree_cache_bytes(key) as i64
    }) {
      Ok(StoreResult::Success(total)) => output.write_resp_int(total),
      // 三域皆缺：再问向量索引登记表第四态（对标 C# HandleMemoryUsage 对
      // RecordType=VectorManager.RecordType 主存记录回 AllocatedSize——存活
      // 向量键 MEMORY USAGE 回正整数而非 nil）。rust 登记表条目为定长索引描述
      // 记录，回其字节尺寸，即向量记录尺寸的下限声明：live HNSW 索引体由
      // VectorManager 以原生对象持有、不入本 wkv 对象堆估算通道（对位 C#
      // HandleMemoryUsage 对向量主存记录仅回 inlineRecordSize 即
      // AllocatedSize，ReadMethods.cs:106）。与本函数 Meta 臂口径相异——
      // Meta 臂活跃树常驻页环容量按 live_tree_cache_bytes 必须计入（见上
      // 方 :610-614 口径注），两臂口径勿互引；键真缺失仍回 nil（C# status
      // != OK → WriteNull）。登记表键不入 wkv 值域，三域读对向量键恒
      // NotFound 而非 RecordOnDisk，慢路径无向量臂。
      Ok(StoreResult::NotFound) => {
        if vector.is_some_and(|vm| {
          vm.read_stored_index(store.session_prefix().as_slice(), key)
            .is_some()
        }) {
          output.write_resp_int(INDEX_SIZE_BYTES as i64);
        } else {
          output.write_resp_null_ver(self.resp_protocol_version);
        }
      }
      Ok(StoreResult::RecordOnDisk) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// OBJECT 子命令应答单点（向量臂与主域臂共用同一四态帧：ENCODING→bulk、
  /// REFCOUNT→1、IDLETIME→0、FREQ→不支持错误）；encoding 为 ENCODING 臂回值
  fn write_object_subcmd_reply(output: &mut Vec<u8>, sub_cmd: ObjectSubCmd, encoding: &[u8]) {
    match sub_cmd {
      ObjectSubCmd::Encoding => output.write_resp_bulk_string(encoding),
      ObjectSubCmd::Refcount => output.write_resp_int(1),
      ObjectSubCmd::Idletime => output.write_resp_int(0),
      ObjectSubCmd::Freq => abort_with_error_message(output, cs::RESP_ERR_OBJECT_FREQ_UNSUPPORTED),
    }
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkOBJECT
  pub fn network_object<'a, D: Device>(
    &mut self,
    sub_cmd: ObjectSubCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, sub_cmd.as_str()) else {
      return Ok(true);
    };

    if vector.is_some_and(|vm| {
      vm.read_stored_index(store.session_prefix().as_slice(), key)
        .is_some()
    }) {
      Self::write_object_subcmd_reply(output, sub_cmd, b"raw");
      return Ok(true);
    }

    // 语义对照 UnifiedStore ReadMethods:HandleObjectEncoding：
    // String 域命中 → raw（值内容任意）；对象信封域命中按内层标签、升阶键
    //（Meta 域命中）按 MetaValue.collection_type 统一经 encoding_of_object_type
    // 单点映射（SortedSet=skiplist / List=quicklist / RangeIndex=raw /
    // 其余=hashtable）；过期键与缺失键一致回 nil（$-1\r\n）
    // 第三参传 None 系 C# 契约正确侧（OBJECT 族恒零计，单点真源注记见
    // user_read.rs read_user_sync 头注，票 zcode-r157c-objenc 案二）
    let (encoding, degrade) = match read_user_sync(store, key, None, |_| ()) {
      Ok(UserRead::Hit(())) => (Some(slow::ENCODING_RAW), false),
      Ok(UserRead::WrongType) => {
        match read_envelope_sync(store, key, slow::encoding_of_envelope_payload) {
          Ok(TagRead::Hit(enc)) => (Some(enc), false),
          Ok(TagRead::Missing) => {
            // 升阶键：Meta 元记录按 collection_type 映射编码名
            match read_tag_sync(store, key, KeyTag::Meta, meta_collection_type_of) {
              Ok(TagRead::Hit(Some(obj_type))) => {
                (Some(slow::encoding_of_object_type(obj_type)), false)
              }
              Ok(TagRead::Hit(None)) | Ok(TagRead::Missing) => (None, false),
              Ok(TagRead::Deferred) | Err(_) => (None, true),
            }
          }
          Ok(TagRead::Deferred) | Err(_) => (None, true),
        }
      }
      Ok(UserRead::Missing) => (None, false),
      _ => (None, true),
    };
    if degrade {
      // 键缺失或过期裁决：TTL 记录已到期即键死（回 nil），否则降级异步
      if matches!(
        ttl_of_sync(store, key),
        Ok(StoreResult::Success(v)) if is_expired(v, now_ticks())
      ) {
        output.write_resp_null_ver(self.resp_protocol_version);
        return Ok(true);
      }
      return Ok(false);
    }
    match encoding {
      Some(encoding) => Self::write_object_subcmd_reply(output, sub_cmd, encoding),
      // C# 键缺失（status != OK）一律回 nil
      None => output.write_resp_null_ver(self.resp_protocol_version),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkOBJECTHELP
  pub fn network_objecthelp<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "object|help");

    const OBJECT_HELP: [&str; 11] = [
      "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "ENCODING <key>",
      "\tReturn the kind of internal representation used in order to store the value associated with a <key>.",
      "FREQ <key>",
      "\tNot supported in Garnet: always returns an error, as access frequency (LFU) is not tracked.",
      "IDLETIME <key>",
      "\tReturn the idle time of the <key>. Garnet does not track per-key idle time, so this is always 0.",
      "REFCOUNT <key>",
      "\tReturn the number of references of the value associated with the <key>. Garnet does not share value objects, so this is always 1.",
      "HELP",
      "\tPrints this help.",
    ];
    output.write_resp_array_len(OBJECT_HELP.len());
    for line in OBJECT_HELP {
      output.write_resp_simple_string(line);
    }
    Ok(true)
  }

  /// ASYNC 参数应用核心（ON/OFF/BARRIER；respProtocolVersion >= 3 才可达）
  ///
  /// 命令面与参数校验对标 C# NetworkASYNC（其 rust 侧唯一锚点载体是本 impl 的
  /// `network_async`），但三臂统一回 [`cs::RESP_ERR_ASYNC_REQUIRED`]：本仓不移植
  /// C# 的异步处理器面（AsyncProcessor 整文件已登记为无需实现，见
  /// js/check/ignore/server.yml），会话既无 C# useAsync 那样的开关位，也无在途完成
  /// 通道，照抄 C# 的 ON/OFF 置位与 BARRIER 空等待只会伪造 +OK。
  #[rustfmt::skip]
  pub(crate) fn apply_async_param(
    &self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if self.resp_protocol_version <= 2 {
      abort_with_error_message(output, cs::RESP_ERR_NOT_SUPPORTED_RESP2);
      return Ok(true);
    }

    let Some([param]) = unpack_args(parse_state, output, "ASYNC") else {
      return Ok(true);
    };

    if param.eq_ignore_ascii_case(b"ON")
      || param.eq_ignore_ascii_case(b"OFF")
      || param.eq_ignore_ascii_case(b"BARRIER")
    {
      abort_with_error_message(output, cs::RESP_ERR_ASYNC_REQUIRED);
    } else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkASYNC
  pub fn network_async<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.apply_async_param(parse_state, output)
  }

  /// libs/server/Resp/BasicCommands.cs:ProcessHelloCommand
  ///
  /// 校验 → 认证 → 升级协议版本 / 落客户端名 → 组 HELLO 应答 map。应答
  /// 组装与集群形态（mode/role）统一委托
  /// [`RespServerSession::process_hello_command_state`]，一处定义。
  pub async fn process_hello_command<'a, D: Device>(
    &mut self,
    resp_protocol_version: Option<u8>,
    username: &[u8],
    password: &[u8],
    client_name: Option<&str>,
    store: &AclStore<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let start_len = output.len();
    // C# 默认 NoAuth 认证器 Authenticate 恒 false → 带 AUTH 的 HELLO 报
    // WRONGPASS（state 版本承接，认证失败已写错误应答）；ACL 档命名用户
    // 经底层存储点查（存储为唯一真源），仅存储无记录时回落内存认证器
    self
      .process_hello_command_state(
        resp_protocol_version,
        username,
        password,
        client_name,
        Some(store),
        output,
      )
      .await;
    // 冷租户/冷库挂起面：HELLO 应答字节已组入 output 本命令段（start_len 起），
    // 段形态移交 SlowWait（与下方事务守卫臂 truncate 同锚，先例见 AUTH 挂起臂
    // auth.rs 与 SELECT 挂起臂 array_commands.rs 的最小应答移交）——点查装载并
    // 重放切库后原样应答并物化暂存标量与元数据，挂起期间本批停止消费；装载
    // 失败即弃本段应答，认证态与协议版本/客户端名保持旧值零撕裂。同批前序
    // 流水线应答留在会话缓冲，经泵冲出单点按序落流，严禁整包卷走（连坐丢帧
    // 即连接级 RESP 配对永久错拍；且挂起闭环以应答首字节 `-` 判装载失败，
    // 前序错误帧开头会连坐误跳装载成功臂的标量物化）
    if let Some((ns, db)) = self.cold_pending_ctx() {
      if self.txn_state != TxnState::None {
        // 事务窗禁停泊围栏（对齐排队期 SELECT 异库中止先例与 AUTH 禁入事务契约）：
        // 会话在途事务窗（Started/Running/Aborted）内严禁停泊挂起，直接弃置
        // ColdContextPending，会话 namespace、acl_user_handle、协议版本严格保持旧值；
        // 回滚本命令累积 output 并写出错误帧
        self.discard_cold_pending_ctx();
        output.truncate(start_len);
        cs::write_error_raw(output, cs::RESP_ERR_HELLO_IN_TXN_UNSUPPORTED);
        return Ok(true);
      }
      let reply = output.split_off(start_len);
      self.park_cold_context_load(
        &store.storage().store,
        ns,
        db,
        reply,
        output,
        cs::RESP_ERR_HELLO_IN_TXN_UNSUPPORTED,
      );
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:FlushDb
  ///
  /// FLUSHDB/FLUSHALL 共同体：解析 \[ASYNC|SYNC\] \[UNSAFETRUNCATELOG\] 选项；
  /// 语法错误即时应答，合法选项降级慢路径异步清库（C# 网络线程同步执行
  /// ShiftBeginAddress，rust 清库为 O(1) 换号秒清，经 SlowWait 闭环后回 +OK，
  /// 客户端见 +OK 时数据已清——C# SYNC 语义）
  pub fn flush_db(
    &mut self,
    _cmd: &str,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_flush_options(parse_state).is_err() {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    }
    // 清库在慢路径执行段闭环（选项由同一解析器重新解析）
    Ok(false)
  }
}

/// HELLO 位序文法解析结果（C# NetworkHELLO 局部量 tmpRespProtocolVersion /
/// authUsername+authPassword / tmpClientName）
#[derive(Default)]
pub(crate) struct HelloArgs<'a> {
  /// 协议版本（None = 裸 HELLO，不改协议；C# byte? tmpRespProtocolVersion）
  pub(crate) protocol_version: Option<u8>,
  /// 合法消费的 AUTH 选项组（用户名, 口令）；None = 未携带
  pub(crate) auth: Option<(&'a [u8], &'a [u8])>,
  /// SETNAME 客户端名（ASCII 33..=126 校验含于文法）
  pub(crate) client_name: Option<&'a str>,
}

/// HELLO 文法拒绝形态（各臂应答帧映射回 C# NetworkHELLO 原点位）
pub(crate) enum HelloParseError<'a> {
  /// 参数超上界（C# :1441-1445 count > 6 wrong arity 门）
  TooManyArgs,
  /// 协议版本位非整数（C# :1455-1458 TryGetInt 门）
  ProtocolNotInteger,
  /// 协议版本越界 2..=3（C# :1460-1463）
  UnsupportedProtocolVersion,
  /// AUTH/SETNAME 悬于选项位但后续参数不足（C# :1475/:1487 回显大写字面量）
  SyntaxErrorOption(&'static str),
  /// 未知选项 token（C# :1496 回显原文，非 ASCII 折叠在应答臂处理）
  SyntaxErrorUnknown { option: &'a [u8] },
  /// SETNAME 值非法客户端名（C# :1490 TryGetClientName 门）
  InvalidClientName,
}

/// HELLO 位序文法单源（HELLO [protover [AUTH username password] [SETNAME clientname]]，
/// libs/server/Resp/BasicCommands.cs:NetworkHELLO :1446-1512 位序消费）
///
/// 执行臂（network_hello）与 MULTI 排队臂
///（txn_resp_commands::network_skip）共用本单点，两处互指防再漂移：排队
/// 中止面按 doc/zh/deviations.md §58a 恰等于「可解析出合法 AUTH 选项组
/// （可触认证冷租户停泊）」之形集——AUTH 落值位（如 SETNAME AUTH 合法
/// 客户端名）、落选项位但尾随不足两参、协议版本位错形等文法拒绝形均不
/// 可触认证，排队不越权预断（C# HELLO 无 NoMulti 旗
/// libs/resources/RespCommandsInfo.json:2107，NetworkHELLO 全函数无事务
/// 门，该形 C# 正常排队）
#[rustfmt::skip]
pub(crate) fn parse_hello_args<'a>(
  parse_state: &[&'a [u8]],
) -> Result<HelloArgs<'a>, HelloParseError<'a>> {
  let count = parse_state.len();
  if count > 6 {
    return Err(HelloParseError::TooManyArgs);
  }
  let mut args = HelloArgs::default();
  if count == 0 {
    return Ok(args);
  }

  let mut token_idx = 0usize;
  // 校验协议版本（溢出走 not-integer 对标 C# TryGetInt；前导零拒收系 rust 严格文法收口，C# TryGetInt 因死参放行 007 落 unsupported version 门，见 doc/zh/deviations.md §32）
  let Some(local_resp_protocol_version) = strict_i32(parse_state[token_idx]) else {
    return Err(HelloParseError::ProtocolNotInteger);
  };
  token_idx += 1;

  if !(2..=3).contains(&local_resp_protocol_version) {
    return Err(HelloParseError::UnsupportedProtocolVersion);
  }
  args.protocol_version = Some(local_resp_protocol_version as u8);

  while token_idx < count {
    let opt = parse_state[token_idx];
    if opt.eq_ignore_ascii_case(b"AUTH") {
      let [_, u, p, ..] = parse_state[token_idx..] else {
        return Err(HelloParseError::SyntaxErrorOption("AUTH"));
      };
      args.auth = Some((u, p));
      token_idx += 3;
    } else if opt.eq_ignore_ascii_case(b"SETNAME") {
      let [_, name_bytes, ..] = parse_state[token_idx..] else {
        return Err(HelloParseError::SyntaxErrorOption("SETNAME"));
      };
      let Some(name) = try_get_client_name_bytes(name_bytes) else {
        return Err(HelloParseError::InvalidClientName);
      };
      token_idx += 2;
      args.client_name = Some(name);
    } else {
      return Err(HelloParseError::SyntaxErrorUnknown { option: opt });
    }
  }
  Ok(args)
}

/// FLUSHDB/FLUSHALL 选项解析单源（快路径校验段与慢路径执行段同一入口）
///
/// FLUSHDB/FLUSHALL 选项集（快路径校验与慢路径执行共用）
pub(crate) struct FlushOptions {
  /// UNSAFETRUNCATELOG：清库后物理截断日志段（破坏性）
  pub(crate) unsafe_truncate_log: bool,
  /// ASYNC：C# 后台线程执行标志（rust 慢路径闭环语义下仅记录）
  pub(crate) async_flush: bool,
}

/// FLUSHDB/FLUSHALL 选项解析单源（快路径校验段与慢路径执行段同一入口）
///
/// C# FlushDb 的逐 token 判定：UNSAFETRUNCATELOG 不可重复，ASYNC/SYNC
/// 互斥且不可重复，未知 token 即语法错误；`async_flush` 仅区分 C#
/// Task.Run 后台执行（rust 慢路径统一闭环后应答，客户端可见语义同 SYNC）
pub(crate) fn parse_flush_options(parse_state: &[&[u8]]) -> Result<FlushOptions, ()> {
  let mut unsafe_truncate_log = false;
  let mut sync_mode: Option<bool> = None;
  for &token in parse_state {
    if token.eq_ignore_ascii_case(b"UNSAFETRUNCATELOG") {
      if unsafe_truncate_log {
        return Err(());
      }
      unsafe_truncate_log = true;
    } else if token.eq_ignore_ascii_case(b"ASYNC") {
      if sync_mode.is_some() {
        return Err(());
      }
      sync_mode = Some(true);
    } else if token.eq_ignore_ascii_case(b"SYNC") {
      if sync_mode.is_some() {
        return Err(());
      }
      sync_mode = Some(false);
    } else {
      return Err(());
    }
  }
  Ok(FlushOptions {
    unsafe_truncate_log,
    async_flush: sync_mode == Some(true),
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_object_sub_cmd_as_str() {
    assert_eq!(ObjectSubCmd::Encoding.as_str(), "object|encoding");
    assert_eq!(ObjectSubCmd::Freq.as_str(), "object|freq");
    assert_eq!(ObjectSubCmd::Idletime.as_str(), "object|idletime");
    assert_eq!(ObjectSubCmd::Refcount.as_str(), "object|refcount");
  }
}
