//! 基础命令（对标 libs/server/Resp/BasicCommands.cs 与 AdminCommands.cs）
//!
//! 目录化拆分：[`get`] 字符串读命令、[`set`] 字符串写命令、[`incr`] 数值自增自减、
//! [`ttl`] TTL 与时间戳换算；本文件保留服务端通用/管理命令（PING/ASKING/
//! ECHO/HELLO/COMMAND/CONFIG/FLUSHDB/FLUSHALL/MEMORY/OBJECT/ASYNC）。

use wresp::resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter};
mod get;
mod incr;
mod set;
pub mod slow;
mod ttl;

use std::{mem, sync::Arc};

use itoa::Buffer as ItoaBuffer;
use wbase::{num::strict_i32, time::now_ticks};
use wkv::{StoreResult, is_expired};
use wresp::{
  catalog::{
    SimpleRespKeySpec, try_get_resp_command_info_by_name, try_get_resp_commands_info,
    try_get_resp_commands_info_count, try_get_simple_resp_command_info,
  },
  check_args::{check_arg_count, unpack_args},
  cmd_strings::{self as cs, RESP_ERR_GENERIC, abort_with_error_message, write_raw},
  ext::{RespSliceExt, RespVecExt},
  key_spec::KeySpecificationFlags,
};
use wval::{GarnetObjectType, KeyTag};

pub(crate) use self::set::{RiWriteGate, apply_set_with_expiry, parse_set_options, ri_write_gate};
pub use self::{
  incr::IncrCmd,
  set::{SetCmd, SetOptions},
  ttl::{
    MAX_TIMESPAN_MILLISECONDS, MAX_TIMESPAN_SECONDS, MAX_UNIX_TIME_MILLISECONDS,
    MAX_UNIX_TIME_SECONDS,
  },
};
use super::{
  config_commands::ServerConfig,
  resp_server_session::RespServerSession,
  vector::vector_manager::{INDEX_SIZE_BYTES, VectorManager},
};
use crate::{
  key_spec::{extract_keys_and_flags_from_slice, extract_keys_from_slice},
  resp::objects::object_store_utils::envelope_heap_estimate,
  session_parse_state_extensions::try_get_client_name_bytes,
  storage::session::common::{
    TagRead, UserRead, read_envelope_sync, read_tag_sync, read_user_sync,
    ttl_sync::{meta_collection_type_of, read_adjudicated_tag_with_size, ttl_of_sync},
  },
};

/// OBJECT 子命令形态（对标 libs/server/Resp/Parser/RespCommand.cs:OBJECT_*）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectSubCmd {
  Encoding,
  Freq,
  Idletime,
  Refcount,
}

impl ObjectSubCmd {
  /// C# 错误文案中的子命令名（NetworkOBJECT 的 subCommandName 映射）
  const fn as_str(self) -> &'static str {
    match self {
      Self::Encoding => "object|encoding",
      Self::Freq => "object|freq",
      Self::Idletime => "object|idletime",
      // C# 默认分支即 refcount
      Self::Refcount => "object|refcount",
    }
  }
}

/// COMMAND GETKEYS[ANDFLAGS] 提取上下文
struct CommandKeysContext<'a> {
  cmd_args: &'a [&'a [u8]],
  key_specs: &'static [SimpleRespKeySpec],
  is_sub_command: bool,
}

impl RespServerSession {
  /// libs/server/Resp/BasicCommands.cs:NetworkPING / ArrayCommands.cs:NetworkArrayPING
  ///
  /// 零参：订阅会话 + RESP2 回 SUSCRIBE_PONG 整帧（`["pong",""]`，redis-py 等按
  /// 帧形态判 pong），否则回 +PONG；单参回显消息 bulk（不受订阅模式影响）；
  /// 多参报参数错误（C# ProcessBasicCommands 依 Count 分流两实现）
  pub fn network_ping(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
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
  pub fn network_asking(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    check_arg_count!(self.parse_state.count == 0, output, "ASKING");
    self.session_asking = 2;
    write_raw(output, cs::RESP_OK);
    Ok(true)
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
    if let Some(infos) = try_get_resp_commands_info(true) {
      if self.resp_protocol_version >= 3 {
        let mut writer = RespWriter::<_, Resp3>::new_ref_p(output);
        writer.write_array_length(infos.len());
        for info in infos.values() {
          info.to_resp_format(&mut writer);
        }
      } else {
        let mut writer = RespWriter::<_, Resp2>::new_ref(output);
        writer.write_array_length(infos.len());
        for info in infos.values() {
          info.to_resp_format(&mut writer);
        }
      }
    } else {
      write_raw(output, cs::RESP_EMPTYLIST);
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_COUNT
  pub fn network_command_count<'a, D: wdev::Device>(
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
  pub fn network_command_docs<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    if self.resp_protocol_version >= 3 {
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
      if let Some(cmds_docs) = super::resp_command_docs::try_get_resp_commands_docs(true) {
        writer.write_map_length(cmds_docs.len());
        for cmd_docs in cmds_docs.values() {
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
  pub fn network_command_info<'a, D: wdev::Device>(
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
    if self.resp_protocol_version >= 3 {
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
    let cmd = super::resp_commands_info_data::resp_command_from_cs_name(cmd_name);
    let mut simple_info = cmd.and_then(try_get_simple_resp_command_info);

    if let Some(info) = simple_info
      && info.is_parent
      && parse_state.len() >= 2
    {
      // C# 上游 TryGetSimpleCommandInfo（BasicCommands.cs:2044）只按首参单名
      // Enum.TryParse：`COMMAND GETKEYS OBJECT ENCODING k` 在 C# 落 OBJECT parent
      // （KeySpecifications 缺席）报 RESP_COMMAND_HAS_NO_KEY_ARGS；rust 有意组合
      // {parent}_{sub} 名再查子命令规格成功提取键，对齐 Redis 行为。子命令规格
      // 同样缺席的（如 CONFIG GET）两侧同报 no-key-args，无分叉。差分测试
      // command_getkeys_parent_sub_lookup（wnode/tests/resp_tests.rs）钉住两面。
      let sub_name = format!("{}_{}", cmd_name, parse_state[1].as_str_safe());
      if let Some(sub_cmd) = super::resp_commands_info_data::resp_command_from_cs_name(&sub_name)
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
  pub fn network_command_getkeys<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(ctx) = Self::prepare_command_keys_context(parse_state, output, "COMMAND|GETKEYS")
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
  pub fn network_command_getkeysandflags<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(ctx) =
      Self::prepare_command_keys_context(parse_state, output, "COMMAND|GETKEYSANDFLAGS")
    else {
      return Ok(true);
    };
    let pairs = extract_keys_and_flags_from_slice(ctx.cmd_args, ctx.key_specs, ctx.is_sub_command);
    output.write_resp_array_len(pairs.len());
    for (key, flags_byte) in pairs {
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      let flags = KeySpecificationFlags::from_bits_retain(u16::from(flags_byte));
      let count = flags.iter().count();
      if self.resp_protocol_version >= 3 {
        output.push(b'~');
        let mut buf = ItoaBuffer::new();
        output.extend_from_slice(buf.format(count).as_bytes());
        output.extend_from_slice(b"\r\n");
      } else {
        output.write_resp_array_len(count);
      }
      for flag in flags.iter_descriptions() {
        output.write_resp_bulk_string(flag.as_bytes());
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkECHO
  pub fn network_echo(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
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
  pub fn network_config_set<D: wdev::Device + 'static>(
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
  pub fn network_hello<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=6, output, "HELLO");
    let count = parse_state.len();

    let mut tmp_resp_protocol_version: Option<u8> = None;
    let mut auth_username: &[u8] = &[];
    let mut auth_password: &[u8] = &[];
    let mut tmp_client_name: Option<&str> = None;

    if count > 0 {
      let mut token_idx = 0usize;
      // 校验协议版本（C# TryGetInt 严格口径）
      let Some(local_resp_protocol_version) = strict_i32(parse_state[token_idx]) else {
        abort_with_error_message(output, cs::RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      token_idx += 1;

      if !(2..=3).contains(&local_resp_protocol_version) {
        abort_with_error_message(output, cs::RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION);
        return Ok(true);
      }
      tmp_resp_protocol_version = Some(local_resp_protocol_version as u8);

      while token_idx < count {
        let param = parse_state[token_idx];
        token_idx += 1;

        if param.eq_ignore_ascii_case(b"AUTH") {
          if count - token_idx < 2 {
            set::write_syntax_error_option(output, "HELLO", "AUTH");
            return Ok(true);
          }
          auth_username = parse_state[token_idx];
          auth_password = parse_state[token_idx + 1];
          token_idx += 2;
        } else if param.eq_ignore_ascii_case(b"SETNAME") {
          if count - token_idx < 1 {
            set::write_syntax_error_option(output, "HELLO", "SETNAME");
            return Ok(true);
          }
          let Some(name) = try_get_client_name_bytes(parse_state[token_idx]) else {
            abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_NAME);
            return Ok(true);
          };
          token_idx += 1;
          tmp_client_name = Some(name);
        } else {
          set::write_syntax_error_option(output, "HELLO", param.as_str_safe());
          return Ok(true);
        }
      }
    }

    self.process_hello_command(
      tmp_resp_protocol_version,
      auth_username,
      auth_password,
      tmp_client_name,
      store,
      output,
    )
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkMemoryUsage
  pub fn network_memory_usage<'a, D: wdev::Device>(
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
      let Some(samples) = strict_i32(parse_state[2]) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
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
      // 信封域磁盘候选 / TTL 须异步裁决：转慢路径闭环（exec_slow MemoryUsage）
      Ok(StoreResult::NotFound) | Ok(StoreResult::RecordOnDisk) => {}
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    // 升阶键（Meta 域命中）：C# 对任意存在键回值（HandleMemoryUsage），统计
    // 口径 = Meta 元记录物理尺寸下限；wbftree 树驻留页内存待 wkv 披露估算
    // API 后并项（树页冷热换入换出，非常驻，不误报）
    match read_adjudicated_tag_with_size(store, key, KeyTag::Meta, |_v, size| size as i64) {
      Ok(StoreResult::Success(size)) => output.write_resp_int(size),
      // 三域皆缺：再问向量索引登记表第四态（对标 C# HandleMemoryUsage 对
      // RecordType=VectorManager.RecordType 主存记录回 AllocatedSize——存活
      // 向量键 MEMORY USAGE 回正整数而非 nil）。rust 登记表条目为定长索引描述
      // 记录，回其字节尺寸下限；live HNSW 索引体由 VectorManager 以原生对象持
      // 有、不落入本 wkv 对象堆估算通道，故与 Meta 臂「树驻留页不计」同款口径
      // 下限声明；键真缺失仍回 nil（C# status != OK → WriteNull）。登记表键不入
      // wkv 值域，三域读对向量键恒 NotFound 而非 RecordOnDisk，慢路径无向量臂。
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

  /// libs/server/Resp/BasicCommands.cs:NetworkOBJECT
  pub fn network_object<'a, D: wdev::Device>(
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
      match sub_cmd {
        ObjectSubCmd::Encoding => output.write_resp_bulk_string(b"raw"),
        ObjectSubCmd::Refcount => output.write_resp_int(1),
        ObjectSubCmd::Idletime => output.write_resp_int(0),
        ObjectSubCmd::Freq => {
          abort_with_error_message(output, cs::RESP_ERR_OBJECT_FREQ_UNSUPPORTED);
        }
      }
      return Ok(true);
    }

    const ENCODING_SKIPLIST: &[u8] = b"skiplist";
    const ENCODING_QUICKLIST: &[u8] = b"quicklist";
    const ENCODING_HASHTABLE: &[u8] = b"hashtable";
    const ENCODING_RAW: &[u8] = b"raw";

    // 语义对照 UnifiedStore ReadMethods:HandleObjectEncoding：
    // String 域命中 → raw（值内容任意）；对象信封域命中按内层标签映射
    //（SortedSet=skiplist / List=quicklist / 其余=hashtable，默认对齐
    // C# ReadMethods.cs:67 `_ => CmdStrings.hashtable`）；升阶键（Meta 域命中）
    // 按 MetaValue.collection_type 同款映射（C# Reader 对任意对象记录统一判定）；
    // 过期键与缺失键一致回 nil（$-1\r\n）
    let (encoding, degrade) = match read_user_sync(store, key, |_| ()) {
      Ok(UserRead::Hit(())) => (Some(ENCODING_RAW), false),
      Ok(UserRead::WrongType) => match read_envelope_sync(store, key, |raw| {
        raw
          .first()
          .copied()
          .and_then(GarnetObjectType::from_u8)
          .map_or(ENCODING_HASHTABLE, |obj_type| match obj_type {
            GarnetObjectType::SortedSet => ENCODING_SKIPLIST,
            GarnetObjectType::List => ENCODING_QUICKLIST,
            _ => ENCODING_HASHTABLE,
          })
      }) {
        Ok(TagRead::Hit(enc)) => (Some(enc), false),
        Ok(TagRead::Missing) => {
          // 升阶键：Meta 元记录按 collection_type 映射编码名
          match read_tag_sync(store, key, KeyTag::Meta, meta_collection_type_of) {
            Ok(TagRead::Hit(Some(obj_type))) => (
              Some(match obj_type {
                GarnetObjectType::SortedSet => ENCODING_SKIPLIST,
                GarnetObjectType::List => ENCODING_QUICKLIST,
                _ => ENCODING_HASHTABLE,
              }),
              false,
            ),
            Ok(TagRead::Hit(None)) | Ok(TagRead::Missing) => (None, false),
            Ok(TagRead::Deferred) | Err(_) => (None, true),
          }
        }
        Ok(TagRead::Deferred) | Err(_) => (None, true),
      },
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
    match (encoding, sub_cmd) {
      (Some(encoding), sub_cmd) => match sub_cmd {
        ObjectSubCmd::Encoding => output.write_resp_bulk_string(encoding),
        ObjectSubCmd::Refcount => output.write_resp_int(1),
        ObjectSubCmd::Idletime => output.write_resp_int(0),
        ObjectSubCmd::Freq => {
          abort_with_error_message(output, cs::RESP_ERR_OBJECT_FREQ_UNSUPPORTED)
        }
      },
      // C# 键缺失（status != OK）一律回 nil
      (None, _) => output.write_resp_null_ver(self.resp_protocol_version),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkOBJECTHELP
  pub fn network_objecthelp<'a, D: wdev::Device>(
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
  pub(crate) fn apply_async_param(
    &mut self,
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
  pub fn network_async<'a, D: wdev::Device>(
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
  pub fn process_hello_command<'a, D: wdev::Device>(
    &mut self,
    resp_protocol_version: Option<u8>,
    username: &[u8],
    password: &[u8],
    client_name: Option<&str>,
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# 默认 NoAuth 认证器 Authenticate 恒 false → 带 AUTH 的 HELLO 报
    // WRONGPASS（state 版本承接，认证失败已写错误应答）；ACL 档命名用户
    // 经底层存储点查（存储为唯一真源），回落内存认证器
    let acl_store = super::acl_store::AclStore::new(store.session);
    self.process_hello_command_state(
      resp_protocol_version,
      username,
      password,
      client_name,
      Some(&acl_store),
      output,
    );
    // 冷租户/冷库挂起面：HELLO 应答字节已组入 output，整体移交 SlowWait——
    // 点查装载并重放切库后原样应答，挂起期间本批停止消费
    if let Some((ns, db)) = self.take_cold_ctx() {
      let reply = mem::take(output);
      self.park_cold_context_load(store.session.store(), ns, db, reply);
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
  let mut opts = FlushOptions {
    unsafe_truncate_log: false,
    async_flush: false,
  };
  let mut sync = false;
  for token in parse_state {
    if token.eq_ignore_ascii_case(b"UNSAFETRUNCATELOG") {
      if opts.unsafe_truncate_log {
        return Err(());
      }
      opts.unsafe_truncate_log = true;
    } else if token.eq_ignore_ascii_case(b"ASYNC") {
      if sync || opts.async_flush {
        return Err(());
      }
      opts.async_flush = true;
    } else if token.eq_ignore_ascii_case(b"SYNC") {
      if sync || opts.async_flush {
        return Err(());
      }
      sync = true;
    } else {
      return Err(());
    }
  }
  Ok(opts)
}
