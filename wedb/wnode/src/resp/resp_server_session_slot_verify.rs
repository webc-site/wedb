//! RESP 服务器会话槽位校验族（对标 libs/server/Resp/RespServerSessionSlotVerify.cs）
//!
//! 从 resp_server_session 外提的 impl RespServerSession 分部：数据命令、自定义命令、
//! 无应答三臂的集群槽位归属校验。判定逻辑与调用点原样迁移，重定向错误由集群切面
//! 直写会话输出缓冲；本文件不改语义，仅承接 C# 同名 partial 的模块边界。

use std::sync::OnceLock;

use wcustom::CommandType;
use wresp::{
  catalog::{
    SimpleRespKeySpec, SimpleRespKeySpecBeginSearch, SimpleRespKeySpecFindKeys, normalize_for_acls,
    try_get_simple_resp_command_info,
  },
  command::{RespCommand, is_data_command, is_read_only},
  key_spec::KeySpecificationFlags,
};
use wtxn::TxnState;

use crate::{
  cluster_session::{ClusterSlotVerificationInput, SlotVerifyGate},
  resp::resp_server_session::RespServerSession,
};

/// 自定义命令单键规格（C# RespServerSessionSlotVerify.cs:14
/// CustomCommandSingleKeySpec：index 型 begin_search=1 + range 型
/// find_keys(step=1, last=0)——单个键位于参数下标 1）。进程级静态，
/// OnceLock 惰性构造规避 SimpleRespKeySpec 的 Vec 字段不可 const。
fn custom_command_single_key_spec() -> &'static [SimpleRespKeySpec] {
  static SPEC: OnceLock<[SimpleRespKeySpec; 1]> = OnceLock::new();
  SPEC.get_or_init(|| {
    [SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        keyword: Vec::new(),
        index: 1,
        is_index_type: true,
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_num_index: 0,
        first_key: 0,
        last_key_or_limit: 0,
        key_step: 1,
        is_range_type: true,
        is_range_limit_type: false,
      },
      flags: KeySpecificationFlags::empty(),
    }]
  })
}

impl RespServerSession {
  /// libs/server/Resp/RespServerSessionSlotVerify.cs:CanServeSlot
  ///
  /// 数据命令按命令键规格提取键位交集群切面校验槽位归属，MOVED/ASK 等
  /// 重定向错误由切面直写输出缓冲；非数据命令与无键规格命令放行。
  /// 事务运行态跳过（C# NetworkMultiKeySlotVerify 首行判定；事务键校验由
  /// 事务域承接）。自定义命令虽不在 IsDataCommand 内但仍触达用户键，按
  /// C# CanServeSlotForCustomCommand 以单键规格走同一校验。
  ///
  /// 迁出同级分部模块后，仅因 `resp_server_session` 主循环处的跨模块调用
  /// 把可见性放宽至 `pub(super)`，判定逻辑与 C# 私有臂行为一致。
  pub(super) fn can_serve_slot(&mut self, cmd: RespCommand) -> SlotVerifyGate {
    if self.txn_state == TxnState::Running {
      return SlotVerifyGate::Serve;
    }
    if !is_data_command(cmd) {
      // C# RespServerSessionSlotVerify.cs:34-38：自定义命令绕过 IsDataCommand
      // 但仍经 CustomCommandSingleKeySpec 校验，槽不属本节点回 MOVED
      if matches!(
        cmd,
        RespCommand::Customrawstringcmd | RespCommand::Customobjcmd
      ) {
        return self.can_serve_slot_for_custom_command();
      }
      return SlotVerifyGate::Serve;
    }
    let Some(info) = try_get_simple_resp_command_info(normalize_for_acls(cmd)) else {
      // C#：命令信息缺失（json 解析失败）时拒绝服务
      return SlotVerifyGate::Redirected;
    };
    if info.key_specs.is_empty() {
      return SlotVerifyGate::Serve;
    }
    let input = ClusterSlotVerificationInput {
      slot: self.active_db_slot(),
      key_specs: &info.key_specs,
      // BITOP 的操作参数被解析器吞掉，键下标需 -2 偏移（同子命令）
      is_sub_command: info.is_sub_command || cmd == RespCommand::Bitop,
      read_only: is_read_only(cmd),
      session_asking: self.session_asking,
      wait_for_stable_slot: matches!(
        cmd,
        RespCommand::Vadd | RespCommand::Vrem | RespCommand::Vsetattr
      ),
    };
    let Some(cluster) = &self.cluster_session else {
      return SlotVerifyGate::Serve;
    };
    let args_buf = self.collect_args();
    let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
    cluster.network_multi_key_slot_verify(&input, &args, &mut self.output)
  }

  /// libs/server/Resp/RespServerSessionSlotVerify.cs:CanServeSlotForCustomCommand
  ///
  /// 自定义命令（CustomRawStringCmd / CustomObjCmd）槽位校验：单键规格
  /// （参数下标 1）+ 解析期填入的当前命令 CommandType 判只读（C# 注释：
  /// cmd.IsReadOnly() 对枚举序越过 LastReadCommand 的自定义值不可用）。
  /// 解析器未填当前命令引用（正常流不应发生）时放行，与无键形态一致。
  fn can_serve_slot_for_custom_command(&mut self) -> SlotVerifyGate {
    let Some((_, custom)) = &self.current_custom_command else {
      return SlotVerifyGate::Serve;
    };
    let input = ClusterSlotVerificationInput {
      slot: self.active_db_slot(),
      key_specs: custom_command_single_key_spec(),
      is_sub_command: false,
      read_only: custom.command_type == CommandType::Read,
      session_asking: self.session_asking,
      wait_for_stable_slot: false,
    };
    let Some(cluster) = &self.cluster_session else {
      return SlotVerifyGate::Serve;
    };
    let args_buf = self.collect_args();
    let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
    cluster.network_multi_key_slot_verify(&input, &args, &mut self.output)
  }

  /// libs/server/Resp/RespServerSessionSlotVerify.cs:CanServeSlotNoResponse
  ///
  /// 校验当前解析态中的命令键是否可由本节点服务（不向 output 写重定向错误）。
  /// 仅在 cluster_session 存在时介入；返回 true 表示可服务，false 表示不可服务。
  pub fn can_serve_slot_no_response(&self, cmd: RespCommand) -> bool {
    let Some(cluster) = &self.cluster_session else {
      return true;
    };
    let Some(info) = try_get_simple_resp_command_info(normalize_for_acls(cmd)) else {
      return false;
    };
    if info.key_specs.is_empty() {
      return true;
    }
    let input = ClusterSlotVerificationInput {
      slot: self.active_db_slot(),
      key_specs: &info.key_specs,
      is_sub_command: info.is_sub_command || cmd == RespCommand::Bitop,
      read_only: is_read_only(cmd),
      session_asking: self.session_asking,
      wait_for_stable_slot: false,
    };
    let mut dummy = Vec::new();
    if self.parse_state.count == 1 {
      let key = self.parse_state.arg_in(&self.recv_buffer, 0);
      return cluster.network_multi_key_slot_verify(&input, &[key], &mut dummy)
        == SlotVerifyGate::Serve;
    }
    let args_buf = self.collect_args();
    let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
    cluster.network_multi_key_slot_verify(&input, &args, &mut dummy) == SlotVerifyGate::Serve
  }
}
