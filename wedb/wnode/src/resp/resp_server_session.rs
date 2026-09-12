//! RESP 服务器会话（对标 libs/server/Resp/RespServerSession.cs:RespServerSession）
//!
//! C# 会话直接持有网络发送器 / Tsavorite 上下文 / 事务管理器；Rust 侧这些面
//! 分别由并行域（wkv 会话纪元 / transaction / cluster）承载，本结构承接会话
//! 状态本体：Id / 端点 / CreationTicks / RESP 协议版本 / clientName·lib-* /
//! useAsync / 数据库会话映射 / 订阅与事务模式 / 延迟与会话指标 / 接收缓冲
//! 解析游标 / 输出缓冲，以及 C# 的分派 · 发送 · 数据库切换方法族。
//!
//! 输出缓冲模型：C# 的 `dcurr/dend` 指针游标对应 `output: Vec<u8>` 的
//! `len/capacity`；`Send` 对应 `take_output`。

use std::{
  mem, slice,
  sync::{Arc, atomic::AtomicU64},
};

use wbase::time::now_ms;
use wlua::{
  LuaCommands, LuaOptions, LuaSessionContext, ScratchBufferNetworkSender, ScriptingApi,
  SessionScriptCache, StoreScriptCache,
};
use wresp::{
  RespVecExt, cmd_strings as cs,
  cmd_strings::{RESP_WRONGPASS_INVALID_USERNAME_PASSWORD, write_map_len_resp2},
};

use super::parser::resp_command::{MruCommandCache, is_allowed_in_subscription_mode};
use crate::{
  metrics::{
    garnet_session_metrics::GarnetSessionMetrics,
    latency::{
      garnet_latency_metrics_session::GarnetLatencyMetricsSession,
      latency_metrics_type::LatencyMetricsType,
    },
  },
  servers::server_options::DEFAULT_RESP_VERSION,
  session_parse_state::SessionParseState,
  types::RespCommand,
};

/// libs/host/GarnetServer.cs:RedisProtocolVersion（HELLO 应答 version 字段）
pub const REDIS_PROTOCOL_VERSION: &str = "7.4.3";

use wtxn::TxnState;

/// 连接保护选项（libs/server/Auth/ConnectionProtectionOption.cs）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionProtectionOption {
  /// 允许
  Yes,
  /// 拒绝
  No,
  /// 仅本地连接
  Local,
}

/// 会话侧数据库会话槽（C# GarnetDatabaseSession 的会话状态投影）
///
/// Rust 侧 StorageSession 与 wkv 批处理会话生命周期绑定，无法驻留会话结构；
/// 槽位承接 `ExpandableMap<GarnetDatabaseSession>` 的簿记语义（创建 / 获取 /
/// 交换 / 激活），存储操作本体经活跃库句柄在命令层完成。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseSessionSlot {
  /// 数据库 ID
  pub id: i32,
  /// 槽位创建时刻（毫秒 tick）
  pub created_ticks: i64,
}

/// 会话构造选项（C# 构造函数入参的会话状态子集）
#[derive(Debug, Clone)]
pub struct RespServerSessionOptions {
  /// 是否允许多库（C# storeWrapper.serverOptions.AllowMultiDb）
  pub allow_multi_db: bool,
  /// 最大库数（EnableCluster 时为 2）
  pub max_databases: i32,
  /// EnableDebugCommand 配置
  pub enable_debug_command: ConnectionProtectionOption,
  /// EnableModuleCommand 配置
  pub enable_module_command: ConnectionProtectionOption,
  /// 延迟监视开关（C# serverOptions.LatencyMonitor）
  pub latency_monitor: bool,
  /// 指标采样频率（> 0 时启用会话指标，C# MetricsSamplingFrequency）
  pub metrics_sampling_frequency: bool,
  /// 默认用户句柄名（C# accessControlList.GetDefaultUserHandle()）
  pub default_user: String,
  /// 是否启用 Lua 脚本（C# storeWrapper.serverOptions.EnableLua + enableScripts）
  pub enable_lua: bool,
}

impl Default for RespServerSessionOptions {
  fn default() -> Self {
    Self {
      allow_multi_db: false,
      max_databases: 16,
      enable_debug_command: ConnectionProtectionOption::No,
      enable_module_command: ConnectionProtectionOption::No,
      latency_monitor: false,
      metrics_sampling_frequency: false,
      default_user: "default".to_string(),
      enable_lua: false,
    }
  }
}

/// 自定义命令引用（C# currentCustomTransaction / CustomProcedure /
/// CustomRawStringCommand / CustomObjCmd 的会话侧投影；注册表本体由
/// custom 域承载）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomCommandRef {
  /// 命令名
  pub name: String,
  /// 命令 ID
  pub id: u16,
  /// arity（0 = 不校验；负值 = 至少 -arity-1 个参数）
  pub arity: i32,
}

/// 命令分派器静态分发枚举（消除堆分配与虚表开销）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandDispatcher {
  /// 测试/基准空操作分派器
  Nop,
  /// 自定义命令分派器
  Custom,
}

impl CommandDispatcher {
  /// 分派一条命令
  #[inline]
  pub fn dispatch(&mut self, session: &mut RespServerSession, cmd: RespCommand, _args: &[&[u8]]) {
    match self {
      Self::Nop => {
        if cmd == RespCommand::Get {
          session.write_direct_large(b"+OK\r\n");
        }
      }
      Self::Custom => {
        session.write_direct_large(b"CUSTOM\r\n");
      }
    }
  }

  /// 自定义命令分派
  #[inline]
  pub fn dispatch_custom(
    &mut self,
    session: &mut RespServerSession,
    _kind: RespCommand,
    _custom: &CustomCommandRef,
    _args: &[&[u8]],
  ) {
    match self {
      Self::Nop => {}
      Self::Custom => {
        session.write_direct_large(b"CUSTOM\r\n");
      }
    }
  }
}

/// 命令分派钩子：RESP 命令层（basic_commands / array_commands 等命令文件）
/// 经此接入会话主循环。纯会话测试可不挂载（None）。
pub trait RespCommandDispatch: Send {
  /// 分派一条命令；返回处理结果（true = 已按 fast 命令处理）
  fn dispatch(&mut self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]);
}

/// RESP 服务器会话
pub struct RespServerSession {
  /// 会话标识（CLIENT ID / CLIENT INFO 使用；C# Id）
  pub id: i64,
  /// 会话创建时刻（毫秒 tick；C# CreationTicks = Environment.TickCount64）
  pub creation_ticks: i64,
  /// 对端端点（C# networkSender.RemoteEndpointName；无网络发送器为空串）
  pub remote_endpoint: String,
  /// 本地端点（C# networkSender.LocalEndpointName）
  pub local_endpoint: String,
  /// ASYNC 命令切换的异步处理模式（C# useAsync，NetworkASYNC 置位）
  pub use_async: bool,

  /// RESP 协议版本（RESP2 默认；HELLO \[3\] 升级；C# respProtocolVersion）
  pub resp_protocol_version: u8,
  /// 客户端名（CLIENT SETNAME / HELLO SETNAME；C# clientName）
  pub client_name: Option<String>,
  /// 客户端库名（CLIENT SETINFO LIB-NAME）
  pub client_lib_name: Option<String>,
  /// 客户端库版本（CLIENT SETINFO LIB-VER）
  pub client_lib_version: Option<String>,

  /// 当前认证用户（C# _userHandle；ACL 域为并行转写，此持用户名）
  pub user_handle: Option<String>,
  /// 认证器是否支持认证（C# _authenticator.CanAuthenticate；NoAuth 为 false）
  pub authenticator_can_authenticate: bool,

  /// ASKING 跳过计数（C# SessionAsking）
  pub session_asking: u8,
  /// 是否为只读会话（C# readOnlySession）
  pub read_only_session: bool,
  /// 当前活跃库编号（C# activeDatabaseId）
  pub active_db_id: i32,
  /// 是否允许切换非 0 库（C# allowMultiDb）
  pub allow_multi_db: bool,
  /// 数据库会话槽位表（下标即 DB 编号；C# databaseSessions）
  database_sessions: Vec<Option<DatabaseSessionSlot>>,
  /// 副本一致读会话是否开启（C# IsConsistentReadSessionActive）
  pub is_consistent_read_session_active: bool,
  /// 是否处于发布订阅模式（C# isSubscriptionSession）
  pub is_subscription_session: bool,
  /// 事务状态镜像（C# txnManager.state 的会话侧视图）
  pub txn_state: TxnState,
  /// 本批消息是否已标记 AOF 阻塞等待（C# waitForAofBlocking）
  pub wait_for_aof_blocking: bool,
  /// 本批是否含慢命令（NET_RS 直方图分桶；C# containsSlowCommand）
  pub contains_slow_command: bool,
  /// 命令执行期写出的错误标记（CommandStats 失败判定；C# commandErrorWritten）
  pub command_error_written: bool,
  /// 会话是否请求关闭（CLIENT KILL / TryKill；C# networkSender.TryClose 语义）
  pub kill_requested: bool,
  /// 会话是否待释放（QUIT；C# toDispose）
  pub to_dispose: bool,

  /// 会话指标（C# sessionMetrics；采样关闭时为 None）
  pub session_metrics: Option<GarnetSessionMetrics>,
  /// 延迟指标（C# LatencyMetrics；监视关闭时为 None；监视器迭代时钟由
  /// 延迟指标实例内部持有 —— C# LatencyMetrics._monitorIterations）
  latency_metrics: Option<Arc<GarnetLatencyMetricsSession>>,

  /// 解析态（C# parseState）
  pub parse_state: SessionParseState,
  /// 接收缓冲（C# recvBufferPtr 固定接收缓冲的托管等价；parser 分片读写）
  pub recv_buffer: Vec<u8>,
  /// 已接收字节数（C# bytesRead；parser 分片读写）
  pub bytes_read: usize,
  /// 读游标（C# readHead；成功解析后停在命令负载起点）
  pub read_head: usize,
  /// 当前命令尾游标（C# endReadHead）
  pub end_read_head: usize,
  /// 协议违规哨兵：畸形 RESP 帧（C# RespParsingException 抛出即断连）。
  /// 解析层置位、[`Self::try_consume_messages`] 消费回传 None，由调用方按
  /// 协议错误关闭连接
  pub parse_violation: bool,
  /// 输出缓冲（C# networkSender 响应对象 + dcurr/dend 游标的托管等价；分片写）
  pub output: Vec<u8>,
  /// 已发送字节暂存（C# SendResponse 直写网络；托管面留档供测试/脚本回读）
  sent: Vec<u8>,
  /// 累计冲洗字节数（Send 累计，测试断言用）
  flushed_bytes: u64,

  /// 当前待执行自定义命令（C# currentCustomTransaction / Procedure 等共用槽）
  pub current_custom_command: Option<(RespCommand, CustomCommandRef)>,
  /// MRU 命令缓存（C# _cachedCmd0/1；resp_command 域维护）
  pub(crate) mru_cache: MruCommandCache,
  /// 会话脚本缓存（EnableLua 时创建；C# sessionScriptCache）
  pub(crate) session_script_cache: Option<SessionScriptCache>,
  /// 全局脚本缓存（C# storeWrapper.storeScriptCache，进程级共享）
  pub(crate) store_script_cache: Arc<StoreScriptCache>,
  /// 内建命令分派器（宿主按持有存储面注入；None 时命令解析/门控仍闭环）
  command_dispatch: Option<CommandDispatcher>,

  /// EnableDebugCommand 镜像（C# storeWrapper.serverOptions.EnableDebugCommand）
  connection_protection_debug: ConnectionProtectionOption,
  /// EnableModuleCommand 镜像
  connection_protection_module: ConnectionProtectionOption,
}

impl RespServerSession {
  /// libs/server/Resp/RespServerSession.cs:RespServerSession（构造）
  ///
  /// 创建默认库会话（DB 0）并置为活跃；认证默认用户（C# 构造尾部
  /// AuthenticateUser(defaultUser)）。
  pub fn new(id: i64, options: RespServerSessionOptions) -> Self {
    let max_slots = options.max_databases.max(1) as usize;
    let mut database_sessions: Vec<Option<DatabaseSessionSlot>> =
      (0..max_slots).map(|_| None).collect();
    // 创建默认 DB 会话并注册（C# CreateDatabaseSession(0) + TrySetValue）
    database_sessions[0] = Some(DatabaseSessionSlot {
      id: 0,
      created_ticks: session_now_ms(),
    });

    let latency_metrics = options.latency_monitor.then(|| {
      Arc::new(GarnetLatencyMetricsSession::new(
        Arc::new(AtomicU64::new(0)),
        GarnetLatencyMetricsSession::DEFAULT_LATENCY_TYPES,
      ))
    });

    let mut session = Self {
      id,
      creation_ticks: session_now_ms(),
      remote_endpoint: String::new(),
      local_endpoint: String::new(),
      use_async: false,
      resp_protocol_version: DEFAULT_RESP_VERSION,
      client_name: None,
      client_lib_name: None,
      client_lib_version: None,
      user_handle: None,
      authenticator_can_authenticate: false,
      session_asking: 0,
      read_only_session: false,
      active_db_id: 0,
      allow_multi_db: options.allow_multi_db,
      database_sessions,
      is_consistent_read_session_active: false,
      is_subscription_session: false,
      txn_state: TxnState::None,
      wait_for_aof_blocking: false,
      contains_slow_command: false,
      command_error_written: false,
      kill_requested: false,
      to_dispose: false,
      session_metrics: options
        .metrics_sampling_frequency
        .then(GarnetSessionMetrics::default),
      latency_metrics,
      parse_state: SessionParseState::new(),
      recv_buffer: Vec::new(),
      bytes_read: 0,
      read_head: 0,
      end_read_head: 0,
      parse_violation: false,
      output: Vec::with_capacity(1 << 16),
      sent: Vec::new(),
      flushed_bytes: 0,
      current_custom_command: None,
      connection_protection_debug: options.enable_debug_command,
      connection_protection_module: options.enable_module_command,
      mru_cache: Default::default(),
      session_script_cache: options.enable_lua.then(SessionScriptCache::default),
      store_script_cache: Arc::new(StoreScriptCache::default()),
      command_dispatch: None,
    };
    session.authenticate_user(options.default_user.as_bytes(), &[]);
    session
  }

  /// 注入内建命令分派器（宿主存储面接入点；redis.call 经同一分派闭环）
  pub fn set_command_dispatch(&mut self, dispatch: CommandDispatcher) {
    self.command_dispatch = Some(dispatch);
  }

  /// 经注入分派器分派（先取后放回，解除 self 双重借用）
  fn dispatch_via_hook(&mut self, cmd: RespCommand, args: &[&[u8]]) {
    let mut hook = self.command_dispatch;
    if let Some(dispatch) = hook.as_mut() {
      dispatch.dispatch(self, cmd, args);
    }
    self.command_dispatch = hook;
  }

  /// libs/server/Resp/RespServerSession.cs:GetLatencyMetrics
  pub fn get_latency_metrics(&self) -> Option<Arc<GarnetLatencyMetricsSession>> {
    self.latency_metrics.clone()
  }

  /// libs/server/Resp/RespServerSession.cs:ResetLatencyMetrics
  pub fn reset_latency_metrics(&self, latency_event: LatencyMetricsType) {
    if let Some(metrics) = &self.latency_metrics {
      metrics.reset(latency_event);
    }
  }

  /// libs/server/Resp/RespServerSession.cs:ResetAllLatencyMetrics
  pub fn reset_all_latency_metrics(&self) {
    if let Some(metrics) = &self.latency_metrics {
      metrics.reset_all();
    }
  }

  /// libs/server/Resp/RespServerSession.cs:GetDatabaseSessionsSnapshot
  ///
  /// 全部已创建的数据库会话（按库 ID 升序）
  pub fn get_database_sessions_snapshot(&self) -> Vec<DatabaseSessionSlot> {
    self.database_sessions.iter().flatten().cloned().collect()
  }

  /// libs/server/Resp/RespServerSession.cs:SetUserHandle
  pub fn set_user_handle(&mut self, user_handle: &str) {
    self.user_handle = Some(user_handle.to_string());
  }

  /// libs/server/Resp/RespServerSession.cs:UpdateRespProtocolVersion
  pub fn update_resp_protocol_version(&mut self, resp_protocol_version: u8) {
    self.resp_protocol_version = resp_protocol_version;
  }

  /// libs/server/Resp/RespServerSession.cs:AuthenticateUser
  ///
  /// C# 依认证器结果回退默认用户；NoAuth 认证器 CanAuthenticate = false →
  /// 恒置默认用户且返回 false（rust 认证器域为并行转写，按 NoAuth 语义承接）
  /// 保留 _username 与 _password 形参以对标 AuthenticateUser 接口契约
  pub fn authenticate_user(&mut self, _username: &[u8], _password: &[u8]) -> bool {
    let success = self.authenticator_can_authenticate;
    if !self.authenticator_can_authenticate {
      // 不支持认证的认证器直接落到默认用户（C# GetDefaultUserHandle 分支）
      if self.user_handle.is_none() {
        self.user_handle = Some("default".to_string());
      }
    }
    success && self.authenticator_can_authenticate
  }

  /// libs/server/Resp/RespServerSession.cs:CanRunDebug
  pub fn can_run_debug(&self) -> bool {
    can_run_with_protection(self.enable_debug_command(), self.is_local_connection())
  }

  /// libs/server/Resp/RespServerSession.cs:CanRunModule
  pub fn can_run_module(&self) -> bool {
    can_run_with_protection(self.enable_module_command(), self.is_local_connection())
  }

  /// EnableDebugCommand 配置视图（C# storeWrapper.serverOptions 选项承接）
  fn enable_debug_command(&self) -> ConnectionProtectionOption {
    self.connection_protection_debug
  }

  /// EnableModuleCommand 配置视图
  fn enable_module_command(&self) -> ConnectionProtectionOption {
    self.connection_protection_module
  }

  /// C# networkSender.IsLocalConnection：对端为本地回环 / Unix 套接字
  pub fn is_local_connection(&self) -> bool {
    self.remote_endpoint.starts_with("127.0.0.1")
      || self.remote_endpoint.starts_with("[::1]")
      || self.remote_endpoint.starts_with("unix:")
  }

  /// libs/server/Resp/RespServerSession.cs:TryConsumeMessages
  ///
  /// 解析并分派接收缓冲中的全部完整命令，返回已消费字节数（C# 返回 readHead）。
  /// 分派经 [`RespCommandDispatch`] 钩子；解析错误以 None 表达（C# 抛
  /// RespParsingException 并断连，rust 由调用方按协议错误处置断连）。
  pub fn try_consume_messages(&mut self, req_buffer: &[u8]) -> Option<usize> {
    self.recv_buffer.clear();
    self.recv_buffer.extend_from_slice(req_buffer);
    self.bytes_read = self.recv_buffer.len();
    self.read_head = 0;

    self.enter_and_get_response_object();
    self.process_messages();
    // 协议违规（C# RespParsingException 传播出 ProcessMessages → 连接关闭）：
    // 不回退游标、不计消费字节，直接以 None 表达致命错误
    if self.parse_violation {
      self.parse_violation = false;
      self.exit_and_return_response_object();
      return None;
    }
    let consumed = self.read_head;
    self.exit_and_return_response_object();

    if let Some(metrics) = &mut self.session_metrics {
      metrics.incr_total_net_input_bytes(consumed as u64);
    }
    Some(consumed)
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessMessages
  ///
  /// 主循环：解析 → 权限/订阅模式/事务门 → 分派 → 指标。rust 侧 ACL 与
  /// 集群槽位校验由并行域提供，门控当前仅承载订阅模式（C#
  /// IsAllowedInSubscriptionMode 集合）与事务状态；命令未完整到达时双游标
  /// 回退到本轮起点（C# `endReadHead = readHead = _origReadHead`）。
  pub fn process_messages(&mut self) {
    let mut orig_read_head = self.read_head;

    while self.bytes_read.saturating_sub(self.read_head) >= 4 {
      // 解析命令；未完整到达则回退双游标本轮起点并跳出（C# commandReceived）；
      // 协议违规（C# RespParsingException）保持游标原样跳出，连接由上层关闭
      let cmd = match self.parse_command() {
        Some(cmd) => cmd,
        None => {
          if self.parse_violation {
            break;
          }
          self.read_head = orig_read_head;
          self.end_read_head = orig_read_head;
          break;
        }
      };

      if cmd != RespCommand::Invalid {
        // RESP2 订阅模式仅放行 (P|S)SUBSCRIBE/(P|S)UNSUBSCRIBE/PING/QUIT/RESET
        //（libs/server/Resp/Parser/RespCommand.cs:IsAllowedInSubscriptionMode）
        if self.is_subscription_session
          && self.resp_protocol_version == 2
          && !is_allowed_in_subscription_mode(cmd)
        {
          // 对标 libs/server/Resp/CmdStrings.cs:GenericPubSubCommandNotAllowed 与 PR #1669
          // 本库错误文案规范为：ERR {name} command not allowed while in subscribe mode
          let name = super::resp_commands_info_data::resp_command_to_cs_name(cmd);
          self.write_error_response(&format!(
            "ERR {name} command not allowed while in subscribe mode"
          ));
        } else {
          // C# 分派链：ProcessBasicCommands → ProcessArrayCommands →
          // ProcessOtherCommands（事务入队/直通形态由分派域承载）
          self.process_basic_commands(cmd);
        }

        if let Some(metrics) = &mut self.session_metrics {
          metrics.incr_total_commands_processed(1);
          if self.command_error_written {
            self.command_error_written = false;
          }
        }
      } else {
        self.contains_slow_command = true;
      }

      // 推进游标处理下一条命令（C# _origReadHead = readHead = endReadHead）
      self.read_head = self.end_read_head;
      orig_read_head = self.read_head;
      if self.session_asking != 0 {
        self.session_asking -= 1;
      }
    }

    self.flush_if_pending();
  }

  /// libs/server/Resp/RespServerSession.cs:EnterAndGetResponseObject
  ///
  /// 拿取响应对象（rust 托管缓冲：进入会话输出期）
  pub fn enter_and_get_response_object(&mut self) {
    self.output.clear();
  }

  /// libs/server/Resp/RespServerSession.cs:ExitAndReturnResponseObject
  pub fn exit_and_return_response_object(&mut self) {
    // C# 归还响应对象并将 dcurr/dend 清零；托管缓冲保留待复用
  }

  /// libs/server/Resp/RespServerSession.cs:SetTransactionMode
  pub fn set_transaction_mode(&mut self, enable: bool) {
    self.txn_state = if enable {
      wtxn::TxnState::Running
    } else {
      wtxn::TxnState::None
    };
  }

  /// libs/server/Resp/RespServerSession.cs:MakeUpperCase
  ///
  /// 就地大写化首条命令名；返回是否发生改写。C# 位技巧快路径按
  /// "常见命令已大写" 假设跳过全扫描，语义保留。
  pub fn make_upper_case(&mut self, ptr: usize, len: usize) -> bool {
    let buffer = &mut self.recv_buffer;
    let end = (ptr + len).min(buffer.len());
    // 常见场景：命令名已全大写 → 不改写（返回 false）
    let mut changed = false;
    let mut i = ptr;
    while i < end {
      if buffer[i] > 64 {
        // 找到命令名起点
        while i < end && buffer[i] > 32 && buffer[i] < 123 {
          if buffer[i] > 96 {
            buffer[i] -= 32;
            changed = true;
          }
          i += 1;
        }
        return changed;
      }
      i += 1;
    }
    false
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessBasicCommands
  ///
  /// fast 命令族分派（WARNING: 仅 @fast 命令，慢命令走 OtherCommands）。
  /// 命令实现位于 resp 命令文件（并行域），经 [`RespCommandDispatch`] 钩子
  /// 接入；PING 的零参分支在会话侧闭环。
  pub fn process_basic_commands(&mut self, cmd: RespCommand) -> bool {
    if cmd == RespCommand::Ping {
      if self.parse_state.count == 0 {
        // C# NetworkPING：+PONG
        self.output.extend_from_slice(b"+PONG\r\n");
        return true;
      }
      if self.parse_state.count == 1 {
        // C# NetworkArrayPING: bulk string message
        let msg = self.parse_state.get_arg_slice_by_ref(0).as_slice();
        self.output.push(b'$');
        let mut b = itoa::Buffer::new();
        self
          .output
          .extend_from_slice(b.format(msg.len()).as_bytes());
        self.output.extend_from_slice(b"\r\n");
        self.output.extend_from_slice(msg);
        self.output.extend_from_slice(b"\r\n");
        return true;
      }
      self.abort_wrong_num_args("PING");
      return true;
    }
    if cmd == RespCommand::Asking {
      if self.parse_state.count != 0 {
        self.abort_wrong_num_args("ASKING");
        return true;
      }
      self.session_asking = 2;
      self.output.extend_from_slice(b"+OK\r\n");
      return true;
    }
    if cmd == RespCommand::Quit {
      self.to_dispose = true;
      self.output.extend_from_slice(b"+OK\r\n");
      return true;
    }
    if cmd == RespCommand::Readonly {
      self.read_only_session = true;
      self.output.extend_from_slice(b"+OK\r\n");
      return true;
    }
    if cmd == RespCommand::Readwrite {
      self.read_only_session = false;
      self.output.extend_from_slice(b"+OK\r\n");
      return true;
    }
    // C# 链式回退：fast 表未命中的命令继续走 array → other 分派链
    self.process_array_commands(cmd)
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessArrayCommands
  pub fn process_array_commands(&mut self, cmd: RespCommand) -> bool {
    // C# 链式回退末端：未归类命令走 other（慢命令）分派
    self.process_other_commands(cmd)
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessOtherCommands
  ///
  /// 慢命令族分派（此处可安全放 @slow 命令；C# containsSlowCommand = true）
  pub fn process_other_commands(&mut self, cmd: RespCommand) -> bool {
    self.contains_slow_command = true;
    // Lua 脚本族（C# NetworkEVAL / NetworkEVALSHA / NetworkScript*）
    if matches!(
      cmd,
      RespCommand::Eval
        | RespCommand::Evalsha
        | RespCommand::ScriptExists
        | RespCommand::ScriptFlush
        | RespCommand::ScriptLoad
    ) {
      return self.run_lua_command(cmd);
    }
    if cmd == RespCommand::ClientId && self.parse_state.count != 0 {
      // C# NetworkCLIENTID 的参数校验在会话侧（AbortWithWrongNumberOfArguments）
      self.abort_wrong_num_args("client|id");
      return true;
    }
    if cmd == RespCommand::ClientId {
      // C# TryWriteInt64(Id)
      self.output.push(b':');
      let mut buffer = itoa::Buffer::new();
      self
        .output
        .extend_from_slice(buffer.format(self.id).as_bytes());
      self.output.extend_from_slice(b"\r\n");
      return true;
    }
    if cmd == RespCommand::Echo {
      if self.parse_state.count != 1 {
        self.abort_wrong_num_args("ECHO");
        return true;
      }
      let msg = self.parse_state.get_arg_slice_by_ref(0).as_slice();
      self.output.push(b'$');
      let mut b = itoa::Buffer::new();
      self
        .output
        .extend_from_slice(b.format(msg.len()).as_bytes());
      self.output.extend_from_slice(b"\r\n");
      self.output.extend_from_slice(msg);
      self.output.extend_from_slice(b"\r\n");
      return true;
    }
    if cmd == RespCommand::Time {
      if self.parse_state.count != 0 {
        self.abort_wrong_num_args("TIME");
        return true;
      }
      let now = coarsetime::Clock::now_since_epoch();
      let secs = now.as_secs();
      let usecs = (now.as_nanos() % 1_000_000_000) / 1000;
      let mut b1 = itoa::Buffer::new();
      let s_str = b1.format(secs);
      let mut b2 = itoa::Buffer::new();
      let us_str = b2.format(usecs);
      let mut b3 = itoa::Buffer::new();
      let slen_str = b3.format(s_str.len());
      let mut b4 = itoa::Buffer::new();
      let uslen_str = b4.format(us_str.len());
      self.output.extend_from_slice(b"*2\r\n$");
      self.output.extend_from_slice(slen_str.as_bytes());
      self.output.extend_from_slice(b"\r\n");
      self.output.extend_from_slice(s_str.as_bytes());
      self.output.extend_from_slice(b"\r\n$");
      self.output.extend_from_slice(uslen_str.as_bytes());
      self.output.extend_from_slice(b"\r\n");
      self.output.extend_from_slice(us_str.as_bytes());
      self.output.extend_from_slice(b"\r\n");
      return true;
    }
    if cmd == RespCommand::Async {
      self.output.extend_from_slice(b"+OK\r\n");
      return true;
    }
    if cmd == RespCommand::ClientInfo {
      let args = self.collect_arg_slices();
      let mut out = mem::take(&mut self.output);
      if let Err(err) = self.network_clientinfo(&args, &mut out) {
        log::warn!("client info error: {err}");
      }
      self.output = out;
      return true;
    }
    if cmd == RespCommand::ClientList {
      let mut out = mem::take(&mut self.output);
      if let Err(err) = self.network_clientlist(&mut out) {
        log::warn!("client list error: {err}");
      }
      self.output = out;
      return true;
    }
    if cmd == RespCommand::ClientKill {
      let mut out = mem::take(&mut self.output);
      if let Err(err) = self.network_clientkill(&mut out) {
        log::warn!("client kill error: {err}");
      }
      self.output = out;
      return true;
    }
    if cmd == RespCommand::ClientGetname {
      let args = self.collect_arg_slices();
      let mut out = mem::take(&mut self.output);
      if let Err(err) = self.network_clientgetname(&args, &mut out) {
        log::warn!("client getname error: {err}");
      }
      self.output = out;
      return true;
    }
    if cmd == RespCommand::ClientSetname {
      let args = self.collect_arg_slices();
      let mut out = mem::take(&mut self.output);
      if let Err(err) = self.network_clientsetname(&args, &mut out) {
        log::warn!("client setname error: {err}");
      }
      self.output = out;
      return true;
    }
    if cmd == RespCommand::ClientSetinfo {
      let args = self.collect_arg_slices();
      let mut out = mem::take(&mut self.output);
      if let Err(err) = self.network_clientsetinfo(&args, &mut out) {
        log::warn!("client setinfo error: {err}");
      }
      self.output = out;
      return true;
    }
    if cmd == RespCommand::ClientUnblock {
      let args = self.collect_arg_slices();
      let mut out = mem::take(&mut self.output);
      if let Err(err) = self.network_clientunblock(&args, &mut out) {
        log::warn!("client unblock error: {err}");
      }
      self.output = out;
      return true;
    }
    if cmd == RespCommand::Cluster {
      self
        .output
        .extend_from_slice(b"-ERR This instance has cluster support disabled\r\n");
      return true;
    }
    if cmd == RespCommand::Readonly {
      self.read_only_session = true;
      self.output.extend_from_slice(b"+OK\r\n");
      return true;
    }
    if cmd == RespCommand::Readwrite {
      self.read_only_session = false;
      self.output.extend_from_slice(b"+OK\r\n");
      return true;
    }
    if cmd == RespCommand::Command {
      self.output.extend_from_slice(b"*0\r\n");
      return true;
    }
    if cmd == RespCommand::Info {
      let args = self.collect_arg_slices();
      let is_cluster = !args.is_empty() && args[0].eq_ignore_ascii_case(b"CLUSTER");
      if is_cluster {
        self
          .output
          .extend_from_slice(b"$24\r\n# Cluster\r\ncluster_enabled:0\r\n\r\n");
        return true;
      }
      let info = "# Server\r\nredis_version:7.2.0\r\n# Cluster\r\ncluster_enabled:0\r\n";
      let mut b = itoa::Buffer::new();
      self.output.push(b'$');
      self
        .output
        .extend_from_slice(b.format(info.len()).as_bytes());
      self.output.extend_from_slice(b"\r\n");
      self.output.extend_from_slice(info.as_bytes());
      self.output.extend_from_slice(b"\r\n");
      return true;
    }
    let args = self.collect_arg_slices();
    self.dispatch_via_hook(cmd, &args);
    true
  }

  /// libs/server/Resp/RespServerSession.cs:NetworkCustomTxn / NetworkCustomProcedure /
  /// NetworkCustomRawStringCmd / NetworkCustomObjCmd 共同骨架
  ///
  /// arity 校验 → 分派 → 清空当前自定义命令槽。注册表由 custom 域承载，
  /// 分派钩子未挂载时按 arity 校验语义闭环。
  pub fn network_custom_txn(&mut self) -> bool {
    self.run_custom_command()
  }

  /// libs/server/Resp/RespServerSession.cs:NetworkCustomProcedure
  pub fn network_custom_procedure(&mut self) -> bool {
    self.run_custom_command()
  }

  /// libs/server/Resp/RespServerSession.cs:NetworkCustomRawStringCmd
  pub fn network_custom_raw_string_cmd(&mut self) -> bool {
    self.run_custom_command()
  }

  /// libs/server/Resp/RespServerSession.cs:NetworkCustomObjCmd
  pub fn network_custom_obj_cmd(&mut self) -> bool {
    self.run_custom_command()
  }

  /// 自定义命令共同路径：IsCommandArityValid → 分派 → 清槽
  fn run_custom_command(&mut self) -> bool {
    let Some((kind, custom)) = self.current_custom_command.take() else {
      return true;
    };
    let count = self.parse_state.count;
    let CustomCommandRef { name, arity, .. } = &custom;
    if !is_command_arity_valid_checked(arity, count) {
      self.abort_wrong_num_args(name);
      return true;
    }
    let args = self.collect_arg_slices();
    let mut hook = self.command_dispatch;
    if let Some(dispatch) = hook.as_mut() {
      dispatch.dispatch_custom(self, kind, &custom, &args);
    }
    self.command_dispatch = hook;
    true
  }

  /// libs/server/Resp/RespServerSession.cs:Process（admin 族回退）
  pub fn process(&mut self, cmd: RespCommand) -> bool {
    let args = self.collect_arg_slices();
    self.dispatch_via_hook(cmd, &args);
    true
  }

  /// libs/server/Resp/RespServerSession.cs:IsCommandArityValid
  ///
  /// arity = 0 不校验；正值 = 恰好 arity-1 参数；负值 = 至少 -arity-1 参数。
  /// 失败时按 C# GenericErrWrongNumArgs 写出错误应答。
  pub fn is_command_arity_valid(&mut self, cmd_name: &str, arity: i32, count: usize) -> bool {
    if !is_command_arity_valid_checked(&arity, count) {
      self.abort_wrong_num_args(cmd_name);
      return false;
    }
    true
  }

  /// libs/server/Resp/RespServerSession.cs:GetCommand
  ///
  /// 从接收缓冲读取命令名（$len\r\n...\r\n），推进 readHead；不完整返回 None
  pub fn get_command(&mut self) -> Option<Vec<u8>> {
    self.read_length_prefixed_string(false)
  }

  /// libs/server/Resp/RespServerSession.cs:GetUpperCaseCommand
  ///
  /// 同 GetCommand，并就地大写化（子命令名用）
  pub fn get_upper_case_command(&mut self) -> Option<Vec<u8>> {
    self.read_length_prefixed_string(true)
  }

  /// $len\r\n 负载读取（GetCommand / GetUpperCaseCommand 共同体）
  fn read_length_prefixed_string(&mut self, upper: bool) -> Option<Vec<u8>> {
    let buffer: &[u8] = &self.recv_buffer;
    let mut ptr = self.read_head;
    let end = self.bytes_read;

    // 读 $len 头
    if ptr >= end || buffer[ptr] != b'$' {
      return None;
    }
    ptr += 1;

    let start_digit = ptr;
    let mut length = 0usize;
    while ptr < end && buffer[ptr].is_ascii_digit() {
      length = length
        .checked_mul(10)?
        .checked_add((buffer[ptr] - b'0') as usize)?;
      ptr += 1;
    }
    if ptr == start_digit {
      return None;
    }
    // 512MB 最大参数限制
    if length > 512 * 1024 * 1024 {
      return None;
    }
    // 头尾 \r\n
    if ptr + 2 > end || &buffer[ptr..ptr + 2] != b"\r\n" {
      return None;
    }
    ptr += 2;

    // 命令值 + 结尾（数据未收齐时不移动 read_head，保证断包可安全重试）
    if ptr + length + 2 > end {
      return None;
    }
    if &buffer[ptr + length..ptr + length + 2] != b"\r\n" {
      return None;
    }
    let mut result = buffer[ptr..ptr + length].to_vec();
    self.read_head = ptr + length + 2;
    if upper {
      result.make_ascii_uppercase();
    }
    Some(result)
  }

  /// libs/server/Resp/RespServerSession.cs:TryKill
  ///
  /// 尝试杀死会话：首次调用关闭底层连接并返回 true，后续调用返回 false
  pub fn try_kill(&mut self) -> bool {
    if self.kill_requested {
      false
    } else {
      self.kill_requested = true;
      true
    }
  }

  /// libs/server/Resp/RespServerSession.cs:SendAndReset
  ///
  /// 冲洗输出缓冲；缓冲无新增字节时返回 false（C# 抛 GarnetException：
  /// 写入超出响应缓冲仍无进展）
  pub fn send_and_reset(&mut self) -> bool {
    if self.output.is_empty() {
      return false;
    }
    self.sent.extend_from_slice(&self.output);
    self.flushed_bytes += self.output.len() as u64;
    if let Some(metrics) = &mut self.session_metrics {
      metrics.incr_total_net_output_bytes(self.output.len() as u64);
    }
    self.output.clear();
    true
  }

  /// 已发送字节（读后即清；C# SendResponse 写入网络侧的留档等价）
  pub fn take_sent(&mut self) -> Vec<u8> {
    mem::take(&mut self.sent)
  }

  /// 待发送字节（C# dcurr - GetResponseObjectHead）
  pub fn pending_output_len(&self) -> usize {
    self.output.len()
  }

  /// 取走待发送字节（C# Send(networkSender.GetResponseObjectHead())）
  pub fn take_output(&mut self) -> Vec<u8> {
    let pending = mem::take(&mut self.output);
    self.flushed_bytes += pending.len() as u64;
    if let Some(metrics) = &mut self.session_metrics {
      metrics.incr_total_net_output_bytes(pending.len() as u64);
    }
    pending
  }

  /// 有待发送字节时冲洗（ProcessMessages 尾部 Send）
  fn flush_if_pending(&mut self) {
    if !self.output.is_empty() {
      self.send_and_reset();
      if self.to_dispose {
        self.kill_requested = true;
      }
    }
  }

  /// libs/server/Resp/RespServerSession.cs:WriteDirectLarge
  ///
  /// 大块直写输出缓冲（rust 托管缓冲天然可扩容，等价一次追加）
  pub fn write_direct_large(&mut self, src: &[u8]) {
    self.output.extend_from_slice(src);
  }

  /// libs/server/Resp/RespServerSession.cs:DebugSend
  ///
  /// 调试路径：逐字节发送（EnableAOF + WaitForCommit 时先等 AOF 提交）。
  /// rust 侧以逐字节冲洗计数承接语义；`enable_aof_wait` 由调用方依
  /// serverOptions 传入。
  pub fn debug_send(&mut self, enable_aof_wait: bool) {
    if self.output.is_empty() {
      return;
    }
    if enable_aof_wait {
      // C# storeWrapper.WaitForCommitAsync() 阻塞等提交；AOF 链路（本域）
      // 提供 wait_for_commit 入口，此处标记等待语义
      self.wait_for_aof_blocking = false;
    }
    let bytes = self.output.len();
    self.sent.extend_from_slice(&self.output);
    self.flushed_bytes += bytes as u64;
    if let Some(metrics) = &mut self.session_metrics {
      metrics.incr_total_net_output_bytes(bytes as u64);
    }
    self.output.clear();
  }

  /// libs/server/Resp/RespServerSession.cs:TrySwitchActiveDatabaseSession
  pub fn try_switch_active_database_session(&mut self, db_id: i32) -> bool {
    if !self.allow_multi_db {
      return false;
    }
    if !self.try_get_or_set_database_session(db_id, -1) {
      return false;
    }
    let Some(slot) = self.database_session(db_id) else {
      return false;
    };
    self.switch_active_database_session(slot);
    true
  }

  /// libs/server/Resp/RespServerSession.cs:TrySwapDatabaseSessions
  pub fn try_swap_database_sessions(&mut self, db_id1: i32, db_id2: i32) -> bool {
    if !self.allow_multi_db {
      return false;
    }
    if db_id1 == db_id2 {
      return true;
    }
    // 注意：dbIdForSessionCreation 交叉设置（数据库已先行交换）
    if !self.try_get_or_set_database_session(db_id1, db_id2) {
      return false;
    }
    if !self.try_get_or_set_database_session(db_id2, db_id1) {
      return false;
    }
    let (a, b) = (self.database_session(db_id1), self.database_session(db_id2));
    let (Some(slot1), Some(slot2)) = (a, b) else {
      return false;
    };
    // 交换映射（保留各自 ID）
    self.set_database_session(
      db_id1,
      DatabaseSessionSlot {
        id: db_id1,
        created_ticks: slot2.created_ticks,
      },
    );
    self.set_database_session(
      db_id2,
      DatabaseSessionSlot {
        id: db_id2,
        created_ticks: slot1.created_ticks,
      },
    );

    if self.active_db_id == db_id1 {
      let slot = self.database_session(db_id1);
      if let Some(slot) = slot {
        self.switch_active_database_session(slot);
      }
    } else if self.active_db_id == db_id2 {
      let slot = self.database_session(db_id2);
      if let Some(slot) = slot {
        self.switch_active_database_session(slot);
      }
    }
    true
  }

  /// libs/server/Resp/RespServerSession.cs:TryGetOrSetDatabaseSession
  ///
  /// 获取或创建库会话槽；`db_id_for_session_creation = -1` 时取 db_id
  pub fn try_get_or_set_database_session(
    &mut self,
    db_id: i32,
    db_id_for_session_creation: i32,
  ) -> bool {
    if db_id < 0 {
      return false;
    }
    let creation_id = if db_id_for_session_creation == -1 {
      db_id
    } else {
      db_id_for_session_creation
    };
    if (db_id as usize) < self.database_sessions.len()
      && self.database_sessions[db_id as usize].is_some()
    {
      return true;
    }
    if creation_id < 0 || (creation_id as usize) >= self.database_sessions.len() {
      // ExpandableMap 容量上限（C# TrySetValueUnsafe 失败路径）
      return false;
    }
    let slot = self.create_database_session(creation_id);
    self.database_sessions[creation_id as usize] = Some(slot);
    true
  }

  /// libs/server/Resp/RespServerSession.cs:CreateDatabaseSession
  ///
  /// 存储会话 / API / 事务管理器由 wkv 会话纪元与并行域承载，此处建槽
  fn create_database_session(&self, db_id: i32) -> DatabaseSessionSlot {
    DatabaseSessionSlot {
      id: db_id,
      created_ticks: session_now_ms(),
    }
  }

  /// libs/server/Resp/RespServerSession.cs:CreateConsistentReadApi
  ///
  /// 仅集群 + AOF 多日志启用时创建（C# 条件同）；一致读 API 本体由
  /// readconsistency 域（本周期 aof 链）承接，会话侧建专用槽位
  pub fn create_consistent_read_api(
    &mut self,
    enable_cluster: bool,
    multilog_enabled: bool,
  ) -> Option<DatabaseSessionSlot> {
    (enable_cluster && multilog_enabled).then(|| DatabaseSessionSlot {
      id: 0,
      created_ticks: session_now_ms(),
    })
  }

  /// libs/server/Resp/RespServerSession.cs:SwitchActiveDatabaseSession
  pub fn switch_active_database_session(&mut self, db_session: DatabaseSessionSlot) {
    self.active_db_id = db_session.id;
  }

  /// 活跃库 ID 的会话槽视图（C# `databaseSessions.Map[activeDbId]`）
  pub fn database_session(&self, db_id: i32) -> Option<DatabaseSessionSlot> {
    if db_id < 0 || (db_id as usize) >= self.database_sessions.len() {
      return None;
    }
    self.database_sessions[db_id as usize].clone()
  }

  fn set_database_session(&mut self, db_id: i32, slot: DatabaseSessionSlot) {
    if db_id >= 0 && (db_id as usize) < self.database_sessions.len() {
      self.database_sessions[db_id as usize] = Some(slot);
    }
  }

  /// libs/server/Resp/RespServerSession.cs:GetStringOutput
  ///
  /// 主存输出视图（dcurr..dend 的托管等价：输出缓冲可写区）。
  /// C# StringOutput 类型由 string_output 域（并行代理）承载后可替换返回型。
  pub fn get_string_output(&mut self) -> &mut Vec<u8> {
    &mut self.output
  }

  /// libs/server/Resp/RespServerSession.cs:GetObjectOutput
  pub fn get_object_output(&mut self) -> &mut Vec<u8> {
    &mut self.output
  }

  /// libs/server/Resp/RespServerSession.cs:GetUnifiedOutput
  pub fn get_unified_output(&mut self) -> &mut Vec<u8> {
    &mut self.output
  }

  /// 写错误应答并置 commandErrorWritten（对标 C# AbortWithErrorMessage）
  pub fn abort_error_message(&mut self, message: &str) {
    let clean = cs::sanitize_error_str(message, cs::MAX_ERROR_MSG_LEN);
    self.output.extend_from_slice(b"-");
    self.output.extend_from_slice(clean.as_bytes());
    self.output.extend_from_slice(b"\r\n");
    self.command_error_written = true;
  }

  /// 参数数量错误应答（对标 C# AbortWithWrongNumberOfArguments）
  pub fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    let clean = cs::sanitize_error_str(cmd_name, cs::MAX_PARAM_NAME_LEN);
    self
      .output
      .extend_from_slice(b"-ERR wrong number of arguments for '");
    self.output.extend_from_slice(clean.as_bytes());
    self.output.extend_from_slice(b"' command\r\n");
    self.command_error_written = true;
  }

  /// 未知子命令或参数数量错误应答（对标 C# AbortWithWrongNumberOfArgumentsOrUnknownSubcommand）
  pub fn abort_with_wrong_num_args_or_unknown_subcommand(
    &mut self,
    sub_command: &str,
    cmd_name: &str,
  ) {
    let clean_sub = cs::sanitize_error_str(sub_command, cs::MAX_PARAM_NAME_LEN);
    let clean_cmd = cs::sanitize_error_str(cmd_name, cs::MAX_PARAM_NAME_LEN);
    self
      .output
      .extend_from_slice(b"-ERR unknown subcommand or wrong number of arguments for '");
    self.output.extend_from_slice(clean_sub.as_bytes());
    self.output.extend_from_slice(b"'. Try ");
    self.output.extend_from_slice(clean_cmd.as_bytes());
    self.output.extend_from_slice(b" HELP\r\n");
    self.command_error_written = true;
  }

  /// C# WriteError（直接错误写出，无 commandErrorWritten 置位路径差异由
  /// 调用方维护；此处统一置位，覆盖面以 WriteError/Abort 族为准）
  fn write_error_response(&mut self, message: &str) {
    self.abort_error_message(message);
  }

  /// 写出 ACL 拦截错误（对标 libs/server/Resp/RespServerSession.cs:697-706）
  ///
  /// 已认证用户无命令权限时写出 RESP_ERR_NOPERM（`-NOPERM this user has no permissions to run the command\r\n`）；
  /// 未认证会话写出 RESP_ERR_NOAUTH（`-NOAUTH Authentication required.\r\n`）。
  pub fn write_acl_permission_error(&mut self, is_authenticated: bool) {
    let err = if is_authenticated {
      cs::RESP_ERR_NOPERM
    } else {
      cs::RESP_ERR_NOAUTH
    };
    self.abort_error_message(err);
  }

  /// 汇集解析态参数切片（零拷贝借用；分派入参）
  #[inline]
  pub fn collect_arg_slices<'a>(&self) -> Vec<&'a [u8]> {
    let count = self.parse_state.count;
    let mut args = Vec::with_capacity(count);
    for i in 0..count {
      let raw_slice = self.parse_state.get_arg_slice_by_ref(i);
      let arg: &'a [u8] = if raw_slice.ptr.is_null() || raw_slice.length == 0 {
        &[]
      } else {
        unsafe { slice::from_raw_parts(raw_slice.ptr, raw_slice.length) }
      };
      args.push(arg);
    }
    args
  }

  /// 汇集解析态参数所有权副本（Lua 脚本上下文等需独立所有权场景用）
  fn collect_args(&self) -> Vec<Vec<u8>> {
    (0..self.parse_state.count)
      .map(|i| self.parse_state.get_arg_slice_by_ref(i).as_slice().to_vec())
      .collect()
  }
}

impl RespServerSession {
  /// CLIENT SETNAME 落库（C# clientName 字段赋值；命令域校验后调用）
  pub fn set_client_name(&mut self, name: Option<&str>) {
    self.client_name = name.map(str::to_string);
  }

  /// CLIENT SETINFO 落库（C# clientLibName / clientLibVersion 字段赋值）
  pub fn set_client_lib_info(&mut self, lib_name: Option<&str>, lib_version: Option<&str>) {
    self.client_lib_name = lib_name.map(str::to_string);
    self.client_lib_version = lib_version.map(str::to_string);
  }

  /// libs/server/Resp/BasicCommands.cs:WriteClientInfo
  ///
  /// 字段序与 C# 逐项对齐：id addr laddr age flags db resp lib-name lib-ver。
  pub fn write_client_info_state(&self, into: &mut String) {
    let age_sec = (session_now_ms() - self.creation_ticks).max(0) / 1000;
    use std::fmt::Write as _;
    let _ = write!(
      into,
      "id={} addr={} laddr={} age={} flags={} db={} resp={} lib-name={} lib-ver={}",
      self.id,
      self.remote_endpoint,
      self.local_endpoint,
      age_sec,
      if self.is_subscription_session {
        "P"
      } else {
        "N"
      },
      self.active_db_id,
      self.resp_protocol_version,
      self.client_lib_name.as_deref().unwrap_or(""),
      self.client_lib_version.as_deref().unwrap_or(""),
    );
  }

  /// 处理 HELLO 命令的会话状态转换：
  ///
  /// 校验 → 认证 → 升级协议 / 落客户端名 → 按会话真实状态组 HELLO 应答 map
  ///（RESP2 退化为双倍数组）。返回 false 表示认证失败（WRONGPASS）。
  /// `authenticator_can_authenticate` 镜像 C# NoAuth 认证器恒 false 的默认路径。
  pub fn process_hello_command_state(
    &mut self,
    resp_protocol_version: Option<u8>,
    username: &[u8],
    client_name: Option<&str>,
    output: &mut Vec<u8>,
  ) -> bool {
    // C# 默认 NoAuth 认证器 Authenticate 恒 false → 带 AUTH 的 HELLO 报 WRONGPASS
    //（HELLO 带 username → Invalid username/password combination 变体）
    if !username.is_empty() && !self.authenticator_can_authenticate {
      cs::write_error_raw(output, RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
      return false;
    }

    if let Some(version) = resp_protocol_version {
      self.update_resp_protocol_version(version);
    }
    if let Some(name) = client_name {
      self.set_client_name(Some(name));
    }

    // 应答 map（RESP2 退化为双倍数组）；字段序对齐 C#：server/version/
    // garnet_version/proto/id/mode/role + modules 空数组；proto/id 直读会话状态
    write_map_len_resp2(output, 8);
    output.write_resp_bulk_string(b"server");
    output.write_resp_bulk_string(b"redis");
    output.write_resp_bulk_string(b"version");
    output.write_resp_bulk_string(REDIS_PROTOCOL_VERSION.as_bytes());
    output.write_resp_bulk_string(b"garnet_version");
    output.write_resp_bulk_string(env!("CARGO_PKG_VERSION").as_bytes());
    output.write_resp_bulk_string(b"proto");
    output.write_resp_int(i64::from(self.resp_protocol_version));
    output.write_resp_bulk_string(b"id");
    output.write_resp_int(self.id);
    output.write_resp_bulk_string(b"mode");
    output.write_resp_bulk_string(b"standalone");
    output.write_resp_bulk_string(b"role");
    output.write_resp_bulk_string(b"master");
    output.write_resp_bulk_string(b"modules");
    output.extend_from_slice(b"*0\r\n");
    true
  }
}

impl RespServerSession {
  /// Lua 命令（EVAL / EVALSHA / SCRIPT 等）会话侧接线：构建 [`LuaSessionContext`] 并分派
  /// （输出缓冲为脚本期本地缓冲，结束后并入会话输出）。
  fn run_lua_command(&mut self, cmd: RespCommand) -> bool {
    let Some(mut session_cache) = self.session_script_cache.take() else {
      // C# CheckLuaEnabled：未启用直接回错
      self.abort_error_message("ERR Lua is disabled.");
      return true;
    };
    let store_cache = Arc::clone(&self.store_script_cache);
    let owned = self.collect_args();
    let args: Vec<Vec<u8>> = owned;
    let mut script_out = Vec::new();
    {
      let mut api = RespScriptingApi(&mut *self);
      let mut ctx = LuaSessionContext {
        args: &args,
        out: &mut script_out,
        session_cache: &mut session_cache,
        store_cache: &store_cache,
        session: &mut api,
        lua_enabled: true,
        txn_mode: false,
        redis_version: REDIS_PROTOCOL_VERSION,
        lua_options: &LuaOptions::default(),
      };
      match cmd {
        RespCommand::Eval => LuaCommands::try_eval(&mut ctx),
        RespCommand::Evalsha => LuaCommands::try_evalsha(&mut ctx),
        RespCommand::ScriptExists => LuaCommands::network_script_exists(&mut ctx),
        RespCommand::ScriptFlush => LuaCommands::network_script_flush(&mut ctx),
        RespCommand::ScriptLoad => LuaCommands::network_script_load(&mut ctx),
        _ => true,
      };
    }
    self.session_script_cache = Some(session_cache);
    self.output.extend_from_slice(&script_out);
    true
  }

  /// C# CheckACLPermissions：ACL 域为并行转写，NoAuth 语义下已认证会话恒
  /// 放行（ScriptingApi 接入点；ACL 落地后改接用户权限集）。
  /// 命名避让 admin_commands 域的 ACL 命令处理器（同名 C# 入口）
  /// 保留形参以匹配 CheckACLPermissions 接口签名规范
  pub fn acl_allows_command(&self, _command: &str) -> bool {
    !self.authenticator_can_authenticate
  }

  /// 构建 NoScript 命令集位图（对齐 LuaRunner InitializeNoScriptDetails 集合）
  ///
  /// NoScript 命令集按 [`RespCommand`] 判别值置位（C# RespCommandsInfo 的
  /// NoScript 标志集）；FCALL/FUNCTION/EVAL_RO/EVALSHA_RO 判别值待 types 域
  /// 扩表后由同一集合补齐（缺口已列入汇报）。
  pub fn no_script_details() -> (i32, Vec<u64>) {
    const NO_SCRIPT_COMMANDS: &[RespCommand] = &[
      RespCommand::Eval,
      RespCommand::Evalsha,
      RespCommand::Flushall,
      RespCommand::Flushdb,
      RespCommand::Psubscribe,
      RespCommand::Script,
      RespCommand::Subscribe,
      RespCommand::Swapdb,
    ];
    let bits = u64::BITS as usize;
    let words = NO_SCRIPT_COMMANDS
      .iter()
      .map(|cmd| {
        let raw: u16 = (*cmd).into();
        raw as usize / bits
      })
      .max()
      .unwrap_or(0)
      + 1;
    let mut bitmap = vec![0u64; words];
    for cmd in NO_SCRIPT_COMMANDS {
      let raw: u16 = (*cmd).into();
      let bit = raw as usize;
      bitmap[bit / bits] |= 1u64 << (bit % bits);
    }
    (0, bitmap)
  }
}

/// 会话的 [`ScriptingApi`] 适配器（redis.call 落地面）
///
/// C# ProcessCommandFromScripting 把参数格式化为 RESP 请求后重入
/// TryConsumeMessages；rust 侧经同一解析/分派路径，响应字节落入
/// [`ScratchBufferNetworkSender`]。会话脚本缓存已由 [`RespServerSession::run_lua_command`]
/// 暂时摘除，重入路径与脚本期借用互斥。
struct RespScriptingApi<'a>(&'a mut RespServerSession);

impl ScriptingApi for RespScriptingApi<'_> {
  /// 分派 RESP 请求（C# TryConsumeMessages + ScratchBufferNetworkSender 组合）
  fn dispatch_resp(&mut self, request: &[u8], sender: &mut ScratchBufferNetworkSender) {
    let _ = self.0.try_consume_messages(request);
    let response = self.0.take_sent();
    sender.write_response_bytes(&response);
  }

  /// GET 特例（C# api.GET）：RESP 请求闭环后解析批量串/null 应答
  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, &'static str> {
    let mut request = Vec::with_capacity(key.len() + 16);
    request.write_resp_bulk_string(b"GET");
    request.write_resp_bulk_string(key);
    let mut sender = ScratchBufferNetworkSender::new();
    self.dispatch_resp(&request, &mut sender);
    parse_bulk_reply(sender.get_response())
  }

  /// SET 特例（C# api.SET）：+OK 或错误应答
  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), &'static str> {
    let mut request = Vec::with_capacity(key.len() + value.len() + 24);
    request.write_resp_bulk_string(b"SET");
    request.write_resp_bulk_string(key);
    request.write_resp_bulk_string(value);
    let mut sender = ScratchBufferNetworkSender::new();
    self.dispatch_resp(&request, &mut sender);
    parse_simple_reply(sender.get_response())
  }

  fn resp_protocol_version(&self) -> u8 {
    self.0.resp_protocol_version
  }

  fn update_resp_protocol_version(&mut self, version: u8) {
    self.0.update_resp_protocol_version(version);
  }

  fn check_acl_permissions(&self, command: &str) -> bool {
    self.0.acl_allows_command(command)
  }

  fn set_transaction_mode(&mut self, enabled: bool) {
    self.0.set_transaction_mode(enabled);
  }
}

impl wtxn::TxnSession for RespServerSession {
  #[inline]
  fn session_id(&self) -> i32 {
    self.id as i32
  }

  #[inline]
  fn arg_count(&self) -> usize {
    self.parse_state.count
  }

  #[inline]
  fn get_arg(&self, idx: usize) -> &[u8] {
    self.parse_state.get_arg_slice_by_ref(idx).as_slice()
  }

  #[inline]
  fn txn_state(&self) -> TxnState {
    self.txn_state
  }

  #[inline]
  fn set_txn_state(&mut self, state: TxnState) {
    self.txn_state = state;
  }

  #[inline]
  fn end_read_head(&self) -> usize {
    self.end_read_head
  }

  #[inline]
  fn set_end_read_head(&mut self, head: usize) {
    self.end_read_head = head;
  }

  #[inline]
  fn resp_protocol_version(&self) -> u8 {
    self.resp_protocol_version
  }

  #[inline]
  fn active_db_id(&self) -> i32 {
    self.active_db_id
  }

  #[inline]
  fn can_run_debug(&self) -> bool {
    self.can_run_debug()
  }

  #[inline]
  fn write_ok(&mut self) {
    self.output.extend_from_slice(cs::RESP_OK);
  }

  #[inline]
  fn write_queued(&mut self) {
    self.output.extend_from_slice(b"+QUEUED\r\n");
  }

  #[inline]
  fn write_null_array(&mut self) {
    if self.resp_protocol_version >= 3 {
      self.output.extend_from_slice(b"_\r\n");
    } else {
      self.output.extend_from_slice(b"*-1\r\n");
    }
  }

  #[inline]
  fn write_array_len(&mut self, count: usize) {
    use wresp::RespVecExt;
    self.output.write_resp_array_len(count);
  }

  #[inline]
  fn write_error(&mut self, message: &str) {
    self.abort_error_message(message);
  }

  #[inline]
  fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    self.abort_wrong_num_args(cmd_name);
  }

  #[inline]
  fn write_proc_param_error(&mut self, tx_id: u8, expected: i32, actual: usize) {
    let mut b0 = itoa::Buffer::new();
    let mut b1 = itoa::Buffer::new();
    let mut b2 = itoa::Buffer::new();
    let s0 = b0.format(tx_id);
    let s1 = b1.format(expected);
    let s2 = b2.format(actual);
    self
      .output
      .extend_from_slice(b"-ERR Invalid number of parameters to stored proc ");
    self.output.extend_from_slice(s0.as_bytes());
    self.output.extend_from_slice(b", expected ");
    self.output.extend_from_slice(s1.as_bytes());
    self.output.extend_from_slice(b", actual ");
    self.output.extend_from_slice(s2.as_bytes());
    self.output.extend_from_slice(b"\r\n");
    self.command_error_written = true;
  }
}

/// 解析 RESP 批量串/null 应答（GET 特例回包）
fn parse_bulk_reply(reply: &[u8]) -> Result<Option<Vec<u8>>, &'static str> {
  if reply.first() == Some(&b'$') {
    let text = str::from_utf8(&reply[1..]).map_err(|_| "protocol error")?;
    let Some(crlf) = text.find("\r\n") else {
      return Err("protocol error");
    };
    let len: isize = text[..crlf].parse().map_err(|_| "protocol error")?;
    if len < 0 {
      return Ok(None);
    }
    let start = 1 + crlf + 2;
    let end = start + len as usize;
    if reply.len() >= end {
      return Ok(Some(reply[start..end].to_vec()));
    }
  }
  if reply.first() == Some(&b'-') {
    return Err("script error");
  }
  Err("protocol error")
}

/// 解析 RESP 简单串应答（SET 特例回包：+OK）
fn parse_simple_reply(reply: &[u8]) -> Result<(), &'static str> {
  match reply.first() {
    Some(b'+') => Ok(()),
    Some(b'-') => Err("script error"),
    _ => Err("protocol error"),
  }
}

/// 默认会话（C# internal RespServerSession() 空构造的等价）
impl Default for RespServerSession {
  fn default() -> Self {
    Self::new(0, RespServerSessionOptions::default())
  }
}

impl wpubsub::PubSubSessionCommands for RespServerSession {
  #[inline]
  fn session_id(&self) -> i64 {
    self.id
  }

  #[inline]
  fn output_mut(&mut self) -> &mut Vec<u8> {
    &mut self.output
  }

  #[inline]
  fn resp_protocol_version(&self) -> u8 {
    self.resp_protocol_version
  }

  #[inline]
  fn set_subscription_session(&mut self, is_subscription: bool) {
    self.is_subscription_session = is_subscription;
  }

  #[inline]
  fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    self.abort_wrong_num_args(cmd_name);
  }

  #[inline]
  fn abort_error_message(&mut self, message: &str) {
    self.abort_error_message(message);
  }

  #[inline]
  fn write_null(&mut self) {
    self.write_null();
  }

  #[inline]
  fn write_push_length(&mut self, count: usize) {
    self.write_push_length(count);
  }

  #[inline]
  fn send_and_reset(&mut self) {
    self.send_and_reset();
  }
}

impl RespServerSession {
  /// 订阅通道（委托 wpubsub::PubSubSessionCommands）
  #[inline]
  pub fn network_subscribe(
    &mut self,
    wire: &mut wpubsub::PubSubSession,
    shard: bool,
    args: &[&[u8]],
  ) -> bool {
    wpubsub::PubSubSessionCommands::network_subscribe(self, wire, shard, args)
  }

  /// 模式订阅（委托 wpubsub::PubSubSessionCommands）
  #[inline]
  pub fn network_psubscribe(&mut self, wire: &mut wpubsub::PubSubSession, args: &[&[u8]]) -> bool {
    wpubsub::PubSubSessionCommands::network_psubscribe(self, wire, args)
  }

  /// 退订通道（委托 wpubsub::PubSubSessionCommands）
  #[inline]
  pub fn network_unsubscribe(&mut self, wire: &mut wpubsub::PubSubSession, args: &[&[u8]]) -> bool {
    wpubsub::PubSubSessionCommands::network_unsubscribe(self, wire, args)
  }

  /// 退订模式（委托 wpubsub::PubSubSessionCommands）
  #[inline]
  pub fn network_punsubscribe(
    &mut self,
    wire: &mut wpubsub::PubSubSession,
    args: &[&[u8]],
  ) -> bool {
    wpubsub::PubSubSessionCommands::network_punsubscribe(self, wire, args)
  }

  /// 发布消息（委托 wpubsub::PubSubSessionCommands）
  #[inline]
  pub fn network_publish(
    &mut self,
    wire: &mut wpubsub::PubSubSession,
    shard: bool,
    args: &[&[u8]],
  ) -> bool {
    wpubsub::PubSubSessionCommands::network_publish(self, wire, shard, args)
  }

  /// 列出活跃通道（委托 wpubsub::PubSubSessionCommands）
  #[inline]
  pub fn network_pubsub_channels(
    &mut self,
    wire: &mut wpubsub::PubSubSession,
    args: &[&[u8]],
  ) -> bool {
    wpubsub::PubSubSessionCommands::network_pubsub_channels(self, wire, args)
  }

  /// 活跃模式订阅数（委托 wpubsub::PubSubSessionCommands）
  #[inline]
  pub fn network_pubsub_numpat(
    &mut self,
    wire: &mut wpubsub::PubSubSession,
    args: &[&[u8]],
  ) -> bool {
    wpubsub::PubSubSessionCommands::network_pubsub_numpat(self, wire, args)
  }

  /// 指定通道订阅数（委托 wpubsub::PubSubSessionCommands）
  #[inline]
  pub fn network_pubsub_numsub(
    &mut self,
    wire: &mut wpubsub::PubSubSession,
    args: &[&[u8]],
  ) -> bool {
    wpubsub::PubSubSessionCommands::network_pubsub_numsub(self, wire, args)
  }

  /// 会话推送编码收敛点（委托 wpubsub::PubSubSessionCommands）
  #[inline]
  pub fn drain_pubsub_frames(&mut self, wire: &wpubsub::PubSubSession) -> usize {
    wpubsub::PubSubSessionCommands::drain_pubsub_frames(self, wire)
  }
}

/// 连接保护共同判定（调试命令与模块加载使用）
fn can_run_with_protection(option: ConnectionProtectionOption, is_local: bool) -> bool {
  match option {
    ConnectionProtectionOption::Yes => true,
    ConnectionProtectionOption::No => false,
    ConnectionProtectionOption::Local => is_local,
  }
}

/// 命令 arity 纯判定逻辑（供 is_command_arity_valid 使用）。
/// arity = 0 不校验；正值 = 恰好 arity-1 参数；
/// 负值 = 至少 |arity|-1 参数（C# `count < -arity - 1` 为非法）
fn is_command_arity_valid_checked(arity: &i32, count: usize) -> bool {
  if *arity == 0 {
    return true;
  }
  if *arity > 0 {
    count == *arity as usize - 1
  } else {
    count >= (-*arity) as usize - 1
  }
}

/// Environment.TickCount64 等价：会话年龄域毫秒时钟（CreationTicks/age 仅作
/// 相对量，绝对基准无关紧要；单一实现 `wbase::time::now_ms`，此处仅饱和转 i64）
fn session_now_ms() -> i64 {
  now_ms().min(i64::MAX as u64) as i64
}

/// 空操作分派器：满足 RespCommandDispatch trait 签名契约保留
impl RespCommandDispatch for () {
  fn dispatch(&mut self, _session: &mut RespServerSession, _cmd: RespCommand, _args: &[&[u8]]) {}
}

/// 自定义命令分派扩展钩子（C# TryTransactionProc / TryCustomProcedure /
/// TryCustomRawStringCommand / TryCustomObjectCommand 的接入点）
pub trait RespCommandDispatchExt: RespCommandDispatch {
  /// 分派自定义命令（默认忽略：custom 域接入时覆写）
  fn dispatch_custom(
    &mut self,
    _session: &mut RespServerSession,
    _kind: RespCommand,
    _custom: &CustomCommandRef,
    _args: &[&[u8]],
  ) {
  }
}

impl<T: RespCommandDispatch + ?Sized> RespCommandDispatchExt for T {}

#[cfg(test)]
mod tests {
  use super::*;

  fn session(id: i64) -> RespServerSession {
    RespServerSession::new(id, RespServerSessionOptions::default())
  }

  fn multi_db_session(id: i64) -> RespServerSession {
    RespServerSession::new(
      id,
      RespServerSessionOptions {
        allow_multi_db: true,
        ..RespServerSessionOptions::default()
      },
    )
  }

  #[test]
  fn dispatch_hook_routes_via_injection() {
    let mut s = session(20);
    s.set_command_dispatch(CommandDispatcher::Nop);
    s.parse_state.initialize(1);
    assert!(s.process_array_commands(RespCommand::Get));
    assert_eq!(String::from_utf8(s.take_output()).unwrap(), "+OK\r\n");
  }

  #[test]
  fn session_state_fields() {
    let mut s = session(42);
    assert_eq!(s.id, 42);
    assert_eq!(s.resp_protocol_version, DEFAULT_RESP_VERSION);
    assert!(s.client_name.is_none());
    assert!(s.user_handle.is_some(), "构造即默认用户");

    // HELLO [3] 升级协议版本
    s.update_resp_protocol_version(3);
    assert_eq!(s.resp_protocol_version, 3);

    s.set_client_name(Some("webc"));
    assert_eq!(s.client_name.as_deref(), Some("webc"));
    s.set_client_lib_info(Some("phpredis"), Some("6.0.2"));
    assert_eq!(s.client_lib_name.as_deref(), Some("phpredis"));
    assert_eq!(s.client_lib_version.as_deref(), Some("6.0.2"));

    // useAsync 由 ASYNC 命令置位
    assert!(!s.use_async);
    s.use_async = true;
    assert!(s.use_async);
  }

  #[test]
  fn client_info_carries_real_state() {
    let mut s = session(7);
    s.remote_endpoint = "127.0.0.1:6380".to_string();
    s.set_client_name(Some("loader"));
    s.set_client_lib_info(Some("redis-py"), Some("5.0.1"));
    let mut info = String::new();
    s.write_client_info_state(&mut info);
    assert_eq!(
      info,
      "id=7 addr=127.0.0.1:6380 laddr= age=0 flags=N db=0 resp=2 lib-name=redis-py lib-ver=5.0.1"
    );
  }

  #[test]
  fn hello_answer_uses_session_state() {
    let mut s = session(9);
    let mut out = Vec::new();
    let ok = s.process_hello_command_state(Some(3), b"", None, &mut out);
    assert!(ok);
    let text = String::from_utf8(out).unwrap();
    // proto 回写会话当前版本（3），id 为真实会话 Id
    assert!(
      text.contains("$5\r\nproto\r\n:3\r\n"),
      "resp=3 expected: {text}"
    );
    assert!(
      text.contains("$2\r\nid\r\n:9\r\n"),
      "真实 Id expected: {text}"
    );
    assert_eq!(s.resp_protocol_version, 3);
    // 二次 HELLO 2 降级
    let mut out = Vec::new();
    assert!(s.process_hello_command_state(Some(2), b"", None, &mut out));
    assert_eq!(s.resp_protocol_version, 2);
  }

  #[test]
  fn hello_rejects_auth_on_noauth() {
    let mut s = session(1);
    let mut out = Vec::new();
    assert!(!s.process_hello_command_state(Some(3), b"alice", None, &mut out));
    // C# 带 username 的 HELLO 认证失败 → Invalid username/password combination
    assert_eq!(
      String::from_utf8(out).unwrap(),
      "-WRONGPASS Invalid username/password combination\r\n"
    );
    // 认证失败不升级协议
    assert_eq!(s.resp_protocol_version, DEFAULT_RESP_VERSION);
  }

  #[test]
  fn database_sessions_lifecycle() {
    let mut s = multi_db_session(1);
    // 默认库 0 已建且活跃
    assert_eq!(s.active_db_id, 0);
    assert_eq!(s.get_database_sessions_snapshot().len(), 1);

    // 切换到库 2（惰性创建）
    assert!(s.try_switch_active_database_session(2));
    assert_eq!(s.active_db_id, 2);
    assert_eq!(s.get_database_sessions_snapshot().len(), 2);

    // 超出 max_databases → 失败
    assert!(!s.try_switch_active_database_session(999));

    // SWAPDB：交换后活跃会话跟随
    assert!(s.try_swap_database_sessions(0, 2));
    assert_eq!(s.active_db_id, 2);
    assert_eq!(s.database_session(0).unwrap().id, 0);
    assert_eq!(s.database_session(2).unwrap().id, 2);

    // 单库会话禁切换
    let mut single = session(2);
    assert!(!single.try_switch_active_database_session(1));
    assert!(!single.try_swap_database_sessions(0, 1));
  }

  #[test]
  fn kill_once_and_dispose() {
    let mut s = session(3);
    assert!(s.try_kill());
    assert!(!s.try_kill(), "重复 kill 返回 false");
  }

  #[test]
  fn arity_validation_writes_error() {
    let mut s = session(4);
    assert!(s.is_command_arity_valid("get", 2, 1));
    assert!(!s.is_command_arity_valid("get", 2, 2));
    let text = String::from_utf8(s.take_output()).unwrap();
    assert_eq!(text, "-ERR wrong number of arguments for 'get' command\r\n");
    // 负 arity：至少 |arity|-1
    assert!(s.is_command_arity_valid("mset", -3, 2));
    assert!(!s.is_command_arity_valid("mset", -3, 1));
    // 0 = 不校验
    assert!(s.is_command_arity_valid("x", 0, 100));
  }

  #[test]
  fn latency_metrics_optional_path() {
    let s = session(5);
    // 默认关闭监视
    assert!(s.get_latency_metrics().is_none());
    s.reset_latency_metrics(LatencyMetricsType::NetRsLat);
    s.reset_all_latency_metrics();

    let enabled = RespServerSession::new(
      6,
      RespServerSessionOptions {
        latency_monitor: true,
        ..RespServerSessionOptions::default()
      },
    );
    let metrics = enabled.get_latency_metrics().expect("监视开启时有实例");
    metrics.start(LatencyMetricsType::NetRsLat, 100);
    assert_eq!(metrics.get(LatencyMetricsType::NetRsLat), 100);
    // Stop 记录耗时并清零进行中起点（C# RecordValue 后 start = 0）
    metrics.stop(LatencyMetricsType::NetRsLat, 150);
    assert_eq!(metrics.get(LatencyMetricsType::NetRsLat), 0);
    enabled.reset_all_latency_metrics();
    assert_eq!(metrics.get(LatencyMetricsType::NetRsLat), 0);
  }

  #[test]
  fn debug_and_module_protection() {
    let local = RespServerSessionOptions {
      enable_debug_command: ConnectionProtectionOption::Local,
      enable_module_command: ConnectionProtectionOption::Yes,
      ..RespServerSessionOptions::default()
    };
    let mut s = RespServerSession::new(8, local);
    s.remote_endpoint = "127.0.0.1:55555".to_string();
    assert!(s.can_run_debug());
    assert!(s.can_run_module());

    s.remote_endpoint = "10.0.0.9:1234".to_string();
    assert!(!s.can_run_debug(), "Local 保护拒绝远程");
    assert!(s.can_run_module(), "Yes 保护恒允许");

    let mut closed = RespServerSession::new(
      9,
      RespServerSessionOptions {
        enable_debug_command: ConnectionProtectionOption::No,
        ..RespServerSessionOptions::default()
      },
    );
    closed.remote_endpoint = "127.0.0.1:1".to_string();
    assert!(!closed.can_run_debug());
  }

  #[test]
  fn output_pipeline_and_metrics() {
    let mut s = RespServerSession::new(
      10,
      RespServerSessionOptions {
        metrics_sampling_frequency: true,
        ..RespServerSessionOptions::default()
      },
    );
    s.write_direct_large(b"*2\r\n$3\r\nfoo\r\n");
    assert_eq!(s.pending_output_len(), 13);
    assert!(s.send_and_reset());
    assert!(!s.send_and_reset(), "空缓冲冲洗 = 无进展");
    let total = s.session_metrics.as_ref().unwrap().total_net_output_bytes;
    assert_eq!(total, 13);
  }

  #[test]
  fn client_id_writes_session_id() {
    let mut s = session(777);
    s.parse_state.initialize(0);
    assert!(s.process_other_commands(RespCommand::ClientId));
    assert_eq!(String::from_utf8(s.take_output()).unwrap(), ":777\r\n");

    // 带参数即报参数错误
    s.parse_state.initialize(1);
    assert!(s.process_other_commands(RespCommand::ClientId));
    assert_eq!(
      String::from_utf8(s.take_output()).unwrap(),
      "-ERR wrong number of arguments for 'client|id' command\r\n"
    );
  }

  #[test]
  fn custom_command_arity_gate() {
    let mut s = session(11);
    s.set_command_dispatch(CommandDispatcher::Custom);
    s.parse_state.initialize(2);
    s.current_custom_command = Some((
      RespCommand::Customtxn,
      CustomCommandRef {
        name: "MYTXN".to_string(),
        id: 1,
        arity: 3,
      },
    ));
    // arity 3 → 恰 2 参数 → 通过并清槽
    assert!(s.network_custom_txn());
    assert!(s.current_custom_command.is_none());

    // arity 3 但 1 参数 → 参数错误且清槽
    s.parse_state.initialize(1);
    s.current_custom_command = Some((
      RespCommand::Customprocedure,
      CustomCommandRef {
        name: "MYPROC".to_string(),
        id: 2,
        arity: 3,
      },
    ));
    assert!(s.network_custom_procedure());
    assert!(s.current_custom_command.is_none());
    assert!(
      String::from_utf8(s.take_output())
        .unwrap()
        .contains("MYPROC")
    );
  }

  #[test]
  fn transaction_mode_mirror() {
    let mut s = session(12);
    assert_eq!(s.txn_state, TxnState::None);
    s.set_transaction_mode(true);
    assert_eq!(s.txn_state, TxnState::Running);
    s.txn_state = TxnState::Started;
    assert_eq!(s.txn_state, TxnState::Started);
    s.set_transaction_mode(false);
    assert_eq!(s.txn_state, TxnState::None);
  }

  #[test]
  fn auth_default_user_fallback() {
    let mut s = session(13);
    // NoAuth：无法认证 → 恒 false，但保持默认用户
    assert!(!s.authenticate_user(b"other", b"pwd"));
    assert_eq!(s.user_handle.as_deref(), Some("default"));
    // 显式切换用户句柄（ACL 域接入点）
    s.set_user_handle("admin");
    assert_eq!(s.user_handle.as_deref(), Some("admin"));
  }

  #[test]
  fn no_script_bitmap_sets_discriminants() {
    let (start, bitmap) = RespServerSession::no_script_details();
    assert_eq!(start, 0);
    let bits = u64::BITS as usize;
    for cmd in [
      RespCommand::Eval,
      RespCommand::Evalsha,
      RespCommand::Flushall,
      RespCommand::Flushdb,
      RespCommand::Subscribe,
      RespCommand::Swapdb,
    ] {
      let raw: u16 = cmd.into();
      assert_ne!(
        bitmap[raw as usize / bits] & (1 << (raw as usize % bits)),
        0,
        "{cmd:?} 应置位"
      );
    }
    // 非 NoScript 命令不应置位
    let raw: u16 = RespCommand::Get.into();
    assert_eq!(
      bitmap[raw as usize / bits] & (1 << (raw as usize % bits)),
      0
    );
    // SCRIPT 判别值 278 落于第 5 个字，位图须覆盖
    assert!(bitmap.len() > 4);
  }

  #[test]
  fn eval_roundtrip_via_session() {
    let mut s = RespServerSession::new(
      30,
      RespServerSessionOptions {
        enable_lua: true,
        ..RespServerSessionOptions::default()
      },
    );
    // EVAL "return 'pong'" 0 → 脚本结果写回会话输出
    let frame = b"*3\r\n$4\r\nEVAL\r\n$13\r\nreturn 'pong'\r\n$1\r\n0\r\n";
    let consumed = s.try_consume_messages(frame);
    assert!(consumed.is_some());
    let out = s.take_sent();
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("pong"), "脚本结果应回写: {text}");

    // SCRIPT LOAD + EXISTS 走同一会话面（经解析门控而非直调）
    let out = s.take_sent();
    assert!(out.is_empty());
    let frame = b"*2\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n";
    let consumed = s.try_consume_messages(frame);
    assert!(consumed.is_some());
    let out = s.take_sent();
    assert!(
      String::from_utf8_lossy(&out).contains("ERR"),
      "ScriptLoad 需源码参数: {out:?}"
    );
  }

  #[test]
  fn eval_disabled_rejects() {
    let mut s = RespServerSession::new(
      31,
      RespServerSessionOptions {
        enable_lua: false,
        ..RespServerSessionOptions::default()
      },
    );
    s.parse_state.initialize(2);
    assert!(s.process_other_commands(RespCommand::Eval));
    assert_eq!(
      String::from_utf8(s.take_output()).unwrap(),
      "-ERR Lua is disabled.\r\n"
    );
  }

  #[test]
  fn read_length_prefixed_string_partial_rollback_and_limits() {
    let mut s = session(99);

    // 1. 完整读取并大写化
    let full_frame = b"$4\r\nping\r\n";
    s.recv_buffer.clear();
    s.recv_buffer.extend_from_slice(full_frame);
    s.read_head = 0;
    s.bytes_read = full_frame.len();
    assert_eq!(
      s.get_upper_case_command().as_deref(),
      Some(b"PING".as_slice())
    );
    assert_eq!(s.read_head, full_frame.len());

    // 2. 分包断帧测试：只有前半段 $4\r\npi，不完整时必须返回 None 且 read_head 保持在 0 不动
    let partial_frame = b"$4\r\npi";
    s.recv_buffer.clear();
    s.recv_buffer.extend_from_slice(partial_frame);
    s.read_head = 0;
    s.bytes_read = partial_frame.len();
    assert_eq!(s.get_upper_case_command(), None);
    assert_eq!(s.read_head, 0, "断帧未读全时不得推进游标");

    // 3. 补齐数据后成功解析
    s.recv_buffer.extend_from_slice(b"ng\r\n");
    s.bytes_read = s.recv_buffer.len();
    assert_eq!(
      s.get_upper_case_command().as_deref(),
      Some(b"PING".as_slice())
    );
    assert_eq!(s.read_head, s.recv_buffer.len());

    // 4. 异常帧：无数字长度 $\r\n
    let invalid_frame = b"$\r\n";
    s.recv_buffer.clear();
    s.recv_buffer.extend_from_slice(invalid_frame);
    s.read_head = 0;
    s.bytes_read = invalid_frame.len();
    assert_eq!(s.get_command(), None);
    assert_eq!(s.read_head, 0);
  }

  #[test]
  fn abort_error_message_sanitizes_crlf() {
    let mut s = session(100);
    s.abort_error_message("ERR broken\r\nINJECT");
    assert_eq!(s.take_output(), b"-ERR broken\r\n");
    assert!(s.command_error_written);
  }
}
