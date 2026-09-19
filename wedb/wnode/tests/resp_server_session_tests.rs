//! RESP 服务端会话集成测试（自 src/resp/resp_server_session.rs 单元测试外迁）
//!
//! 验证 RespServerSession 的命令分发、HELLO/AUTH/ACL 鉴权、协议版本升级、
//! 集群门控与重定向、网络泵持久游标与半包断流、事务重放以及 Lua 超时治理。

use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicI64, Ordering},
};

use compio::time::sleep;
use parking_lot::Mutex;
use wacl::{AccessControlList, AclPassword, GarnetAclAuthenticator, User, UserHandle};
use wconf::{DEFAULT_MAX_DATABASES, DEFAULT_RESP_VERSION, NodeArgs};
use wcustom::{CommandType, CustomObjectFns};
use wdev::SegmentedDevice;
use wlua::{LuaOptions, LuaTimeoutManager};
use wmetric::{LatencyMetricsType, SessionMetricsHandle};
use wnode::{
  ClusterProvider, ClusterSessionFace,
  cluster_session::{ClusterSlotVerificationInput, SlotVerifyGate},
  resp::{
    acl_commands::AclAuthOutcome,
    acl_store::AclStore,
    garnet_api::{GarnetApiFace, StoreGarnetApi, TxnProcRun},
    resp_server_session::{
      ConnectionProtectionOption, CustomCommandRef, RespServerSession, RespServerSessionOptions,
    },
    slow_path::SlowFuture,
  },
  service::spawn_lua_timeout_tick,
};
use wnode_test::drain_output;
use wpubsub::subscribe_broker::SubscribeBroker;
use wresp::{
  catalog::{RespCommandFlags, try_get_resp_command_info_by_cmd},
  command::RespCommand,
};
use wtxn::{TxnLockTable, TxnState, WatchVersionMap};
use wval::CustomObjectType;

fn session(id: i64) -> RespServerSession {
  RespServerSession::new(id, RespServerSessionOptions::default())
}

/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）
fn pump_feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  s.try_consume_messages()
}

fn session_frame(frame: &[u8]) -> (RespServerSession, Vec<u8>) {
  let mut s = session(0);
  let consumed = pump_feed(&mut s, frame);
  assert!(consumed.is_some());
  let out = drain_output(&mut s);
  (s, out)
}

/// 挂载真实存储执行域的会话帧消费（AUTH / ACL 族经底层存储点查，
/// 须与生产装配同径；返回 TempDir 保活数据文件）
fn session_frame_with_store(frame: &[u8]) -> (RespServerSession, Vec<u8>, tempfile::TempDir) {
  let (dir, store) = wtest_base::open_test_store("resp-session-frame.db").unwrap();
  let mut s = session(0);
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  let consumed = pump_feed(&mut s, frame);
  assert!(consumed.is_some());
  let out = drain_output(&mut s);
  (s, out, dir)
}

/// ACL 档测试座：default 用户表（+@all nopass）+ 认证器挂载（attach 自动认证）
fn acl_session(id: i64) -> (RespServerSession, Arc<Mutex<GarnetAclAuthenticator>>) {
  let authenticator = Arc::new(Mutex::new(GarnetAclAuthenticator::new(Arc::new(
    AccessControlList::default(),
  ))));
  let mut s = session(id);
  s.attach_acl(Some(Arc::clone(&authenticator)));
  (s, authenticator)
}

/// 桩切面：记录只读写态与批纪元快照并按开关注写 MOVED
struct StubClusterSession {
  read_only: AtomicBool,
  redirect: AtomicBool,
  disposed: AtomicBool,
  local_epoch: AtomicI64,
  /// gossip 对端节点 id（None = 普通连接；驱动 CLIENT 类型门测试）
  remote_node_id: Mutex<Option<u128>>,
}

impl StubClusterSession {
  fn new() -> Self {
    Self::with_remote(None)
  }

  fn with_remote(remote_node_id: Option<u128>) -> Self {
    Self {
      read_only: AtomicBool::new(false),
      redirect: AtomicBool::new(false),
      disposed: AtomicBool::new(false),
      local_epoch: AtomicI64::new(0),
      remote_node_id: Mutex::new(remote_node_id),
    }
  }
}

impl ClusterSessionFace for StubClusterSession {
  fn set_read_only_session(&self) {
    self.read_only.store(true, Ordering::Relaxed);
  }

  fn set_read_write_session(&self) {
    self.read_only.store(false, Ordering::Relaxed);
  }

  fn remote_node_id(&self) -> Option<u128> {
    *self.remote_node_id.lock()
  }

  fn local_current_epoch(&self) -> i64 {
    self.local_epoch.load(Ordering::Relaxed)
  }

  fn acquire_current_epoch(&self) {
    self.local_epoch.store(1, Ordering::Relaxed);
  }

  fn release_current_epoch(&self) {
    self.local_epoch.store(0, Ordering::Relaxed);
  }

  fn network_multi_key_slot_verify(
    &self,
    _input: &ClusterSlotVerificationInput<'_>,
    _args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> SlotVerifyGate {
    if self.redirect.load(Ordering::Relaxed) {
      output.extend_from_slice(b"-MOVED 0 stub:1\r\n");
      return SlotVerifyGate::Redirected;
    }
    SlotVerifyGate::Serve
  }

  fn process_cluster_commands(
    &self,
    _cmd: RespCommand,
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _slot: u16,
  ) -> bool {
    output.extend_from_slice(b"+STUBCLUSTER\r\n");
    true
  }

  fn dispose(&self) {
    self.disposed.store(true, Ordering::Relaxed);
  }
}

/// provider 桩：远端节点角色可控（trait 其余方法全默认体，
/// 仅覆盖 CLIENT 类型门消费的 is_replica_node）
struct StubProvider {
  replica_node: AtomicBool,
}

impl ClusterProvider for StubProvider {
  fn is_replica_node(&self, _node_id: u128) -> bool {
    self.replica_node.load(Ordering::Relaxed)
  }
}

/// 装配指定集群形态的会话并取 CLIENT INFO 的 flags 字符
/// （BasicCommands.cs:1968-1986 类型门矩阵的会话侧全链驱动）
fn client_info_flags(
  attach_cluster: bool,
  remote_node_id: Option<u128>,
  attach_provider: bool,
  replica_peer: bool,
) -> String {
  let mut s = session(60);
  if attach_cluster {
    s.attach_cluster_session(Arc::new(StubClusterSession::with_remote(remote_node_id)));
  }
  if attach_provider {
    let provider = StubProvider {
      replica_node: AtomicBool::new(replica_peer),
    };
    s.attach_cluster_provider(Arc::new(provider));
  }
  assert!(pump_feed(&mut s, b"*2\r\n$6\r\nCLIENT\r\n$4\r\nINFO\r\n").is_some());
  let out = String::from_utf8(drain_output(&mut s)).unwrap();
  out
    .split_once(" flags=")
    .and_then(|(_, rest)| rest.split(' ').next())
    .unwrap_or_default()
    .to_string()
}

/// CLIENT 类型门按 gossip 对端判定（普通客户端不再误标 M/S）
#[test]
fn client_type_gate_by_remote_node_id() {
  // 集群形态普通客户端（切面在位、无 gossip 链）：N——本节点角色不参与
  assert_eq!(client_info_flags(true, None, true, false), "N");
  assert_eq!(client_info_flags(true, None, true, true), "N");
  // gossip 链确立：按远端节点角色出 M/S，互斥
  let master_peer = client_info_flags(true, Some(7), true, false);
  let replica_peer = client_info_flags(true, Some(7), true, true);
  assert_eq!(master_peer, "M");
  assert_eq!(replica_peer, "S");
  // provider 缺席回落 N（C# clusterProvider is not null 双门）
  assert_eq!(client_info_flags(true, Some(7), false, false), "N");
  // 单机形态：N
  assert_eq!(client_info_flags(false, None, false, false), "N");
  // 普通连接（N）与 gossip 连接（M/S）类型互异 → TYPE 过滤命中互斥
  assert_ne!(master_peer, "N");
  assert_ne!(replica_peer, "N");
}

/// 集群形态订阅会话（gossip 未确立）按 P，不再被误标 M/S
///
/// 订阅模式命令门按 C# 只在 RESP2 生效（RespServerSession.cs:657
/// `isSubscriptionSession && respProtocolVersion == 2 &&
/// !cmd.IsAllowedInSubscriptionMode()`；允许集 RespCommand.cs:733-742 确无
/// CLIENT_INFO，故 RESP2 订阅态下 CLIENT INFO 必被拒）。本用例考的是类型门
/// 而非命令门，按 C# 「RESP3 uses distinct push types... all commands are
/// valid」以 RESP3 会话驱动。
#[test]
fn client_type_gate_pubsub_without_gossip_link() {
  let mut s = session(61);
  s.attach_cluster_session(Arc::new(StubClusterSession::new()));
  s.is_subscription_session = true;
  s.resp_protocol_version = 3;
  assert!(pump_feed(&mut s, b"*2\r\n$6\r\nCLIENT\r\n$4\r\nINFO\r\n").is_some());
  let out = String::from_utf8(drain_output(&mut s)).unwrap();
  assert!(out.contains(" flags=P "), "unexpected CLIENT INFO: {out}");
}

/// NodeArgs → 会话选项基线单点投影（对标 C# Options.cs:771 GetServerOptions；
/// 原单机 main 与集群 boot 两份手抄映射的收口断言）
#[test]
fn node_args_projects_session_options() {
  // 缺省：库数取配置默认、AOF/延迟/统计/Lua 全关、无超时管理器
  let opts = RespServerSessionOptions::from(&NodeArgs::default());
  assert_eq!(DEFAULT_MAX_DATABASES as u64, opts.max_databases);
  assert!(!opts.latency_monitor);
  assert!(!opts.command_stats_monitor);
  assert!(!opts.enable_lua);
  assert!(!opts.enable_aof);
  assert!(opts.lua_timeout_manager.is_none());
  // WaitForCommit 参数源 `--aof-commit-wait`，缺省关（C# 默认同值）
  assert!(!opts.wait_for_commit);
  // DEBUG 连接保护档缺省 No（C# 枚举零值，Options.cs:594 → :1018 投影链）
  assert_eq!(ConnectionProtectionOption::No, opts.enable_debug_command);

  // 旋钮全量透传；负值钳 0（max_databases 下界、Lua 超时非负）
  let node = NodeArgs {
    max_databases: -1,
    latency_monitor: true,
    commandstats_monitor: true,
    enable_lua: true,
    lua_script_timeout_ms: -5,
    lua_transaction_mode: true,
    aof: true,
    aof_commit_wait: true,
    enable_debug_command: ConnectionProtectionOption::Yes,
    ..NodeArgs::default()
  };
  let opts = RespServerSessionOptions::from(&node);
  assert_eq!(0, opts.max_databases);
  // DEBUG 保护档透传（会话门 can_run_debug 的取值源，恒 No 隐式缺省已消除）
  assert_eq!(ConnectionProtectionOption::Yes, opts.enable_debug_command);
  assert!(opts.latency_monitor);
  assert!(opts.command_stats_monitor);
  assert!(opts.enable_lua);
  assert_eq!(0, opts.lua_options.timeout_millis);
  assert!(opts.lua_txn_mode);
  assert!(opts.enable_aof);
  // AOF 提交等待档透传（会话门 aof_commit_mode_gate 的第二个入参源）
  assert!(opts.wait_for_commit);
  // 超时非正 = C# InfiniteTimeSpan 语义，不建管理器（须在 compio 运行时内
  // 方可装配正超时，本用例走零超时分支）
  assert!(opts.lua_timeout_manager.is_none());
}

#[test]
fn dispatch_routes_via_garnet_api_injection() {
  struct EchoApi;
  impl GarnetApiFace for EchoApi {
    fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, _args: &[&[u8]]) {
      session.write_direct_large(b"+OK\r\n");
      assert_eq!(cmd, RespCommand::Get);
    }

    fn exec_slow(
      self: Arc<Self>,
      _cmd: RespCommand,
      _args: Vec<Vec<u8>>,
      _resp_version: u8,
    ) -> SlowFuture {
      SlowFuture::new(async { Vec::new() })
    }

    // RESP 分派面 stub：本测试只验证 exec 注入路由，事务过程驱动不在断言面
    fn run_txn_proc(&self, _run: TxnProcRun<'_>) -> bool {
      false
    }
  }
  let mut s = session(20);
  s.set_garnet_api(Arc::new(EchoApi));
  s.parse_state.initialize(1);
  assert!(s.process_array_commands(RespCommand::Get));
  assert_eq!(String::from_utf8(drain_output(&mut s)).unwrap(), "+OK\r\n");
}

#[test]
fn dispatch_without_api_reports_error() {
  let mut s = session(21);
  s.parse_state.initialize(1);
  assert!(s.process_array_commands(RespCommand::Get));
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "-ERR store execution domain not attached\r\n"
  );
}

#[test]
fn client_info_carries_real_state() {
  let mut s = session(7);
  s.remote_endpoint = "127.0.0.1:6380".to_string();
  s.set_client_name(Some("loader"));
  s.set_client_lib_info(Some("redis-py"), Some("5.0.1"));

  let mut out = Vec::new();
  assert!(s.process_hello_command_state::<SegmentedDevice>(
    Some(3),
    b"",
    b"",
    None,
    None,
    &mut out
  ));
  let text = String::from_utf8(out).unwrap();
  assert!(text.starts_with("%8\r\n"), "RESP3 map 头 expected: {text}");
  assert!(
    text.contains("$5\r\nproto\r\n:3\r\n"),
    "resp=3 expected: {text}"
  );
  assert!(
    text.contains("$2\r\nid\r\n:7\r\n"),
    "真实 Id expected: {text}"
  );

  let mut info = String::new();
  s.write_client_info_state(&mut info);
  // 字段序对标 C# WriteClientInfo：id addr laddr [name] age [user] flags …
  // name/user 为条件段（C# BasicCommands.cs:1958/:1967），SETNAME/AUTH 后必出
  assert_eq!(
    info,
    "id=7 addr=127.0.0.1:6380 laddr= name=loader age=0 user=default flags=N db=0 resp=3 lib-name=redis-py lib-ver=5.0.1 pubsub-dropped=0"
  );
}

#[test]
fn hello_resp2_keeps_doubled_array_header() {
  let mut s = session(9);
  let mut out = Vec::new();
  assert!(s.process_hello_command_state::<SegmentedDevice>(None, b"", b"", None, None, &mut out));
  let text = String::from_utf8(out).unwrap();
  assert!(text.starts_with("*16\r\n"), "RESP2 数组头 expected: {text}");
}

#[test]
fn database_id_validates_against_session_max_databases() {
  let mut s = RespServerSession::new(
    40,
    RespServerSessionOptions {
      max_databases: 4,
      ..RespServerSessionOptions::default()
    },
  );

  let mut out = Vec::new();
  assert!(!s.try_parse_database_id(b"abc", &mut out));
  assert_eq!(
    String::from_utf8(out).unwrap(),
    "-ERR value is not an integer or out of range.\r\n"
  );

  for dbid in ["4", "5", "-1"] {
    let mut out = Vec::new();
    assert!(!s.try_parse_database_id(dbid.as_bytes(), &mut out));
    assert_eq!(
      String::from_utf8(out).unwrap(),
      "-ERR DB index is out of range.\r\n"
    );
  }

  // 线面值域按 C# TryGetInt int32 档（AdminCommands.cs:TryParseDatabaseId →
  // ParseUtils.cs:TryReadInt）逐组对位：超 int32 值域属「不是整数」档，
  // int32 域内负数与域内越界才走「库号越界」档；前导零与尾随垃圾同非整数档
  for (dbid, want) in [
    ("2147483647", "-ERR DB index is out of range.\r\n"),
    (
      "2147483648",
      "-ERR value is not an integer or out of range.\r\n",
    ),
    (
      "3000000000",
      "-ERR value is not an integer or out of range.\r\n",
    ),
    (
      "-2147483649",
      "-ERR value is not an integer or out of range.\r\n",
    ),
    ("-2147483648", "-ERR DB index is out of range.\r\n"),
    ("003", "-ERR value is not an integer or out of range.\r\n"),
    ("3x", "-ERR value is not an integer or out of range.\r\n"),
  ] {
    let mut out = Vec::new();
    assert!(!s.try_parse_database_id(dbid.as_bytes(), &mut out));
    assert_eq!(String::from_utf8(out).unwrap(), want, "DBID {dbid}");
  }

  let mut out = Vec::new();
  assert!(s.try_parse_database_id(b"3", &mut out));
  assert!(out.is_empty());
}

#[test]
fn database_id_allows_non_zero_in_cluster_mode() {
  let stub = Arc::new(StubClusterSession::new());
  let mut s = session(41);
  s.attach_cluster_session(stub);

  let mut out = Vec::new();
  assert!(s.try_parse_database_id(b"1", &mut out));
  assert!(out.is_empty());
}

#[test]
fn hello_rejects_auth_on_noauth() {
  let mut s = session(1);
  let mut out = Vec::new();
  assert!(!s.process_hello_command_state::<SegmentedDevice>(
    Some(3),
    b"alice",
    b"secret",
    None,
    None,
    &mut out
  ));
  assert_eq!(
    String::from_utf8(out).unwrap(),
    "-WRONGPASS Invalid username/password combination\r\n"
  );
  assert_eq!(s.resp_protocol_version, DEFAULT_RESP_VERSION);
}

#[test]
fn hello_auth_with_acl_credentials() {
  let (mut s, authenticator) = acl_session(2);
  // alice 规则落存储记录（命名用户权威在 KeyTag::Acl 点查，无内存用户表）
  let (_dir, store) = wtest_base::open_test_store("resp-hello-acl.db").unwrap();
  let acl_store_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_store_session);
  let mut user = User::new("alice".to_string());
  user.set_enabled(true);
  user.add_password_hash(AclPassword::from_string("secret"));
  acl_store.write(0, b"alice", &user.to_bytes()).unwrap();

  let mut out = Vec::new();
  assert!(s.process_hello_command_state::<SegmentedDevice>(
    Some(3),
    b"alice",
    b"secret",
    Some("myclient"),
    Some(&acl_store),
    &mut out
  ));
  assert_eq!(s.resp_protocol_version, 3);
  assert_eq!(s.client_name.as_deref(), Some("myclient"));
  assert_eq!(s.user_handle.as_deref(), Some("alice"));
  let text = String::from_utf8(out).unwrap();
  assert!(text.contains("$5\r\nproto\r\n:3\r\n"));

  let mut s2 = session(3);
  s2.attach_acl(Some(authenticator));
  let mut out2 = Vec::new();
  assert!(!s2.process_hello_command_state::<SegmentedDevice>(
    Some(3),
    b"alice",
    b"wrongpwd",
    Some("newclient"),
    Some(&acl_store),
    &mut out2
  ));
  assert_eq!(
    String::from_utf8(out2).unwrap(),
    "-WRONGPASS Invalid username/password combination\r\n"
  );
  assert_eq!(s2.resp_protocol_version, DEFAULT_RESP_VERSION);
  assert_eq!(s2.client_name, None);
}

#[test]
fn database_switch_lifecycle() {
  let mut s = session(1);
  assert_eq!(s.active_db_id, 0);

  assert!(s.try_switch_active_database_session(2));
  assert_eq!(s.active_db_id, 2);

  assert!(!s.try_switch_active_database_session(999));
}

#[test]
fn arity_validation_writes_error() {
  let mut s = session(4);
  assert!(s.is_command_arity_valid("get", 2, 1));
  assert!(!s.is_command_arity_valid("get", 2, 2));
  let text = String::from_utf8(drain_output(&mut s)).unwrap();
  assert_eq!(text, "-ERR wrong number of arguments for 'get' command\r\n");
  assert!(s.is_command_arity_valid("mset", -3, 2));
  assert!(!s.is_command_arity_valid("mset", -3, 1));
  assert!(s.is_command_arity_valid("x", 0, 100));
}

#[test]
fn latency_metrics_optional_path() {
  let s = session(5);
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
  metrics.stop(LatencyMetricsType::NetRsLat, 150);
  assert_eq!(metrics.get(LatencyMetricsType::NetRsLat), 0);
  enabled.reset_all_latency_metrics();
  assert_eq!(metrics.get(LatencyMetricsType::NetRsLat), 0);
}

#[test]
fn debug_protection() {
  let local = RespServerSessionOptions {
    enable_debug_command: ConnectionProtectionOption::Local,
    ..RespServerSessionOptions::default()
  };
  let mut s = RespServerSession::new(8, local);
  s.remote_endpoint = "127.0.0.1:55555".to_string();
  assert!(s.can_run_debug());

  s.remote_endpoint = "10.0.0.9:1234".to_string();
  assert!(!s.can_run_debug(), "Local 保护拒绝远程");

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
  let mut s = RespServerSession::new(10, RespServerSessionOptions::default());
  // 会话指标句柄经唯一注入口装配（生产由 service.rs 采样门控创建后同口注入）
  s.attach_session_metrics(Some(Arc::new(SessionMetricsHandle::default())));
  s.write_direct_large(b"*2\r\n$3\r\nfoo\r\n");
  assert_eq!(s.pending_output_len(), 13);
  let mut out = Vec::new();
  s.take_output_into(&mut out);
  assert_eq!(out, b"*2\r\n$3\r\nfoo\r\n");
  assert_eq!(s.pending_output_len(), 0);
  // 空缓冲再冲：零字节入目标缓冲、零记账（C# SendAndReset 的「无进展即抛」
  // 探针在可扩容托管缓冲下无从触发）
  let mut again = Vec::new();
  s.take_output_into(&mut again);
  assert!(again.is_empty(), "空缓冲冲出 = 零字节");
  let total = s
    .session_metrics
    .as_ref()
    .unwrap()
    .snapshot()
    .total_net_output_bytes;
  assert_eq!(total, 13);
}

/// arity 门控测试桩执行体（恒成功空操作；仅测试域使用）
fn stub_fns() -> CustomObjectFns {
  fn ok_initial(_args: &[&[u8]], _output: &mut Vec<u8>, _resp_version: u8) -> bool {
    true
  }
  fn ok_update(
    _payload: &mut Vec<u8>,
    _args: &[&[u8]],
    _output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    true
  }
  fn ok_read(_payload: &[u8], _args: &[&[u8]], _output: &mut Vec<u8>, _resp_version: u8) -> bool {
    true
  }
  fn ok_missing(_args: &[&[u8]], _output: &mut Vec<u8>, _resp_version: u8) {}
  fn never_empty(_payload: &[u8]) -> bool {
    false
  }
  CustomObjectFns {
    need_initial_update: ok_initial,
    updater: ok_update,
    reader: ok_read,
    not_found: ok_missing,
    is_empty: never_empty,
  }
}

#[test]
fn custom_command_arity_gate() {
  let mut s = session(11);
  s.parse_state.initialize(2);
  s.current_custom_command = Some((
    RespCommand::Customtxn,
    CustomCommandRef {
      name: "MYTXN",
      command_type: CommandType::Read,
      arity: 3,
      object_tag: CustomObjectType::Roaring.as_u8(),
      fns: stub_fns(),
    },
  ));
  assert!(s.run_custom_command());
  assert!(s.current_custom_command.is_none());
  assert!(
    String::from_utf8(drain_output(&mut s))
      .unwrap()
      .contains("unknown command")
  );

  s.parse_state.initialize(1);
  s.current_custom_command = Some((
    RespCommand::Customprocedure,
    CustomCommandRef {
      name: "MYPROC",
      command_type: CommandType::Read,
      arity: 3,
      object_tag: CustomObjectType::Roaring.as_u8(),
      fns: stub_fns(),
    },
  ));
  assert!(s.run_custom_command());
  assert!(s.current_custom_command.is_none());
  assert!(
    String::from_utf8(drain_output(&mut s))
      .unwrap()
      .contains("MYPROC")
  );
}

#[test]
fn readonly_writes_through_cluster_aspect() {
  let stub = Arc::new(StubClusterSession::new());
  let mut s = session(30);
  s.attach_cluster_session(stub.clone());
  s.parse_state.initialize(0);
  assert!(s.process_basic_commands(RespCommand::Readonly));
  assert_eq!(String::from_utf8(drain_output(&mut s)).unwrap(), "+OK\r\n");
  assert!(stub.read_only.load(Ordering::Relaxed));
  assert!(s.read_only_session);

  s.parse_state.initialize(0);
  assert!(s.process_basic_commands(RespCommand::Readwrite));
  assert!(!stub.read_only.load(Ordering::Relaxed));
  assert!(!s.read_only_session);
}

#[test]
fn cluster_gate_redirects_data_command() {
  let stub = Arc::new(StubClusterSession::new());
  stub.redirect.store(true, Ordering::Relaxed);
  let mut s = session(31);
  s.attach_cluster_session(stub.clone());
  assert!(pump_feed(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "-MOVED 0 stub:1\r\n"
  );
}

#[test]
fn cluster_gate_passes_local_data_command() {
  let stub = Arc::new(StubClusterSession::new());
  let mut s = session(32);
  s.attach_cluster_session(stub.clone());
  assert!(pump_feed(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  let sent = String::from_utf8(drain_output(&mut s)).unwrap();
  assert!(!sent.starts_with("-MOVED"));
  assert_eq!(sent, "-ERR store execution domain not attached\r\n");
}

#[test]
fn cluster_command_routes_to_aspect() {
  let stub = Arc::new(StubClusterSession::new());
  let mut s = session(33);
  s.attach_cluster_session(stub.clone());
  assert!(pump_feed(&mut s, b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n").is_some());
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "+STUBCLUSTER\r\n"
  );
}

#[test]
fn dispose_releases_cluster_aspect() {
  let stub = Arc::new(StubClusterSession::new());
  let mut s = session(34);
  s.attach_cluster_session(stub.clone());
  s.dispose();
  assert!(stub.disposed.load(Ordering::Relaxed));
  assert!(s.cluster_session.is_none());
}

#[test]
fn auth_default_user_fallback() {
  let mut s = session(13);
  assert!(!s.authenticate_user(b"other", b"pwd"));
  assert_eq!(s.user_handle.as_deref(), Some("default"));
  assert!(s.acl_permits(RespCommand::Get));

  let acl = Arc::new(AccessControlList::new("").unwrap());
  s.attach_acl(Some(Arc::new(Mutex::new(GarnetAclAuthenticator::new(acl)))));
  let handle = Arc::new(UserHandle::new(Arc::new(User::new("admin".into()))));
  s.set_user_handle(handle);
  assert_eq!(s.user_handle.as_deref(), Some("admin"));
  assert!(!s.acl_permits(RespCommand::Get));

  assert!(!s.authenticate_user(b"invalid", b"badpwd"));
  assert_eq!(
    s.user_handle.as_deref(),
    Some("admin"),
    "认证失败保留原已认证用户"
  );
}

#[test]
fn acl_default_user_allows_all() {
  let (mut s, _) = acl_session(40);
  assert!(s.acl_user_handle.is_some(), "default 用户自动认证挂载");
  assert!(s.check_acl_permissions(RespCommand::Get));
  assert!(s.check_acl_permissions(RespCommand::Set));

  assert!(pump_feed(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "-ERR store execution domain not attached\r\n"
  );
}

#[test]
fn acl_limited_user_filters_commands() {
  let (mut s, _authenticator) = acl_session(41);
  // limited 规则落存储记录，经存储点查认证（命名用户权威在 KeyTag::Acl）
  let (_dir, store) = wtest_base::open_test_store("resp-session-limited.db").unwrap();
  let acl_store_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_store_session);
  let mut user = User::new("limited".to_string());
  user.add_command(RespCommand::Get).unwrap();
  user.set_enabled(true);
  user.add_password_hash(AclPassword::from_string("pwd"));
  acl_store.write(0, b"limited", &user.to_bytes()).unwrap();

  let outcome = s.authenticate_user_via_store(&acl_store, b"limited", b"pwd");
  let AclAuthOutcome::Success(handle, _) = outcome else {
    panic!("limited 认证必须成功");
  };
  s.set_user_handle(handle);

  assert!(s.check_acl_permissions(RespCommand::Get));
  assert!(!s.check_acl_permissions(RespCommand::Set), "位图外命令拒绝");

  assert!(pump_feed(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").is_some());
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "-NOPERM this user has no permissions to run the command\r\n"
  );

  assert!(pump_feed(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "-ERR store execution domain not attached\r\n"
  );
}

#[test]
fn acl_unauthenticated_rejects_with_noauth() {
  let authenticator = Arc::new(Mutex::new(GarnetAclAuthenticator::new(Arc::new(
    AccessControlList::new("pwd").unwrap(),
  ))));
  let mut s = session(42);
  s.attach_acl(Some(Arc::clone(&authenticator)));
  assert!(s.acl_user_handle.is_none());

  assert!(!s.check_acl_permissions(RespCommand::Get));
  assert!(pump_feed(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "-NOAUTH Authentication required.\r\n"
  );

  assert!(s.check_acl_permissions(RespCommand::Quit));
}

#[test]
fn acl_no_auth_authenticator_allows_all() {
  let mut s = session(43);
  assert!(s.acl_user_handle.is_none());
  assert!(s.check_acl_permissions(RespCommand::Get));
  assert!(s.check_acl_permissions(RespCommand::Set));
  // redis.acl_check_cmd 链路的会话单点：成帧请求经独立缓冲解析出枚举
  assert_eq!(
    s.parse_resp_command_buffer(b"*1\r\n$3\r\nGET\r\n"),
    Some(RespCommand::Get)
  );
}

#[test]
fn no_script_gate_blocks_script_phase_commands() {
  let mut s = session(44);
  assert!(s.check_script_permissions(RespCommand::Subscribe));

  s.attach_no_script_bitmap();
  assert!(!s.check_script_permissions(RespCommand::Subscribe));
  assert!(!s.check_script_permissions(RespCommand::Eval));
  assert!(!s.check_script_permissions(RespCommand::Evalsha));
  assert!(s.check_script_permissions(RespCommand::Get));
  assert!(s.check_script_permissions(RespCommand::ScriptExists));
  assert!(s.check_script_permissions(RespCommand::AclCat));

  s.no_script_bitmap = None;
  assert!(s.check_script_permissions(RespCommand::Subscribe));
}

#[test]
fn script_window_scopes_no_script_gate() {
  let mut s = RespServerSession::new(
    45,
    RespServerSessionOptions {
      enable_lua: true,
      ..RespServerSessionOptions::default()
    },
  );
  let frame = b"*3\r\n$4\r\nEVAL\r\n$35\r\nreturn redis.call('SUBSCRIBE','ch')\r\n$1\r\n0\r\n";
  assert!(pump_feed(&mut s, frame).is_some());
  let out = drain_output(&mut s);
  let text = String::from_utf8_lossy(&out);
  assert!(
    text.contains("not allowed from script"),
    "脚本内 SUBSCRIBE 应回 NOSCRIPT: {text}"
  );

  assert!(pump_feed(&mut s, b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n").is_some());
  let out = drain_output(&mut s);
  let text = String::from_utf8_lossy(&out);
  assert!(
    text.contains("SUBSCRIBE is disabled"),
    "窗口外 SUBSCRIBE 应被门放行（回订阅禁用文案）: {text}"
  );
  assert!(!s.is_subscription_session);
}

#[test]
fn no_script_bitmap_sets_discriminants() {
  let (start, bitmap) = RespServerSession::no_script_details();

  let info =
    |cmd: RespCommand| try_get_resp_command_info_by_cmd(cmd, false).expect("命令元数据已导入");
  let stepped = |cmd: RespCommand| u16::from(cmd) as usize - start as usize;

  for cmd in [
    RespCommand::Eval,
    RespCommand::Evalsha,
    RespCommand::Watch,
    RespCommand::Subscribe,
    RespCommand::Async,
    RespCommand::AclCat,
  ] {
    assert!(
      info(cmd).flags.intersects(RespCommandFlags::NO_SCRIPT),
      "{cmd:?} 前置：元数据须带 NoScript 标志"
    );
    let byte = stepped(cmd);
    assert_ne!(bitmap[byte / 8] & (1 << (byte % 8)), 0, "{cmd:?} 应置位");
  }
  let byte = stepped(RespCommand::Info);
  assert_eq!(bitmap[byte / 8] & (1 << (byte % 8)), 0);
  assert!(bitmap.len() * 8 > stepped(RespCommand::Watchos));
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
  let frame = b"*3\r\n$4\r\nEVAL\r\n$13\r\nreturn 'pong'\r\n$1\r\n0\r\n";
  let consumed = pump_feed(&mut s, frame);
  assert!(consumed.is_some());
  let out = drain_output(&mut s);
  let text = String::from_utf8_lossy(&out);
  assert!(text.contains("pong"), "脚本结果应回写: {text}");

  let out = drain_output(&mut s);
  assert!(out.is_empty());
  let frame = b"*2\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n";
  let consumed = pump_feed(&mut s, frame);
  assert!(consumed.is_some());
  let out = drain_output(&mut s);
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
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "-ERR This instance has Lua scripting support disabled\r\n"
  );
}

#[test]
fn eval_infinite_loop_times_out_and_session_recovers() {
  use std::{
    thread,
    time::{Duration, Instant},
  };

  let manager = Arc::new(LuaTimeoutManager::new(50));
  let mut s = RespServerSession::new(
    32,
    RespServerSessionOptions {
      enable_lua: true,
      lua_timeout_manager: Some(Arc::clone(&manager)),
      ..RespServerSessionOptions::default()
    },
  );

  let stop = Arc::new(AtomicBool::new(false));
  let ticker = {
    let (manager, stop) = (Arc::clone(&manager), Arc::clone(&stop));
    thread::spawn(move || {
      while !stop.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(5));
        manager.tick();
      }
    })
  };

  let frame = b"*3\r\n$4\r\nEVAL\r\n$17\r\nwhile true do end\r\n$1\r\n0\r\n";
  let start = Instant::now();
  let consumed = pump_feed(&mut s, frame);
  let elapsed = start.elapsed();
  stop.store(true, Ordering::Relaxed);
  ticker.join().unwrap();

  assert!(consumed.is_some());
  assert!(
    elapsed >= Duration::from_millis(50),
    "中断早于配置超时: {elapsed:?}"
  );
  assert!(
    elapsed < Duration::from_secs(10),
    "死循环未被中断（会话挂死）: {elapsed:?}"
  );
  assert_eq!(
    String::from_utf8_lossy(&drain_output(&mut s)),
    "-ERR Lua script exceeded configured timeout\r\n"
  );

  let frame = b"*3\r\n$4\r\nEVAL\r\n$9\r\nreturn 42\r\n$1\r\n0\r\n";
  assert!(pump_feed(&mut s, frame).is_some());
  assert_eq!(String::from_utf8_lossy(&drain_output(&mut s)), ":42\r\n");
  let frame = b"*1\r\n$4\r\nPING\r\n";
  assert!(pump_feed(&mut s, frame).is_some());
  assert_eq!(String::from_utf8_lossy(&drain_output(&mut s)), "+PONG\r\n");
}

#[test]
fn eval_dead_loop_interrupted_by_runtime_tick_task() {
  use std::{thread, time::Duration};

  use compio::runtime::Runtime;

  let manager = Arc::new(LuaTimeoutManager::new(40));
  let mut s = RespServerSession::new(
    35,
    RespServerSessionOptions {
      enable_lua: true,
      lua_timeout_manager: Some(Arc::clone(&manager)),
      ..RespServerSessionOptions::default()
    },
  );

  let frame = b"*3\r\n$4\r\nEVAL\r\n$17\r\nwhile true do end\r\n$1\r\n0\r\n".to_vec();
  let session_thread = thread::spawn(move || {
    let consumed = pump_feed(&mut s, &frame);
    (consumed.is_some(), drain_output(&mut s))
  });

  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    spawn_lua_timeout_tick(manager);
    while !session_thread.is_finished() {
      sleep(Duration::from_millis(5)).await;
    }
  });

  let (consumed, out) = session_thread.join().unwrap();
  assert!(consumed);
  assert_eq!(
    String::from_utf8_lossy(&out),
    "-ERR Lua script exceeded configured timeout\r\n"
  );
}

#[test]
fn eval_without_timeout_runs_finite_heavy_loop() {
  let mut s = RespServerSession::new(
    33,
    RespServerSessionOptions {
      enable_lua: true,
      ..RespServerSessionOptions::default()
    },
  );
  let script = b"local s = 0 for i = 1, 100000 do s = s + i end return s";
  let frame = format!(
    "*3\r\n$4\r\nEVAL\r\n${}\r\n{}\r\n$1\r\n0\r\n",
    script.len(),
    String::from_utf8_lossy(script)
  );
  assert!(pump_feed(&mut s, frame.as_bytes()).is_some());
  assert_eq!(
    String::from_utf8_lossy(&drain_output(&mut s)),
    ":5000050000\r\n"
  );
}

#[test]
fn lua_options_and_txn_mode_assembled_from_config() {
  let mut s = RespServerSession::new(
    34,
    RespServerSessionOptions {
      enable_lua: true,
      lua_txn_mode: true,
      lua_options: LuaOptions {
        timeout_millis: 1_234,
        ..LuaOptions::default()
      },
      ..RespServerSessionOptions::default()
    },
  );
  assert!(s.lua_txn_mode);
  assert_eq!(s.lua_options.timeout_millis, 1_234);

  let script = b"return redis.call('PING')";
  let frame = format!(
    "*4\r\n$4\r\nEVAL\r\n${}\r\n{}\r\n$1\r\n1\r\n$1\r\nk\r\n",
    script.len(),
    String::from_utf8_lossy(script)
  );
  assert!(pump_feed(&mut s, frame.as_bytes()).is_some());
  assert_eq!(
    String::from_utf8_lossy(&drain_output(&mut s)),
    "$4\r\nPONG\r\n"
  );
}

#[test]
fn read_length_prefixed_string_partial_rollback_and_limits() {
  let mut s = session(99);

  let full_frame = b"$4\r\nping\r\n";
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(full_frame);
  s.read_head = 0;
  s.bytes_read = full_frame.len();
  let (start, len) = s.get_upper_case_command_range().unwrap();
  assert_eq!(&s.recv_buffer[start..start + len], b"PING");
  assert_eq!(s.read_head, full_frame.len());

  let partial_frame = b"$4\r\npi";
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(partial_frame);
  s.read_head = 0;
  s.bytes_read = partial_frame.len();
  assert!(s.get_upper_case_command_range().is_none());
  assert_eq!(s.read_head, 0, "断帧未读全时不得推进游标");

  s.recv_buffer.extend_from_slice(b"ng\r\n");
  s.bytes_read = s.recv_buffer.len();
  let (start, len) = s.get_upper_case_command_range().unwrap();
  assert_eq!(&s.recv_buffer[start..start + len], b"PING");
  assert_eq!(s.read_head, s.recv_buffer.len());

  let invalid_frame = b"$\r\n";
  s.recv_buffer.clear();
  s.recv_buffer.extend_from_slice(invalid_frame);
  s.read_head = 0;
  s.bytes_read = invalid_frame.len();
  assert!(s.get_command_range().is_none());
  assert_eq!(s.read_head, 0);
}

#[test]
fn auth_noauth_rejects_with_configured_error() {
  let (_, out, _dir) = session_frame_with_store(b"*2\r\n$4\r\nAUTH\r\n$3\r\npwd\r\n");
  assert_eq!(
    out,
    b"-ERR Client sent AUTH, but configured authenticator does not accept passwords\r\n"
  );
}

#[test]
fn async_resp2_reports_unsupported() {
  let (_, out) = session_frame(b"*2\r\n$5\r\nASYNC\r\n$2\r\nON\r\n");
  assert_eq!(out, b"-ERR command not supported in RESP2\r\n");
}

#[test]
fn command_root_with_args_reports_unknown_subcommand() {
  let (_, out) = session_frame(b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nNOPE\r\n");
  assert_eq!(out, b"-ERR unknown subcommand 'NOPE'.\r\n");
}

#[test]
fn latency_help_lists_subcommands() {
  let (_, out) = session_frame(b"*2\r\n$7\r\nLATENCY\r\n$4\r\nHELP\r\n");
  assert!(out.starts_with(b"*"), "帮助文本数组: {out:?}");
  assert!(String::from_utf8_lossy(&out).contains("HISTOGRAM"));
}

#[test]
fn latency_histogram_without_monitor_is_empty_array() {
  let (_, out) = session_frame(b"*2\r\n$7\r\nLATENCY\r\n$9\r\nHISTOGRAM\r\n");
  assert_eq!(out, b"*0\r\n");
}

#[test]
fn slowlog_len_without_container_is_zero() {
  let (_, out) = session_frame(b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n");
  assert_eq!(out, b":0\r\n");
}

#[test]
fn acl_cat_without_authenticator_reports_disabled() {
  let (_, out, _dir) = session_frame_with_store(b"*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n");
  assert_eq!(out, b"-ERR ACL Authenticator is disabled.\r\n");
}

#[test]
fn publish_without_broker_reports_disabled() {
  let (_, out) = session_frame(b"*3\r\n$7\r\nPUBLISH\r\n$1\r\nc\r\n$1\r\nv\r\n");
  assert_eq!(
    out,
    b"-ERR PUBLISH is disabled, enable it with --pubsub option.\r\n"
  );
}

#[test]
fn subscribe_without_broker_reports_disabled() {
  let (_, out) = session_frame(b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n");
  assert_eq!(
    out,
    b"-ERR SUBSCRIBE is disabled, enable it with --pubsub option.\r\n"
  );
}

#[test]
fn multi_without_txn_components_reports_error() {
  let (_, out) = session_frame(b"*1\r\n$5\r\nMULTI\r\n");
  assert_eq!(out, b"-ERR unknown command\r\n");
}

#[test]
fn watch_in_txn_gate_is_registered_with_components() {
  use std::sync::Arc as StdArc;
  let mut s = session(0);
  s.attach_transaction_components(StdArc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  let consumed = pump_feed(&mut s, b"*1\r\n$5\r\nWATCH\r\n");
  assert!(consumed.is_some());
  assert!(drain_output(&mut s).starts_with(b"-ERR wrong number of arguments"));
}

#[test]
fn multi_exec_roundtrip_without_writes() {
  use std::sync::Arc as StdArc;
  let mut s = session(0);
  s.attach_transaction_components(StdArc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  assert!(pump_feed(&mut s, b"*1\r\n$5\r\nMULTI\r\n").is_some());
  assert_eq!(drain_output(&mut s), b"+OK\r\n");
  assert!(pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n").is_some());
  assert_eq!(drain_output(&mut s), b"+QUEUED\r\n");
  assert!(pump_feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n").is_some());
  assert_eq!(drain_output(&mut s), b"*1\r\n+PONG\r\n");
}

#[test]
fn attach_pubsub_enables_publish_path() {
  use std::sync::Arc as StdArc;
  let mut s = session(0);
  s.attach_pubsub(StdArc::new(SubscribeBroker::new()));
  assert!(pump_feed(&mut s, b"*3\r\n$7\r\nPUBLISH\r\n$1\r\nc\r\n$1\r\nv\r\n").is_some());
  assert_eq!(drain_output(&mut s), b":0\r\n");
}

#[test]
fn abort_error_message_sanitizes_crlf() {
  let mut s = session(100);
  s.abort_error_message("ERR broken\r\nINJECT");
  assert_eq!(drain_output(&mut s), b"-ERR broken\r\n");
  assert!(s.command_error_written);
}

#[test]
fn scratch_pending_half_packet_and_full_reset() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nPI"), Some(10));
  assert_eq!(s.read_head, 0);
  assert_eq!(pump_feed(&mut s, b"NG\r\n"), Some(0));
  assert!(s.recv_buffer.is_empty());
  assert_eq!(s.bytes_read, 0);
  assert_eq!(s.read_head, 0);
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "+PONG\r\n"
  );
  assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n"), Some(0));
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "+PONG\r\n"
  );
}

#[test]
fn scratch_pending_pipeline_partial_tail() {
  let mut s = session(0);
  assert_eq!(
    pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPI"),
    Some(10)
  );
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "+PONG\r\n"
  );
  assert_eq!(s.read_head, 14);
  assert_eq!(pump_feed(&mut s, b"NG\r\n"), Some(0));
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "+PONG\r\n"
  );
  assert!(s.recv_buffer.is_empty());
}

#[test]
fn scratch_pending_multi_exec_cross_batch_replay() {
  use std::sync::Arc as StdArc;
  let mut s = session(0);
  s.attach_transaction_components(StdArc::new(WatchVersionMap::new(64)), TxnLockTable::new());

  assert_eq!(pump_feed(&mut s, b"*1\r\n$5\r\nMULTI\r\n"), Some(0));
  assert_eq!(drain_output(&mut s), b"+OK\r\n");
  assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n"), Some(0));
  assert_eq!(drain_output(&mut s), b"+QUEUED\r\n");
  assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), Some(0));
  assert_eq!(drain_output(&mut s), b"*1\r\n+PONG\r\n");
  assert!(s.recv_buffer.is_empty());
  assert_eq!(s.txn_state, TxnState::None);
}

#[test]
fn scratch_pending_multi_exec_same_batch_matches_redis() {
  let mut s = session(0);
  s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  assert_eq!(
    pump_feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nEXEC\r\n"
    ),
    Some(0)
  );
  assert_eq!(drain_output(&mut s), b"+OK\r\n+QUEUED\r\n*1\r\n+PONG\r\n");
}

#[test]
fn scratch_pending_violation_signals_none() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*A\r\nGET\r\n"), None);
  assert!(s.parse_violation.is_none(), "哨兵应被消费复位");
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Unexpected character 'A'.\r\n"
  );
}

#[test]
fn fatal_disconnect_signals_none_without_error_line() {
  let mut s = session(0);
  s.fatal_disconnect = true;
  assert!(pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n").is_none());
  assert_eq!(drain_output(&mut s), b"+PONG\r\n");
}

#[test]
fn scratch_pending_violation_preserves_prior_replies() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n*A\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"+PONG\r\n-ERR Protocol Error: Unexpected character 'A'.\r\n"
  );
}

#[test]
fn scratch_pending_empty_is_harmless_noop() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b""), Some(0));
  assert!(drain_output(&mut s).is_empty());
}

#[test]
fn scratch_pending_cmd_bad_sigil_disconnects() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n:5\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Unexpected character ':'.\r\n"
  );
}

#[test]
fn scratch_pending_cmd_non_digit_length_disconnects() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$abc\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Unexpected character 'A'.\r\n"
  );
}

#[test]
fn scratch_pending_cmd_length_u64_overflow_disconnects() {
  let mut s = session(0);
  let frame = format!("*1\r\n${}\r\n", "9".repeat(23));
  assert_eq!(pump_feed(&mut s, frame.as_bytes()), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 9999999999999999999\r\n"
  );
}

#[test]
fn scratch_pending_cmd_length_int_overflow_disconnects() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$3000000000\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 3000000000\r\n"
  );
}

#[test]
fn scratch_pending_cmd_bad_header_terminator_disconnects() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$3\rXabc\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Unexpected character '\\x0d'.\r\n"
  );
}

#[test]
fn scratch_pending_cmd_bad_value_terminator_disconnects() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$3\r\nabc\rX"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Unexpected character '\\x0d'.\r\n"
  );
}

#[test]
fn scratch_pending_cmd_negative_length_disconnects() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$-1\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Invalid string length '-1'.\r\n"
  );

  let mut s = session(1);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$-5\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Invalid string length '-5'.\r\n"
  );

  let mut s = session(2);
  assert_eq!(pump_feed(&mut s, b"*1\r\n_\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Invalid string length '-1'.\r\n"
  );
}

#[test]
fn scratch_pending_cmd_subcommand_bad_sigil_disconnects() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*2\r\n$7\r\nCLUSTER\r\n:5\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Unexpected character ':'.\r\n"
  );
}

#[test]
fn scratch_pending_cmd_violation_preserves_prior_replies() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n*1\r\n:5\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"+PONG\r\n-ERR Protocol Error: Unexpected character ':'.\r\n"
  );
}

#[test]
fn scratch_pending_cmd_half_packet_keeps_waiting() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$3\r\nab"), Some(10));
  assert!(drain_output(&mut s).is_empty());
  assert_eq!(s.read_head, 0);

  let mut s = session(1);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$600000000\r\n"), Some(16));
  assert!(drain_output(&mut s).is_empty());
}
