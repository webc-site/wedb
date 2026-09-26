//! RESP 服务端会话集成测试（自 src/resp/resp_server_session.rs 单元测试外迁）
//!
//! 验证 RespServerSession 的命令分发、HELLO/AUTH/ACL 鉴权、协议版本升级、
//! 集群门控与重定向、网络泵持久游标与半包断流、事务重放以及 Lua 超时治理。

use std::{
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
  },
};

use compio::runtime::Runtime;
use parking_lot::Mutex;
use wacl::{AccessControlList, AclPassword, GarnetAclAuthenticator, User, UserHandle};
use wbase::endpoint::ip_is_loopback;
use wconf::{DEFAULT_MAX_DATABASES, DEFAULT_RESP_VERSION, NodeArgs};
use wdev::SegmentedDevice;
use wlua::{LuaOptions, LuaTimeoutManager};
use wmetric::{LatencyMetricsType, SessionMetricsHandle};
use wnode::{
  ClusterProvider, ClusterSessionFace, PeerSource,
  cluster_session::{ClusterSlotVerificationInput, SlotVerifyGate},
  resp::{
    acl_commands::{AclAuthOutcome, AclGateVerdict},
    acl_store::AclStore,
    garnet_api::{GarnetApiFace, StoreGarnetApi},
    resp_server_session::{
      ConnectionProtectionOption, RespServerSession, RespServerSessionOptions,
    },
    slow_path::SlowFuture,
  },
};
use wnode_test::drain_output;
use wpubsub::{channel_ns::ChannelNsPrefix, subscribe_broker::SubscribeBroker};
use wresp::{
  catalog::{RespCommandFlags, try_get_resp_command_info_by_cmd},
  command::RespCommand,
};
use wtxn::{TxnLockTable, TxnState, WatchVersionMap};

/// 同步测试壳内闭环 async 命令链（ACL 存储访问全链 async 化的测试对位）
fn block_on<F: Future>(fut: F) -> F::Output {
  Runtime::new().unwrap().block_on(fut)
}

fn session(id: i64) -> RespServerSession {
  RespServerSession::new(id, RespServerSessionOptions::default())
}

/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）
///
/// 网络泵替身（与 drive_loop 逐位同构）：消费轮应答冲出 → 内联 await 驱动
/// 停车臂（ACL 挂载刷新重驱重评 / AUTH·HELLO·ACL 族产应答闭环 / 慢路径
/// 挂起并回）→ 续消费流水线余量；应答字节最后并回会话输出缓冲供断言
///（对位泵池化响应块随本轮实写）
fn pump_feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  let mut resp_buf = Vec::new();
  let mut remaining = s.try_consume_messages();
  s.take_output_into(&mut resp_buf);
  Runtime::new().unwrap().block_on(async {
    loop {
      // 重驱型刷新臂：点查后重入消费，门链以新挂载重评
      if s.take_pending_acl_refresh() {
        if let Some(api) = s.garnet_api.clone() {
          api.exec_acl_refresh(s).await;
        }
        remaining = s.try_consume_messages().or(remaining);
        s.take_output_into(&mut resp_buf);
        continue;
      }
      // 产应答型异步臂：认证/规则读写回写会话本地态，应答冲出后续消费
      if let Some((cmd, args, parked_output_len)) = s.take_pending_auth_acl() {
        let views: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        if let Some(api) = s.garnet_api.clone() {
          let _ = api.exec_auth_acl(s, cmd, &views).await;
        }
        s.account_parked_auth_acl_failure(cmd, parked_output_len);
        s.take_output_into(&mut resp_buf);
        remaining = s.try_consume_messages().or(remaining);
        s.take_output_into(&mut resp_buf);
        continue;
      }
      // 慢路径挂起（如 AUTH 冷上下文装载）：await 闭环，应答按流水线并回
      if let Some(slow) = s.take_slow_wait() {
        let reply = slow.resolve().await;
        s.resolve_slow_wait_into(&reply, &mut resp_buf);
        continue;
      }
      break;
    }
  });
  s.output.extend_from_slice(&resp_buf);
  remaining
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
fn acl_session(id: i64) -> (RespServerSession, Arc<GarnetAclAuthenticator>) {
  let authenticator = Arc::new(GarnetAclAuthenticator::new(Arc::new(
    AccessControlList::default(),
  )));
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
  /// 无应答判定口调用计数（用例据此断言判定走的是免渲染出口）
  no_response_calls: AtomicUsize,
  /// 有应答渲染口调用计数（免渲染用例据此断言判定从未落到渲染臂）
  render_calls: AtomicUsize,
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
      no_response_calls: AtomicUsize::new(0),
      render_calls: AtomicUsize::new(0),
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
    self.render_calls.fetch_add(1, Ordering::Relaxed);
    if self.redirect.load(Ordering::Relaxed) {
      output.extend_from_slice(b"-MOVED 0 stub:1\r\n");
      return SlotVerifyGate::Redirected;
    }
    SlotVerifyGate::Serve
  }

  fn network_multi_key_slot_verify_no_response(
    &self,
    _input: &ClusterSlotVerificationInput<'_>,
    _args: &[&[u8]],
  ) -> bool {
    // 与有应答臂同一裁决源，只回判定不落渲染（C# 两方法分离形态）
    self.no_response_calls.fetch_add(1, Ordering::Relaxed);
    self.redirect.load(Ordering::Relaxed)
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

  // 旋钮全量透传；max_databases 下界钳 0（Lua 超时装配点不再钳制：负值/过小值
  // 已在 wconf 启动校验 IntRangeValidation(10, int.MaxValue, isRequired:false)
  // 拒启，见 wconf/tests/lua_four_knobs_config_wiring.rs，本处取合法缺省 0）
  let node = NodeArgs {
    max_databases: -1,
    latency_monitor: true,
    commandstats_monitor: true,
    enable_lua: true,
    lua_script_timeout_ms: 0,
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
      session.output.extend_from_slice(b"+OK\r\n");
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
  // CLIENT SETINFO 落库（字段 pub 直赋，同生产 network_clientsetinfo 形态）
  s.client_lib_name = Some("redis-py".to_string());
  s.client_lib_version = Some("5.0.1".to_string());

  let mut out = Vec::new();
  assert!(block_on(s.process_hello_command_state::<SegmentedDevice>(
    Some(3),
    b"",
    b"",
    None,
    None,
    &mut out
  )));
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
    "id=7 addr=127.0.0.1:6380 laddr= name=loader age=0 user=default flags=N db=0 resp=3 lib-name=redis-py lib-ver=5.0.1"
  );
}

#[test]
fn hello_resp2_keeps_doubled_array_header() {
  let mut s = session(9);
  let mut out = Vec::new();
  assert!(block_on(s.process_hello_command_state::<SegmentedDevice>(
    None, b"", b"", None, None, &mut out
  )));
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
  // int32 域内负数与域内越界才走「库号越界」档；前导零拒收系 rust 严格收口（C# 死参放行 007，见 doc/zh/deviations.md §32），尾随垃圾同非整数档
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
  assert!(!block_on(s.process_hello_command_state::<SegmentedDevice>(
    Some(3),
    b"alice",
    b"secret",
    None,
    None,
    &mut out
  )));
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
  block_on(acl_store.write(0, b"alice", &user.to_bytes())).unwrap();

  let mut out = Vec::new();
  assert!(block_on(s.process_hello_command_state::<SegmentedDevice>(
    Some(3),
    b"alice",
    b"secret",
    Some("myclient"),
    Some(&acl_store),
    &mut out
  )));
  assert_eq!(s.resp_protocol_version, 3);
  assert_eq!(s.client_name.as_deref(), Some("myclient"));
  assert_eq!(s.user_name(), Some("alice"));
  let text = String::from_utf8(out).unwrap();
  assert!(text.contains("$5\r\nproto\r\n:3\r\n"));

  let mut s2 = session(3);
  s2.attach_acl(Some(authenticator));
  let mut out2 = Vec::new();
  assert!(!block_on(
    s2.process_hello_command_state::<SegmentedDevice>(
      Some(3),
      b"alice",
      b"wrongpwd",
      Some("newclient"),
      Some(&acl_store),
      &mut out2
    )
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
  // 延迟复位无会话面出口：C# ResetLatencyMetrics 的跨线程复位臂在 rust
  // 归属转移（退役槽由属主线程版本翻转点就地清零，见监视器
  // cleanup_global_latency_metrics 注释），会话不暴露 &self 复位口。

  let mut enabled = RespServerSession::new(
    6,
    RespServerSessionOptions {
      latency_monitor: true,
      ..RespServerSessionOptions::default()
    },
  );
  let metrics = enabled.latency_metrics.as_mut().expect("监视开启时有实例");
  metrics.start(LatencyMetricsType::NetRsLat, 100);
  assert_eq!(metrics.get(LatencyMetricsType::NetRsLat), 100);
  metrics.stop(LatencyMetricsType::NetRsLat, 150);
  assert_eq!(metrics.get(LatencyMetricsType::NetRsLat), 0);
  // 停表样本直落本会话独占表的当前版本槽（属主直读观测，无锁无快照）
  let ver = metrics.version();
  assert_eq!(
    metrics.metrics[LatencyMetricsType::NetRsLat.idx()].latency[ver].len(),
    1
  );
}

#[test]
fn debug_protection() {
  let local = RespServerSessionOptions {
    enable_debug_command: ConnectionProtectionOption::Local,
    ..RespServerSessionOptions::default()
  };
  let mut s = RespServerSession::new(8, local);
  // 本地判定读 accept 侧折叠的来源类型（回环布尔经
  // wbase::endpoint::ip_is_loopback 单源自 typed 地址折出，对位 C#
  // IPAddress.IsLoopback），不再解析展示字符串
  s.peer_source = ip_peer("127.0.0.1:55555");
  assert!(s.can_run_debug());
  // 127.0.0.0/8 全段回环（C# IPAddress.IsLoopback 口径）
  s.peer_source = ip_peer("127.1.2.3:6379");
  assert!(s.can_run_debug());
  s.peer_source = ip_peer("[::1]:55555");
  assert!(s.can_run_debug());

  // UDS 端点恒为本地连接（C# UnixDomainSocketEndPoint 恒真臂；来源类型
  // 判据下与展示文本形态无关，命名路径/未命名空串同判）
  s.peer_source = PeerSource::Unix;
  assert!(s.can_run_debug(), "UDS 端点应判本地");

  s.peer_source = ip_peer("10.0.0.9:1234");
  assert!(!s.can_run_debug(), "Local 保护拒绝远程");

  let mut closed = RespServerSession::new(
    9,
    RespServerSessionOptions {
      enable_debug_command: ConnectionProtectionOption::No,
      ..RespServerSessionOptions::default()
    },
  );
  closed.peer_source = ip_peer("127.0.0.1:1");
  assert!(!closed.can_run_debug());
}

/// IP 端点来源折叠（accept 侧同源路径：typed SocketAddr 经
/// wbase::endpoint::ip_is_loopback 折出回环布尔）
fn ip_peer(addr: &str) -> PeerSource {
  PeerSource::Ip {
    loopback: ip_is_loopback(addr.parse().unwrap()),
  }
}

/// UDS 未命名对端（getpeername 无名 → 展示串空串）在 Local 档放行：
/// 判定读来源类型不受空串文本误导（对标 C# UnixDomainSocketEndPoint
/// 恒真臂，未命名对端 ToString 空串亦本地）
#[test]
fn debug_local_allows_unnamed_uds_peer() {
  let mut s = RespServerSession::new(
    10,
    RespServerSessionOptions {
      enable_debug_command: ConnectionProtectionOption::Local,
      ..RespServerSessionOptions::default()
    },
  );
  s.set_remote_endpoint("", PeerSource::Unix);
  assert!(
    s.can_run_debug(),
    "UDS 未命名对端（空串展示文本）应判本地放行"
  );
}

/// v4-mapped IPv6 回环对端（::ffff:127.0.0.1）在 Local 档放行：
/// typed 判定先解映射再按 IPv4 口径（对位 C# IPAddress.IsLoopback
/// 文档明载行为；其展示串 "[::ffff:127.0.0.1]:port" 不匹配旧
/// "[::1]" 前缀判据的漏判已随判据换源消除）
#[test]
fn debug_local_allows_v4_mapped_loopback() {
  assert!(
    ip_is_loopback("[::ffff:127.0.0.1]:6379".parse().unwrap()),
    "v4-mapped 回环解映射后应判真"
  );
  // 对偶臂：v4-mapped 非回环段仍远程（拒向语义不扩大敞口）
  assert!(
    !ip_is_loopback("[::ffff:10.0.0.1]:6379".parse().unwrap()),
    "v4-mapped 非回环段应判假"
  );
  let mut s = RespServerSession::new(
    11,
    RespServerSessionOptions {
      enable_debug_command: ConnectionProtectionOption::Local,
      ..RespServerSessionOptions::default()
    },
  );
  s.set_remote_endpoint(
    "[::ffff:127.0.0.1]:6379",
    ip_peer("[::ffff:127.0.0.1]:6379"),
  );
  assert!(s.can_run_debug(), "v4-mapped 回环应判本地放行");
}

#[test]
fn output_pipeline_and_metrics() {
  let mut s = RespServerSession::new(10, RespServerSessionOptions::default());
  // 会话指标句柄经唯一注入口装配（生产由 service.rs 采样门控创建后同口注入）
  s.attach_session_metrics(Some(Arc::new(SessionMetricsHandle::default())));
  s.output.extend_from_slice(b"*2\r\n$3\r\nfoo\r\n");
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

#[test]
fn readonly_writes_through_cluster_aspect() {
  let stub = Arc::new(StubClusterSession::new());
  let mut s = session(30);
  s.attach_cluster_session(stub.clone());
  s.parse_state.initialize(0);
  assert!(s.process_basic_commands(RespCommand::Readonly));
  assert_eq!(String::from_utf8(drain_output(&mut s)).unwrap(), "+OK\r\n");
  assert!(stub.read_only.load(Ordering::Relaxed));

  s.parse_state.initialize(0);
  assert!(s.process_basic_commands(RespCommand::Readwrite));
  assert_eq!(String::from_utf8(drain_output(&mut s)).unwrap(), "+OK\r\n");
  assert!(!stub.read_only.load(Ordering::Relaxed));
}

/// READONLY/READWRITE 无 arity 校验：带参恒 +OK 且切面照常置位（C# NetworkREADONLY/
/// NetworkREADWRITE 全程不读 parseState.Count，与同族 NetworkASKING 同一口径；
/// 部分集群代理与脚本化客户端在重连序列中会附带冗余参数）
#[test]
fn readonly_readwrite_extra_arguments_tolerated() {
  // 带参形态恒 +OK（standalone 无切面，C# clusterSession 为 null 同构）
  let (_s, out) = session_frame(b"*2\r\n$8\r\nREADONLY\r\n$3\r\nfoo\r\n");
  assert_eq!(String::from_utf8(out).unwrap(), "+OK\r\n");

  let (_s, out) = session_frame(b"*3\r\n$9\r\nREADWRITE\r\n$3\r\nbar\r\n$3\r\nbaz\r\n");
  assert_eq!(String::from_utf8(out).unwrap(), "+OK\r\n");

  // 带参形态下切面置位经 Fake 原子验证（会话侧无镜像字段，唯一真源在切面）
  let stub = Arc::new(StubClusterSession::new());
  let mut s = session(31);
  s.attach_cluster_session(stub.clone());
  s.parse_state.initialize(2);
  assert!(s.process_basic_commands(RespCommand::Readonly));
  assert_eq!(String::from_utf8(drain_output(&mut s)).unwrap(), "+OK\r\n");
  assert!(stub.read_only.load(Ordering::Relaxed));

  s.parse_state.initialize(1);
  assert!(s.process_basic_commands(RespCommand::Readwrite));
  assert_eq!(String::from_utf8(drain_output(&mut s)).unwrap(), "+OK\r\n");
  assert!(!stub.read_only.load(Ordering::Relaxed));
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

/// 无应答判定口免渲染：会话槽位校验口只回「可否服务」裁决，不向 output
/// 写重定向字节（对位 C# 判定口与渲染口两分形态）；单键臂与多键臂共用同一无应答出口
///
/// 零渲染的直证两条：一为有应答渲染口调用计数恒 0（改前两臂都经渲染口 + 一块
/// 随即丢弃的缓冲，故该计数即免渲染判据，见
/// [`cluster_gate_redirects_data_command`] 同桩渲染臂会落 `-MOVED` 字节）；
/// 二为输出缓冲字节长度不变——先在缓冲预置哨兵帧，使「不追加」不被
/// 「空缓冲」这一初始态吞没（预置非空仍逐字不变才是强断言）。
#[test]
fn cluster_no_response_gate_judges_without_render() {
  const SENTINEL: &[u8] = b"+SENTINEL\r\n";
  let stub = Arc::new(StubClusterSession::new());
  stub.redirect.store(true, Ordering::Relaxed);
  let mut s = session(35);
  s.attach_cluster_session(stub.clone());

  // 单键臂（parse_state.count == 1）：解析落定即判，不执行命令
  s.recv_buffer
    .extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  s.bytes_read = s.recv_buffer.len();
  assert_eq!(s.parse_command(), Some(RespCommand::Get));
  assert_eq!(s.parse_state.count, 1);
  s.output.extend_from_slice(SENTINEL);
  let len_before = s.output.len();
  assert!(!s.can_serve_slot_no_response(RespCommand::Get));
  assert_eq!(
    s.output.len(),
    len_before,
    "无应答口失败时渲染口不得向输出缓冲追加字节"
  );
  assert_eq!(&s.output[..], SENTINEL);

  // 多键臂（解析态视图采集面 collect_arg_views）：MGET 三键走同一无应答出口
  let mut s2 = session(36);
  s2.attach_cluster_session(stub.clone());
  s2.recv_buffer
    .extend_from_slice(b"*4\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
  s2.bytes_read = s2.recv_buffer.len();
  assert_eq!(s2.parse_command(), Some(RespCommand::Mget));
  assert_eq!(s2.parse_state.count, 3);
  s2.output.extend_from_slice(SENTINEL);
  let len_before = s2.output.len();
  assert!(!s2.can_serve_slot_no_response(RespCommand::Mget));
  assert_eq!(
    s2.output.len(),
    len_before,
    "无应答多键臂失败时渲染口不得向输出缓冲追加字节"
  );
  assert_eq!(&s2.output[..], SENTINEL);

  // 两臂各一次判定，且全程未触碰有应答渲染口
  assert_eq!(stub.no_response_calls.load(Ordering::Relaxed), 2);
  assert_eq!(stub.render_calls.load(Ordering::Relaxed), 0);
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "+SENTINEL\r\n"
  );
  assert_eq!(
    String::from_utf8(drain_output(&mut s2)).unwrap(),
    "+SENTINEL\r\n"
  );

  // 放行态：桩不重定向时无应答臂返回可服务，同样零渲染
  stub.redirect.store(false, Ordering::Relaxed);
  let len_before = s.output.len();
  assert!(s.can_serve_slot_no_response(RespCommand::Get));
  assert_eq!(s.output.len(), len_before);
  assert_eq!(stub.no_response_calls.load(Ordering::Relaxed), 3);
  assert_eq!(stub.render_calls.load(Ordering::Relaxed), 0);
}

/// 事务运行态（MULTI/EXEC 在途）无应答前视臂直透放行，不再前视判槽——
/// 对位 C# RespClusterSlotVerify.cs:124 `NetworkMultiKeySlotVerifyNoResponse`
/// 首行 `txnManager.state == TxnState.Running`。
///
/// 红/绿直证：桩置受限槽。非事务期 `can_serve_slot_no_response` 判不可服务且确经
/// 切面判定（no_response_calls +1），坐实「本槽受限、旧事务期亦会走到此判红分支」；
/// 置 txn_state=Running 后返回可服务，且判定核不再被触达（计数冻结）。旧实现（无门）
/// 在事务期同样会前视判红（返回 false、计数 +1），新实现判绿且冻结计数。
#[test]
fn cluster_txn_running_no_response_gate_bypasses_slot_check() {
  let stub = Arc::new(StubClusterSession::new());
  stub.redirect.store(true, Ordering::Relaxed);
  let mut s = session(41);
  s.attach_cluster_session(stub.clone());

  s.recv_buffer
    .extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  s.bytes_read = s.recv_buffer.len();
  assert_eq!(s.parse_command(), Some(RespCommand::Get));
  assert_eq!(s.parse_state.count, 1);

  // 非事务期：受限槽经切面判定为不可服务（旧/新一致，坐实判据源确受限）
  assert!(!s.can_serve_slot_no_response(RespCommand::Get));
  assert_eq!(stub.no_response_calls.load(Ordering::Relaxed), 1);

  // 事务运行态：门在触达判定核之前短路，返回可服务且不再前视判槽
  s.txn_state = TxnState::Running;
  assert!(s.can_serve_slot_no_response(RespCommand::Get));
  assert_eq!(
    stub.no_response_calls.load(Ordering::Relaxed),
    1,
    "事务运行态无应答臂须短路放行，不得再触达切面判定核"
  );
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
  assert_eq!(s.user_name(), Some("default"));
  assert!(s.acl_permits(RespCommand::Get));

  let acl = Arc::new(AccessControlList::new("").unwrap());
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(acl))));
  let handle = Arc::new(UserHandle::new(Arc::new(User::new("admin".into()))));
  s.set_user_handle(handle, None, false);
  assert_eq!(s.user_name(), Some("admin"));
  assert!(!s.acl_permits(RespCommand::Get));

  assert!(!s.authenticate_user(b"invalid", b"badpwd"));
  assert_eq!(s.user_name(), Some("admin"), "认证失败保留原已认证用户");
}

#[test]
fn acl_default_user_allows_all() {
  let (mut s, _) = acl_session(40);
  assert!(s.acl_user_handle.is_some(), "default 用户自动认证挂载");
  assert!(matches!(
    s.check_acl_permissions(RespCommand::Get),
    AclGateVerdict::Permitted
  ));
  assert!(matches!(
    s.check_acl_permissions(RespCommand::Set),
    AclGateVerdict::Permitted
  ));

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
  block_on(acl_store.write(0, b"limited", &user.to_bytes())).unwrap();

  let outcome = block_on(s.authenticate_user_via_store(&acl_store, b"limited", b"pwd"));
  let AclAuthOutcome::Success(handle, _, generation) = outcome else {
    panic!("limited 认证必须成功");
  };
  s.set_user_handle(handle, generation, true);

  assert!(matches!(
    s.check_acl_permissions(RespCommand::Get),
    AclGateVerdict::Permitted
  ));
  assert!(
    !matches!(
      s.check_acl_permissions(RespCommand::Set),
      AclGateVerdict::Permitted
    ),
    "位图外命令拒绝"
  );

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
  let authenticator = Arc::new(GarnetAclAuthenticator::new(Arc::new(
    AccessControlList::new("pwd").unwrap(),
  )));
  let mut s = session(42);
  s.attach_acl(Some(Arc::clone(&authenticator)));
  assert!(s.acl_user_handle.is_none());

  assert!(!matches!(
    s.check_acl_permissions(RespCommand::Get),
    AclGateVerdict::Permitted
  ));
  assert!(pump_feed(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").is_some());
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "-NOAUTH Authentication required.\r\n"
  );

  assert!(matches!(
    s.check_acl_permissions(RespCommand::Quit),
    AclGateVerdict::Permitted
  ));
}

#[test]
fn acl_no_auth_authenticator_allows_all() {
  let mut s = session(43);
  // 免认证档构造尾落 default 单例句柄（C# AuthenticateUser 的
  // GetDefaultUserHandle 兜底臂，CLIENT INFO/LIST 恒带 default 用户）
  assert_eq!(s.user_name(), Some("default"));
  assert!(matches!(
    s.check_acl_permissions(RespCommand::Get),
    AclGateVerdict::Permitted
  ));
  assert!(matches!(
    s.check_acl_permissions(RespCommand::Set),
    AclGateVerdict::Permitted
  ));
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

/// 看门狗超时中断的会话面闭环（对标 C#
/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:IntentionalTimeout
/// 的 Timeout 报错段 + Safe 复活段）。非 compio 的普通线程上下文形态：
/// 会话在无任何运行时的 OS 线程上直跑同步 pcall——旧实现下该线程上
/// `compio::runtime::spawn` 直接 Panic，且协作式 tick 会被死循环饿死；
/// 现看门狗由 `LuaTimeoutManager::start` 的专属 OS 线程自驱，脚本永不
/// 让出也按时抢占。回帧逐字节 = 本仓错误单源 wlua state.rs::TIMEOUT_ERROR
/// 的 RESP `-` 前缀帧。
#[test]
fn eval_infinite_loop_times_out_and_session_recovers() {
  use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
  };

  let manager = Arc::new(LuaTimeoutManager::new(50));
  manager.start();

  let (tx, rx) = mpsc::channel();
  let session_thread = {
    let (manager, tx) = (Arc::clone(&manager), tx);
    thread::spawn(move || {
      let mut s = RespServerSession::new(
        32,
        RespServerSessionOptions {
          enable_lua: true,
          lua_timeout_manager: Some(manager),
          ..RespServerSessionOptions::default()
        },
      );
      let frame = b"*3\r\n$4\r\nEVAL\r\n$17\r\nwhile true do end\r\n$1\r\n0\r\n";
      let start = Instant::now();
      let consumed = pump_feed(&mut s, frame).is_some();
      let elapsed = start.elapsed();
      let timed_out = drain_output(&mut s);

      // Safe 段：同会话后续命令复活（EVAL 正常回值 + PING）。
      let revived = pump_feed(
        &mut s,
        b"*3\r\n$4\r\nEVAL\r\n$9\r\nreturn 42\r\n$1\r\n0\r\n",
      )
      .is_some();
      let eval_out = drain_output(&mut s);
      let pinged = pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n").is_some();
      let ping_out = drain_output(&mut s);
      let _ = tx.send((
        consumed, elapsed, timed_out, revived, eval_out, pinged, ping_out,
      ));
    })
  };
  // 对可观测状态收敛：等待的是死循环 EVAL 的实际回帧，上界仅防看门狗
  // 失效时的挂死证词（旧协作式饥饿形态即落在此失败面）。
  let (consumed, elapsed, timed_out, revived, eval_out, pinged, ping_out) = rx
    .recv_timeout(Duration::from_secs(10))
    .expect("看门狗未按时抢占死循环，会话线程未回帧（饥饿挂死）");
  let _ = session_thread.join();

  assert!(consumed);
  assert!(
    elapsed >= Duration::from_millis(50),
    "中断早于配置超时: {elapsed:?}"
  );
  assert_eq!(
    String::from_utf8_lossy(&timed_out),
    "-ERR Lua script exceeded configured timeout\r\n"
  );
  assert!(revived);
  assert_eq!(String::from_utf8_lossy(&eval_out), ":42\r\n");
  assert!(pinged);
  assert_eq!(String::from_utf8_lossy(&ping_out), "+PONG\r\n");

  // 停机收口显式化（C# Dispose 的 Cancel + Join）：join 返回即线程收敛，
  // Drop 二次 dispose 幂等。
  manager.dispose();
}

/// reactor 占用形态下的看门狗抢占闭环（票面命题的单/多 worker 形态）：
/// 会话在 compio worker 线程上直跑同步 pcall——单 worker 时整个 reactor
/// 被死循环独占（旧协作式 tick 任务正在此饿死，本用例即其回归面），多
/// worker 时全部 worker 各自独占一个死循环。专属看门狗线程与 reactor 无
/// 关，按 tick 节拍（超时 / 10）统一抢占全部登记（C# "Shared thread for
/// all timeouts"）。回帧逐字节判据同
/// [`eval_infinite_loop_times_out_and_session_recovers`]。
#[test]
fn eval_dead_loop_interrupted_by_runtime_tick_task() {
  use std::{sync::mpsc, thread, time::Duration};

  use compio::runtime::Runtime;

  /// 会话线程体：自建单 worker 运行时（thread-per-core 生产形态）并让死循环
  /// EVAL 独占该运行时——回帧经 channel 交回主线程断言。
  fn eval_dead_loop(
    manager: &Arc<LuaTimeoutManager>,
    id: i64,
    tx: &mpsc::Sender<(i64, bool, Vec<u8>)>,
  ) {
    let mut s = RespServerSession::new(
      id,
      RespServerSessionOptions {
        enable_lua: true,
        lua_timeout_manager: Some(Arc::clone(manager)),
        ..RespServerSessionOptions::default()
      },
    );
    let rt = Runtime::new().unwrap();
    let frame = b"*3\r\n$4\r\nEVAL\r\n$17\r\nwhile true do end\r\n$1\r\n0\r\n".to_vec();
    let (consumed, out) = rt.block_on(async {
      let consumed = pump_feed(&mut s, &frame).is_some();
      (consumed, drain_output(&mut s))
    });
    let _ = tx.send((id, consumed, out));
  }

  /// 对可观测状态收敛：等待的是会话的实际回帧，上界仅防饥饿挂死证词
  /// （旧协作式 tick 绑定 reactor 的形态即落在此失败面）。
  fn recv_frame(rx: &mpsc::Receiver<(i64, bool, Vec<u8>)>) -> (i64, bool, Vec<u8>) {
    rx.recv_timeout(Duration::from_secs(10))
      .expect("看门狗未按时抢占 reactor 上的死循环，会话未回帧（饥饿挂死）")
  }

  fn assert_timeout_frame(id: i64, consumed: bool, out: Vec<u8>) {
    assert!(consumed, "会话 {id} 帧未消费");
    assert_eq!(
      String::from_utf8_lossy(&out),
      "-ERR Lua script exceeded configured timeout\r\n",
      "会话 {id} 回帧逐字节"
    );
  }

  let manager = Arc::new(LuaTimeoutManager::new(40));
  manager.start();
  let (tx, rx) = mpsc::channel();

  // ① 单 worker：全进程唯一 reactor 线程被死循环 EVAL 霸占，pcall 永不
  // 让出，tick 若仍绑协作调度即饿死。
  let single = {
    let manager = Arc::clone(&manager);
    let tx = tx.clone();
    thread::spawn(move || eval_dead_loop(&manager, 35, &tx))
  };
  let (id, consumed, out) = recv_frame(&rx);
  assert_timeout_frame(id, consumed, out);
  let _ = single.join();

  // ② 多 worker：两 worker 线程各持独立运行时并发死循环（生产多核
  // thread-per-core 形态），共一管理器一 watchdog 统一抢占全部登记
  // （C# "Shared thread for all timeouts"）。
  let multi: Vec<_> = (0..2)
    .map(|i| {
      let (manager, tx) = (Arc::clone(&manager), tx.clone());
      thread::spawn(move || eval_dead_loop(&manager, 36 + i, &tx))
    })
    .collect();
  drop(tx);
  for _ in 0..2 {
    let (id, consumed, out) = recv_frame(&rx);
    assert_timeout_frame(id, consumed, out);
  }
  for worker in multi {
    let _ = worker.join();
  }

  // 停机收口显式化（C# Dispose 的 Cancel + Join）；Drop 二次 dispose 幂等。
  manager.dispose();
}

/// 非 compio 线程直接投影会话选项的无 Panic 回归（票面 e 项：旧实现在
/// `assemble_lua_timeout` 走 `compio::runtime::spawn`，TLS 无当前线程运行时
/// 即 Panic；现看门狗为管理器自持专属 OS 线程，与运行时无关，`from` 任意
/// 线程安全——本用例主线程即无任何 compio 上下文的普通 OS 线程）。
#[test]
fn session_options_from_node_args_is_runtime_free() {
  let node = NodeArgs {
    enable_lua: true,
    lua_script_timeout_ms: 100,
    ..NodeArgs::default()
  };
  let opts = RespServerSessionOptions::from(&node);
  let manager = opts
    .lua_timeout_manager
    .expect("正超时装配管理器（含专属看门狗线程）");
  assert_eq!(manager.timeout_millis(), 100);
  // 装配即启动：dispose 置停机位并 join 收口返回；Drop 二次 dispose 幂等。
  manager.dispose();
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
fn eval_with_timeout_runs_finite_heavy_loop_then_kills_infinite() {
  use std::{
    thread,
    time::{Duration, Instant},
  };

  // 配置超时 + 真实看门狗节拍：有限重循环（已设未到期的单调正值截止）
  // 必须完整跑完——safepoint 只认激活哨兵、零时钟轮询，不误杀；
  // 随后死循环在同一会话被看门狗单调时钟判决精准掐断并恢复。
  let manager = Arc::new(LuaTimeoutManager::new(80));
  let mut s = RespServerSession::new(
    36,
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

  let frame = b"*3\r\n$4\r\nEVAL\r\n$17\r\nwhile true do end\r\n$1\r\n0\r\n";
  let start = Instant::now();
  let consumed = pump_feed(&mut s, frame);
  let elapsed = start.elapsed();
  stop.store(true, Ordering::Relaxed);
  ticker.join().unwrap();

  assert!(consumed.is_some());
  assert!(
    elapsed >= Duration::from_millis(80),
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
}

#[test]
fn lua_options_assembled_from_config() {
  let mut s = RespServerSession::new(
    34,
    RespServerSessionOptions {
      enable_lua: true,
      lua_options: LuaOptions {
        timeout_millis: 1_234,
        ..LuaOptions::default()
      },
      ..RespServerSessionOptions::default()
    },
  );
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
fn hello_auth_nopass_rejects_with_wrongpass() {
  let (_dir, store) = wtest_base::open_test_store("resp-hello-nopass.db").unwrap();
  let acl_store_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_store_session);
  let mut user = User::new("alice".to_string());
  user.set_enabled(true);
  user.add_password_hash(AclPassword::from_string("secret"));
  block_on(acl_store.write(0, b"alice", &user.to_bytes())).unwrap();

  let mut s = session(0);
  assert!(!s.authenticator_can_authenticate);
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));

  let consumed = pump_feed(
    &mut s,
    b"*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$5\r\nalice\r\n$6\r\nsecret\r\n",
  );
  assert!(consumed.is_some());
  let out = drain_output(&mut s);
  assert_eq!(out, b"-WRONGPASS Invalid username/password combination\r\n");
  assert_eq!(s.resp_protocol_version, DEFAULT_RESP_VERSION);
  assert_eq!(s.client_name, None);
  // 免认证形态构造即落默认用户（RespServerSession::new 尾部
  // authenticate_user(default)；C# RespServerSession.cs:306 构造尾同调，
  // AuthenticateUser 的 !CanAuthenticate 臂 success=true 落
  // GetDefaultUserHandle 兜底，失败臂只写 WRONGPASS 不撤句柄
  // —— BasicCommands.cs:ProcessHelloCommand）
  assert_eq!(s.user_name(), Some("default"));
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
  // 半包残余平移收口（C# ShiftTransportReceiveBuffer）：已消费前缀
  //（首条 PING 14 字节）平移丢弃，残余半截驻留首部、游标归零
  assert_eq!(s.read_head, 0);
  assert_eq!(s.end_read_head, 0);
  assert_eq!(s.bytes_read, 10);
  assert_eq!(s.recv_buffer, b"*1\r\n$4\r\nPI");
  assert_eq!(pump_feed(&mut s, b"NG\r\n"), Some(0));
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "+PONG\r\n"
  );
  assert!(s.recv_buffer.is_empty());
}

/// 多轮管道流式粘包半包交叉：连续补完尾半截 + 追加新半截，验证已消费
/// 前缀每批平移收口，recv_buffer 长度不无界累加、容量恒守 64KB 常驻水位
///（C# NetworkHandler.Read 每轮 Process 后 ShiftTransportReceiveBuffer 的
/// 会话侧收口对偶）
#[test]
fn scratch_pending_pipeline_stream_shift_keeps_buffer_bounded() {
  let mut s = session(0);
  // 单轮批：14n 字节 PING 流水线 + 10 字节半截，总量 65516（泵单轮读取
  // 上限 64KB 内；修复前已消费前缀驻留缓冲，第二轮追加即越 64KB 触发
  // 扩容；修复后残余平移至首部，容量恒守水位）
  let pings_per_round = ((1 << 16) - 24) / b"*1\r\n$4\r\nPING\r\n".len();
  let mut batch = Vec::with_capacity(1 << 16);
  for _ in 0..pings_per_round {
    batch.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
  }
  batch.extend_from_slice(b"*1\r\n$4\r\nPI");
  let half = b"*1\r\n$4\r\nPI".len();
  for round in 0..3 {
    // 次轮起先补完上轮尾半截（NG\r\n），再喂新流水线 + 新半截
    let mut feed = Vec::with_capacity(batch.len() + 4);
    if round > 0 {
      feed.extend_from_slice(b"NG\r\n");
    }
    feed.extend_from_slice(&batch);
    assert_eq!(pump_feed(&mut s, &feed), Some(half), "round {round}");
    assert_eq!(s.read_head, 0, "round {round}");
    assert_eq!(s.recv_buffer.len(), half, "round {round}");
    assert_eq!(s.recv_buffer, b"*1\r\n$4\r\nPI", "round {round}");
    assert!(
      s.recv_buffer.capacity() <= 1 << 16,
      "round {round}: capacity {}",
      s.recv_buffer.capacity()
    );
    // 次轮起的应答含补完上轮半截的 1 条 PONG
    let expect_pongs = pings_per_round + usize::from(round > 0);
    assert_eq!(
      drain_output(&mut s).len(),
      expect_pongs * b"+PONG\r\n".len(),
      "round {round}"
    );
  }
  assert_eq!(pump_feed(&mut s, b"NG\r\n"), Some(0));
  assert!(s.recv_buffer.is_empty());
  assert_eq!(drain_output(&mut s).len(), b"+PONG\r\n".len());
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
    b"-ERR Protocol Error: Unexpected character 'X'.\r\n"
  );
}

/// 发现二锁用例：*3\rX\r\n 与 $3\rX\r\n 终止符次位不符，回显首个不符字节 'X' 并断连
#[test]
fn protocol_error_bad_length_header_terminator_unexpected_token_lock() {
  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*3\rX\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Unexpected character 'X'.\r\n"
  );

  let mut s = session(0);
  assert_eq!(pump_feed(&mut s, b"*1\r\n$3\rX\r\n"), None);
  assert_eq!(
    drain_output(&mut s),
    b"-ERR Protocol Error: Unexpected character 'X'.\r\n"
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

/// 订阅态跨命名空间切换门（wedb 自有面验收：task/ing/wpubsub-auth-namespace-leak.md）：
/// 会话在旧 ns 持有活跃订阅时 AUTH 换租必须拒绝，防止 broker 旧订阅悬挂与
/// 跨租户消息经 drain 泄漏；全退订后同一切换放行
#[test]
fn auth_to_foreign_ns_rejected_while_subscribed() {
  let broker = Arc::new(SubscribeBroker::new());
  let (mut s, _auth) = acl_session(50);
  s.attach_pubsub(broker.clone());
  let (_dir, store) = wtest_base::open_test_store("resp-auth-ns-switch.db").unwrap();
  let acl_store_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_store_session);
  let mut bob = User::new("bob".to_string());
  bob.set_enabled(true);
  bob.add_password_hash(AclPassword::from_string("secret"));
  block_on(acl_store.write(1, b"bob", &bob.to_bytes())).unwrap();

  // ns0 会话订阅 chan（broker 键 0:chan）
  assert!(s.network_subscribe(false, &[b"chan"]));
  assert_eq!(broker.num_subscriptions(b"0:chan"), 1);
  drain_output(&mut s);

  // 订阅态 AUTH 1#bob 换租：拒绝且订阅/计数/ns 原样保留
  let args: Vec<&[u8]> = vec![b"1#bob", b"secret"];
  assert!(block_on(s.network_auth_session::<SegmentedDevice>(&args, &acl_store)).unwrap());
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "-ERR Can't change namespace while subscribed\r\n"
  );
  assert_eq!(s.namespace, 0);
  assert_eq!(broker.num_subscriptions(b"0:chan"), 1);
  assert_eq!(s.pubsub.num_active_channels(), 1);
  assert!(s.is_subscription_session);

  // 主动全部退订后再切换：正常放行，绑定 ns1
  assert!(s.network_unsubscribe(&[]));
  assert_eq!(broker.num_subscriptions(b"0:chan"), 0);
  drain_output(&mut s);
  assert!(block_on(s.network_auth_session::<SegmentedDevice>(&args, &acl_store)).unwrap());
  assert_eq!(String::from_utf8(drain_output(&mut s)).unwrap(), "+OK\r\n");
  assert_eq!(s.namespace, 1);
}

/// HELLO 认证臂同门：订阅态换租拒绝，认证中断即协议不升级、客户端名不落
#[test]
fn hello_auth_to_foreign_ns_rejected_while_subscribed() {
  let broker = Arc::new(SubscribeBroker::new());
  let (mut s, _auth) = acl_session(51);
  s.attach_pubsub(broker.clone());
  let (_dir, store) = wtest_base::open_test_store("resp-hello-ns-switch.db").unwrap();
  let acl_store_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_store_session);
  let mut bob = User::new("bob".to_string());
  bob.set_enabled(true);
  bob.add_password_hash(AclPassword::from_string("secret"));
  block_on(acl_store.write(1, b"bob", &bob.to_bytes())).unwrap();

  assert!(s.network_subscribe(false, &[b"chan"]));
  drain_output(&mut s);

  let mut out = Vec::new();
  assert!(!block_on(s.process_hello_command_state::<SegmentedDevice>(
    Some(3),
    b"1#bob",
    b"secret",
    Some("myclient"),
    Some(&acl_store),
    &mut out
  )));
  assert_eq!(
    String::from_utf8(out).unwrap(),
    "-ERR Can't change namespace while subscribed\r\n"
  );
  assert_eq!(s.resp_protocol_version, DEFAULT_RESP_VERSION);
  assert_eq!(s.client_name, None);
  assert_eq!(s.namespace, 0);
  assert_eq!(s.pubsub.num_active_channels(), 1);
  assert!(s.is_subscription_session);
  assert_eq!(broker.num_subscriptions(b"0:chan"), 1);
}

/// 同命名空间重新 AUTH 不受限：订阅与计数完整保留
///
/// 论式随 doc/zh/db.md §2.2 缺省口径对齐：ns1 会话重认证走裸名 `bob`（裸名
/// 必落当前租户），显式 `<ns>#` 前缀仅 ns0 可携——原用例在 ns1 会话写 `1#bob`
/// 期望放行，与 §2.2/§3.5「namespace != 0 的连接严禁携带 `#`」及在管面同口径
/// 门禁（acl_namespace_admin_tests §3.3）相悖，属红用例出生即错的过期论式，
/// 非产品回归
#[test]
fn same_ns_reauth_keeps_subscription() {
  let broker = Arc::new(SubscribeBroker::new());
  let (mut s, _auth) = acl_session(52);
  s.attach_pubsub(broker.clone());
  let (_dir, store) = wtest_base::open_test_store("resp-auth-same-ns.db").unwrap();
  let acl_store_session = store.new_session().unwrap();
  let acl_store = AclStore::new(&acl_store_session);
  let mut bob = User::new("bob".to_string());
  bob.set_enabled(true);
  bob.add_password_hash(AclPassword::from_string("secret"));
  block_on(acl_store.write(1, b"bob", &bob.to_bytes())).unwrap();

  // 未订阅先切换至 ns1（ns0 会话可携显式 `1#` 前缀）
  let args: Vec<&[u8]> = vec![b"1#bob", b"secret"];
  assert!(block_on(s.network_auth_session::<SegmentedDevice>(&args, &acl_store)).unwrap());
  assert_eq!(String::from_utf8(drain_output(&mut s)).unwrap(), "+OK\r\n");
  assert_eq!(s.namespace, 1);

  // ns1 内订阅（隔离键 1:chan）
  assert!(s.network_subscribe(false, &[b"chan"]));
  assert_eq!(broker.num_subscriptions(b"1:chan"), 1);
  drain_output(&mut s);

  // 同 ns 裸名重新 AUTH：放行且订阅原样保留（blocked_by_subscription 仅拦
  // target_ns != 当前 ns，同 ns 换租判定不触发）
  let bare_args: Vec<&[u8]> = vec![b"bob", b"secret"];
  assert!(block_on(s.network_auth_session::<SegmentedDevice>(&bare_args, &acl_store)).unwrap());
  assert_eq!(String::from_utf8(drain_output(&mut s)).unwrap(), "+OK\r\n");
  assert_eq!(s.namespace, 1);
  assert_eq!(broker.num_subscriptions(b"1:chan"), 1);
  assert_eq!(s.pubsub.num_active_channels(), 1);
  assert!(s.is_subscription_session);

  // ns1 会话携同租显式前缀 `1#bob`：认证臂与在管臂同口径门禁拒绝（判据
  // foreign_namespace_denied 的 `#` 拦截臂，落位前拦截），订阅/计数/ns 原样
  // 不受扰动
  assert!(block_on(s.network_auth_session::<SegmentedDevice>(&args, &acl_store)).unwrap());
  assert_eq!(
    String::from_utf8(drain_output(&mut s)).unwrap(),
    "-ERR permission denied: only namespace 0 can manage foreign namespaces\r\n"
  );
  assert_eq!(s.namespace, 1);
  assert_eq!(broker.num_subscriptions(b"1:chan"), 1);
  assert_eq!(s.pubsub.num_active_channels(), 1);
  assert!(s.is_subscription_session);
}

/// 丢弃计数观测面链路（rn14 盲区收口：邮箱满水位拒收 → 真值单源计数可读；
/// 投影显影环由 consumer_registry 内联测试锚定——pubsub-drop 仅非零输出）。
/// 真实路径：SUBSCRIBE 经命令解析入中枢，洪水经 broker 广播直击订阅者邮箱，
/// 拒收在邮箱内单点入账；未接线与容量内形态恒 0（零丢弃不得入账）。
#[test]
fn pubsub_dropped_counter_flows_into_client_view() {
  use wpubsub::session_commands::DEFAULT_MAILBOX_CAPACITY;

  let mut s = session(0);
  // --pubsub 关闭形态（未接线）：观测面恒 0
  assert_eq!(s.pubsub.dropped(), 0);

  let broker = Arc::new(SubscribeBroker::new());
  s.attach_pubsub(Arc::clone(&broker));
  assert!(pump_feed(&mut s, b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nhot\r\n").is_some());
  let _ = drain_output(&mut s);
  assert_eq!(s.pubsub.dropped(), 0, "接线后零丢弃");

  let channel = ChannelNsPrefix::new(s.namespace).isolate(b"hot");

  // 容量内洪水：广播逐帧命中唯一订阅者邮箱，零计数
  for _ in 0..DEFAULT_MAILBOX_CAPACITY {
    assert_eq!(broker.publish_now(&channel, b"payload"), 1);
  }
  assert_eq!(s.pubsub.dropped(), 0, "容量内成功帧不得入账");

  // 满位窗口：继续广播即拒收丢尾，计数逐帧单调累加并在视图导出面可读
  for rejected in 1..=5u64 {
    assert_eq!(broker.publish_now(&channel, b"overflow"), 1);
    assert_eq!(s.pubsub.dropped(), rejected);
  }

  // 会话排空推送帧（drain_pubsub_frames）后水位回落,历史丢弃量
  // 不回退——观测面单调,慢订阅者丢尾量恒可运维定位
  assert_eq!(s.drain_pubsub_frames(), DEFAULT_MAILBOX_CAPACITY);
  assert_eq!(broker.publish_now(&channel, b"after-drain"), 1);
  assert_eq!(s.pubsub.dropped(), 5, "恢复收帧后历史丢弃量不得回退");
}
