use std::{fs::read_dir, str::from_utf8};

use wbase::pool::LimitedFixedBufferPool;
use wconf::{RuntimeServerConfig, RuntimeServerOptions};
use wnode::resp::{
  acl_store::AclStore, config_commands::ServerConfig, resp_server_session::RespServerSession,
};
use wnode_test::{err_frame, pump};
use wresp::{argslice::ArgSlice, cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR, command::RespCommand};

#[test]
fn ping_test() {
  let s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_ping(&[], &mut out).unwrap();
  assert_eq!(out, b"+PONG\r\n");
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:PingMessageTest
#[test]
fn ping_message_test() {
  let s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_ping(&[b"HELLO"], &mut out).unwrap();
  assert_eq!(out, b"$5\r\nHELLO\r\n");
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:PingErrorMessageTest
#[test]
fn ping_error_message_test() {
  let s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_ping(&[b"HELLO", b"WORLD"], &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'PING' command\r\n"
  );
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:EchoWithNoMessageReturnErrorTest
#[test]
fn echo_with_no_message_return_error_test() {
  let s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_echo(&[], &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'ECHO' command\r\n"
  );
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:EchoWithMessagesReturnErrorTest
#[test]
fn echo_with_messages_return_error_test() {
  let s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_echo(&[b"HELLO", b"WORLD"], &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'ECHO' command\r\n"
  );
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:EchoWithMessageTest
#[test]
fn echo_with_message_test() {
  let s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_echo(&[b"HELLO"], &mut out).unwrap();
  assert_eq!(out, b"$5\r\nHELLO\r\n");
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:TimeCommandTest
#[test]
fn time_command_test() {
  let mut s = RespServerSession::default();
  assert!(s.process_other_commands(RespCommand::Time));
  assert!(s.output.starts_with(b"*2\r\n$"));
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:TimeWithReturnErrorTest
#[test]
fn time_with_return_error_test() {
  let mut s = RespServerSession::default();
  // 经解析状态注入 1 个多余参数，走真实 TIME 分派路径断言参数校验
  s.parse_state.count = 1;
  s.parse_state.root_buffer.push(ArgSlice::new(0, 1));
  assert!(s.process_other_commands(RespCommand::Time));
  assert_eq!(
    s.output,
    b"-ERR wrong number of arguments for 'TIME' command\r\n"
  );
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:SeSaveTest
#[test]
fn se_save_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_save(&[b"1", b"2"], &mut out).unwrap();
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'SAVE' command\r\n"
  );
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:SimpleConfigGet
#[test]
fn config_get_slave_read_only_test() {
  let mut s = ServerConfig;
  let rc = RuntimeServerConfig::with_defaults();
  let mut out = Vec::new();
  // RESP2 会话（双倍数组口径）
  s.network_config_get(&[b"slave-read-only"], &rc, 2, &mut out)
    .unwrap();
  assert_eq!(out, b"*2\r\n$15\r\nslave-read-only\r\n$3\r\nyes\r\n");
}

/// libs/server/Resp/AdminCommands.cs:NetworkHCOLLECT/NetworkZCOLLECT 集成测试
mod admin_collect {
  use std::{sync::Arc, thread, time::Duration};

  use compio::runtime::Runtime;
  use wdev::SegmentedDevice;
  use wkv::WedbStore;
  use wnode::{
    MessageConsumerFace, RespSessionConsumer,
    resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  };
  use wnode_test::err_frame;
  use wresp::cmd_strings::RESP_ERR_WRONG_TYPE;
  use wtest_base::{resp_frame_str, test_store_config};

  fn with_session(f: impl FnOnce(&mut RespSessionConsumer)) {
    Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("collect.db")).unwrap());
      let config = test_store_config();
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
      let mut consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api);
      f(&mut consumer);
    });
  }

  fn feed(consumer: &mut RespSessionConsumer, args: &[&str]) -> Vec<u8> {
    // 泵等价序：直填会话接收缓冲 → 唯一入口消费
    let mut scratch = consumer.take_recv_scratch();
    scratch.extend_from_slice(&resp_frame_str(args));
    consumer.return_recv_scratch(scratch);
    let mut resp = Vec::new();
    assert!(consumer.try_consume_messages_into(&mut resp).is_some());
    resp
  }

  /// test/standalone/Garnet.test/RespAdminCommandsTests.cs:ConfigWrongNumberOfArguments
  #[test]
  fn config_wrong_number_of_arguments() {
    with_session(|c| {
      let resp = feed(c, &["CONFIG"]);
      assert_eq!(
        resp,
        b"-ERR wrong number of arguments for 'CONFIG' command\r\n"
      );
    });
  }

  /// test/standalone/Garnet.test/RespAdminCommandsTests.cs:ConfigGetWrongNumberOfArguments
  #[test]
  fn config_get_wrong_number_of_arguments() {
    with_session(|c| {
      let resp = feed(c, &["CONFIG", "GET"]);
      assert_eq!(
        resp,
        b"-ERR wrong number of arguments for 'CONFIG|GET' command\r\n"
      );
    });
  }

  #[test]
  fn hcollect_keeps_live_fields_and_reports_ok() {
    with_session(|c| {
      feed(c, &["HSET", "h", "f1", "v1", "f2", "v2"]);

      feed(c, &["HCOLLECT", "h"]);
      let resp = feed(c, &["HLEN", "h"]);
      assert_eq!(resp, b":2\r\n");
      let resp = feed(c, &["HCOLLECT", "missing"]);
      assert_eq!(resp, b"+OK\r\n");
    });
  }

  #[test]
  fn hcollect_clears_expired_fields() {
    with_session(|c| {
      feed(c, &["HSET", "hx", "keep", "v", "gone", "v"]);
      feed(c, &["HPEXPIRE", "hx", "100", "FIELDS", "1", "gone"]);

      thread::sleep(Duration::from_millis(200));
      let resp = feed(c, &["HCOLLECT", "hx"]);
      assert_eq!(resp, b"+OK\r\n");
      let resp = feed(c, &["HLEN", "hx"]);
      assert_eq!(resp, b":1\r\n");
    });
  }

  #[test]
  fn hcollect_wrongtype_and_star_degrade() {
    with_session(|c| {
      feed(c, &["SET", "str", "x"]);

      let resp = feed(c, &["HCOLLECT", "str"]);
      assert_eq!(resp, err_frame(RESP_ERR_WRONG_TYPE));

      let resp = feed(c, &["HCOLLECT", "*"]);
      assert!(resp.is_empty(), "同步段仅校验，不残留输出");
      let slow = c.take_slow_wait().expect("HCOLLECT * 应挂起慢路径");
      let resp = Runtime::new().unwrap().block_on(slow.resolve());
      assert_eq!(resp, b"+OK\r\n");
    });
  }

  #[test]
  fn zcollect_reports_ok_and_wrongtype() {
    with_session(|c| {
      feed(c, &["ZADD", "z", "1", "m"]);

      let resp = feed(c, &["ZCOLLECT", "z"]);
      assert_eq!(resp, b"+OK\r\n");
      let resp = feed(c, &["ZCARD", "z"]);
      assert_eq!(resp, b":1\r\n");

      let resp = feed(c, &["ZCOLLECT", "missing"]);
      assert_eq!(resp, b"+OK\r\n");

      feed(c, &["SET", "str", "x"]);
      let resp = feed(c, &["ZCOLLECT", "str"]);
      assert_eq!(resp, err_frame(RESP_ERR_WRONG_TYPE));
    });
  }
}

// ---------------------------------------------------------------------------
// SAVE / BGSAVE / LASTSAVE / COMMITAOF 接线回归
//（C# NetworkSAVE → storeWrapper.TakeCheckpointAsync；NetworkCOMMITAOF →
// CommitAOFAsync 后恒回 "AOF file committed"）
// ---------------------------------------------------------------------------

use std::{path::PathBuf, sync::Arc};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, PeerSource, RespSessionConsumer,
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::{ConnectionProtectionOption, RespServerSessionOptions as Opts},
  },
};
use wtest_base::{resp_frame_str, test_store_config};

/// 挂检查点通道的会话消费者（独立临时目录）
fn checkpoint_consumer() -> (
  RespSessionConsumer,
  PathBuf,
  Arc<SingleDatabaseManager<SegmentedDevice>>,
) {
  checkpoint_consumer_with(|opts| opts)
}

/// 同 [`Self::checkpoint_consumer`]，允许定制会话选项
fn checkpoint_consumer_with(
  customize: impl FnOnce(Opts) -> Opts,
) -> (
  RespSessionConsumer,
  PathBuf,
  Arc<SingleDatabaseManager<SegmentedDevice>>,
) {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("admin.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线；GC 默认禁用，对标 -1）
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  let cp_dir = dir.join("Store").join("checkpoints");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    device,
    cp_dir.clone(),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(cp_dir.clone(), db));
  let api =
    StoreGarnetApi::new(store.new_session().unwrap()).with_database_manager(Arc::clone(&mgr));
  let mut consumer = RespSessionConsumer::new(1, customize(Opts::default()), Arc::new(api));
  // 本地回环来源（typed 判定折叠形，accept 侧同源口径）：Local 保护档放行
  consumer.set_remote_endpoint("127.0.0.1:0", PeerSource::Ip { loopback: true });
  // 生产形态注入独立运行时配置实例（裸构造回落 shared_default 为进程级
  // 共享单例，CONFIG SET 用例不得跨用例串扰）
  consumer.set_runtime_config(Arc::new(RuntimeServerConfig::new(
    RuntimeServerOptions::default(),
  )));
  (consumer, cp_dir, mgr)
}

/// 单命令往返（同步快路径）
fn roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// 慢命令往返（网络泵角色由 block_on 承担）
fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// COMMITAOF：会话侧校验后转挂存储执行域慢路径（与 SAVE 同通道）；
/// 未装配执行域按失败惯例降级；AOF 禁用态（C# !EnableAOF 不做 I/O，
/// NetworkCOMMITAOF 无视提交结果恒回文案）慢路径闭环 → "AOF file committed"
#[test]
fn commitaof_replies_fixed_text() {
  // 未装配存储执行域：route_slow_command 降级写明错误，不静默
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_commitaof(&[], &mut out).unwrap();
  assert_eq!(out, b"-ERR generic error\r\n");
  assert!(s.take_slow_wait().is_none());

  // 装配检查点通道（AOF 禁用态 aof: None）：转挂慢路径 + 闭环恒回文案
  let rt = Runtime::new().unwrap();
  let (mut c, _cp_dir, _mgr) = checkpoint_consumer();
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$9\r\nCOMMITAOF\r\n"),
    b"+AOF file committed\r\n"
  );
  // 带 DBID 形态同款闭环（会话侧校验通过 → 慢路径）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$9\r\nCOMMITAOF\r\n$1\r\n0\r\n"),
    b"+AOF file committed\r\n"
  );
  // DBID 非法：会话侧校验即拒（不转挂慢路径）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*2\r\n$9\r\nCOMMITAOF\r\n$2\r\n16\r\n"),
    b"-ERR DB index is out of range.\r\n"
  );
}

/// SAVE / BGSAVE / LASTSAVE：检查点通道闭环 + LASTSAVE 时间戳推进
#[test]
fn save_bgsave_lastsave_via_checkpoint_channel() {
  let rt = Runtime::new().unwrap();
  let (mut c, cp_dir, mgr) = checkpoint_consumer();

  // 无检查点前 LASTSAVE → :0（C# DateTimeOffset.FromUnixTimeSeconds(0) 初值；
  // LASTSAVE 经存储执行域 checkpoint 通道读取，慢路径闭环）
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$8\r\nLASTSAVE\r\n"),
    b":0\r\n"
  );

  // SAVE → +OK；检查点目录产生快照；last_save_ms 推进
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$4\r\nSAVE\r\n"),
    b"+OK\r\n"
  );
  let dir_ok = read_dir(&cp_dir)
    .map(|entries| entries.count() > 0)
    .unwrap_or(false);
  assert!(dir_ok, "检查点目录应生成快照条目: {}", cp_dir.display());
  let after_save_ms = mgr.last_save_ms();
  assert!(after_save_ms > 0, "SAVE 后 last_save_ms 应推进");

  // LASTSAVE → 秒级时间戳（与 SAVE 时刻一致，慢路径读取）
  let out = slow_roundtrip(&rt, &mut c, b"*1\r\n$8\r\nLASTSAVE\r\n");
  let expect = format!(":{}\r\n", after_save_ms / 1000);
  assert_eq!(out, expect.as_bytes());

  // BGSAVE → "Background saving started"
  assert_eq!(
    slow_roundtrip(&rt, &mut c, b"*1\r\n$6\r\nBGSAVE\r\n"),
    b"+Background saving started\r\n"
  );

  // SAVE 带 DBID 参数校验：非法 DBID → 拒绝（同步段）
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$4\r\nSAVE\r\n$3\r\nABC\r\n"),
    b"-ERR value is not an integer or out of range.\r\n"
  );
}

/// INFO：wmetric 段分发接线（server 段真实填充 + cluster/replication 投影；
/// 对标 C# NetworkINFO 的段分发口径）
#[test]
fn info_sections_via_wmetric_provider() {
  let (mut c, _dir, _ts) = checkpoint_consumer();

  // INFO（无参）→ DEFAULT 集合 bulk string 帧
  let out = roundtrip(&mut c, b"*1\r\n$4\r\nINFO\r\n");
  let text = String::from_utf8_lossy(&out);
  assert!(
    text.starts_with('$'),
    "应为 bulk string 帧: {}",
    &text[..24]
  );
  assert!(text.contains("# Server\r\n"), "应含 Server 段: {text}");
  assert!(text.contains("garnet_version:"), "应含版本指标: {text}");
  assert!(text.contains("run_id:"), "应含运行实例 id: {text}");
  assert!(text.contains("cluster_enabled:0\r\n"), "单机形态: {text}");
  assert!(
    text.contains("connected_slaves:0"),
    "应含 Replication 段: {text}"
  );
  assert!(
    text.contains("connected_clients:"),
    "应含 Clients 段: {text}"
  );
  assert!(!text.contains("db0:"), "键空间空库不出段: {text}");

  // 非法段 → C# 口径错误
  let out = roundtrip(&mut c, b"*2\r\n$4\r\nINFO\r\n$6\r\nNOSUCH\r\n");
  assert_eq!(out, b"-ERR Invalid section NOSUCH. Try INFO HELP\r\n");
}

/// INFO 非法段名含 CRLF：回显段须过净化单点，一请求只出一帧
/// （帧注入回归，对标 wmetric InfoCommand 的 cs::abort_with_error_message 单口）
#[test]
fn info_invalid_section_crlf_cannot_inject_frame() {
  let (mut c, _dir, _ts) = checkpoint_consumer();

  // 段名 = "BAD\r\n:1\r\n"（bulk string 内的 CRLF 由解析器原样保留）
  let out = roundtrip(&mut c, b"*2\r\n$4\r\nINFO\r\n$9\r\nBAD\r\n:1\r\n\r\n");
  assert_eq!(out, b"-ERR Invalid section BAD. Try INFO HELP\r\n");
  assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 1);
}

/// INFO CLUSTER 单段与 INFO RESET / INFO HELP 语义
#[test]
fn info_cluster_section_and_reset_help() {
  let (mut c, _dir, _ts) = checkpoint_consumer();

  let out = roundtrip(&mut c, b"*2\r\n$4\r\nINFO\r\n$7\r\nCLUSTER\r\n");
  let text = String::from_utf8_lossy(&out);
  assert_eq!(text, "$30\r\n# Cluster\r\ncluster_enabled:0\r\n\r\n");

  // RESET → +OK（C# resetEventFlags 置位路径）
  assert_eq!(
    roundtrip(&mut c, b"*2\r\n$4\r\nINFO\r\n$5\r\nRESET\r\n"),
    b"+OK\r\n"
  );

  // HELP → 数组形态帮助文本
  let out = roundtrip(&mut c, b"*2\r\n$4\r\nINFO\r\n$4\r\nHELP\r\n");
  assert_eq!(out[0], b'*');
}

// ---------------------------------------------------------------------------
// DEBUG PURGEBP / EXPDELSCAN 接线回归
//（C# PurgeBPCommand：ServerListener 遍历 storeWrapper.Servers 直清监听池、
// 不依赖集群；MigrationManager/ReplicationManager 经 ClusterPurgeBufferPool，
// clusterSession == null → CLUSTER_DISABLED，成功回 "GC completed for <type>"；
// NetworkEXPDELSCAN → storeWrapper.ExpiredKeyDeletionScan，应答 *2 计数对）
// ---------------------------------------------------------------------------

/// DEBUG PURGEBP：单机无集群切面，迁移/复制管理器 → CLUSTER_DISABLED；
/// ManagerType 解析失败 → 语法错误
#[test]
fn purgebp_without_cluster_disabled() {
  let (mut c, _dir, _ts) = checkpoint_consumer_with(|opts| Opts {
    enable_debug_command: ConnectionProtectionOption::Yes,
    ..opts
  });

  let out = roundtrip(
    &mut c,
    b"*3\r\n$5\r\nDEBUG\r\n$7\r\nPURGEBP\r\n$16\r\nMigrationManager\r\n",
  );
  assert_eq!(out, b"-ERR This instance has cluster support disabled\r\n");

  let out = roundtrip(
    &mut c,
    b"*3\r\n$5\r\nDEBUG\r\n$7\r\nPURGEBP\r\n$18\r\nReplicationManager\r\n",
  );
  assert_eq!(out, b"-ERR This instance has cluster support disabled\r\n");

  let out = roundtrip(
    &mut c,
    b"*3\r\n$5\r\nDEBUG\r\n$7\r\nPURGEBP\r\n$4\r\nnope\r\n",
  );
  assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));
}

/// DEBUG PURGEBP ServerListener：单机模式（无集群切面）直清监听层缓冲池
/// 并回 "GC completed for ServerListener"（C# PurgeBPCommand.cs:63-66 遍历
/// storeWrapper.Servers 调 GarnetServerTcp.Purge 的等价承接）；purge 后
/// 池闲置计数归零
#[test]
fn purgebp_server_listener_standalone_purges_pool() {
  let (mut c, _dir, _ts) = checkpoint_consumer_with(|opts| Opts {
    enable_debug_command: ConnectionProtectionOption::Yes,
    ..opts
  });

  let pool = LimitedFixedBufferPool::new(4096, 4);
  c.attach_buffer_pool(Arc::clone(&pool));

  // 借出后归还制造闲置缓冲（网络泵日常运行态的真实闲置来源）
  drop(pool.get_ref(4096));
  assert_eq!(pool.free_count(), 1, "归还后应有一块闲置缓冲");

  let out = roundtrip(
    &mut c,
    b"*3\r\n$5\r\nDEBUG\r\n$7\r\nPURGEBP\r\n$14\r\nServerListener\r\n",
  );
  assert_eq!(out, b"+GC completed for ServerListener\r\n");
  assert_eq!(pool.free_count(), 0, "purge 后闲置计数应归零");
}

/// EXPDELSCAN：空库慢路径闭环，应答 *2 计数对（expired=0 scanned=0）
#[test]
fn expdelscan_reports_zero_counts_on_empty_store() {
  let rt = Runtime::new().unwrap();
  let (mut c, _dir, _ts) = checkpoint_consumer();

  // 非法 DBID → 快路径拦截（C# TryParseDatabaseId 口径）
  let out = roundtrip(&mut c, b"*2\r\n$10\r\nEXPDELSCAN\r\n$3\r\nabc\r\n");
  assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

  // 无参 → 慢路径默认库 0：*2 + 两个十进制计数 bulk string
  let out = slow_roundtrip(&rt, &mut c, b"*1\r\n$10\r\nEXPDELSCAN\r\n");
  assert_eq!(out, b"*2\r\n$1\r\n0\r\n$1\r\n0\r\n");
}

/// EXPDELSCAN 与后台过期键删除扫描互斥门（C#
/// AdminCommands.cs:NetworkEXPDELSCAN 频率门）：freq 槽位 > 0 时拒绝
///（RESP_ERR_EXPDELSCAN_INVALID），<= 0 时受理
#[test]
fn expdelscan_rejected_while_bg_scan_enabled() {
  let rt = Runtime::new().unwrap();
  let (mut c, _dir, _ts) = checkpoint_consumer();

  // freq > 0 → 频率门拒绝（判据序 arity → 频率门 → DBID），文案与
  // C# CmdStrings.cs:RESP_ERR_EXPDELSCAN_INVALID 逐字一致
  let out = roundtrip(
    &mut c,
    &resp_frame_str(&["CONFIG", "SET", "expired-key-deletion-scan-freq", "5"]),
  );
  assert_eq!(out, b"+OK\r\n");
  let out = slow_roundtrip(&rt, &mut c, b"*1\r\n$10\r\nEXPDELSCAN\r\n");
  assert_eq!(
    out,
    &b"-ERR Cannot execute EXPDELSCAN with background expired key deletion scan enabled\r\n"[..]
  );

  // freq <= 0 → 受理，恢复 *2 计数对
  let out = roundtrip(
    &mut c,
    &resp_frame_str(&["CONFIG", "SET", "expired-key-deletion-scan-freq", "-1"]),
  );
  assert_eq!(out, b"+OK\r\n");
  let out = slow_roundtrip(&rt, &mut c, b"*1\r\n$10\r\nEXPDELSCAN\r\n");
  assert_eq!(out, b"*2\r\n$1\r\n0\r\n$1\r\n0\r\n");
}

/// DEBUG FLUSHANDEVICT：慢路径刷盘并驱逐主存储全部页面至磁盘区，返回 +OK\r\n，且键数据仍可冷读回
#[test]
fn debug_flushandevict_success_and_cold_read() {
  let rt = Runtime::new().unwrap();
  let (mut c, _dir, _ts) = checkpoint_consumer_with(|opts| Opts {
    enable_debug_command: ConnectionProtectionOption::Local,
    ..opts
  });

  // 写入键值
  let out = roundtrip(&mut c, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
  assert_eq!(out, b"+OK\r\n");

  // 验证内存读出
  let out = roundtrip(&mut c, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(out, b"$3\r\nbar\r\n");

  // 执行 DEBUG FLUSHANDEVICT（慢路径刷盘并驱逐全部内存页）
  let out = slow_roundtrip(
    &rt,
    &mut c,
    b"*2\r\n$5\r\nDEBUG\r\n$13\r\nFLUSHANDEVICT\r\n",
  );
  assert!(out.starts_with(b"+OK"));

  // 驱逐后仍能从磁盘冷读回（走慢路径异步磁盘读）
  let out = slow_roundtrip(&rt, &mut c, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(out, b"$3\r\nbar\r\n");
}

/// DEBUG FLUSHANDEVICT：远程连接且为 Local 保护时拒绝执行
#[test]
fn debug_flushandevict_disallowed_on_remote_endpoint() {
  let (mut c, _dir, _ts) = checkpoint_consumer_with(|opts| Opts {
    enable_debug_command: ConnectionProtectionOption::Local,
    ..opts
  });
  c.set_remote_endpoint("10.0.0.1:1234", PeerSource::Ip { loopback: false });

  let out = roundtrip(&mut c, b"*2\r\n$5\r\nDEBUG\r\n$13\r\nFLUSHANDEVICT\r\n");
  assert!(out.starts_with(b"-ERR DEBUG command not allowed."));
}

/// DEBUG FLUSHANDEVICT：参数数量不匹配报错
#[test]
fn debug_flushandevict_wrong_arg_count() {
  let (mut c, _dir, _ts) = checkpoint_consumer_with(|opts| Opts {
    enable_debug_command: ConnectionProtectionOption::Local,
    ..opts
  });

  let out = roundtrip(
    &mut c,
    b"*3\r\n$5\r\nDEBUG\r\n$13\r\nFLUSHANDEVICT\r\n$5\r\nextra\r\n",
  );
  assert_eq!(
    out,
    b"-ERR unknown subcommand or wrong number of arguments for 'FLUSHANDEVICT'. Try DEBUG HELP\r\n"
  );
}

/// 多会话并发 SAVE / BGSAVE：常驻 SingleDatabaseManager 互斥状态协同
#[test]
fn save_bgsave_concurrent_mutual_exclusion() {
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("admin_concurrent.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device)).unwrap());
  let cp_dir = dir.join("Store").join("checkpoints");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    device,
    cp_dir.clone(),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(cp_dir.clone(), db));

  // 两个会话共享同一常驻 SingleDatabaseManager 实例
  let api1 =
    StoreGarnetApi::new(store.new_session().unwrap()).with_database_manager(Arc::clone(&mgr));
  let mut c1 = RespSessionConsumer::new(1, Opts::default(), Arc::new(api1));
  c1.set_remote_endpoint("127.0.0.1:0", PeerSource::Ip { loopback: true });

  let api2 =
    StoreGarnetApi::new(store.new_session().unwrap()).with_database_manager(Arc::clone(&mgr));
  let mut c2 = RespSessionConsumer::new(2, Opts::default(), Arc::new(api2));
  c2.set_remote_endpoint("127.0.0.1:0", PeerSource::Ip { loopback: true });

  // 当 mgr 处于 paused 状态时，会话 2 执行 SAVE 和 BGSAVE 均互斥拒绝
  assert!(mgr.try_pause_checkpoints());
  assert_eq!(
    slow_roundtrip(&rt, &mut c2, b"*1\r\n$4\r\nSAVE\r\n"),
    b"-ERR checkpoint already in progress\r\n"
  );
  assert_eq!(
    slow_roundtrip(&rt, &mut c2, b"*1\r\n$6\r\nBGSAVE\r\n"),
    b"-ERR checkpoint already in progress\r\n"
  );
  mgr.resume_checkpoints();

  // 释放后会话 1 执行 SAVE 成功
  assert_eq!(
    slow_roundtrip(&rt, &mut c1, b"*1\r\n$4\r\nSAVE\r\n"),
    b"+OK\r\n"
  );

  // 两会话查询 LASTSAVE 时读到相同的新时间戳
  let last1 = slow_roundtrip(&rt, &mut c1, b"*1\r\n$8\r\nLASTSAVE\r\n");
  let last2 = slow_roundtrip(&rt, &mut c2, b"*1\r\n$8\r\nLASTSAVE\r\n");
  assert_eq!(last1, last2);
  assert_ne!(last1, b":0\r\n");
}

/// 案一回归锁：DEBUG ERROR 传入非 ASCII 字节按 ASCII 逐字节折为 '?'
///（0xFF 载荷回 "-?\r\n"，多字节 "错误" 回 "-??????\r\n"，CRLF 载荷被截断净化）
#[test]
fn debug_error_non_ascii_folding() {
  let (mut c, _dir, _ts) = checkpoint_consumer_with(|opts| Opts {
    enable_debug_command: ConnectionProtectionOption::Yes,
    ..opts
  });

  // 单字节 0xFF -> 折叠为 '?'
  let out = roundtrip(
    &mut c,
    b"*3\r\n$5\r\nDEBUG\r\n$5\r\nERROR\r\n$1\r\n\xff\r\n",
  );
  assert_eq!(out, b"-?\r\n");

  // 多字节非 ASCII "错误"（UTF-8 6 字节） -> 逐字节折叠为 6 个 '?'
  let out = roundtrip(
    &mut c,
    b"*3\r\n$5\r\nDEBUG\r\n$5\r\nERROR\r\n$6\r\n\xe9\x94\x99\xe8\xaf\xaf\r\n",
  );
  assert_eq!(out, b"-??????\r\n");

  // CRLF 载荷被写帧单点净化截断
  let out = roundtrip(
    &mut c,
    b"*3\r\n$5\r\nDEBUG\r\n$5\r\nERROR\r\n$8\r\nerr\r\nmsg\r\n",
  );
  assert_eq!(out, b"-err\r\n");
}

/// 案一回归锁：DEBUG 未知子命令与 arity 错误文案非 ASCII 字节折叠
#[test]
fn debug_unknown_and_arity_non_ascii_folding() {
  let (mut c, _dir, _ts) = checkpoint_consumer_with(|opts| Opts {
    enable_debug_command: ConnectionProtectionOption::Yes,
    ..opts
  });

  // 未知子命令 0xFF -> '?'
  let out = roundtrip(&mut c, b"*2\r\n$5\r\nDEBUG\r\n$1\r\n\xff\r\n");
  assert_eq!(out, b"-ERR unknown subcommand '?'. Try DEBUG HELP\r\n");

  // 未知子命令 "未知"（6 字节） -> '??????'
  let out = roundtrip(
    &mut c,
    b"*2\r\n$5\r\nDEBUG\r\n$6\r\n\xe6\x9c\xaa\xe7\x9f\xa5\r\n",
  );
  assert_eq!(out, b"-ERR unknown subcommand '??????'. Try DEBUG HELP\r\n");

  // 子命令参数数量错误回显
  let out = roundtrip(
    &mut c,
    b"*4\r\n$5\r\nDEBUG\r\n$6\r\nERROR\xff\r\n$1\r\na\r\n$1\r\nb\r\n",
  );
  assert_eq!(out, b"-ERR unknown subcommand 'ERROR?'. Try DEBUG HELP\r\n");
}

/// 案一回归锁：HELLO 未知非 ASCII 选项错误帧回显包含折叠字符
#[compio::test]
async fn hello_unknown_option_non_ascii_folding() {
  let (_dir, session, mut s) = wnode_test::test_env(false);
  let store = AclStore::new(&session);
  let mut out = Vec::new();

  // 单字节 0xFF 选项 -> '?'
  s.network_hello(&[b"3", &[0xFF]], &store, &mut out)
    .await
    .unwrap();
  assert_eq!(out, b"-ERR Syntax error in HELLO option '?'\r\n");

  // 多字节非 ASCII "未知" 选项 -> '??????'
  out.clear();
  s.network_hello(&[b"3", "未知".as_bytes()], &store, &mut out)
    .await
    .unwrap();
  assert_eq!(out, b"-ERR Syntax error in HELLO option '??????'\r\n");
}

/// RESP3 会话 INFO server 段首帧为 =<len>\r\ntxt:；RESP2 回归保持 $ bulk 不变；
/// 慢路径与快路径两臂均对齐型制契约。
#[test]
fn info_resp3_verbatim_string_and_resp2_bulk_in_session() {
  let rt = Runtime::new().unwrap();
  let (mut c, _dir, _mgr) = checkpoint_consumer();

  // 1. RESP2 默认会话：INFO server 返回 $ bulk string 帧
  let out_resp2 = roundtrip(&mut c, b"*2\r\n$4\r\nINFO\r\n$6\r\nSERVER\r\n");
  assert!(
    out_resp2.starts_with(b"$"),
    "RESP2 应以 bulk string ($) 开头"
  );
  let crlf2 = out_resp2
    .windows(2)
    .position(|w| w == b"\r\n")
    .expect("CRLF required");
  let len_str2 = from_utf8(&out_resp2[1..crlf2]).expect("len utf8");
  let len2: usize = len_str2.parse().expect("valid length");
  let payload2 = &out_resp2[crlf2 + 2..out_resp2.len() - 2];
  assert_eq!(
    len2,
    payload2.len(),
    "RESP2 bulk length 必须等于 payload 字节数"
  );
  assert!(payload2.starts_with(b"# Server\r\n"));

  // 2. 慢路径 RESP2 会话：INFO STOREHASHTABLE 同样返回 $ bulk string 帧
  let slow_resp2 = slow_roundtrip(
    &rt,
    &mut c,
    b"*2\r\n$4\r\nINFO\r\n$14\r\nSTOREHASHTABLE\r\n",
  );
  assert!(
    slow_resp2.starts_with(b"$"),
    "慢路径 RESP2 应以 bulk string 开头"
  );

  // 3. 升级会话协议至 RESP3（会话协议版本置 3，对齐 RESP3 会话测试先例）
  c.session_mut().resp_protocol_version = 3;

  // 4. RESP3 会话：INFO server 返回 =<len>\r\ntxt:<info>\r\n verbatim 帧
  let out_resp3 = roundtrip(&mut c, b"*2\r\n$4\r\nINFO\r\n$6\r\nSERVER\r\n");
  assert!(
    out_resp3.starts_with(b"="),
    "RESP3 应以 verbatim string (=) 开头"
  );
  let crlf3 = out_resp3
    .windows(2)
    .position(|w| w == b"\r\n")
    .expect("CRLF required");
  let len_str3 = from_utf8(&out_resp3[1..crlf3]).expect("len utf8");
  let total_len3: usize = len_str3.parse().expect("valid length");
  assert_eq!(
    &out_resp3[crlf3 + 2..crlf3 + 6],
    b"txt:",
    "RESP3 verbatim 前缀必须为 txt:"
  );
  let text3 = &out_resp3[crlf3 + 6..out_resp3.len() - 2];
  assert_eq!(
    total_len3,
    4 + text3.len(),
    "RESP3 verbatim 长度必须包含 4 字节 txt: 元数据"
  );
  assert_eq!(text3, payload2, "RESP3 与 RESP2 的正文文本内容必须完全一致");

  // 5. 慢路径 RESP3 会话：INFO STOREHASHTABLE 同样以 = verbatim 帧出帧
  let slow_resp3 = slow_roundtrip(
    &rt,
    &mut c,
    b"*2\r\n$4\r\nINFO\r\n$14\r\nSTOREHASHTABLE\r\n",
  );
  assert!(
    slow_resp3.starts_with(b"="),
    "慢路径 RESP3 应以 verbatim string 开头"
  );
  let crlf_slow = slow_resp3
    .windows(2)
    .position(|w| w == b"\r\n")
    .expect("CRLF required");
  let len_slow: usize = from_utf8(&slow_resp3[1..crlf_slow])
    .unwrap()
    .parse()
    .unwrap();
  assert_eq!(&slow_resp3[crlf_slow + 2..crlf_slow + 6], b"txt:");
  let text_slow = &slow_resp3[crlf_slow + 6..slow_resp3.len() - 2];
  assert_eq!(len_slow, 4 + text_slow.len());
}
