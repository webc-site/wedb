use wconf::RuntimeServerConfig;
use wnode::resp::config_commands::ServerConfig;
use wnode::resp::resp_server_session::RespServerSession;
use wresp::{ArgSlice, RespCommand};

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:PingTest
#[test]
fn ping_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_ping(&[], &mut out).unwrap();
  assert_eq!(out, b"+PONG\r\n");
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:PingMessageTest
#[test]
fn ping_message_test() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_ping(&[b"HELLO"], &mut out).unwrap();
  assert_eq!(out, b"$5\r\nHELLO\r\n");
}

/// test/standalone/Garnet.test/RespAdminCommandsTests.cs:PingErrorMessageTest
#[test]
fn ping_error_message_test() {
  let mut s = RespServerSession::default();
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
  let mut s = RespServerSession::default();
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
  let mut s = RespServerSession::default();
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
  let mut s = RespServerSession::default();
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
  s.parse_state.root_buffer.push(ArgSlice::new(b"X".as_ptr(), 1));
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
  s.network_config_get(&[b"slave-read-only"], &rc, &mut out)
    .unwrap();
  assert_eq!(out, b"*2\r\n$15\r\nslave-read-only\r\n$3\r\nyes\r\n");
}

/// libs/server/Resp/AdminCommands.cs:NetworkHCOLLECT/NetworkZCOLLECT 集成测试
mod admin_collect {
  use std::{sync::Arc, thread, time::Duration};

  use compio::runtime::Runtime;
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};
  use wnode::{
    MessageConsumerFace, RespSessionConsumer,
    resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  };

  fn with_session(f: impl FnOnce(&mut RespSessionConsumer)) {
    Runtime::new().unwrap().block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("collect.db")).unwrap());
      let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let api = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
      let mut consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api);
      f(&mut consumer);
    });
  }

  fn frame(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
      out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
    }
    out
  }

  fn feed(consumer: &mut RespSessionConsumer, args: &[&str]) -> Vec<u8> {
    let (consumed, resp) = consumer.try_consume_messages(&frame(args));
    assert!(consumed > 0);
    resp
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
      assert_eq!(
        resp,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
      );

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
      assert_eq!(
        resp,
        b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
      );
    });
  }
}

// ---------------------------------------------------------------------------
// SAVE / BGSAVE / LASTSAVE / COMMITAOF 接线回归
//（C# NetworkSAVE → storeWrapper.TakeCheckpointAsync；NetworkCOMMITAOF →
// CommitAOFAsync 后恒回 "AOF file committed"）
// ---------------------------------------------------------------------------

use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{CheckpointCtx, StoreGarnetApi},
    resp_server_session::{ConnectionProtectionOption, RespServerSessionOptions as Opts},
  },
};

/// 挂检查点通道的会话消费者（独立临时目录）
fn checkpoint_consumer() -> (
  RespSessionConsumer,
  std::path::PathBuf,
  Arc<std::sync::atomic::AtomicI64>,
) {
  checkpoint_consumer_with(|opts| opts)
}

/// 同 [`Self::checkpoint_consumer`]，允许定制会话选项
fn checkpoint_consumer_with(
  customize: impl FnOnce(Opts) -> Opts,
) -> (
  RespSessionConsumer,
  std::path::PathBuf,
  Arc<std::sync::atomic::AtomicI64>,
) {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("admin.db")).unwrap());
  let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let cp_dir = dir.join("Store").join("checkpoints");
  let last_save_ms = Arc::new(std::sync::atomic::AtomicI64::new(0));
  let api = StoreGarnetApi::new(store.new_session().unwrap()).with_checkpoint_ctx(CheckpointCtx {
    dir: cp_dir.clone(),
    last_save_ms: Arc::clone(&last_save_ms),
  });
  (
    RespSessionConsumer::new(1, customize(Opts::default()), Arc::new(api)),
    cp_dir,
    last_save_ms,
  )
}

/// 单命令往返（同步快路径）
fn roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = c.try_consume_messages(frame);
  assert_eq!(consumed, frame.len(), "帧应被完整消费: {frame:?}");
  out
}

/// 慢命令往返（网络泵角色由 block_on 承担）
fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = c.try_consume_messages(frame);
  assert_eq!(consumed, frame.len(), "帧应被完整消费: {frame:?}");
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// COMMITAOF → "AOF file committed"（C# 无视提交结果恒回该文案）
#[test]
fn commitaof_replies_fixed_text() {
  let mut s = RespServerSession::default();
  let mut out = Vec::new();
  let _ = s.network_commitaof(&[], &mut out).unwrap();
  assert_eq!(out, b"+AOF file committed\r\n");

  let mut out = Vec::new();
  let _ = s.network_commitaof(&[b"0"], &mut out).unwrap();
  assert_eq!(out, b"+AOF file committed\r\n");
}

/// SAVE / BGSAVE / LASTSAVE：检查点通道闭环 + LASTSAVE 时间戳推进
#[test]
fn save_bgsave_lastsave_via_checkpoint_channel() {
  let rt = Runtime::new().unwrap();
  let (mut c, cp_dir, last_save_ms) = checkpoint_consumer();

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
  let dir_ok = std::fs::read_dir(&cp_dir)
    .map(|entries| entries.count() > 0)
    .unwrap_or(false);
  assert!(dir_ok, "检查点目录应生成快照条目: {}", cp_dir.display());
  let after_save_ms = last_save_ms.load(std::sync::atomic::Ordering::Acquire);
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
//（C# PurgeBPCommand：clusterSession == null → CLUSTER_DISABLED，成功回
// "GC completed for <type>"；NetworkEXPDELSCAN → storeWrapper.
// ExpiredKeyDeletionScan，应答 *2 计数对）
// ---------------------------------------------------------------------------

/// DEBUG PURGEBP：单机无集群切面 → CLUSTER_DISABLED；ManagerType 解析
/// 失败 → 语法错误
#[test]
fn purgebp_without_cluster_disabled() {
  let (mut c, _dir, _ts) = checkpoint_consumer_with(|opts| Opts {
    enable_debug_command: ConnectionProtectionOption::Yes,
    ..opts
  });

  let out = roundtrip(&mut c, b"*3\r\n$5\r\nDEBUG\r\n$7\r\nPURGEBP\r\n$16\r\nMigrationManager\r\n");
  assert_eq!(
    out,
    b"-ERR This instance has cluster support disabled\r\n"
  );

  let out = roundtrip(&mut c, b"*3\r\n$5\r\nDEBUG\r\n$7\r\nPURGEBP\r\n$4\r\nnope\r\n");
  assert_eq!(out, b"-ERR syntax error\r\n");
}

/// EXPDELSCAN：空库慢路径闭环，应答 *2 计数对（expired=0 scanned=0）
#[test]
fn expdelscan_reports_zero_counts_on_empty_store() {
  let rt = Runtime::new().unwrap();
  let (mut c, _dir, _ts) = checkpoint_consumer();

  // 非法 DBID → 快路径拦截（C# TryParseDatabaseId 口径）
  let out = roundtrip(&mut c, b"*2\r\n$10\r\nEXPDELSCAN\r\n$3\r\nabc\r\n");
  assert_eq!(
    out,
    b"-ERR value is not an integer or out of range.\r\n"
  );

  // 无参 → 慢路径默认库 0：*2 + 两个十进制计数 bulk string
  let out = slow_roundtrip(&rt, &mut c, b"*1\r\n$10\r\nEXPDELSCAN\r\n");
  assert_eq!(out, b"*2\r\n$1\r\n0\r\n$1\r\n0\r\n");
}
