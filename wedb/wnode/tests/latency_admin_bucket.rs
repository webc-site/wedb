//! 慢命令批延迟 NET_RS_LAT_ADMIN 分桶集成测试（zcode-r31-slowlat 立项 2 验收）
//!
//! C# ProcessOtherCommands（RespServerSession.cs:1063-1065）与
//! ProcessAdminCommands（AdminCommands.cs:31）入口统一置位 containsSlowCommand，
//! 批出口按该标记切桶（:589-592）：慢命令批记 NET_RS_LAT_ADMIN，NET_RS_LAT
//! 只承载 fast 命令样本（ProcessBasicCommands 头注 ：815：慢命令混入 NET_RS
//! 会破坏延迟跟踪）。
//!
//! rust 对位：`process_other_commands` 会话臂伞单点置位 + `dispatch_slow` 入口
//! 置位（存储域 slow 段）+ AUTH/HELLO/ACL 预筛停车置位。验收：
//! 1. 会话侧闭环慢命令（LATENCY RESET / SLOWLOG LEN / SLOWLOG GET / DEBUG
//!    JMAP / ECHO / TIME / CLIENT ID / EVAL / ROLE）逐批各记一条
//!    NET_RS_LAT_ADMIN 样本，NET_RS_LAT 桶零混入；
//! 2. fast 命令（PING / SET / GET）批延迟只入 NET_RS_LAT，ADMIN 桶零增长。

use std::{mem::take, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wmetric::LatencyMetricsType;
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};

/// 小容量单文件存储 + 挂接执行域的会话（`latency_monitor` 即 C# 延迟监视门控）
fn open_env(
  tag: &str,
  latency_monitor: bool,
) -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  RespServerSession,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      latency_monitor,
      ..Default::default()
    },
  );
  s.set_garnet_api(api.clone());
  (Runtime::new().unwrap(), api, store, s, dir)
}

/// 喂入 RESP 帧并驱动单批消费（批首 latency_batch_start、批尾
/// latency_batch_stop 随 [`RespServerSession::try_consume_messages`] 闭环）
fn feed(s: &mut RespServerSession, frame: &[u8]) {
  s.recv_buffer.extend_from_slice(frame);
  s.bytes_read = s.recv_buffer.len();
  s.read_head = 0;
  s.end_read_head = 0;
  s.output.clear();
  s.try_consume_messages();
}

/// 会话双槽直方图样本总数（不装全局监视器、迭代时钟不推进，样本恒驻
/// 会话槽，直读即为批出口记账）
fn hist_count(s: &RespServerSession, latency_type: LatencyMetricsType) -> u64 {
  let entry = &s.get_latency_metrics().unwrap().metrics[latency_type.idx()];
  entry.latency[0].len() + entry.latency[1].len()
}

/// 会话侧闭环慢命令逐批各记一条 ADMIN 样本，NET_RS_LAT 零混入
#[test]
fn session_slow_commands_land_in_admin_bucket() {
  let (_rt, _api, _store, mut s, _dir) = open_env("lat-admin.db", true);
  assert_eq!(hist_count(&s, LatencyMetricsType::NetRsLatAdmin), 0);
  assert_eq!(hist_count(&s, LatencyMetricsType::NetRsLat), 0);

  for (idx, frame) in [
    &b"*2\r\n$7\r\nLATENCY\r\n$5\r\nRESET\r\n"[..],
    b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n",
    b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n",
    b"*2\r\n$5\r\nDEBUG\r\n$4\r\nJMAP\r\n",
    b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n",
    b"*1\r\n$4\r\nTIME\r\n",
    b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n",
    // Lua 未启用：run_lua_command 会话臂报 LUA disabled 错误帧，置位照常
    b"*3\r\n$4\r\nEVAL\r\n$8\r\nreturn 1\r\n$1\r\n0\r\n",
    b"*1\r\n$4\r\nROLE\r\n",
  ]
  .into_iter()
  .enumerate()
  {
    feed(&mut s, frame);
    let expect = idx as u64 + 1;
    assert_eq!(
      hist_count(&s, LatencyMetricsType::NetRsLatAdmin),
      expect,
      "慢命令批必须记 NET_RS_LAT_ADMIN：{frame:?} 应答 {:?}",
      String::from_utf8_lossy(&s.output)
    );
    assert_eq!(
      hist_count(&s, LatencyMetricsType::NetRsLat),
      0,
      "NET_RS_LAT 不得混入慢命令样本：{frame:?}"
    );
  }
}

/// fast 命令批延迟只入 NET_RS_LAT，ADMIN 桶零增长
#[test]
fn fast_commands_stay_in_net_rs_bucket() {
  let (_rt, _api, _store, mut s, _dir) = open_env("lat-fast.db", true);

  feed(&mut s, b"*1\r\n$4\r\nPING\r\n");
  assert_eq!(take(&mut s.output), b"+PONG\r\n");
  assert_eq!(hist_count(&s, LatencyMetricsType::NetRsLat), 1);
  assert_eq!(hist_count(&s, LatencyMetricsType::NetRsLatAdmin), 0);

  feed(&mut s, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
  assert_eq!(take(&mut s.output), b"+OK\r\n");
  feed(&mut s, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
  assert_eq!(take(&mut s.output), b"$1\r\nv\r\n");
  assert_eq!(hist_count(&s, LatencyMetricsType::NetRsLat), 3);
  assert_eq!(hist_count(&s, LatencyMetricsType::NetRsLatAdmin), 0);
}

/// 关延迟监视的会话不建延迟表：慢命令照常闭环（分桶面整体短路，语义等同
/// C# latencyMetrics?. 空条件跳过）
#[test]
fn slow_commands_without_latency_monitor_do_not_panic() {
  let (_rt, _api, _store, mut s, _dir) = open_env("lat-admin-off.db", false);
  assert!(s.get_latency_metrics().is_none());
  feed(&mut s, b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n");
  assert_eq!(take(&mut s.output), b":0\r\n");
}
