//! 存储会话 pending 闭环延迟计时集成测试（PENDING_LAT 计时点验收）
//!
//! 对位 C#：`StorageSession.StartPendingMetrics` / `StopPendingMetrics`
//! （libs/server/Storage/Session/Metrics.cs）在每次 `CompletePendingWithOutputs`
//! 前后成对起停 PENDING_LAT（MainStore/AdvancedOps.cs 两个 GET_CompletePending
//! 重载）。rust 全部异步闭环收敛于 `StorageSession::with_pending_metrics`
//! 单一漏斗，本测试验收该漏斗真的起停表：
//! 1. 开延迟监视的会话上跑一次真正走磁盘候选降级（= pending）的读命令，
//!    会话延迟表 PENDING_LAT 槽出样本，并经监视器同款归并口在
//!    `LATENCY HISTOGRAM` 无参回显里不再是空项；
//! 2. 关延迟监视的会话不建延迟表，同一读命令应答逐字节一致（表不在位即
//!    零取时零分配，语义等同 C# `latencyMetrics?.` 短路）。

use std::{mem::take, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wmetric::{GarnetLatencyMetrics, LatencyMetricsType, RespLatencyCommands};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::command::RespCommand;

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
  // 执行域挂入会话即单点回挂延迟表（C# 构造 storageSession 时下传
  // LatencyMetrics 的对位时序）
  s.set_garnet_api(api.clone());
  (Runtime::new().unwrap(), api, store, s, dir)
}

/// 同步闭环执行（热键写入：无磁盘候选可装载，绝不经 pending 漏斗）
fn sync_exec(api: &GarnetApi, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) {
  s.output.clear();
  api.exec(s, cmd, args);
  assert!(
    s.pending_slow.is_none(),
    "热键 {cmd} 不该挂起慢路径： {:?}",
    String::from_utf8_lossy(&s.output)
  );
}

/// 冷键降级执行：驱动挂起体闭环（网络泵同款驱动方式）并返回应答字节
fn cold_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  assert!(
    s.output.is_empty(),
    "冷键 {cmd} 必须降级挂起而非同步应答：{:?}",
    String::from_utf8_lossy(&s.output)
  );
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("冷键 {cmd} 降级未挂起 SlowWait"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 会话延迟表 PENDING_LAT 槽当前版本的样本数（起停表的观测面）
fn pending_samples(s: &RespServerSession) -> u64 {
  let latency = s
    .get_latency_metrics()
    .expect("延迟监视开启时会话必建延迟表");
  let snapshot = latency.metrics_snapshot().expect("延迟表未释放");
  snapshot[LatencyMetricsType::PendingLat.idx()].latency[latency.version()].len()
}

/// 开延迟监视：磁盘候选降级读（真正 pending）在 PENDING_LAT 出样本，
/// 归并后 LATENCY HISTOGRAM 无参回显含 PENDING_LAT
#[test]
fn pending_read_records_pending_latency() {
  let (rt, api, store, mut s, _dir) = open_env("pending-lat.db", true);

  sync_exec(&api, &mut s, RespCommand::Hset, &[b"h", b"f1", b"v1"]);
  assert_eq!(take(&mut s.output), b":1\r\n");
  assert_eq!(
    pending_samples(&s),
    0,
    "同步闭环（无 pending）不得记 PENDING_LAT"
  );

  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Hget, &[b"h", b"f1"]),
    b"$2\r\nv1\r\n"
  );

  let samples = pending_samples(&s);
  assert!(
    samples >= 1,
    "磁盘候选降级读未经 with_pending_metrics 起停 PENDING_LAT（样本数 {samples}）"
  );

  // 监视器同款归并（同一版本缓冲）后走真实命令面回显
  let latency = s.get_latency_metrics().unwrap();
  let mut global = GarnetLatencyMetrics::new(GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES);
  global.merge_session_snapshot(
    &latency.metrics_snapshot().expect("延迟表未释放"),
    latency.version(),
  );
  let mut out = Vec::new();
  RespLatencyCommands::network_latency_histogram(&[], Some(&global), &mut out);
  let echo = String::from_utf8_lossy(&out).into_owned();
  assert!(
    echo.contains("PENDING_LAT"),
    "LATENCY HISTOGRAM 回显缺 PENDING_LAT：{echo:?}"
  );
  // pending 计时只落本类别，不得串入网络接收桶
  assert!(
    !echo.contains("NET_RS_LAT"),
    "PENDING_LAT 计时串扰网络桶：{echo:?}"
  );
}

/// 关延迟监视：会话不建延迟表，同一降级读应答逐字节一致（零起停表形态）
#[test]
fn pending_read_without_latency_monitor_is_unchanged() {
  let (rt, api, store, mut s, _dir) = open_env("pending-lat-off.db", false);
  assert!(
    s.get_latency_metrics().is_none(),
    "延迟监视关闭时会话不应持有延迟表"
  );

  sync_exec(&api, &mut s, RespCommand::Hset, &[b"h", b"f1", b"v1"]);
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Hget, &[b"h", b"f1"]),
    b"$2\r\nv1\r\n"
  );
}
