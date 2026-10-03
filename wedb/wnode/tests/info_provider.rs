//! INFO 命令数据源集成测试（对标 libs/server/Resp/InfoProvider.cs）

use std::sync::Arc;

use wconf::RuntimeServerConfig;
use wmetric::{DbSnapshot, GarnetInfoMetrics, InfoProvider};
use wnode::{
  ClusterProvider,
  resp::{
    RespServerSession,
    info_provider::{InfoScanResult, InfoSlowSource, InfoSurface, SessionInfoSource, run_id},
  },
};
use wresp::metrics::{InfoMetricsType, MetricsItem};

/// 返回哨兵字段的集群提供方，用于验证 SessionInfoSource 四臂纯转调
struct FwdProvider;

impl ClusterProvider for FwdProvider {
  fn get_run_id(&self) -> String {
    "sentinel_run_id_40_chars_0123456789abcdef".to_string()
  }
  fn get_replication_info(&self) -> Vec<MetricsItem> {
    vec![MetricsItem::new("sentinel_repl", "RID")]
  }
  fn get_gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem> {
    vec![MetricsItem::new(
      "sentinel_gossip",
      metrics_disabled.to_string(),
    )]
  }
  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem> {
    vec![MetricsItem::new("sentinel_bp", "BPOOL")]
  }
  fn get_checkpoint_info(&self) -> Vec<MetricsItem> {
    vec![MetricsItem::new("sentinel_ckpt", "CKPT")]
  }
}

fn session_with_cluster() -> RespServerSession {
  let mut session = RespServerSession::default();
  session.attach_cluster_provider(Arc::new(FwdProvider));
  session
}

#[test]
fn test_run_id_cluster_enabled_dispatches_to_provider() {
  let mut session = session_with_cluster();
  session.set_runtime_config(Arc::new(RuntimeServerConfig::new(
    wconf::RuntimeServerOptions {
      enable_cluster: true,
      ..Default::default()
    },
  )));
  let src = SessionInfoSource::new(&session);
  let facts = src.server_facts();
  assert_eq!(facts.run_id, "sentinel_run_id_40_chars_0123456789abcdef");
}

#[test]
fn test_run_id_standalone_uses_process_run_id() {
  let session = RespServerSession::default();
  let src = SessionInfoSource::new(&session);
  let facts = src.server_facts();
  assert_eq!(facts.run_id, run_id());
  assert_eq!(facts.run_id.len(), 40);
}

#[test]
fn replication_forwards_to_provider() {
  let session = session_with_cluster();
  let src = SessionInfoSource::new(&session);
  let items = src.replication_info().expect("集群态应有复制段");
  assert!(
    items
      .iter()
      .any(|i| i.name.as_ref() == "sentinel_repl" && i.value == "RID")
  );
}

#[test]
fn gossip_threads_metrics_disabled() {
  let session = session_with_cluster();
  let src = SessionInfoSource::new(&session);
  assert!(src.gossip_stats(true).iter().any(|i| i.value == "true"));
  assert!(src.gossip_stats(false).iter().any(|i| i.value == "false"));
}

#[test]
fn buffer_pool_projects_metrics_item_to_tuples() {
  let session = session_with_cluster();
  let src = SessionInfoSource::new(&session);
  assert_eq!(
    src.buffer_pool_stats(),
    vec![("sentinel_bp".to_string(), "BPOOL".to_string())]
  );
}

#[test]
fn checkpoint_forwards_to_provider() {
  let session = session_with_cluster();
  let src = SessionInfoSource::new(&session);
  let ck = src.checkpoint_info().expect("集群态应有 checkpoint 段");
  assert!(
    ck.iter()
      .any(|i| i.name.as_ref() == "sentinel_ckpt" && i.value == "CKPT")
  );
}

#[test]
fn standalone_session_has_no_cluster_facet() {
  let session = RespServerSession::default();
  let src = SessionInfoSource::new(&session);
  assert!(src.replication_info().is_none());
  assert!(src.checkpoint_info().is_none());
  assert!(src.gossip_stats(false).is_empty());
  assert!(src.buffer_pool_stats().is_empty());
  // 裸会话无监听层池句柄：socket 行走 trait 缺省空表，与 C# 无 server 态同
  assert!(src.server_socket_buffer_pool_stats().is_empty());
}

/// 组合源 BPSTATS 空表打头不炸（工单 wnode-bpstats-server-socket-lines-missing
/// 验证点 c：无池无集群形态段仅剩段头，渲染闭环无 panic）
#[test]
fn slow_source_bpstats_empty_table_renders_bare_header() {
  let session = RespServerSession::default();
  let surface = InfoSurface::capture(
    &SessionInfoSource::new(&session),
    &[InfoMetricsType::BpStats, InfoMetricsType::Keyspace],
  );
  let src = InfoSlowSource::new(surface, InfoScanResult::default());
  let mut info = GarnetInfoMetrics::new();
  let text = info.get_resp_info(&[InfoMetricsType::BpStats], 0, &src);
  assert_eq!(text, "# BufferPoolStats\r\n");
}

/// 组合源行表合并：物理行 + 扫描虚库行（工单 zcode-r126c-infosec1 案一：
/// 混合段请求 STORE 段读物理事实、KEYSPACE 段枚举虚库号，最小扫描
/// 事实不外溢——虚库行经 virtual_db 标记被 wmetric 物理事实消费点跳过）
#[test]
fn slow_source_merges_virtual_keyspace_rows() {
  let session = RespServerSession::default();
  let sections = [InfoMetricsType::Store, InfoMetricsType::Keyspace];
  let surface = InfoSurface::capture(&SessionInfoSource::new(&session), &sections);
  // 裸会话无执行域：面快照空行 → 纯扫描形态（与旧扫描臂字节一致）
  let scan = InfoScanResult {
    keyspace: vec![(0, 3, 1), (1, 2, 0)],
    ..Default::default()
  };
  let src = InfoSlowSource::new(surface, scan);
  let rows = src.databases();
  assert_eq!(rows.iter().map(|r| r.id).collect::<Vec<_>>(), vec![0, 1]);
  // 纯扫描形态行表：无物理事实持有者，恒虚库标记
  assert!(rows.iter().all(|r| r.virtual_db));
  assert_eq!(src.keyspace_stats(1), (2, 0));
  // 物理行在场时虚库号以 virtual_db 行补齐并保持升序
  let mut surface = InfoSurface::capture(&SessionInfoSource::new(&session), &sections);
  surface.databases = vec![DbSnapshot {
    id: 0,
    system_state: "Running".to_string(),
    ..Default::default()
  }];
  let scan = InfoScanResult {
    keyspace: vec![(0, 3, 1), (2, 2, 0)],
    ..Default::default()
  };
  let src = InfoSlowSource::new(surface, scan);
  let rows = src.databases();
  // 连续号段 0..=max 补齐（与纯扫描生成形态同口径）：扫描未报的间隙号
  // 亦出虚库行，key_count==0 在 KEYSACE 渲染侧自然丢行
  assert_eq!(rows.iter().map(|r| r.id).collect::<Vec<_>>(), vec![0, 1, 2]);
  assert!(!rows[0].virtual_db && rows[0].system_state == "Running");
  assert!(rows[1].virtual_db && rows[2].virtual_db);
  let mut info = GarnetInfoMetrics::new();
  let text = info.get_resp_info(&sections, 0, &src);
  // STORE 段仅物理行填事实（虚库行不触发第二行零值事实）
  assert!(text.contains("SystemState:Running"));
  assert_eq!(text.matches("SystemState").count(), 1);
  assert!(text.contains("db0:keys=3,expires=1,avg_ttl=0"));
  assert!(text.contains("db2:keys=2,expires=0,avg_ttl=0"));
  assert!(!text.contains("db1:"), "零键间隙行须被渲染侧丢行: {text}");
}

/// 会话侧回归纯转调后，集群复制字段名字面量一处定义只留在集群层
#[test]
fn wnode_production_has_no_handbuilt_replication_literals() {
  let src = include_str!("../src/resp/info_provider.rs");
  let production = src.split("#[cfg(test)]").next().unwrap_or(src);
  for name in [
    "master_replid",
    "second_repl_offset",
    "sync_driver_count",
    "master_failover_state",
  ] {
    assert!(
      !production.contains(name),
      "会话侧不应再手工拼装复制字段字面量 {name}"
    );
  }
  // 五臂转调句柄同名方法，生产调用点存在
  for fwd in [
    "get_run_id",
    "get_replication_info",
    "get_gossip_stats",
    "get_buffer_pool_stats",
    "get_checkpoint_info",
  ] {
    assert!(production.contains(fwd), "会话侧应转调 {fwd}");
  }
}
