//! 活跃消费者注册表连接计数与 CLIENT 行/监视器采样面集成测
//!
//! r329 自 `wnode/src/servers/consumer_registry.rs` 内联测模块迁入（纯 pub API 面）；
//! 触 pub(crate)/私有面的测（is_terminating/wait_terminate、active_handler_count
//! 直读 helper、dispose 系）留守内联。

use wnode::{
  servers::{ClientView, ConsumerRegistry},
  session_parse_state_extensions::ClientType,
};

/// 注册/注销闭环与连接计数（C# TotalConnectionsReceived/Disposed 语义；
/// received 计数点已前移到 accept 成功分支，register 本身不再计数）
#[test]
fn register_unregister_cycles_counters() {
  let registry = ConsumerRegistry::new();
  registry.note_connection_received();
  let entry = registry.register(1, "127.0.0.1:7000".into(), "127.0.0.1:6379".into());
  assert_eq!(registry.connection_totals(), (1, 0, 1));
  assert_eq!(entry.id, 1);
  assert_eq!(entry.remote_endpoint, "127.0.0.1:7000");
  assert_eq!(registry.get(1).map(|e| e.id), Some(1));

  registry.unregister(1);
  assert_eq!(registry.connection_totals(), (1, 1, 0));
  assert!(registry.get(1).is_none());
  // 重复注销为无害空操作
  registry.unregister(1);
  assert_eq!(registry.connection_totals(), (1, 1, 0));
}

/// received - disposed = 活跃条目数 不变量在计数点前移后的短命连接配对
///（容量门拒绝臂：received 已计、无条目收场，disposed 直计配对）
#[test]
fn rejected_connection_notes_paired_dispose() {
  let registry = ConsumerRegistry::new();
  registry.note_connection_received();
  registry.note_connection_disposed();
  // 无条目、无在途：received/disposed 配对，活跃为零
  assert_eq!(registry.connection_totals(), (1, 1, 0));
}

/// CLIENT INFO 行字段序逐项对齐 C# WriteClientInfo(零丢弃形态即
/// C# 保形面:rust 自有 pubsub-drop 仅非零显影,见显影专测)
#[test]
fn client_info_line_matches_csharp_field_order() {
  let registry = ConsumerRegistry::new();
  let entry = registry.register(9, "127.0.0.1:40000".into(), "127.0.0.1:6379".into());
  entry.update_view(ClientView {
    name: Some("tester".into()),
    user: Some("default".into()),
    lib_name: Some("lib".into()),
    lib_ver: Some("1.0".into()),
    db: 2,
    resp: 3,
    pubsub_dropped: 0,
    client_type: ClientType::Pubsub,
  });

  let mut line = String::new();
  entry.write_client_info(&mut line, entry.creation_ticks + 5_000);
  assert_eq!(
    line,
    "id=9 addr=127.0.0.1:40000 laddr=127.0.0.1:6379 name=tester age=5 \
     user=default flags=P db=2 resp=3 lib-name=lib lib-ver=1.0"
  );
  // 零丢弃不显影(C# 字段序逐位保形)
  assert!(!line.contains("pubsub-drop"));
}

/// pubsub-drop 显影专测(rust 自有观测面,C# 无对位字段):非零
/// 丢弃量行尾显影,值随投影同轨重导刷新
#[test]
fn client_info_line_renders_pubsub_drop_when_dropped() {
  let registry = ConsumerRegistry::new();
  let entry = registry.register(10, "127.0.0.1:40001".into(), "127.0.0.1:6379".into());
  entry.update_view(ClientView {
    name: Some("slow".into()),
    user: Some("default".into()),
    lib_name: Some(String::new()),
    lib_ver: Some(String::new()),
    db: 0,
    resp: 2,
    pubsub_dropped: 42,
    client_type: ClientType::Pubsub,
  });

  let mut line = String::new();
  entry.write_client_info(&mut line, entry.creation_ticks + 5_000);
  assert_eq!(
    line,
    "id=10 addr=127.0.0.1:40001 laddr=127.0.0.1:6379 name=slow age=5 \
     user=default flags=P db=0 resp=2 lib-name= lib-ver= pubsub-drop=42"
  );

  // 投影重导收敛于同一发布轨:计数推进随下批整体刷新
  entry.update_view(ClientView {
    pubsub_dropped: 43,
    ..ClientView::default()
  });
  line.clear();
  entry.write_client_info(&mut line, entry.creation_ticks + 6_000);
  assert!(line.ends_with(" pubsub-drop=43"));
}

/// 监视器快照承接连接计数与网络字节镜像
#[test]
fn monitor_sample_carries_counters_and_bytes() {
  let registry = ConsumerRegistry::new();
  registry.note_connection_received();
  let entry = registry.register(3, "127.0.0.1:7002".into(), "127.0.0.1:6379".into());
  entry.add_net_bytes(128, 64);

  let sample = registry.monitor_sample();
  assert_eq!(sample.total_connections_received, 1);
  assert_eq!(sample.total_connections_disposed, 0);
  assert_eq!(sample.total_connections_active, 1);
  assert_eq!(sample.sessions.len(), 1);
  assert_eq!(sample.sessions[0].metrics.get_total_net_input_bytes(), 128);
  assert_eq!(sample.sessions[0].metrics.get_total_net_output_bytes(), 64);
}

/// INFO RESET STATS 连接计数复位（C# ResetConnectionsReceived 语义）
#[test]
fn reset_totals_keeps_active() {
  let registry = ConsumerRegistry::new();
  registry.register(1, "a".into(), String::new());
  registry.register(2, "b".into(), String::new());
  registry.unregister(1);
  registry.reset_connection_totals();
  assert_eq!(registry.connection_totals(), (1, 0, 1));
}
