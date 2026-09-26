//! 无盘全量同步纪元排空等待超时判败收口回归测试
//!
//! 背景（rust 自有机制，C# 无对位）：rust 快照源为活存储 live scan，扫描
//! 键门窗口不变量「记录效果进快照 ⟺ 记录地址 ≤ 锚」的唯一承重件是 Blocking
//! 相末的纪元静止排空等待——C# 无限自旋必达，rust 以 cluster_node_timeout
//! 有界化后超时返 false。false 若被忽略，锚照样起算，排空未达成即仍有批内
//! 在途写会话，其后续提交记录地址恒 > 锚、效果却可被读值折叠进快照，
//! ObjectStoreRMW 非幂等记录（HINCRBY/LPUSH 类）被快照与 AOF 续推双重应用，
//! 静默永久主从发散。本测试锁定返值承判收口：排空未达成即整轮判败——
//! - 驱动器上抛 Err（文案含排空未达成），批内会话收敛 FAILED（不静默放行）；
//! - 键门经守卫 Drop 整门注销：判败后主端真 RESP 写路径不挂起；
//! - 零扇出帧发出：副本不持有主端预置键（半快照副本绝不放行转入增量）；
//! - 批量窗随清册关闭：判败后可再入册下一批（副本沿节流重连整备重跑）。
//!
//! 不追平夹具：注册会话先行批首纪元快照（[`ClusterSessionFace::acquire_current_epoch`]
//! 即 RESP 批入口同款调用面），随后纪元再推进——该会话快照恒落后，静止等待
//! 必超时；cluster_node_timeout 设极小值使超时即刻达。
//!
//! 反证基线（revert-proof）：排空返值改回忽略即本轮同步照常成功——Err 断言、
//! FAILED 断言与副本零键断言同时变红。

mod common;
#[path = "common/primary_assets.rs"]
mod primary_assets_core;
use primary_assets_core::primary_assets;

#[path = "common/cluster_consumer_store.rs"]
mod cluster_cc_store;

use std::{num::NonZeroUsize, str::from_utf8, sync::Arc};

use cluster_cc_store::cluster_consumer;
use common::{open_node, provider_with_role, replica_host};
use waof::AofAddress;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  replication::{
    cluster_replication_session::ClusterReplicationSession,
    diskless_replication::sync_status::SyncStatus, recovery_status::RecoveryStatus,
    replica_replay_task::ReplayAssets, sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};
use wkv::WedbStore;
use wnode::{
  ClusterSessionFace, MessageConsumerFace, RespSessionConsumer,
  aof::waof_sublog::single_log_aof,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  service::NodeService,
  storage::session::storage_session::StorageSession,
};

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00B1;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00B2;

/// 纪元静止等待超时上限毫秒（夹具会话恒不追平，超时必达且即刻判败）
const DRAIN_TIMEOUT_MS: u64 = 50;

/// 主端预置字符串键（判败后副本读取必为 nil——零扇出帧直证）
const FILLER_KEY: &[u8] = b"drain:filler";
/// 门注销探测键（真 RESP HINCRBY 写路径：门在场即挂起空应答）
const PROBE_KEY: &[u8] = b"drain:probe";
const FIELD: &[u8] = b"fld";

/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = consumer.try_consume_messages_into(&mut resp);
  resp
}

/// RESP 数组命令组帧
fn resp_command(parts: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", parts.len()).into_bytes();
  for part in parts {
    out.extend(format!("${}\r\n", part.len()).into_bytes());
    out.extend_from_slice(part);
    out.extend(b"\r\n");
  }
  out
}

/// RESP GET 读值（None = 键不在场）
fn get_value(consumer: &mut RespSessionConsumer, key: &[u8]) -> Option<Vec<u8>> {
  let frame = resp_command(&[b"GET", key]);
  let out = pump(consumer, &frame);
  let text = from_utf8(&out).unwrap();
  if text.starts_with("$-1") {
    return None;
  }
  let mut parts = text.split("\r\n");
  let len: usize = parts
    .next()
    .and_then(|head| head.strip_prefix('$'))
    .and_then(|n| n.parse().ok())
    .unwrap_or_else(|| panic!("GET 应答帧异常: {text:?}"));
  let body = parts.next().unwrap_or("");
  Some(body.as_bytes()[..len].to_vec())
}

/// 主端预置字符串键（存储直写经事件汇入账 AOF，与写窗 sibling 用例同形）
async fn seed_filler(store: &Arc<WedbStore<SegmentedDevice>>) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  StorageSession::new_readonly(batch)
    .upsert_string(FILLER_KEY, b"v".as_slice())
    .await
    .unwrap();
}

/// 纪元排空超时判败收口：夹具会话恒不追平 → 驱动器 Err、会话 FAILED、
/// 门已注销、副本零扇出帧、批量窗关闭可入册下一批
#[compio::test]
async fn diskless_epoch_drain_timeout_fails_closed() {
  // ===== 主端装配（AOF 门面 + 存储事件汇 + 预置键）
  let source = open_node("drain_source");
  let provider_p = provider_with_role(
    &source,
    PRIMARY_ID,
    7110,
    NodeRole::Primary,
    PRIMARY_ID,
    true,
    Some(0),
  );
  let aof_options = RuntimeServerOptions::default();
  let primary_aof =
    single_log_aof(Arc::clone(&source.wal), &aof_options).expect("装配主端 single_log_aof");
  let _service =
    NodeService::new(Arc::clone(&source.store), primary_aof).expect("注册存储 AOF 事件汇");
  seed_filler(&source.store).await;
  let mut seeder = cluster_consumer(&provider_p, &source.store);
  let out = pump(
    &mut seeder,
    &resp_command(&[b"HSET", PROBE_KEY, FIELD, b"1"]),
  );
  assert_eq!(out, b":1\r\n", "HSET 预置应答异常");

  // ===== 副本装配（接收面 + 宿主服务器监听）
  let replica = open_node("drain_replica");
  let provider_r = provider_with_role(
    &replica,
    REPLICA_ID,
    7111,
    NodeRole::Replica,
    PRIMARY_ID,
    true,
    Some(0),
  );
  let rm_r = provider_r.replication_manager().unwrap();
  let replica_aof =
    single_log_aof(Arc::clone(&replica.wal), &aof_options).expect("装配副本 single_log_aof");
  rm_r.set_replay_assets(Some(Arc::new(ReplayAssets::new(
    replica_aof,
    Arc::clone(&replica.store),
    None,
    None,
  ))));
  assert!(
    rm_r.begin_recovery(RecoveryStatus::ReadRole, false),
    "副本恢复门控就位"
  );
  provider_r.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
    Arc::clone(&provider_r),
    Arc::clone(&replica.wal),
    None,
  ))));
  let (server, replica_addr) = replica_host(&provider_r, NonZeroUsize::new(1));
  let mut replica_reader = RespSessionConsumer::new(
    2,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(replica.store.new_session().unwrap())),
  );

  // ===== 不追平夹具：注册会话先行批首纪元快照（RESP 批入口同款调用面），
  //     随后纪元再推进——静止等待判定 entry_epoch < current_epoch 恒成立，
  //     超时上限即刻达
  provider_p.bump_current_epoch();
  let lag = provider_p.create_cluster_session();
  lag.acquire_current_epoch();
  assert_eq!(lag.local_current_epoch(), provider_p.current_epoch());
  provider_p.set_cluster_node_timeout_ms(DRAIN_TIMEOUT_MS);

  // ===== 直驱 leader 会话驱动（持会话柄以断言批内收敛态）
  let rm_p = provider_p.replication_manager().unwrap();
  let assets = primary_assets(&source, &rm_p);
  let meta = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: REPLICA_ID,
    current_primary_repl_id: rm_r.primary_repl_id(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 0),
    current_aof_tail_address: AofAddress::create(1, 0),
    current_replication_offset: AofAddress::create(1, 0),
    checkpoint_entry: None,
  };
  let manager = Arc::clone(&rm_p.replication_sync_manager);
  let session = manager
    .add_replica_sync_session(
      replica_addr.clone(),
      meta.clone(),
      rm_p.sublog_count(),
      Arc::clone(&rm_p.aof_sync_driver_store),
    )
    .expect("首批入册");
  let err = manager
    .replication_sync_driver(&session, &provider_p, &rm_p, &assets, PRIMARY_ID)
    .await
    .expect_err("纪元排空未达成必须整轮判败，不得带病续传");
  assert!(
    err.contains("epoch drain not settled"),
    "判败文案须指认排空未达成: {err}"
  );

  // ===== 批内会话收敛 FAILED（Err 收敛臂全员判败，非静默放行）
  let info = session.status_info();
  assert_eq!(info.sync_status, SyncStatus::Failed);
  assert!(
    info
      .error
      .as_deref()
      .is_some_and(|e| e.contains("epoch drain not settled")),
    "会话错误文案须同源: {:?}",
    info.error
  );

  // ===== 零扇出帧发出：判败早于任何快照帧/映射帧，副本不持有主端预置键
  assert_eq!(get_value(&mut replica_reader, FILLER_KEY), None);

  // ===== 键门经守卫 Drop 整门注销：主端真 RESP 写路径即刻应答不挂起
  let mut probe = cluster_consumer(&provider_p, &source.store);
  let out = pump(
    &mut probe,
    &resp_command(&[b"HINCRBY", PROBE_KEY, FIELD, b"1"]),
  );
  assert!(!out.is_empty(), "判败后写命令不得滞留键门（整门须已注销）");
  assert!(
    from_utf8(&out).unwrap().starts_with(':'),
    "HINCRBY 应得整数应答: {:?}",
    from_utf8(&out).unwrap()
  );

  // ===== 批量窗随清册关闭：可再入册下一批（副本沿节流重连整备重跑形态）
  let mut next_meta = meta;
  next_meta.origin_node_id = REPLICA_ID + 1;
  let next = manager
    .add_replica_sync_session(
      replica_addr,
      next_meta,
      rm_p.sublog_count(),
      Arc::clone(&rm_p.aof_sync_driver_store),
    )
    .expect("判败收口后批量窗须已关，允许下一批入册");
  assert_eq!(next.status_info().sync_status, SyncStatus::Initializing);

  // 夹具会话落账释放（判败收口不残留注册面泄漏：夹具 drop 后静止等待
  // 即恢复正常放行——死弱引用枚举时自清扫）
  drop(lag);
  provider_p.set_cluster_node_timeout_ms(2_000);
  assert!(
    provider_p.bump_and_wait_for_epoch_transition_async().await,
    "夹具释放后排空等待须恢复放行（注册面无残留锁死静止判定）"
  );

  server.dispose();
}
