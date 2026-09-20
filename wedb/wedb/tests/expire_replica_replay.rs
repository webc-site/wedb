//! 条件 EXPIRE（NX/XX/GT/LT）+ 副本全量+增量同步的 TTL 口径锁定回归
//!
//! 锁定 aof_processor_store_ops.rs:store_rmw Pexpireat 臂的刻意差异声明：
//! C# EXPIRE 族条件经 ExpirationWithOption 低 4 位随 RMW 条目全量携带、副本端
//! 重评估（UnifiedStore/PrivateMethods.cs）；rust 条目只携带主端已线性化裁决的
//! 绝对毫秒（TtlWrite 镜像单点，service.rs:on_aof_store_event），重放按无条件
//! 绝对过期执行（TtlOpt::NONE）。该分叉的一致性前提是「条目存在 ⇔ 主端已按
//! 条件裁决并真实改写 TTL 记录」：本测试在主端真 RESP 路径泵一批 NX/XX/GT/LT
//! 组合（含成与拒两类裁决、无 TTL/缺失/覆盖写/过去时间戳边界），经无盘全量
//! 快照 + AOF 增量推流回放副本后，逐键断言副本 PEXPIRETIME 与主端毫秒精确
//! 相等、被拒条件绝不令副本凭空出现/丢失 TTL。
//!
//! 对标 C# 测试面：libs 集成侧 EXPIRE 选项族随 AOF/复制传播的镜像一致性
//! （C# 靠条目携带 option 副本重评估达成；rust 靠主端裁决单点达成，殊途同归，
//! 本用例即该口径的回归栅栏）。

use std::{str::from_utf8, sync::Arc, time::Duration};

use compio::runtime::Runtime;
use waof::{AofAddress, WalConfig, WalLog};
use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
  hash_slot::{HashSlot, SlotState},
  replication::{
    aof_replication_pump::AofReplicationPump,
    cluster_replication_session::ClusterReplicationSession, recovery_status::RecoveryStatus,
    replica_diskless_sync::try_begin_diskless_sync_async, replica_replay_task::ReplayAssets,
    replica_sync_session::ReplicaSyncSession, sync_metadata::SyncMetadata,
  },
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole},
};
use wkv::WedbStore;
use wnode::{
  GarnetServer, MessageConsumerFace, RespSessionConsumer, SessionProviderFace, WireFormat,
  aof::waof_sublog::single_log_aof,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  service::NodeService,
};
use wtest_base::{resp_frame, test_store_config, wait_for};
use wtxn::{TxnLockTable, WatchVersionMap};

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00E1;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00E2;

/// NX：无 TTL 键上生效
const K_NXF: &[u8] = b"k:nxf";
/// NX：已有 TTL 键上被拒，保持基线
const K_NXE: &[u8] = b"k:nxe";
/// XX：无 TTL 键上被拒，副本绝不许凭空出现 TTL（本用例核心防误判面）
const K_XXM: &[u8] = b"k:xxm";
/// XX：已有 TTL 键上生效
const K_XXE: &[u8] = b"k:xxe";
/// GT：新值低于基线被拒
const K_GTL: &[u8] = b"k:gtl";
/// GT：新值高于基线生效
const K_GTH: &[u8] = b"k:gth";
/// LT：新值高于基线被拒
const K_LTH: &[u8] = b"k:lth";
/// LT：新值低于基线生效
const K_LTL: &[u8] = b"k:ltl";
/// XX+GT 双词：一拒一成一
const K_XGT: &[u8] = b"k:xgt";
/// XX+LT 双词：一拒一成一
const K_XLT: &[u8] = b"k:xlt";
/// 覆盖写边界：SET 覆写清 TTL（Persist 条目），双端回到无 TTL
const K_OVW: &[u8] = b"k:ovw";
/// 过去时间戳边界：EXPIREAT 已逝时刻即时清除（DELIFEXPIM/墓碑条目）
const K_PST: &[u8] = b"k:pst";
/// 缺失键边界：EXPIRE 主端 :0 零条目，副本保持缺失
const K_GHOST: &[u8] = b"k:ghost";

/// 独立存储节点（db 文件 + wal）
struct NodeStorage {
  _dir: tempfile::TempDir,
  store: Arc<WedbStore<SegmentedDevice>>,
  wal: Arc<WalLog<SegmentedDevice>>,
}

fn open_node(tag: &str) -> NodeStorage {
  let dir = tempfile::tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.db"))).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let wal_device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());
  NodeStorage {
    _dir: dir,
    store,
    wal,
  }
}

/// 带角色 provider（槽位图全量 Stable 本地，对齐宿主装配；与
/// diskless_sync_write_window 同款拓扑）
fn provider_with_role(
  node: &NodeStorage,
  node_id: u128,
  port: i32,
  role: NodeRole,
) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  provider.set_replica_diskless_sync_delay(0);
  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port,
    config_epoch: 1,
    role,
    replica_of_node_id: (role == NodeRole::Replica).then_some(PRIMARY_ID),
    hostname: None,
  });
  for s in 0..CLUSTER_SLOT_COUNT {
    config.slot_map[s] = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state: SlotState::Stable,
    };
  }
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);
  provider.set_store(Arc::clone(&node.store));
  provider.set_wal(Arc::clone(&node.wal));
  provider
}

/// 副本宿主会话装配面（每连接新集群会话——对齐宿主 get_session）
struct ReplicaSessionProvider {
  provider: Arc<ClusterProvider>,
}

impl SessionProviderFace for ReplicaSessionProvider {
  type Consumer = RespSessionConsumer;

  fn get_session(
    &self,
    _wire_format: WireFormat,
    _network_sender_id: u64,
  ) -> Option<RespSessionConsumer> {
    let cluster_session = self.provider.create_cluster_session();
    let store = self.provider.try_store()?;
    let mut consumer = RespSessionConsumer::with_cluster(
      1,
      RespServerSessionOptions::default(),
      cluster_session,
      self.provider.provider_handle(),
      Arc::new(StoreGarnetApi::new(store.new_session().ok()?)),
    );
    consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
    Some(consumer)
  }
}

/// 主端真 RESP 写会话（集群门评 → 存储执行域；存储事件汇已注册，
/// 写命令同步入账 AOF）
fn cluster_consumer(
  provider: &Arc<ClusterProvider>,
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> RespSessionConsumer {
  let cluster_session = provider.create_cluster_session();
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    provider.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}

/// 读侧会话（非集群直连执行域，主从共用）
fn reader(store: &Arc<WedbStore<SegmentedDevice>>, id: u64) -> RespSessionConsumer {
  RespSessionConsumer::new(
    id,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = consumer.try_consume_messages_into(&mut resp);
  resp
}

/// 泵入命令并锁定精确应答（应答偏差即主端裁决变化，第一时间现场失败）
fn exec(consumer: &mut RespSessionConsumer, parts: &[&[u8]], expected: &[u8]) {
  let out = pump(consumer, &resp_frame(parts));
  assert_eq!(
    out,
    expected,
    "命令 {:?} 应答异常: {:?}",
    parts
      .iter()
      .map(|p| from_utf8(p).unwrap())
      .collect::<Vec<_>>(),
    from_utf8(&out).unwrap_or("<binary>")
  );
}

/// 整数应答解析（PEXPIRETIME/EXISTS/EXPIRE 族皆 `:N\r\n`）
fn parse_int(reply: &[u8]) -> i64 {
  let text = from_utf8(reply).unwrap();
  text
    .strip_prefix(':')
    .unwrap_or_else(|| panic!("整数应答异常: {text:?}"))
    .trim_end()
    .parse::<i64>()
    .unwrap_or_else(|e| panic!("整数应答解析失败: {text:?} {e}"))
}

/// PEXPIRETIME 读绝对过期 Unix 毫秒（-1 无 TTL / -2 键缺失；逐毫秒口径即
/// 主从一致性断言的度量单位）
fn pexpiretime(consumer: &mut RespSessionConsumer, key: &[u8]) -> i64 {
  parse_int(&pump(consumer, &resp_frame(&[b"PEXPIRETIME", key])))
}

fn exists(consumer: &mut RespSessionConsumer, key: &[u8]) -> i64 {
  parse_int(&pump(consumer, &resp_frame(&[b"EXISTS", key])))
}

fn get_value(consumer: &mut RespSessionConsumer, key: &[u8]) -> Option<Vec<u8>> {
  let out = pump(consumer, &resp_frame(&[b"GET", key]));
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

/// 条件 EXPIRE + 副本全量+增量回放逐键 TTL 一致：主端 RESP 预置与基线 TTL →
/// 无盘全量同步（快照侧 TTL 保留先比对一轮）→ 同步收口后主端再执行 NX/XX/
/// GT/LT 组合批（全部走 AOF StoreRMW Pexpireat/Persist/DELIFEXPIM 条目增量
/// 回放）→ 副本位点追平后逐键断言副本 PEXPIRETIME 与主端毫秒精确相等，
/// 且各裁决语义（成/拒/清/删）在主端 RESP 应答上逐条锁定
#[test]
fn conditional_expire_replica_replay_locks_ttl() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 主端：AOF 门面 + 存储事件汇（RESP 写入账）+ 集群拓扑
    let source = open_node("cond_expire_source");
    let provider_p = provider_with_role(&source, PRIMARY_ID, 7000, NodeRole::Primary);
    let aof_options = RuntimeServerOptions::default();
    let primary_aof =
      single_log_aof(Arc::clone(&source.wal), &aof_options).expect("装配主端 single_log_aof");
    let _service =
      NodeService::new(Arc::clone(&source.store), primary_aof).expect("注册存储 AOF 事件汇");

    // ===== 预置走真 RESP 写路径（条目入账 + 快照覆盖两侧共用同一事实源）
    let mut primary = cluster_consumer(&provider_p, &source.store);
    for key in [
      K_NXF, K_NXE, K_XXM, K_XXE, K_GTL, K_GTH, K_LTH, K_LTL, K_XGT, K_XLT, K_OVW, K_PST,
    ] {
      exec(&mut primary, &[b"SET", key, b"v"], b"+OK\r\n");
    }
    // 基线 TTL（111s）：条件批的 NX/XX/GT/LT 比较对象，全部随快照进副本
    for key in [
      K_NXE, K_XXE, K_GTL, K_GTH, K_LTH, K_LTL, K_XGT, K_XLT, K_OVW,
    ] {
      exec(&mut primary, &[b"EXPIRE", key, b"111"], b":1\r\n");
    }

    // ===== 副本：回放装配 + 接收会话 + 宿主服务器（同 write_window 骨架）
    let replica = open_node("cond_expire_replica");
    let provider_r = provider_with_role(&replica, REPLICA_ID, 7001, NodeRole::Replica);
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
    let server = GarnetServer::new(
      &["127.0.0.1:0".to_string()],
      65536,
      100,
      Arc::new(ReplicaSessionProvider {
        provider: Arc::clone(&provider_r),
      }),
    )
    .unwrap();
    server.start(None).unwrap();
    let replica_addr = server.local_addr().unwrap().to_string();

    // ===== 全量同步发起（预置与基线 TTL 随快照导入副本）
    let rm_p = provider_p.replication_manager().unwrap();
    let assets = PrimaryReplicationAssets {
      wal: Arc::clone(&source.wal),
      pump: Arc::new(AofReplicationPump::new(Arc::clone(
        &rm_p.aof_sync_driver_store,
      ))),
      sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm_p))),
    };
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
    let sync_from =
      try_begin_diskless_sync_async(&provider_p, &assets, PRIMARY_ID, &replica_addr, &meta)
        .await
        .expect("无盘全量同步应完成");
    assert!(
      sync_from.get(0).is_some_and(|addr| addr > 0),
      "预置写入后授予位点必须非零"
    );

    let mut p_reader = reader(&source.store, 3);
    let mut r_reader = reader(&replica.store, 2);

    // ===== 快照侧先比一轮：基线 TTL 逐毫秒一致（read_live_value 携真实
    // expire_unix_ms 的口径由副本端直接验证），并留存被拒场景的基线值
    let base = [K_NXE, K_GTL, K_LTH]
      .map(|k| pexpiretime(&mut p_reader, k))
      .to_vec();
    for key in [K_NXF, K_XXM, K_PST] {
      assert_eq!(pexpiretime(&mut p_reader, key), -1, "无 TTL 键主端为 -1");
      assert_eq!(pexpiretime(&mut r_reader, key), -1, "无 TTL 键副本为 -1");
    }
    for key in [
      K_NXE, K_XXE, K_GTL, K_GTH, K_LTH, K_LTL, K_XGT, K_XLT, K_OVW,
    ] {
      let (p, r) = (
        pexpiretime(&mut p_reader, key),
        pexpiretime(&mut r_reader, key),
      );
      assert!(p > 0, "基线 TTL 键主端应为正毫秒: {key:?} {p}");
      assert_eq!(p, r, "基线 TTL 经快照后主从逐毫秒一致: {key:?}");
    }

    // ===== 同步收口后的条件 EXPIRE 批：全部落 AOF 增量条目回放路径。
    // 相对秒与基线差 ≥44s，测试执行期漂移不足以翻转任何 GT/LT 判定
    exec(&mut primary, &[b"EXPIRE", K_NXF, b"111", b"NX"], b":1\r\n");
    exec(&mut primary, &[b"EXPIRE", K_NXE, b"222", b"NX"], b":0\r\n");
    exec(&mut primary, &[b"EXPIRE", K_XXM, b"333", b"XX"], b":0\r\n");
    exec(&mut primary, &[b"EXPIRE", K_XXE, b"222", b"XX"], b":1\r\n");
    exec(&mut primary, &[b"EXPIRE", K_GTL, b"55", b"GT"], b":0\r\n");
    exec(&mut primary, &[b"EXPIRE", K_GTH, b"222", b"GT"], b":1\r\n");
    exec(&mut primary, &[b"EXPIRE", K_LTH, b"222", b"LT"], b":0\r\n");
    exec(&mut primary, &[b"EXPIRE", K_LTL, b"55", b"LT"], b":1\r\n");
    exec(
      &mut primary,
      &[b"EXPIRE", K_XGT, b"100", b"XX", b"GT"],
      b":0\r\n",
    );
    exec(
      &mut primary,
      &[b"EXPIRE", K_XGT, b"222", b"XX", b"GT"],
      b":1\r\n",
    );
    exec(
      &mut primary,
      &[b"EXPIRE", K_XLT, b"222", b"XX", b"LT"],
      b":0\r\n",
    );
    exec(
      &mut primary,
      &[b"EXPIRE", K_XLT, b"55", b"XX", b"LT"],
      b":1\r\n",
    );
    // 缺失键：主端 :0、零条目
    exec(&mut primary, &[b"EXPIRE", K_GHOST, b"111"], b":0\r\n");
    // 覆盖写：SET 语义清 TTL（TtlWrite 墓碑 → Persist 条目）
    exec(&mut primary, &[b"SET", K_OVW, b"v2"], b"+OK\r\n");
    // 过去时间戳：即时物理清除（DELIFEXPIM/墓碑条目）
    exec(&mut primary, &[b"EXPIREAT", K_PST, b"100"], b":1\r\n");

    // ===== 增量收口：提交 + 补扫 + 副本位点追平日志尾
    source.wal.commit().await.unwrap();
    let _ = assets.pump.sync_backlog(&source.wal).await;
    let new_tail = source.wal.tail_address() as i64;
    assert!(
      wait_for(
        || rm_r.get_current_replication_offset().get(0) == Some(new_tail),
        Duration::from_secs(10),
      )
      .await,
      "副本复制位点必须追平主端日志尾"
    );

    // ===== 生效场景：副本 PEXPIRETIME 与主端毫秒精确相等（零容差）
    for key in [K_NXF, K_XXE, K_GTH, K_LTL, K_XGT, K_XLT] {
      let (p, r) = (
        pexpiretime(&mut p_reader, key),
        pexpiretime(&mut r_reader, key),
      );
      assert!(p > 0, "条件生效后主端 TTL 为正: {key:?} {p}");
      assert_eq!(
        p, r,
        "条件 EXPIRE 经 AOF 回放后副本 TTL 逐毫秒一致: {key:?}"
      );
    }
    // ===== 被拒场景：主端保持基线且副本与主端一致（XX 无 TTL 拒 = 副本
    // 保持 -1，杜绝误判凭空 TTL；GT/LT/NX 拒 = 副本保持基线毫秒）
    assert_eq!(
      pexpiretime(&mut p_reader, K_XXM),
      -1,
      "XX 无 TTL 被拒主端保持 -1"
    );
    assert_eq!(
      pexpiretime(&mut r_reader, K_XXM),
      -1,
      "XX 被拒零条目，副本绝不许凭空出现 TTL"
    );
    for (key, b) in [(K_NXE, base[0]), (K_GTL, base[1]), (K_LTH, base[2])] {
      let (p, r) = (
        pexpiretime(&mut p_reader, key),
        pexpiretime(&mut r_reader, key),
      );
      assert_eq!(p, b, "被拒条件不得改动主端 TTL: {key:?}");
      assert_eq!(r, b, "被拒条件零条目，副本 TTL 逐毫秒保持基线: {key:?}");
    }
    // ===== 缺失键边界：双端 -2 且 EXISTS 0（EXPIRE 不造键）
    assert_eq!(exists(&mut p_reader, K_GHOST), 0);
    assert_eq!(
      exists(&mut r_reader, K_GHOST),
      0,
      "缺失键副本不得被 EXPIRE 造出"
    );
    assert_eq!(pexpiretime(&mut p_reader, K_GHOST), -2);
    assert_eq!(pexpiretime(&mut r_reader, K_GHOST), -2);
    // ===== 覆盖写边界：双端无 TTL 且值为新值
    assert_eq!(pexpiretime(&mut p_reader, K_OVW), -1);
    assert_eq!(
      pexpiretime(&mut r_reader, K_OVW),
      -1,
      "SET 覆盖写清 TTL 经 Persist 条目同步到副本"
    );
    assert_eq!(
      get_value(&mut p_reader, K_OVW).as_deref(),
      Some(b"v2".as_ref())
    );
    assert_eq!(
      get_value(&mut r_reader, K_OVW).as_deref(),
      Some(b"v2".as_ref())
    );
    // ===== 过去时间戳边界：双端整键消失
    assert_eq!(exists(&mut p_reader, K_PST), 0, "主端过去时间戳即时清除");
    assert_eq!(
      exists(&mut r_reader, K_PST),
      0,
      "过去时间戳清除条目回放后副本整键消失"
    );
    assert_eq!(pexpiretime(&mut p_reader, K_PST), -2);
    assert_eq!(pexpiretime(&mut r_reader, K_PST), -2);
    // ===== 值面兜底：生效键值随快照在场且未被回放破坏
    assert_eq!(
      get_value(&mut r_reader, K_NXF).as_deref(),
      Some(b"v".as_ref())
    );
    assert_eq!(
      get_value(&mut r_reader, K_XXE).as_deref(),
      Some(b"v".as_ref())
    );

    server.dispose();
  });
}
