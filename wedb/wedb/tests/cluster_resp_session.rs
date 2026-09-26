//! 集群会话切面集成测试：RespServerSession + ClusterSession + StoreGarnetApi
//! 装配形态下的槽位验证、MOVED 重定向、本地槽真执行、CLUSTER
//! 命令族与 ROLE 集群分支，对标 garnet/test/cluster 会话级用例
//! （RespRoundTrip / ClusterManagementTests）
use std::{str::from_utf8, sync::Arc, time::Duration};

use compio::runtime::Runtime;
use wbase::hash_slot::{CLUSTER_SLOT_COUNT, slot_of};

/// 默认会话 (0,0) 库槽位（库级定槽 doc/zh/db.md 4.1：键内容不参与定槽）
const SLOT0: u16 = slot_of(0, 0);
/// 远端节点承载的槽位（与 SLOT0 异槽）
const REMOTE_SLOT: u16 = SLOT0 ^ 1;

/// 混合器反查扫描上界：均匀离散下 2^20 内对任一目标槽位必命中
/// （同时作为远端库消费者的库数上界，保证 SELECT 可达命中库）
const DB_FOR_SLOT_SCAN_BOUND: u64 = 1 << 20;

/// 枚举 db 使 slot_of(0, db) 落在目标槽位（混合器均匀离散，2^20 内必命中）
fn db_for_slot(target: u16) -> u64 {
  (0..DB_FOR_SLOT_SCAN_BOUND)
    .find(|&d| slot_of(0, d) == target)
    .unwrap_or_else(|| panic!("db_for_slot({target}) 未命中"))
}

/// SELECT 帧构造
fn select_frame(db: u64) -> Vec<u8> {
  let db_str = db.to_string();
  format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db_str.len(), db_str).into_bytes()
}
use wconf::{RuntimeServerConfig, RuntimeServerOptions};
use wdev::SegmentedDevice;
use wedb::{
  error::{RespScope, cluster_err_text},
  server::{
    cluster::{ClusterPreferredEndpointType, IClusterProvider},
    cluster_manager::ClusterManager,
    cluster_provider::ClusterProvider,
    cluster_session::{ClusterSession, cluster_sub_name},
    hash_slot::{HashSlot, SlotState},
    worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
  },
};
use wkv::WedbStore;
use wmetric::{GarnetServerMonitor, SessionMetricsHandle};
use wnode::{
  MessageConsumerFace, PrimaryTasks, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  service::{StorageSessionProvider, open_node_with_config},
};
use wpubsub::subscribe_broker::SubscribeBroker;
use wresp::{catalog::get_resp_command_name, command::RespCommand};
use wtest_base::{SilentNode, test_store_config};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 装配双主节点拓扑：node_1（本地）持 0..8192，node_2@7001 持 8192..16384
fn two_primary_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().unwrap_or_else(|| {
    let m = Arc::new(ClusterManager::new(Arc::clone(&cp)));
    *cp.cluster_manager.write() = Some(Arc::clone(&m));
    m
  });
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    let remote_worker_id = config.workers.len() as u16;
    config.workers.push(Worker {
      nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE12),
      address: "127.0.0.1".into(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
    for slot in 0..CLUSTER_SLOT_COUNT {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    config.slot_map[REMOTE_SLOT as usize] = HashSlot {
      worker_id: remote_worker_id,
      state: SlotState::Stable,
    };
  }
  cp
}

/// 构造挂接集群切面 + 存储执行域的会话消费者（单机与集群同一执行路径）
///
/// 存储同步注入集群提供者（槽位校验 exists 探测与命令执行同源，对标
/// C# clusterProvider.storeWrapper 单一存储面）
fn cluster_consumer_with_databases(
  cp: &ClusterProvider,
  max_databases: u64,
) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("gate.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions {
      max_databases,
      ..RespServerSessionOptions::default()
    },
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}

/// 默认 2 库上界消费者（保留 SELECT 越界拦截语义的用例用此形态）
fn cluster_consumer(cp: &ClusterProvider) -> RespSessionConsumer {
  cluster_consumer_with_databases(cp, 2)
}

/// 远端库消费者：库数上界抬至 `db_for_slot` 扫描全域，SELECT 可达远端
/// 槽位库（库级定槽下「远端节点数据」以会话切库表达，键内容不参与定槽）
fn cluster_consumer_remote_dbs(cp: &ClusterProvider) -> RespSessionConsumer {
  cluster_consumer_with_databases(cp, DB_FOR_SLOT_SCAN_BOUND)
}

/// 单命令往返（单帧完整到达）
fn roundtrip(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(consumer, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

#[test]
fn cluster_session_basic_commands() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  // PING → +PONG（快路径会话内闭环）
  assert_eq!(
    roundtrip(&mut consumer, b"*1\r\n$4\r\nPING\r\n"),
    b"+PONG\r\n"
  );

  // READONLY / READWRITE → +OK（集群切面置位）
  assert_eq!(
    roundtrip(&mut consumer, b"*1\r\n$8\r\nREADONLY\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, b"*1\r\n$9\r\nREADWRITE\r\n"),
    b"+OK\r\n"
  );

  // CLUSTER MYID → 真实节点 ID
  let out = roundtrip(&mut consumer, b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n");
  assert_eq!(out, format!("$32\r\n{NODE1_HEX}\r\n").into_bytes());

  // CLUSTER KEYSLOT foo → 会话当前库槽位（库级定槽 doc/zh/db.md 4.1：
  // Slot = Mixer(ns, db) 与键内容无关，同库任意键同槽回声）
  let out = roundtrip(
    &mut consumer,
    b"*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$3\r\nfoo\r\n",
  );
  assert_eq!(out, format!(":{SLOT0}\r\n").into_bytes());
}

/// ROLE 集群主节点分支：*3 master :offset *0（无挂载副本；offset 为 rm 初始
/// 复制位点，空日志起点 0——rust WalLog 无头区，判据 begin == tail）
#[test]
fn cluster_session_role_primary() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(&mut consumer, b"*1\r\n$4\r\nROLE\r\n");
  let expect = "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n";
  assert_eq!(out, expect.as_bytes());
}

/// ROLE 集群副本分支：*5 slave <主地址> <主端口> <连接状态> <offset>
///
/// 对标 C# ClusterRoleCommand 副本视角断言（RoleType=slave、主节点地址端口、
/// 连接状态、复制位点）；无 gossip 连接时状态判定为 "connect"
#[test]
fn cluster_session_role_replica() {
  let cp = two_primary_provider();
  cp.cluster_manager()
    .unwrap()
    .current_config
    .write()
    .make_replica_of(Some(0x0000_0000_0000_0000_0000_0000_0000_DE12));
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(&mut consumer, b"*1\r\n$4\r\nROLE\r\n");
  let expect = "*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:7001\r\n$7\r\nconnect\r\n:0\r\n";
  assert_eq!(out, expect.as_bytes());
}

/// FLUSHDB/FLUSHALL 副本只读拦截：集群副本角色上客户端清库被拒
///
/// 对标 C# NetworkFLUSHDB/NetworkFLUSHALL 门（EnableCluster &&
/// clusterProvider.IsReplica() && !clusterSession.IsInternalWriteSession →
/// CmdStrings.RESP_ERR_FLUSHALL_READONLY_REPLICA）
#[test]
fn flush_replica_read_only_rejected() {
  let cp = two_primary_provider();
  cp.cluster_manager()
    .unwrap()
    .current_config
    .write()
    .make_replica_of(Some(0x0000_0000_0000_0000_0000_0000_0000_DE12));
  let mut consumer = cluster_consumer(&cp);

  let expect: &[u8] = b"-ERR You can't write against a read only replica.\r\n";
  // 同步门：错误即回，无慢路径挂起，库不清
  let out = roundtrip(&mut consumer, b"*1\r\n$7\r\nFLUSHDB\r\n");
  assert_eq!(out, expect);
  let out = roundtrip(&mut consumer, b"*1\r\n$8\r\nFLUSHALL\r\n");
  assert_eq!(out, expect);
}

/// SWAPDB 集群按库归属门禁——同物理节点放行（doc/zh/db.md SWAPDB 条款：
/// 两库掌管槽位均由本地节点持有时走异步域换号执行，效果与单机一致；
/// 两库槽位 slot_of(0,0)/slot_of(0,1) 在本拓扑均归本地）
#[test]
fn swapdb_cluster_same_node_swaps() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  // db0 写标记值 a
  assert_eq!(
    roundtrip(&mut consumer, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\na\r\n"),
    b"+OK\r\n"
  );
  // db1 写标记值 b
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nb\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );

  // SWAPDB 0 1 → 同步段归属门禁放行 → 慢路径虚拟 ID 互换 +OK
  let (consumed, mut out) = pump(
    &mut consumer,
    b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n",
  );
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("SWAPDB 应挂起慢路径");
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b"+OK\r\n");

  // 互换生效：SELECT 1 见原 db0 的 a，SELECT 0 见原 db1 的 b
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
    b"$1\r\na\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
    b"$1\r\nb\r\n"
  );

  // 同库互换（0 0）：归属门禁通过后同步短路 +OK（无搬移语义，数据不动）
  assert_eq!(
    roundtrip(
      &mut consumer,
      b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n0\r\n"
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
    b"$1\r\nb\r\n"
  );
}

/// SWAPDB 集群按库归属门禁——跨节点拦截（任一库掌管槽位非本地 → 新语义
/// 文案报错；同库互换同样受门禁约束——门禁先于同库短路，换库仍改变本地
/// 槽位集合）
#[test]
fn swapdb_cluster_cross_node_rejected() {
  let cp = two_primary_provider();
  // db1 掌管槽位改判远端主节点（worker 1 = node_2@7001）
  {
    let cm = cp.cluster_manager().unwrap();
    let mut config = cm.current_config.write();
    let remote_worker_id = (config.workers.len() - 1) as u16;
    config.slot_map[slot_of(0, 1) as usize] = HashSlot {
      worker_id: remote_worker_id,
      state: SlotState::Stable,
    };
  }
  let mut consumer = cluster_consumer(&cp);

  let expect: &[u8] = b"-ERR SWAPDB databases are not served by this node\r\n";
  assert_eq!(
    roundtrip(
      &mut consumer,
      b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n"
    ),
    expect
  );
  // 同库互换（1 1）同样拦截
  assert_eq!(
    roundtrip(
      &mut consumer,
      b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n1\r\n$1\r\n1\r\n"
    ),
    expect
  );
  // 校验序锁定：下标解析与 max_databases 上界错误先于归属门禁返回
  assert_eq!(
    roundtrip(
      &mut consumer,
      b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$3\r\nabc\r\n"
    ),
    b"-ERR invalid second DB index.\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n9\r\n"
    ),
    b"-ERR DB index is out of range.\r\n"
  );
}

/// CLUSTER NODES：双主拓扑两行节点信息（对标 C# ClusterNodeCommand：行数 =
/// 节点数、首字段 nodeid、地址 host:port@bus、master 行主 id "-" 与
/// config-epoch "1"、connected 状态）
#[test]
fn cluster_session_cluster_nodes() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(&mut consumer, b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n");

  // bulk 字符串帧 $<len>\r\n<payload>\r\n → 剥帧头帧尾取载荷
  let text = String::from_utf8_lossy(&out);
  let payload = text
    .strip_prefix('$')
    .and_then(|rest| rest.split_once("\r\n"))
    .map(|(_, body)| body.strip_suffix("\r\n").unwrap_or(body))
    .unwrap_or_default();
  let lines: Vec<&str> = payload.split('\n').filter(|l| !l.is_empty()).collect();
  assert_eq!(lines.len(), 2, "双主拓扑应有 2 行节点信息: {payload}");

  let (local, remote) = (lines[0], lines[1]);
  assert!(
    local.starts_with(&format!("{NODE1_HEX} 127.0.0.1:7000@17000 ")),
    "本地行首字段应为 nodeid + 地址@总线端口: {local}"
  );
  assert!(
    local.contains("myself,master - ") && local.contains(" 1 connected "),
    "本地行应含 myself,master、主 id '-'、config-epoch 1、connected: {local}"
  );
  assert!(
    remote.starts_with(&format!("{NODE2_HEX} 127.0.0.1:7001@17001 ")),
    "远端行首字段应为 nodeid + 地址@总线端口: {remote}"
  );
  assert!(
    remote.contains("master - ") && remote.contains(" 1 disconnected "),
    "远端行应含 master、主 id '-'、config-epoch 1；无 gossip 连接时为 \
     disconnected（本地行恒 connected）: {remote}"
  );
}

/// CLUSTER SHARDS：验证分片信息输出与节点连接状态透传（对标 C# RespClusterBasicCommands:346）
/// 验证当远端节点未建立 gossip 连接时 health 为 offline；
/// 当远端节点连接建立后 health 正确翻转为 online（修复传递 None 导致恒 offline 的缺陷）
#[compio::test]
async fn cluster_session_cluster_shards() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  // 1. 初始状态：未建立远端 gossip 连接
  // node_1 (local): health 应为 online
  // node_2 (remote): health 应为 offline
  let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "SHARDS"]));
  let text = String::from_utf8_lossy(&out);
  assert!(text.contains("*2\r\n"), "双主拓扑应有 2 个分片: {text}");
  assert!(text.contains(NODE1_HEX), "分片应包含 node_1: {text}");
  assert!(text.contains(NODE2_HEX), "分片应包含 node_2: {text}");
  assert!(
    text.contains("$5\r\nslots\r\n"),
    "应包含 slots 字段: {text}"
  );
  assert!(
    text.contains("$5\r\nnodes\r\n"),
    "应包含 nodes 字段: {text}"
  );
  assert_eq!(
    text.matches("online").count(),
    1,
    "初始状态仅本地主节点 node_1 应为 online: {text}"
  );
  assert_eq!(
    text.matches("offline").count(),
    1,
    "初始状态远端主节点 node_2 应为 offline: {text}"
  );

  // 2. 模拟远端节点 node_2 连接建立（gossip 连接池注册 + 假端点真建链）
  let fake = SilentNode::bind(2).await;
  let gm = cp.gossip_manager().expect("gossip manager 在场");
  let conn = gm.connection_store.get_or_add(
    0x0000_0000_0000_0000_0000_0000_0000_DE12,
    "127.0.0.1",
    fake.port() as i32,
    &cp,
  );
  conn.initialize_async(Duration::from_secs(1)).await;
  assert!(
    cp.get_connection_info(0x0000_0000_0000_0000_0000_0000_0000_DE12)
      .connected,
    "node_2 应标记为已连接"
  );

  // 3. 再次查询 CLUSTER SHARDS：通过传入的 cluster_provider 正确获取连接状态，
  //    node_2 的 health 翻转为 online
  let out2 = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "SHARDS"]));
  let text2 = String::from_utf8_lossy(&out2);
  assert!(
    !text2.contains("offline"),
    "远端建立连接后不应包含 offline: {text2}"
  );
  assert_eq!(
    text2.matches("online").count(),
    2,
    "两个主节点的 health 应均为 online: {text2}"
  );

  // 4. 重置 node_2 连接状态为断开（调用 dispose 释放连接）
  conn.client.dispose();
  assert!(
    !cp
      .get_connection_info(0x0000_0000_0000_0000_0000_0000_0000_DE12)
      .connected,
    "node_2 应标记为断开"
  );
  let out3 = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "SHARDS"]));
  let text3 = String::from_utf8_lossy(&out3);
  assert_eq!(
    text3.matches("online").count(),
    1,
    "断开后仅本地主节点为 online: {text3}"
  );
  assert_eq!(
    text3.matches("offline").count(),
    1,
    "断开后远端主节点 node_2 应恢复为 offline: {text3}"
  );
}

/// 远端槽位数据命令 → -MOVED 重定向（对标 CanServeSlot → NetworkMultiKeySlotVerify；
/// 库级定槽 doc/zh/db.md 4.1：会话切远端库，键内容不参与定槽）
#[test]
fn cluster_session_moved_redirect() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer_remote_dbs(&cp);
  let out = roundtrip(&mut consumer, &select_frame(db_for_slot(REMOTE_SLOT)));
  assert_eq!(out, b"+OK\r\n");
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(
    out,
    format!("-MOVED {REMOTE_SLOT} 127.0.0.1:7001\r\n").into_bytes()
  );
}

/// 本地槽位数据命令过门后真正执行（对标 CanServeSlot 放行 → ProcessBasicCommands
/// → IGarnetApi 存储执行；与 MOVED 重定向同一条会话路径）
#[test]
fn cluster_session_local_slot_executes() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  // SET → +OK（本地槽在存储执行域真正落写）
  assert_eq!(
    roundtrip(
      &mut consumer,
      b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$1\r\nv\r\n"
    ),
    b"+OK\r\n"
  );
  // GET → $1 v
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n"),
    b"$1\r\nv\r\n"
  );
  // EXPIRE bar 100 → :1；TTL → 正数（100s 窗口内）
  assert_eq!(
    roundtrip(
      &mut consumer,
      b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nbar\r\n$3\r\n100\r\n"
    ),
    b":1\r\n"
  );
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nTTL\r\n$3\r\nbar\r\n");
  let text = String::from_utf8_lossy(&out);
  let secs: i64 = text
    .trim_end_matches("\r\n")
    .trim_start_matches(':')
    .parse()
    .unwrap();
  assert!((1..=100).contains(&secs), "TTL 应在 (0,100] 内: {text}");

  // DEL bar → :1；GET → $-1
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$3\r\nDEL\r\n$3\r\nbar\r\n"),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n"),
    b"$-1\r\n"
  );
}

/// 同帧管道：本地库真执行与远端库 MOVED 混合消费（游标推进正确性；
/// 库级定槽下本地/远端以 SELECT 切换会话库表达，键内容不参与定槽）
#[test]
fn cluster_session_pipeline_mixed_local_remote() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer_remote_dbs(&cp);
  // 本地库 GET bar（真执行）→ SELECT 远端槽位库 → 远端库 GET foo（MOVED）
  let mut frame = b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n".to_vec();
  frame.extend_from_slice(&select_frame(db_for_slot(REMOTE_SLOT)));
  frame.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  let (consumed, out) = pump(&mut consumer, &frame);
  assert_eq!(consumed, Some(0));
  // bar 本地库未写过 → nil；SELECT → +OK；远端库 GET → MOVED <远端槽> 7001
  assert_eq!(
    out,
    format!("$-1\r\n+OK\r\n-MOVED {REMOTE_SLOT} 127.0.0.1:7001\r\n").into_bytes()
  );
}

/// ASKING 后导入态槽位放行（对标 SingleKeyReadWriteSlotVerify IMPORTING + SessionAsking）
#[test]
fn cluster_session_asking_importing_slot() {
  let cp = two_primary_provider();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    // node_2 已由 two_primary_provider 注册为槽位属主；改会话库槽 SLOT0
    // 为导入态（库级定槽：GET bar 落 (0,0) 库 → SLOT0，键内容不参与定槽）
    let node2_worker_id = config
      .workers
      .iter()
      .position(|w| w.nodeid == Some(0x0000_0000_0000_0000_0000_0000_0000_DE12))
      .expect("node_2 应已注册") as u16;
    config.slot_map[SLOT0 as usize] = HashSlot {
      worker_id: node2_worker_id,
      state: SlotState::Importing,
    };
  }
  let mut consumer = cluster_consumer(&cp);

  // 无 ASKING → MOVED 至源节点
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
  assert_eq!(
    out,
    format!("-MOVED {SLOT0} 127.0.0.1:7001\r\n").into_bytes()
  );

  // ASKING 置位后同一键通过槽位门
  assert_eq!(
    roundtrip(&mut consumer, b"*1\r\n$6\r\nASKING\r\n"),
    b"+OK\r\n"
  );
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
  assert!(
    !out.starts_with(b"-MOVED"),
    "ASKING 后导入槽位应放行: {out:?}"
  );
}

/// 集群对外渲染面非 ASCII 宣告主机名折叠锁（zcode-r139c-cluenc）：
/// 「café」（UTF-8 五字节）宣告主机名在 Hostname 偏好下，CLUSTER
/// NODES/SLOTS/SHARDS 渲染臂与 MOVED 重定向帧一律经 wbase::ascii_sanitize
/// 单机制逐字节折 '?' 为 "caf??"（bulk 头 $5 与折叠体等长自洽），对外值域
/// 恒 ASCII——对标 C# 渲染出口 Encoding.ASCII 落字节折叠（TryWriteAsciiDirect/
/// WriteAsciiLargeRespString/RespClusterSlotVerify 组串收口）。C# UTF-16 逐
/// 字符折形为 "caf?"/$4，两侧系值域收拢非逐字节等形，登记见 deviations.md
#[test]
fn cluster_render_non_ascii_hostname_folds_to_ascii() {
  let cp = two_primary_provider();
  {
    let m = cp.cluster_manager().unwrap();
    let mut config = m.current_config.write();
    for w in config.workers.iter_mut().skip(1) {
      w.hostname = Some("café".into());
    }
  }
  cp.set_preferred_endpoint_type(ClusterPreferredEndpointType::Hostname);
  let mut consumer = cluster_consumer_remote_dbs(&cp);

  // 1. CLUSTER NODES：宣告主机名元数据段折 '?'，整帧恒 ASCII
  let out = roundtrip(&mut consumer, b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n");
  assert!(out.is_ascii(), "CLUSTER NODES 应答应恒 ASCII 字节: {out:?}");
  let text = String::from_utf8(out).unwrap();
  assert_eq!(
    text.matches(",caf?? ").count(),
    2,
    "NODES 两行宣告主机名均应逐字节折叠: {text}"
  );

  // 2. CLUSTER SLOTS：Hostname 偏好下端点位折叠 bulk 头体自洽
  let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "SLOTS"]));
  assert!(out.is_ascii(), "CLUSTER SLOTS 应答应恒 ASCII 字节: {out:?}");
  let text = String::from_utf8(out).unwrap();
  assert!(
    text.contains("$5\r\ncaf??\r\n"),
    "SLOTS 端点位应为折叠形 $5\\r\\ncaf?? : {text}"
  );

  // 3. CLUSTER SHARDS：endpoint 与 hostname 两臂均折叠（双主 × 两臂 = 4 处）
  let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "SHARDS"]));
  assert!(
    out.is_ascii(),
    "CLUSTER SHARDS 应答应恒 ASCII 字节: {out:?}"
  );
  let text = String::from_utf8(out).unwrap();
  assert_eq!(
    text.matches("$5\r\ncaf??\r\n").count(),
    4,
    "SHARDS endpoint/hostname 双臂双节点均应折叠: {text}"
  );

  // 4. MOVED 重定向帧：端点实参折叠，帧字节钉死
  let out = roundtrip(&mut consumer, &select_frame(db_for_slot(REMOTE_SLOT)));
  assert_eq!(out, b"+OK\r\n");
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(
    out,
    format!("-MOVED {REMOTE_SLOT} caf??:7001\r\n").into_bytes(),
    "MOVED 帧端点应逐字节折叠为 caf??"
  );
}

/// 单机形态（未挂切面）CLUSTER MYID → 集群支持未启用错误
#[test]
fn standalone_session_cluster_disabled() {
  use wnode::resp::{
    garnet_api::GarnetApiFace, resp_server_session::RespServerSession, slow_path::SlowFuture,
  };

  /// 桩存储执行域：CLUSTER MYID 在会话侧拦截，不应到达存储执行域
  struct UnreachableApi;
  impl GarnetApiFace for UnreachableApi {
    fn exec(&self, _session: &mut RespServerSession, cmd: RespCommand, _args: &[&[u8]]) {
      panic!("会话侧命令不应进入存储执行域: {cmd:?}");
    }

    fn exec_slow(
      self: Arc<Self>,
      _cmd: RespCommand,
      _args: Vec<Vec<u8>>,
      _resp_version: u8,
    ) -> SlowFuture {
      SlowFuture::new(async { panic!("会话侧命令不应进入慢路径执行域") })
    }
  }

  let mut consumer = RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(UnreachableApi),
  );
  let frame = b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n";
  let (consumed, out) = pump(&mut consumer, frame);
  assert_eq!(consumed, Some(0));
  assert_eq!(out, b"-ERR This instance has cluster support disabled\r\n");
}

/// CLUSTER RESET 专用装配：存储注入 provider（同一 store 构造执行域与
/// 慢路径扫描域，对标 C# clusterProvider.storeWrapper 的单一存储可达面）
fn reset_consumer(
  cp: &ClusterProvider,
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 打开 RESET 测试专用存储（GC 关闭）
fn reset_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let config = test_store_config();
  Arc::new(WedbStore::open(config, device).unwrap())
}

/// CLUSTER RESET 慢路径：无槽键时 SOFT 重置 +OK 且保留节点 ID
///
/// 对标 garnet/test/cluster/ClusterManagementTests.cs 的 CLUSTER RESET 用例
/// （TryReset 无键路径：newNodeId 保留、FlushDB 仅 HARD 触发）
#[test]
fn cluster_reset_soft_without_keys() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let store = reset_store("reset.db");
  cp.set_store(Arc::clone(&store));
  let mut consumer = reset_consumer(&cp, &store);

  // CLUSTER RESET → +OK（慢路径：HasKeysInSlots 判定 + TryReset）
  let (consumed, out) = pump(&mut consumer, b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n");
  assert_eq!(consumed, Some(0));
  assert!(out.is_empty(), "同步段仅校验，不残留输出");
  let slow = consumer.take_slow_wait().expect("RESET 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b"+OK\r\n");

  // SOFT 重置保留节点 ID（HARD 才换新 id）
  let out = pump(&mut consumer, b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n").1;
  assert_eq!(out, format!("$32\r\n{NODE1_HEX}\r\n").into_bytes());
}

/// CLUSTER RESET 慢路径：本节点槽上有键时拒绝（HasKeysInSlots 判定）
#[test]
fn cluster_reset_with_local_slot_keys_rejected() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let store = reset_store("reset2.db");
  cp.set_store(Arc::clone(&store));
  let mut consumer = reset_consumer(&cp, &store);

  // 本地库键写入（库级定槽：会话库槽决定归属）
  let key = String::from("rk0");
  let frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len());
  let (consumed, out) = pump(&mut consumer, frame.as_bytes());
  assert_eq!(consumed, Some(0));
  assert_eq!(out, b"+OK\r\n");

  // CLUSTER RESET → 槽键在场拒绝（SOFT 不清库：键存留、配置未动，
  // C# TryReset 拒否先行且不触发 FlushDB）
  let (consumed, out) = pump(&mut consumer, b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n");
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("RESET 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(
    out,
    b"-ERR CLUSTER RESET can't be called with master nodes containing keys\r\n"
  );
  // SOFT 拒否后键存留（DBSIZE 慢路径 :1）
  let (consumed, out) = pump(&mut consumer, b"*1\r\n$6\r\nDBSIZE\r\n");
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("DBSIZE 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b":1\r\n");
  // SOFT 拒否后配置未动（节点 ID 保留）
  let out = pump(&mut consumer, b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n").1;
  assert_eq!(out, format!("$32\r\n{NODE1_HEX}\r\n").into_bytes());

  // 参数校验（同步段）：多余参数 / 非整数过期秒数
  let out = pump(
    &mut consumer,
    b"*5\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n$4\r\nSOFT\r\n$1\r\n6\r\n$1\r\n7\r\n",
  )
  .1;
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|reset' command\r\n"
  );
  let out = pump(
    &mut consumer,
    b"*4\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n$4\r\nSOFT\r\n$1\r\nx\r\n",
  )
  .1;
  assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
}

/// CLUSTER RESET HARD 持键：应答仍为键检查拒绝、配置未动，但清库照常
/// 执行（C# RespClusterBasicCommands.cs:491 事实——`if (!soft)
/// FlushDB(true)` 位于 TryReset 之后、不问其成败）
#[test]
fn cluster_reset_hard_with_keys_rejected_flushes() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let store = reset_store("reset4.db");
  cp.set_store(Arc::clone(&store));
  let mut consumer = reset_consumer(&cp, &store);

  let key = String::from("hk9");
  let frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len());
  let (consumed, out) = pump(&mut consumer, frame.as_bytes());
  assert_eq!(consumed, Some(0));
  assert_eq!(out, b"+OK\r\n");

  let (consumed, out) = pump(
    &mut consumer,
    b"*3\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n$4\r\nHARD\r\n",
  );
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("RESET 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(
    out,
    b"-ERR CLUSTER RESET can't be called with master nodes containing keys\r\n"
  );

  // 清库照常（键已删）
  let (consumed, out) = pump(&mut consumer, b"*1\r\n$6\r\nDBSIZE\r\n");
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("DBSIZE 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b":0\r\n");

  // 配置未换：SOFT 语义保留 nodeId 同样适用于被拒的 HARD（HARD 换新 id
  // 只在 TryReset 成功路径发生），节点 ID 不变
  let out = pump(&mut consumer, b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n").1;
  assert_eq!(out, format!("$32\r\n{NODE1_HEX}\r\n").into_bytes());
}

/// CLUSTER RESET HARD：清空全部用户键（C# `!soft → FlushDB(true)`）
#[test]
fn cluster_reset_hard_flushes_keys() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let store = reset_store("reset3.db");
  cp.set_store(Arc::clone(&store));
  let mut consumer = reset_consumer(&cp, &store);

  // 写入一个本地库键（库级定槽：会话库槽决定归属）
  let key = String::from("hk0");
  let frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len());
  let (consumed, out) = pump(&mut consumer, frame.as_bytes());
  assert_eq!(consumed, Some(0));
  assert_eq!(out, b"+OK\r\n");

  // 先 DBSIZE 验证 :1（慢路径计数）
  let (consumed, out) = pump(&mut consumer, b"*1\r\n$6\r\nDBSIZE\r\n");
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("DBSIZE 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b":1\r\n");

  // 把本地槽全部改归远端（构造"无本地槽键"形态过 HasKeysInSlots 门）
  {
    let m = cp.cluster_manager().unwrap();
    let mut config = m.current_config.write();
    for slot in config.slot_map.iter_mut() {
      if slot.worker_id == LOCAL_WORKER_ID as u16 {
        slot.worker_id = 2; // 归属远端 worker（node_2）
      }
    }
  }

  // CLUSTER RESET HARD → +OK（含清库）
  let (consumed, out) = pump(
    &mut consumer,
    b"*3\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n$4\r\nHARD\r\n",
  );
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("RESET 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b"+OK\r\n");

  let (_consumed, out) = pump(&mut consumer, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n");
  assert_eq!(out, b"+OK\r\n");

  // HARD 清库后：DBSIZE → :0
  let (consumed, out) = pump(&mut consumer, b"*1\r\n$6\r\nDBSIZE\r\n");
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("DBSIZE 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b":0\r\n");
}

// ---------------------------------------------------------------------------
// CLUSTER 子命令族接线回归（SETSLOT/ADDSLOTS/DELSLOTS 族 / MEET / FORGET /
// REPLICAS / MYPARENTID / BANLIST / MTASKS / ENDPOINT / SETCONFIGEPOCH /
// COUNTKEYSINSLOT / GETKEYSINSLOT / FLUSHALL / GOSSIP / REPLICAOF / FAILOVER）
// ---------------------------------------------------------------------------

use wnode_test::{err_frame, pump};
use wresp::cmd_strings::{
  RESP_ERR_GENERIC_SYNTAX_ERROR, cluster::ERR_GENERIC_REPLICATION_AOF_TURNEDOFF,
};
use wtest_base::{resp_frame, resp_frame_str};

const NODE1_HEX: &str = "0000000000000000000000000000de11";
const NODE2_HEX: &str = "0000000000000000000000000000de12";
const NODE3_HEX: &str = "0000000000000000000000000000de13";

/// 追加第三个已知主节点 node_3（不持有任何槽位），供 IMPORTING 源属主
/// 不匹配臂（C# ClusterManagerSlotState.cs 的 TryPrepareSlotForImport :238-243）
/// 构造「节点合法存在但非该槽属主」的拓扑
fn add_extra_primary(cp: &ClusterProvider) {
  let m = cp.cluster_manager().unwrap();
  let mut config = m.current_config.write();
  config.workers.push(Worker {
    nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE13),
    address: "127.0.0.1".into(),
    port: 7002,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
}

/// 挂共享存储的集群会话消费者（provider.set_store 与执行域同源：
/// COUNTKEYSINSLOT / GETKEYSINSLOT / FLUSHALL 慢路径经 provider 下达）
fn cluster_store_consumer(
  cp: &ClusterProvider,
) -> (RespSessionConsumer, Arc<WedbStore<SegmentedDevice>>) {
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  cluster_store_consumer_with(cp, test_store_config())
}

/// 指定存储配置的会话消费者（GC 扫描开启等定制场景；装配体与上同源单点）
fn cluster_store_consumer_with(
  cp: &ClusterProvider,
  config: wkv::StoreConfig,
) -> (RespSessionConsumer, Arc<WedbStore<SegmentedDevice>>) {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("cluster.db")).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  cp.set_store(Arc::clone(&store));
  let consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  (consumer, store)
}

/// CLUSTER SETSLOT：MIGRATING 保持本地服务 → STABLE 复位 → NODE 属主转移
#[test]
fn cluster_setslot_state_transitions() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let m = cp.cluster_manager().unwrap();
  // 会话库槽位（库级定槽：SET bar 落 (0,0) 库 → SLOT0），帧参数复用
  let slot_str = SLOT0.to_string();

  // 本地槽 SLOT0 先落键再置 MIGRATING → 键仍在本地，读继续本地服务
  //（C# CanOperateOnKey：MIGRATING 槽键存在才放行，缺失即 ASK）
  let out = roundtrip(&mut consumer, &resp_frame_str(&["SET", "bar", "v"]));
  assert_eq!(out, b"+OK\r\n");
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "MIGRATING", NODE2_HEX]),
  );
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(m.current_config().get_state(SLOT0), SlotState::Migrating);
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["GET", "bar"])),
    b"$1\r\nv\r\n"
  );

  // STABLE 复位
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "STABLE"]),
  );
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(m.current_config().get_state(SLOT0), SlotState::Stable);

  // NODE 转移属主至 node_2 → 后续 GET bar → MOVED
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "NODE", NODE2_HEX]),
  );
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["GET", "bar"])),
    format!("-MOVED {SLOT0} 127.0.0.1:7001\r\n").into_bytes()
  );

  // 非法槽位状态 → "not supported."；STABLE 带 node-id → 语法错误
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "FOO", NODE2_HEX]),
  );
  assert_eq!(out, b"-ERR Slot state FOO not supported.\r\n");
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "STABLE", NODE2_HEX]),
  );
  assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

  // 属主转回本地并复位，避免影响其他用例（每用例独立 provider，无实际污染）
  let _ = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "NODE", NODE1_HEX]),
  );
  let _ = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "STABLE"]),
  );
}

/// CLUSTER SETSLOT / SETSLOTSRANGE MIGRATING 二次排期拒否报文对标
/// C# libs/cluster/Server 下 ClusterManagerSlotState 的单槽臂
/// TryPrepareSlotForMigration（:116）与批量臂 TryPrepareSlotsForMigration（:188）：
/// `ERR Slot already scheduled for
/// migration from {源节点ID}`——无槽号、不提 import，from 段取 Migrating
/// 槽 eff 属主投影（HashSlot.workerId → LOCAL_WORKER_ID）的源节点 ID；
/// 锁死与 REPLICATE 持槽拒否两条映射不再互相串台
#[test]
fn cluster_setslot_migration_reschedule_rejection() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let slot_str = SLOT0.to_string();

  // 单槽臂（:114-118）：首排期 OK → 二次排期拒否，from 段为源节点 node_1
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "MIGRATING", NODE2_HEX])
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "MIGRATING", NODE2_HEX])
    ),
    format!("-ERR Slot already scheduled for migration from {NODE1_HEX}\r\n").into_bytes()
  );

  // 复位后批量臂（:186-190）：SETSLOTSRANGE 首排期 OK → 二次排期同报文
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "STABLE"]),
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&[
        "CLUSTER",
        "SETSLOTSRANGE",
        "MIGRATING",
        NODE2_HEX,
        &slot_str,
        &slot_str
      ])
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&[
        "CLUSTER",
        "SETSLOTSRANGE",
        "MIGRATING",
        NODE2_HEX,
        &slot_str,
        &slot_str
      ])
    ),
    format!("-ERR Slot already scheduled for migration from {NODE1_HEX}\r\n").into_bytes()
  );
}

/// CLUSTER REPLICATE 前置门：本节点仍持有分配槽 → 拒绝降副本，报文
/// `ERR Primary has been assigned slots and cannot be a replica`
/// （C# ClusterManagerWorkerState.cs:180-183
/// RESP_ERR_GENERIC_CANNOT_MAKE_REPLICA_WITH_ASSIGNED_SLOTS）；断言报文
/// 不含任何槽号字样与 migration 排期文案，锁死不复用
/// SlotAlreadyScheduled 载体（C# 对位臂无魔法槽号 0）
#[test]
fn cluster_replicate_with_assigned_slots_rejection() {
  let cp = two_primary_provider();
  let m = cp.cluster_manager().unwrap();
  Runtime::new().unwrap().block_on(async {
    // 本地 node_1 持有分配槽（two_primary_provider 全槽归本地）→
    // force=false 前置门在 begin_recovery 之前拒绝，无恢复锁残留
    let err = m
      .try_add_replica_async(0x0000_0000_0000_0000_0000_0000_0000_DE12, false, false)
      .await
      .unwrap_err();
    let msg = cluster_err_text(err, RespScope::WorkerReplica);
    assert_eq!(
      msg,
      "ERR Primary has been assigned slots and cannot be a replica"
    );
    // 报文不含任何槽号字样与排期文案：两条映射互不串台
    assert!(!msg.contains("already scheduled"));
    assert!(!msg.chars().any(|c| c.is_ascii_digit()));
  });
}

/// CLUSTER SETSLOT <slot> NODE：IMPORTING 态传入非本地节点 ID →
/// C# ClusterManagerSlotState.cs:TryPrepareSlotForOwnershipChange :356-362
/// 回 `ERR Input nodeid {nodeid} different from local nodeid {LocalNodeId}.`
/// （节点在拓扑中合法已知，不得误报 I don't know about node）；
/// 传本地节点 ID → :364-374 导入收尾成功
#[test]
fn cluster_setslot_node_importing_nodeid_mismatch() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let m = cp.cluster_manager().unwrap();
  let slot_str = REMOTE_SLOT.to_string();

  // 先将 REMOTE_SLOT 自 node_2 置 IMPORTING
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "IMPORTING", NODE2_HEX])
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    m.current_config().get_state(REMOTE_SLOT),
    SlotState::Importing
  );

  // NODE 传 node_2（已知但非本地）→ 单一机制报文（缺陷臂旧态误回
  // ERR I don't know about node）
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "NODE", NODE2_HEX])
    ),
    b"-ERR Input nodeid 0000000000000000000000000000de12 different from local nodeid 0000000000000000000000000000de11.\r\n"
  );
  assert_eq!(
    m.current_config().get_state(REMOTE_SLOT),
    SlotState::Importing
  );

  // NODE 传本地 node_1 → 收尾成功，槽回 Stable 且属主落本地
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &slot_str, "NODE", NODE1_HEX])
    ),
    b"+OK\r\n"
  );
  assert_eq!(m.current_config().get_state(REMOTE_SLOT), SlotState::Stable);
  assert!(m.current_config().is_local(REMOTE_SLOT, false));

  // NODE 传拓扑未知节点 → 前门如实报节点未知（C#
  // ClusterManagerSlotState.cs:TryPrepareSlotForOwnershipChange :335-339）
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&[
        "CLUSTER",
        "SETSLOT",
        &slot_str,
        "NODE",
        "0000000000000000000000000000de99"
      ])
    ),
    b"-ERR I don't know about node 0000000000000000000000000000de99\r\n"
  );
}

/// CLUSTER SETSLOT <slot> IMPORTING 各前置校验臂错误报文逐一对标
/// C# ClusterManagerSlotState.cs:TryPrepareSlotForImport :220-249
#[test]
fn cluster_setslot_importing_precondition_errors() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let slot0_str = SLOT0.to_string();
  let remote_str = REMOTE_SLOT.to_string();

  // 未知节点门（:220-224）
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&[
        "CLUSTER",
        "SETSLOT",
        &remote_str,
        "IMPORTING",
        "0000000000000000000000000000de99"
      ])
    ),
    b"-ERR I don't know about node 0000000000000000000000000000de99\r\n"
  );

  // 本地持有门（:232-236）：SLOT0 本地已持
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &slot0_str, "IMPORTING", NODE2_HEX])
    ),
    format!("-ERR This is a local hash slot {SLOT0} and is already imported\r\n").into_bytes()
  );

  // 源属主不匹配门（:238-243）：node_3 已知但不持 REMOTE_SLOT
  add_extra_primary(&cp);
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &remote_str, "IMPORTING", NODE3_HEX])
    ),
    format!("-ERR Slot {REMOTE_SLOT} is not owned by {NODE3_HEX}\r\n").into_bytes()
  );

  // 状态非 Stable 门（:245-249）：二次 IMPORTING 同槽
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &remote_str, "IMPORTING", NODE2_HEX])
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &remote_str, "IMPORTING", NODE2_HEX])
    ),
    b"-ERR Slot already scheduled for import from 0000000000000000000000000000de12\r\n"
  );

  // 本地角色门（:226-230）：降本地为 Replica，门序先于本地持有门
  // （对 SLOT0 本地槽报角色错而非 already imported，锁定 C# 判定序）
  cp.cluster_manager()
    .unwrap()
    .current_config
    .write()
    .set_local_worker_role(NodeRole::Replica);
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", &slot0_str, "IMPORTING", NODE2_HEX])
    ),
    b"-ERR Importing node REPLICA is not a master node.\r\n"
  );
}

/// CLUSTER SETSLOTSRANGE IMPORTING 批量臂前置校验错误报文对标
/// C# ClusterManagerSlotState.cs:TryPrepareSlotsForImport :273-311
#[test]
fn cluster_setslotsrange_importing_precondition_errors() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let slot0_str = SLOT0.to_string();
  let remote_str = REMOTE_SLOT.to_string();

  // 未知节点门（:275-279）：nodeid 合法 hex 但不在拓扑内
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&[
        "CLUSTER",
        "SETSLOTSRANGE",
        "IMPORTING",
        "0000000000000000000000000000de99",
        &remote_str,
        &remote_str
      ])
    ),
    b"-ERR I don't know about node 0000000000000000000000000000de99\r\n"
  );

  // 本地持有门（:292-296）
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&[
        "CLUSTER",
        "SETSLOTSRANGE",
        "IMPORTING",
        NODE2_HEX,
        &slot0_str,
        &slot0_str
      ])
    ),
    format!("-ERR This is a local hash slot {SLOT0} and is already imported\r\n").into_bytes()
  );

  // 源属主不匹配门（:299-304）
  add_extra_primary(&cp);
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&[
        "CLUSTER",
        "SETSLOTSRANGE",
        "IMPORTING",
        NODE3_HEX,
        &remote_str,
        &remote_str
      ])
    ),
    format!("-ERR Slot {REMOTE_SLOT} is not owned by {NODE3_HEX}\r\n").into_bytes()
  );

  // 状态非 Stable 门（:307-311）：二次批量 IMPORTING 同槽
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&[
        "CLUSTER",
        "SETSLOTSRANGE",
        "IMPORTING",
        NODE2_HEX,
        &remote_str,
        &remote_str
      ])
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&[
        "CLUSTER",
        "SETSLOTSRANGE",
        "IMPORTING",
        NODE2_HEX,
        &remote_str,
        &remote_str
      ])
    ),
    b"-ERR Slot already scheduled for import from 0000000000000000000000000000de12\r\n"
  );

  // 本地角色门（:282-286）
  cp.cluster_manager()
    .unwrap()
    .current_config
    .write()
    .set_local_worker_role(NodeRole::Replica);
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&[
        "CLUSTER",
        "SETSLOTSRANGE",
        "IMPORTING",
        NODE2_HEX,
        &remote_str,
        &remote_str
      ])
    ),
    b"-ERR Importing node REPLICA is not a master node.\r\n"
  );
}

/// CLUSTER SETSLOT / SETSLOTSRANGE 双故障报错次序锁：「非法 hex node-id＋非法
/// 槽参」同现时首个错误帧与 C# 观测面一致——C# nodeId 仅 GetString 零校验透传
/// （RespClusterSlotManagementCommands.cs:452-455/:554），hex 拒收门后置于
/// xor/越界/try_parse_slots 门之后（r123 裁）；单故障臂证 hex 防御收紧未回退
/// （§13/r119 A6）
#[test]
fn cluster_setslot_multi_fault_gate_order() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  // 越界门先于 hex 门：IMPORTING＋非法 hex → 首报 Slot out of range（C# :466）
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", "99999", "IMPORTING", "notahex"])
    ),
    b"-ERR Slot out of range\r\n"
  );

  // xor 门（在场性判定）先于越界与 hex 门：STABLE＋node-id → 首报 syntax error（C# :458）
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", "99999", "STABLE", "notahex"])
    ),
    b"-ERR syntax error\r\n"
  );

  // SETSLOTSRANGE：hex 门后置于 try_parse_slots，奇尾首报 Invalid or out of range slot（C# :558）
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOTSRANGE", "MIGRATING", "notahex", "99999"])
    ),
    b"-ERR Invalid or out of range slot\r\n"
  );

  // 单故障负锁：仅 hex 非法（槽参合法）→ hex 拒收门仍触 syntax error，防御未回退
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", "5", "IMPORTING", "notahex"])
    ),
    b"-ERR syntax error\r\n"
  );
}

/// CLUSTER ADDSLOTS / DELSLOTS / ADDSLOTSRANGE / DELSLOTSRANGE：占用、未指派
/// 与越界错误语义
#[test]
fn cluster_addslots_delslots_semantics() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let m = cp.cluster_manager().unwrap();

  // 槽 200 本地已持有 → ADDSLOTS 报 busy
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "ADDSLOTS", "200"]),
  );
  assert_eq!(out, b"-ERR Slot 200 is already busy\r\n");

  // DELSLOTS 200 → +OK（槽解指派）→ ADDSLOTS 200 → +OK（回收）
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELSLOTS", "200"])
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTS", "200"])
    ),
    b"+OK\r\n"
  );
  assert_eq!(m.current_config().get_state(200), SlotState::Stable);

  // 重复 DELSLOTS → not assigned
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELSLOTS", "200", "200"])
    ),
    b"-ERR Slot 200 specified multiple times\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELSLOTSRANGE", "200", "202"])
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTSRANGE", "200", "202"])
    ),
    b"+OK\r\n"
  );
  for slot in [200u16, 201, 202] {
    assert_eq!(m.current_config().get_state(slot), SlotState::Stable);
  }

  // 越界 → "ERR Slot out of range"；区间参数个数为奇 → 元数错误
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTS", "16384"])
    ),
    b"-ERR Slot out of range\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTSRANGE", "300"])
    ),
    b"-ERR wrong number of arguments for 'cluster|addslotsrange' command\r\n"
  );

  // 非整数槽位参数 → C# ERR_INVALID_SLOT 口径（ADDSLOTS 与 ADDSLOTSRANGE
  // 两臂同文案，ClusterCommands.cs:71/:79）
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTS", "abc"])
    ),
    b"-ERR Invalid or out of range slot\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTSRANGE", "x", "1"])
    ),
    b"-ERR Invalid or out of range slot\r\n"
  );

  // 判定序：倒挂先于越界（ClusterCommands.cs:86 在 :92 之前），动态文案带两端实参
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTSRANGE", "20000", "10000"])
    ),
    b"-ERR Invalid range 20000 > 10000!\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTSRANGE", "5", "1"])
    ),
    b"-ERR Invalid range 5 > 1!\r\n"
  );
  // 不倒挂而越界 → 仍报 "ERR Slot out of range"
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTSRANGE", "30000", "40000"])
    ),
    b"-ERR Slot out of range\r\n"
  );
  // 重复槽位（区间交叠）→ 动态实参文案
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTSRANGE", "900", "902", "902", "903"])
    ),
    b"-ERR Slot 902 specified multiple times\r\n"
  );
}

/// COUNTKEYSINSLOT / GETKEYSINSLOT：本地槽真扫描（慢路径），远端槽 MOVED 重定向
#[test]
fn cluster_countkeys_getkeys_in_slot() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let (mut consumer, _store) = cluster_store_consumer(&cp);

  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["SET", "bar", "v"])),
    b"+OK\r\n"
  );

  // COUNTKEYSINSLOT <SLOT0> → :1（库级聚合慢路径扫描）
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", &SLOT0.to_string()]),
  );
  assert_eq!(out, b":1\r\n");

  // GETKEYSINSLOT <SLOT0> 10 → *1 bar
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "GETKEYSINSLOT", &SLOT0.to_string(), "10"]),
  );
  assert_eq!(out, b"*1\r\n$3\r\nbar\r\n");

  // 远端槽 → MOVED
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", &REMOTE_SLOT.to_string()]),
  );
  assert_eq!(
    out,
    format!("-MOVED {REMOTE_SLOT} 127.0.0.1:7001\r\n").into_bytes()
  );
}

/// CLUSTER FLUSHALL：慢路径清库闭环
#[test]
fn cluster_flushall_slow_path() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let (mut consumer, _store) = cluster_store_consumer(&cp);
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["SET", "bar", "v"])),
    b"+OK\r\n"
  );

  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "FLUSHALL"]),
  );
  assert_eq!(out, b"+OK\r\n");

  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["SELECT", "0"])),
    b"+OK\r\n"
  );

  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["GET", "bar"])),
    b"$-1\r\n"
  );
}

/// CLUSTER DELKEYSINSLOT / DELKEYSINSLOTRANGE：参数越界校验与慢路径删键
#[test]
fn cluster_del_keys_in_slot_bounds_and_slow_path() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let (mut consumer, _store) = cluster_store_consumer(&cp);

  // 参数数量校验
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT"])
    ),
    b"-ERR wrong number of arguments for 'cluster|delkeysinslot' command\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT", "1", "2"])
    ),
    b"-ERR wrong number of arguments for 'cluster|delkeysinslot' command\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOTRANGE"])
    ),
    b"-ERR wrong number of arguments for 'cluster|delkeysinslotrange' command\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOTRANGE", "0"])
    ),
    b"-ERR wrong number of arguments for 'cluster|delkeysinslotrange' command\r\n"
  );

  // 单槽非法整数
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT", "invalid"])
    ),
    b"-ERR Invalid or out of range slot\r\n"
  );

  // 单槽越界截断防御：-1、16384、65536 均报错，杜绝截断为槽 0
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT", "-1"])
    ),
    b"-ERR Slot out of range\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT", "16384"])
    ),
    b"-ERR Slot out of range\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT", "65536"])
    ),
    b"-ERR Slot out of range\r\n"
  );

  // 区间分支越界、倒挂与重复校验
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOTRANGE", "-1", "10"])
    ),
    b"-ERR Slot out of range\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOTRANGE", "0", "16384"])
    ),
    b"-ERR Slot out of range\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOTRANGE", "10", "5"])
    ),
    b"-ERR Invalid range 10 > 5!\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOTRANGE", "1", "3", "2", "4"])
    ),
    b"-ERR Slot 2 specified multiple times\r\n"
  );

  // 合法槽位（会话 (0,0) 库槽位）慢路径删除执行：库级聚合按
  // slot_of(ns, db) 命中本库后整库删键
  let slot_str = SLOT0.to_string();
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["SET", "bar", "v"])),
    b"+OK\r\n"
  );
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT", &slot_str]),
  );
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["GET", "bar"])),
    b"$-1\r\n"
  );

  // 合法槽位慢路径区间删除执行
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["SET", "bar", "v2"])),
    b"+OK\r\n"
  );
  let out_range = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "DELKEYSINSLOTRANGE", &slot_str, &slot_str]),
  );
  assert_eq!(out_range, b"+OK\r\n");
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["GET", "bar"])),
    b"$-1\r\n"
  );
}

/// CLUSTER MYPARENTID / BANLIST / MTASKS / ENDPOINT / REPLICAS / FORGET
#[test]
fn cluster_node_inspection_commands() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let m = cp.cluster_manager().unwrap();
  let mut consumer = cluster_consumer(&cp);

  // 主节点视角：MYPARENTID = 自身 id；BANLIST 空；MTASKS :0
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "MYPARENTID"])),
    format!("$32\r\n{NODE1_HEX}\r\n").as_bytes()
  );
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "BANLIST"])),
    b"*0\r\n"
  );
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "MTASKS"])),
    b":0\r\n"
  );

  // ENDPOINT 已知 / 未知节点
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ENDPOINT", NODE2_HEX])
    ),
    b"$14\r\n127.0.0.1:7001\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ENDPOINT", "ghost"])
    ),
    b"$12\r\nunassigned:0\r\n"
  );

  // 翻转为 node_2 副本：MYPARENTID → node_2；REPLICAS node_2 → 本节点行
  m.current_config
    .write()
    .make_replica_of(Some(0x0000_0000_0000_0000_0000_0000_0000_DE12));
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "MYPARENTID"])),
    format!("$32\r\n{NODE2_HEX}\r\n").as_bytes()
  );
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "REPLICAS", NODE2_HEX]),
  );
  let text = String::from_utf8_lossy(&out);
  assert!(text.starts_with("*1\r\n$"), "node_2 应有 1 个副本: {text}");
  assert!(text.contains(NODE1_HEX), "副本行应为 node_1: {text}");

  // REPLICAS 节点 id 大写 hex 形态可解析（内部收敛 u128，无字符串大小写语义）
  let node2_hex_upper = NODE2_HEX.to_ascii_uppercase();
  let out_upper = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "REPLICAS", &node2_hex_upper]),
  );
  let text_upper = String::from_utf8_lossy(&out_upper);
  assert!(
    text_upper.starts_with("*1\r\n$"),
    "NODE_2 应有 1 个副本: {text_upper}"
  );

  // REPLICAS 未知节点（合法 hex 但不在拓扑内）→ 空数组 *0\r\n
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "REPLICAS", &"f".repeat(32)])
    ),
    b"*0\r\n"
  );

  // REPLICAS 参数错误
  let err_no_arg = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "REPLICAS"]));
  assert!(err_no_arg.starts_with(b"-ERR wrong number of arguments"));
  let err_extra_arg = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "REPLICAS", NODE2_HEX, "extra"]),
  );
  assert!(err_extra_arg.starts_with(b"-ERR wrong number of arguments"));

  // FORGET 未知节点（合法 hex 但不在拓扑内）→ "I don't know about node"
  // 摘除段取 active_merge_lock 写锁（SuspendConfigMerge 挂起窗口）属异步
  // 域，同步段登记 SlowWait（cluster_session/basic.rs:network_cluster_forget）
  // 后由网络泵驱动产出应答——对标 C# RespClusterBasicCommands.cs:80-95
  // 先 ReleaseCurrentEpoch 再 TryRemoveWorker 的同一临界区：须走慢路径夹具
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "FORGET", &"a".repeat(32)])
    ),
    b"-ERR I don't know about node aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.\r\n"
  );
  // FORGET 自身 → 拒绝（C# ClusterManagerWorkerState.cs:62 取
  // CmdStrings.cs:42 ERR_GENERIC_CANNOT_FORGET_MYSELF）
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "FORGET", NODE1_HEX])
    ),
    b"-ERR I tried hard but I can't forget myself\r\n"
  );
}

/// CLUSTER SET-CONFIG-EPOCH：多节点拓扑拒绝指派（C# NumWorkers > 1 分支）
#[test]
fn cluster_setconfigepoch_rejected_on_multi_worker() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SET-CONFIG-EPOCH", "9"])
    ),
    b"-ERR The user can assign a config epoch only when the node does not know any other node\r\n"
  );
}

/// CLUSTER HELP：数组形态帮助文本（对标 ClusterCommandInfo.GetClusterCommands）
#[test]
fn cluster_help_array() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "HELP"]));
  let text = String::from_utf8_lossy(&out);
  assert!(
    text.starts_with("*64\r\n"),
    "应返回 64 行帮助: {}",
    &text[..40]
  );
  assert!(text.contains("+ADDSLOTS <slot> [<slot> ...]"), "{text}");
}

/// REPLICAOF NO ONE：副本翻主（保留数据）；REPLICAOF <addr> <port>：前台发起
/// attach，失败即回 -ERR 且复位回主（C# ReplicaOfCommand 同口径，绝不先回 OK）
#[test]
fn cluster_replicaof_semantics() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let m = cp.cluster_manager().unwrap();
  let mut consumer = cluster_consumer(&cp);

  // 主节点执行 REPLICAOF NO ONE → +OK（幂等，仍为主）
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["REPLICAOF", "NO", "ONE"])),
    b"+OK\r\n"
  );
  assert!(m.current_config().is_primary());

  // REPLICAOF 127.0.0.1 7001 → 未知端口/地址解析失败路径
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["REPLICAOF", "127.0.0.1", "ABC"])
    ),
    b"-ERR REPLICAOF failed to parse port 'ABC'\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["REPLICAOF", "10.0.0.1", "7000"])
    ),
    b"-ERR I don't know about node 10.0.0.1:7000.\r\n"
  );

  // 已知节点 → 前台发起 attach（C# BlockingWait 同口径，失败即回 -ERR
  // 绝不先回 OK）：本装配未接线本地 wal（recover_replication 前置），
  // attach 失败透传文案；AllowReplicaResetOnFailure 复位臂把节点翻回主
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["REPLICAOF", "127.0.0.1", "7001"]),
  );
  assert_eq!(
    out,
    format!(
      "-ERR replication recovery to {} skipped: local wal not wired\r\n",
      0xDE12
    )
    .into_bytes()
  );
  assert!(m.current_config().is_primary());

  // 失败复位后 REPLICAOF NO ONE → +OK 且仍为主（幂等）
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["REPLICAOF", "NO", "ONE"]),
  );
  assert_eq!(out, b"+OK\r\n");
  assert!(m.current_config().is_primary());
}

/// REPLICAOF NO ONE 升主收尾自愈面（zcode-r55-toposwitch 发现二）：恢复锁内
/// 先恢复 Primary 类后台任务、后释放恢复锁（C# ReplicaOfCommand.cs:50-56
/// StartPrimaryTasks 在 try 块内、EndRecovery 于 finally 最后；与 failover
/// 接管路径 take_over_as_primary_async 次序归一）。收尾终态：恢复态
/// NoRecovery、任务域角色位复位、GC 扫描运转
#[compio::test]
async fn cluster_replicaof_no_one_resumes_primary_tasks() {
  let cp = two_primary_provider();
  let m = cp.cluster_manager().unwrap();
  let rm = cp.replication_manager().unwrap();
  // GC 扫描开启（50ms 短间隔）：NO ONE 恢复臂 start_gc 按当前配置重拉
  let mut config = test_store_config();
  config.gc.enabled = true;
  config.gc.scan_interval_ms = 50;
  let (mut consumer, store) = cluster_store_consumer_with(&cp, config);
  cp.set_primary_tasks(Arc::new(PrimaryTasks::default()));

  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["REPLICAOF", "NO", "ONE"])),
    b"+OK\r\n"
  );
  assert!(m.current_config().is_primary(), "NO ONE 后应为配置主角色");
  assert!(!rm.is_recovering(), "NO ONE 收尾后恢复态必须为 NoRecovery");
  assert!(
    !cp.primary_tasks().unwrap().is_replica(),
    "NO ONE 后任务域角色位必须复位（Primary 类周期任务复跑）"
  );
  assert!(store.gc_running(), "NO ONE 后内置 GC 扫描必须恢复运转");
}

/// CLUSTER FAILOVER 的 AOF 前置门（对标 RespClusterFailoverCommands.cs:52-76）：
/// 未装配 AOF 的会话一律拒绝（门覆盖 FORCE 与 ABORT 臂），failover_manager 不进
/// 入 failover 状态；装配 AOF 后门放行，链路照常（主节点形态走既有 is_replica 拒绝臂）
#[test]
fn cluster_failover_requires_aof_gate() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let fm = cp.failover_manager().unwrap();

  // 未装配 AOF：FORCE 臂拒绝，failover_manager 状态不动（修复前穿透到
  // is_replica 拒绝臂，缺 AOF 副本可发起提升）
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "FAILOVER", "FORCE"]),
  );
  assert_eq!(out, err_frame(ERR_GENERIC_REPLICATION_AOF_TURNEDOFF));
  // ABORT 臂同被门覆盖（C# EnableAOF else 臂先于 ABORT 分支）
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "FAILOVER", "ABORT"]),
  );
  assert_eq!(out, err_frame(ERR_GENERIC_REPLICATION_AOF_TURNEDOFF));
  assert_eq!(fm.get_failover_status(), "no-failover");

  // 装配 AOF 后门放行：主节点形态走 is_replica 拒绝臂（行为与门前置形态同源）
  let dir = tempfile::tempdir().unwrap();
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    dir.path().join("aof.db"),
    None,
    RuntimeServerOptions::default(),
    {
      |network_sender_id, api| {
        Some(RespSessionConsumer::new(
          network_sender_id,
          RespServerSessionOptions::default(),
          Arc::new(api),
        ))
      }
    },
  )
  .unwrap();
  cp.set_aof(provider.aof().cloned());
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "FAILOVER", "FORCE"]),
  );
  assert_eq!(out, b"-ERR Node is not configured as a REPLICA\r\n");
}

/// CLUSTER MEET 端口 int32 值域门（对标 C# SessionParseState.TryGetInt）：
/// 超 int32 端口拒绝且不假成功 +OK（修复前 strict_i64 接收后 as i32 静默截断，
/// 4294967296 截为 0 假成功入队 meet）；错误文案对标 C#
/// CmdStrings.GenericErrInvalidPort（libs/cluster/CmdStrings.cs:87）
#[test]
fn cluster_meet_port_int32_range_rejected() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "MEET", "127.0.0.1", "4294967296"]),
  );
  assert_eq!(out, b"-ERR Invalid TCP base port specified: 4294967296\r\n");
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "MEET", "127.0.0.1", "2147483648"])
    ),
    b"-ERR Invalid TCP base port specified: 2147483648\r\n"
  );
  // 合法端口照常 +OK（后台 meet 连接失败仅 warn，不影响应答）
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "MEET", "127.0.0.1", "7001"])
    ),
    b"+OK\r\n"
  );
}

/// CLUSTER MEET 非法端口回显 ASCII 折叠锁（对标 C# parseState.GetString →
/// Encoding.ASCII.GetString：≥0x80 逐字节折 '?'）：`C3 A9`（é 的 UTF-8 双字节）
/// 回显 `??`、孤立 `0xE9` 回显 `?`——钉死折叠形，防 from_utf8_lossy 回潮
#[test]
fn cluster_meet_invalid_port_echo_ascii_fold() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame(&[b"CLUSTER", b"MEET", b"127.0.0.1", &[0xC3, 0xA9]])
    ),
    b"-ERR Invalid TCP base port specified: ??\r\n"
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame(&[b"CLUSTER", b"MEET", b"127.0.0.1", &[0xE9]])
    ),
    b"-ERR Invalid TCP base port specified: ?\r\n"
  );
}

/// CLUSTER ADDSLOTS 越界大数拒绝回归锚：slot 族 strict_i64 接收、超
/// [0,16383] 走 OutOfRange 应答的现状口径（登记见 doc/zh/deviations.md 13）
#[test]
fn cluster_addslots_out_of_range_anchor() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "ADDSLOTS", "3000000000"]),
  );
  assert_eq!(out, b"-ERR Slot out of range\r\n");
}

/// 顶层 FAILOVER：非主节点拒绝（C# Cannot failover a non-master node）+ 选项解析
#[test]
fn cluster_failover_top_level_semantics() {
  let cp = two_primary_provider();
  let m = cp.cluster_manager().unwrap();
  let mut consumer = cluster_consumer(&cp);

  // 翻为副本角色后 FAILOVER → 拒绝
  m.current_config
    .write()
    .make_replica_of(Some(0x0000_0000_0000_0000_0000_0000_0000_DE12));
  let out = roundtrip(&mut consumer, &resp_frame_str(&["FAILOVER"]));
  assert_eq!(out, b"-ERR Cannot failover a non-master node\r\n");

  // 非法选项 → 语法错误
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["FAILOVER", "BOGUS"])),
    err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR)
  );
}

/// CLUSTER GOSSIP WITHMEET：合并自身配置并回当前配置字节（WITHMEET 强制应答）
#[test]
fn cluster_gossip_withmeet_roundtrip() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let m = cp.cluster_manager().unwrap();
  let mut consumer = cluster_consumer(&cp);
  let payload = m.current_config().to_byte_array();

  // 配置载荷是二进制：逐字节组 RESP 帧，帧长发帧头按字节数计，不得经
  // char 有损转换（>0x7f 字节的 UTF-8 编码长度 ≠ 字节数，帧长即失真）
  let mut frame: Vec<u8> = Vec::from(b"*4\r\n");
  for part in [b"CLUSTER".as_slice(), b"GOSSIP", b"WITHMEET", &payload] {
    frame.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
    frame.extend_from_slice(part);
    frame.extend_from_slice(b"\r\n");
  }

  // 合并段取 active_merge_lock 读锁属异步域，同步段登记 SlowWait
  // （cluster_session/basic.rs:network_cluster_gossip）后由网络泵驱动产出
  // 应答——对标 C# RespClusterBasicCommands.cs:401-410 先 ReleaseCurrentEpoch
  // 再 TryMerge 的同一临界区：须走慢路径夹具
  let out = slow_roundtrip(&rt, &mut consumer, &frame);

  // WITHMEET 强制回当前配置字节（C# :422-427
  // TryWriteBulkString(current.ToByteArray())）：按 bulk string 帧解析，
  // 要求帧长自洽且载荷等于会话当前配置的序列化字节
  let replied = parse_bulk_frame(&out)
    .unwrap_or_else(|| panic!("WITHMEET 应答应为完整 bulk string 帧，实得 {out:?}"));
  assert!(
    !replied.is_empty(),
    "WITHMEET 应回非空配置载荷，实得 {} 字节",
    replied.len()
  );
  assert_eq!(replied, m.current_config().to_byte_array().as_slice());
}

/// 解析单条 RESP bulk string 应答帧（`$<len>\r\n<payload>\r\n`）取载荷：
/// 帧头长度与实到字节自洽、帧尾 CRLF 齐备，不接受整帧之外的多余字节
fn parse_bulk_frame(out: &[u8]) -> Option<&[u8]> {
  let (&tag, rest) = out.split_first()?;
  (tag == b'$').then_some(())?;
  let header_len = rest.windows(2).position(|w| w == b"\r\n")?;
  let len: usize = from_utf8(&rest[..header_len]).ok()?.parse().ok()?;
  let body = rest.get(header_len + 2..)?;
  let payload = body.get(..len)?;
  body
    .get(len..len.checked_add(2)?)
    .filter(|tail| *tail == b"\r\n")?;
  Some(payload)
}

/// 慢命令往返（同步段消费挂起 → block_on 驱动慢路径应答）
fn slow_roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frame_bytes: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame_bytes);
  assert_eq!(consumed, Some(0), "帧应被完整消费");
  let Some(slow) = c.take_slow_wait() else {
    return out;
  };
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  out
}

/// CLUSTER SLOTSTATE：槽位状态符号投影（STABLE "=" / MIGRATING ">" 等）
#[test]
fn cluster_slotstate_projection() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let m = cp.cluster_manager().unwrap();

  // 槽 5061 本地 STABLE（SLOTSTATE 为槽位直查，与键无关）→ "+5061 = node_1"
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SLOTSTATE", "5061"]),
  );
  assert_eq!(out, format!("+5061 = {NODE1_HEX}\r\n").into_bytes());

  // 置 MIGRATING 后 → "> node_1"（属主仍为源节点）
  {
    let mut config = m.current_config.write();
    config.slot_map[5061].state = SlotState::Migrating;
  }
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SLOTSTATE", "5061"]),
  );
  assert_eq!(out, format!("+5061 > {NODE1_HEX}\r\n").into_bytes());

  // 越界 → "ERR Slot out of range"
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SLOTSTATE", "16384"]),
  );
  assert_eq!(out, b"-ERR Slot out of range\r\n");
}

// ---------------------------------------------------------------------------
// 零依赖指令接线回归：CLUSTER RESERVE / ADVANCE_TIME / MLOG_KEY_TIME /
// APPENDLOG（AOF 门控关闭路径的报错口径）
// ---------------------------------------------------------------------------

/// CLUSTER RESERVE：VECTOR_SET_CONTEXTS 预保留上下文 → *n + 逐上下文
/// 十进制简单串；非法类型 / 非法计数按 C# 元数与文案口径拒绝
#[test]
fn cluster_reserve_contexts() {
  let dir = tempfile::tempdir().unwrap();
  // 小预算测试配置注入（生产缺省经 open_from_args 走 store_config() 的 StoreConfig::auto）
  let (_store, _broker, vector_manager) =
    open_node_with_config(test_store_config(), dir.path().join("reserve.db")).unwrap();
  let cp = two_primary_provider();
  cp.set_vector_manager(vector_manager);
  let mut consumer = cluster_consumer(&cp);

  let rt = Runtime::new().unwrap();

  // 预保留 3 个上下文 → *3 + 3 个十进制简单串
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "RESERVE", "VECTOR_SET_CONTEXTS", "3"]),
  );
  assert_eq!(out, b"*3\r\n+8\r\n+16\r\n+24\r\n");

  // 未识别保留类型 → "Unrecognized reservation type"
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "RESERVE", "STRING_CONTEXTS", "3"]),
  );
  assert_eq!(out, b"-Unrecognized reservation type\r\n");

  // 计数非正 → 元数错误
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "RESERVE", "VECTOR_SET_CONTEXTS", "0"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|reserve' command\r\n"
  );
}

/// CLUSTER RESERVE：向量管理器未装配 → 集群未初始化口径
#[test]
fn cluster_reserve_without_vector_manager_rejected() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "RESERVE", "VECTOR_SET_CONTEXTS", "2"]),
  );
  assert_eq!(out, b"-ERR Cluster not initialized\r\n");
}

/// CLUSTER ADVANCE_TIME：元数错误回显完整命令名；AOF 门控关闭（无重放
/// 驱动）按 C# 静默口径无应答
#[test]
fn cluster_advance_time_gate_and_silence() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "ADVANCE_TIME", "0"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|advance_time' command\r\n"
  );

  // 无重放驱动（AOF 未挂）→ C# GetReplayDriver?.SignalTimeAdvance 同口径
  // 静默无应答
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "ADVANCE_TIME", "0", "7"]),
  );
  assert_eq!(out, b"");
}

/// CLUSTER MLOG_KEY_TIME：AOF 门控关闭（单物理日志）→ C#
/// ERR_MULTI_LOG_DISABLED 同口径显式报错
#[test]
fn cluster_mlog_key_time_disabled_without_aof() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "MLOG_KEY_TIME", "key1"]),
  );
  assert_eq!(out, b"-ERR Multi-log disabled\r\n");
}

/// CLUSTER APPENDLOG：副本接收会话未注入（AOF 门控关闭）→ 集群未初始化口径
#[test]
fn cluster_appendlog_without_replica_session_rejected() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "APPENDLOG", NODE1_HEX, "0", "-1", "-1", "-1"]),
  );
  assert_eq!(out, b"-ERR Cluster not initialized\r\n");
}

/// AOF 门控点亮路径冒烟：open_with_config_and_aof 装配后 aof() 在场；单物理日志形态
/// 下 MLOG_KEY_TIME 仍按 C# 单日志部署口径回 ERR_MULTI_LOG_DISABLED
#[test]
fn cluster_mlog_key_time_with_aof_single_log() {
  let dir = tempfile::tempdir().unwrap();
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    dir.path().join("aof.db"),
    None,
    RuntimeServerOptions::default(),
    {
      |network_sender_id, api| {
        Some(RespSessionConsumer::new(
          network_sender_id,
          RespServerSessionOptions::default(),
          Arc::new(api),
        ))
      }
    },
  )
  .unwrap();
  assert!(provider.aof().is_some(), "AOF 门控点亮后门面在场");

  let cp = two_primary_provider();
  cp.set_aof(provider.aof().cloned());
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "MLOG_KEY_TIME", "key1"]),
  );
  assert_eq!(out, b"-ERR Multi-log disabled\r\n");
}

/// CLUSTER SETSLOT / SETSLOTSRANGE：拒绝 FAIL 状态，返回错误应答且不 panic
#[test]
fn cluster_setslot_and_setslotsrange_reject_fail_state() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  // CLUSTER SETSLOT <slot> FAIL <node_id>
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "FAIL", NODE2_HEX]),
  );
  assert_eq!(out, b"-ERR Slot state FAIL not supported.\r\n");

  // CLUSTER SETSLOTSRANGE FAIL <node_id> <slots...>
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&[
      "CLUSTER",
      "SETSLOTSRANGE",
      "FAIL",
      NODE2_HEX,
      "5061",
      "5062",
    ]),
  );
  assert_eq!(out, b"-ERR Invalid slot state\r\n");

  // 小写 fail 同样被拒绝且不 panic
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "fail", NODE2_HEX]),
  );
  assert_eq!(out, b"-ERR Slot state fail not supported.\r\n");

  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&[
      "CLUSTER",
      "SETSLOTSRANGE",
      "fail",
      NODE2_HEX,
      "5061",
      "5062",
    ]),
  );
  assert_eq!(out, b"-ERR Invalid slot state\r\n");
}

/// CLUSTER SETSLOTSRANGE：与 ADDSLOTS/DELSLOTS 族共用同一槽位解析器，
/// 判定序与文案同口径——非整数/奇数尾巴缺伴 → "ERR Invalid or out of range slot"、
/// 倒挂先于越界 → "ERR Invalid range <start> > <end>!"、越界 →
/// "ERR Slot out of range"、交叠 → "ERR Slot <n> specified multiple times"
#[test]
fn cluster_setslotsrange_slot_parse_errors() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOTSRANGE", "STABLE", "abc", "def"]),
  );
  assert_eq!(out, b"-ERR Invalid or out of range slot\r\n");

  // 倒挂且两端皆越界：仍按 C# 序报倒挂动态文案（越界在其后）
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOTSRANGE", "STABLE", "20000", "10000"]),
  );
  assert_eq!(out, b"-ERR Invalid range 20000 > 10000!\r\n");

  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOTSRANGE", "STABLE", "0", "16384"]),
  );
  assert_eq!(out, b"-ERR Slot out of range\r\n");

  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&[
      "CLUSTER",
      "SETSLOTSRANGE",
      "STABLE",
      "700",
      "701",
      "701",
      "702",
    ]),
  );
  assert_eq!(out, b"-ERR Slot 701 specified multiple times\r\n");

  // 奇数尾巴：残单参缺区间终点伴参 → 显式报错帧，不得静默截断回 +OK
  //（C# TryParseSlots 第二个 TryGetInt 失败 → ERR_INVALID_SLOT）
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOTSRANGE", "STABLE", "1", "5", "7"]),
  );
  assert_eq!(out, b"-ERR Invalid or out of range slot\r\n");

  // 偶数参正常路径回归：STABLE 200 201 → 重置槽状态回 +OK
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOTSRANGE", "STABLE", "200", "201"]),
  );
  assert_eq!(out, b"+OK\r\n");
}

/// 验证集群与顶层复制/故障转移命令在参数数量错误时输出的 GenericErrWrongNumArgs 包含正确的命令名称
/// 对标 C# ClusterSession.cs:119-121 (string.Format(CmdStrings.GenericErrWrongNumArgs, cmdName.ToLowerInvariant()))
#[test]
fn cluster_wrong_number_of_arguments_command_names() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  // CLUSTER ADVANCE_TIME: 预期 2 参，给出 0 或 1 参 → cluster|advance_time（非 advancetime）
  let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "ADVANCE_TIME"]));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|advance_time' command\r\n"
  );
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "ADVANCE_TIME", "0"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|advance_time' command\r\n"
  );

  // CLUSTER MLOG_KEY_TIME: 预期 1-2 参，给出 0 参或 3 参 → cluster|mlog_key_time（非 mlogkeytime）
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "MLOG_KEY_TIME"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|mlog_key_time' command\r\n"
  );
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "MLOG_KEY_TIME", "k1", "FRONTIER", "EXTRA"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|mlog_key_time' command\r\n"
  );

  // CLUSTER FAILOVER: 预期最多 2 参，给出 3 参 → cluster|failover（非顶层 failover）
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "FAILOVER", "FORCE", "10", "EXTRA"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|failover' command\r\n"
  );

  // CLUSTER GOSSIP: 预期 1-2 参，给出 0 参或 3 参 → cluster|gossip
  let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "GOSSIP"]));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|gossip' command\r\n"
  );
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "GOSSIP", "WITHMEET", "payload", "EXTRA"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|gossip' command\r\n"
  );

  // REPLICAOF: 预期恰好 2 参，给出 1 参 → replicaof
  let out = roundtrip(&mut consumer, &resp_frame_str(&["REPLICAOF", "127.0.0.1"]));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'replicaof' command\r\n"
  );

  // SECONDARYOF: 预期恰好 2 参，给出 1 参 → secondaryof（非 replicaof）
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["SECONDARYOF", "127.0.0.1"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'secondaryof' command\r\n"
  );

  // CLUSTER BUMPEPOCH: 预期恰好 0 参，给出 1 参 → cluster|bumpepoch
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "BUMPEPOCH", "EXTRA"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|bumpepoch' command\r\n"
  );

  // 零参 CLUSTER 子命令族（next 条2）：C# ClusterSession.cs:117-122 统一
  // parseState.Count != 0 → GenericErrWrongNumArgs，尾参必须拒绝不得静默吞
  for (sub, name) in [
    ("NODES", "cluster|nodes"),
    ("MYID", "cluster|myid"),
    ("SHARDS", "cluster|shards"),
    ("INFO", "cluster|info"),
    ("HELP", "cluster|help"),
    ("MYPARENTID", "cluster|myparentid"),
    ("SLOTS", "cluster|slots"),
    ("BANLIST", "cluster|banlist"),
    ("MTASKS", "cluster|mtasks"),
    ("FLUSHALL", "cluster|flushall"),
  ] {
    let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", sub, "EXTRA"]));
    assert_eq!(
      out,
      format!("-ERR wrong number of arguments for '{name}' command\r\n").into_bytes(),
      "零参子命令缺尾参门: {sub}"
    );
  }

  // ADDSLOTS/DELSLOTS 计数上界门（C# RespClusterSlotManagementCommands.cs
  // :25/:193 形态校验段 Count >= MAX_HASH_SLOT_VALUE → invalidParameters）
  let mut parts = vec!["CLUSTER", "ADDSLOTS"];
  parts.extend(vec!["0"; CLUSTER_SLOT_COUNT]);
  let out = roundtrip(&mut consumer, &resp_frame_str(&parts));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|addslots' command\r\n"
  );
  let mut parts = vec!["CLUSTER", "DELSLOTS"];
  parts.extend(vec!["0"; CLUSTER_SLOT_COUNT]);
  let out = roundtrip(&mut consumer, &resp_frame_str(&parts));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|delslots' command\r\n"
  );
}

/// total_cluster_commands_processed 计数口径（对位 C# 分派两级的出口归属：
/// 内层 CLUSTER_* switch 出口逐命令累加、顶层 MIGRATE / FAILOVER /
/// REPLICAOF / SECONDARYOF 走外层臂不经内层出口故不计数，CLUSTER 根命令
/// 两级不入亦不计数；参数错误应答不豁免，C# 内层出口在 switch 之后无条件累加）
#[test]
fn cluster_command_processed_metrics_scope() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let metrics = Arc::new(SessionMetricsHandle::default());
  consumer.attach_session_metrics(Some(Arc::clone(&metrics)));
  let counted = || metrics.snapshot().get_total_cluster_commands_processed();

  // 内层入口：命令臂与参数错误臂同入账
  assert_eq!(counted(), 0);
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "MYID"])),
    format!("$32\r\n{NODE1_HEX}\r\n").into_bytes()
  );
  assert_eq!(counted(), 1);
  roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "INFO"]));
  assert_eq!(counted(), 2);
  // CLUSTER GOSSIP 零参落参数错误臂：应答 wrong-num-args 照样入账
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "GOSSIP"])),
    b"-ERR wrong number of arguments for 'cluster|gossip' command\r\n"
  );
  assert_eq!(counted(), 3);

  // 外层三臂：顶层命令一律不计数（应答面不变）
  for (frame, expect) in [
    (
      resp_frame_str(&["MIGRATE", "127.0.0.1", "7001"]),
      b"-ERR wrong number of arguments for 'migrate' command\r\n".to_vec(),
    ),
    (
      resp_frame_str(&["REPLICAOF", "127.0.0.1"]),
      b"-ERR wrong number of arguments for 'replicaof' command\r\n".to_vec(),
    ),
    (
      resp_frame_str(&["SECONDARYOF", "127.0.0.1"]),
      b"-ERR wrong number of arguments for 'secondaryof' command\r\n".to_vec(),
    ),
    (
      resp_frame_str(&["FAILOVER", "BOGUS"]),
      b"-ERR syntax error\r\n".to_vec(),
    ),
  ] {
    assert_eq!(roundtrip(&mut consumer, &frame), expect);
  }
  assert_eq!(counted(), 3);

  // CLUSTER 根命令（无子命令）：解析层回显 wrong-num-args，命令不入外层三臂
  // 亦不入内层 switch，故不计数
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER"])),
    b"-ERR wrong number of arguments for 'CLUSTER' command\r\n"
  );
  assert_eq!(counted(), 3);
}

/// 验证 cluster_sub_name 匹配表与 wnode::resp::get_resp_command_name 的小写对标结果完全一致
#[test]
fn cluster_sub_name_aligns_with_resp_commands_info() {
  let test_commands = [
    (RespCommand::ClusterAdvanceTime, "cluster|advance_time"),
    (RespCommand::ClusterMlogKeyTime, "cluster|mlog_key_time"),
    (RespCommand::ClusterFailover, "cluster|failover"),
    (RespCommand::Failover, "failover"),
    (RespCommand::Replicaof, "replicaof"),
    (RespCommand::Secondaryof, "secondaryof"),
    (RespCommand::ClusterAddslots, "cluster|addslots"),
    (RespCommand::ClusterAddslotsrange, "cluster|addslotsrange"),
    (RespCommand::ClusterAppendlog, "cluster|appendlog"),
    (RespCommand::ClusterAttachSync, "cluster|attach_sync"),
    (RespCommand::ClusterBanlist, "cluster|banlist"),
    (RespCommand::ClusterBumpepoch, "cluster|bumpepoch"),
    (
      RespCommand::ClusterCountkeysinslot,
      "cluster|countkeysinslot",
    ),
    (RespCommand::ClusterDelkeysinslot, "cluster|delkeysinslot"),
    (
      RespCommand::ClusterDelkeysinslotrange,
      "cluster|delkeysinslotrange",
    ),
    (RespCommand::ClusterDelslots, "cluster|delslots"),
    (RespCommand::ClusterDelslotsrange, "cluster|delslotsrange"),
    (RespCommand::ClusterEndpoint, "cluster|endpoint"),
    (
      RespCommand::ClusterFailreplicationoffset,
      "cluster|failreplicationoffset",
    ),
    (RespCommand::ClusterFailstopwrites, "cluster|failstopwrites"),
    (RespCommand::ClusterFlushall, "cluster|flushall"),
    (RespCommand::ClusterFlushallNs, "cluster|flushall_ns"),
    (RespCommand::ClusterForget, "cluster|forget"),
    (RespCommand::ClusterGetkeysinslot, "cluster|getkeysinslot"),
    (RespCommand::ClusterGossip, "cluster|gossip"),
    (RespCommand::ClusterHelp, "cluster|help"),
    (RespCommand::ClusterInfo, "cluster|info"),
    (
      RespCommand::ClusterInitiateReplicaSync,
      "cluster|initiate_replica_sync",
    ),
    (RespCommand::ClusterKeyslot, "cluster|keyslot"),
    (RespCommand::ClusterMeet, "cluster|meet"),
    (RespCommand::ClusterMigrate, "cluster|migrate"),
    (RespCommand::ClusterMtasks, "cluster|mtasks"),
    (RespCommand::ClusterMyid, "cluster|myid"),
    (RespCommand::ClusterMyparentid, "cluster|myparentid"),
    (RespCommand::ClusterNodes, "cluster|nodes"),
    (RespCommand::ClusterPublish, "cluster|publish"),
    (RespCommand::ClusterReplicas, "cluster|replicas"),
    (RespCommand::ClusterReplicate, "cluster|replicate"),
    (RespCommand::ClusterReserve, "cluster|reserve"),
    (RespCommand::ClusterReset, "cluster|reset"),
    (
      RespCommand::ClusterSendCkptFileSegment,
      "cluster|send_ckpt_file_segment",
    ),
    (
      RespCommand::ClusterSendCkptMetadata,
      "cluster|send_ckpt_metadata",
    ),
    (
      RespCommand::ClusterSetconfigepoch,
      "cluster|set-config-epoch",
    ),
    (RespCommand::ClusterSetslot, "cluster|setslot"),
    (RespCommand::ClusterSetslotsrange, "cluster|setslotsrange"),
    (RespCommand::ClusterShards, "cluster|shards"),
    (RespCommand::ClusterSlots, "cluster|slots"),
    (RespCommand::ClusterSlotstate, "cluster|slotstate"),
    (RespCommand::ClusterSnapshotData, "cluster|snapshot_data"),
    (RespCommand::ClusterSpublish, "cluster|spublish"),
    (RespCommand::Migrate, "migrate"),
  ];

  for (cmd, expected) in test_commands {
    assert_eq!(
      cluster_sub_name(cmd),
      expected,
      "cluster_sub_name 与预期不一致: {cmd:?}"
    );
    let name_from_info = get_resp_command_name(cmd).to_ascii_lowercase();
    assert_eq!(
      name_from_info, expected,
      "wnode 元数据名与预期不一致: {cmd:?}"
    );
  }
}

/// 集群形态事务同槽成功执行：会话 (0,0) 库整批键共槽 SLOT0 归本地
/// （库级定槽，键内容不参与）→ EXEC 正常提交执行
#[test]
fn cluster_session_multi_exec_same_slot_success() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  let req = b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$2\r\nv1\r\n*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n*1\r\n$4\r\nEXEC\r\n";
  let out = roundtrip(&mut consumer, req);
  assert_eq!(
    out,
    b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n$2\r\nv1\r\n"
  );
}

/// 集群形态事务远端槽重定向：会话切远端库（库级定槽）→ EXEC 拦截返回 -MOVED
#[test]
fn cluster_session_multi_exec_remote_slot_moved() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer_remote_dbs(&cp);

  let out = roundtrip(&mut consumer, &select_frame(db_for_slot(REMOTE_SLOT)));
  assert_eq!(out, b"+OK\r\n");
  let req =
    b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$2\r\nv1\r\n*1\r\n$4\r\nEXEC\r\n";
  let out = roundtrip(&mut consumer, req);
  assert_eq!(
    out,
    format!("+OK\r\n+QUEUED\r\n-MOVED {REMOTE_SLOT} 127.0.0.1:7001\r\n").into_bytes()
  );
}

/// CLUSTER BUMPEPOCH 命令对齐 C#：
/// 1. 0 参时成功返回 +OK\r\n，且本地节点的 config_epoch 自增
/// 2. 参数非空时返回 wrong number of arguments
#[test]
fn cluster_bumpepoch_success_and_args_check() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  let initial_epoch = cp
    .cluster_manager()
    .unwrap()
    .current_config()
    .local_node_config_epoch();

  // 1. 正常 0 参调用 -> 返回 +OK\r\n
  let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "BUMPEPOCH"]));
  assert_eq!(out, b"+OK\r\n");

  // 验证 epoch 递增
  let new_epoch = cp
    .cluster_manager()
    .unwrap()
    .current_config()
    .local_node_config_epoch();
  assert!(new_epoch > initial_epoch);

  // 2. 带参数调用 -> 返回 wrong number of arguments
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "BUMPEPOCH", "extra"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|bumpepoch' command\r\n"
  );
}

/// CLUSTER ATTACH_SYNC 与 CLUSTER SYNC 参数校验测试：
/// 对标 C# NetworkClusterAttachSync/NetworkClusterSync 参数不足时返回 wrong number of arguments
#[test]
fn cluster_attach_sync_and_sync_explicit_reject() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "ATTACH_SYNC"]));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|attach_sync' command\r\n"
  );

  let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "SYNC"]));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|sync' command\r\n"
  );
}

/// CLUSTER PUBLISH 接收面测试：
/// 1. broker 缺席时回 -ERR PUBLISH is disabled, enable it with --pubsub option.
/// 2. 参数不足或过多时回 wrong number of arguments
/// 3. broker 在场时本地投递且无应答写出（对标 C# NetworkClusterPublish）
#[test]
fn cluster_publish_disabled_and_args_check() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  // 1. 未注入 broker 时，回 wresp::cmd_strings 禁用模板回填 PUBLISH 的错误帧
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "PUBLISH", "ch", "msg"]),
  );
  assert_eq!(
    out,
    b"-ERR PUBLISH is disabled, enable it with --pubsub option.\r\n"
  );

  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SPUBLISH", "ch", "msg"]),
  );
  assert_eq!(
    out,
    b"-ERR PUBLISH is disabled, enable it with --pubsub option.\r\n"
  );

  // 2. 参数不足或超限 -> wrong number of arguments
  let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "PUBLISH"]));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|publish' command\r\n"
  );

  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "PUBLISH", "ch"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|publish' command\r\n"
  );

  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "PUBLISH", "ch", "msg", "extra"]),
  );
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|publish' command\r\n"
  );
}

#[test]
fn cluster_publish_local_delivery_success() {
  let cp = two_primary_provider();
  let broker = Arc::new(SubscribeBroker::new());
  cp.set_pubsub(Some(Arc::clone(&broker)));

  let mut sub_consumer = cluster_consumer(&cp);
  sub_consumer.attach_pubsub(Arc::clone(&broker));

  // 订阅者会话订阅通道 "chan1"
  let out = roundtrip(&mut sub_consumer, &resp_frame_str(&["SUBSCRIBE", "chan1"]));
  assert_eq!(out, b"*3\r\n$9\r\nsubscribe\r\n$5\r\nchan1\r\n:1\r\n");

  // 集群对端会话发送 CLUSTER PUBLISH 0:chan1 msg1 -> 本地投递，C# 无应答写出
  //（out 应为空）。广播面通道即 ns 隔离键（发布端 network_publish 折叠后原样
  // 透传，收端以该键直入本地 broker，租户分区随键跨节点贯通）
  let mut peer_consumer = cluster_consumer(&cp);
  peer_consumer.attach_pubsub(Arc::clone(&broker));
  let out = roundtrip(
    &mut peer_consumer,
    &resp_frame_str(&["CLUSTER", "PUBLISH", "0:chan1", "msg1"]),
  );
  assert!(out.is_empty(), "CLUSTER PUBLISH 成功时无应答写出");

  // 直投无积压：roundtrip 返回即已分发至订阅者邮箱，无需任何后台驱动

  // 验证订阅者邮箱已收到广播消息，推送帧通道名剥离隔离前缀还原裸通道
  let mut push_frames = Vec::new();
  sub_consumer.drain_pubsub_into(&mut push_frames);
  assert!(!push_frames.is_empty(), "订阅者会话应收到推送消息帧");
  let push_str = String::from_utf8_lossy(&push_frames);
  assert!(push_str.contains("$5\r\nchan1\r\n"));
  assert!(push_str.contains("msg1"));
}

/// 跨节点 SPUBLISH 接收面路由（分片定向，对标 C# 前提差异）：
/// 同节点 broker 上 SSUBSCRIBE ch（分片订阅者）与 SUBSCRIBE ch（普通订阅者）
/// 并存，集群对端先后发 CLUSTER SPUBLISH / CLUSTER PUBLISH——
/// 分片帧仅达分片订阅者（smessage），普通帧仅达普通订阅者（message），
/// 双向断言同名频道不串台（C# 单订阅图无此区分，rust 三表分域接收端分派）
#[test]
fn cluster_spublish_delivers_shard_domain_without_crosstalk() {
  let cp = two_primary_provider();
  let broker = Arc::new(SubscribeBroker::new());
  cp.set_pubsub(Some(Arc::clone(&broker)));

  // 分片订阅者（A 节点 SSUBSCRIBE ch）
  let mut shard_consumer = cluster_consumer(&cp);
  shard_consumer.attach_pubsub(Arc::clone(&broker));
  let out = roundtrip(&mut shard_consumer, &resp_frame_str(&["SSUBSCRIBE", "ch"]));
  assert_eq!(out, b"*3\r\n$10\r\nssubscribe\r\n$2\r\nch\r\n:1\r\n");

  // 普通订阅者（对照，同名频道 SUBSCRIBE ch）
  let mut std_consumer = cluster_consumer(&cp);
  std_consumer.attach_pubsub(Arc::clone(&broker));
  let out = roundtrip(&mut std_consumer, &resp_frame_str(&["SUBSCRIBE", "ch"]));
  assert_eq!(out, b"*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n");

  // 集群对端会话发 CLUSTER SPUBLISH 0:ch sm1（B 节点 SPUBLISH 跨节点到达形态）
  let mut peer = cluster_consumer(&cp);
  peer.attach_pubsub(Arc::clone(&broker));
  let out = roundtrip(
    &mut peer,
    &resp_frame_str(&["CLUSTER", "SPUBLISH", "0:ch", "sm1"]),
  );
  assert!(out.is_empty(), "CLUSTER SPUBLISH 成功时无应答写出");
  // 直投无积压：仅分片订阅者被通知

  // 分片订阅者收 smessage（通道名剥隔离前缀还原裸名），普通订阅者零接收
  let mut shard_frames = Vec::new();
  shard_consumer.drain_pubsub_into(&mut shard_frames);
  assert_eq!(
    shard_frames.as_slice(),
    b"*3\r\n$8\r\nsmessage\r\n$2\r\nch\r\n$3\r\nsm1\r\n",
    "分片订阅者应收到 smessage 推送帧"
  );
  let mut std_frames = Vec::new();
  std_consumer.drain_pubsub_into(&mut std_frames);
  assert!(std_frames.is_empty(), "普通订阅者不得串台接收分片消息");

  // 反向：CLUSTER PUBLISH 0:ch pm1 仅达普通订阅者
  let out = roundtrip(
    &mut peer,
    &resp_frame_str(&["CLUSTER", "PUBLISH", "0:ch", "pm1"]),
  );
  assert!(out.is_empty());
  // 直投无积压：仅普通订阅者被通知

  let mut std_frames = Vec::new();
  std_consumer.drain_pubsub_into(&mut std_frames);
  assert_eq!(
    std_frames.as_slice(),
    b"*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$3\r\npm1\r\n",
    "普通订阅者应收到 message 推送帧"
  );
  let mut shard_frames = Vec::new();
  shard_consumer.drain_pubsub_into(&mut shard_frames);
  assert!(shard_frames.is_empty(), "分片订阅者不得串台接收普通消息");
}

/// 自定义命令（扩展 R.* 族）集群槽位校验：会话库槽位不属本节点回 MOVED，
/// 本节点库真实执行。对标 C# RespServerSessionSlotVerify.cs:34-79
/// CanServeSlotForCustomCommand —— CustomObjCmd（编译期静态清单扩展命令，动态注册
/// 哨兵 CustomRawStringCmd/CustomTxn/CustomProcedure 已按转写规范删除）虽非 data
/// command，仍经 CustomCommandSingleKeySpec 走 NetworkMultiKeySlotVerify
/// （库级定槽下槽位取会话 (ns, active_db)，键名不参与定槽），防自定义命令
/// 静默写入错误节点

#[test]
fn cluster_custom_command_slot_verify_moved_and_serve() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer_remote_dbs(&cp);

  // 会话切远端槽位库 → R.SETBIT / R.GETBIT 应回 MOVED <远端槽> 127.0.0.1:7001
  let out = roundtrip(&mut consumer, &select_frame(db_for_slot(REMOTE_SLOT)));
  assert_eq!(out, b"+OK\r\n");
  let remote_key = String::from("cc_remote0");
  let moved = format!("-MOVED {REMOTE_SLOT} 127.0.0.1:7001\r\n");
  let set_frame = format!(
    "*4\r\n$8\r\nR.SETBIT\r\n${}\r\n{remote_key}\r\n$2\r\n42\r\n$1\r\n1\r\n",
    remote_key.len()
  );
  assert_eq!(
    roundtrip(&mut consumer, set_frame.as_bytes()),
    moved.as_bytes(),
    "远端库 R.SETBIT 应回 MOVED"
  );

  // 只读自定义命令同样重定向（read_only 按 CommandType.Read 判定）
  let get_frame = format!(
    "*3\r\n$8\r\nR.GETBIT\r\n${}\r\n{remote_key}\r\n$2\r\n42\r\n",
    remote_key.len()
  );
  assert_eq!(
    roundtrip(&mut consumer, get_frame.as_bytes()),
    moved.as_bytes(),
    "远端库 R.GETBIT 应回 MOVED"
  );

  // 会话回本地库 → 正常执行：置位返回旧位 :0，读回 :1
  let out = roundtrip(&mut consumer, &select_frame(0));
  assert_eq!(out, b"+OK\r\n");
  let local_key = String::from("cc_local0");
  let set_frame = format!(
    "*4\r\n$8\r\nR.SETBIT\r\n${}\r\n{local_key}\r\n$2\r\n42\r\n$1\r\n1\r\n",
    local_key.len()
  );
  assert_eq!(
    roundtrip(&mut consumer, set_frame.as_bytes()),
    b":0\r\n",
    "本地库 R.SETBIT 应真实执行"
  );
  let get_frame = format!(
    "*3\r\n$8\r\nR.GETBIT\r\n${}\r\n{local_key}\r\n$2\r\n42\r\n",
    local_key.len()
  );
  assert_eq!(
    roundtrip(&mut consumer, get_frame.as_bytes()),
    b":1\r\n",
    "本地库 R.GETBIT 应读到置位结果"
  );
}

/// 负槽位及越界槽位请求必须返回 ERR Slot out of range
#[test]
fn cluster_negative_and_out_of_range_slots_rejected() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  let err_out_of_range = b"-ERR Slot out of range\r\n";

  // COUNTKEYSINSLOT 负槽位与超限槽位
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", "-1"])
    ),
    err_out_of_range
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", "16384"])
    ),
    err_out_of_range
  );

  // GETKEYSINSLOT 负槽位与超限槽位
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "GETKEYSINSLOT", "-1", "10"])
    ),
    err_out_of_range
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "GETKEYSINSLOT", "16384", "10"])
    ),
    err_out_of_range
  );

  // SETSLOT 负槽位与超限槽位
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", "-1", "STABLE"])
    ),
    err_out_of_range
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", "16384", "STABLE"])
    ),
    err_out_of_range
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SETSLOT", "-1", "MIGRATING", NODE2_HEX])
    ),
    err_out_of_range
  );

  // SLOTSTATE 负槽位与超限槽位
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SLOTSTATE", "-1"])
    ),
    err_out_of_range
  );
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "SLOTSTATE", "16384"])
    ),
    err_out_of_range
  );

  // ADDSLOTS 负槽位
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "ADDSLOTS", "-1"])
    ),
    err_out_of_range
  );

  // DELKEYSINSLOT 负槽位
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "DELKEYSINSLOT", "-1"])
    ),
    err_out_of_range
  );
}

#[test]
fn cluster_reset_hard_and_delkeysinslot_non_default_domain_types() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let store = reset_store("reset_domain_types.db");
  cp.set_store(Arc::clone(&store));
  let mut consumer = reset_consumer(&cp, &store);

  let target_slot = slot_of(0, 1);

  // 1. SELECT 非默认 db
  let (_consumed, out) = pump(&mut consumer, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n");
  assert_eq!(out, b"+OK\r\n");

  // 写一个 String 键
  let (_consumed, out) = pump(
    &mut consumer,
    b"*3\r\n$3\r\nSET\r\n$3\r\nhk1\r\n$1\r\nv\r\n",
  );
  assert_eq!(out, b"+OK\r\n");

  // 写一个 Hash 键
  let (_consumed, out) = pump(
    &mut consumer,
    b"*4\r\n$4\r\nHSET\r\n$4\r\nhash\r\n$2\r\nf1\r\n$2\r\nv1\r\n",
  );
  assert_eq!(out, b":1\r\n");

  // 写一个 ZSet 键
  let (_consumed, out) = pump(
    &mut consumer,
    b"*4\r\n$4\r\nZADD\r\n$4\r\nzset\r\n$1\r\n1\r\n$2\r\nm1\r\n",
  );
  assert_eq!(out, b":1\r\n");

  // CLUSTER RESET SOFT 应该因有键被拒绝
  let (_consumed, out) = pump(
    &mut consumer,
    b"*3\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n$4\r\nSOFT\r\n",
  );
  let slow = consumer.take_slow_wait().expect("RESET 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(
    out,
    b"-ERR CLUSTER RESET can't be called with master nodes containing keys\r\n"
  );

  // CLUSTER RESET HARD 应该因为持键报错，但依旧会清理全域
  let (_consumed, out) = pump(
    &mut consumer,
    b"*3\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n$4\r\nHARD\r\n",
  );
  let slow = consumer.take_slow_wait().expect("RESET 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(
    out,
    b"-ERR CLUSTER RESET can't be called with master nodes containing keys\r\n"
  );

  // DBSIZE 应为 0
  let (_consumed, out) = pump(&mut consumer, b"*1\r\n$6\r\nDBSIZE\r\n");
  let slow = consumer.take_slow_wait().expect("DBSIZE 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b":0\r\n");

  // =========================
  // 再写一遍用来测 DELKEYSINSLOT
  // =========================
  pump(
    &mut consumer,
    b"*3\r\n$3\r\nSET\r\n$3\r\nhk2\r\n$1\r\nv\r\n",
  );
  pump(
    &mut consumer,
    b"*4\r\n$4\r\nHSET\r\n$4\r\nhsh2\r\n$2\r\nf1\r\n$2\r\nv1\r\n",
  );

  // 此时 db 1 (slot: target_slot) 有 2 个键
  let slot_str = target_slot.to_string();
  let frame = format!(
    "*3\r\n$7\r\nCLUSTER\r\n$13\r\nDELKEYSINSLOT\r\n${}\r\n{}\r\n",
    slot_str.len(),
    slot_str
  );

  let (_consumed, out) = pump(&mut consumer, frame.as_bytes());
  let slow = consumer
    .take_slow_wait()
    .expect("DELKEYSINSLOT 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b"+OK\r\n");

  // 验证键被清理
  let (_consumed, out) = pump(&mut consumer, b"*1\r\n$6\r\nDBSIZE\r\n");
  let slow = consumer.take_slow_wait().expect("DBSIZE 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b":0\r\n");
}

/// CLUSTER COUNTKEYSINSLOT 前导零文法锁（工单 zcode-r31-intparse / doc/zh/deviations.md §32b）
///
/// C# TryGetInt 死参导致放行 007 为槽 7，Rust 统一 strict_i64 严格收口，
/// 007 拒收恒回 Invalid or out of range slot 错误帧；对照组 "7" 正常返回槽内键计数 :0
#[test]
fn cluster_countkeysinslot_grammar_lock() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let (mut consumer, _store) = cluster_store_consumer(&cp);

  let err_invalid_slot = b"-ERR Invalid or out of range slot\r\n";
  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", "007"])
    ),
    err_invalid_slot,
    "CLUSTER COUNTKEYSINSLOT 007 必须恒回 invalid slot 错误帧"
  );

  assert_eq!(
    slow_roundtrip(
      &rt,
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", "7"])
    ),
    b":0\r\n",
    "对照组 CLUSTER COUNTKEYSINSLOT 7 必须成功返回计数"
  );
}

/// INFO STATS gossip 11 行名序（C# ClusterProvider.cs:313-330 GetGossipStats 逐名同序）
const GOSSIP_STATS_ROW_NAMES: [&str; 11] = [
  "meet_requests_recv",
  "meet_requests_succeed",
  "meet_requests_failed",
  "gossip_success_count",
  "gossip_failed_count",
  "gossip_timeout_count",
  "gossip_full_send",
  "gossip_empty_send",
  "gossip_bytes_send",
  "gossip_bytes_recv",
  "gossip_open_connections",
];

/// INFO STATS 往返并剥出 RESP2 bulk 载荷（同步段直返与慢路径挂起两态同径）
fn info_stats_body(rt: &Runtime, consumer: &mut RespSessionConsumer) -> String {
  let out = slow_roundtrip(rt, consumer, &resp_frame_str(&["INFO", "STATS"]));
  let text = from_utf8(&out).expect("INFO 应答应为 UTF-8 文本");
  let (len_s, rest) = text
    .strip_prefix('$')
    .unwrap_or_else(|| panic!("非 RESP2 bulk 帧: {out:?}"))
    .split_once("\r\n")
    .expect("bulk 帧头");
  let body = rest.strip_suffix("\r\n").expect("bulk 帧尾");
  assert_eq!(
    len_s.parse::<usize>().unwrap(),
    body.len(),
    "bulk 长度字节与载荷实长不符"
  );
  body.to_string()
}

/// INFO STATS 集群缺省形态（三 metrics 开关全关，监视器缺席）gossip 11 行整段在场：
/// C# PopulateStatsInfo 的 clusterEnabled 单门无条件并入，metricsDisabled 仅逐行
/// 折零不折行（GarnetInfoMetrics.cs:212-219 + ClusterProvider.cs:313-330），缺省
/// 配置集群节点应答 30 行（19 基础零值 + 11 gossip 零值，对标
/// test/standalone/Garnet.test/RespInfoTests.cs 的会话级 INFO 形态）；监视器在位
/// 形态 gossip 行如实读数，既有真实值行路径回归不破
#[test]
fn cluster_info_stats_gossip_rows_without_monitor() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  // 集群开启（C# serverOptions.EnableCluster = true；监视器三开关全关：
  // MetricsSamplingFrequency 缺省 0、CommandStatsMonitor/LatencyMonitor 缺省 false）
  consumer
    .session_mut()
    .set_runtime_config(Arc::new(RuntimeServerConfig::new(RuntimeServerOptions {
      enable_cluster: true,
      ..Default::default()
    })));

  // gossip 真实计数置位：缺席形态须逐行折 "0"，在位形态须如实读数
  cp.gossip_manager()
    .expect("集群提供者应装配 gossip 管理器")
    .stats
    .update_meet_requests_recv();

  // 段一：监视器缺席（进程级监视器槽未装）——11 行 gossip 零值行逐名在场
  let body = info_stats_body(&rt, &mut consumer);
  for name in GOSSIP_STATS_ROW_NAMES {
    let row = format!("\r\n{name}:0\r\n");
    assert!(
      body.contains(&row),
      "监视器缺席形态缺 gossip 零值行 {name}: {body}"
    );
  }
  // 段体 30 行：19 基础 + 11 gossip（段头一行不计）
  let lines = body.split("\r\n").filter(|l| !l.is_empty()).count() - 1;
  assert_eq!(lines, 30, "STATS 段行数应为 19 基础 + 11 gossip: {body}");

  // 段二：监视器在位（进程级槽首装即赢）——metrics_disabled 判据随监视器快照
  // 在场翻转，gossip 行如实读数（GossipStats 真实计数直出）
  let monitor = Arc::new(GarnetServerMonitor::new(1, true, false, false));
  assert!(monitor.install_global(), "测试进程监视器槽应首次安装成功");
  let body = info_stats_body(&rt, &mut consumer);
  assert!(
    body.contains("\r\nmeet_requests_recv:1\r\n"),
    "监视器在位形态 gossip 行应如实读数: {body}"
  );
}
