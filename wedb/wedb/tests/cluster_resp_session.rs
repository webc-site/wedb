//! 集群会话切面集成测试：RespServerSession + ClusterSession + StoreGarnetApi
//! 装配形态下的槽位验证、MOVED/CROSSSLOT 重定向、本地槽真执行、CLUSTER
//! 命令族与 ROLE 集群分支，对标 garnet/test/cluster 会话级用例
//! （RespRoundTrip / ClusterManagementTests）
use std::sync::Arc;

use compio::runtime::Runtime;
use waof::FIRST_VALID_AOF_ADDRESS;
use wbase::hash_slot::hash_slot as cluster_slot;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  cluster_session::{ClusterSession, cluster_sub_name},
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wedb_test::{SilentNode, test_store_config};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::StoreGarnetApi, get_resp_command_name,
    resp_server_session::RespServerSessionOptions,
  },
  service::{StorageSessionProvider, open_node_with_config},
};
use wresp::RespCommand;
use wtxn::WatchVersionMap;

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
      node_id: "node_1",
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
    let remote_worker_id = config.workers.len() as u16;
    config.workers.push(Worker {
      nodeid: Some("node_2".to_string()),
      address: "127.0.0.1".to_string(),
      port: 7001,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
    for slot in 0..8192 {
      config.slot_map[slot] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
    for slot in 8192..16384 {
      config.slot_map[slot] = HashSlot {
        worker_id: remote_worker_id,
        state: SlotState::Stable,
      };
    }
  }
  cp
}

/// 构造挂接集群切面 + 存储执行域的会话消费者（单机与集群同一执行路径）
///
/// 存储同步注入集群提供者（槽位校验 exists 探测与命令执行同源，对标
/// C# clusterProvider.storeWrapper 单一存储面）
fn cluster_consumer(cp: &ClusterProvider) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("gate.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster_session(
    1,
    RespServerSessionOptions {
      max_databases: 2,
      ..RespServerSessionOptions::default()
    },
    cluster_session,
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)));
  consumer
}

/// 单命令往返（单帧完整到达）
fn roundtrip(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(consumer, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  out
}

/// 集群形态下基础命令与会话命令族
/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
/// 返回 (消费后残余, 应答)：Some(0) = 完整消费，None = 协议违规
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  (remaining, resp)
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
  assert_eq!(out, b"$6\r\nnode_1\r\n");

  // CLUSTER KEYSLOT foo → 12182
  let out = roundtrip(
    &mut consumer,
    b"*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$3\r\nfoo\r\n",
  );
  assert_eq!(out, b":12182\r\n");
}

/// ROLE 集群主节点分支：*3 master :offset *0（无挂载副本；offset 为 rm 初始
/// 复制位点 FIRST_VALID_AOF_ADDRESS）
#[test]
fn cluster_session_role_primary() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(&mut consumer, b"*1\r\n$4\r\nROLE\r\n");
  let expect = format!(
    "*3\r\n$6\r\nmaster\r\n:{}\r\n*0\r\n",
    FIRST_VALID_AOF_ADDRESS
  );
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
    .make_replica_of(Some("node_2"));
  let mut consumer = cluster_consumer(&cp);
  let out = roundtrip(&mut consumer, b"*1\r\n$4\r\nROLE\r\n");
  let expect = format!(
    "*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:7001\r\n$7\r\nconnect\r\n:{}\r\n",
    FIRST_VALID_AOF_ADDRESS
  );
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
    .make_replica_of(Some("node_2"));
  let mut consumer = cluster_consumer(&cp);

  let expect: &[u8] = b"-ERR You can't write against a read only replica.\r\n";
  // 同步门：错误即回，无慢路径挂起，库不清
  let out = roundtrip(&mut consumer, b"*1\r\n$7\r\nFLUSHDB\r\n");
  assert_eq!(out, expect);
  let out = roundtrip(&mut consumer, b"*1\r\n$8\r\nFLUSHALL\r\n");
  assert_eq!(out, expect);
}

/// 内部写会话豁免：副本角色 + internal_write 置位（C# AofProcessor 回放会话
/// 形态，ReplicaDisklessSync.cs:248 / ReplicaDiskbasedSync.cs:349 置位链）→
/// FLUSHDB 真清库；键写入经槽位验证 internalWriteSession 豁免副本重定向
///（ClusterSlotVerify.cs:82）
#[test]
fn flush_replica_internal_write_session_executes() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  cp.cluster_manager()
    .unwrap()
    .current_config
    .write()
    .make_replica_of(Some("node_2"));
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  cluster_session.set_internal_write(true);
  let store = reset_store("flush-replica.db");
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster_session(
    1,
    RespServerSessionOptions {
      max_databases: 2,
      ..RespServerSessionOptions::default()
    },
    cluster_session,
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );

  // 本地槽键写入（内部写会话豁免重定向，同主节点写入形态）
  let key = (0u32..)
    .map(|i| format!("fw{i}"))
    .find(|k| cluster_slot(k.as_bytes()) < 8192)
    .unwrap();
  let set_frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len());
  assert_eq!(roundtrip(&mut consumer, set_frame.as_bytes()), b"+OK\r\n");

  // FLUSHDB → 慢路径 +OK（门放行，库已清）
  let (consumed, out) = pump(&mut consumer, b"*1\r\n$7\r\nFLUSHDB\r\n");
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("FLUSHDB 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b"+OK\r\n");

  // 清库生效：DBSIZE → :0
  let (consumed, out) = pump(&mut consumer, b"*1\r\n$6\r\nDBSIZE\r\n");
  assert_eq!(consumed, Some(0));
  let slow = consumer.take_slow_wait().expect("DBSIZE 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b":0\r\n");
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
    local.starts_with("node_1 127.0.0.1:7000@17000 "),
    "本地行首字段应为 nodeid + 地址@总线端口: {local}"
  );
  assert!(
    local.contains("myself,master - ") && local.contains(" 1 connected "),
    "本地行应含 myself,master、主 id '-'、config-epoch 1、connected: {local}"
  );
  assert!(
    remote.starts_with("node_2 127.0.0.1:7001@17001 "),
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
#[test]
fn cluster_session_cluster_shards() {
  Runtime::new().unwrap().block_on(async {
    let cp = two_primary_provider();
    let mut consumer = cluster_consumer(&cp);

    // 1. 初始状态：未建立远端 gossip 连接
    // node_1 (local): health 应为 online
    // node_2 (remote): health 应为 offline
    let out = roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "SHARDS"]));
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("*2\r\n"), "双主拓扑应有 2 个分片: {text}");
    assert!(text.contains("node_1"), "分片应包含 node_1: {text}");
    assert!(text.contains("node_2"), "分片应包含 node_2: {text}");
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
    let conn = gm
      .connection_store
      .get_or_add("node_2", "127.0.0.1", fake.port() as i32);
    conn.initialize_async().await;
    assert!(
      cp.get_connection_info("node_2").connected,
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
      !cp.get_connection_info("node_2").connected,
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
  });
}

/// 远端槽位数据命令 → -MOVED 重定向（对标 CanServeSlot → NetworkMultiKeySlotVerify）
#[test]
fn cluster_session_moved_redirect() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  // "foo" 哈希槽 12182 ∈ node_2 范围
  assert_eq!(cluster_slot(b"foo"), 12182);
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
  assert_eq!(out, b"-MOVED 12182 127.0.0.1:7001\r\n");
}

/// 跨槽位多键命令 → -CROSSSLOT（首键之后槽位不一致）
#[test]
fn cluster_session_crossslot() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  // foo(12182) 与 bar(5061) 不同槽位
  let out = roundtrip(
    &mut consumer,
    b"*3\r\n$4\r\nMGET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
  );
  assert_eq!(
    out,
    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n"
  );
}

/// 本地槽位数据命令过门后真正执行（对标 CanServeSlot 放行 → ProcessBasicCommands
/// → IGarnetApi 存储执行；与 MOVED 重定向同一条会话路径）
#[test]
fn cluster_session_local_slot_executes() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  assert_eq!(cluster_slot(b"bar"), 5061);

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

/// 同帧管道：远端槽 MOVED 与本地槽真执行混合消费（游标推进正确性）
#[test]
fn cluster_session_pipeline_mixed_local_remote() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let frame = b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
  let (consumed, out) = pump(&mut consumer, frame);
  assert_eq!(consumed, Some(0));
  // bar(5061) 本地 → nil（未写过）；foo(12182) 远端 → MOVED
  assert_eq!(out, b"$-1\r\n-MOVED 12182 127.0.0.1:7001\r\n");
}

/// ASKING 后导入态槽位放行（对标 SingleKeyReadWriteSlotVerify IMPORTING + SessionAsking）
#[test]
fn cluster_session_asking_importing_slot() {
  let cp = two_primary_provider();
  let m = cp.cluster_manager().unwrap();
  {
    let mut config = m.current_config.write();
    // node_2 已由 two_primary_provider 注册为槽位属主；改其 5061 槽为导入态
    let node2_worker_id = config
      .workers
      .iter()
      .position(|w| w.nodeid.as_deref() == Some("node_2"))
      .expect("node_2 应已注册") as u16;
    config.slot_map[cluster_slot(b"bar") as usize] = HashSlot {
      worker_id: node2_worker_id,
      state: SlotState::Importing,
    };
  }
  let mut consumer = cluster_consumer(&cp);

  // 无 ASKING → MOVED 至源节点
  let out = roundtrip(&mut consumer, b"*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n");
  assert_eq!(out, b"-MOVED 5061 127.0.0.1:7001\r\n");

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

/// 单机形态（未挂切面）CLUSTER MYID → 集群支持未启用错误
#[test]
fn standalone_session_cluster_disabled() {
  use wnode::resp::{garnet_api::GarnetApiFace, resp_server_session::RespServerSession};

  /// 桩存储执行域：CLUSTER MYID 在会话侧拦截，不应到达存储执行域
  struct UnreachableApi;
  impl GarnetApiFace for UnreachableApi {
    fn exec(&self, _session: &mut RespServerSession, cmd: wresp::RespCommand, _args: &[&[u8]]) {
      panic!("会话侧命令不应进入存储执行域: {cmd:?}");
    }

    async fn exec_slow(&self, _cmd: wresp::RespCommand, _args: Vec<Vec<u8>>) -> Vec<u8> {
      panic!("会话侧命令不应进入慢路径执行域")
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
  RespSessionConsumer::with_cluster_session(
    1,
    RespServerSessionOptions {
      max_databases: 2,
      ..RespServerSessionOptions::default()
    },
    cluster_session,
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 打开 RESET 测试专用存储（GC 关闭）
fn reset_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
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
  assert_eq!(out, b"$6\r\nnode_1\r\n");
}

/// CLUSTER RESET 慢路径：本节点槽上有键时拒绝（HasKeysInSlots 判定）
#[test]
fn cluster_reset_with_local_slot_keys_rejected() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let store = reset_store("reset2.db");
  cp.set_store(Arc::clone(&store));
  let mut consumer = reset_consumer(&cp, &store);

  // 找一个落在本地槽（0..8192）的键写入
  let key = (0u32..)
    .map(|i| format!("rk{i}"))
    .find(|k| cluster_slot(k.as_bytes()) < 8192)
    .unwrap();
  let frame = format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len());
  let (consumed, out) = pump(&mut consumer, frame.as_bytes());
  assert_eq!(consumed, Some(0));
  assert_eq!(out, b"+OK\r\n");

  // CLUSTER RESET → 槽键在场拒绝
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

/// CLUSTER RESET HARD：清空全部用户键（C# `!soft → FlushDB(true)`）
#[test]
fn cluster_reset_hard_flushes_keys() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let store = reset_store("reset3.db");
  cp.set_store(Arc::clone(&store));
  let mut consumer = reset_consumer(&cp, &store);

  // 写入一个本地槽键（槽位归属判定需键 hash 进 0..8192）
  let key = (0u32..)
    .map(|i| format!("hk{i}"))
    .find(|k| cluster_slot(k.as_bytes()) < 8192)
    .unwrap();
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

use wedb_test::resp_frame_str;

/// 挂共享存储的集群会话消费者（provider.set_store 与执行域同源：
/// COUNTKEYSINSLOT / GETKEYSINSLOT / FLUSHALL 慢路径经 provider 下达）
fn cluster_store_consumer(
  cp: &ClusterProvider,
) -> (RespSessionConsumer, Arc<WedbStore<SegmentedDevice>>) {
  let cluster_session: Arc<ClusterSession> = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("cluster.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  cp.set_store(Arc::clone(&store));
  let consumer = RespSessionConsumer::with_cluster_session(
    1,
    RespServerSessionOptions {
      max_databases: 2,
      ..RespServerSessionOptions::default()
    },
    cluster_session,
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

  // 本地槽 5061（bar）先落键再置 MIGRATING → 键仍在本地，读继续本地服务
  //（C# CanOperateOnKey：MIGRATING 槽键存在才放行，缺失即 ASK）
  let out = roundtrip(&mut consumer, &resp_frame_str(&["SET", "bar", "v"]));
  assert_eq!(out, b"+OK\r\n");
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "MIGRATING", "node_2"]),
  );
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(m.current_config().get_state(5061), SlotState::Migrating);
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["GET", "bar"])),
    b"$1\r\nv\r\n"
  );

  // STABLE 复位
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "STABLE"]),
  );
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(m.current_config().get_state(5061), SlotState::Stable);

  // NODE 转移属主至 node_2 → 后续 GET bar → MOVED
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "NODE", "node_2"]),
  );
  assert_eq!(out, b"+OK\r\n");
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["GET", "bar"])),
    b"-MOVED 5061 127.0.0.1:7001\r\n"
  );

  // 非法槽位状态 → "not supported."；STABLE 带 node-id → 语法错误
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "FOO", "node_2"]),
  );
  assert_eq!(out, b"-ERR Slot state FOO not supported.\r\n");
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "STABLE", "node_2"]),
  );
  assert_eq!(out, b"-ERR syntax error\r\n");

  // 属主转回本地并复位，避免影响其他用例（每用例独立 provider，无实际污染）
  let _ = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "NODE", "node_1"]),
  );
  let _ = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "STABLE"]),
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
}

/// COUNTKEYSINSLOT / GETKEYSINSLOT：本地槽真扫描（慢路径），远端槽 MOVED 重定向
#[test]
fn cluster_countkeys_getkeys_in_slot() {
  let rt = Runtime::new().unwrap();
  let cp = two_primary_provider();
  let (mut consumer, _store) = cluster_store_consumer(&cp);

  assert_eq!(cluster_slot(b"bar"), 5061);
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["SET", "bar", "v"])),
    b"+OK\r\n"
  );

  // COUNTKEYSINSLOT 5061 → :1（慢路径扫描）
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", "5061"]),
  );
  assert_eq!(out, b":1\r\n");

  // GETKEYSINSLOT 5061 10 → *1 bar
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "GETKEYSINSLOT", "5061", "10"]),
  );
  assert_eq!(out, b"*1\r\n$3\r\nbar\r\n");

  // 远端槽 12182（foo）→ MOVED
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "COUNTKEYSINSLOT", "12182"]),
  );
  assert_eq!(out, b"-MOVED 12182 127.0.0.1:7001\r\n");
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
    roundtrip(&mut consumer, &resp_frame_str(&["GET", "bar"])),
    b"$-1\r\n"
  );
}

/// CLUSTER MYPARENTID / BANLIST / MTASKS / ENDPOINT / REPLICAS / FORGET
#[test]
fn cluster_node_inspection_commands() {
  let cp = two_primary_provider();
  let m = cp.cluster_manager().unwrap();
  let mut consumer = cluster_consumer(&cp);

  // 主节点视角：MYPARENTID = 自身 id；BANLIST 空；MTASKS :0
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "MYPARENTID"])),
    b"$6\r\nnode_1\r\n"
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
      &resp_frame_str(&["CLUSTER", "ENDPOINT", "node_2"])
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
  m.current_config.write().make_replica_of(Some("node_2"));
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["CLUSTER", "MYPARENTID"])),
    b"$6\r\nnode_2\r\n"
  );
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "REPLICAS", "node_2"]),
  );
  let text = String::from_utf8_lossy(&out);
  assert!(text.starts_with("*1\r\n$"), "node_2 应有 1 个副本: {text}");
  assert!(text.contains("node_1"), "副本行应为 node_1: {text}");

  // FORGET 未知节点 → "I don't know about node"
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "FORGET", "ghost"])
    ),
    b"-ERR I don't know about node ghost.\r\n"
  );
  // FORGET 自身 → 拒绝
  assert_eq!(
    roundtrip(
      &mut consumer,
      &resp_frame_str(&["CLUSTER", "FORGET", "node_1"])
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

/// REPLICAOF NO ONE：副本翻主（保留数据）；REPLICAOF <addr> <port>：配置翻转 +OK
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

  // 已知节点 → 配置翻转 +OK（慢路径闭环）
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["REPLICAOF", "127.0.0.1", "7001"]),
  );
  assert_eq!(out, b"+OK\r\n");
  assert!(m.current_config().is_replica());
  assert_eq!(m.current_config().local_node_primary_id(), Some("node_2"));

  // 副本再执行 REPLICAOF NO ONE → +OK 且翻回主节点
  let out = slow_roundtrip(
    &rt,
    &mut consumer,
    &resp_frame_str(&["REPLICAOF", "NO", "ONE"]),
  );
  assert_eq!(out, b"+OK\r\n");
  assert!(m.current_config().is_primary());
}

/// 顶层 FAILOVER：非主节点拒绝（C# Cannot failover a non-master node）+ 选项解析
#[test]
fn cluster_failover_top_level_semantics() {
  let cp = two_primary_provider();
  let m = cp.cluster_manager().unwrap();
  let mut consumer = cluster_consumer(&cp);

  // 翻为副本角色后 FAILOVER → 拒绝
  m.current_config.write().make_replica_of(Some("node_2"));
  let out = roundtrip(&mut consumer, &resp_frame_str(&["FAILOVER"]));
  assert_eq!(out, b"-ERR Cannot failover a non-master node\r\n");

  // 非法选项 → 语法错误
  assert_eq!(
    roundtrip(&mut consumer, &resp_frame_str(&["FAILOVER", "BOGUS"])),
    b"-ERR syntax error\r\n"
  );
}

/// CLUSTER GOSSIP WITHMEET：合并自身配置并回当前配置字节（WITHMEET 强制应答）
#[test]
fn cluster_gossip_withmeet_roundtrip() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);
  let payload = cp
    .cluster_manager()
    .unwrap()
    .current_config()
    .to_byte_array();

  let payload_str = payload.iter().map(|&b| b as char).collect::<String>();
  let parts = vec![
    "CLUSTER".to_string(),
    "GOSSIP".to_string(),
    "WITHMEET".to_string(),
    payload_str,
  ];
  let mut f = format!("*{}\r\n", parts.len());
  for p in &parts {
    f.push_str(&format!("${}\r\n{p}\r\n", p.len()));
  }
  let (consumed, out) = pump(&mut consumer, f.as_bytes());
  assert_eq!(consumed, Some(0));

  // WITHMEET 强制回配置字节：bulk string 且非空
  assert_eq!(out[0], b'$');
  assert!(out.len() > 10, "应回非空配置载荷: {}", out.len());
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

  // 槽 5061（bar）本地 STABLE → "+5061 = node_1"
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SLOTSTATE", "5061"]),
  );
  assert_eq!(out, b"+5061 = node_1\r\n");

  // 置 MIGRATING 后 → "> node_1"（属主仍为源节点）
  {
    let mut config = m.current_config.write();
    config.slot_map[5061].state = SlotState::Migrating;
  }
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SLOTSTATE", "5061"]),
  );
  assert_eq!(out, b"+5061 > node_1\r\n");

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
  // 小预算测试配置注入（生产缺省 open_node 走 StoreConfig::auto）
  let (_store, _broker, vector_manager) =
    open_node_with_config(test_store_config(), dir.path().join("reserve.db")).unwrap();
  let cp = two_primary_provider();
  cp.set_vector_manager(vector_manager);
  let mut consumer = cluster_consumer(&cp);

  // 预保留 3 个上下文 → *3 + 3 个十进制简单串
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "RESERVE", "VECTOR_SET_CONTEXTS", "3"]),
  );
  assert_eq!(out, b"*3\r\n+8\r\n+16\r\n+24\r\n");

  // 未识别保留类型 → "Unrecognized reservation type"
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "RESERVE", "STRING_CONTEXTS", "3"]),
  );
  assert_eq!(out, b"-Unrecognized reservation type\r\n");

  // 计数非正 → 元数错误
  let out = roundtrip(
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
/// RESP_ERR_MULTI_LOG_DISABLED 同口径显式报错
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
    &resp_frame_str(&["CLUSTER", "APPENDLOG", "node_1", "0", "-1", "-1", "-1"]),
  );
  assert_eq!(out, b"-ERR Cluster not initialized\r\n");
}

/// AOF 门控点亮路径冒烟：open_with_aof 装配后 aof() 在场；单物理日志形态
/// 下 MLOG_KEY_TIME 仍按 C# 单日志部署口径回 RESP_ERR_MULTI_LOG_DISABLED
#[test]
fn cluster_mlog_key_time_with_aof_single_log() {
  let dir = tempfile::tempdir().unwrap();
  let provider = StorageSessionProvider::open_with_config_and_aof(
    test_store_config(),
    dir.path().join("aof.db"),
    None,
    None,
    {
      |network_sender_id, api| {
        Some(RespSessionConsumer::new(
          network_sender_id,
          RespServerSessionOptions::default(),
          api,
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
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "FAIL", "node_2"]),
  );
  assert_eq!(out, b"-ERR Slot state FAIL not supported.\r\n");

  // CLUSTER SETSLOTSRANGE FAIL <node_id> <slots...>
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOTSRANGE", "FAIL", "node_2", "5061", "5062"]),
  );
  assert_eq!(out, b"-ERR Invalid slot state\r\n");

  // 小写 fail 同样被拒绝且不 panic
  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOT", "5061", "fail", "node_2"]),
  );
  assert_eq!(out, b"-ERR Slot state fail not supported.\r\n");

  let out = roundtrip(
    &mut consumer,
    &resp_frame_str(&["CLUSTER", "SETSLOTSRANGE", "fail", "node_2", "5061", "5062"]),
  );
  assert_eq!(out, b"-ERR Invalid slot state\r\n");
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

/// 集群形态事务跨槽校验：MULTI 内多键涉及不同槽位（bar 5061, foo 12182）→ EXEC 拦截返回 -CROSSSLOT
#[test]
fn cluster_session_multi_exec_crossslot() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  let req = b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$2\r\nv1\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$2\r\nv2\r\n*1\r\n$4\r\nEXEC\r\n";
  let out = roundtrip(&mut consumer, req);
  assert_eq!(
    out,
    b"+OK\r\n+QUEUED\r\n+QUEUED\r\n-CROSSSLOT Keys in request don't hash to the same slot\r\n"
  );
}

/// 集群形态事务 WATCH 键与事务内部键跨槽校验：WATCH bar(5061) + SET local_k2(其他本地槽) → EXEC 拦截返回 -CROSSSLOT
#[test]
fn cluster_session_watch_multi_exec_crossslot() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  // 找一个同属本地主节点（< 8192）但不同于 bar(5061) 的键
  let other_key = (0u32..)
    .map(|i| format!("wk{i}"))
    .find(|k| {
      let s = cluster_slot(k.as_bytes());
      s < 8192 && s != 5061
    })
    .unwrap();

  assert_eq!(
    roundtrip(&mut consumer, b"*2\r\n$5\r\nWATCH\r\n$3\r\nbar\r\n"),
    b"+OK\r\n"
  );
  let req = format!(
    "*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n${}\r\n{other_key}\r\n$2\r\nv1\r\n*1\r\n$4\r\nEXEC\r\n",
    other_key.len()
  );
  let out = roundtrip(&mut consumer, req.as_bytes());
  assert_eq!(
    out,
    b"+OK\r\n+QUEUED\r\n-CROSSSLOT Keys in request don't hash to the same slot\r\n"
  );
}

/// 集群形态事务同槽成功执行：所有键均在本地槽（bar 5061）→ EXEC 正常提交执行
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

/// 集群形态事务远端槽重定向：事务涉及远端槽（foo 12182 -> node_2 127.0.0.1:7001）→ EXEC 拦截返回 -MOVED
#[test]
fn cluster_session_multi_exec_remote_slot_moved() {
  let cp = two_primary_provider();
  let mut consumer = cluster_consumer(&cp);

  let req =
    b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$2\r\nv1\r\n*1\r\n$4\r\nEXEC\r\n";
  let out = roundtrip(&mut consumer, req);
  assert_eq!(out, b"+OK\r\n+QUEUED\r\n-MOVED 12182 127.0.0.1:7001\r\n");
}
