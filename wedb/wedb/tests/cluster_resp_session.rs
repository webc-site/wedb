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
  cluster_session::ClusterSession,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};

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

/// 打开临时存储引擎并包装成存储执行域（每调用独立目录，GC 关闭）
fn store_garnet_api() -> Arc<StoreGarnetApi<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("cluster.db")).unwrap());
  let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  Arc::new(StoreGarnetApi::new(store.new_session().unwrap()))
}

/// 构造挂接集群切面 + 存储执行域的会话消费者（单机与集群同一执行路径）
fn cluster_consumer(cp: &ClusterProvider) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = Arc::new(cp.create_cluster_session());
  RespSessionConsumer::with_cluster_session(
    1,
    RespServerSessionOptions {
      max_databases: 2,
      ..RespServerSessionOptions::default()
    },
    cluster_session,
    store_garnet_api(),
  )
}

/// 单命令往返（单帧完整到达）
fn roundtrip(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = consumer.try_consume_messages(frame);
  assert_eq!(consumed, frame.len(), "帧应被完整消费: {frame:?}");
  out
}

/// 集群形态下基础命令与会话命令族
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
  let (consumed, out) = consumer.try_consume_messages(frame);
  assert_eq!(consumed, frame.len());
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
  let (consumed, out) = consumer.try_consume_messages(frame);
  assert_eq!(consumed, frame.len());
  assert_eq!(out, b"-ERR This instance has cluster support disabled\r\n");
}

/// CLUSTER RESET 专用装配：存储注入 provider（同一 store 构造执行域与
/// 慢路径扫描域，对标 C# clusterProvider.storeWrapper 的单一存储可达面）
fn reset_consumer(
  cp: &ClusterProvider,
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = Arc::new(cp.create_cluster_session());
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
  let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
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
  let (consumed, out) = consumer.try_consume_messages(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n");
  assert_eq!(consumed, 28);
  assert!(out.is_empty(), "同步段仅校验，不残留输出");
  let slow = consumer.take_slow_wait().expect("RESET 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b"+OK\r\n");

  // SOFT 重置保留节点 ID（HARD 才换新 id）
  let out = consumer
    .try_consume_messages(b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n")
    .1;
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
  let (consumed, out) = consumer.try_consume_messages(frame.as_bytes());
  assert_eq!(consumed, frame.len());
  assert_eq!(out, b"+OK\r\n");

  // CLUSTER RESET → 槽键在场拒绝
  let (consumed, out) = consumer.try_consume_messages(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n");
  assert_eq!(consumed, 28);
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
  let out = consumer
    .try_consume_messages(
      b"*5\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n$4\r\nSOFT\r\n$1\r\n6\r\n$1\r\n7\r\n",
    )
    .1;
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'cluster|reset' command\r\n"
  );
  let out = consumer
    .try_consume_messages(b"*4\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n$4\r\nSOFT\r\n$1\r\nx\r\n")
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
  let (consumed, out) = consumer.try_consume_messages(frame.as_bytes());
  assert_eq!(consumed, frame.len());
  assert_eq!(out, b"+OK\r\n");

  // 先 DBSIZE 验证 :1（慢路径计数）
  let (consumed, out) = consumer.try_consume_messages(b"*1\r\n$6\r\nDBSIZE\r\n");
  assert_eq!(consumed, 16);
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
  let (consumed, out) =
    consumer.try_consume_messages(b"*3\r\n$7\r\nCLUSTER\r\n$5\r\nRESET\r\n$4\r\nHARD\r\n");
  assert_eq!(consumed, 38);
  let slow = consumer.take_slow_wait().expect("RESET 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b"+OK\r\n");

  // HARD 清库后：DBSIZE → :0
  let (consumed, out) = consumer.try_consume_messages(b"*1\r\n$6\r\nDBSIZE\r\n");
  assert_eq!(consumed, 16);
  let slow = consumer.take_slow_wait().expect("DBSIZE 应挂起慢路径");
  let mut out = out;
  rt.block_on(async {
    out.extend_from_slice(&slow.resolve().await);
  });
  assert_eq!(out, b":0\r\n");
}
