use std::net::{TcpListener, UdpSocket};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use aok::{OK, Void};
use compio::time::sleep;
use log::info;
use webc_cmd::{DEFAULT_SHARD_COUNT, calculate_shard_id};
use wedb_cluster::conf::{ClusterMode, Conf, Endpoint, FjallConf, RaftConf, RedisConf};
use wedb_cluster::node::RaftNodeBuilder;
use wedb_cluster::redis::server::RedisServer;

static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

fn get_free_port() -> u16 {
  loop {
    let cur = NEXT_PORT.load(Ordering::Relaxed);
    if cur == 0 {
      let init = fastrand::u16(40000..55000);
      let _ = NEXT_PORT.compare_exchange(0, init, Ordering::Relaxed, Ordering::Relaxed);
    }
    let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
    if port >= 65000 {
      NEXT_PORT.store(40000, Ordering::Relaxed);
    }
    if TcpListener::bind(("127.0.0.1", port)).is_ok()
      && UdpSocket::bind(("127.0.0.1", port)).is_ok()
    {
      return port;
    }
  }
}

#[compio::test]
async fn test_full_cluster_3replicas_scaling_and_healing_simulation() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    node_id: 1,
    mode: ClusterMode::default(),
    topology: None,
    raft: RaftConf {
      endpoint: Endpoint::new("127.0.0.1", raft_port),
      advertise_endpoint: Endpoint::new("127.0.0.1", raft_port),
      join: Vec::new(),
      heartbeat_interval: Some(50),
      election_timeout_min: Some(150),
      election_timeout_max: Some(300),
    },
    fjall: FjallConf::new(dir.path().to_str().unwrap()),
    redis: RedisConf {
      addr: format!("127.0.0.1:{redis_port}"),
      enabled: true,
    },
  };

  let node = RaftNodeBuilder::from_conf(&conf).await?;
  let redis_server = RedisServer::start(node.clone(), conf.redis.addr.clone()).await?;

  sleep(Duration::from_millis(300)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // =========================================================================
  // 阶段 1: 模拟 100 台节点的大规模集群初始化与 3 副本虚拟分片分布
  // =========================================================================
  info!("--- Phase 1: 100 Nodes Cluster 3-Replica Initialization ---");
  {
    let mut topo = node.sharding().write().unwrap();
    // 注册 100 台物理节点
    for i in 1..=100 {
      topo.register_node(i, format!("192.168.1.{i}:6379"));
    }
    topo.rebalance_3replicas();

    // 验证 1024 个分片每个严格只有 3 个副本，且 Leader 必须在其副本集中
    assert_eq!(topo.shards.len(), DEFAULT_SHARD_COUNT as usize);
    for shard in &topo.shards {
      assert_eq!(shard.replicas.len(), 3);
      assert!(shard.replicas.contains(&shard.leader_node_id));
    }

    // 验证负载均匀性：100 台机器平均承载 30~31 个分片副本
    let replica_counts = topo.count_node_replicas();
    assert_eq!(replica_counts.len(), 100);
    for (&node_id, &cnt) in &replica_counts {
      assert!(
        (30..=31).contains(&cnt),
        "Node {node_id} has unbalanced replica count: {cnt}"
      );
    }
  }

  // =========================================================================
  // 阶段 2: 租户写入与分片路由映射验证
  // =========================================================================
  info!("--- Phase 2: Multi-Tenant Namespace Routing ---");
  let test_namespaces = [
    "tenant_finance",
    "tenant_gaming",
    "tenant_social",
    "tenant_ecommerce",
    "tenant_ai_service",
  ];

  for (idx, &ns) in test_namespaces.iter().enumerate() {
    let token = format!("tok_{ns}_{idx}");
    let add_res: String = redis::cmd("NAMESPACE")
      .arg("ADD")
      .arg(ns)
      .arg(&token)
      .query(&mut con)
      .unwrap_or_else(|e| panic!("Failed to add namespace {ns}: {e:?}"));
    assert_eq!(add_res, "OK");

    // 验证分片哈希计算与 Leader 归属
    let shard_id = calculate_shard_id(ns, DEFAULT_SHARD_COUNT);
    assert!(shard_id < DEFAULT_SHARD_COUNT);

    let topo = node.sharding().read().unwrap();
    let shard_info = topo.get_shard_for_namespace(ns).unwrap();
    assert_eq!(shard_info.shard_id, shard_id);
    assert_eq!(shard_info.replicas.len(), 3);
    assert!(shard_info.replicas.contains(&shard_info.leader_node_id));
  }

  // =========================================================================
  // 阶段 3: 线上扩容（Online Scale-Out：100 台 -> 110 台）
  // =========================================================================
  info!("--- Phase 3: Online Scale-Out (100 -> 110 Nodes) ---");
  {
    let mut topo = node.sharding().write().unwrap();
    // 新增 10 台物理节点
    for i in 101..=110 {
      topo.register_node(i, format!("192.168.1.{i}:6379"));
    }
    topo.rebalance_3replicas();

    // 扩容后平均单机副本数降至 27~28
    let counts_110 = topo.count_node_replicas();
    assert_eq!(counts_110.len(), 110);
    for (&node_id, &cnt) in &counts_110 {
      assert!(
        (27..=28).contains(&cnt),
        "Node {node_id} has unbalanced replica count after scale-out: {cnt}"
      );
    }
  }

  // =========================================================================
  // 阶段 4: 线上缩容（Online Scale-In：下线排空 Node 5）
  // =========================================================================
  info!("--- Phase 4: Online Scale-In (Drain Node 5) ---");
  {
    let mut topo = node.sharding().write().unwrap();
    topo.drain_node(5).unwrap();

    // 验证 Node 5 已从集群中注销，且所有分片均不再包含 Node 5
    assert!(!topo.nodes.contains_key(&5));
    for shard in &topo.shards {
      assert!(!shard.replicas.contains(&5));
      assert_eq!(shard.replicas.len(), 3);
    }
  }

  // =========================================================================
  // 阶段 5: 模拟突发宕机与自动自愈（Crash & Self-Healing）
  // =========================================================================
  info!("--- Phase 5: Crash Detection and 3-Replica Self-Healing ---");
  {
    let mut topo = node.sharding().write().unwrap();
    // 模拟 Node 10 和 Node 20 突发失联
    let dead_nodes = vec![10, 20];
    let heal_actions = topo.auto_heal(&dead_nodes);

    assert!(!heal_actions.is_empty());
    info!("Auto-healed {} shard replica slots", heal_actions.len());

    // 验证所有分片重新自愈为满血 3 副本，且不包含故障节点
    for shard in &topo.shards {
      assert!(!shard.replicas.contains(&10));
      assert!(!shard.replicas.contains(&20));
      assert_eq!(shard.replicas.len(), 3);
      assert!(shard.leader_node_id != 10 && shard.leader_node_id != 20);
    }
  }

  // =========================================================================
  // 阶段 6: 模拟节点恢复/临时超额副本与自动 GC 清理（>3 Replicas GC）
  // =========================================================================
  info!("--- Phase 6: Excess Replica Pruning and Disk GC ---");
  {
    let mut topo = node.sharding().write().unwrap();
    // 模拟网络恢复，向分片 0、1、2 注入多余的第 4 副本
    topo.shards[0].replicas.push(888);
    topo.shards[1].replicas.push(888);
    topo.shards[2].replicas.push(888);

    assert_eq!(topo.shards[0].replicas.len(), 4);
    assert_eq!(topo.shards[1].replicas.len(), 4);
    assert_eq!(topo.shards[2].replicas.len(), 4);

    // 触发自动 GC 修剪
    let pruned = topo.prune_excess_replicas(3);
    assert_eq!(pruned.len(), 3);

    // 验证所有分片严格恢复为 3 副本
    for shard in &topo.shards {
      assert_eq!(shard.replicas.len(), 3);
      assert!(shard.replicas.contains(&shard.leader_node_id));
    }
  }

  // =========================================================================
  // 阶段 7: 验证 Redis 协议层 CLUSTER SHARDS / CLUSTER NODES / CLUSTER INFO
  // =========================================================================
  info!("--- Phase 7: Redis Protocol CLUSTER Commands Verification ---");
  let shards_reply: redis::Value = redis::cmd("CLUSTER").arg("SHARDS").query(&mut con)?;
  match shards_reply {
    redis::Value::Array(arr) => assert_eq!(arr.len(), 1024),
    redis::Value::Map(map) => assert_eq!(map.len(), 1024),
    _ => panic!("Expected Array or Map from CLUSTER SHARDS"),
  }

  let slots_reply: redis::Value = redis::cmd("CLUSTER").arg("SLOTS").query(&mut con)?;
  match slots_reply {
    redis::Value::Array(arr) => assert_eq!(arr.len(), 1024),
    _ => panic!("Expected Array from CLUSTER SLOTS"),
  }

  let nodes_reply: String = redis::cmd("CLUSTER").arg("NODES").query(&mut con)?;
  assert!(nodes_reply.contains("myself,master"));

  let info_reply: String = redis::cmd("CLUSTER").arg("INFO").query(&mut con)?;
  assert!(info_reply.contains("cluster_state:ok"));

  let myid_reply: String = redis::cmd("CLUSTER").arg("MYID").query(&mut con)?;
  assert_eq!(myid_reply.len(), 40);

  let keyslot_reply: i64 = redis::cmd("CLUSTER")
    .arg("KEYSLOT")
    .arg("user:10001")
    .query(&mut con)?;
  assert_eq!(keyslot_reply, webc_cmd::Crc::key_slot(b"user:10001") as i64);
  assert!((0..16384).contains(&keyslot_reply));

  let meet_reply: String = redis::cmd("CLUSTER")
    .arg("MEET")
    .arg("192.168.1.130")
    .arg("6379")
    .query(&mut con)?;
  assert_eq!(meet_reply, "OK");

  let rebal_reply: String = redis::cmd("CLUSTER").arg("REBALANCE").query(&mut con)?;
  assert_eq!(rebal_reply, "OK");

  // =========================================================================
  // 阶段 8: 验证多机架/多可用区容灾与最小数据迁移增量重平衡
  // =========================================================================
  info!("--- Phase 8: Multi-Rack Awareness & Incremental Migration ---");
  {
    let mut topo = node.sharding().write().unwrap();
    // 为节点 1..=30 分配 Rack 1，31..=60 分配 Rack 2，61..=90 分配 Rack 3
    for i in 1..=30 {
      topo.set_node_rack(i, 1);
    }
    for i in 31..=60 {
      topo.set_node_rack(i, 2);
    }
    for i in 61..=90 {
      topo.set_node_rack(i, 3);
    }
    topo.rebalance_weighted_with_target_replicas(3);

    // 验证分片均分布在不同机架上
    for shard in &topo.shards {
      assert_eq!(shard.replicas.len(), 3);
      let r1 = topo.get_node_rack(shard.replicas[0]);
      let r2 = topo.get_node_rack(shard.replicas[1]);
      let r3 = topo.get_node_rack(shard.replicas[2]);
      if r1 != 0 && r2 != 0 && r3 != 0 {
        assert!(r1 != r2 && r2 != r3 && r1 != r3);
      }
    }

    // 增量扩容：加入 Node 120
    topo.register_node_with_rack(120, "192.168.1.120:6379", 100, 1);
    let plan = topo.rebalance_incremental(3);
    assert!(!plan.is_empty());
    info!(
      "Incremental migration moved {} shard replicas to Node 120",
      plan.len()
    );
  }

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("100-Node 3-Replica Lifecycle & Self-Healing simulation passed perfectly!");
  OK
}
