use webc_cmd::meta_cache::MetaCache;
use webc_cmd::sharding::{NodeLocation, ShardTopology};
use webc_cmd::slot::Crc;
use wedb_resp::RespValue;

#[test]
fn test_crc_and_hashtag_extraction() {
  assert_eq!(Crc::extract_hashtag(b"user:1001"), b"user:1001");
  assert_eq!(Crc::extract_hashtag(b"user:{1001}:info"), b"1001");
  assert_eq!(Crc::extract_hashtag(b"user:{}:info"), b"user:{}:info");

  // 验证 CRC16 槽位
  let slot = Crc::key_slot(b"user:{1001}:info");
  assert!((0..16384).contains(&slot));

  // 验证 CRC32
  let crc32_val = Crc::crc32(b"123456789");
  assert_eq!(crc32_val, 0xcbf43926);

  // 验证 CRC64 查表法加速结果与位运算基准一致
  let data = b"Hello Redis CRC64 Jones test vector 1234567890";
  let fast_crc = Crc::crc64(data);
  assert_ne!(fast_crc, 0);
}

#[test]
fn test_meta_cache_basic_get_put_invalidate() {
  let cache = MetaCache::new(1024);

  assert_eq!(cache.get_namespace_by_token("tok_apple"), None);
  assert_eq!(cache.get_token_by_namespace("tenant_apple"), None);

  // 写入映射
  cache.put("tenant_apple", "tok_apple");
  assert_eq!(
    cache.get_namespace_by_token("tok_apple"),
    Some(hipstr::HipStr::borrowed("tenant_apple"))
  );
  assert_eq!(
    cache.get_token_by_namespace("tenant_apple"),
    Some(hipstr::HipStr::borrowed("tok_apple"))
  );

  // 更新 Token
  cache.put("tenant_apple", "tok_apple_new");
  assert_eq!(
    cache.get_namespace_by_token("tok_apple_new"),
    Some(hipstr::HipStr::borrowed("tenant_apple"))
  );
  assert_eq!(
    cache.get_token_by_namespace("tenant_apple"),
    Some(hipstr::HipStr::borrowed("tok_apple_new"))
  );

  // 废弃删除
  cache.invalidate("tenant_apple", Some("tok_apple_new"));
  assert_eq!(cache.get_namespace_by_token("tok_apple_new"), None);
  assert_eq!(cache.get_token_by_namespace("tenant_apple"), None);
}

#[test]
fn test_100_nodes_cluster_3replicas_and_scaling_simulation() {
  // 1. 初始化 100 台物理节点的集群拓扑
  let mut topo = ShardTopology::new(1024);
  for i in 1..=100 {
    topo.register_node(i, format!("192.168.1.{i}:6379"));
  }
  topo.rebalance_3replicas();

  // 验证 1024 个分片每个严格拥有 3 个副本
  for shard in &topo.shards {
    assert_eq!(shard.replicas.len(), 3);
    assert!(shard.replicas.contains(&shard.leader_node_id));
  }

  // 验证 100 台机器的副本负载分布（3072 / 100 = 30.72，每台机器 30~31 个副本）
  let counts = topo.count_node_replicas();
  assert_eq!(counts.len(), 100);
  for (&node_id, &cnt) in &counts {
    assert!(
      (30..=31).contains(&cnt),
      "Node {node_id} has unexpected replica count: {cnt}"
    );
  }

  // 验证 Leader 分布（1024 / 100 = 10.24，每台机器 10~11 个 Leader）
  let leaders = topo.count_node_leaders();
  assert_eq!(leaders.len(), 100);
  for (&node_id, &cnt) in &leaders {
    assert!(
      (10..=11).contains(&cnt),
      "Node {node_id} has unexpected leader count: {cnt}"
    );
  }

  // 2. 线上扩容：新增 10 台机器 (101..=110)
  for i in 101..=110 {
    topo.register_node(i, format!("192.168.1.{i}:6379"));
  }
  topo.rebalance_3replicas();
  let counts_110 = topo.count_node_replicas();
  assert_eq!(counts_110.len(), 110);
  for (&node_id, &cnt) in &counts_110 {
    assert!(
      (27..=28).contains(&cnt),
      "Node {node_id} has unexpected replica count after scale-out: {cnt}"
    );
  }

  // 3. 线上缩容：优雅排空 Node 5
  topo.drain_node(5).unwrap();
  assert!(!topo.nodes.contains_key(&5));
  for shard in &topo.shards {
    assert!(!shard.replicas.contains(&5));
    assert_eq!(shard.replicas.len(), 3);
  }

  // 4. 模拟突发宕机自愈：Node 10 和 Node 20 突发故障
  let dead_nodes = vec![10, 20];
  let heal_actions = topo.auto_heal(&dead_nodes);
  assert!(!heal_actions.is_empty());
  for shard in &topo.shards {
    assert!(!shard.replicas.contains(&10));
    assert!(!shard.replicas.contains(&20));
    assert_eq!(shard.replicas.len(), 3);
    assert!(shard.leader_node_id != 10 && shard.leader_node_id != 20);
  }

  // 5. 模拟节点恢复/临时重复引发超过 3 副本，触发自动修剪 GC
  topo.shards[0].replicas.push(999);
  assert_eq!(topo.shards[0].replicas.len(), 4);

  let pruned = topo.prune_excess_replicas(3);
  assert_eq!(pruned.len(), 1);
  assert_eq!(pruned[0], (0, 999));
  assert_eq!(topo.shards[0].replicas.len(), 3);

  // 6. 验证 RESP 输出结构
  let resp_shards = topo.to_cluster_shards_resp();
  if let RespValue::Arr(items) = resp_shards {
    assert_eq!(items.len(), 1024);
  } else {
    panic!("Expected RespValue::Arr");
  }

  let resp_nodes = topo.to_cluster_nodes_resp(1);
  if let RespValue::Blob(b) = resp_nodes {
    let s = String::from_utf8(b).unwrap();
    assert!(s.contains("myself,master"));
  } else {
    panic!("Expected RespValue::Blob");
  }
}

#[test]
fn test_heterogeneous_weights_and_out_of_space_isolation() {
  let mut topo = ShardTopology::new(1024);
  topo.register_node_with_weight(1, "192.168.1.1:6379", 200);
  topo.register_node_with_weight(2, "192.168.1.2:6379", 100);
  topo.register_node_with_weight(3, "192.168.1.3:6379", 100);
  topo.register_node_with_weight(4, "192.168.1.4:6379", 0);

  topo.rebalance_3replicas();

  for shard in &topo.shards {
    assert_eq!(shard.replicas.len(), 3);
    assert!(!shard.replicas.contains(&4));
  }

  let counts = topo.count_node_replicas();
  assert_eq!(*counts.get(&4).unwrap_or(&0), 0);
  assert_eq!(*counts.get(&1).unwrap(), 1024);
  assert_eq!(*counts.get(&2).unwrap(), 1024);
  assert_eq!(*counts.get(&3).unwrap(), 1024);
}

#[test]
fn test_rack_aware_multi_az_placement() {
  let mut topo = ShardTopology::new(1024);
  topo.register_node_with_rack(1, "192.168.1.1:6379", 100, 1);
  topo.register_node_with_rack(2, "192.168.1.2:6379", 100, 1);
  topo.register_node_with_rack(3, "192.168.1.3:6379", 100, 1);
  topo.register_node_with_rack(4, "192.168.2.1:6379", 100, 2);
  topo.register_node_with_rack(5, "192.168.2.2:6379", 100, 2);
  topo.register_node_with_rack(6, "192.168.2.3:6379", 100, 2);
  topo.register_node_with_rack(7, "192.168.3.1:6379", 100, 3);
  topo.register_node_with_rack(8, "192.168.3.2:6379", 100, 3);
  topo.register_node_with_rack(9, "192.168.3.3:6379", 100, 3);

  topo.rebalance_weighted_with_target_replicas(3);

  for shard in &topo.shards {
    assert_eq!(shard.replicas.len(), 3);
    let mut shard_racks: Vec<u32> = shard
      .replicas
      .iter()
      .map(|&id| topo.get_node_rack(id))
      .collect();
    shard_racks.sort_unstable();
    shard_racks.dedup();
    assert_eq!(shard_racks.len(), 3);
  }
}

#[test]
fn test_incremental_minimal_migration_rebalance() {
  let mut topo = ShardTopology::new(1024);
  for i in 1..=3 {
    topo.register_node(i, format!("192.168.1.{i}:6379"));
  }
  topo.rebalance_3replicas();

  let initial_counts = topo.count_node_replicas();
  assert_eq!(*initial_counts.get(&1).unwrap(), 1024);
  assert_eq!(*initial_counts.get(&2).unwrap(), 1024);
  assert_eq!(*initial_counts.get(&3).unwrap(), 1024);

  topo.register_node(4, "192.168.1.4:6379");
  let migration_plan = topo.rebalance_incremental(3);

  assert_eq!(migration_plan.len(), 768);

  let final_counts = topo.count_node_replicas();
  assert_eq!(*final_counts.get(&4).unwrap(), 768);
  assert_eq!(*final_counts.get(&1).unwrap(), 768);
  assert_eq!(*final_counts.get(&2).unwrap(), 768);
  assert_eq!(*final_counts.get(&3).unwrap(), 768);

  for shard in &topo.shards {
    assert_eq!(shard.replicas.len(), 3);
    let mut unique = shard.replicas.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), 3);
  }
}

#[test]
fn test_under_replicated_degraded_and_auto_expansion() {
  let mut topo = ShardTopology::new(1024);
  assert!(topo.is_degraded());
  assert_eq!(topo.degraded_shard_count(3), 1024);
  assert_eq!(topo.under_replicated_shards(3).len(), 1024);

  topo.register_node(2, "192.168.1.2:6379");
  let exp1 = topo.auto_expand_under_replicated(3);
  assert_eq!(exp1.len(), 1024);
  assert!(topo.is_degraded());
  assert_eq!(topo.degraded_shard_count(3), 1024);

  topo.register_node(3, "192.168.1.3:6379");
  let exp2 = topo.auto_expand_under_replicated(3);
  assert_eq!(exp2.len(), 1024);
  assert!(!topo.is_degraded());
  assert_eq!(topo.degraded_shard_count(3), 0);
  assert_eq!(topo.under_replicated_shards(3).len(), 0);

  for shard in &topo.shards {
    assert_eq!(shard.replicas.len(), 3);
  }

  topo.remove_node(3);
  assert!(topo.is_degraded());
  assert_eq!(topo.degraded_shard_count(3), 1024);

  topo.register_node(4, "192.168.1.4:6379");
  let exp3 = topo.auto_expand_under_replicated(3);
  assert_eq!(exp3.len(), 1024);
  assert!(!topo.is_degraded());
  assert_eq!(topo.degraded_shard_count(3), 0);

  for shard in &topo.shards {
    assert_eq!(shard.replicas.len(), 3);
    assert!(shard.replicas.contains(&4));
  }
}

#[test]
fn test_3az_multi_region_location_placement() {
  let mut topo = ShardTopology::new(1024);
  topo.register_node_with_location(
    1,
    "10.0.1.1:6379",
    100,
    NodeLocation::new("cn-beijing", "zone-a", "rack-01", "host-1"),
  );
  topo.register_node_with_location(
    2,
    "10.0.1.2:6379",
    100,
    NodeLocation::new("cn-beijing", "zone-a", "rack-02", "host-2"),
  );
  topo.register_node_with_location(
    3,
    "10.0.2.1:6379",
    100,
    NodeLocation::new("cn-beijing", "zone-b", "rack-01", "host-3"),
  );
  topo.register_node_with_location(
    4,
    "10.0.2.2:6379",
    100,
    NodeLocation::new("cn-beijing", "zone-b", "rack-02", "host-4"),
  );
  topo.register_node_with_location(
    5,
    "10.0.3.1:6379",
    100,
    NodeLocation::new("cn-beijing", "zone-c", "rack-01", "host-5"),
  );
  topo.register_node_with_location(
    6,
    "10.0.3.2:6379",
    100,
    NodeLocation::new("cn-beijing", "zone-c", "rack-02", "host-6"),
  );

  topo.rebalance_weighted_with_target_replicas(3);

  for shard in &topo.shards {
    assert_eq!(shard.replicas.len(), 3);
    let mut zones: Vec<String> = shard
      .replicas
      .iter()
      .map(|&id| topo.get_node_location(id).unwrap().zone.clone())
      .collect();
    zones.sort();
    zones.dedup();
    assert_eq!(zones.len(), 3);
  }
}

#[test]
fn test_cluster_slots_nodes_shards_official_topology_spec() {
  let mut topo = ShardTopology::new(1024);
  topo.register_node(1, "127.0.0.1:17379");
  topo.register_node(2, "127.0.0.1:17380");
  topo.register_node(3, "127.0.0.1:17381");
  topo.rebalance_3replicas();

  // 1. CLUSTER SLOTS 校验：所有 16384 个槽位无缝覆盖
  let slots_resp = topo.to_cluster_slots_resp();
  if let RespValue::Arr(items) = slots_resp {
    assert_eq!(items.len(), 1024);
    if let RespValue::Arr(first_shard) = &items[0] {
      assert_eq!(first_shard[0], RespValue::Int(0));
      assert_eq!(first_shard[1], RespValue::Int(15));
      if let RespValue::Arr(master) = &first_shard[2] {
        assert_eq!(master[0], RespValue::Blob(b"127.0.0.1".to_vec()));
        assert_eq!(master[1], RespValue::Int(17379));
      }
    }
    if let RespValue::Arr(last_shard) = &items[1023] {
      assert_eq!(last_shard[0], RespValue::Int(16368));
      assert_eq!(last_shard[1], RespValue::Int(16383));
    }
  } else {
    panic!("Expected RespValue::Arr");
  }

  // 2. CLUSTER NODES 校验：3 节点均分配槽位（0-5460, 5461-10922, 10923-16383）
  let nodes_resp = topo.to_cluster_nodes_resp(1);
  if let RespValue::Blob(b) = nodes_resp {
    let s = String::from_utf8(b).unwrap();
    let lines: Vec<&str> = s.lines().collect();
    assert_eq!(lines.len(), 3);
    // Node 1 行包含 myself,master 及 0-5460 槽位
    let node1_line = lines.iter().find(|l| l.contains("myself,master")).unwrap();
    assert!(node1_line.contains("0-5460"), "Node 1 line: {node1_line}");
    assert!(node1_line.contains("127.0.0.1:17379@27379"));

    // Node 2 行包含 master 及 5461-10922 槽位
    let node2_line = lines
      .iter()
      .find(|l| l.contains("127.0.0.1:17380"))
      .unwrap();
    assert!(node2_line.contains("master"), "Node 2 line: {node2_line}");
    assert!(
      node2_line.contains("5461-10922"),
      "Node 2 line: {node2_line}"
    );

    // Node 3 行包含 master 及 10923-16383 槽位
    let node3_line = lines
      .iter()
      .find(|l| l.contains("127.0.0.1:17381"))
      .unwrap();
    assert!(node3_line.contains("master"), "Node 3 line: {node3_line}");
    assert!(
      node3_line.contains("10923-16383"),
      "Node 3 line: {node3_line}"
    );
  } else {
    panic!("Expected RespValue::Blob");
  }

  // 3. CLUSTER INFO 校验：slots assigned = 16384, ok = 16384
  let info_resp = topo.to_cluster_info_resp();
  if let RespValue::Blob(b) = info_resp {
    let s = String::from_utf8(b).unwrap();
    assert!(s.contains("cluster_state:ok"));
    assert!(s.contains("cluster_slots_assigned:16384"));
    assert!(s.contains("cluster_slots_ok:16384"));
    assert!(s.contains("cluster_known_nodes:3"));
  } else {
    panic!("Expected RespValue::Blob");
  }
}

#[test]
fn test_slot_allocation_exact_boundaries_and_coverage() {
  use webc_cmd::sharding::calc_node_slot_range;

  // 1 节点
  assert_eq!(calc_node_slot_range(0, 1), (0, 16383));

  // 2 节点
  assert_eq!(calc_node_slot_range(0, 2), (0, 8191));
  assert_eq!(calc_node_slot_range(1, 2), (8192, 16383));

  // 3 节点（标准 Redis Cluster 划分）
  assert_eq!(calc_node_slot_range(0, 3), (0, 5460));
  assert_eq!(calc_node_slot_range(1, 3), (5461, 10922));
  assert_eq!(calc_node_slot_range(2, 3), (10923, 16383));

  // 4 节点
  assert_eq!(calc_node_slot_range(0, 4), (0, 4095));
  assert_eq!(calc_node_slot_range(1, 4), (4096, 8191));
  assert_eq!(calc_node_slot_range(2, 4), (8192, 12287));
  assert_eq!(calc_node_slot_range(3, 4), (12288, 16383));

  // 验证 N = 1..=100 时全区间连续且严格无重叠无空隙覆盖 0..16383
  for n in 1..=100 {
    let mut prev_end = 0;
    for i in 0..n {
      let (start, end) = calc_node_slot_range(i, n);
      if i == 0 {
        assert_eq!(start, 0);
      } else {
        assert_eq!(start, prev_end + 1, "Gap or overlap at n={n}, i={i}");
      }
      assert!(end >= start);
      prev_end = end;
    }
    assert_eq!(prev_end, 16383, "Did not end at 16383 for n={n}");
  }
}

#[test]
fn test_get_leader_for_slot_3_nodes_routing() {
  let mut topo = ShardTopology::new(1024);
  topo.register_node(1, "127.0.0.1:17379");
  topo.register_node(2, "127.0.0.1:17380");
  topo.register_node(3, "127.0.0.1:17381");
  topo.rebalance_3replicas();

  // 0..=5460 -> Node 1
  assert_eq!(topo.get_leader_for_slot(0), Some(1));
  assert_eq!(topo.get_leader_for_slot(2000), Some(1));
  assert_eq!(topo.get_leader_for_slot(5460), Some(1));

  // 5461..=10922 -> Node 2
  assert_eq!(topo.get_leader_for_slot(5461), Some(2));
  assert_eq!(topo.get_leader_for_slot(8000), Some(2));
  assert_eq!(topo.get_leader_for_slot(10922), Some(2));

  // 10923..=16383 -> Node 3
  assert_eq!(topo.get_leader_for_slot(10923), Some(3));
  assert_eq!(topo.get_leader_for_slot(14000), Some(3));
  assert_eq!(topo.get_leader_for_slot(16383), Some(3));
}

#[test]
fn test_redis_official_crc16_and_keyslot_spec() {
  // 官方 CRC16 测试向量
  assert_eq!(Crc::crc16(b"123456789"), 0x31C3);
  assert_eq!(Crc::key_slot(b"123456789"), 12739);
  assert_eq!(Crc::key_slot(b"foo"), 12182);
  assert_eq!(Crc::key_slot(b"bar"), 5061);

  // Hash Tag 提取与槽位一致性
  assert_eq!(
    Crc::key_slot(b"{user100}:profile"),
    Crc::key_slot(b"user100")
  );
  assert_eq!(
    Crc::key_slot(b"{user100}:orders"),
    Crc::key_slot(b"user100")
  );
  assert_eq!(Crc::key_slot(b"user100:{orders}"), Crc::key_slot(b"orders"));
  assert_eq!(
    Crc::key_slot(b"user100:{}:orders"),
    Crc::key_slot(b"user100:{}:orders")
  );
  assert_eq!(Crc::key_slot(b"foo{{bar}}zap"), Crc::key_slot(b"{bar"));
}
