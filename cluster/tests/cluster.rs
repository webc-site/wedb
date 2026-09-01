use std::net::{TcpListener, UdpSocket};
use std::sync::atomic::{AtomicU16, Ordering};
use std::thread;
use std::time::Duration;

use aok::{OK, Void};
use compio::time::sleep;
use log::info;
use redis::Commands;
use webc_cmd::sharding::calc_node_slot_range;
use wedb_cluster::conf::{ClusterMode, Conf, Endpoint, FjallConf, RaftConf, RedisConf};
use wedb_cluster::node::RaftNodeBuilder;
use wedb_cluster::redis::RedisServer;
use wedb_raft::types::{BatchWriteReq, UpsertKV};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

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

/// 辅助函数：根据节点槽位范围自动生成匹配 key
fn find_key_for_node(prefix: &str, node_idx: usize, num_nodes: usize) -> (String, u16) {
  let (start, end) = calc_node_slot_range(node_idx, num_nodes);
  (0..)
    .map(|i| {
      let key = format!("{prefix}:k_{node_idx}_{i}");
      let slot = webc_cmd::Crc::key_slot(key.as_bytes());
      (key, slot)
    })
    .find(|(_, slot)| (*slot as u32) >= start && (*slot as u32) <= end)
    .unwrap()
}

fn open_test_redis_conn(port: u16) -> aok::Result<redis::Connection> {
  let client = redis::Client::open(format!("redis://127.0.0.1:{port}"))?;
  let con = client.get_connection()?;
  con.set_read_timeout(Some(Duration::from_secs(3)))?;
  con.set_write_timeout(Some(Duration::from_secs(3)))?;
  Ok(con)
}

fn wait_get_sync(con: &mut redis::Connection, key: &str, expected: &str) -> String {
  for _ in 0..60 {
    if let Ok(val) = con.get::<_, String>(key)
      && val == expected
    {
      return val;
    }
    thread::sleep(Duration::from_millis(15));
  }
  con.get(key).unwrap_or_default()
}

fn wait_set_sync(
  con: &mut redis::Connection,
  key: &str,
  val: &str,
) -> Result<(), redis::RedisError> {
  for _ in 0..60 {
    if con.set::<_, _, ()>(key, val).is_ok() {
      return Ok(());
    }
    thread::sleep(Duration::from_millis(15));
  }
  con.set(key, val)
}

#[compio::test]
async fn test_three_node_distributed_cluster() -> Void {
  let dir1 = tempfile::tempdir()?;
  let dir2 = tempfile::tempdir()?;
  let dir3 = tempfile::tempdir()?;

  let raft_port1 = get_free_port();
  let redis_port1 = get_free_port();
  let raft_port2 = get_free_port();
  let redis_port2 = get_free_port();
  let raft_port3 = get_free_port();
  let redis_port3 = get_free_port();

  let cfg1 = Conf {
    node_id: 1,
    mode: ClusterMode::Raft,
    topology: None,
    raft: RaftConf {
      endpoint: Endpoint::new("127.0.0.1", raft_port1),
      advertise_endpoint: Endpoint::new("127.0.0.1", raft_port1),
      join: Vec::new(),
      heartbeat_interval: Some(20),
      election_timeout_min: Some(50),
      election_timeout_max: Some(100),
    },
    fjall: FjallConf::new(dir1.path().to_str().unwrap()),
    redis: RedisConf {
      addr: format!("127.0.0.1:{redis_port1}"),
      enabled: true,
    },
  };

  let cfg2 = Conf {
    node_id: 2,
    mode: ClusterMode::Raft,
    topology: None,
    raft: RaftConf {
      endpoint: Endpoint::new("127.0.0.1", raft_port2),
      advertise_endpoint: Endpoint::new("127.0.0.1", raft_port2),
      join: vec![format!("127.0.0.1:{raft_port1}")],
      heartbeat_interval: Some(20),
      election_timeout_min: Some(50),
      election_timeout_max: Some(100),
    },
    fjall: FjallConf::new(dir2.path().to_str().unwrap()),
    redis: RedisConf {
      addr: format!("127.0.0.1:{redis_port2}"),
      enabled: true,
    },
  };

  let cfg3 = Conf {
    node_id: 3,
    mode: ClusterMode::Raft,
    topology: None,
    raft: RaftConf {
      endpoint: Endpoint::new("127.0.0.1", raft_port3),
      advertise_endpoint: Endpoint::new("127.0.0.1", raft_port3),
      join: vec![
        format!("127.0.0.1:{raft_port1}"),
        format!("127.0.0.1:{raft_port2}"),
      ],
      heartbeat_interval: Some(20),
      election_timeout_min: Some(50),
      election_timeout_max: Some(100),
    },
    fjall: FjallConf::new(dir3.path().to_str().unwrap()),
    redis: RedisConf {
      addr: format!("127.0.0.1:{redis_port3}"),
      enabled: true,
    },
  };

  info!("Starting node 1 (Leader / Seed)...");
  let node1 = RaftNodeBuilder::from_conf(&cfg1).await?;
  let srv1 = RedisServer::start(node1.clone(), cfg1.redis.addr.clone()).await?;

  sleep(Duration::from_millis(150)).await;

  info!("Starting node 2 (Follower 1)...");
  let node2 = RaftNodeBuilder::from_conf(&cfg2).await?;
  let srv2 = RedisServer::start(node2.clone(), cfg2.redis.addr.clone()).await?;

  info!("Starting node 3 (Follower 2)...");
  let node3 = RaftNodeBuilder::from_conf(&cfg3).await?;
  let srv3 = RedisServer::start(node3.clone(), cfg3.redis.addr.clone()).await?;

  sleep(Duration::from_millis(200)).await;

  let handle = thread::spawn(move || -> aok::Void {
    let mut con1 = open_test_redis_conn(redis_port1)?;

    let slot_greeting: i64 = redis::cmd("CLUSTER")
      .arg("KEYSLOT")
      .arg("cluster:greeting")
      .query(&mut con1)?;
    assert_eq!(
      slot_greeting,
      webc_cmd::Crc::key_slot(b"cluster:greeting") as i64
    );

    let slot_hashtag: i64 = redis::cmd("CLUSTER")
      .arg("KEYSLOT")
      .arg("{user100}:profile")
      .query(&mut con1)?;
    assert_eq!(slot_hashtag, webc_cmd::Crc::key_slot(b"user100") as i64);

    let cluster_nodes: String = redis::cmd("CLUSTER").arg("NODES").query(&mut con1)?;
    assert!(cluster_nodes.contains("myself,master"));
    assert!(cluster_nodes.contains("0-5460"));
    assert!(cluster_nodes.contains("5461-10922"));
    assert!(cluster_nodes.contains("10923-16383"));

    let (key1, _) = find_key_for_node("cluster", 0, 3);
    let (key2, _) = find_key_for_node("cluster", 1, 3);
    let (key3, _) = find_key_for_node("cluster", 2, 3);

    let mut con2 = open_test_redis_conn(redis_port2)?;
    let mut con3 = open_test_redis_conn(redis_port3)?;

    wait_set_sync(&mut con1, &key1, "val1")?;
    let val1 = wait_get_sync(&mut con1, &key1, "val1");
    assert_eq!(val1, "val1");

    wait_set_sync(&mut con2, &key2, "val2")?;
    let val2 = wait_get_sync(&mut con2, &key2, "val2");
    assert_eq!(val2, "val2");

    wait_set_sync(&mut con3, &key3, "val3")?;
    let val3 = wait_get_sync(&mut con3, &key3, "val3");
    assert_eq!(val3, "val3");

    wait_set_sync(&mut con1, &key2, "val2_direct")?;
    let val2_read = wait_get_sync(&mut con1, &key2, "val2_direct");
    assert_eq!(val2_read, "val2_direct");

    wait_set_sync(&mut con1, &key3, "val3_direct")?;
    let val3_read = wait_get_sync(&mut con1, &key3, "val3_direct");
    assert_eq!(val3_read, "val3_direct");

    wait_set_sync(&mut con2, &key1, "val1_direct2")?;
    let val1_read2 = wait_get_sync(&mut con2, &key1, "val1_direct2");
    assert_eq!(val1_read2, "val1_direct2");

    wait_set_sync(&mut con3, &key1, "val1_direct3")?;
    let val1_read3 = wait_get_sync(&mut con3, &key1, "val1_direct3");
    assert_eq!(val1_read3, "val1_direct3");

    let counter_key = format!("{{{key1}}}:counter");
    let mut counter: i64 = 0;
    for _ in 0..60 {
      if let Ok(c) = con1.incr::<_, _, i64>(&counter_key, 42) {
        counter = c;
        break;
      }
      thread::sleep(Duration::from_millis(15));
    }
    assert_eq!(counter, 42);

    let mut counter_read3: i64 = 0;
    for _ in 0..60 {
      if let Ok(v) = con3.get::<_, i64>(&counter_key) {
        counter_read3 = v;
        if counter_read3 == 42 {
          break;
        }
      }
      thread::sleep(Duration::from_millis(15));
    }
    assert_eq!(counter_read3, 42);

    OK
  });

  while !handle.is_finished() {
    sleep(Duration::from_millis(10)).await;
  }
  handle.join().unwrap()?;

  info!("Verifying internal forward fault-tolerance...");
  node2
    .batch_write(BatchWriteReq {
      entries: vec![UpsertKV::insert(
        "cluster:fwd_test".to_string(),
        b"fwd_val".to_vec(),
      )],
    })
    .await?;

  // Cleanup
  srv1.shutdown().await?;
  srv2.shutdown().await?;
  srv3.shutdown().await?;
  node1.shutdown().await?;
  node2.shutdown().await?;
  node3.shutdown().await?;

  info!("3-node distributed Redis cluster test passed successfully!");
  OK
}

#[compio::test]
async fn test_invalid_join_address_fails() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();
  let cfg = Conf {
    node_id: 10,
    mode: ClusterMode::default(),
    topology: None,
    raft: RaftConf {
      endpoint: Endpoint::new("127.0.0.1", raft_port),
      advertise_endpoint: Endpoint::new("127.0.0.1", raft_port),
      join: vec!["1=127.0.0.1:17001".to_string()],
      heartbeat_interval: Some(50),
      election_timeout_min: Some(150),
      election_timeout_max: Some(300),
    },
    fjall: FjallConf::new(dir.path().to_str().unwrap()),
    redis: RedisConf {
      addr: format!("127.0.0.1:{redis_port}"),
      enabled: false,
    },
  };

  let result = RaftNodeBuilder::from_conf(&cfg).await;
  assert!(result.is_err());
  let err_msg = result.err().unwrap().to_string();
  assert!(
    err_msg.contains("1=127.0.0.1:17001"),
    "Error message should contain the invalid parameter, got: {err_msg}"
  );
  assert!(
    err_msg.contains("must be standard IP:PORT format"),
    "Error message should indicate socket address parse failure, got: {err_msg}"
  );

  OK
}

#[compio::test]
async fn test_three_node_sharding_cluster_direct_write_and_redirect() -> Void {
  let dir1 = tempfile::tempdir()?;
  let dir2 = tempfile::tempdir()?;
  let dir3 = tempfile::tempdir()?;

  let raft_port1 = get_free_port();
  let redis_port1 = get_free_port();
  let raft_port2 = get_free_port();
  let redis_port2 = get_free_port();
  let raft_port3 = get_free_port();
  let redis_port3 = get_free_port();

  let cfg1 = Conf {
    node_id: 101,
    mode: ClusterMode::Sharding,
    topology: None,
    raft: RaftConf {
      endpoint: Endpoint::new("127.0.0.1", raft_port1),
      advertise_endpoint: Endpoint::new("127.0.0.1", raft_port1),
      join: Vec::new(),
      heartbeat_interval: Some(20),
      election_timeout_min: Some(50),
      election_timeout_max: Some(100),
    },
    fjall: FjallConf::new(dir1.path().to_str().unwrap()),
    redis: RedisConf {
      addr: format!("127.0.0.1:{redis_port1}"),
      enabled: true,
    },
  };

  let cfg2 = Conf {
    node_id: 102,
    mode: ClusterMode::Sharding,
    topology: None,
    raft: RaftConf {
      endpoint: Endpoint::new("127.0.0.1", raft_port2),
      advertise_endpoint: Endpoint::new("127.0.0.1", raft_port2),
      join: vec![format!("127.0.0.1:{raft_port1}")],
      heartbeat_interval: Some(20),
      election_timeout_min: Some(50),
      election_timeout_max: Some(100),
    },
    fjall: FjallConf::new(dir2.path().to_str().unwrap()),
    redis: RedisConf {
      addr: format!("127.0.0.1:{redis_port2}"),
      enabled: true,
    },
  };

  let cfg3 = Conf {
    node_id: 103,
    mode: ClusterMode::Sharding,
    topology: None,
    raft: RaftConf {
      endpoint: Endpoint::new("127.0.0.1", raft_port3),
      advertise_endpoint: Endpoint::new("127.0.0.1", raft_port3),
      join: vec![format!("127.0.0.1:{raft_port1}")],
      heartbeat_interval: Some(20),
      election_timeout_min: Some(50),
      election_timeout_max: Some(100),
    },
    fjall: FjallConf::new(dir3.path().to_str().unwrap()),
    redis: RedisConf {
      addr: format!("127.0.0.1:{redis_port3}"),
      enabled: true,
    },
  };

  info!("Starting 3 Sharding nodes...");
  let node1 = RaftNodeBuilder::from_conf(&cfg1).await?;
  let srv1 = RedisServer::start(node1.clone(), cfg1.redis.addr.clone()).await?;

  sleep(Duration::from_millis(150)).await;

  let node2 = RaftNodeBuilder::from_conf(&cfg2).await?;
  let srv2 = RedisServer::start(node2.clone(), cfg2.redis.addr.clone()).await?;

  let node3 = RaftNodeBuilder::from_conf(&cfg3).await?;
  let srv3 = RedisServer::start(node3.clone(), cfg3.redis.addr.clone()).await?;

  sleep(Duration::from_millis(200)).await;

  let mut con1 = open_test_redis_conn(redis_port1)?;
  let mut con2 = open_test_redis_conn(redis_port2)?;
  let mut con3 = open_test_redis_conn(redis_port3)?;

  let (key1, _) = find_key_for_node("sharding", 0, 3);
  let (key2, _) = find_key_for_node("sharding", 1, 3);
  let (key3, _) = find_key_for_node("sharding", 2, 3);

  // 1. 各分片节点直写本地存储引擎（微秒级本地直接落盘）
  let _: () = con1.set(&key1, "fast_val1")?;
  let val1 = wait_get_sync(&mut con1, &key1, "fast_val1");
  assert_eq!(val1, "fast_val1");

  let _: () = con2.set(&key2, "fast_val2")?;
  let val2 = wait_get_sync(&mut con2, &key2, "fast_val2");
  assert_eq!(val2, "fast_val2");

  let _: () = con3.set(&key3, "fast_val3")?;
  let val3 = wait_get_sync(&mut con3, &key3, "fast_val3");
  assert_eq!(val3, "fast_val3");

  // 2. 验证跨节点极速直写无需 -MOVED 重定向（零客户端二次往返）
  let _: () = con1.set(&key2, "direct_val2")?;
  let val2_from1: String = con1.get(&key2)?;
  assert_eq!(val2_from1, "direct_val2");

  let _: () = con1.set(&key3, "direct_val3")?;
  let val3_from1: String = con1.get(&key3)?;
  assert_eq!(val3_from1, "direct_val3");

  // 3. 验证 CLUSTER NODES 报告所有 3 个节点均为 Master
  let nodes_resp: String = redis::cmd("CLUSTER").arg("NODES").query(&mut con1)?;
  assert!(nodes_resp.contains("myself,master"));
  assert!(nodes_resp.contains("0-5460"));
  assert!(nodes_resp.contains("5461-10922"));
  assert!(nodes_resp.contains("10923-16383"));

  // Cleanup
  srv1.shutdown().await?;
  srv2.shutdown().await?;
  srv3.shutdown().await?;
  node1.shutdown().await?;
  node2.shutdown().await?;
  node3.shutdown().await?;

  info!("3-node Sharding mode direct write test passed!");
  OK
}
