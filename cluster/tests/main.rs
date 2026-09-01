use std::net::{TcpListener, UdpSocket};
use std::str::from_utf8;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use aok::{OK, Void};
use bytes::BytesMut;
use compio::time::sleep;
use core::num::NonZeroUsize;
use log::info;
use redis::Commands;
use wedb_cluster::conf::{ClusterMode, Conf, Endpoint, FjallConf, RaftConf, RedisConf};
use wedb_cluster::node::RaftNodeBuilder;
use wedb_cluster::redis::{RedisServer, RespValue, parse_resp};
use wedb_raft::types::{
  Cmd, GetKVReq, LogEntry, ScanPrefixReq, TxnCondition, TxnReply, TxnReq, UpsertKV,
};

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

#[test]
fn test_resp3_protocol_codec() -> Void {
  // Test RESP3 Null: _\r\n
  let null_val = RespValue::Null;
  assert_eq!(null_val.serialize_to_vec(), b"_\r\n");
  let mut buf = BytesMut::from(&b"_\r\n"[..]);
  let parsed_null = parse_resp(&mut buf)?.unwrap();
  assert_eq!(parsed_null, RespValue::Null);

  // Test RESP3 Bool: #t\r\n and #f\r\n
  let bool_true = RespValue::Bool(true);
  assert_eq!(bool_true.serialize_to_vec(), b"#t\r\n");
  let mut buf = BytesMut::from(&b"#t\r\n"[..]);
  let parsed_bool = parse_resp(&mut buf)?.unwrap();
  assert_eq!(parsed_bool, RespValue::Bool(true));

  // Test RESP3 Float: ,3.5\r\n
  let float_val = RespValue::Float(3.5);
  let mut buf = BytesMut::from(&b",3.5\r\n"[..]);
  let parsed_float = parse_resp(&mut buf)?.unwrap();
  assert_eq!(parsed_float, float_val);

  // Test RESP3 Map: %2\r\n+key1\r\n:10\r\n+key2\r\n:20\r\n
  let map_val = RespValue::Map(vec![
    (RespValue::Simple("key1".to_string()), RespValue::Int(10)),
    (RespValue::Simple("key2".to_string()), RespValue::Int(20)),
  ]);
  let map_bytes = map_val.serialize_to_vec();
  let mut buf = BytesMut::from(&map_bytes[..]);
  let parsed_map = parse_resp(&mut buf)?.unwrap();
  assert_eq!(parsed_map, map_val);

  // Test RESP3 Set: ~2\r\n+elem1\r\n+elem2\r\n
  let set_val = RespValue::Set(vec![
    RespValue::Simple("elem1".to_string()),
    RespValue::Simple("elem2".to_string()),
  ]);
  let set_bytes = set_val.serialize_to_vec();
  let mut buf = BytesMut::from(&set_bytes[..]);
  let parsed_set = parse_resp(&mut buf)?.unwrap();
  assert_eq!(parsed_set, set_val);

  info!("RESP3 protocol parser and serializer tests passed!");
  OK
}

#[compio::test]
async fn test_single_node_raft_kv() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();
  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 1,
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
      enabled: false,
    },
  };

  let node = RaftNodeBuilder::from_conf(&conf).await?;

  sleep(Duration::from_millis(500)).await;

  // 1. Write
  let entry = LogEntry::new(Cmd::UpsertKV(UpsertKV::insert("user:1", "alice")));
  node.write(entry).await?;

  // 2. Read (Local Fast-Path & ReadIndex Linearizable Read)
  let val = node
    .read(GetKVReq {
      key: "user:1".to_string(),
    })
    .await?;
  assert_eq!(val, Some(b"alice".to_vec()));

  let val_linearizable = node
    .read_linearizable(GetKVReq {
      key: "user:1".to_string(),
    })
    .await?;
  assert_eq!(val_linearizable, Some(b"alice".to_vec()));

  // 3. Scan Prefix (Local Fast-Path & ReadIndex Linearizable Scan)
  let entry2 = LogEntry::new(Cmd::UpsertKV(UpsertKV::insert("user:2", "bob")));
  node.write(entry2).await?;

  let scanned = node
    .scan_prefix(ScanPrefixReq {
      prefix: b"user:".to_vec(),
    })
    .await?;
  assert_eq!(scanned.len(), 2);

  let scanned_linearizable = node
    .scan_prefix_linearizable(ScanPrefixReq {
      prefix: b"user:".to_vec(),
    })
    .await?;
  assert_eq!(scanned_linearizable.len(), 2);

  // 4. Txn (Conditional write)
  let txn_req = TxnReq::new(vec![TxnCondition::eq("user:1", "alice")])
    .if_then(UpsertKV::insert("user:1", "alice_updated"));
  let txn_rep = node.txn(txn_req).await?;
  match txn_rep {
    TxnReply::Success { branch, .. } => assert!(branch),
  }

  let val_updated = node
    .read(GetKVReq {
      key: "user:1".to_string(),
    })
    .await?;
  assert_eq!(val_updated, Some(b"alice_updated".to_vec()));

  // 5. Delete
  node
    .write(LogEntry::new(Cmd::UpsertKV(UpsertKV::delete("user:1"))))
    .await?;
  let val_deleted = node
    .read(GetKVReq {
      key: "user:1".to_string(),
    })
    .await?;
  assert_eq!(val_deleted, None);

  node.shutdown().await?;
  info!("Single node Raft KV test passed!");
  OK
}

#[compio::test]
async fn test_redis_server_protocol_and_cmds() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();
  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: fastrand::u64(100..10000),
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

  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // PING
  let pong: String = redis::cmd("PING").query(&mut con)?;
  assert_eq!(pong, "PONG");

  let echo: String = redis::cmd("ECHO").arg("hello_world").query(&mut con)?;
  assert_eq!(echo, "hello_world");

  // SET & GET
  let _: () = con.set("mykey", "myval")?;
  let val: String = con.get("mykey")?;
  assert_eq!(val, "myval");

  // EXISTS
  let exists: bool = con.exists("mykey")?;
  assert!(exists);

  let not_exists: bool = con.exists("nonexistent")?;
  assert!(!not_exists);

  // INCR & DECR
  let _: () = con.set("counter", 10)?;
  let counter: i64 = con.incr("counter", 5)?;
  assert_eq!(counter, 15);
  let counter: i64 = con.decr("counter", 3)?;
  assert_eq!(counter, 12);

  // MSET & MGET
  let _: () = con.mset(&[("k1", "v1"), ("k2", "v2"), ("k3", "v3")])?;
  let mvals: Vec<String> = con.mget(&["k1", "k2", "k3"])?;
  assert_eq!(mvals, vec!["v1", "v2", "v3"]);

  // MSETNX
  let msetnx_fail: i64 = redis::cmd("MSETNX")
    .arg("k1")
    .arg("new_v1")
    .arg("k_unique")
    .arg("unique_val")
    .query(&mut con)?;
  assert_eq!(msetnx_fail, 0);

  let msetnx_ok: i64 = redis::cmd("MSETNX")
    .arg("k_u1")
    .arg("v_u1")
    .arg("k_u2")
    .arg("v_u2")
    .query(&mut con)?;
  assert_eq!(msetnx_ok, 1);

  // STRLEN & APPEND
  let len: usize = con.strlen("k1")?;
  assert_eq!(len, 2);

  let new_len: usize = con.append("k1", "_extra")?;
  assert_eq!(new_len, 8);
  let k1_val: String = con.get("k1")?;
  assert_eq!(k1_val, "v1_extra");

  // INCRBYFLOAT
  let float_res: String = redis::cmd("INCRBYFLOAT")
    .arg("float_counter")
    .arg(2.5)
    .query(&mut con)?;
  assert_eq!(float_res, "2.5");

  // SETNX
  let setnx_res: bool = con.set_nx("k2", "v2_new")?;
  assert!(!setnx_res);
  let setnx_res2: bool = con.set_nx("k4_new", "v4_new")?;
  assert!(setnx_res2);

  // GETSET
  let old_val: String = con.getset("k2", "v2_replaced")?;
  assert_eq!(old_val, "v2");
  let new_v2: String = con.get("k2")?;
  assert_eq!(new_v2, "v2_replaced");

  // GETDEL
  let getdel_val: String = redis::cmd("GETDEL").arg("k4_new").query(&mut con)?;
  assert_eq!(getdel_val, "v4_new");
  let getdel_gone: Option<String> = con.get("k4_new")?;
  assert_eq!(getdel_gone, None);

  // SETEX & TTL & EXPIRE & PERSIST
  let _: () = redis::cmd("SETEX")
    .arg("ttl_key")
    .arg(10)
    .arg("ttl_val")
    .query(&mut con)?;
  let ttl_val: i64 = redis::cmd("TTL").arg("ttl_key").query(&mut con)?;
  assert!(ttl_val > 0 && ttl_val <= 10);

  let persist_res: i64 = redis::cmd("PERSIST").arg("ttl_key").query(&mut con)?;
  assert_eq!(persist_res, 1);
  let ttl_after_persist: i64 = redis::cmd("TTL").arg("ttl_key").query(&mut con)?;
  assert_eq!(ttl_after_persist, -1);

  // PREFIX / SCANPREFIX (Rockraft scan_prefix)
  let prefix_res: Vec<String> = redis::cmd("PREFIX").arg("k_u").query(&mut con)?;
  assert_eq!(prefix_res.len(), 4); // [k_u1, v_u1, k_u2, v_u2]

  // BATCH (Rockraft atomic mixed batch write)
  let _: () = redis::cmd("BATCH")
    .arg("SET")
    .arg("batch_k1")
    .arg("batch_v1")
    .arg("DEL")
    .arg("k_u1")
    .arg("SET")
    .arg("batch_k2")
    .arg("batch_v2")
    .query(&mut con)?;
  let b1: String = con.get("batch_k1")?;
  let b2: String = con.get("batch_k2")?;
  let ku1_deleted: Option<String> = con.get("k_u1")?;
  assert_eq!(b1, "batch_v1");
  assert_eq!(b2, "batch_v2");
  assert_eq!(ku1_deleted, None);

  // TXN (Bitcode conditional transactions)
  let txn_req = TxnReq {
    condition: vec![TxnCondition::eq("batch_k1", "batch_v1")],
    if_then: vec![UpsertKV::insert("batch_k1", "batch_v1_updated")],
    else_then: vec![UpsertKV::insert("batch_k1", "fallback")],
    return_previous: true,
  };
  let txn_payload = bitcode::encode(&txn_req);
  let txn_reply: redis::Value = redis::cmd("TXN").arg(txn_payload).query(&mut con)?;
  assert!(matches!(txn_reply, redis::Value::Map(_)));
  let updated_b1: String = con.get("batch_k1")?;
  assert_eq!(updated_b1, "batch_v1_updated");

  // CLUSTER cmds
  let cluster_nodes: String = redis::cmd("CLUSTER").arg("NODES").query(&mut con)?;
  assert!(cluster_nodes.contains("master"));

  let cluster_info: String = redis::cmd("CLUSTER").arg("INFO").query(&mut con)?;
  assert!(cluster_info.contains("cluster_state:ok"));

  // RAFT management / health / metrics / snapshot cmds
  let raft_health: redis::Value = redis::cmd("RAFT.HEALTH").query(&mut con)?;
  assert!(matches!(raft_health, redis::Value::Map(_)));

  let raft_metrics: redis::Value = redis::cmd("RAFT.METRICS").query(&mut con)?;
  assert!(matches!(raft_metrics, redis::Value::Map(_)));

  let snap_res: String = redis::cmd("RAFT.SNAPSHOT").query(&mut con)?;
  assert!(snap_res.contains("SNAPSHOT: index="));

  // DBSIZE & KEYS
  let dbsize: i64 = redis::cmd("DBSIZE").query(&mut con)?;
  assert!(dbsize >= 4);

  let keys: Vec<String> = con.keys("k*")?;
  assert!(keys.len() >= 2);

  // DEL
  let deleted: i64 = con.del(&["k1", "k2"])?;
  assert_eq!(deleted, 2);

  // INFO & ROLE
  let info_str: String = redis::cmd("INFO").query(&mut con)?;
  assert!(info_str.contains("redis_mode:distributed-raft"));
  assert!(info_str.contains("role:master"));

  // GETRANGE & SETRANGE
  let _: () = con.set("range_key", "Hello World")?;
  let r1: String = redis::cmd("GETRANGE")
    .arg("range_key")
    .arg(0)
    .arg(4)
    .query(&mut con)?;
  assert_eq!(r1, "Hello");
  let r2: String = redis::cmd("GETRANGE")
    .arg("range_key")
    .arg(-5)
    .arg(-1)
    .query(&mut con)?;
  assert_eq!(r2, "World");

  let setrange_len: i64 = redis::cmd("SETRANGE")
    .arg("range_key")
    .arg(6)
    .arg("Redis")
    .query(&mut con)?;
  assert_eq!(setrange_len, 11);
  let r3: String = con.get("range_key")?;
  assert_eq!(r3, "Hello Redis");

  // LCS (Longest Common Subsequence)
  let _: () = con.set("lcs_k1", "ohmytext")?;
  let _: () = con.set("lcs_k2", "mynewtext")?;
  let lcs_len: i64 = redis::cmd("LCS")
    .arg("lcs_k1")
    .arg("lcs_k2")
    .arg("LEN")
    .query(&mut con)?;
  assert_eq!(lcs_len, 6);
  let lcs_val: String = redis::cmd("LCS")
    .arg("lcs_k1")
    .arg("lcs_k2")
    .query(&mut con)?;
  assert_eq!(lcs_val, "mytext");

  // DIGEST
  let digest: String = redis::cmd("DIGEST").arg("range_key").query(&mut con)?;
  assert_eq!(digest.len(), 16);

  // CAS, CAD, DELEX
  let cas_res: i64 = redis::cmd("CAS")
    .arg("range_key")
    .arg("Hello Redis")
    .arg("Hello WebDB")
    .query(&mut con)?;
  assert_eq!(cas_res, 1);
  let cas_val: String = con.get("range_key")?;
  assert_eq!(cas_val, "Hello WebDB");

  let cad_fail: i64 = redis::cmd("CAD")
    .arg("range_key")
    .arg("Wrong Value")
    .query(&mut con)?;
  assert_eq!(cad_fail, 0);

  let delex_ok: i64 = redis::cmd("DELEX")
    .arg("range_key")
    .arg("IFEQ")
    .arg("Hello WebDB")
    .query(&mut con)?;
  assert_eq!(delex_ok, 1);
  let cad_gone: Option<String> = con.get("range_key")?;
  assert_eq!(cad_gone, None);

  // BITMAP: SETBIT & GETBIT
  let old_b0: i64 = redis::cmd("SETBIT")
    .arg("bm_k")
    .arg(7)
    .arg(1)
    .query(&mut con)?;
  assert_eq!(old_b0, 0);
  let b7: i64 = redis::cmd("GETBIT").arg("bm_k").arg(7).query(&mut con)?;
  assert_eq!(b7, 1);
  let b0: i64 = redis::cmd("GETBIT").arg("bm_k").arg(0).query(&mut con)?;
  assert_eq!(b0, 0);

  // BITCOUNT
  let _: () = redis::cmd("SETBIT")
    .arg("bm_k")
    .arg(0)
    .arg(1)
    .query(&mut con)?;
  let bitcnt: i64 = redis::cmd("BITCOUNT").arg("bm_k").query(&mut con)?;
  assert_eq!(bitcnt, 2);

  let bitcnt_range: i64 = redis::cmd("BITCOUNT")
    .arg("bm_k")
    .arg(0)
    .arg(3)
    .arg("BIT")
    .query(&mut con)?;
  assert_eq!(bitcnt_range, 1); // bit 0 is 1, bits 1..3 are 0

  // BITPOS
  let pos_1: i64 = redis::cmd("BITPOS").arg("bm_k").arg(1).query(&mut con)?;
  assert_eq!(pos_1, 0);

  let pos_0: i64 = redis::cmd("BITPOS").arg("bm_k").arg(0).query(&mut con)?;
  assert_eq!(pos_0, 1);

  // BITOP (AND, OR, XOR, NOT)
  let _: () = redis::cmd("SET")
    .arg("b1")
    .arg(b"\x0f".as_slice())
    .query(&mut con)?;
  let _: () = redis::cmd("SET")
    .arg("b2")
    .arg(b"\xf0".as_slice())
    .query(&mut con)?;

  let bitop_and_len: i64 = redis::cmd("BITOP")
    .arg("AND")
    .arg("b_dest_and")
    .arg("b1")
    .arg("b2")
    .query(&mut con)?;
  assert_eq!(bitop_and_len, 1);
  let band_val: Vec<u8> = con.get("b_dest_and")?;
  assert_eq!(band_val, vec![0x00]);

  let bitop_or_len: i64 = redis::cmd("BITOP")
    .arg("OR")
    .arg("b_dest_or")
    .arg("b1")
    .arg("b2")
    .query(&mut con)?;
  assert_eq!(bitop_or_len, 1);
  let bor_val: Vec<u8> = con.get("b_dest_or")?;
  assert_eq!(bor_val, vec![0xff]);

  let bitop_xor_len: i64 = redis::cmd("BITOP")
    .arg("XOR")
    .arg("b_dest_xor")
    .arg("b1")
    .arg("b2")
    .query(&mut con)?;
  assert_eq!(bitop_xor_len, 1);
  let bxor_val: Vec<u8> = con.get("b_dest_xor")?;
  assert_eq!(bxor_val, vec![0xff]);

  let bitop_not_len: i64 = redis::cmd("BITOP")
    .arg("NOT")
    .arg("b_dest_not")
    .arg("b1")
    .query(&mut con)?;
  assert_eq!(bitop_not_len, 1);
  let bnot_val: Vec<u8> = con.get("b_dest_not")?;
  assert_eq!(bnot_val, vec![0xf0]);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis protocol & cmds test passed!");
  OK
}

#[compio::test]
async fn test_redis_namespace_and_multidb_isolation() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: fastrand::u64(100..10000),
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

  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  let current_ns: String = redis::cmd("NAMESPACE").arg("CURRENT").query(&mut con)?;
  assert_eq!(current_ns, "default");

  let _: () = con.set("k0", "val0_default")?;
  let k0_val: String = con.get("k0")?;
  assert_eq!(k0_val, "val0_default");

  // 切换至 DB 1
  let select_ok: String = redis::cmd("SELECT").arg(1).query(&mut con)?;
  assert_eq!(select_ok, "OK");

  let current_ns_db1: String = redis::cmd("NAMESPACE").arg("CURRENT").query(&mut con)?;
  assert_eq!(current_ns_db1, "default");

  // 验证 DB 0 的键在 DB 1 不可见
  let k0_in_db1: Option<String> = con.get("k0")?;
  assert_eq!(k0_in_db1, None);

  // 在 DB 1 写入键值与哈希结构
  let _: () = con.set("k1", "val1_db1")?;
  let _: () = con.hset("user:100", "name", "alice")?;
  let hval: String = con.hget("user:100", "name")?;
  assert_eq!(hval, "alice");

  // 切回 DB 0
  let _: () = redis::cmd("SELECT").arg(0).query(&mut con)?;
  let k1_in_db0: Option<String> = con.get("k1")?;
  assert_eq!(k1_in_db0, None);
  let h_in_db0: Option<String> = con.hget("user:100", "name")?;
  assert_eq!(h_in_db0, None);

  let add_res: String = redis::cmd("NAMESPACE")
    .arg("ADD")
    .arg("tenant_apple")
    .arg("token_apple_123")
    .query(&mut con)?;
  assert_eq!(add_res, "OK");

  let get_token: String = redis::cmd("NAMESPACE")
    .arg("GET")
    .arg("tenant_apple")
    .query(&mut con)?;
  assert_eq!(get_token, "token_apple_123");

  let list_ns: Vec<String> = redis::cmd("NAMESPACE")
    .arg("GET")
    .arg("*")
    .query(&mut con)?;
  assert!(list_ns.contains(&"tenant_apple".to_string()));
  assert!(list_ns.contains(&"token_apple_123".to_string()));

  let auth_res: String = redis::cmd("AUTH").arg("token_apple_123").query(&mut con)?;
  assert_eq!(auth_res, "OK");

  let apple_ns: String = redis::cmd("NAMESPACE").arg("CURRENT").query(&mut con)?;
  assert_eq!(apple_ns, "tenant_apple");

  // 在 tenant_apple 写入数据 (此时在 tenant_apple, db=0)
  let _: () = con.set("apple_item", "macbook")?;
  let item: String = con.get("apple_item")?;
  assert_eq!(item, "macbook");

  // 切换至 DB 1 验证隔离
  let _: () = redis::cmd("SELECT").arg(1).query(&mut con)?;
  let item_in_db1: Option<String> = con.get("apple_item")?;
  assert_eq!(item_in_db1, None);

  // 切回 DB 0
  let _: () = redis::cmd("SELECT").arg(0).query(&mut con)?;
  let item_in_db0: Option<String> = con.get("apple_item")?;
  assert_eq!(item_in_db0, Some("macbook".to_string()));

  let set_token_res: String = redis::cmd("NAMESPACE")
    .arg("SET")
    .arg("tenant_apple")
    .arg("token_apple_456")
    .query(&mut con)?;
  assert_eq!(set_token_res, "OK");

  let new_token: String = redis::cmd("NAMESPACE")
    .arg("GET")
    .arg("tenant_apple")
    .query(&mut con)?;
  assert_eq!(new_token, "token_apple_456");

  let auth_new: String = redis::cmd("AUTH").arg("token_apple_456").query(&mut con)?;
  assert_eq!(auth_new, "OK");
  let ns_after_auth: String = redis::cmd("NAMESPACE").arg("CURRENT").query(&mut con)?;
  assert_eq!(ns_after_auth, "tenant_apple");

  let _: () = redis::cmd("SELECT").arg(1).query(&mut con)?;
  let _: () = redis::cmd("SETEX")
    .arg("move_key")
    .arg(100)
    .arg("move_val")
    .query(&mut con)?;
  let _: () = con.hset("move_hash", "f1", "v1")?;

  let move_res1: i64 = redis::cmd("MOVE").arg("move_key").arg(0).query(&mut con)?;
  assert_eq!(move_res1, 1);

  let move_res2: i64 = redis::cmd("MOVE").arg("move_hash").arg(0).query(&mut con)?;
  assert_eq!(move_res2, 1);

  // DB 1 中源键已不存在
  let in_db1: Option<String> = con.get("move_key")?;
  assert_eq!(in_db1, None);

  // 切换至 DB 0 验证键值与 TTL 已完整迁移
  let _: () = redis::cmd("SELECT").arg(0).query(&mut con)?;
  let in_db0: Option<String> = con.get("move_key")?;
  assert_eq!(in_db0, Some("move_val".to_string()));
  let ttl_migrated: i64 = redis::cmd("TTL").arg("move_key").query(&mut con)?;
  assert!(ttl_migrated > 0 && ttl_migrated <= 100);

  let hval_migrated: Option<String> = con.hget("move_hash", "f1")?;
  assert_eq!(hval_migrated, Some("v1".to_string()));

  let _: () = redis::cmd("AUTH").arg("admin").query(&mut con)?;
  let _: () = con.set("movex_k", "val_from_db0")?;

  // 在 tenant_apple 预设同名键
  let _: () = redis::cmd("AUTH").arg("token_apple_456").query(&mut con)?;
  let _: () = con.set("movex_k", "val_old_apple")?;

  // 切回默认命名空间
  let _: () = redis::cmd("AUTH").arg("admin").query(&mut con)?;
  // 不带 REPLACE: 目标已存在 -> 失败返回 0
  let movex_fail: i64 = redis::cmd("MOVEX")
    .arg("movex_k")
    .arg("token_apple_456")
    .query(&mut con)?;
  assert_eq!(movex_fail, 0);

  // 带 REPLACE: 覆盖目标 -> 成功返回 1
  let movex_ok: i64 = redis::cmd("MOVEX")
    .arg("movex_k")
    .arg("token_apple_456")
    .arg("REPLACE")
    .query(&mut con)?;
  assert_eq!(movex_ok, 1);

  // 切到 tenant_apple 验证已覆盖
  let _: () = redis::cmd("AUTH").arg("token_apple_456").query(&mut con)?;
  let apple_val: String = con.get("movex_k")?;
  assert_eq!(apple_val, "val_from_db0");

  let _: () = redis::cmd("SELECT").arg(1).query(&mut con)?;
  let _: () = con.set("swap_k1", "data_in_db1")?;

  let _: () = redis::cmd("SELECT").arg(2).query(&mut con)?;
  let _: () = con.set("swap_k2", "data_in_db2")?;

  let swap_res: String = redis::cmd("SWAPDB").arg(1).arg(2).query(&mut con)?;
  assert_eq!(swap_res, "OK");

  // 检查 DB 1 现在拥有 swap_k2
  let _: () = redis::cmd("SELECT").arg(1).query(&mut con)?;
  let db1_k2: Option<String> = con.get("swap_k2")?;
  assert_eq!(db1_k2, Some("data_in_db2".to_string()));
  let db1_k1: Option<String> = con.get("swap_k1")?;
  assert_eq!(db1_k1, None);

  // 检查 DB 2 现在拥有 swap_k1
  let _: () = redis::cmd("SELECT").arg(2).query(&mut con)?;
  let db2_k1: Option<String> = con.get("swap_k1")?;
  assert_eq!(db2_k1, Some("data_in_db1".to_string()));
  let db2_k2: Option<String> = con.get("swap_k2")?;
  assert_eq!(db2_k2, None);

  let _: () = redis::cmd("AUTH").arg("admin").query(&mut con)?;
  let _: () = redis::cmd("SELECT").arg(0).query(&mut con)?;
  let del_ns_res: String = redis::cmd("NAMESPACE")
    .arg("DEL")
    .arg("tenant_apple")
    .query(&mut con)?;
  assert_eq!(del_ns_res, "OK");

  let all_ns: Vec<String> = redis::cmd("NAMESPACE")
    .arg("GET")
    .arg("*")
    .query(&mut con)?;
  assert!(!all_ns.contains(&"tenant_apple".to_string()));

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis namespace and multi-db isolation tests passed!");
  OK
}

#[compio::test]
async fn test_pipeline_multidb_under_namespace() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: fastrand::u64(100..10000),
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

  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. 添加租户命名空间
  let _: () = redis::cmd("NAMESPACE")
    .arg("ADD")
    .arg("tenant_pipe")
    .arg("token_pipe_secret")
    .query(&mut con)?;

  // 2. 鉴权切换至 tenant_pipe
  let _: () = redis::cmd("AUTH")
    .arg("token_pipe_secret")
    .query(&mut con)?;

  // 3. 在单个 Pipeline 中跨当前 namespace 下的多个不同 DB 进行批量读写
  let mut pipe = redis::pipe();
  pipe
    .cmd("SELECT")
    .arg(0)
    .set("pipe_key", "val_db0")
    .cmd("SELECT")
    .arg(1)
    .set("pipe_key", "val_db1")
    .cmd("SELECT")
    .arg(2)
    .set("pipe_key", "val_db2")
    .cmd("SELECT")
    .arg(0)
    .get("pipe_key")
    .cmd("SELECT")
    .arg(1)
    .get("pipe_key")
    .cmd("SELECT")
    .arg(2)
    .get("pipe_key");

  let results: ((), (), (), (), (), (), (), String, (), String, (), String) =
    pipe.query(&mut con)?;
  assert_eq!(results.7, "val_db0");
  assert_eq!(results.9, "val_db1");
  assert_eq!(results.11, "val_db2");

  // 4. 验证在另一个默认命名空间连接中完全隔离
  let mut con2 = client.get_connection()?;
  let val_d0: Option<String> = redis::cmd("GET").arg("pipe_key").query(&mut con2)?;
  assert_eq!(val_d0, None);

  let _: () = redis::cmd("SELECT").arg(1).query(&mut con2)?;
  let val_d1: Option<String> = redis::cmd("GET").arg("pipe_key").query(&mut con2)?;
  assert_eq!(val_d1, None);

  let _: () = redis::cmd("SELECT").arg(2).query(&mut con2)?;
  let val_d2: Option<String> = redis::cmd("GET").arg("pipe_key").query(&mut con2)?;
  assert_eq!(val_d2, None);

  redis_server.shutdown().await?;
  node.shutdown().await?;
  OK
}

#[compio::test]
async fn test_redis_hash_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();
  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 3,
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

  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. 基础 HSET / HGET / HMSET / HMGET
  let added: i64 = con.hset("h1", "f1", "v1")?;
  assert_eq!(added, 1);
  let updated: i64 = con.hset("h1", "f1", "v1_new")?;
  assert_eq!(updated, 0);

  let val: String = con.hget("h1", "f1")?;
  assert_eq!(val, "v1_new");

  let nil_val: Option<String> = con.hget("h1", "nonexistent")?;
  assert_eq!(nil_val, None);

  let _: () = con.hset_multiple("h1", &[("f2", "v2"), ("f3", "v3")])?;
  let mvals: Vec<Option<String>> = redis::cmd("HMGET")
    .arg("h1")
    .arg(&["f1", "f2", "f3", "f4"])
    .query(&mut con)?;
  assert_eq!(
    mvals,
    vec![
      Some("v1_new".to_string()),
      Some("v2".to_string()),
      Some("v3".to_string()),
      None
    ]
  );

  // 2. HEXISTS / HSTRLEN / HLEN / HSETNX
  let exists: bool = con.hexists("h1", "f2")?;
  assert!(exists);
  let exists_none: bool = con.hexists("h1", "f999")?;
  assert!(!exists_none);

  let strlen: usize = redis::cmd("HSTRLEN").arg("h1").arg("f1").query(&mut con)?;
  assert_eq!(strlen, 6); // "v1_new" length

  let hlen: usize = con.hlen("h1")?;
  assert_eq!(hlen, 3); // f1, f2, f3

  let setnx_fail: i64 = con.hset_nx("h1", "f1", "v_nx")?;
  assert_eq!(setnx_fail, 0);
  let setnx_ok: i64 = con.hset_nx("h1", "f4", "v4")?;
  assert_eq!(setnx_ok, 1);
  assert_eq!(con.hlen::<_, usize>("h1")?, 4);

  // 3. 数值增减 (HINCRBY / HINCRBYFLOAT)
  let _: () = con.hset("h_num", "int_val", "100")?;
  let new_int: i64 = con.hincr("h_num", "int_val", 25)?;
  assert_eq!(new_int, 125);
  let decr_int: i64 = con.hincr("h_num", "int_val", -50)?;
  assert_eq!(decr_int, 75);

  let new_created: i64 = con.hincr("h_num", "new_counter", 10)?;
  assert_eq!(new_created, 10);

  let _: () = con.hset("h_num", "flt_val", "10.5")?;
  let new_flt: f64 = redis::cmd("HINCRBYFLOAT")
    .arg("h_num")
    .arg("flt_val")
    .arg(2.5)
    .query(&mut con)?;
  assert!((new_flt - 13.0).abs() < 1e-6);

  // 4. 全表获取 (HGETALL / HKEYS / HVALS)
  let all: Vec<(String, String)> = con.hgetall("h1")?;
  assert_eq!(all.len(), 4);

  let keys: Vec<String> = con.hkeys("h1")?;
  assert_eq!(keys.len(), 4);
  assert!(keys.contains(&"f1".to_string()));
  assert!(keys.contains(&"f2".to_string()));
  assert!(keys.contains(&"f3".to_string()));
  assert!(keys.contains(&"f4".to_string()));

  let vals: Vec<String> = con.hvals("h1")?;
  assert_eq!(vals.len(), 4);
  assert!(vals.contains(&"v1_new".to_string()));
  assert!(vals.contains(&"v2".to_string()));
  assert!(vals.contains(&"v3".to_string()));
  assert!(vals.contains(&"v4".to_string()));

  // 5. HRANDFIELD (随机采样测试)
  let rand_single: String = redis::cmd("HRANDFIELD").arg("h1").query(&mut con)?;
  assert!(keys.contains(&rand_single));

  let rand_distinct: Vec<String> = redis::cmd("HRANDFIELD").arg("h1").arg(2).query(&mut con)?;
  assert_eq!(rand_distinct.len(), 2);
  assert_ne!(rand_distinct[0], rand_distinct[1]);

  let rand_with_replacement: Vec<String> =
    redis::cmd("HRANDFIELD").arg("h1").arg(-5).query(&mut con)?;
  assert_eq!(rand_with_replacement.len(), 5);

  let rand_with_values: Vec<String> = redis::cmd("HRANDFIELD")
    .arg("h1")
    .arg(2)
    .arg("WITHVALUES")
    .query(&mut con)?;
  assert_eq!(rand_with_values.len(), 4); // 2 pairs [k1, v1, k2, v2]

  // 6. HSCAN (匹配与分页测试)
  let scan_all: (u64, Vec<String>) = redis::cmd("HSCAN").arg("h1").arg(0).query(&mut con)?;
  assert_eq!(scan_all.1.len(), 8); // 4 key-value pairs

  let scan_match: (u64, Vec<String>) = redis::cmd("HSCAN")
    .arg("h1")
    .arg(0)
    .arg("MATCH")
    .arg("f?")
    .query(&mut con)?;
  assert_eq!(scan_match.1.len(), 8); // 4 pairs f1, f2, f3, f4

  let scan_match_single: (u64, Vec<String>) = redis::cmd("HSCAN")
    .arg("h1")
    .arg(0)
    .arg("MATCH")
    .arg("f1")
    .query(&mut con)?;
  assert_eq!(scan_match_single.1.len(), 2); // 1 pair (f1, v1_new)

  // 7. HRANGEBYLEX
  let lex_res: Vec<String> = redis::cmd("HRANGEBYLEX")
    .arg("h1")
    .arg("[f1")
    .arg("[f3")
    .query(&mut con)?;
  assert_eq!(lex_res.len(), 6); // f1, v1_new, f2, v2, f3, v3

  // 8. Redis 7.4+ 字段级 TTL 测试 (HEXPIRE / HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME / HPERSIST)
  let _: () = con.hset_multiple("h_ttl", &[("a", "1"), ("b", "2"), ("c", "3")])?;

  // HEXPIRE 设置字段 a 与 b 的 TTL 为 10 秒
  let exp_res: Vec<i64> = redis::cmd("HEXPIRE")
    .arg("h_ttl")
    .arg(10)
    .arg("FIELDS")
    .arg(2)
    .arg("a")
    .arg("b")
    .query(&mut con)?;
  assert_eq!(exp_res, vec![1, 1]);

  // HTTL 查询剩余过期时间 (a, b 应 >= 1，c 为持久 -1，d 不存在 -2)
  let ttl_res: Vec<i64> = redis::cmd("HTTL")
    .arg("h_ttl")
    .arg("FIELDS")
    .arg(4)
    .arg("a")
    .arg("b")
    .arg("c")
    .arg("d")
    .query(&mut con)?;
  assert!(ttl_res[0] > 0 && ttl_res[0] <= 10);
  assert!(ttl_res[1] > 0 && ttl_res[1] <= 10);
  assert_eq!(ttl_res[2], -1);
  assert_eq!(ttl_res[3], -2);

  // HPTTL 毫秒级 TTL
  let pttl_res: Vec<i64> = redis::cmd("HPTTL")
    .arg("h_ttl")
    .arg("FIELDS")
    .arg(2)
    .arg("a")
    .arg("c")
    .query(&mut con)?;
  assert!(pttl_res[0] > 0 && pttl_res[0] <= 10000);
  assert_eq!(pttl_res[1], -1);

  // 条件 NX / XX / GT / LT 测试
  let cond_nx: Vec<i64> = redis::cmd("HEXPIRE")
    .arg("h_ttl")
    .arg(20)
    .arg("NX")
    .arg("FIELDS")
    .arg(2)
    .arg("a")
    .arg("c")
    .query(&mut con)?;
  assert_eq!(cond_nx, vec![0, 1]); // a 已有 TTL 则为 0，c 为持久成功设置为 1

  let cond_xx: Vec<i64> = redis::cmd("HEXPIRE")
    .arg("h_ttl")
    .arg(30)
    .arg("XX")
    .arg("FIELDS")
    .arg(1)
    .arg("a")
    .query(&mut con)?;
  assert_eq!(cond_xx, vec![1]); // a 存在 TTL 成功更新

  // HPERSIST 移除过期时间
  let persist_res: Vec<i64> = redis::cmd("HPERSIST")
    .arg("h_ttl")
    .arg("FIELDS")
    .arg(2)
    .arg("a")
    .arg("b")
    .query(&mut con)?;
  assert_eq!(persist_res, vec![1, 1]);

  let ttl_after_persist: Vec<i64> = redis::cmd("HTTL")
    .arg("h_ttl")
    .arg("FIELDS")
    .arg(2)
    .arg("a")
    .arg("b")
    .query(&mut con)?;
  assert_eq!(ttl_after_persist, vec![-1, -1]);

  // 立即过期删除 (seconds <= 0)
  let imm_del: Vec<i64> = redis::cmd("HEXPIRE")
    .arg("h_ttl")
    .arg(0)
    .arg("FIELDS")
    .arg(1)
    .arg("a")
    .query(&mut con)?;
  assert_eq!(imm_del, vec![2]); // 返回 2 表示立即删除成功
  let exists_a: bool = con.hexists("h_ttl", "a")?;
  assert!(!exists_a);

  // 9. 毫秒级 TTL 到期自动淘汰测试 (HPEXPIRE + sleep)
  let _: () = con.hset_multiple("h_exp", &[("temp1", "val1"), ("perm", "val2")])?;
  let hp_res: Vec<i64> = redis::cmd("HPEXPIRE")
        .arg("h_exp")
        .arg(800) // 800ms
        .arg("FIELDS")
        .arg(1)
        .arg("temp1")
        .query(&mut con)?;
  assert_eq!(hp_res, vec![1]);

  // HGETEX 测试
  let getex_val: String = redis::cmd("HGETEX")
    .arg("h_exp")
    .arg("temp1")
    .query(&mut con)?;
  assert_eq!(getex_val, "val1");

  // 等待 1 秒使 temp1 到期
  sleep(Duration::from_millis(1000)).await;

  let expired_get: Option<String> = con.hget("h_exp", "temp1")?;
  assert_eq!(expired_get, None);

  let expired_exists: bool = con.hexists("h_exp", "temp1")?;
  assert!(!expired_exists);

  let exp_len: usize = con.hlen("h_exp")?;
  assert_eq!(exp_len, 1); // 仅剩 perm

  let exp_all: Vec<(String, String)> = con.hgetall("h_exp")?;
  assert_eq!(exp_all.len(), 1);
  assert_eq!(exp_all[0].0, "perm");

  // 10. HINCRBY 保留现有 TTL 测试
  let _: () = con.hset("h_incr_ttl", "counter", "10")?;
  let _: Vec<i64> = redis::cmd("HEXPIRE")
    .arg("h_incr_ttl")
    .arg(10)
    .arg("FIELDS")
    .arg(1)
    .arg("counter")
    .query(&mut con)?;
  let new_counter: i64 = con.hincr("h_incr_ttl", "counter", 5)?;
  assert_eq!(new_counter, 15);
  let ttl_after_incr: Vec<i64> = redis::cmd("HTTL")
    .arg("h_incr_ttl")
    .arg("FIELDS")
    .arg(1)
    .arg("counter")
    .query(&mut con)?;
  assert!(ttl_after_incr[0] > 0 && ttl_after_incr[0] <= 10);

  // 11. HDEL 删除与清理
  let del_cnt: i64 = con.hdel("h1", &["f1", "f2"])?;
  assert_eq!(del_cnt, 2);
  let hlen_after_del: usize = con.hlen("h1")?;
  assert_eq!(hlen_after_del, 2); // f3, f4

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis Hash comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_list_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();
  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 4,
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

  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. LPUSH / RPUSH / LPUSHX / RPUSHX / LLEN
  let len1: i64 = con.rpush("l1", &["a", "b", "c"])?;
  assert_eq!(len1, 3);
  let len2: i64 = con.lpush("l1", &["d", "e"])?;
  assert_eq!(len2, 5); // 顺序应为 e, d, a, b, c
  let range1: Vec<String> = con.lrange("l1", 0, -1)?;
  assert_eq!(range1, vec!["e", "d", "a", "b", "c"]);

  // LPUSHX / RPUSHX
  let px_nonexist: i64 = redis::cmd("LPUSHX")
    .arg("nonexist")
    .arg("val")
    .query(&mut con)?;
  assert_eq!(px_nonexist, 0);
  let rpx_nonexist: i64 = redis::cmd("RPUSHX")
    .arg("nonexist")
    .arg("val")
    .query(&mut con)?;
  assert_eq!(rpx_nonexist, 0);
  let px_exist: i64 = redis::cmd("LPUSHX")
    .arg("l1")
    .arg("head_x")
    .query(&mut con)?;
  assert_eq!(px_exist, 6);
  let rpx_exist: i64 = redis::cmd("RPUSHX")
    .arg("l1")
    .arg("tail_x")
    .query(&mut con)?;
  assert_eq!(rpx_exist, 7);

  // LLEN
  let llen: i64 = con.llen("l1")?;
  assert_eq!(llen, 7);

  // 2. LPOP / RPOP (单项与多项)
  let pop_head: String = con.lpop("l1", None)?;
  assert_eq!(pop_head, "head_x");
  let pop_tail: String = con.rpop("l1", None)?;
  assert_eq!(pop_tail, "tail_x");
  let pop_multi_head: Vec<String> = con.lpop("l1", NonZeroUsize::new(2))?;
  assert_eq!(pop_multi_head, vec!["e", "d"]);
  let pop_multi_tail: Vec<String> = con.rpop("l1", NonZeroUsize::new(2))?;
  assert_eq!(pop_multi_tail, vec!["c", "b"]);
  let pop_last: String = con.lpop("l1", None)?;
  assert_eq!(pop_last, "a");

  // 列表空时返回 nil
  let pop_nil: Option<String> = con.lpop("l1", None)?;
  assert_eq!(pop_nil, None);
  let rpop_nil: Option<String> = con.rpop("l1", None)?;
  assert_eq!(rpop_nil, None);

  // 3. LINDEX / LSET
  let _: i64 = con.rpush("l2", &["zero", "one", "two", "three"])?;
  let idx_0: String = con.lindex("l2", 0)?;
  assert_eq!(idx_0, "zero");
  let idx_last: String = con.lindex("l2", -1)?;
  assert_eq!(idx_last, "three");
  let idx_neg4: String = con.lindex("l2", -4)?;
  assert_eq!(idx_neg4, "zero");
  let idx_out: Option<String> = con.lindex("l2", 100)?;
  assert_eq!(idx_out, None);
  let idx_neg_out: Option<String> = con.lindex("l2", -100)?;
  assert_eq!(idx_neg_out, None);

  let _: () = con.lset("l2", 1, "ONE")?;
  let _: () = con.lset("l2", -1, "THREE")?;
  let r_l2: Vec<String> = con.lrange("l2", 0, -1)?;
  assert_eq!(r_l2, vec!["zero", "ONE", "two", "THREE"]);

  let err_lset: redis::RedisResult<()> = con.lset("l2", 100, "err");
  assert!(err_lset.is_err());

  // 4. LRANGE 边界与负索引测试
  let sub_range: Vec<String> = con.lrange("l2", 1, 2)?;
  assert_eq!(sub_range, vec!["ONE", "two"]);
  let neg_range: Vec<String> = con.lrange("l2", -2, -1)?;
  assert_eq!(neg_range, vec!["two", "THREE"]);
  let empty_range: Vec<String> = con.lrange("l2", 2, 1)?;
  assert!(empty_range.is_empty());
  let out_range: Vec<String> = con.lrange("l2", 10, 20)?;
  assert!(out_range.is_empty());

  // 5. LTRIM
  let _: i64 = con.rpush("l3", &["0", "1", "2", "3", "4", "5"])?;
  let _: () = con.ltrim("l3", 1, 3)?;
  let trim_res: Vec<String> = con.lrange("l3", 0, -1)?;
  assert_eq!(trim_res, vec!["1", "2", "3"]);
  let _: () = con.ltrim("l3", 10, 20)?;
  let trim_empty: Vec<String> = con.lrange("l3", 0, -1)?;
  assert!(trim_empty.is_empty());

  // 6. LREM（对标 Kvrocks 最小位移量覆盖算法）
  let _: i64 = con.rpush("l4", &["x", "a", "x", "b", "x", "c", "x"])?; // len 7
  // count > 0: 从头到尾删除 1 个 x
  let rem1: i64 = con.lrem("l4", 1, "x")?;
  assert_eq!(rem1, 1);
  let rem1_res: Vec<String> = con.lrange("l4", 0, -1)?;
  assert_eq!(rem1_res, vec!["a", "x", "b", "x", "c", "x"]);

  // count < 0: 从尾到头删除 2 个 x
  let rem2: i64 = con.lrem("l4", -2, "x")?;
  assert_eq!(rem2, 2);
  let rem2_res: Vec<String> = con.lrange("l4", 0, -1)?;
  assert_eq!(rem2_res, vec!["a", "x", "b", "c"]);

  // count == 0: 删除所有剩余的 x
  let rem3: i64 = con.lrem("l4", 0, "x")?;
  assert_eq!(rem3, 1);
  let rem3_res: Vec<String> = con.lrange("l4", 0, -1)?;
  assert_eq!(rem3_res, vec!["a", "b", "c"]);

  // 7. LINSERT（对标 Kvrocks 最小位移量插入算法）
  let _: i64 = con.rpush("l5", &["hello", "world"])?;
  let ins_before: i64 = redis::cmd("LINSERT")
    .arg("l5")
    .arg("BEFORE")
    .arg("world")
    .arg("there")
    .query(&mut con)?;
  assert_eq!(ins_before, 3);
  let ins_after: i64 = redis::cmd("LINSERT")
    .arg("l5")
    .arg("AFTER")
    .arg("world")
    .arg("!")
    .query(&mut con)?;
  assert_eq!(ins_after, 4);
  let l5_range: Vec<String> = con.lrange("l5", 0, -1)?;
  assert_eq!(l5_range, vec!["hello", "there", "world", "!"]);
  let ins_miss: i64 = redis::cmd("LINSERT")
    .arg("l5")
    .arg("BEFORE")
    .arg("nonexist")
    .arg("foo")
    .query(&mut con)?;
  assert_eq!(ins_miss, -1);

  // 8. LPOS（完整支持 RANK, COUNT, MAXLEN）
  let _: i64 = con.rpush("l6", &["a", "b", "c", "d", "3", "2", "3", "4", "3"])?;
  let pos_first: Option<i64> = redis::cmd("LPOS").arg("l6").arg("3").query(&mut con)?;
  assert_eq!(pos_first, Some(4));

  let pos_rank2: Option<i64> = redis::cmd("LPOS")
    .arg("l6")
    .arg("3")
    .arg("RANK")
    .arg(2)
    .query(&mut con)?;
  assert_eq!(pos_rank2, Some(6));

  let pos_rank_neg1: Option<i64> = redis::cmd("LPOS")
    .arg("l6")
    .arg("3")
    .arg("RANK")
    .arg(-1)
    .query(&mut con)?;
  assert_eq!(pos_rank_neg1, Some(8));

  let pos_count: Vec<i64> = redis::cmd("LPOS")
    .arg("l6")
    .arg("3")
    .arg("COUNT")
    .arg(2)
    .query(&mut con)?;
  assert_eq!(pos_count, vec![4, 6]);

  let pos_maxlen: Vec<i64> = redis::cmd("LPOS")
    .arg("l6")
    .arg("3")
    .arg("COUNT")
    .arg(2)
    .arg("MAXLEN")
    .arg(5)
    .query(&mut con)?;
  assert_eq!(pos_maxlen, vec![4]);

  // 9. LMOVE 与 RPOPLPUSH
  let _: i64 = con.rpush("l7", &["1", "2", "3"])?;
  // 单列表右侧弹出压入左侧: 1, 2, 3 -> 3, 1, 2
  let move_self: String = redis::cmd("LMOVE")
    .arg("l7")
    .arg("l7")
    .arg("RIGHT")
    .arg("LEFT")
    .query(&mut con)?;
  assert_eq!(move_self, "3");
  let l7_res: Vec<String> = con.lrange("l7", 0, -1)?;
  assert_eq!(l7_res, vec!["3", "1", "2"]);

  // RPOPLPUSH 兼容
  let rpoplpush_res: String = con.rpoplpush("l7", "l7")?;
  assert_eq!(rpoplpush_res, "2");
  let l7_res2: Vec<String> = con.lrange("l7", 0, -1)?;
  assert_eq!(l7_res2, vec!["2", "3", "1"]);

  // 双列表移动
  let _: i64 = con.rpush("src_list", &["s1", "s2"])?;
  let _: i64 = con.rpush("dst_list", &["d1"])?;
  let moved: String = redis::cmd("LMOVE")
    .arg("src_list")
    .arg("dst_list")
    .arg("LEFT")
    .arg("RIGHT")
    .query(&mut con)?;
  assert_eq!(moved, "s1");
  let src_res: Vec<String> = con.lrange("src_list", 0, -1)?;
  assert_eq!(src_res, vec!["s2"]);
  let dst_res: Vec<String> = con.lrange("dst_list", 0, -1)?;
  assert_eq!(dst_res, vec!["d1", "s1"]);

  // 10. BLPOP / BRPOP / BLMOVE / LMPOP
  let _: i64 = con.rpush("b_target", &["val_b"])?;
  let blpop_res: (String, String) = con.blpop(&["nonexist_list", "b_target"], 0.0)?;
  assert_eq!(blpop_res, ("b_target".to_string(), "val_b".to_string()));

  let _: i64 = con.rpush("lmpop_list", &["e1", "e2", "e3"])?;
  let lmpop_res: (String, Vec<String>) = redis::cmd("LMPOP")
    .arg(2)
    .arg("empty_k")
    .arg("lmpop_list")
    .arg("LEFT")
    .arg("COUNT")
    .arg(2)
    .query(&mut con)?;
  assert_eq!(lmpop_res.0, "lmpop_list");
  assert_eq!(lmpop_res.1, vec!["e1", "e2"]);

  // 11. TYPE 命令确认
  let list_type: String = redis::cmd("TYPE").arg("dst_list").query(&mut con)?;
  assert_eq!(list_type, "list");

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis List comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_set_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();
  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 5,
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

  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. SADD / SCARD / SISMEMBER / SMISMEMBER / SMEMBERS / SREM
  let add_cnt1: i64 = con.sadd("set1", &["a", "b", "c"])?;
  assert_eq!(add_cnt1, 3);
  let add_cnt2: i64 = con.sadd("set1", &["b", "c", "d"])?;
  assert_eq!(add_cnt2, 1); // 只有 'd' 被新增

  let scard1: i64 = con.scard("set1")?;
  assert_eq!(scard1, 4);

  let is_a: bool = con.sismember("set1", "a")?;
  let is_z: bool = con.sismember("set1", "z")?;
  assert!(is_a);
  assert!(!is_z);

  let smis: Vec<i64> = redis::cmd("SMISMEMBER")
    .arg("set1")
    .arg("a")
    .arg("b")
    .arg("z")
    .query(&mut con)?;
  assert_eq!(smis, vec![1, 1, 0]);

  let mut members: Vec<String> = con.smembers("set1")?;
  members.sort();
  assert_eq!(members, vec!["a", "b", "c", "d"]);

  let rem_cnt: i64 = con.srem("set1", &["a", "z"])?;
  assert_eq!(rem_cnt, 1);
  let scard2: i64 = con.scard("set1")?;
  assert_eq!(scard2, 3);

  let set_type: String = redis::cmd("TYPE").arg("set1").query(&mut con)?;
  assert_eq!(set_type, "set");

  // 2. SPOP（单项与多项批量 Pop）
  let _: i64 = con.sadd("set_pop", &["1", "2", "3", "4", "5"])?;
  let pop_one: String = con.spop("set_pop")?;
  assert!(["1", "2", "3", "4", "5"].contains(&pop_one.as_str()));
  let card_after_pop1: i64 = con.scard("set_pop")?;
  assert_eq!(card_after_pop1, 4);

  let pop_two: Vec<String> = redis::cmd("SPOP").arg("set_pop").arg(2).query(&mut con)?;
  assert_eq!(pop_two.len(), 2);
  let card_after_pop2: i64 = con.scard("set_pop")?;
  assert_eq!(card_after_pop2, 2);

  let pop_all_remaining: Vec<String> = redis::cmd("SPOP").arg("set_pop").arg(10).query(&mut con)?;
  assert_eq!(pop_all_remaining.len(), 2);
  let card_empty: i64 = con.scard("set_pop")?;
  assert_eq!(card_empty, 0);

  let pop_nil: Option<String> = redis::cmd("SPOP").arg("set_pop").query(&mut con)?;
  assert_eq!(pop_nil, None);

  // 3. SRANDMEMBER（支持正数非重复随机与负数可重复随机）
  let _: i64 = con.sadd("set_rand", &["alpha", "beta", "gamma"])?;
  let rand_one: String = con.srandmember("set_rand")?;
  assert!(["alpha", "beta", "gamma"].contains(&rand_one.as_str()));

  let rand_pos2: Vec<String> = redis::cmd("SRANDMEMBER")
    .arg("set_rand")
    .arg(2)
    .query(&mut con)?;
  assert_eq!(rand_pos2.len(), 2);
  assert_ne!(rand_pos2[0], rand_pos2[1]); // 非重复

  let rand_pos_overflow: Vec<String> = redis::cmd("SRANDMEMBER")
    .arg("set_rand")
    .arg(10)
    .query(&mut con)?;
  assert_eq!(rand_pos_overflow.len(), 3);

  let rand_neg5: Vec<String> = redis::cmd("SRANDMEMBER")
    .arg("set_rand")
    .arg(-5)
    .query(&mut con)?;
  assert_eq!(rand_neg5.len(), 5); // 负数允许重复返回 5 个元素

  // 4. SMOVE
  let _: i64 = con.sadd("src_set", &["item1", "item2", "item3"])?;
  let _: i64 = con.sadd("dst_set", &["target1"])?;
  let move_succ: bool = con.smove("src_set", "dst_set", "item1")?;
  assert!(move_succ);
  assert!(!con.sismember::<_, _, bool>("src_set", "item1")?);
  assert!(con.sismember::<_, _, bool>("dst_set", "item1")?);

  let move_fail: bool = con.smove("src_set", "dst_set", "nonexist")?;
  assert!(!move_fail);

  let move_same: bool = con.smove("src_set", "src_set", "item2")?;
  assert!(move_same);

  // 5. SUNION & SUNIONSTORE
  let _: i64 = con.sadd("u1", &["1", "2", "3"])?;
  let _: i64 = con.sadd("u2", &["3", "4", "5"])?;
  let _: i64 = con.sadd("u3", &["5", "6", "7"])?;

  let mut union_res: Vec<String> = con.sunion(&["u1", "u2", "u3"])?;
  union_res.sort();
  assert_eq!(union_res, vec!["1", "2", "3", "4", "5", "6", "7"]);

  let union_store_cnt: i64 = con.sunionstore("u_dst", &["u1", "u2", "u3"])?;
  assert_eq!(union_store_cnt, 7);
  let u_dst_card: i64 = con.scard("u_dst")?;
  assert_eq!(u_dst_card, 7);

  // 6. SINTER & SINTERSTORE & SINTERCARD
  let _: i64 = con.sadd("i1", &["a", "b", "c", "d"])?;
  let _: i64 = con.sadd("i2", &["b", "c", "e"])?;
  let _: i64 = con.sadd("i3", &["b", "c", "d", "f"])?;

  let mut inter_res: Vec<String> = con.sinter(&["i1", "i2", "i3"])?;
  inter_res.sort();
  assert_eq!(inter_res, vec!["b", "c"]);

  let inter_store_cnt: i64 = con.sinterstore("i_dst", &["i1", "i2", "i3"])?;
  assert_eq!(inter_store_cnt, 2);
  let i_dst_card: i64 = con.scard("i_dst")?;
  assert_eq!(i_dst_card, 2);

  // SINTERCARD
  let inter_card_all: i64 = redis::cmd("SINTERCARD")
    .arg(3)
    .arg("i1")
    .arg("i2")
    .arg("i3")
    .query(&mut con)?;
  assert_eq!(inter_card_all, 2);

  let inter_card_limit: i64 = redis::cmd("SINTERCARD")
    .arg(3)
    .arg("i1")
    .arg("i2")
    .arg("i3")
    .arg("LIMIT")
    .arg(1)
    .query(&mut con)?;
  assert_eq!(inter_card_limit, 1);

  let inter_card_single: i64 = redis::cmd("SINTERCARD").arg(1).arg("i1").query(&mut con)?;
  assert_eq!(inter_card_single, 4);

  let inter_card_empty: i64 = redis::cmd("SINTERCARD")
    .arg(2)
    .arg("i1")
    .arg("nonexist_set")
    .query(&mut con)?;
  assert_eq!(inter_card_empty, 0);

  // 7. SDIFF & SDIFFSTORE
  let _: i64 = con.sadd("d1", &["a", "b", "c", "d"])?;
  let _: i64 = con.sadd("d2", &["c"])?;
  let _: i64 = con.sadd("d3", &["a", "c", "e"])?;

  let mut diff_res: Vec<String> = con.sdiff(&["d1", "d2", "d3"])?;
  diff_res.sort();
  assert_eq!(diff_res, vec!["b", "d"]);

  let diff_store_cnt: i64 = con.sdiffstore("d_dst", &["d1", "d2", "d3"])?;
  assert_eq!(diff_store_cnt, 2);
  let d_dst_card: i64 = con.scard("d_dst")?;
  assert_eq!(d_dst_card, 2);

  // 8. SSCAN
  let _: i64 = con.sadd(
    "scan_set",
    &["apple", "banana", "apricot", "berry", "orange"],
  )?;
  let scan_res: (String, Vec<String>) = redis::cmd("SSCAN")
    .arg("scan_set")
    .arg(0)
    .arg("MATCH")
    .arg("ap*")
    .arg("COUNT")
    .arg(10)
    .query(&mut con)?;
  assert_eq!(scan_res.0, "0");
  let mut matched_members = scan_res.1;
  matched_members.sort();
  assert_eq!(matched_members, vec!["apple", "apricot"]);

  // 9. DEL 删除
  let del_cnt: i64 = con.del("set1")?;
  assert_eq!(del_cnt, 1);
  let card_after_del: i64 = con.scard("set1")?;
  assert_eq!(card_after_del, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis Set comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_zset_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();
  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 6,
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

  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. ZADD 负数/正数/零/小数浮点数保序编码验证 (IEEE 754 严格升序)
  let add_cnt: i64 = redis::cmd("ZADD")
    .arg("z_order")
    .arg(100.5)
    .arg("m_pos100")
    .arg(-100.5)
    .arg("m_neg100")
    .arg(0.0)
    .arg("m_zero")
    .arg(-1.5)
    .arg("m_neg1")
    .arg(1.5)
    .arg("m_pos1")
    .query(&mut con)?;
  assert_eq!(add_cnt, 5);

  let zcard: i64 = con.zcard("z_order")?;
  assert_eq!(zcard, 5);

  let range_all: Vec<String> = con.zrange("z_order", 0, -1)?;
  assert_eq!(
    range_all,
    vec!["m_neg100", "m_neg1", "m_zero", "m_pos1", "m_pos100"]
  );

  // 2. ZSCORE & ZMSCORE & ZRANK & ZREVRANK
  let score_neg: f64 = con.zscore("z_order", "m_neg100")?;
  assert_eq!(score_neg, -100.5);

  let zmscore: Vec<Option<f64>> = redis::cmd("ZMSCORE")
    .arg("z_order")
    .arg("m_neg100")
    .arg("nonexist")
    .arg("m_pos1")
    .query(&mut con)?;
  assert_eq!(zmscore, vec![Some(-100.5), None, Some(1.5)]);

  let rank_neg: Option<i64> = con.zrank("z_order", "m_neg100")?;
  assert_eq!(rank_neg, Some(0));
  let rank_pos: Option<i64> = con.zrank("z_order", "m_pos100")?;
  assert_eq!(rank_pos, Some(4));

  let rev_rank_neg: Option<i64> = con.zrevrank("z_order", "m_neg100")?;
  assert_eq!(rev_rank_neg, Some(4));
  let rev_rank_pos: Option<i64> = con.zrevrank("z_order", "m_pos100")?;
  assert_eq!(rev_rank_pos, Some(0));

  // ZRANK / ZREVRANK WITHSCORE
  let rank_with_score: (i64, String) = redis::cmd("ZRANK")
    .arg("z_order")
    .arg("m_pos1")
    .arg("WITHSCORE")
    .query(&mut con)?;
  assert_eq!(rank_with_score.0, 3);
  assert_eq!(rank_with_score.1, "1.5");

  let revrank_with_score: (i64, String) = redis::cmd("ZREVRANK")
    .arg("z_order")
    .arg("m_pos1")
    .arg("WITHSCORE")
    .query(&mut con)?;
  assert_eq!(revrank_with_score.0, 1);
  assert_eq!(revrank_with_score.1, "1.5");

  // 3. ZADD 标志位：NX, XX, GT, LT, CH, INCR
  // NX: 仅不存在时插入
  let zadd_nx: i64 = redis::cmd("ZADD")
    .arg("z_flags")
    .arg("NX")
    .arg(10.0)
    .arg("a")
    .arg(20.0)
    .arg("b")
    .query(&mut con)?;
  assert_eq!(zadd_nx, 2);

  let zadd_nx_dup: i64 = redis::cmd("ZADD")
    .arg("z_flags")
    .arg("NX")
    .arg(15.0)
    .arg("a")
    .arg(30.0)
    .arg("c")
    .query(&mut con)?;
  assert_eq!(zadd_nx_dup, 1); // 仅 c 插入，a 保持 10.0
  assert_eq!(con.zscore::<_, _, f64>("z_flags", "a")?, 10.0);

  // XX: 仅存在时更新
  let zadd_xx: i64 = redis::cmd("ZADD")
        .arg("z_flags")
        .arg("XX")
        .arg("CH")
        .arg(15.0)
        .arg("a")
        .arg(40.0)
        .arg("d") // 不存在，跳过
        .query(&mut con)?;
  assert_eq!(zadd_xx, 1);
  assert_eq!(con.zscore::<_, _, f64>("z_flags", "a")?, 15.0);
  assert_eq!(con.zscore::<_, _, Option<f64>>("z_flags", "d")?, None);

  // GT: 仅当大于旧分值时更新
  let zadd_gt: i64 = redis::cmd("ZADD")
        .arg("z_flags")
        .arg("GT")
        .arg("CH")
        .arg(10.0)
        .arg("a") // 10.0 < 15.0, 不更新
        .arg(25.0)
        .arg("b") // 25.0 > 20.0, 更新
        .query(&mut con)?;
  assert_eq!(zadd_gt, 1);
  assert_eq!(con.zscore::<_, _, f64>("z_flags", "a")?, 15.0);
  assert_eq!(con.zscore::<_, _, f64>("z_flags", "b")?, 25.0);

  // LT: 仅当小于旧分值时更新
  let zadd_lt: i64 = redis::cmd("ZADD")
        .arg("z_flags")
        .arg("LT")
        .arg("CH")
        .arg(10.0)
        .arg("a") // 10.0 < 15.0, 更新
        .arg(30.0)
        .arg("b") // 30.0 > 25.0, 不更新
        .query(&mut con)?;
  assert_eq!(zadd_lt, 1);
  assert_eq!(con.zscore::<_, _, f64>("z_flags", "a")?, 10.0);
  assert_eq!(con.zscore::<_, _, f64>("z_flags", "b")?, 25.0);

  // INCR: 单元素累加
  let incr_res: String = redis::cmd("ZADD")
    .arg("z_flags")
    .arg("INCR")
    .arg(5.5)
    .arg("a")
    .query(&mut con)?;
  assert_eq!(incr_res, "15.5");
  assert_eq!(con.zscore::<_, _, f64>("z_flags", "a")?, 15.5);

  // ZINCRBY
  let zincrby_res: String = con.zincr("z_flags", "b", -5.0)?;
  assert_eq!(zincrby_res, "20");
  assert_eq!(con.zscore::<_, _, f64>("z_flags", "b")?, 20.0);

  // 4. ZCOUNT & ZLEXCOUNT
  let _: i64 = redis::cmd("ZADD")
    .arg("z_range")
    .arg(1.0)
    .arg("one")
    .arg(2.0)
    .arg("two")
    .arg(3.0)
    .arg("three")
    .arg(4.0)
    .arg("four")
    .arg(5.0)
    .arg("five")
    .query(&mut con)?;

  let count_all: i64 = redis::cmd("ZCOUNT")
    .arg("z_range")
    .arg("-inf")
    .arg("+inf")
    .query(&mut con)?;
  assert_eq!(count_all, 5);

  let count_inclusive: i64 = redis::cmd("ZCOUNT")
    .arg("z_range")
    .arg("2.0")
    .arg("4.0")
    .query(&mut con)?;
  assert_eq!(count_inclusive, 3);

  let count_exclusive: i64 = redis::cmd("ZCOUNT")
    .arg("z_range")
    .arg("(2.0")
    .arg("(5.0")
    .query(&mut con)?;
  assert_eq!(count_exclusive, 2); // 3.0, 4.0

  let lex_count: i64 = redis::cmd("ZLEXCOUNT")
    .arg("z_range")
    .arg("[a")
    .arg("[z")
    .query(&mut con)?;
  assert_eq!(lex_count, 5);

  let lex_count_ex: i64 = redis::cmd("ZLEXCOUNT")
    .arg("z_range")
    .arg("(five")
    .arg("[three")
    .query(&mut con)?;
  assert_eq!(lex_count_ex, 3); // four, one, three

  // 5. ZRANGE 多维度全功能测试
  // 按索引切片 + REV + WITHSCORES
  let range_rev_scores: Vec<(String, f64)> = con.zrevrange_withscores("z_range", 0, 1)?;
  assert_eq!(
    range_rev_scores,
    vec![("five".to_string(), 5.0), ("four".to_string(), 4.0)]
  );

  // BYSCORE + LIMIT
  let byscore_limit: Vec<String> = redis::cmd("ZRANGE")
    .arg("z_range")
    .arg("2.0")
    .arg("5.0")
    .arg("BYSCORE")
    .arg("LIMIT")
    .arg(1)
    .arg(2)
    .query(&mut con)?;
  assert_eq!(byscore_limit, vec!["three", "four"]);

  // BYLEX
  let bylex_res: Vec<String> = redis::cmd("ZRANGE")
    .arg("z_range")
    .arg("[a")
    .arg("[p")
    .arg("BYLEX")
    .query(&mut con)?;
  assert_eq!(bylex_res, vec!["five", "four", "one"]);

  // 6. ZPOPMIN & ZPOPMAX & ZMPOP
  let _: i64 = redis::cmd("ZADD")
    .arg("z_pop")
    .arg(10)
    .arg("p1")
    .arg(20)
    .arg("p2")
    .arg(30)
    .arg("p3")
    .arg(40)
    .arg("p4")
    .query(&mut con)?;

  let pop_min1: (String, f64) = con.zpopmin("z_pop", 1)?;
  assert_eq!(pop_min1, ("p1".to_string(), 10.0));

  let pop_max2: Vec<(String, f64)> = con.zpopmax("z_pop", 2)?;
  assert_eq!(
    pop_max2,
    vec![("p4".to_string(), 40.0), ("p3".to_string(), 30.0)]
  );

  assert_eq!(con.zcard::<_, i64>("z_pop")?, 1);

  // ZMPOP
  let _: i64 = redis::cmd("ZADD")
    .arg("z_mpop1")
    .arg(1)
    .arg("m1")
    .arg(2)
    .arg("m2")
    .query(&mut con)?;
  let zmpop_res: (String, Vec<(String, String)>) = redis::cmd("ZMPOP")
    .arg(1)
    .arg("z_mpop1")
    .arg("MIN")
    .arg("COUNT")
    .arg(2)
    .query(&mut con)?;
  assert_eq!(zmpop_res.0, "z_mpop1");
  assert_eq!(
    zmpop_res.1,
    vec![
      ("m1".to_string(), "1".to_string()),
      ("m2".to_string(), "2".to_string())
    ]
  );

  // 7. ZREMRANGEBYRANK / SCORE / LEX
  let _: i64 = redis::cmd("ZADD")
    .arg("z_del")
    .arg(1)
    .arg("d1")
    .arg(2)
    .arg("d2")
    .arg(3)
    .arg("d3")
    .arg(4)
    .arg("d4")
    .arg(5)
    .arg("d5")
    .query(&mut con)?;

  let rem_rank: i64 = con.zremrangebyrank("z_del", 0, 1)?;
  assert_eq!(rem_rank, 2); // d1, d2 删除

  let rem_score: i64 = con.zrembyscore("z_del", 4, 5)?;
  assert_eq!(rem_score, 2); // d4, d5 删除

  assert_eq!(con.zcard::<_, i64>("z_del")?, 1);
  assert_eq!(con.zrange::<_, Vec<String>>("z_del", 0, -1)?, vec!["d3"]);

  // 8. ZRANDMEMBER
  let _: i64 = redis::cmd("ZADD")
    .arg("z_rand")
    .arg(10)
    .arg("r1")
    .arg(20)
    .arg("r2")
    .arg(30)
    .arg("r3")
    .query(&mut con)?;

  let rand_one: String = redis::cmd("ZRANDMEMBER").arg("z_rand").query(&mut con)?;
  assert!(["r1", "r2", "r3"].contains(&rand_one.as_str()));

  let rand_pos2: Vec<String> = redis::cmd("ZRANDMEMBER")
    .arg("z_rand")
    .arg(2)
    .query(&mut con)?;
  assert_eq!(rand_pos2.len(), 2);
  assert_ne!(rand_pos2[0], rand_pos2[1]);

  let rand_neg5: Vec<String> = redis::cmd("ZRANDMEMBER")
    .arg("z_rand")
    .arg(-5)
    .query(&mut con)?;
  assert_eq!(rand_neg5.len(), 5);

  // 9. ZRANGESTORE
  let rangestore_cnt: i64 = redis::cmd("ZRANGESTORE")
    .arg("z_dst_store")
    .arg("z_rand")
    .arg(0)
    .arg(1)
    .query(&mut con)?;
  assert_eq!(rangestore_cnt, 2);
  assert_eq!(con.zcard::<_, i64>("z_dst_store")?, 2);

  // 10. 集合运算：ZINTER / ZUNION / ZDIFF / ZINTERSTORE / ZUNIONSTORE / ZDIFFSTORE / ZINTERCARD
  let _: i64 = redis::cmd("ZADD")
    .arg("z_s1")
    .arg(1.0)
    .arg("a")
    .arg(2.0)
    .arg("b")
    .arg(3.0)
    .arg("c")
    .query(&mut con)?;
  let _: i64 = redis::cmd("ZADD")
    .arg("z_s2")
    .arg(10.0)
    .arg("b")
    .arg(20.0)
    .arg("c")
    .arg(30.0)
    .arg("d")
    .query(&mut con)?;

  // ZINTER 带权重 WEIGHTS 2 3 与 AGGREGATE MAX
  let zinter_res: Vec<(String, f64)> = redis::cmd("ZINTER")
    .arg(2)
    .arg("z_s1")
    .arg("z_s2")
    .arg("WEIGHTS")
    .arg(2)
    .arg(3)
    .arg("AGGREGATE")
    .arg("MAX")
    .arg("WITHSCORES")
    .query(&mut con)?;
  // b: max(2*2=4, 10*3=30) = 30; c: max(3*2=6, 20*3=60) = 60
  assert_eq!(
    zinter_res,
    vec![("b".to_string(), 30.0), ("c".to_string(), 60.0)]
  );

  // ZINTERCARD
  let intercard: i64 = redis::cmd("ZINTERCARD")
    .arg(2)
    .arg("z_s1")
    .arg("z_s2")
    .query(&mut con)?;
  assert_eq!(intercard, 2);

  // ZINTERSTORE
  let interstore_cnt: i64 = redis::cmd("ZINTERSTORE")
    .arg("z_inter_dst")
    .arg(2)
    .arg("z_s1")
    .arg("z_s2")
    .query(&mut con)?;
  assert_eq!(interstore_cnt, 2);
  assert_eq!(con.zscore::<_, _, f64>("z_inter_dst", "b")?, 12.0); // 2.0 + 10.0 = 12.0

  // ZUNION
  let zunion_res: Vec<String> = redis::cmd("ZUNION")
    .arg(2)
    .arg("z_s1")
    .arg("z_s2")
    .query(&mut con)?;
  assert_eq!(zunion_res, vec!["a", "b", "c", "d"]);

  // ZUNIONSTORE
  let unionstore_cnt: i64 = redis::cmd("ZUNIONSTORE")
    .arg("z_union_dst")
    .arg(2)
    .arg("z_s1")
    .arg("z_s2")
    .query(&mut con)?;
  assert_eq!(unionstore_cnt, 4);

  // ZDIFF & ZDIFFSTORE
  let zdiff_res: Vec<String> = redis::cmd("ZDIFF")
    .arg(2)
    .arg("z_s1")
    .arg("z_s2")
    .query(&mut con)?;
  assert_eq!(zdiff_res, vec!["a"]);

  let zdiffstore_cnt: i64 = redis::cmd("ZDIFFSTORE")
    .arg("z_diff_dst")
    .arg(2)
    .arg("z_s1")
    .arg("z_s2")
    .query(&mut con)?;
  assert_eq!(zdiffstore_cnt, 1);
  assert_eq!(con.zcard::<_, i64>("z_diff_dst")?, 1);

  // 11. ZSCAN
  let _: i64 = redis::cmd("ZADD")
    .arg("z_scan")
    .arg(1.0)
    .arg("apple")
    .arg(2.0)
    .arg("banana")
    .arg(3.0)
    .arg("apricot")
    .arg(4.0)
    .arg("orange")
    .query(&mut con)?;

  let scan_res: (String, Vec<(String, f64)>) = redis::cmd("ZSCAN")
    .arg("z_scan")
    .arg(0)
    .arg("MATCH")
    .arg("ap*")
    .arg("COUNT")
    .arg(10)
    .query(&mut con)?;
  assert_eq!(scan_res.0, "0");
  assert_eq!(
    scan_res.1,
    vec![("apple".to_string(), 1.0), ("apricot".to_string(), 3.0)]
  );

  // 12. DEL 清理验证
  let del_cnt: i64 = con.del("z_order")?;
  assert_eq!(del_cnt, 1);
  assert_eq!(con.zcard::<_, i64>("z_order")?, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis ZSet comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_sortedint_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();
  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 8,
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

  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. SIADD 批量添加与去重校验
  let add_cnt: i64 = redis::cmd("SIADD")
        .arg("si_test")
        .arg(10)
        .arg(30)
        .arg(20)
        .arg(30) // 重复输入应只算一次
        .query(&mut con)?;
  assert_eq!(add_cnt, 3);

  let add_cnt2: i64 = redis::cmd("SIADD")
    .arg("si_test")
    .arg(40)
    .arg(50)
    .query(&mut con)?;
  assert_eq!(add_cnt2, 2);

  let add_cnt_dup: i64 = redis::cmd("SIADD")
    .arg("si_test")
    .arg(10)
    .arg(20)
    .query(&mut con)?;
  assert_eq!(add_cnt_dup, 0);

  // 2. SICARD 基数查询
  let card: i64 = redis::cmd("SICARD").arg("si_test").query(&mut con)?;
  assert_eq!(card, 5);

  let card_none: i64 = redis::cmd("SICARD").arg("si_non_exist").query(&mut con)?;
  assert_eq!(card_none, 0);

  // 3. SIEXISTS 批量存在性检测
  let exists: Vec<i64> = redis::cmd("SIEXISTS")
    .arg("si_test")
    .arg(10)
    .arg(15)
    .arg(20)
    .arg(99)
    .query(&mut con)?;
  assert_eq!(exists, vec![1, 0, 1, 0]);

  let exists_single: Vec<i64> = redis::cmd("SIEXISTS")
    .arg("si_test")
    .arg(30)
    .query(&mut con)?;
  assert_eq!(exists_single, vec![1]);

  let exists_none: Vec<i64> = redis::cmd("SIEXISTS")
    .arg("si_non_exist")
    .arg(10)
    .arg(20)
    .query(&mut con)?;
  assert_eq!(exists_none, vec![0, 0]);

  // 4. SIRANGE 升序范围扫描与游标分页
  let range_all: Vec<String> = redis::cmd("SIRANGE")
    .arg("si_test")
    .arg(0)
    .arg(10)
    .query(&mut con)?;
  assert_eq!(range_all, vec!["10", "20", "30", "40", "50"]);

  let range_offset: Vec<String> = redis::cmd("SIRANGE")
    .arg("si_test")
    .arg(1)
    .arg(2)
    .query(&mut con)?;
  assert_eq!(range_offset, vec!["20", "30"]);

  let range_cursor: Vec<String> = redis::cmd("SIRANGE")
    .arg("si_test")
    .arg(0)
    .arg(2)
    .arg("CURSOR")
    .arg(20)
    .query(&mut con)?;
  assert_eq!(range_cursor, vec!["30", "40"]);

  // 5. SIREVRANGE 降序范围扫描与游标分页
  let rev_all: Vec<String> = redis::cmd("SIREVRANGE")
    .arg("si_test")
    .arg(0)
    .arg(10)
    .query(&mut con)?;
  assert_eq!(rev_all, vec!["50", "40", "30", "20", "10"]);

  let rev_offset: Vec<String> = redis::cmd("SIREVRANGE")
    .arg("si_test")
    .arg(1)
    .arg(2)
    .query(&mut con)?;
  assert_eq!(rev_offset, vec!["40", "30"]);

  let rev_cursor: Vec<String> = redis::cmd("SIREVRANGE")
    .arg("si_test")
    .arg(0)
    .arg(2)
    .arg("CURSOR")
    .arg(40)
    .query(&mut con)?;
  assert_eq!(rev_cursor, vec!["30", "20"]);

  // 6. SIRANGEBYVALUE 值区间检索（开闭区间、无穷大、LIMIT 分页）
  let rbv_inclusive: Vec<String> = redis::cmd("SIRANGEBYVALUE")
    .arg("si_test")
    .arg(20)
    .arg(40)
    .query(&mut con)?;
  assert_eq!(rbv_inclusive, vec!["20", "30", "40"]);

  let rbv_exclusive_min: Vec<String> = redis::cmd("SIRANGEBYVALUE")
    .arg("si_test")
    .arg("(20")
    .arg("[40")
    .query(&mut con)?;
  assert_eq!(rbv_exclusive_min, vec!["30", "40"]);

  let rbv_exclusive_max: Vec<String> = redis::cmd("SIRANGEBYVALUE")
    .arg("si_test")
    .arg("[20")
    .arg("(40")
    .query(&mut con)?;
  assert_eq!(rbv_exclusive_max, vec!["20", "30"]);

  let rbv_exclusive_both: Vec<String> = redis::cmd("SIRANGEBYVALUE")
    .arg("si_test")
    .arg("(20")
    .arg("(40")
    .query(&mut con)?;
  assert_eq!(rbv_exclusive_both, vec!["30"]);

  let rbv_inf_limit: Vec<String> = redis::cmd("SIRANGEBYVALUE")
    .arg("si_test")
    .arg("-inf")
    .arg("+inf")
    .arg("LIMIT")
    .arg(1)
    .arg(3)
    .query(&mut con)?;
  assert_eq!(rbv_inf_limit, vec!["20", "30", "40"]);

  // 7. SIREVRANGEBYVALUE 逆序值区间检索
  let rev_rbv_inclusive: Vec<String> = redis::cmd("SIREVRANGEBYVALUE")
    .arg("si_test")
    .arg(40)
    .arg(20)
    .query(&mut con)?;
  assert_eq!(rev_rbv_inclusive, vec!["40", "30", "20"]);

  let rev_rbv_exclusive_max: Vec<String> = redis::cmd("SIREVRANGEBYVALUE")
    .arg("si_test")
    .arg("(40")
    .arg("[20")
    .query(&mut con)?;
  assert_eq!(rev_rbv_exclusive_max, vec!["30", "20"]);

  let rev_rbv_exclusive_min: Vec<String> = redis::cmd("SIREVRANGEBYVALUE")
    .arg("si_test")
    .arg("[40")
    .arg("(20")
    .query(&mut con)?;
  assert_eq!(rev_rbv_exclusive_min, vec!["40", "30"]);

  let rev_rbv_exclusive_both: Vec<String> = redis::cmd("SIREVRANGEBYVALUE")
    .arg("si_test")
    .arg("(40")
    .arg("(20")
    .query(&mut con)?;
  assert_eq!(rev_rbv_exclusive_both, vec!["30"]);

  let rev_rbv_inf_limit: Vec<String> = redis::cmd("SIREVRANGEBYVALUE")
    .arg("si_test")
    .arg("+inf")
    .arg("-inf")
    .arg("LIMIT")
    .arg(1)
    .arg(3)
    .query(&mut con)?;
  assert_eq!(rev_rbv_inf_limit, vec!["40", "30", "20"]);

  // 8. TYPE 识别验证
  let type_res: String = redis::cmd("TYPE").arg("si_test").query(&mut con)?;
  assert_eq!(type_res, "sortedint");

  // 9. SIREM 部分删除与清空元数据自动级联清理
  let rem_cnt1: i64 = redis::cmd("SIREM")
        .arg("si_test")
        .arg(20)
        .arg(99) // 99 不存在
        .query(&mut con)?;
  assert_eq!(rem_cnt1, 1);

  let card_after_rem1: i64 = redis::cmd("SICARD").arg("si_test").query(&mut con)?;
  assert_eq!(card_after_rem1, 4);

  let exists_after_rem: Vec<i64> = redis::cmd("SIEXISTS")
    .arg("si_test")
    .arg(20)
    .query(&mut con)?;
  assert_eq!(exists_after_rem, vec![0]);

  // 删空剩余项
  let rem_cnt2: i64 = redis::cmd("SIREM")
    .arg("si_test")
    .arg(10)
    .arg(30)
    .arg(40)
    .arg(50)
    .query(&mut con)?;
  assert_eq!(rem_cnt2, 4);

  let card_empty: i64 = redis::cmd("SICARD").arg("si_test").query(&mut con)?;
  assert_eq!(card_empty, 0);

  let type_after_empty: String = redis::cmd("TYPE").arg("si_test").query(&mut con)?;
  assert_eq!(type_after_empty, "none");

  // 10. DEL 级联物理删除验证
  let _: i64 = redis::cmd("SIADD")
    .arg("del_si")
    .arg(100)
    .arg(200)
    .arg(300)
    .query(&mut con)?;
  let del_cnt: i64 = con.del("del_si")?;
  assert_eq!(del_cnt, 1);
  assert_eq!(con.del::<_, i64>("del_si")?, 0);

  let card_after_del: i64 = redis::cmd("SICARD").arg("del_si").query(&mut con)?;
  assert_eq!(card_after_del, 0);

  let range_after_del: Vec<String> = redis::cmd("SIRANGE")
    .arg("del_si")
    .arg(0)
    .arg(10)
    .query(&mut con)?;
  assert!(range_after_del.is_empty());

  // 11. 多命名空间多租户隔离验证
  let _: () = redis::cmd("NAMESPACE")
    .arg("ADD")
    .arg("ns_si")
    .arg("tok_si")
    .query(&mut con)?;

  let mut con_ns = client.get_connection()?;
  let _: () = redis::cmd("AUTH").arg("tok_si").query(&mut con_ns)?;

  let ns_add_cnt: i64 = redis::cmd("SIADD")
    .arg("si_test")
    .arg(999)
    .arg(1000)
    .query(&mut con_ns)?;
  assert_eq!(ns_add_cnt, 2);

  let card_ns: i64 = redis::cmd("SICARD").arg("si_test").query(&mut con_ns)?;
  assert_eq!(card_ns, 2);

  let card_default: i64 = redis::cmd("SICARD").arg("si_test").query(&mut con)?;
  assert_eq!(card_default, 0);

  let range_ns: Vec<String> = redis::cmd("SIRANGE")
    .arg("si_test")
    .arg(0)
    .arg(10)
    .query(&mut con_ns)?;
  assert_eq!(range_ns, vec!["999", "1000"]);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis SortedInt comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_stream_comprehensive() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 9,
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
  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. XADD 与自动 ID 生成
  let id1: String = redis::cmd("XADD")
    .arg("mystream")
    .arg("*")
    .arg("sensor-id")
    .arg("1234")
    .arg("temperature")
    .arg("19.8")
    .query(&mut con)?;
  assert!(id1.contains('-'));

  let id2: String = redis::cmd("XADD")
    .arg("mystream")
    .arg("*")
    .arg("sensor-id")
    .arg("1235")
    .arg("temperature")
    .arg("20.1")
    .query(&mut con)?;
  assert!(id2 > id1);

  // 2. XLEN
  let len: i64 = redis::cmd("XLEN").arg("mystream").query(&mut con)?;
  assert_eq!(len, 2);

  // 3. NOMKSTREAM 选项
  let nomk_res: redis::Value = redis::cmd("XADD")
    .arg("nonexistent_stream")
    .arg("NOMKSTREAM")
    .arg("*")
    .arg("k")
    .arg("v")
    .query(&mut con)?;
  assert_eq!(nomk_res, redis::Value::Nil);

  // 4. XRANGE
  let range: redis::Value = redis::cmd("XRANGE")
    .arg("mystream")
    .arg("-")
    .arg("+")
    .query(&mut con)?;
  if let redis::Value::Array(items) = range {
    assert_eq!(items.len(), 2);
  } else {
    panic!("Expected array from XRANGE");
  }

  // 5. XREVRANGE
  let rev_range: redis::Value = redis::cmd("XREVRANGE")
    .arg("mystream")
    .arg("+")
    .arg("-")
    .arg("COUNT")
    .arg(1)
    .query(&mut con)?;
  if let redis::Value::Array(items) = rev_range {
    assert_eq!(items.len(), 1);
  } else {
    panic!("Expected array from XREVRANGE");
  }

  // 6. XTRIM MAXLEN
  let _: String = redis::cmd("XADD")
    .arg("mystream")
    .arg("*")
    .arg("sensor-id")
    .arg("1236")
    .arg("temperature")
    .arg("21.5")
    .query(&mut con)?;

  let trimmed: i64 = redis::cmd("XTRIM")
    .arg("mystream")
    .arg("MAXLEN")
    .arg(2)
    .query(&mut con)?;
  assert_eq!(trimmed, 1);

  let len_after_trim: i64 = redis::cmd("XLEN").arg("mystream").query(&mut con)?;
  assert_eq!(len_after_trim, 2);

  // 7. XINFO STREAM
  let info_stream: redis::Value = redis::cmd("XINFO")
    .arg("STREAM")
    .arg("mystream")
    .query(&mut con)?;
  if let redis::Value::Array(info) = info_stream {
    assert!(info.len() >= 4);
  } else {
    panic!("Expected array from XINFO STREAM");
  }

  // 8. 消费组: XGROUP CREATE / READGROUP / PENDING / ACK
  let _: () = redis::cmd("XGROUP")
    .arg("CREATE")
    .arg("mystream")
    .arg("mygroup")
    .arg("0-0")
    .query(&mut con)?;

  let read_group: redis::Value = redis::cmd("XREADGROUP")
    .arg("GROUP")
    .arg("mygroup")
    .arg("consumer1")
    .arg("COUNT")
    .arg(1)
    .arg("STREAMS")
    .arg("mystream")
    .arg(">")
    .query(&mut con)?;
  if let redis::Value::Array(streams) = read_group {
    assert_eq!(streams.len(), 1);
  } else {
    panic!("Expected array from XREADGROUP");
  }

  let pending: redis::Value = redis::cmd("XPENDING")
    .arg("mystream")
    .arg("mygroup")
    .query(&mut con)?;
  if let redis::Value::Array(p_items) = pending {
    assert_eq!(p_items[0], redis::Value::Int(1));
  } else {
    panic!("Expected array from XPENDING");
  }

  let acked: i64 = redis::cmd("XACK")
    .arg("mystream")
    .arg("mygroup")
    .arg(&id2)
    .query(&mut con)?;
  assert_eq!(acked, 1);

  let pending_after_ack: redis::Value = redis::cmd("XPENDING")
    .arg("mystream")
    .arg("mygroup")
    .query(&mut con)?;
  if let redis::Value::Array(p_items) = pending_after_ack {
    assert_eq!(p_items[0], redis::Value::Int(0));
  }

  // 9. XDEL
  let del_cnt: i64 = redis::cmd("XDEL")
    .arg("mystream")
    .arg(&id2)
    .query(&mut con)?;
  assert!(del_cnt <= 1);

  // 10. 全局 DEL 级联清理
  let _: () = redis::cmd("DEL").arg("mystream").query(&mut con)?;

  let len_after_del: i64 = redis::cmd("XLEN").arg("mystream").query(&mut con)?;
  assert_eq!(len_after_del, 0);

  // 11. 多命名空间多租户隔离验证
  let _: () = redis::cmd("NAMESPACE")
    .arg("ADD")
    .arg("ns_stream")
    .arg("tok_stream")
    .query(&mut con)?;

  let mut con_ns = client.get_connection()?;
  let _: () = redis::cmd("AUTH").arg("tok_stream").query(&mut con_ns)?;

  let ns_id: String = redis::cmd("XADD")
    .arg("stream_test")
    .arg("*")
    .arg("tenant")
    .arg("corp_a")
    .query(&mut con_ns)?;
  assert!(ns_id.contains('-'));

  let ns_len: i64 = redis::cmd("XLEN").arg("stream_test").query(&mut con_ns)?;
  assert_eq!(ns_len, 1);

  let default_len: i64 = redis::cmd("XLEN").arg("stream_test").query(&mut con)?;
  assert_eq!(default_len, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis Stream comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_bloom_chain_comprehensive() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 10,
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
  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. BF.RESERVE 基本创建与重复报错
  let res: String = redis::cmd("BF.RESERVE")
    .arg("bf1")
    .arg("0.01")
    .arg("10")
    .query(&mut con)?;
  assert_eq!(res, "OK");

  let err_res: redis::RedisResult<String> = redis::cmd("BF.RESERVE")
    .arg("bf1")
    .arg("0.01")
    .arg("10")
    .query(&mut con);
  assert!(err_res.is_err());

  // 2. TYPE 返回 MBbloom--
  let key_type: String = redis::cmd("TYPE").arg("bf1").query(&mut con)?;
  assert_eq!(key_type, "MBbloom--");

  // 3. BF.ADD 与 BF.EXISTS
  let add_res: i64 = redis::cmd("BF.ADD")
    .arg("bf1")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(add_res, 1);

  let add_dup: i64 = redis::cmd("BF.ADD")
    .arg("bf1")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(add_dup, 0);

  let exists: i64 = redis::cmd("BF.EXISTS")
    .arg("bf1")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(exists, 1);

  let not_exists: i64 = redis::cmd("BF.EXISTS")
    .arg("bf1")
    .arg("world")
    .query(&mut con)?;
  assert_eq!(not_exists, 0);

  // 4. BF.MADD 与 BF.MEXISTS
  let madd_res: Vec<i64> = redis::cmd("BF.MADD")
    .arg("bf1")
    .arg("k1")
    .arg("k2")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(madd_res, vec![1, 1, 0]);

  let mexists_res: Vec<i64> = redis::cmd("BF.MEXISTS")
    .arg("bf1")
    .arg("k1")
    .arg("k2")
    .arg("unknown")
    .query(&mut con)?;
  assert_eq!(mexists_res, vec![1, 1, 0]);

  // 5. BF.CARD 与 BF.INFO
  let card: i64 = redis::cmd("BF.CARD").arg("bf1").query(&mut con)?;
  assert_eq!(card, 3); // hello, k1, k2

  let filters_count: i64 = redis::cmd("BF.INFO")
    .arg("bf1")
    .arg("FILTERS")
    .query(&mut con)?;
  assert_eq!(filters_count, 1);

  // 6. 动态扩容测试 (Auto-scaling)
  for i in 0..25 {
    let _: i64 = redis::cmd("BF.ADD")
      .arg("bf1")
      .arg(format!("elem_{i}"))
      .query(&mut con)?;
  }
  let scaled_filters: i64 = redis::cmd("BF.INFO")
    .arg("bf1")
    .arg("FILTERS")
    .query(&mut con)?;
  assert!(scaled_filters >= 2);

  for i in 0..25 {
    let e: i64 = redis::cmd("BF.EXISTS")
      .arg("bf1")
      .arg(format!("elem_{i}"))
      .query(&mut con)?;
    assert_eq!(e, 1);
  }

  // 7. BF.INSERT 指令
  let ins_res: Vec<i64> = redis::cmd("BF.INSERT")
    .arg("bf_ins")
    .arg("CAPACITY")
    .arg("50")
    .arg("ERROR")
    .arg("0.02")
    .arg("ITEMS")
    .arg("item1")
    .arg("item2")
    .query(&mut con)?;
  assert_eq!(ins_res, vec![1, 1]);

  // 8. NONSCALING 过滤器
  let _: String = redis::cmd("BF.RESERVE")
    .arg("bf_fixed")
    .arg("0.01")
    .arg("5")
    .arg("NONSCALING")
    .query(&mut con)?;

  for i in 0..5 {
    let _: i64 = redis::cmd("BF.ADD")
      .arg("bf_fixed")
      .arg(format!("fixed_{i}"))
      .query(&mut con)?;
  }
  let overflow_res: redis::RedisResult<i64> = redis::cmd("BF.ADD")
    .arg("bf_fixed")
    .arg("overflow")
    .query(&mut con);
  assert!(overflow_res.is_err());

  // 9. 级联清理 (DEL)
  let del_res: i64 = redis::cmd("DEL").arg("bf1").query(&mut con)?;
  assert_eq!(del_res, 1);

  let exists_after_del: i64 = redis::cmd("EXISTS").arg("bf1").query(&mut con)?;
  assert_eq!(exists_after_del, 0);

  // 10. 多命名空间隔离
  let _: () = redis::cmd("NAMESPACE")
    .arg("ADD")
    .arg("ns_bloom")
    .arg("tok_bloom")
    .query(&mut con)?;

  let mut con_ns = client.get_connection()?;
  let _: () = redis::cmd("AUTH").arg("tok_bloom").query(&mut con_ns)?;

  let _: String = redis::cmd("BF.RESERVE")
    .arg("bf_tenant")
    .arg("0.01")
    .arg("100")
    .query(&mut con_ns)?;

  let _: i64 = redis::cmd("BF.ADD")
    .arg("bf_tenant")
    .arg("tenant_item")
    .query(&mut con_ns)?;

  let in_ns: i64 = redis::cmd("BF.EXISTS")
    .arg("bf_tenant")
    .arg("tenant_item")
    .query(&mut con_ns)?;
  assert_eq!(in_ns, 1);

  let in_default: i64 = redis::cmd("BF.EXISTS")
    .arg("bf_tenant")
    .arg("tenant_item")
    .query(&mut con)?;
  assert_eq!(in_default, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis Bloom Chain comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_cuckoo_chain_comprehensive() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 11,
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
  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. CF.RESERVE 基本创建与参数校验
  let res: String = redis::cmd("CF.RESERVE")
    .arg("cf1")
    .arg("64")
    .arg("BUCKETSIZE")
    .arg("2")
    .arg("MAXITERATIONS")
    .arg("50")
    .arg("EXPANSION")
    .arg("2")
    .query(&mut con)?;
  assert_eq!(res, "OK");

  let dup_err: redis::RedisResult<String> = redis::cmd("CF.RESERVE")
    .arg("cf1")
    .arg("64")
    .query(&mut con);
  assert!(dup_err.is_err());

  // 2. TYPE 命令返回 MBbloomCF
  let key_type: String = redis::cmd("TYPE").arg("cf1").query(&mut con)?;
  assert_eq!(key_type, "MBbloomCF");

  // 3. CF.ADD 与 CF.EXISTS
  let add1: i64 = redis::cmd("CF.ADD")
    .arg("cf1")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(add1, 1);

  let exists: i64 = redis::cmd("CF.EXISTS")
    .arg("cf1")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(exists, 1);

  let not_exists: i64 = redis::cmd("CF.EXISTS")
    .arg("cf1")
    .arg("world")
    .query(&mut con)?;
  assert_eq!(not_exists, 0);

  // 4. CF.ADDNX 测试
  let addnx1: i64 = redis::cmd("CF.ADDNX")
    .arg("cf1")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(addnx1, 0);

  let addnx2: i64 = redis::cmd("CF.ADDNX")
    .arg("cf1")
    .arg("new_key")
    .query(&mut con)?;
  assert_eq!(addnx2, 1);

  // 5. CF.MEXISTS 与 CF.COUNT
  let mexists: Vec<i64> = redis::cmd("CF.MEXISTS")
    .arg("cf1")
    .arg("hello")
    .arg("new_key")
    .arg("none")
    .query(&mut con)?;
  assert_eq!(mexists, vec![1, 1, 0]);

  let count: i64 = redis::cmd("CF.COUNT")
    .arg("cf1")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(count, 1);

  // 6. CF.DEL 测试与重复删除
  let del1: i64 = redis::cmd("CF.DEL")
    .arg("cf1")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(del1, 1);

  let del2: i64 = redis::cmd("CF.DEL")
    .arg("cf1")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(del2, 0);

  let exists_after_del: i64 = redis::cmd("CF.EXISTS")
    .arg("cf1")
    .arg("hello")
    .query(&mut con)?;
  assert_eq!(exists_after_del, 0);

  // 7. CF.INSERT 与 CF.INSERTNX
  let ins_res: Vec<i64> = redis::cmd("CF.INSERT")
    .arg("cf_ins")
    .arg("CAPACITY")
    .arg("100")
    .arg("ITEMS")
    .arg("a")
    .arg("b")
    .query(&mut con)?;
  assert_eq!(ins_res, vec![1, 1]);

  let insnx_res: Vec<i64> = redis::cmd("CF.INSERTNX")
    .arg("cf_ins")
    .arg("ITEMS")
    .arg("a")
    .arg("c")
    .query(&mut con)?;
  assert_eq!(insnx_res, vec![0, 1]);

  // 8. CF.INFO 查看指标
  let info: Vec<redis::Value> = redis::cmd("CF.INFO").arg("cf1").query(&mut con)?;
  assert!(!info.is_empty());

  // 9. 级联清理 (DEL)
  let del_res: i64 = redis::cmd("DEL").arg("cf1").query(&mut con)?;
  assert_eq!(del_res, 1);

  let exists_cf: i64 = redis::cmd("EXISTS").arg("cf1").query(&mut con)?;
  assert_eq!(exists_cf, 0);

  // 10. 多命名空间隔离
  let _: () = redis::cmd("NAMESPACE")
    .arg("ADD")
    .arg("ns_cuckoo")
    .arg("tok_cuckoo")
    .query(&mut con)?;

  let mut con_ns = client.get_connection()?;
  let _: () = redis::cmd("AUTH").arg("tok_cuckoo").query(&mut con_ns)?;

  let _: String = redis::cmd("CF.RESERVE")
    .arg("cf_tenant")
    .arg("128")
    .query(&mut con_ns)?;

  let _: i64 = redis::cmd("CF.ADD")
    .arg("cf_tenant")
    .arg("secret")
    .query(&mut con_ns)?;

  let in_ns: i64 = redis::cmd("CF.EXISTS")
    .arg("cf_tenant")
    .arg("secret")
    .query(&mut con_ns)?;
  assert_eq!(in_ns, 1);

  let in_default: i64 = redis::cmd("CF.EXISTS")
    .arg("cf_tenant")
    .arg("secret")
    .query(&mut con)?;
  assert_eq!(in_default, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis Cuckoo Chain comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_timeseries_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 11,
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
  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. TS.CREATE
  let create_res: String = redis::cmd("TS.CREATE")
    .arg("ts:temp")
    .arg("RETENTION")
    .arg(100000)
    .arg("CHUNK_SIZE")
    .arg(256)
    .arg("DUPLICATE_POLICY")
    .arg("LAST")
    .arg("LABELS")
    .arg("sensor_id")
    .arg("101")
    .arg("location")
    .arg("building_a")
    .query(&mut con)?;
  assert_eq!(create_res, "OK");

  // 2. TYPE 命令精确返回 timeseries
  let type_res: String = redis::cmd("TYPE").arg("ts:temp").query(&mut con)?;
  assert_eq!(type_res, "timeseries");

  // 3. TS.ALTER
  let alter_res: String = redis::cmd("TS.ALTER")
    .arg("ts:temp")
    .arg("RETENTION")
    .arg(200000)
    .arg("LABELS")
    .arg("sensor_id")
    .arg("101")
    .arg("location")
    .arg("building_b")
    .query(&mut con)?;
  assert_eq!(alter_res, "OK");

  // 4. TS.ADD 写入采样点
  let t1: i64 = redis::cmd("TS.ADD")
    .arg("ts:temp")
    .arg(1000)
    .arg(20.5)
    .query(&mut con)?;
  assert_eq!(t1, 1000);

  let t2: i64 = redis::cmd("TS.ADD")
    .arg("ts:temp")
    .arg(2000)
    .arg(21.5)
    .query(&mut con)?;
  assert_eq!(t2, 2000);

  let t3: i64 = redis::cmd("TS.ADD")
    .arg("ts:temp")
    .arg(3000)
    .arg(22.5)
    .query(&mut con)?;
  assert_eq!(t3, 3000);

  let t4: i64 = redis::cmd("TS.ADD")
    .arg("ts:temp")
    .arg(4000)
    .arg(23.5)
    .query(&mut con)?;
  assert_eq!(t4, 4000);

  // 重复时间戳写入按 LAST 策略覆盖
  let t2_dup: i64 = redis::cmd("TS.ADD")
    .arg("ts:temp")
    .arg(2000)
    .arg(25.0)
    .query(&mut con)?;
  assert_eq!(t2_dup, 2000);

  // 5. TS.GET 获取最新点
  let last_sample: Vec<redis::Value> = redis::cmd("TS.GET").arg("ts:temp").query(&mut con)?;
  assert_eq!(last_sample.len(), 2);
  if let (redis::Value::Int(ts), redis::Value::BulkString(val_bytes)) =
    (&last_sample[0], &last_sample[1])
  {
    assert_eq!(*ts, 4000);
    let val_str = from_utf8(val_bytes).unwrap();
    assert_eq!(val_str, "23.5");
  } else {
    panic!("Invalid TS.GET response: {:?}", last_sample);
  }

  // 6. TS.RANGE 范围查询
  let range_res: Vec<Vec<redis::Value>> = redis::cmd("TS.RANGE")
    .arg("ts:temp")
    .arg(1000)
    .arg(3000)
    .query(&mut con)?;
  assert_eq!(range_res.len(), 3); // 1000, 2000 (updated), 3000

  // 7. TS.RANGE 带 AGGREGATION
  let agg_res: Vec<Vec<redis::Value>> = redis::cmd("TS.RANGE")
    .arg("ts:temp")
    .arg(1000)
    .arg(4000)
    .arg("AGGREGATION")
    .arg("avg")
    .arg(2000)
    .query(&mut con)?;
  assert!(!agg_res.is_empty());

  // 8. TS.REVRANGE 倒序查询
  let rev_res: Vec<Vec<redis::Value>> = redis::cmd("TS.REVRANGE")
    .arg("ts:temp")
    .arg(1000)
    .arg(4000)
    .arg("COUNT")
    .arg(2)
    .query(&mut con)?;
  assert_eq!(rev_res.len(), 2);
  if let redis::Value::Int(ts) = &rev_res[0][0] {
    assert_eq!(*ts, 4000);
  }

  // 9. TS.MADD 批量插入
  let create_ts2: String = redis::cmd("TS.CREATE")
    .arg("ts:pressure")
    .arg("LABELS")
    .arg("sensor_id")
    .arg("102")
    .arg("location")
    .arg("building_b")
    .query(&mut con)?;
  assert_eq!(create_ts2, "OK");

  let madd_res: Vec<i64> = redis::cmd("TS.MADD")
    .arg("ts:pressure")
    .arg(1000)
    .arg(101.3)
    .arg("ts:pressure")
    .arg(2000)
    .arg(101.5)
    .query(&mut con)?;
  assert_eq!(madd_res.len(), 2);
  assert_eq!(madd_res[0], 1000);
  assert_eq!(madd_res[1], 2000);

  // 10. TS.INCRBY / TS.DECRBY
  let incr_ts: i64 = redis::cmd("TS.INCRBY")
    .arg("ts:temp")
    .arg(1.5)
    .arg("TIMESTAMP")
    .arg(5000)
    .query(&mut con)?;
  assert_eq!(incr_ts, 5000);

  let get_incr: Vec<redis::Value> = redis::cmd("TS.GET").arg("ts:temp").query(&mut con)?;
  if let redis::Value::BulkString(val_bytes) = &get_incr[1] {
    let val: f64 = from_utf8(val_bytes).unwrap().parse().unwrap();
    assert!((val - 25.0).abs() < 1e-6); // 23.5 + 1.5 = 25.0
  }

  // 11. TS.QUERYINDEX 标签过滤
  let matched_keys: Vec<String> = redis::cmd("TS.QUERYINDEX")
    .arg("location=building_b")
    .query(&mut con)?;
  assert_eq!(matched_keys.len(), 2);
  assert!(matched_keys.contains(&"ts:temp".to_string()));
  assert!(matched_keys.contains(&"ts:pressure".to_string()));

  // 12. TS.MGET 多时序获取
  let mget_res: Vec<redis::Value> = redis::cmd("TS.MGET")
    .arg("WITHLABELS")
    .arg("FILTER")
    .arg("location=building_b")
    .query(&mut con)?;
  assert_eq!(mget_res.len(), 2);

  // 13. TS.MRANGE 多时序范围
  let mrange_res: Vec<redis::Value> = redis::cmd("TS.MRANGE")
    .arg(1000)
    .arg(5000)
    .arg("WITHLABELS")
    .arg("FILTER")
    .arg("location=building_b")
    .query(&mut con)?;
  assert_eq!(mrange_res.len(), 2);

  // 14. TS.DEL 删除指定区间采样点
  let del_samples: i64 = redis::cmd("TS.DEL")
    .arg("ts:temp")
    .arg(1000)
    .arg(2000)
    .query(&mut con)?;
  assert!(del_samples >= 2);

  // 15. TS.INFO 查看元数据
  let info_res: Vec<redis::Value> = redis::cmd("TS.INFO").arg("ts:temp").query(&mut con)?;
  assert!(!info_res.is_empty());

  // 16. DEL 级联清理
  let del_k: i64 = redis::cmd("DEL").arg("ts:temp").query(&mut con)?;
  assert_eq!(del_k, 1);

  let exists_ts: i64 = redis::cmd("EXISTS").arg("ts:temp").query(&mut con)?;
  assert_eq!(exists_ts, 0);

  // 17. 多命名空间隔离
  let _: () = redis::cmd("NAMESPACE")
    .arg("ADD")
    .arg("ns_ts")
    .arg("tok_ts")
    .query(&mut con)?;

  let mut con_ns = client.get_connection()?;
  let _: () = redis::cmd("AUTH").arg("tok_ts").query(&mut con_ns)?;

  let _: String = redis::cmd("TS.CREATE")
    .arg("ts:tenant")
    .query(&mut con_ns)?;

  let _: i64 = redis::cmd("TS.ADD")
    .arg("ts:tenant")
    .arg(1000)
    .arg(99.9)
    .query(&mut con_ns)?;

  let exists_in_ns: i64 = redis::cmd("EXISTS").arg("ts:tenant").query(&mut con_ns)?;
  assert_eq!(exists_in_ns, 1);

  let exists_in_default: i64 = redis::cmd("EXISTS").arg("ts:tenant").query(&mut con)?;
  assert_eq!(exists_in_default, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis TimeSeries comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_tdigest_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 12,
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
  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. TDIGEST.CREATE
  // 1. TDIGEST.CREATE
  let create_res: String = redis::cmd("TDIGEST.CREATE")
    .arg("td1")
    .arg("COMPRESSION")
    .arg(100)
    .query(&mut con)?;
  assert_eq!(create_res, "OK");

  // 2. TYPE 精确返回 TDIS-TYPE
  let type_res: String = redis::cmd("TYPE").arg("td1").query(&mut con)?;
  assert_eq!(type_res, "TDIS-TYPE");

  // 3. TDIGEST.ADD 批量添加观测值
  let add_res: String = redis::cmd("TDIGEST.ADD")
    .arg("td1")
    .arg(10.0)
    .arg(20.0)
    .arg(30.0)
    .arg(40.0)
    .arg(50.0)
    .arg(60.0)
    .arg(70.0)
    .arg(80.0)
    .arg(90.0)
    .arg(100.0)
    .query(&mut con)?;
  assert_eq!(add_res, "OK");

  // 4. TDIGEST.MIN / TDIGEST.MAX
  let min_val: String = redis::cmd("TDIGEST.MIN").arg("td1").query(&mut con)?;
  let min_f: f64 = min_val.parse().unwrap();
  assert!((min_f - 10.0).abs() < 1e-6);

  let max_val: String = redis::cmd("TDIGEST.MAX").arg("td1").query(&mut con)?;
  let max_f: f64 = max_val.parse().unwrap();
  assert!((max_f - 100.0).abs() < 1e-6);

  // 5. TDIGEST.QUANTILE
  let quantiles: Vec<String> = redis::cmd("TDIGEST.QUANTILE")
    .arg("td1")
    .arg(0.0)
    .arg(0.5)
    .arg(1.0)
    .query(&mut con)?;
  assert_eq!(quantiles.len(), 3);
  let median: f64 = quantiles[1].parse().unwrap();
  assert!((40.0..=70.0).contains(&median));

  // 6. TDIGEST.CDF
  let cdfs: Vec<String> = redis::cmd("TDIGEST.CDF")
    .arg("td1")
    .arg(55.0)
    .query(&mut con)?;
  assert_eq!(cdfs.len(), 1);
  let cdf_v: f64 = cdfs[0].parse().unwrap();
  assert!(cdf_v > 0.3 && cdf_v < 0.8);

  // 7. TDIGEST.RANK / TDIGEST.REVRANK
  let ranks: Vec<i64> = redis::cmd("TDIGEST.RANK")
    .arg("td1")
    .arg(50.0)
    .query(&mut con)?;
  assert_eq!(ranks.len(), 1);
  assert!((3..=6).contains(&ranks[0]));

  let rev_ranks: Vec<i64> = redis::cmd("TDIGEST.REVRANK")
    .arg("td1")
    .arg(50.0)
    .query(&mut con)?;
  assert_eq!(rev_ranks.len(), 1);
  assert!((3..=6).contains(&rev_ranks[0]));

  // 8. TDIGEST.BYRANK / TDIGEST.BYREVRANK
  let by_rank: Vec<String> = redis::cmd("TDIGEST.BYRANK")
    .arg("td1")
    .arg(0)
    .arg(9)
    .query(&mut con)?;
  assert_eq!(by_rank.len(), 2);

  let by_rev_rank: Vec<String> = redis::cmd("TDIGEST.BYREVRANK")
    .arg("td1")
    .arg(0)
    .query(&mut con)?;
  assert_eq!(by_rev_rank.len(), 1);

  // 9. TDIGEST.TRIMMED_MEAN
  let tmean_str: String = redis::cmd("TDIGEST.TRIMMED_MEAN")
    .arg("td1")
    .arg(0.1)
    .arg(0.9)
    .query(&mut con)?;
  let tmean: f64 = tmean_str.parse().unwrap();
  assert!((40.0..=70.0).contains(&tmean));

  // 10. TDIGEST.INFO
  let info: Vec<redis::Value> = redis::cmd("TDIGEST.INFO").arg("td1").query(&mut con)?;
  assert!(!info.is_empty());

  // 11. TDIGEST.MERGE
  let _: String = redis::cmd("TDIGEST.CREATE")
    .arg("td2")
    .arg("COMPRESSION")
    .arg(100)
    .query(&mut con)?;
  let _: String = redis::cmd("TDIGEST.ADD")
    .arg("td2")
    .arg(150.0)
    .arg(200.0)
    .query(&mut con)?;

  let merge_res: String = redis::cmd("TDIGEST.MERGE")
    .arg("td_merged")
    .arg(2)
    .arg("td1")
    .arg("td2")
    .query(&mut con)?;
  assert_eq!(merge_res, "OK");

  let merged_max: String = redis::cmd("TDIGEST.MAX").arg("td_merged").query(&mut con)?;
  let m_max: f64 = merged_max.parse().unwrap();
  assert!((m_max - 200.0).abs() < 1e-6);

  // 12. TDIGEST.RESET
  let reset_res: String = redis::cmd("TDIGEST.RESET").arg("td1").query(&mut con)?;
  assert_eq!(reset_res, "OK");

  let min_after_reset: String = redis::cmd("TDIGEST.MIN").arg("td1").query(&mut con)?;
  assert_eq!(min_after_reset, "nan");

  // 13. DEL 级联清理
  let del_res: i64 = redis::cmd("DEL").arg("td_merged").query(&mut con)?;
  assert_eq!(del_res, 1);

  let exists_td: i64 = redis::cmd("EXISTS").arg("td_merged").query(&mut con)?;
  assert_eq!(exists_td, 0);

  // 14. 多命名空间隔离
  let _: () = redis::cmd("NAMESPACE")
    .arg("ADD")
    .arg("ns_td")
    .arg("tok_td")
    .query(&mut con)?;

  let mut con_ns = client.get_connection()?;
  let _: () = redis::cmd("AUTH").arg("tok_td").query(&mut con_ns)?;

  let _: String = redis::cmd("TDIGEST.CREATE")
    .arg("td_tenant")
    .query(&mut con_ns)?;

  let _: String = redis::cmd("TDIGEST.ADD")
    .arg("td_tenant")
    .arg(42.0)
    .query(&mut con_ns)?;

  let in_ns: i64 = redis::cmd("EXISTS").arg("td_tenant").query(&mut con_ns)?;
  assert_eq!(in_ns, 1);

  let in_default: i64 = redis::cmd("EXISTS").arg("td_tenant").query(&mut con)?;
  assert_eq!(in_default, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis TDigest comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_json_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 13,
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
  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. JSON.SET 基本写入与 JSON.GET 读取
  let set_res: String = redis::cmd("JSON.SET")
    .arg("doc1")
    .arg("$")
    .arg(r#"{"name":"wedb","version":1,"active":true,"tags":["db","kv"],"meta":{"rating":9.5}}"#)
    .query(&mut con)?;
  assert_eq!(set_res, "OK");

  let get_root: String = redis::cmd("JSON.GET")
    .arg("doc1")
    .arg("$")
    .query(&mut con)?;
  assert!(get_root.contains("wedb"));

  // 2. TYPE 与 JSON.TYPE 验证
  let type_res: String = redis::cmd("TYPE").arg("doc1").query(&mut con)?;
  assert_eq!(type_res, "ReJSON-RL");

  let json_type: Vec<String> = redis::cmd("JSON.TYPE")
    .arg("doc1")
    .arg("$.name")
    .query(&mut con)?;
  assert_eq!(json_type, vec!["string"]);

  let json_type_num: Vec<String> = redis::cmd("JSON.TYPE")
    .arg("doc1")
    .arg("$.version")
    .query(&mut con)?;
  assert_eq!(json_type_num, vec!["integer"]);

  // 3. JSON.SET NX / XX 条件写入
  let set_nx_fail: redis::Value = redis::cmd("JSON.SET")
    .arg("doc1")
    .arg("$.name")
    .arg(r#""new_name""#)
    .arg("NX")
    .query(&mut con)?;
  assert_eq!(set_nx_fail, redis::Value::Nil);

  let set_xx_ok: String = redis::cmd("JSON.SET")
    .arg("doc1")
    .arg("$.name")
    .arg(r#""wedb_v2""#)
    .arg("XX")
    .query(&mut con)?;
  assert_eq!(set_xx_ok, "OK");

  // 4. JSON.NUMINCRBY & JSON.NUMMULTBY 数值运算
  let num_incr: String = redis::cmd("JSON.NUMINCRBY")
    .arg("doc1")
    .arg("$.version")
    .arg(2)
    .query(&mut con)?;
  assert_eq!(num_incr, "[3]");

  let num_mult: String = redis::cmd("JSON.NUMMULTBY")
    .arg("doc1")
    .arg("$.version")
    .arg(4)
    .query(&mut con)?;
  assert_eq!(num_mult, "[12]");

  // 5. JSON.STRAPPEND & JSON.STRLEN 字符串操作
  let strappend_len: Vec<i64> = redis::cmd("JSON.STRAPPEND")
    .arg("doc1")
    .arg("$.name")
    .arg(r#""_pro""#)
    .query(&mut con)?;
  assert_eq!(strappend_len, vec![11]);

  let strlen_res: Vec<i64> = redis::cmd("JSON.STRLEN")
    .arg("doc1")
    .arg("$.name")
    .query(&mut con)?;
  assert_eq!(strlen_res, vec![11]);

  // 6. JSON.ARR* 数组操作 (ARRAPPEND, ARRLEN, ARRINSERT, ARRINDEX, ARRPOP, ARRTRIM)
  let arr_append_len: Vec<i64> = redis::cmd("JSON.ARRAPPEND")
    .arg("doc1")
    .arg("$.tags")
    .arg(r#""raft""#)
    .arg(r#""lsm""#)
    .query(&mut con)?;
  assert_eq!(arr_append_len, vec![4]);

  let arr_len: Vec<i64> = redis::cmd("JSON.ARRLEN")
    .arg("doc1")
    .arg("$.tags")
    .query(&mut con)?;
  assert_eq!(arr_len, vec![4]);

  let arr_idx: Vec<i64> = redis::cmd("JSON.ARRINDEX")
    .arg("doc1")
    .arg("$.tags")
    .arg(r#""raft""#)
    .query(&mut con)?;
  assert_eq!(arr_idx, vec![2]);

  let arr_insert_len: Vec<i64> = redis::cmd("JSON.ARRINSERT")
    .arg("doc1")
    .arg("$.tags")
    .arg(1)
    .arg(r#""nosql""#)
    .query(&mut con)?;
  assert_eq!(arr_insert_len, vec![5]);

  let popped: Vec<String> = redis::cmd("JSON.ARRPOP")
    .arg("doc1")
    .arg("$.tags")
    .arg(-1)
    .query(&mut con)?;
  assert_eq!(popped, vec![r#""lsm""#]);

  let trim_res: Vec<i64> = redis::cmd("JSON.ARRTRIM")
    .arg("doc1")
    .arg("$.tags")
    .arg(0)
    .arg(1)
    .query(&mut con)?;
  assert_eq!(trim_res, vec![2]);

  // 7. JSON.OBJKEYS & JSON.OBJLEN 对象键操作
  let obj_keys: Vec<Vec<String>> = redis::cmd("JSON.OBJKEYS")
    .arg("doc1")
    .arg("$.meta")
    .query(&mut con)?;
  assert_eq!(obj_keys, vec![vec!["rating".to_string()]]);

  let obj_len: Vec<i64> = redis::cmd("JSON.OBJLEN")
    .arg("doc1")
    .arg("$.meta")
    .query(&mut con)?;
  assert_eq!(obj_len, vec![1]);

  // 8. JSON.TOGGLE 布尔反转
  let toggled: Vec<i64> = redis::cmd("JSON.TOGGLE")
    .arg("doc1")
    .arg("$.active")
    .query(&mut con)?;
  assert_eq!(toggled, vec![0]);

  // 9. JSON.MERGE (RFC 7396 Patch)
  let _: String = redis::cmd("JSON.MERGE")
    .arg("doc1")
    .arg("$.meta")
    .arg(r#"{"verified":true,"rating":null}"#)
    .query(&mut con)?;
  let meta_val: String = redis::cmd("JSON.GET")
    .arg("doc1")
    .arg("$.meta")
    .query(&mut con)?;
  assert!(meta_val.contains("verified"));
  assert!(!meta_val.contains("rating"));

  // 10. JSON.MSET & JSON.MGET 批量操作
  let _: String = redis::cmd("JSON.MSET")
    .arg("doc2")
    .arg("$")
    .arg(r#"{"title":"doc2"}"#)
    .arg("doc3")
    .arg("$")
    .arg(r#"{"title":"doc3"}"#)
    .query(&mut con)?;

  let mget_res: Vec<String> = redis::cmd("JSON.MGET")
    .arg("doc2")
    .arg("doc3")
    .arg("$.title")
    .query(&mut con)?;
  assert_eq!(mget_res, vec![r#"["doc2"]"#, r#"["doc3"]"#]);

  // 11. JSON.RESP 协议返回
  let resp_out: redis::Value = redis::cmd("JSON.RESP")
    .arg("doc2")
    .arg("$")
    .query(&mut con)?;
  assert!(matches!(
    resp_out,
    redis::Value::BulkString(_) | redis::Value::Array(_)
  ));

  // 12. JSON.DEL 删除节点与整键
  let del_prop: i64 = redis::cmd("JSON.DEL")
    .arg("doc1")
    .arg("$.active")
    .query(&mut con)?;
  assert_eq!(del_prop, 1);

  let del_doc: i64 = redis::cmd("JSON.DEL")
    .arg("doc1")
    .arg("$")
    .query(&mut con)?;
  assert_eq!(del_doc, 1);

  let exists_doc1: i64 = redis::cmd("EXISTS").arg("doc1").query(&mut con)?;
  assert_eq!(exists_doc1, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis JSON comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_ft_search_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 14,
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
  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. FT.CREATE 创建 JSON 倒排索引
  let create_res: String = redis::cmd("FT.CREATE")
    .arg("idx_books")
    .arg("ON")
    .arg("JSON")
    .arg("PREFIX")
    .arg(1)
    .arg("book:")
    .arg("SCHEMA")
    .arg("title")
    .arg("TEXT")
    .arg("WEIGHT")
    .arg(2.0)
    .arg("tags")
    .arg("TAG")
    .arg("SEPARATOR")
    .arg(",")
    .arg("price")
    .arg("NUMERIC")
    .arg("SORTABLE")
    .query(&mut con)?;
  assert_eq!(create_res, "OK");

  let _: String = redis::cmd("JSON.SET")
    .arg("book:1")
    .arg("$")
    .arg(r#"{"title":"Rust Programming Guide","tags":"rust,systems,fast","price":49.9}"#)
    .query(&mut con)?;

  let _: String = redis::cmd("JSON.SET")
    .arg("book:2")
    .arg("$")
    .arg(r#"{"title":"Distributed Database Engineering","tags":"db,raft,storage","price":89.0}"#)
    .query(&mut con)?;

  let _: String = redis::cmd("JSON.SET")
    .arg("book:3")
    .arg("$")
    .arg(r#"{"title":"Advanced Rust Database Storage","tags":"rust,db,storage","price":65.5}"#)
    .query(&mut con)?;

  // 3. FT.SEARCH 全文检索查询
  // (a) 单词项检索
  let res_rust: redis::Value = redis::cmd("FT.SEARCH")
    .arg("idx_books")
    .arg("rust")
    .query(&mut con)?;
  match res_rust {
    redis::Value::Array(arr) => {
      assert_eq!(arr[0], redis::Value::Int(2)); // book:1 and book:3
    }
    _ => panic!("Expected array response from FT.SEARCH"),
  }

  // (b) 多词项交集
  let res_inter: redis::Value = redis::cmd("FT.SEARCH")
    .arg("idx_books")
    .arg("rust database")
    .query(&mut con)?;
  match res_inter {
    redis::Value::Array(arr) => {
      assert_eq!(arr[0], redis::Value::Int(1)); // only book:3
    }
    _ => panic!("Expected array response"),
  }

  // (c) TAG 标签检索
  let res_tag: redis::Value = redis::cmd("FT.SEARCH")
    .arg("idx_books")
    .arg("@tags:{db}")
    .query(&mut con)?;
  match res_tag {
    redis::Value::Array(arr) => {
      assert_eq!(arr[0], redis::Value::Int(2)); // book:2 and book:3
    }
    _ => panic!("Expected array response"),
  }

  // (d) NUMERIC 数值范围检索
  let res_numeric: redis::Value = redis::cmd("FT.SEARCH")
    .arg("idx_books")
    .arg("@price:[50 90]")
    .query(&mut con)?;
  match res_numeric {
    redis::Value::Array(arr) => {
      assert_eq!(arr[0], redis::Value::Int(2)); // book:2 (89.0) and book:3 (65.5)
    }
    _ => panic!("Expected array response"),
  }

  // (e) SORTBY 排序与 LIMIT 分页
  let res_sort: redis::Value = redis::cmd("FT.SEARCH")
    .arg("idx_books")
    .arg("rust")
    .arg("SORTBY")
    .arg("price")
    .arg("ASC")
    .arg("LIMIT")
    .arg(0)
    .arg(1)
    .query(&mut con)?;
  match res_sort {
    redis::Value::Array(arr) => {
      assert_eq!(arr[0], redis::Value::Int(2)); // total 2
      assert_eq!(arr[1], redis::Value::BulkString(b"book:1".to_vec())); // lowest price 49.9
    }
    _ => panic!("Expected array response"),
  }

  // (f) NOCONTENT 选项
  let res_nocontent: redis::Value = redis::cmd("FT.SEARCH")
    .arg("idx_books")
    .arg("rust")
    .arg("NOCONTENT")
    .query(&mut con)?;
  match res_nocontent {
    redis::Value::Array(arr) => {
      assert_eq!(arr.len(), 3); // count + 2 doc ids (no contents)
      assert_eq!(arr[0], redis::Value::Int(2));
    }
    _ => panic!("Expected array response"),
  }

  // 4. FT.EXPLAIN 查询执行计划
  let explain_res: String = redis::cmd("FT.EXPLAIN")
    .arg("idx_books")
    .arg("rust @tags:{db} @price:[50 100]")
    .query(&mut con)?;
  assert!(explain_res.contains("INTERSECT"));

  // 5. FT.INFO 索引元数据详情
  let info_res: redis::Value = redis::cmd("FT.INFO").arg("idx_books").query(&mut con)?;
  match info_res {
    redis::Value::Array(arr) => {
      assert!(arr.len() >= 6);
    }
    _ => panic!("Expected array from FT.INFO"),
  }

  // 6. FT.TAGVALS 提取所有标签
  let tagvals: Vec<String> = redis::cmd("FT.TAGVALS")
    .arg("idx_books")
    .arg("tags")
    .query(&mut con)?;
  assert!(tagvals.contains(&"rust".to_string()));
  assert!(tagvals.contains(&"db".to_string()));
  assert!(tagvals.contains(&"storage".to_string()));

  // 7. FT.ALIASADD / FT.ALIASDEL 别名管理
  let alias_add: String = redis::cmd("FT.ALIASADD")
    .arg("books_alias")
    .arg("idx_books")
    .query(&mut con)?;
  assert_eq!(alias_add, "OK");

  let res_alias: redis::Value = redis::cmd("FT.SEARCH")
    .arg("books_alias")
    .arg("rust")
    .query(&mut con)?;
  match res_alias {
    redis::Value::Array(arr) => {
      assert_eq!(arr[0], redis::Value::Int(2));
    }
    _ => panic!("Expected array response via alias"),
  }

  let alias_del: String = redis::cmd("FT.ALIASDEL")
    .arg("books_alias")
    .query(&mut con)?;
  assert_eq!(alias_del, "OK");

  // 8. FT._LIST / FT.LIST 列出索引
  let list_res: Vec<String> = redis::cmd("FT._LIST").query(&mut con)?;
  assert!(list_res.contains(&"idx_books".to_string()));

  // 9. FT.DROPINDEX DD (级联删除索引与文档)
  let drop_res: String = redis::cmd("FT.DROPINDEX")
    .arg("idx_books")
    .arg("DD")
    .query(&mut con)?;
  assert_eq!(drop_res, "OK");

  let exists_b1: i64 = redis::cmd("EXISTS").arg("book:1").query(&mut con)?;
  assert_eq!(exists_b1, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis FT Search comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_hyperloglog_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 15,
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
  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. PFSELFTEST 内部自测
  let selftest_res: String = redis::cmd("PFSELFTEST").query(&mut con)?;
  assert_eq!(selftest_res, "OK");

  // 2. PFADD 添加元素
  let add1: i64 = redis::cmd("PFADD")
    .arg("hll1")
    .arg("apple")
    .arg("banana")
    .arg("cherry")
    .query(&mut con)?;
  assert_eq!(add1, 1);

  // 重复添加相同元素返回 0
  let add_dup: i64 = redis::cmd("PFADD")
    .arg("hll1")
    .arg("apple")
    .arg("banana")
    .query(&mut con)?;
  assert_eq!(add_dup, 0);

  // 3. PFCOUNT 单键基数估算
  let count1: i64 = redis::cmd("PFCOUNT").arg("hll1").query(&mut con)?;
  assert_eq!(count1, 3);

  // 4. TYPE 命令检查
  let hll_type: String = redis::cmd("TYPE").arg("hll1").query(&mut con)?;
  assert_eq!(hll_type, "hyperloglog");

  // 5. 多键合并估算与 PFMERGE
  let _: i64 = redis::cmd("PFADD")
    .arg("hll2")
    .arg("cherry")
    .arg("durian")
    .arg("elderberry")
    .query(&mut con)?;

  let multi_count: i64 = redis::cmd("PFCOUNT")
    .arg("hll1")
    .arg("hll2")
    .query(&mut con)?;
  assert_eq!(multi_count, 5); // apple, banana, cherry, durian, elderberry

  let merge_res: String = redis::cmd("PFMERGE")
    .arg("hll_merged")
    .arg("hll1")
    .arg("hll2")
    .query(&mut con)?;
  assert_eq!(merge_res, "OK");

  let merged_count: i64 = redis::cmd("PFCOUNT").arg("hll_merged").query(&mut con)?;
  assert_eq!(merged_count, 5);

  // 6. 大规模基数统计精度验证（5,000 个唯一元素，分批批量添加）
  for chunk_start in (0..5000).step_by(500) {
    let mut cmd = redis::cmd("PFADD");
    cmd.arg("hll_large");
    for i in chunk_start..(chunk_start + 500) {
      cmd.arg(format!("elem_{i}"));
    }
    let _: i64 = cmd.query(&mut con)?;
  }

  let large_count: i64 = redis::cmd("PFCOUNT").arg("hll_large").query(&mut con)?;
  let error_pct = (large_count as f64 - 5000.0).abs() / 5000.0;
  assert!(
    error_pct < 0.03,
    "HLL estimation error is too large: {error_pct:.4}"
  );

  // 7. 级联清理验证 (DEL)
  let del_cnt: i64 = redis::cmd("DEL").arg("hll1").query(&mut con)?;
  assert_eq!(del_cnt, 1);

  let count_after_del: i64 = redis::cmd("PFCOUNT").arg("hll1").query(&mut con)?;
  assert_eq!(count_after_del, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis HyperLogLog comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_redis_geo_comprehensive_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 16,
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
  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. GEOADD 添加主要城市地理坐标
  // Beijing: 116.405285, 39.904989
  // Shanghai: 121.472644, 31.231706
  // Guangzhou: 113.264434, 23.129162
  // Shenzhen: 114.057868, 22.543099
  // Tianjin: 117.190182, 39.125596
  let added: i64 = redis::cmd("GEOADD")
    .arg("china_cities")
    .arg(116.405285)
    .arg(39.904989)
    .arg("Beijing")
    .arg(121.472644)
    .arg(31.231706)
    .arg("Shanghai")
    .arg(113.264434)
    .arg(23.129162)
    .arg("Guangzhou")
    .arg(114.057868)
    .arg(22.543099)
    .arg("Shenzhen")
    .arg(117.190182)
    .arg(39.125596)
    .arg("Tianjin")
    .query(&mut con)?;
  assert_eq!(added, 5);

  // 2. TYPE 检查底层基于 ZSet
  let geo_type: String = redis::cmd("TYPE").arg("china_cities").query(&mut con)?;
  assert_eq!(geo_type, "zset");

  // 3. GEOPOS 获取坐标
  let pos_res: redis::Value = redis::cmd("GEOPOS")
    .arg("china_cities")
    .arg("Beijing")
    .arg("NonExisting")
    .query(&mut con)?;
  match pos_res {
    redis::Value::Array(arr) => {
      assert_eq!(arr.len(), 2);
      match &arr[0] {
        redis::Value::Array(coords) => {
          assert_eq!(coords.len(), 2);
        }
        _ => panic!("Expected array coords"),
      }
      assert_eq!(arr[1], redis::Value::Nil);
    }
    _ => panic!("Expected array from GEOPOS"),
  }

  // 4. GEOHASH 获取 11 位 Base32 编码
  let hash_res: Vec<Option<String>> = redis::cmd("GEOHASH")
    .arg("china_cities")
    .arg("Beijing")
    .arg("Shanghai")
    .query(&mut con)?;
  assert_eq!(hash_res.len(), 2);
  assert_eq!(hash_res[0].as_ref().map(|s| s.len()), Some(11));
  assert_eq!(hash_res[1].as_ref().map(|s| s.len()), Some(11));

  // 5. GEODIST 计算两点球面距离 (km / m / mi / ft)
  let dist_km: String = redis::cmd("GEODIST")
    .arg("china_cities")
    .arg("Beijing")
    .arg("Tianjin")
    .arg("km")
    .query(&mut con)?;
  let dist_km_f: f64 = dist_km.parse().unwrap();
  // 北京到天津距离大约在 100km ~ 130km 之间
  assert!(
    dist_km_f > 100.0 && dist_km_f < 130.0,
    "Beijing-Tianjin distance: {dist_km_f}"
  );

  let dist_m: String = redis::cmd("GEODIST")
    .arg("china_cities")
    .arg("Guangzhou")
    .arg("Shenzhen")
    .arg("m")
    .query(&mut con)?;
  let dist_m_f: f64 = dist_m.parse().unwrap();
  // 广州到深圳大约 90km ~ 120km 即 90,000m ~ 120,000m
  assert!(
    dist_m_f > 90000.0 && dist_m_f < 120000.0,
    "Guangzhou-Shenzhen distance: {dist_m_f}"
  );

  // 6. GEORADIUS 圆形范围检索（9 邻域与精准过滤）
  let radius_res: Vec<String> = redis::cmd("GEORADIUS")
    .arg("china_cities")
    .arg(116.405285)
    .arg(39.904989)
    .arg(200.0)
    .arg("km")
    .arg("ASC")
    .query(&mut con)?;
  assert!(radius_res.contains(&"Beijing".to_string()));
  assert!(radius_res.contains(&"Tianjin".to_string()));
  assert!(!radius_res.contains(&"Shanghai".to_string()));

  // 带 WITHCOORD 和 WITHDIST
  let radius_detail: redis::Value = redis::cmd("GEORADIUS")
    .arg("china_cities")
    .arg(116.405285)
    .arg(39.904989)
    .arg(200.0)
    .arg("km")
    .arg("WITHDIST")
    .arg("WITHCOORD")
    .arg("ASC")
    .query(&mut con)?;
  match radius_detail {
    redis::Value::Array(arr) => {
      assert!(arr.len() >= 2);
    }
    _ => panic!("Expected array from GEORADIUS with detail"),
  }

  // 7. GEORADIUSBYMEMBER 按成员圆形检索
  let by_member_res: Vec<String> = redis::cmd("GEORADIUSBYMEMBER")
    .arg("china_cities")
    .arg("Guangzhou")
    .arg(150.0)
    .arg("km")
    .query(&mut con)?;
  assert!(by_member_res.contains(&"Guangzhou".to_string()));
  assert!(by_member_res.contains(&"Shenzhen".to_string()));
  assert!(!by_member_res.contains(&"Beijing".to_string()));

  // 8. GEOSEARCH 支持 FROMLONLAT / FROMMEMBER 及 BYRADIUS / BYBOX
  let search_box_res: Vec<String> = redis::cmd("GEOSEARCH")
    .arg("china_cities")
    .arg("FROMLONLAT")
    .arg(116.405285)
    .arg(39.904989)
    .arg("BYBOX")
    .arg(300.0)
    .arg(300.0)
    .arg("km")
    .arg("ASC")
    .query(&mut con)?;
  assert!(search_box_res.contains(&"Beijing".to_string()));
  assert!(search_box_res.contains(&"Tianjin".to_string()));

  // 9. GEOSEARCHSTORE 存储检索结果
  let stored_count: i64 = redis::cmd("GEOSEARCHSTORE")
    .arg("bj_nearby")
    .arg("china_cities")
    .arg("FROMMEMBER")
    .arg("Beijing")
    .arg("BYRADIUS")
    .arg(200.0)
    .arg("km")
    .query(&mut con)?;
  assert!(stored_count >= 2);

  let zcard_res: i64 = redis::cmd("ZCARD").arg("bj_nearby").query(&mut con)?;
  assert_eq!(zcard_res, stored_count);

  // 10. 级联清理验证 (DEL)
  let del_cnt: i64 = redis::cmd("DEL").arg("china_cities").query(&mut con)?;
  assert_eq!(del_cnt, 1);

  let zcard_after_del: i64 = redis::cmd("ZCARD").arg("china_cities").query(&mut con)?;
  assert_eq!(zcard_after_del, 0);

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("Redis Geo comprehensive test suite passed!");
  OK
}

#[compio::test]
async fn test_newly_supported_redis_cmds_suite() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: 17,
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
  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. SDIFFCARD & SUNIONCARD
  let _: i64 = redis::cmd("SADD")
    .arg("set_a")
    .arg("1")
    .arg("2")
    .arg("3")
    .arg("4")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 1 SADD a failed: {e:?}"));
  let _: i64 = redis::cmd("SADD")
    .arg("set_b")
    .arg("3")
    .arg("4")
    .arg("5")
    .arg("6")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 1 SADD b failed: {e:?}"));
  let diff_card: i64 = redis::cmd("SDIFFCARD")
    .arg(2)
    .arg("set_a")
    .arg("set_b")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 1 SDIFFCARD failed: {e:?}"));
  assert_eq!(diff_card, 2);
  let diff_card_lim: i64 = redis::cmd("SDIFFCARD")
    .arg(2)
    .arg("set_a")
    .arg("set_b")
    .arg("LIMIT")
    .arg(1)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 1 SDIFFCARD LIMIT failed: {e:?}"));
  assert_eq!(diff_card_lim, 1);
  let union_card: i64 = redis::cmd("SUNIONCARD")
    .arg(2)
    .arg("set_a")
    .arg("set_b")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 1 SUNIONCARD failed: {e:?}"));
  assert_eq!(union_card, 6);
  let union_card_lim: i64 = redis::cmd("SUNIONCARD")
    .arg(2)
    .arg("set_a")
    .arg("set_b")
    .arg("LIMIT")
    .arg(3)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 1 SUNIONCARD LIMIT failed: {e:?}"));
  assert_eq!(union_card_lim, 3);

  // 2. INCREX
  let inc1: Vec<i64> = redis::cmd("INCREX")
    .arg("counter")
    .arg("BYINT")
    .arg(10)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 2 INCREX 1 failed: {e:?}"));
  assert_eq!(inc1, vec![10, 10]);
  let inc2: Vec<i64> = redis::cmd("INCREX")
    .arg("counter")
    .arg("BYINT")
    .arg(5)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 2 INCREX 2 failed: {e:?}"));
  assert_eq!(inc2, vec![15, 5]);
  let inc3: Vec<i64> = redis::cmd("INCREX")
    .arg("counter")
    .arg("BYINT")
    .arg(100)
    .arg("UBOUND")
    .arg(20)
    .arg("SATURATE")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 2 INCREX 3 failed: {e:?}"));
  assert_eq!(inc3, vec![20, 5]);
  let inc_flt: Vec<String> = redis::cmd("INCREX")
    .arg("flt_counter")
    .arg("BYFLOAT")
    .arg("2.5")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 2 INCREX BYFLOAT failed: {e:?}"));
  assert_eq!(inc_flt[0], "2.5");

  // 3. BITFIELD & BITFIELD_RO
  let bf_res: Vec<Option<i64>> = redis::cmd("BITFIELD")
    .arg("bf_test")
    .arg("SET")
    .arg("u8")
    .arg(0)
    .arg(255)
    .arg("GET")
    .arg("u8")
    .arg(0)
    .arg("OVERFLOW")
    .arg("SAT")
    .arg("INCRBY")
    .arg("u8")
    .arg(0)
    .arg(10)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 3 BITFIELD failed: {e:?}"));
  assert_eq!(bf_res, vec![Some(0), Some(255), Some(255)]);

  let bf_ro: Vec<Option<i64>> = redis::cmd("BITFIELD_RO")
    .arg("bf_test")
    .arg("GET")
    .arg("u8")
    .arg(0)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 3 BITFIELD_RO failed: {e:?}"));
  assert_eq!(bf_ro, vec![Some(255)]);

  // 4. HGETDEL
  let _: i64 = redis::cmd("HSET")
    .arg("h1")
    .arg("f1")
    .arg("v1")
    .arg("f2")
    .arg("v2")
    .arg("f3")
    .arg("v3")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 4 HSET failed: {e:?}"));
  let hgd1: Option<String> = redis::cmd("HGETDEL")
    .arg("h1")
    .arg("f1")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 4 HGETDEL 1 failed: {e:?}"));
  assert_eq!(hgd1, Some("v1".to_string()));
  let hexists1: i64 = redis::cmd("HEXISTS")
    .arg("h1")
    .arg("f1")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 4 HEXISTS failed: {e:?}"));
  assert_eq!(hexists1, 0);
  let hgd2: Vec<Option<String>> = redis::cmd("HGETDEL")
    .arg("h1")
    .arg("FIELDS")
    .arg(2)
    .arg("f2")
    .arg("f3")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 4 HGETDEL 2 failed: {e:?}"));
  assert_eq!(hgd2, vec![Some("v2".to_string()), Some("v3".to_string())]);
  let hlen1: i64 = redis::cmd("HLEN")
    .arg("h1")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 4 HLEN failed: {e:?}"));
  assert_eq!(hlen1, 0);

  // 5. SORT & SORT_RO
  let _: i64 = redis::cmd("RPUSH")
    .arg("mylist")
    .arg("10")
    .arg("5")
    .arg("30")
    .arg("20")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 5 RPUSH failed: {e:?}"));
  let sorted_asc: Vec<String> = redis::cmd("SORT")
    .arg("mylist")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 5 SORT failed: {e:?}"));
  assert_eq!(sorted_asc, vec!["5", "10", "20", "30"]);
  let sorted_desc_lim: Vec<String> = redis::cmd("SORT")
    .arg("mylist")
    .arg("DESC")
    .arg("LIMIT")
    .arg(0)
    .arg(2)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 5 SORT DESC failed: {e:?}"));
  assert_eq!(sorted_desc_lim, vec!["30", "20"]);
  let sort_stored: i64 = redis::cmd("SORT")
    .arg("mylist")
    .arg("STORE")
    .arg("stored_sorted")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 5 SORT STORE failed: {e:?}"));
  assert_eq!(sort_stored, 4);
  let stored_items: Vec<String> = redis::cmd("LRANGE")
    .arg("stored_sorted")
    .arg(0)
    .arg(-1)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 5 LRANGE stored failed: {e:?}"));
  assert_eq!(stored_items, vec!["5", "10", "20", "30"]);

  // 6. LMOVEM & BLMOVEM
  let _: i64 = redis::cmd("RPUSH")
    .arg("src_m")
    .arg("a")
    .arg("b")
    .arg("c")
    .arg("d")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 6 RPUSH failed: {e:?}"));
  let moved_m: Vec<String> = redis::cmd("LMOVEM")
    .arg("src_m")
    .arg("dst_m")
    .arg("LEFT")
    .arg("RIGHT")
    .arg("COUNT")
    .arg(2)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 6 LMOVEM failed: {e:?}"));
  assert_eq!(moved_m, vec!["a", "b"]);
  let src_rem: Vec<String> = redis::cmd("LRANGE")
    .arg("src_m")
    .arg(0)
    .arg(-1)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 6 LRANGE src failed: {e:?}"));
  assert_eq!(src_rem, vec!["c", "d"]);
  let dst_rem: Vec<String> = redis::cmd("LRANGE")
    .arg("dst_m")
    .arg(0)
    .arg(-1)
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 6 LRANGE dst failed: {e:?}"));
  assert_eq!(dst_rem, vec!["a", "b"]);

  // 7. SPUBLISH & PUBSUB
  let _: i64 = redis::cmd("SPUBLISH")
    .arg("chat_chan")
    .arg("hello")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 7 SPUBLISH failed: {e:?}"));
  let pubsub_help: Vec<String> = redis::cmd("PUBSUB")
    .arg("HELP")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 7 PUBSUB HELP failed: {e:?}"));
  assert!(!pubsub_help.is_empty());

  // 8. CLIENT & OBJECT
  let client_id: i64 = redis::cmd("CLIENT")
    .arg("ID")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 8 CLIENT ID failed: {e:?}"));
  assert_eq!(client_id, 1);
  let client_help: Vec<String> = redis::cmd("CLIENT")
    .arg("HELP")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 8 CLIENT HELP failed: {e:?}"));
  assert!(!client_help.is_empty());
  let obj_enc: String = redis::cmd("OBJECT")
    .arg("ENCODING")
    .arg("mylist")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 8 OBJECT ENCODING failed: {e:?}"));
  assert_eq!(obj_enc, "quicklist");
  let obj_help: Vec<String> = redis::cmd("OBJECT")
    .arg("HELP")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 8 OBJECT HELP failed: {e:?}"));
  assert!(!obj_help.is_empty());

  // 9. XACKDEL, XNACK, XDELEX
  let _: String = redis::cmd("XADD")
    .arg("stream_test")
    .arg("1000-0")
    .arg("field1")
    .arg("val1")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 9 XADD failed: {e:?}"));
  let _: String = redis::cmd("XGROUP")
    .arg("CREATE")
    .arg("stream_test")
    .arg("grp1")
    .arg("0")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 9 XGROUP CREATE failed: {e:?}"));
  let _: redis::Value = redis::cmd("XREADGROUP")
    .arg("GROUP")
    .arg("grp1")
    .arg("c1")
    .arg("COUNT")
    .arg(1)
    .arg("STREAMS")
    .arg("stream_test")
    .arg(">")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 9 XREADGROUP failed: {e:?}"));
  let xackdel_res: Vec<i64> = redis::cmd("XACKDEL")
    .arg("stream_test")
    .arg("grp1")
    .arg("1000-0")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 9 XACKDEL failed: {e:?}"));
  assert_eq!(xackdel_res, vec![1, 1]);
  let xlen_after: i64 = redis::cmd("XLEN")
    .arg("stream_test")
    .query(&mut con)
    .unwrap_or_else(|e| panic!("Step 9 XLEN failed: {e:?}"));
  assert_eq!(xlen_after, 0);

  redis_server
    .shutdown()
    .await
    .unwrap_or_else(|e| panic!("redis_server.shutdown failed: {e:?}"));
  node
    .shutdown()
    .await
    .unwrap_or_else(|e| panic!("node.shutdown failed: {e:?}"));

  info!("All newly added Redis cmds & args test suite passed!");
  OK
}

#[compio::test]
async fn test_cluster_metacache_and_virtual_sharding() -> Void {
  let dir = tempfile::tempdir()?;
  let raft_port = get_free_port();
  let redis_port = get_free_port();

  let conf = Conf {
    mode: ClusterMode::default(),
    topology: None,
    node_id: fastrand::u64(100..10000),
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

  sleep(Duration::from_millis(500)).await;

  let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}"))?;
  let mut con = client.get_connection()?;

  // 1. 测试命名空间注册与 MetaCache 自动填充
  let _: () = redis::cmd("NAMESPACE")
    .arg("ADD")
    .arg("tenant_fast")
    .arg("token_fast_123")
    .query(&mut con)?;

  // 验证 MetaCache 中存在该映射
  assert_eq!(
    node.meta_cache().get_namespace_by_token("token_fast_123"),
    Some(hipstr::HipStr::borrowed("tenant_fast"))
  );
  assert_eq!(
    node.meta_cache().get_token_by_namespace("tenant_fast"),
    Some(hipstr::HipStr::borrowed("token_fast_123"))
  );

  // 2. 测试基于 MetaCache 的快速 AUTH
  let auth_res: String = redis::cmd("AUTH").arg("token_fast_123").query(&mut con)?;
  assert_eq!(auth_res, "OK");

  let curr_ns: String = redis::cmd("NAMESPACE").arg("CURRENT").query(&mut con)?;
  assert_eq!(curr_ns, "tenant_fast");

  // 3. 测试虚拟分片拓扑管理
  {
    let mut topo = node.sharding().write().unwrap();
    topo.register_node(2, "127.0.0.1:15002");
    topo.register_node(3, "127.0.0.1:15003");
    topo.rebalance();

    let shard_id = webc_cmd::calculate_shard_id("tenant_fast", topo.shard_count);
    assert!(shard_id < topo.shard_count);
    let leader_id = topo.get_leader_for_namespace("tenant_fast").unwrap();
    assert!(leader_id >= 1);
  }

  // 4. 测试 NAMESPACE DEL 联动 MetaCache 失效
  let _: () = redis::cmd("AUTH").arg("admin").query(&mut con)?;
  let _: () = redis::cmd("NAMESPACE")
    .arg("DEL")
    .arg("tenant_fast")
    .query(&mut con)?;

  assert_eq!(
    node.meta_cache().get_namespace_by_token("token_fast_123"),
    None
  );
  assert_eq!(
    node.meta_cache().get_token_by_namespace("tenant_fast"),
    None
  );

  redis_server.shutdown().await?;
  node.shutdown().await?;

  info!("MetaCache and virtual sharding integration tests passed!");
  OK
}
