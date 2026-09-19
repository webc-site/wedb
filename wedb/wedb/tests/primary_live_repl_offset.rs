//! 主端运行期复制位点动态读集成测试（对照 ./garnet
//! libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationOffset /
//! GetReplicationOffset 主端角色动态读 appendOnlyFile.Log.TailAddress 语义）
//!
//! 暴露原缺陷：主端 replication_offset 字段仅在 boot 启动回填与副本重放链
//! 推进，主端 AOF 追加后字段永停旧值，导致 INFO replication / CLUSTER NODES
//! / gossip 携带的位点冻结在启动值，主端 FAILOVER 追平基准恒判失败。修法为
//! 位点取值单点化：只在 ReplicationManager 的角色分支 getter 内动态读日志尾，
//! 消费方一律不改（第三测锁的正是这条单点约束）。修前前两测红，修后绿。

use std::{str::from_utf8, sync::Arc};

use waof::{AofAddress, AofEntryType};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster::IClusterProvider,
  cluster_provider::ClusterProvider,
  worker::{LocalWorkerSpec, NodeRole},
};
use wkv::WedbStore;
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, MessageConsumerFace, RecordShape, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wnode_test::test_sublogs;
use wtest_base::test_store_config;
use wtxn::{TxnLockTable, WatchVersionMap};

/// 装配单子日志真实 AOF 门面并注入 provider（同步触发主端位点动态读源注入）
fn wire_aof(provider: &Arc<ClusterProvider>, tag: &str) -> Arc<GarnetLog> {
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = test_sublogs(tag, 1);
  let log = Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog"));
  provider.set_aof(Some(Arc::new(GarnetAppendOnlyFile::new(
    Arc::clone(&log),
    &options,
    None,
  ))));
  log
}

/// 向 AOF 追加若干记录，推进日志尾
fn append_records(log: &Arc<GarnetLog>, n: usize) {
  for i in 0..n {
    let key = format!("k{i}");
    let record = RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: key.as_bytes(),
      value: b"v",
      input: &[],
      database_id: 0,
    };
    let _ = log.enqueue(&record);
  }
}

/// 主端角色 AOF 追加后 get_replication_offset / get_current_replication_offset
/// 随日志尾前进（对标 C# PRIMARY 分支动态读 Log.TailAddress；修前读冻结字段
/// 恒停 0，本断言红）
#[test]
fn primary_aof_append_advances_replication_offset() {
  let provider = ClusterProvider::new();
  provider
    .cluster_manager()
    .expect("cluster manager 在场")
    .try_set_local_node_role(NodeRole::Primary);

  let log = wire_aof(&provider, "primary_live_repl_offset_append");
  let rm = provider.replication_manager().expect("rm 在场");

  // 追加前：位点 = 日志尾（新日志起点）
  let before = rm.get_current_replication_offset();
  assert_eq!(
    before.get(0),
    Some(log.get_tail_address(0)),
    "追加前位点应等于日志尾"
  );

  append_records(&log, 3);

  // 追加后：单槽位点与完整地址均须随真实日志尾前进
  let tail = log.get_tail_address(0);
  assert!(tail > 0, "AOF 追加后日志尾应前进: {tail}");
  assert_eq!(
    rm.get_replication_offset(0),
    tail,
    "主端逐槽位点应动态读日志尾（修前冻结在旧值）"
  );
  assert_eq!(
    rm.get_current_replication_offset().get(0),
    Some(tail),
    "主端完整位点应动态读日志尾（修前冻结在旧值）"
  );

  // 追加更多记录，位点继续前进（证明非一次性对齐，而是每次动态读）
  append_records(&log, 2);
  let tail2 = log.get_tail_address(0);
  assert!(
    rm.get_current_replication_offset().get(0) > before.get(0),
    "再次追加后位点应继续前进: before={before:?} tail2={tail2}"
  );
}

/// 副本角色：位点仍走重放链回推字段，不受日志尾动态读影响
#[test]
fn replica_offset_still_reads_replayed_field() {
  let provider = ClusterProvider::new();
  provider
    .cluster_manager()
    .expect("cluster manager 在场")
    .try_set_local_node_role(NodeRole::Replica);

  let log = wire_aof(&provider, "primary_live_repl_offset_replica");
  append_records(&log, 3);
  let tail = log.get_tail_address(0);

  let rm = provider.replication_manager().expect("rm 在场");
  // 副本运行期位点由重放链权威回推（此处模拟应用一条到 7）
  rm.set_current_replication_offset(AofAddress::create(1, 7));
  assert_eq!(
    rm.get_current_replication_offset().get(0),
    Some(7),
    "副本位点应读重放回推字段，而非主端日志尾 {tail}"
  );
}

/// CLUSTER FAILOVER 停写应答锁单点读法：应答必须等于
/// rm.get_current_replication_offset()（对标 C#
/// RespClusterFailoverCommands.cs:129 读 ReplicationOffset 属性本身）。
/// TryStopWrites 已把本端降级为副本（C# ClusterConfig.cs:1291 MakeReplicaOf
/// 置 Role = REPLICA），getter 的角色分支据此回退字段——本端与 C# 同值。
/// 断言应答不等于日志尾，是防「在消费方另起第二条主端位点入口」的回归锁。
#[test]
fn fail_stop_writes_ack_follows_single_offset_getter() {
  let provider = primary_provider();
  let log = wire_aof(&provider, "primary_live_repl_offset_stopwrites");
  append_records(&log, 4);
  let tail = log.tail_address();
  let rm = provider.replication_manager().expect("rm 在场");

  let mut consumer = cluster_consumer(&provider);
  // CLUSTER FAILSTOPWRITES <32hex 副本节点 id>
  let frame =
    b"*3\r\n$7\r\nCLUSTER\r\n$14\r\nFAILSTOPWRITES\r\n$32\r\n0000000000000000000000000000beef\r\n";
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应被完整消费");

  // 应答为单条 bulk string，载荷即停写位点（对标 C# AofAddress.ToString）
  let ack =
    parse_bulk_string(&resp).unwrap_or_else(|| panic!("停写应答应为 bulk string，实得 {resp:?}"));
  // 停写后本端已降级为副本（前置条件，否则下面两条断言无意义）
  assert!(!provider.is_primary(), "FAILSTOPWRITES 应把本端降级为副本");
  assert_eq!(
    ack,
    rm.get_current_replication_offset().to_aof_string(),
    "停写应答须等于收口 getter 的返回值，不得另起读法"
  );
  assert_ne!(
    ack,
    tail.to_aof_string(),
    "副本角色下应答走 replication_offset 字段（C# 同语义），不得绕过角色分支直取日志尾"
  );
}

/// 解析单条 RESP bulk string 载荷（$<len>\r\n<payload>\r\n）
fn parse_bulk_string(resp: &[u8]) -> Option<String> {
  if resp.first() != Some(&b'$') {
    return None;
  }
  let head_end = resp.windows(2).position(|w| w == b"\r\n")?;
  let len: usize = from_utf8(&resp[1..head_end]).ok()?.parse().ok()?;
  let payload_start = head_end + 2;
  let payload = resp.get(payload_start..payload_start + len)?;
  String::from_utf8(payload.to_vec()).ok()
}

/// 主端单节点拓扑（本节点角色 PRIMARY，供 network_cluster_fail_stop_writes
/// 走主端动态读位点面）
fn primary_provider() -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  let m = cp.cluster_manager().expect("cluster manager 在场");
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
  }
  cp
}

/// 构造挂接集群切面 + 存储执行域的会话消费者（对标 cluster_resp_session 装配）
fn cluster_consumer(cp: &ClusterProvider) -> RespSessionConsumer {
  let cluster_session = cp.create_cluster_session();
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("gate.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  cp.set_store(Arc::clone(&store));
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    cp.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
}
