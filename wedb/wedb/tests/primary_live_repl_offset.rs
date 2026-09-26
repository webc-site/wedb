//! 主端运行期复制位点动态读集成测试（对照 ./garnet
//! libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationOffset /
//! GetReplicationOffset 主端角色动态读 appendOnlyFile.Log.TailAddress 语义）
//!
//! 暴露原缺陷：主端 replication_offset 字段仅在 boot 启动回填与副本重放链
//! 推进，主端 AOF 追加后字段永停旧值，导致 INFO replication / CLUSTER NODES
//! / gossip 携带的位点冻结在启动值，主端 FAILOVER 追平基准恒判失败。修法为
//! 位点取值单点化：只在 ReplicationManager 的角色分支 getter 内动态读日志尾，
//! 消费方一律不改（第三测锁的正是这条单点约束）。修前前两测红，修后绿。

#[path = "common/cluster_consumer_fresh.rs"]
mod cluster_cc_fresh;

use std::{str::from_utf8, sync::Arc, time::Duration};

use aok::Void;
use cluster_cc_fresh::cluster_consumer;
use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use waof::{AofAddress, AofEntryType};
use wconf::RuntimeServerOptions;
use wedb::server::{
  cluster_provider::ClusterProvider,
  worker::{LocalWorkerSpec, NodeRole},
};
use wnode::{
  GarnetAppendOnlyFile, GarnetLog, MessageConsumerFace, RecordShape, RespSessionConsumer,
};
use wnode_test::test_sublogs;

/// 装配单子日志真实 AOF 门面并注入 provider（同步触发主端位点动态读源注入）
fn wire_aof(provider: &Arc<ClusterProvider>, tag: &str) -> Arc<GarnetLog> {
  let options = RuntimeServerOptions::default();
  let (dirs, backends) = test_sublogs(tag, 1);
  for d in dirs {
    let _ = d.keep();
  }
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
  log.wait_for_commit(0, 0);

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
  log.wait_for_commit(0, 0);
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

  let mut consumer = cluster_consumer(&provider, "gate.db");
  // CLUSTER FAILSTOPWRITES <32hex 副本节点 id>
  let frame =
    b"*3\r\n$7\r\nCLUSTER\r\n$14\r\nFAILSTOPWRITES\r\n$32\r\n0000000000000000000000000000beef\r\n";
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应被完整消费");

  if let Some(slow) = consumer.take_slow_wait() {
    Runtime::new().unwrap().block_on(async {
      resp.extend_from_slice(&slow.resolve().await);
    });
  }

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

#[test]
fn fail_stop_writes_invalid_hex_returns_syntax_error() {
  let provider = primary_provider();
  let log = wire_aof(&provider, "primary_stopwrites_invalid_hex");
  append_records(&log, 2);

  let mut consumer = cluster_consumer(&provider, "gate.db");
  // CLUSTER FAILSTOPWRITES <非法 hex 字符>
  let frame = b"*3\r\n$7\r\nCLUSTER\r\n$14\r\nFAILSTOPWRITES\r\n$11\r\ninvalid_hex\r\n";
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应被完整消费");

  // 应答为 syntax error，且不产生慢路径等待
  assert_eq!(
    resp, b"-ERR syntax error\r\n",
    "非法 hex 节点 ID 必须返回 syntax error"
  );
  assert!(
    consumer.take_slow_wait().is_none(),
    "非法参数不应触发慢路径"
  );
  assert!(
    provider.is_primary(),
    "非法参数不得误触发 try_reset_replica 复位降级"
  );
}

#[test]
fn fail_stop_writes_empty_param_resets_replica() {
  let provider = primary_provider();
  let log = wire_aof(&provider, "primary_stopwrites_empty_param");
  append_records(&log, 2);
  let rm = provider.replication_manager().expect("rm 在场");

  let mut consumer = cluster_consumer(&provider, "gate.db");
  // 先设为从节点以验证复位回主节点
  let m = provider.cluster_manager().expect("cluster manager 在场");
  m.try_stop_writes(0x0000_0000_0000_0000_0000_0000_0000_beef);
  assert!(!provider.is_primary(), "停写后先置为副本");

  // CLUSTER FAILSTOPWRITES "" (空参复位)
  let frame = b"*3\r\n$7\r\nCLUSTER\r\n$14\r\nFAILSTOPWRITES\r\n$0\r\n\r\n";
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应被完整消费");

  if let Some(slow) = consumer.take_slow_wait() {
    Runtime::new().unwrap().block_on(async {
      resp.extend_from_slice(&slow.resolve().await);
    });
  }

  let ack =
    parse_bulk_string(&resp).unwrap_or_else(|| panic!("停写应答应为 bulk string，实得 {resp:?}"));
  assert_eq!(
    ack,
    rm.get_current_replication_offset().to_aof_string(),
    "空参复位应答应返回当前复制位点"
  );
  assert!(
    provider.is_primary(),
    "空参应成功触发 try_reset_replica 复位为主节点"
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

/// CLUSTER FAILSTOPWRITES 非空但非法 hex 参数不得触发从节点复位：C# 入口按
/// IsNullOrEmpty 分流，非空一律进停写路径，未知/非法 id 静默不执行（该命令
/// only for internode use，不报错）。旧实现把 hex 解析失败当空参走复位分支，
/// 打错一个字符即把副本升主、清主指针并递增配置纪元，本断言修前红、修后绿。
#[test]
fn fail_stop_writes_invalid_hex_arg_does_not_reset_replica() {
  const PRIMARY_ID: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0AB1;
  let provider = Arc::new(ClusterProvider::new());
  let manager = provider.cluster_manager().expect("cluster manager 在场");
  {
    let mut config = manager.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Replica,
      replica_of_node_id: Some(PRIMARY_ID),
      hostname: None,
    });
  }
  wire_aof(&provider, "fail_stop_writes_invalid_hex");
  let mut consumer = cluster_consumer(&provider, "gate.db");

  // 32 字符全非 hex 的非法节点 id（非空）
  let frame =
    b"*3\r\n$7\r\nCLUSTER\r\n$14\r\nFAILSTOPWRITES\r\n$32\r\nzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz\r\n";
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应被完整消费");
  assert_eq!(
    resp, b"-ERR syntax error\r\n",
    "非法 hex 节点 ID 必须返回 syntax error"
  );

  let config = manager.current_config.read();
  assert!(
    config.is_replica(),
    "非法 hex 参数不得把副本升主（旧实现误走复位分支，修前红）"
  );
  assert_eq!(
    config.local_node_primary_id(),
    Some(PRIMARY_ID),
    "非法 hex 参数不得清除主节点指针"
  );
  assert_eq!(
    config.local_node_config_epoch(),
    1,
    "非法 hex 参数不得递增配置纪元"
  );
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

/// 拼装 `CLUSTER FAILREPLICATIONOFFSET <payload>` 请求帧（payload 为二进制，
/// 不经字符串路径）
fn fail_repl_offset_frame(payload: &[u8]) -> Vec<u8> {
  let mut out = Vec::new();
  out.extend_from_slice(b"*3\r\n$7\r\nCLUSTER\r\n$21\r\nFAILREPLICATIONOFFSET\r\n");
  out.extend_from_slice(format!("${}\r\n", payload.len()).as_bytes());
  out.extend_from_slice(payload);
  out.extend_from_slice(b"\r\n");
  out
}

/// 消费一帧并记录即时应答（慢路径应答由调用方经 take_slow_wait 驱动）
fn consume_frame(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0), "帧应被完整消费");
  resp
}

/// FAILREPLICATIONOFFSET 带前缀二进制载荷收端直拍（对标 C#
/// RespClusterFailoverCommands.cs:151 FromByteArray + :154
/// WaitForReplicationOffsetAsync）：请求形以发端同一
/// AofAddress::to_aof_binary 构造锁双端同形；位点已追平 → 慢路径即刻应答
/// 当下位点逗号串（应答侧 ToString 文本形不动）
#[test]
fn fail_repl_offset_binary_caughtup_acks_current() -> Void {
  let provider = primary_provider();
  provider
    .cluster_manager()
    .expect("cluster manager 在场")
    .try_set_local_node_role(NodeRole::Replica);
  let rm = provider.replication_manager().expect("rm 在场");
  rm.set_current_replication_offset(AofAddress::create(1, 1000));

  let mut consumer = cluster_consumer(&provider, "gate.db");
  let resp = consume_frame(
    &mut consumer,
    &fail_repl_offset_frame(&AofAddress::create(1, 1000).to_aof_binary()),
  );
  assert!(resp.is_empty(), "追平臂走慢路径，消费段不产即时应答");
  let ack = match consumer.take_slow_wait() {
    Some(slow) => Runtime::new()
      .unwrap()
      .block_on(async { slow.resolve().await }),
    None => panic!("合法载荷应登记慢路径位点等待"),
  };
  let ack = parse_bulk_string(&ack).expect("追平应答应为 bulk string 位点串");
  assert_eq!(ack, rm.get_current_replication_offset().to_aof_string());
  assert_eq!(ack, "1000", "追平即刻应答当下位点");
  aok::OK
}

/// 落后副本迟答：目标位点领先本地位点 → 等待挂起，位点推进唤醒后应答
/// 推进位点本体（对标 C# WaitForReplicationOffsetAsync 轮询环追平语义，
/// 事件驱动等价形态）
#[test]
fn fail_repl_offset_binary_behind_acks_after_offset_advance() -> Void {
  Runtime::new()?.block_on(async {
    let provider = primary_provider();
    provider
      .cluster_manager()
      .expect("cluster manager 在场")
      .try_set_local_node_role(NodeRole::Replica);
    let rm = provider.replication_manager().expect("rm 在场");
    rm.set_current_replication_offset(AofAddress::create(1, 1000));

    let mut consumer = cluster_consumer(&provider, "gate.db");
    let resp = consume_frame(
      &mut consumer,
      &fail_repl_offset_frame(&AofAddress::create(1, 1050).to_aof_binary()),
    );
    assert!(resp.is_empty());
    let slow = consumer.take_slow_wait().expect("落后应登记位点等待");

    // 80ms 后推进本地位点至目标：wake_offset_waiters 精准唤醒挂起等待
    let advancer = provider.clone();
    spawn(async move {
      sleep(Duration::from_millis(80)).await;
      advancer
        .replication_manager()
        .expect("rm 在场")
        .set_current_replication_offset(AofAddress::create(1, 1050));
    })
    .detach();
    let ack = slow.resolve().await;

    let ack = parse_bulk_string(&ack).expect("追平应答应为 bulk string 位点串");
    assert_eq!(ack, "1050", "位点推进唤醒后应答推进位点本体");
    aok::OK
  })
}

/// 畸形载荷拒收锁：前缀与实长不符 / 前缀越界 / 旧 ASCII 十进制串形一律
/// syntax error 且不登记慢路径——乙档病理（十进制数字节被裸 8B LE 解为
/// ≈4×10^18 巨值、等待环永不满足）的回归防线
#[test]
fn fail_repl_offset_malformed_payload_rejected() -> Void {
  let provider = primary_provider();
  let mut consumer = cluster_consumer(&provider, "gate.db");
  let cases: &[&[u8]] = &[
    &[],
    b"1000", // 旧 ASCII 逗号串发端形：4 字节体不符 1+8N 线形，必须拒
    b"100000000",
    &[4, 0, 0],    // 前缀 4、实长不足
    &[9, 0, 0, 0], // 前缀越 MAX_SUBLOG_COUNT
  ];
  for payload in cases {
    let resp = consume_frame(&mut consumer, &fail_repl_offset_frame(payload));
    assert_eq!(
      resp, b"-ERR syntax error\r\n",
      "畸形载荷 {payload:?} 必须回 syntax error"
    );
    assert!(
      consumer.take_slow_wait().is_none(),
      "畸形载荷不得登记慢路径位点等待"
    );
  }
  aok::OK
}
