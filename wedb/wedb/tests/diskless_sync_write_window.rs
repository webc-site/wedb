//! 无盘全量同步快照扫描键门并发写窗回归测试
//!
//! 对标 C# 快照/重放互斥（Tsavorite 版本冻结流式检查点：检查点版本之后的
//! 写入不进快照、由 [checkpointCoveredAofAddress, tail] 的 AOF 重放单向覆
//! 盖，快照与重放互斥）：libs/cluster/Server/Replication/PrimaryOps/
//! libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:TakeStreamingCheckpointAsync
//! + ReplicationSnapshotIterator.cs:StoreSnapshotIterator。
//!
//! rust 快照源为活存储 live scan，扫描键门（diskless_replication/
//! scan_key_gate）承担同款互斥：锚前全阻排空 → 门内取锚 → 锚后按未读键集
//! 栅放、读值装帧即释门。本测试在真 RESP 写路径上并发 HINCRBY 同键
//!（ObjectStoreRMW 增量语义记录，重放非幂等、双重应用最敏感）贯穿整轮
//! 全量同步，断言同步完成后副本与主端终值严格一致：
//! - 挂起观测：HINCRBY 泵入后应答为空 = 写命令被键门挂起（Wait 裁决），
//!   键读值释门后经等待体重评放行——锚后写效果绝不进快照，恰经 AOF 续推
//!   应用一次；挂起前完成的增量属锚前写（快照覆盖侧），逐轮应答如实累计；
//! - 收敛断言：副本终值 = 主端终值 = 写窗运行体逐轮应答终值（无翻倍、
//!   无丢失），填充键与锚前预置同步在副本在场。

#[path = "common/replica_host.rs"]
mod replica_host;

use wedb_test::diskless_provider::diskless_provider;

#[path = "common/diskless_sync_kick.rs"]
mod diskless_sync_kick;
use diskless_sync_kick::try_full_sync;

#[path = "common/replay_rig.rs"]
mod replay_rig;
use std::{num::NonZeroUsize, str::from_utf8, sync::Arc, time::Duration};

use compio::runtime::spawn;
use replay_rig::ReplayRig;
use wbase::future::yield_now;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::worker::NodeRole;
use wedb_test::{
  cluster_consumer_store::cluster_consumer,
  node_storage::open_node,
  resp_pump_scratch::pump,
  resp_value::{get_value, store_reader},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer, aof::waof_sublog::single_log_aof,
  resp::resp_server_session::RespServerSessionOptions, service::NodeService,
  storage::session::storage_session::StorageSession,
};
use wtest_base::{resp_frame, wait_for};

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 写窗目标键（预置顺序即扫描顺序——hlog 按记录地址枚举：填充键 →
/// hash:early → hash:late；hash:late 最后读值，其写窗横跨大半轮同步）
const KEY_EARLY: &[u8] = b"hash:early";
const KEY_LATE: &[u8] = b"hash:late";
const FIELD: &[u8] = b"fld";
const SEED: i64 = 100;

/// 阶段 2 每键追加增量轮数（释门后即时执行的锚后增量）
const EXTRA_ROUNDS: usize = 4;
/// 键门闭窗探测上界（超过仍无挂起即键门未在同步窗口闭窗，编排/门控缺陷）
const PARK_PROBE_ROUNDS: usize = 5000;

/// 整数应答解析（HINCRBY 应答 = 增量后新值；`:N\r\n` 整数帧与
/// `$len\r\nN\r\n` 批量帧两形态兼容）
fn parse_int_reply(reply: &[u8]) -> i64 {
  let text = from_utf8(reply).unwrap();
  if let Some(rest) = text.strip_prefix(':') {
    return rest
      .trim_end()
      .parse::<i64>()
      .unwrap_or_else(|e| panic!("整数应答异常: {text:?} {e}"));
  }
  let mut parts = text.split("\r\n");
  let len: usize = parts
    .next()
    .and_then(|head| head.strip_prefix('$'))
    .and_then(|n| n.parse().ok())
    .unwrap_or_else(|| panic!("非整数应答: {text:?}"));
  let body = parts.next().unwrap_or("");
  body
    .get(..len)
    .unwrap_or_else(|| panic!("批量应答长度异常: {text:?}"))
    .parse::<i64>()
    .unwrap_or_else(|e| panic!("批量应答非数值: {text:?} {e}"))
}

/// 单轮 HINCRBY：泵入 → 挂起则驱动等待体（键读值释门后终评）→ 驻留字节
/// 原位重评放行执行。返回 (增量后新值, 是否发生挂起)
async fn hinby_round(consumer: &mut RespSessionConsumer, key: &[u8]) -> (i64, bool) {
  let frame = resp_frame(&[b"HINCRBY", key, FIELD, b"1"]);
  let out = pump(consumer, &frame);
  let parked = out.is_empty();
  let out = if parked {
    // 键门挂起（Wait 裁决）：等待体登记、游标回退零消费；键读值释门后
    // 等待体终评，泵下一轮直读同一缓冲重评放行（持久游标模型）
    let slow = consumer.take_slow_wait().expect("键门挂起应登记等待体");
    let _ = slow.resolve().await;
    let mut out = Vec::new();
    let _ = consumer.try_consume_messages_into(&mut out);
    out
  } else {
    out
  };
  (parse_int_reply(&out), parked)
}

/// 写窗单键运行体：阶段 1 泵 HINCRBY 直至观测到键门挂起（挂起前完成的
/// 增量属锚前写——快照覆盖侧，逐轮应答如实累计；键门永不闭窗即门控/编排
/// 缺陷，按上界失败）；阶段 2 键释门后追加 EXTRA_ROUNDS 轮（锚后写，记录
/// 地址恒 > 锚，恰经 AOF 续推应用一次）。返回 (终值, 挂起轮数)
async fn write_window_arm(mut consumer: RespSessionConsumer, key: &'static [u8]) -> (i64, usize) {
  let mut value = SEED;
  let mut parks = 0;
  for _ in 0..PARK_PROBE_ROUNDS {
    let (next, parked) = hinby_round(&mut consumer, key).await;
    value = next;
    if parked {
      parks += 1;
      break;
    }
    // 键门未闭窗：协作让步与同步交错推进（禁睡眠——扫描读阶段不切任务，
    // 写窗只在快照装批 send 的让步点被调度，睡醒即整窗溜过）
    yield_now().await;
  }
  assert!(parks > 0, "键门必须在本轮同步窗口内闭窗挂起并发写");
  for _ in 0..EXTRA_ROUNDS {
    let (next, parked) = hinby_round(&mut consumer, key).await;
    value = next;
    parks += usize::from(parked);
  }
  (value, parks)
}

/// 预置填充键（撑开扫描窗口跨多个装批：写窗挂起窗落在装批 send 的让步
/// 点——读阶段不切任务；存储直写经事件汇入账 AOF，锚前记录不重放、效果
/// 随快照覆盖）
async fn seed_fillers(store: &Arc<WedbStore<SegmentedDevice>>) {
  for i in 0..140u32 {
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    StorageSession::new_readonly(batch)
      .upsert_string(format!("filler:{i}").as_bytes(), format!("v{i}").as_bytes())
      .await
      .unwrap();
  }
}

/// RESP HSET 预置（真写路径：对象域写入 + 写侧条目入账）
fn hset_seed(consumer: &mut RespSessionConsumer, key: &[u8], value: i64) {
  let text = value.to_string();
  let frame = resp_frame(&[b"HSET", key, FIELD, text.as_bytes()]);
  let out = pump(consumer, &frame);
  assert_eq!(out, b":1\r\n", "HSET 预置应答异常");
}

/// RESP HGET 读值（副本收敛断言面）
fn hget_value(consumer: &mut RespSessionConsumer, key: &[u8]) -> i64 {
  let frame = resp_frame(&[b"HGET", key, FIELD]);
  let out = pump(consumer, &frame);
  parse_int_reply(&out)
}

/// 全量同步期间并发 HINCRBY 写窗：键门闭窗挂起贯穿锚点窗口，副本终值
/// 与主端严格一致（增量语义记录恰应用一次，无双重应用发散）
#[compio::test]
async fn diskless_sync_write_window_keeps_replica_converged() {
  // ===== 主端：AOF 门面 + 存储事件汇（RESP 写入账）+ 集群拓扑 + 预置
  let source = open_node("write_window_source");
  let provider_p = diskless_provider(
    &source,
    PRIMARY_ID,
    7000,
    NodeRole::Primary,
    PRIMARY_ID,
    true,
  );
  let primary_aof = single_log_aof(Arc::clone(&source.wal), &RuntimeServerOptions::default())
    .expect("装配主端 single_log_aof");
  let _service =
    NodeService::new(Arc::clone(&source.store), primary_aof).expect("注册存储 AOF 事件汇");

  seed_fillers(&source.store).await;
  // 键预置走写窗会话（真 RESP 路径：对象域写入 + 写侧条目入账 AOF）
  let mut seeder = cluster_consumer(&provider_p, &source.store);
  hset_seed(&mut seeder, KEY_EARLY, SEED);
  hset_seed(&mut seeder, KEY_LATE, SEED);

  // ===== 副本：回放装配 + 接收会话 + 宿主服务器（快照帧与 AOF 推流的真
  //    socket 接收面；装配束单源见 common/replay_rig.rs）
  let ReplayRig {
    node: replica,
    provider: _provider_r,
    rm: rm_r,
    server,
    addr: replica_addr,
  } = replay_rig::replay_rig(
    "write_window_replica",
    REPLICA_ID,
    PRIMARY_ID,
    7001,
    true,
    NonZeroUsize::new(1),
  );

  // 副本读回面：非集群会话直连执行域（副本同步后处恢复收敛态，RESP
  // 集群门评不介入）
  let mut replica_reader = store_reader(&replica.store, 2, RespServerSessionOptions::default());

  // ===== 全量同步发起（FullResync：两端历史不一致 + 副本零位点）
  // （推流资产与发起收口见 diskless_sync_kick 单源）

  // ===== 并发编排：两个写窗任务（真 RESP 路径并发 HINCRBY 各盯一键）
  //    与全量同步在同一执行器交错推进；HINCRBY 挂起 = 键门闭窗信号（锚点
  //    窗口写栅生效），释门重评放行的增量恰落快照读值之后
  let writer_early = spawn(write_window_arm(
    cluster_consumer(&provider_p, &source.store),
    KEY_EARLY,
  ));
  let writer_late = spawn(write_window_arm(
    cluster_consumer(&provider_p, &source.store),
    KEY_LATE,
  ));
  let (sync_from, assets) = try_full_sync(
    &provider_p,
    &source,
    &replica_addr,
    PRIMARY_ID,
    REPLICA_ID,
    &rm_r,
    Some("无盘全量同步应完成"),
  )
  .await;
  assert!(
    sync_from.get(0).is_some_and(|addr| addr > 0),
    "预置写入后授予位点（键门闭窗排空点日志尾）必须非零"
  );
  let Ok((early_value, early_parks)) = writer_early.await else {
    panic!("hash:early 写窗任务不应 panic");
  };
  let Ok((late_value, late_parks)) = writer_late.await else {
    panic!("hash:late 写窗任务不应 panic");
  };
  assert!(
    early_parks > 0 && late_parks > 0,
    "两目标键的写窗均须观测到键门挂起"
  );

  // ===== 续推收口：写窗尾帧补扫后副本位点追平主端尾
  // 先等提交栅栏落地再补扫再截尾：写窗 HINCRBY 走 auto_commit 分支，
  // `WaofSublog::commit` 仅向常驻 committer_loop 折叠信号（见
  // wnode/src/aof/waof_sublog.rs 与 garnet_log/mod.rs:106
  // `auto_commit: commit_frequency_ms == 0`），commit 元数据帧随批尾由
  // committer_loop 于稍后 `wal.enqueue` 写出，与 writer 任务返回之间无先后
  // 保证（同 C# TsavoriteLog.TryEnqueueCommitRecord 的异步侧写形态）。
  // 若跳过 commit 栅栏：`sync_backlog` 扫至当时的 safe_tail、`new_tail`
  // 亦按当时的 tail 截快照，committer_loop 稍后落笔的元数据帧会推主端尾
  // 越过 new_tail，副本按事件驱动追至新尾即与旧快照 `==` 判据永久错位
  // ——满载时 committer_loop 让核延后即命中，实测本机在满载 nextest 并发
  // 下 3 次复现 1 次（DIAG 显示 replica_off = primary_tail_at_fail ≠
  // new_tail，差值恰为一枚 32B 元数据帧）。此处以 `wal.commit().await`
  // 与 committer_loop 的 commit_to 走同一 GroupCommitPipeline 栅栏
  // （waof/src/wal/flush.rs::commit_to），await 返回即所有在途提交覆盖的
  // 元数据帧已入日志尾，同步续推后 new_tail 稳定；语义与同门 sibling 用例
  // expire_replica_replay.rs 的「commit → sync_backlog → 追平」序一致，
  // 不削弱「副本位点追平主端尾」断言本体。
  source.wal.commit().await.unwrap();
  let _ = assets.pump.sync_backlog(&source.wal).await;
  let new_tail = source.wal.tail_address() as i64;
  let caught_up = wait_for(
    || rm_r.get_current_replication_offset().get(0) == Some(new_tail),
    Duration::from_secs(10),
  )
  .await;
  assert!(caught_up, "副本复制位点必须追平主端尾");

  // ===== 收敛断言：副本终值 = 主端终值 = 写窗逐轮应答终值
  //（快照基值 + AOF 续推增量恰应用一次；双重应用即副本翻倍、丢增量即
  // 副本落后）
  let mut primary_reader = store_reader(&source.store, 3, RespServerSessionOptions::default());
  let primary_early = hget_value(&mut primary_reader, KEY_EARLY);
  let primary_late = hget_value(&mut primary_reader, KEY_LATE);
  let replica_early = hget_value(&mut replica_reader, KEY_EARLY);
  let replica_late = hget_value(&mut replica_reader, KEY_LATE);
  assert_eq!(primary_early, early_value, "主端终值须与写窗逐轮应答一致");
  assert_eq!(primary_late, late_value, "主端终值须与写窗逐轮应答一致");
  assert_eq!(
    replica_early, primary_early,
    "副本终值须与主端一致（增量恰应用一次）"
  );
  assert_eq!(
    replica_late, primary_late,
    "副本终值须与主端一致（增量恰应用一次）"
  );
  assert_eq!(
    get_value(&mut replica_reader, b"filler:0").as_deref(),
    Some(b"v0".as_slice()),
    "填充键须随快照在副本在场"
  );

  server.dispose();
}
