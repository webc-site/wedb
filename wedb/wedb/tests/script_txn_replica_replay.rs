//! 脚本写复制数据面 + 事务复制收敛（命令级全量+增量双路径回归）
//!
//! 对标 C# 集群复制测试面（test/cluster/Garnet.test.cluster.replication/
//! ReplicationTests/ClusterReplicationBaseTests.cs）：
//! - ClusterReplicationLua（:1173）：主端 EVAL redis.call('SET',...) 写两键
//!   → 副本同步 → 副本端 EVAL return redis.call('GET',...) 断言值收敛。
//! - ClusterReplicationSimpleTransactionTest（:1684）：MULTI/EXEC 事务经
//!   复制流副本重放收敛（C# 以 INCRBY 多轮迭代锁重放不发散）。
//!
//! 本档两用例均在主端真 RESP 路径（集群会话消费面）泵命令，经无盘全量
//! 快照同步后断言快照收敛，再于同步收口后泵第二批（AOF 增量条目回放
//! 路径）断言增量收敛——全量与增量两条收敛路径对脚本写/事务写分别钉值。

use std::{num::NonZeroUsize, str::from_utf8, sync::Arc, time::Duration};

use wedb::server::cluster_provider::ClusterProvider;
use wedb_test::{diskless_provider::diskless_provider, node_storage::open_node};

#[path = "common/diskless_sync_kick.rs"]
mod diskless_sync_kick;
use diskless_sync_kick::try_full_sync;

#[path = "common/replay_rig.rs"]
mod replay_rig;
use replay_rig::ReplayRig;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{cluster::IClusterProvider, worker::NodeRole};
use wedb_test::{
  cluster_consumer_with::cluster_consumer_with,
  resp_pump_scratch::pump,
  resp_value::{get_value, parse_bulk, store_reader},
};
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer, aof::waof_sublog::single_log_aof,
  resp::resp_server_session::RespServerSessionOptions, service::NodeService,
};
use wtest_base::{resp_frame, wait_for};

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00F1;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00F2;

/// 主端真 RESP 集群会话消费者（Lua 启用；装配尾单源见
/// `wedb_test::cluster_consumer_with`，其余同 cluster_consumer_store.rs
/// 形态——写路径同生产）
fn lua_cluster_consumer(
  provider: &ClusterProvider,
  store: &Arc<WedbStore<SegmentedDevice>>,
) -> RespSessionConsumer {
  cluster_consumer_with(
    provider.create_cluster_session(),
    provider.provider_handle(),
    store,
    RespServerSessionOptions {
      enable_lua: true,
      ..RespServerSessionOptions::default()
    },
  )
}

/// 副本端 Lua 启用会话 options（读侧直连执行域；对位 garnet 副本端脚本读）
fn lua_reader_options() -> RespServerSessionOptions {
  RespServerSessionOptions {
    enable_lua: true,
    ..RespServerSessionOptions::default()
  }
}
/// 泵入命令并锁定精确应答（应答偏差即行为变化，第一时间现场失败）
fn exec(consumer: &mut RespSessionConsumer, parts: &[&[u8]], expected: &[u8]) {
  let out = pump(consumer, &resp_frame(parts));
  assert_eq!(
    out,
    expected,
    "命令 {:?} 应答异常: {:?}",
    parts
      .iter()
      .map(|p| from_utf8(p).unwrap())
      .collect::<Vec<_>>(),
    from_utf8(&out).unwrap_or("<binary>")
  );
}

/// EVAL 脚本读值：`return redis.call('GET', KEYS[1])`（对位 garnet
/// ClusterReplicationLua 副本端断言形态；脚本返回 bulk string，nil 返 None；
/// bulk 解析单源见 `wedb_test::resp_value`）
fn eval_get(consumer: &mut RespSessionConsumer, key: &[u8]) -> Option<Vec<u8>> {
  let script = b"return redis.call('GET', KEYS[1])";
  parse_bulk(
    pump(consumer, &resp_frame(&[b"EVAL", script, b"1", key])),
    "EVAL GET",
  )
}

/// 脚本写复制数据面（garnet ClusterReplicationLua :1173 对位）：
/// 主端 EVAL redis.call('SET', KEYS[1], ARGV[1]) 写 foo=bar / fizz=buzz →
/// 全量快照同步 → 副本端 EVAL return redis.call('GET') 逐键断言收敛 →
/// 同步收口后主端再 EVAL 写第三键（AOF 增量条目回放路径）→ 副本位点
/// 追平后 EVAL 读断言增量收敛
#[compio::test]
async fn lua_script_writes_replicate_to_replica() -> aok::Void {
  // ===== 主端：AOF 门面 + 存储事件汇 + 集群拓扑 + Lua 会话
  let source = open_node("lua_repl_source");
  let provider_p = diskless_provider(
    &source,
    PRIMARY_ID,
    7000,
    NodeRole::Primary,
    PRIMARY_ID,
    true,
  );
  let aof_options = RuntimeServerOptions::default();
  let primary_aof = single_log_aof(Arc::clone(&source.wal), &aof_options)?;
  let _service = NodeService::new(Arc::clone(&source.store), primary_aof)?;
  let mut primary = lua_cluster_consumer(&provider_p, &source.store);

  // ===== 第一批：主端 EVAL 写两键（garnet 同款脚本形态，无 return → nil）
  let set_script = b"redis.call('SET', KEYS[1], ARGV[1])";
  exec(
    &mut primary,
    &[b"EVAL", set_script, b"1", b"foo", b"bar"],
    b"$-1\r\n",
  );
  exec(
    &mut primary,
    &[b"EVAL", set_script, b"1", b"fizz", b"buzz"],
    b"$-1\r\n",
  );

  // ===== 副本整套装配 + 全量同步（foo/fizz 随快照导入）
  let ReplayRig {
    node: replica,
    provider: _provider_r,
    rm: rm_r,
    server,
    addr: replica_addr,
  } = replay_rig::replay_rig(
    "lua_repl_replica",
    REPLICA_ID,
    PRIMARY_ID,
    7001,
    true,
    NonZeroUsize::new(1),
  );
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
    "脚本写入入账后授予位点必须非零"
  );

  // ===== 快照收敛：副本端 EVAL 读（garnet 断言面：值经脚本读回收敛）
  let mut r_reader = store_reader(&replica.store, 2, lua_reader_options());
  assert_eq!(
    eval_get(&mut r_reader, b"foo").as_deref(),
    Some(b"bar".as_ref()),
    "副本端 EVAL GET foo 必须收敛为 bar（快照路径）"
  );
  assert_eq!(
    eval_get(&mut r_reader, b"fizz").as_deref(),
    Some(b"buzz".as_ref()),
    "副本端 EVAL GET fizz 必须收敛为 buzz（快照路径）"
  );

  // ===== 第二批：同步收口后 EVAL 写第三键（AOF 增量回放路径）
  exec(
    &mut primary,
    &[b"EVAL", set_script, b"1", b"third", b"qux"],
    b"$-1\r\n",
  );
  source.wal.commit().await?;
  let _ = assets.pump.sync_backlog(&source.wal).await;
  let new_tail = source.wal.tail_address() as i64;
  assert!(
    wait_for(
      || rm_r.get_current_replication_offset().get(0) == Some(new_tail),
      Duration::from_secs(10),
    )
    .await,
    "副本复制位点必须追平主端日志尾（脚本写增量路径）"
  );
  assert_eq!(
    eval_get(&mut r_reader, b"third").as_deref(),
    Some(b"qux".as_ref()),
    "副本端 EVAL GET third 必须收敛为 qux（AOF 增量回放路径）"
  );
  // 增量不得破坏快照侧已收敛值
  assert_eq!(
    eval_get(&mut r_reader, b"foo").as_deref(),
    Some(b"bar".as_ref()),
    "增量回放后快照侧值不得漂移"
  );

  server.dispose();
  Ok(())
}

/// 事务复制收敛（garnet ClusterReplicationSimpleTransactionTest :1684 对位）：
/// 主端 MULTI/INCRBY ×3/EXEC 循环多轮（garnet 以 10 轮迭代锁重放不发散，
/// 本档 3 轮即锁同判据：事务净效应逐键精确累加，EXEC 应答整帧锁定）→
/// 全量快照同步 → 副本 GET 逐键断言终值 → 同步收口后再 MULTI/SET/SET/
/// EXEC（AOF 增量路径）→ 副本 GET 两键断言收敛
#[compio::test]
async fn multi_exec_txn_replicates_and_converges() -> aok::Void {
  // ===== 主端装配（同上：AOF + 事件汇 + 集群拓扑 + 事务会话）
  let source = open_node("txn_repl_source");
  let provider_p = diskless_provider(
    &source,
    PRIMARY_ID,
    7000,
    NodeRole::Primary,
    PRIMARY_ID,
    true,
  );
  let aof_options = RuntimeServerOptions::default();
  let primary_aof = single_log_aof(Arc::clone(&source.wal), &aof_options)?;
  let _service = NodeService::new(Arc::clone(&source.store), primary_aof)?;
  let mut primary = lua_cluster_consumer(&provider_p, &source.store);

  // ===== 第一批：MULTI / INCRBY ×3（+QUEUED）/ EXEC 循环 3 轮。
  // EXEC 应答整帧锁定：*3 + 三个本轮自增终值（第 round 轮 a/b/c 各为
  // round×10 / round×15 / round×20，累计即 30/45/60）
  for round in 1u64..=3 {
    exec(&mut primary, &[b"MULTI"], b"+OK\r\n");
    exec(&mut primary, &[b"INCRBY", b"txa", b"10"], b"+QUEUED\r\n");
    exec(&mut primary, &[b"INCRBY", b"txb", b"15"], b"+QUEUED\r\n");
    exec(&mut primary, &[b"INCRBY", b"txc", b"20"], b"+QUEUED\r\n");
    let (a, b, c) = (round * 10, round * 15, round * 20);
    let expected = format!("*3\r\n:{a}\r\n:{b}\r\n:{c}\r\n").into_bytes();
    exec(&mut primary, &[b"EXEC"], &expected);
  }

  // ===== 副本整套装配 + 全量同步（txa/txb/txc 事务净效应随快照导入）
  let ReplayRig {
    node: replica,
    provider: _provider_r,
    rm: rm_r,
    server,
    addr: replica_addr,
  } = replay_rig::replay_rig(
    "txn_repl_replica",
    REPLICA_ID,
    PRIMARY_ID,
    7001,
    true,
    NonZeroUsize::new(1),
  );
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
    "事务写入入账后授予位点必须非零"
  );

  // ===== 快照收敛：副本 GET 逐键断言事务净效应（3 轮累计 30/45/60）
  let mut r_reader = store_reader(&replica.store, 2, lua_reader_options());
  assert_eq!(
    get_value(&mut r_reader, b"txa").as_deref(),
    Some(b"30".as_ref()),
    "副本 txa 必须收敛为 3 轮事务累计 30（快照路径）"
  );
  assert_eq!(
    get_value(&mut r_reader, b"txb").as_deref(),
    Some(b"45".as_ref()),
    "副本 txb 必须收敛为 3 轮事务累计 45（快照路径）"
  );
  assert_eq!(
    get_value(&mut r_reader, b"txc").as_deref(),
    Some(b"60".as_ref()),
    "副本 txc 必须收敛为 3 轮事务累计 60（快照路径）"
  );

  // ===== 第二批：同步收口后 MULTI/SET/SET/EXEC（AOF 增量回放路径）
  exec(&mut primary, &[b"MULTI"], b"+OK\r\n");
  exec(&mut primary, &[b"SET", b"txd", b"delta1"], b"+QUEUED\r\n");
  exec(&mut primary, &[b"SET", b"txe", b"delta2"], b"+QUEUED\r\n");
  exec(&mut primary, &[b"EXEC"], b"*2\r\n+OK\r\n+OK\r\n");
  source.wal.commit().await?;
  let _ = assets.pump.sync_backlog(&source.wal).await;
  let new_tail = source.wal.tail_address() as i64;
  assert!(
    wait_for(
      || rm_r.get_current_replication_offset().get(0) == Some(new_tail),
      Duration::from_secs(10),
    )
    .await,
    "副本复制位点必须追平主端日志尾（事务写增量路径）"
  );
  assert_eq!(
    get_value(&mut r_reader, b"txd").as_deref(),
    Some(b"delta1".as_ref()),
    "副本 txd 必须经 AOF 增量回放收敛（事务增量路径）"
  );
  assert_eq!(
    get_value(&mut r_reader, b"txe").as_deref(),
    Some(b"delta2".as_ref()),
    "副本 txe 必须经 AOF 增量回放收敛（事务增量路径）"
  );
  // 增量不得破坏快照侧已收敛值
  assert_eq!(
    get_value(&mut r_reader, b"txa").as_deref(),
    Some(b"30".as_ref()),
    "增量回放后快照侧事务净效应不得漂移"
  );

  server.dispose();
  Ok(())
}
