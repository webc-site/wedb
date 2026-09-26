//! 无盘全量同步快照窗换号族域钉回归测试（工单 wnode-snapshot-swap-window-ungated）
//!
//! 对标 C#：N.A.（换号教义缺席——C# FlushDB=ResetDatabase 清表原位重建无代际、
//! 快照=整条 LogRecord 字节直拷无读值期二次解析、TrySwapDatabases 入站
//! ActiveConsumers 计数于 C# PSYNC 拓扑结构性封死同窗；三面各缺一肢，无对物
//! 段不复刻，本文件为 rust 自研「多租户换号教义 × 无盘活扫描」交叠面的
//! 内部契约锁，skip 注记按票面测试验证点 4）。
//!
//! 锁面：快照迭代器装载/读值逐域切上下文经 `set_virtual_context` 域钉枚举
//! 时物理对后，帧戳/键集/读值/释门四点恒等枚举时刻物理域——「快照 = 锚时
//! 物理域投影 + 锚后绝对值记录续推」单律：
//! - SWAPDB 三落位（甲=枚举后装载前，经快照装载窗留钩
//!   [`replication_snapshot_iterator::TEST_SNAPSHOT_LOAD_HOOK`] 单点拾取；
//!   乙=装载后首读前、丙=前两键读值之间，
//!   经同族快照读窗留钩 `TEST_SNAPSHOT_READ_AT` 定臂）各跑一次 SWAPDB 1 2
//!   成对换指，断言副本终态两域内容与主端换后态逐键全等、零 Gone 静默跳发
//!   （落位乙/丙形断言 b 键以旧域值入帧而非跳发）；
//! - FLUSHDB 单律锁：枚举后窗位注 FLUSHDB（作用于 db2），断言旧域全量入帧
//!   （域钉后为完整投影）随 GcDeadDb 退役、副本该库逻辑终态空 = 主。
//!
//! 注入体一律走 wkv 换号内核同款真原语（路由格成对换指 / bump_generation /
//! flush_db 换号段 / DbMetaRecord 原子批经 try_persist_dbmeta_sync 真实入账
//! 续推），与 RESP 慢路径执行段对内核的调用逐环同构，非 mock；留钩只解决
//! 「异步换号入口无法在同步钩内 await」的定序问题，不改写任何裁决逻辑。
//!
//! 负向锁（revert-proof，人工复跑口径）：还原迭代器装载/读值两处为仅
//! `set_context` 逻辑重解析（去掉 `set_virtual_context` 域钉），SWAPDB 三测
//! 与 FLUSHDB 测的旧域投影断言必红——换号介入窗内读值域被逐 op 重解析扳向
//! 换后域，帧内容与帧戳物理域互串、副本经 DbSwap 回放扳指后两域内容对调级
//! 永久发散（FLUSHDB 形未读键集 Gone 跳发致旧域投影残缺）。

mod common;
#[path = "common/primary_assets.rs"]
mod primary_assets_core;
use primary_assets_core::primary_assets;

#[path = "common/cluster_consumer_store.rs"]
mod cluster_cc_store;

use std::{
  num::NonZeroUsize,
  str::from_utf8,
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use async_lock::Mutex;
use cluster_cc_store::cluster_consumer;
use common::{open_node, provider_with_role, replica_host};
use compio::runtime::Runtime;
use waof::AofAddress;
use wbase::{convert::expire_after_to_ticks, time::now_ticks};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  replication::{
    cluster_replication_session::ClusterReplicationSession,
    diskless_replication::replication_snapshot_iterator::{
      TEST_SNAPSHOT_LOAD_HOOK, TEST_SNAPSHOT_READ_AT, TEST_SNAPSHOT_READ_HOOK,
    },
    recovery_status::RecoveryStatus,
    replica_diskless_sync::try_begin_diskless_sync_async,
    replica_replay_task::ReplayAssets,
    sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};
use wkv::{DbMetaRecord, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  aof::waof_sublog::single_log_aof,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  service::NodeService,
  storage::session::storage_session::StorageSession,
};
use wtest_base::wait_for;

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A1;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A2;

/// 留钩静态跨用例串行门（钩槽与臂位系进程级单例，四用例共一二进制内
/// 并行会互相抢占武装，全程持锁串行；本线程 block_on 驱动，阻塞持锁安全）
static CASE_GUARD: Mutex<()> = Mutex::new(());

/// 换号介入落位：甲=快照装载窗留钩单点拾取（枚举后、装载前）；
/// 乙/丙=快照读窗留钩臂位（0=装载后首键读值前，1=前两键读值之间）
#[derive(Clone, Copy)]
enum InjectAt {
  LoadHook,
  ReadAt(usize),
}

/// 窗内注入的换号族动作（内核真原语体）
enum Injection {
  /// SWAPDB 1 2：路由格成对换指 + 换代 + DbSwap 成对记录续推
  Swap12,
  /// FLUSHDB（作用于 db2）：O(1) 换号 + DbMap/GcDeadDb/NextId 原子批续推
  FlushDb2,
}

/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = consumer.try_consume_messages_into(&mut resp);
  resp
}

/// RESP 数组命令组帧
fn resp_command(parts: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", parts.len()).into_bytes();
  for part in parts {
    out.extend(format!("${}\r\n", part.len()).into_bytes());
    out.extend_from_slice(part);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// SELECT 库 + GET 读值（真 RESP 面；None = 键不存在）
fn select_get(consumer: &mut RespSessionConsumer, db: u8, key: &[u8]) -> Option<Vec<u8>> {
  let dbn = db.to_string();
  assert_eq!(
    pump(consumer, &resp_command(&[b"SELECT", dbn.as_bytes()])),
    b"+OK\r\n",
    "SELECT 应答异常"
  );
  let out = pump(consumer, &resp_command(&[b"GET", key]));
  let text = from_utf8(&out).unwrap();
  if text.starts_with("$-1") {
    return None;
  }
  let mut parts = text.split("\r\n");
  let len: usize = parts
    .next()
    .and_then(|head| head.strip_prefix('$'))
    .and_then(|n| n.parse().ok())
    .unwrap_or_else(|| panic!("GET 应答帧异常: {text:?}"));
  let body = parts.next().unwrap_or("");
  Some(
    body
      .as_bytes()
      .get(..len)
      .unwrap_or_else(|| panic!("批量应答长度异常: {text:?}"))
      .to_vec(),
  )
}

type DomainFixtures<'a> = [(u8, &'a [(&'a [u8], &'a [u8])]); 2];

/// 双库同名键夹具（票面形态）：db1={a=1,b=2}、db2={a=9}，真 RESP 写路径
/// （写侧条目经事件汇入账 AOF，锚前记录效果随扫描读值覆盖）
fn seed_fixture(consumer: &mut RespSessionConsumer) {
  let domains: DomainFixtures<'_> = [(1, &[(b"a", b"1"), (b"b", b"2")]), (2, &[(b"a", b"9")])];
  for (db, sets) in domains {
    let dbn = db.to_string();
    assert_eq!(
      pump(consumer, &resp_command(&[b"SELECT", dbn.as_bytes()])),
      b"+OK\r\n"
    );
    for (k, v) in sets {
      assert_eq!(pump(consumer, &resp_command(&[b"SET", k, v])), b"+OK\r\n");
    }
  }
}

/// SWAPDB 1 2 内核真原语注入体（与 swap_databases 编排逐环同构：路由格
/// 成对换指 → bump_generation → DbSwap 成对记录 + 双 0x02 映射原子批入账，
/// 记录地址恒 > 锚、经增量续推副本回放扳指）
fn swap12_kernel(store: &Arc<WedbStore<SegmentedDevice>>) {
  let routing = store.vdb.routing_for(0);
  let f1 = routing.table.get(1).expect("db1 映射在册");
  let f2 = routing.table.get(2).expect("db2 映射在册");
  routing.table.set(1, f2);
  routing.table.set(2, f1);
  store.vdb.bump_generation();
  let session = store.new_session().expect("注入体解析会话");
  let degraded = session
    .try_persist_dbmeta_sync(&[
      Some(DbMetaRecord::DbSwap {
        vns: 0,
        logic_db1: 1,
        logic_db2: 2,
        swapped_db1: f2,
        swapped_db2: f1,
      }),
      Some(DbMetaRecord::DbMap {
        vns: 0,
        logic_db: 1,
        vdb: f2,
      }),
      Some(DbMetaRecord::DbMap {
        vns: 0,
        logic_db: 2,
        vdb: f1,
      }),
    ])
    .expect("DbSwap 成对批入账");
  assert!(degraded.is_empty(), "DbSwap 批同步落盘，不允许降级未落");
}

/// FLUSHDB（db2）内核真原语注入体（与 store.flush_database 编排同构：
/// vdb.flush_db 换号段 → [新映射, 旧域墓碑, 0x05 分配水位] 原子批入账）
fn flushdb2_kernel(store: &Arc<WedbStore<SegmentedDevice>>) {
  let expired_at =
    expire_after_to_ticks(now_ticks(), store.config.gc.db_gc_reclaim_delay_secs as i64);
  let tail_address = store.tail_address();
  let (vns, new_vdb, old_vdb_opt) = store.vdb.flush_db(0, 2, expired_at, tail_address);
  let old_vdb = old_vdb_opt.expect("db2 既有映射换出旧号");
  let session = store.new_session().expect("注入体解析会话");
  let degraded = session
    .try_persist_dbmeta_sync(&[
      Some(DbMetaRecord::DbMap {
        vns,
        logic_db: 2,
        vdb: new_vdb,
      }),
      Some(DbMetaRecord::GcDeadDb {
        expired_at,
        vns,
        old_vdb,
        tail_address,
      }),
      Some(DbMetaRecord::NextId {
        next_virtual_id: store.vdb.next_virtual_id.load(Ordering::Relaxed),
      }),
    ])
    .expect("flush 换号批入账");
  assert!(degraded.is_empty(), "flush 批同步落盘，不允许降级未落");
}

/// 单场景运行体：装配主从两节点 → 预置夹具 → 按落位武装留钩 → 全量同步
/// （同步跑在本任务栈上，钩在任务栈内同步触发——定序无竞态）→ 续推追平
/// → 终态断言
async fn run_case(tag: &'static str, at: InjectAt, injection: Injection) {
  let _serial = CASE_GUARD.lock().await;

  // ===== 主端：AOF 门面 + 存储事件汇 + 真 RESP 预置 =====
  let source = open_node(&format!("{tag}_source"));
  let provider_p = provider_with_role(
    &source,
    PRIMARY_ID,
    7100,
    NodeRole::Primary,
    PRIMARY_ID,
    true,
    Some(0),
  );
  let aof_options = RuntimeServerOptions::default();
  let primary_aof =
    single_log_aof(Arc::clone(&source.wal), &aof_options).expect("装配主端 single_log_aof");
  let _service =
    NodeService::new(Arc::clone(&source.store), primary_aof).expect("注册存储 AOF 事件汇");
  let mut seeder = cluster_consumer(&provider_p, &source.store);
  seed_fixture(&mut seeder);

  // 枚举时物理号基线（域钉目标 = 副本旧域探针口径）
  let (vns1, vdb1) = source.store.vdb.get_virtual_ids(0, 1);
  let (vns2, vdb2) = source.store.vdb.get_virtual_ids(0, 2);
  assert_eq!((vns1, vns2), (0, 0), "根租户物理号恒 0");
  assert!(
    vdb1 > 0 && vdb2 > 0 && vdb1 != vdb2,
    "db1/db2 各自独立物理域在册"
  );

  // ===== 副本：回放装配 + 接收宿主 =====
  let replica = open_node(&format!("{tag}_replica"));
  let provider_r = provider_with_role(
    &replica,
    REPLICA_ID,
    7101,
    NodeRole::Replica,
    PRIMARY_ID,
    true,
    Some(0),
  );
  let rm_r = provider_r.replication_manager().unwrap();
  let replica_aof =
    single_log_aof(Arc::clone(&replica.wal), &aof_options).expect("装配副本 single_log_aof");
  rm_r.set_replay_assets(Some(Arc::new(ReplayAssets::new(
    replica_aof,
    Arc::clone(&replica.store),
    None,
    None,
  ))));
  assert!(
    rm_r.begin_recovery(RecoveryStatus::ReadRole, false),
    "副本恢复门控就位"
  );
  provider_r.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
    Arc::clone(&provider_r),
    Arc::clone(&replica.wal),
    None,
  ))));
  let (server, replica_addr) = replica_host(&provider_r, NonZeroUsize::new(1));

  // ===== 武装落位（钩体 = 换号内核真原语，快照任务栈内同步执行）=====
  let swap = matches!(injection, Injection::Swap12);
  let hook_store = Arc::clone(&source.store);
  let hook = move || {
    if swap {
      swap12_kernel(&hook_store);
    } else {
      flushdb2_kernel(&hook_store);
    }
  };
  match at {
    InjectAt::LoadHook => *TEST_SNAPSHOT_LOAD_HOOK.lock() = Some(Box::new(hook)),
    InjectAt::ReadAt(idx) => {
      *TEST_SNAPSHOT_READ_HOOK.lock() = Some(Box::new(hook));
      TEST_SNAPSHOT_READ_AT.store(idx, Ordering::Relaxed);
    }
  }

  // ===== 全量同步发起（FullResync：两端历史不一致 + 副本零位点）=====
  let rm_p = provider_p.replication_manager().unwrap();
  let assets = primary_assets(&source, &rm_p);
  let meta = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: REPLICA_ID,
    current_primary_repl_id: rm_r.primary_repl_id(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 0),
    current_aof_tail_address: AofAddress::create(1, 0),
    current_replication_offset: AofAddress::create(1, 0),
    checkpoint_entry: None,
  };
  let sync_from =
    try_begin_diskless_sync_async(&provider_p, &assets, PRIMARY_ID, &replica_addr, &meta)
      .await
      .expect("换号介入快照窗后全量同步仍须完成（域钉使窗内换绑对本会话无感）");
  assert!(
    sync_from.get(0).is_some_and(|addr| addr > 0),
    "授予位点（键门闭窗排空点日志尾）必须非零"
  );

  // 收钩自锁：钩未被消费即定序失效（武装泄漏入下一用例形态，显式判死）
  match at {
    InjectAt::LoadHook => {
      assert!(
        TEST_SNAPSHOT_LOAD_HOOK.lock().is_none(),
        "装载窗留钩须被快照装载定序消费"
      );
    }
    InjectAt::ReadAt(_) => {
      TEST_SNAPSHOT_READ_AT.store(usize::MAX, Ordering::Relaxed);
      assert!(
        TEST_SNAPSHOT_READ_HOOK.lock().is_none(),
        "读窗留钩须被快照读值定序消费"
      );
    }
  }

  // ===== 续推收口（commit 栅栏 → 补扫 → 位点追平，同写窗先例口径）=====
  source.wal.commit().await.unwrap();
  let _ = assets.pump.sync_backlog(&source.wal).await;
  let new_tail = source.wal.tail_address() as i64;
  let caught_up = wait_for(
    || rm_r.get_current_replication_offset().get(0) == Some(new_tail),
    Duration::from_secs(10),
  )
  .await;
  assert!(caught_up, "副本复制位点必须追平主端尾");

  // ===== 终态断言 =====
  let mut primary_reader = RespSessionConsumer::new(
    3,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(source.store.new_session().unwrap())),
  );
  let mut replica_reader = RespSessionConsumer::new(
    4,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(replica.store.new_session().unwrap())),
  );
  match injection {
    Injection::Swap12 => {
      // 主端换后态基线：db1={a:9}、db2={a:1,b:2}（SWAPDB 确已落窗）
      assert_eq!(
        select_get(&mut primary_reader, 1, b"a").as_deref(),
        Some(b"9".as_slice()),
        "主端 db1.a = 换入值 9"
      );
      assert_eq!(
        select_get(&mut primary_reader, 1, b"b"),
        None,
        "主端 db1.b 随换指不可见"
      );
      assert_eq!(
        select_get(&mut primary_reader, 2, b"a").as_deref(),
        Some(b"1".as_slice()),
        "主端 db2.a = 换入值 1"
      );
      assert_eq!(
        select_get(&mut primary_reader, 2, b"b").as_deref(),
        Some(b"2".as_slice()),
        "主端 db2.b = 换入值 2"
      );
      // 副本终态 = 主端换后态逐键全等（旧域旧内容入帧 + DbSwap 回放扳指；
      // 乙/丙形主锁面：b 键以旧域值入帧、零 Gone 静默跳发）
      assert_eq!(
        select_get(&mut replica_reader, 2, b"a").as_deref(),
        Some(b"1".as_slice()),
        "副本 db2.a 须=旧 db1 域值 1（换号撕裂形：域内键集/读值与帧戳不同源即红）"
      );
      assert_eq!(
        select_get(&mut replica_reader, 2, b"b").as_deref(),
        Some(b"2".as_slice()),
        "副本 db2.b 须以旧域值入帧而非 Gone 跳发"
      );
      assert_eq!(
        select_get(&mut replica_reader, 1, b"a").as_deref(),
        Some(b"9".as_slice()),
        "副本 db1.a 须=换入旧 db2 域值 9"
      );
      assert_eq!(
        select_get(&mut replica_reader, 1, b"b"),
        None,
        "副本 db1.b 须随扳指离场（值互串形即红）"
      );
    }
    Injection::FlushDb2 => {
      // 主端：db1 不受波及、db2 换号清空
      assert_eq!(
        select_get(&mut primary_reader, 1, b"a").as_deref(),
        Some(b"1".as_slice()),
        "主端 db1 不受 FLUSHDB db2 波及"
      );
      assert_eq!(
        select_get(&mut primary_reader, 2, b"a"),
        None,
        "主端 db2 已换号清空"
      );
      // 副本逻辑终态 = 主（DbMap 绝对值 + GcDeadDb 经锚后续推扳指+判死）
      assert_eq!(
        select_get(&mut replica_reader, 1, b"a").as_deref(),
        Some(b"1".as_slice()),
        "副本 db1 完好"
      );
      assert_eq!(
        select_get(&mut replica_reader, 2, b"a"),
        None,
        "副本 db2 终态空 = 主"
      );
      // 旧域全量投影锁：域钉后旧物理域 (0, vdb2) 的枚举键集全部随旧戳入帧
      // 落副本旧域（Gone 跳发残缺投影即红），随 GcDeadDb 延时退役
      let probe = replica.store.new_session().unwrap();
      // 直设探针显式携逻辑域真值 (0, 2)（版本轨=逻辑域种子；纯读零 bump）
      probe.set_virtual_context(0, vdb2, 0, 2);
      let storage = StorageSession::new_readonly(probe.enter_batch());
      assert_eq!(
        storage.read_string(b"a").await.unwrap().as_deref(),
        Some(b"9".as_slice()),
        "副本旧 db2 域须收齐枚举时刻全量投影（帧戳域=读值域）"
      );
    }
  }
  server.dispose();
}

/// 落位甲：换号介入域枚举之后、逐域装载之前（快照装载窗留钩单点定序）
#[test]
fn snapshot_swapdb_after_enumeration_before_load_pins_old_domain() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    run_case("snap_swap_a", InjectAt::LoadHook, Injection::Swap12).await;
  });
}

/// 落位乙：换号介入全部装载之后、首键读值之前（读窗留钩臂位 0）
#[test]
fn snapshot_swapdb_after_load_before_first_read_pins_old_domain() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    run_case("snap_swap_b", InjectAt::ReadAt(0), Injection::Swap12).await;
  });
}

/// 落位丙：换号介入同域前两键读值之间（读窗留钩臂位 1）——断言后段键
/// 仍以旧域值入帧（b 键不跳发），域钉旁路代数守卫窗内换代无感
#[test]
fn snapshot_swapdb_between_key_reads_pins_old_domain() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    run_case("snap_swap_c", InjectAt::ReadAt(1), Injection::Swap12).await;
  });
}

/// FLUSHDB 单律锁：枚举后窗位注 FLUSHDB，旧域全量入帧随 GcDeadDb 退役，
/// 副本该库终态空 = 主（正投影形，非「脆性押注记录绝对值」）
#[test]
fn snapshot_flushdb_in_window_projects_whole_old_domain() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    run_case("snap_flush_w", InjectAt::ReadAt(0), Injection::FlushDb2).await;
  });
}
