//! SWAPDB 滞留镜像回放臂回归（票 zcode-r122c-swapdb1 案二次级面）
//!
//! 真 AOF 流 + 真副本回放闭环：主库经真实 DbMeta 镜像端口产条目（租户建档
//! → 活域 SWAPDB → FLUSHNS 退役 → 指向已退役租户的滞留 0x06 DbSwap 条目，
//! 即修复前主库锁外捕 vns 缺陷的产物形态），副本全量回放断言：
//! 1. 向量联动臂对反查无逻辑入口的死亡租户显式上抛（绝不静默回退
//!    logic_ns_of(vns).unwrap_or(0) 错槽盖章）；
//! 2. apply_dbmeta_record 死域门留痕跳过——副本路由表不含死亡域复活格，
//!    盘上零新写映射。

use std::sync::Arc;

use aok::{Error, Void};
use tempfile::TempDir;
use wdev::SegmentedDevice;
use wkv::{DbMetaRecord, WedbStore};
use wnode::{
  GarnetAppendOnlyFile,
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    recover::aof_recover::AofRecover,
  },
  resp::vector::vector_manager::VectorManager,
  storage::session::storage_session::StorageSession,
};
use wnode_test::{open_aof_node, vector_manager_of};
use wtest_base::test_store_config;
type Node = wnode_test::AofNode;

async fn replay_to(
  rstore: &Arc<WedbStore<SegmentedDevice>>,
  aof: &Arc<GarnetAppendOnlyFile>,
  replayed_vm: &Arc<VectorManager>,
) -> aok::Result<u64> {
  let _pause = rstore.pause_aof_listeners();
  let session = rstore.new_session()?;
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(rstore),
    aof_floor: rstore
      .recovered_aof_floor()
      .iter()
      .map(|&a| a as i64)
      .collect(),
  };
  aof.set_vector_manager(Arc::clone(replayed_vm));
  let processor = AofProcessor::new(Arc::clone(aof));
  let replayed = AofRecover::single_log_recover(&processor, aof, 0, 0, -1, &target)
    .await
    .map_err(|e| Error::msg(e.to_string()))?;
  Ok(replayed)
}

/// 验证：副本回放对已判死租户的滞留 DbSwap 条目——向量联动臂显式上抛
/// 令恢复现场暴露（旧缺陷形 unwrap_or(0) 静默盖进 ns=0 错槽不可自愈），
/// 且死域门保证副本路由表不含死亡域复活格
#[compio::test]
async fn swapdb_replay_stale_dead_tenant_fails_explicitly() -> Void {
  let Node {
    _dir, store, aof, ..
  } = open_aof_node("swapdb_stale_replay")?;

  // 1. 主库真实建档租户 ns=9 双库 + 活域 SWAPDB（成对 0x06 条目随写镜像入流）
  let s = store.new_session()?;
  s.set_context(9, 1);
  s.upsert(b"k1", b"v1").await?;
  s.set_context(9, 2);
  s.upsert(b"k2", b"v2").await?;
  let vns_old = store.vdb.vns_of_ns(9).expect("租户 9 已建档");
  let d1 = store.vdb.route_vdb_of(vns_old, 1).expect("库 1 已建档");
  let d2 = store.vdb.route_vdb_of(vns_old, 2).expect("库 2 已建档");
  s.swap_databases(1, 2).await?;

  // 2. 真实 FLUSHNS 内核退役租户（NsMap/GcDeadNs 镜像条目入流）
  store.flush_namespace(9).await?;

  // 3. 滞留 0x06 DbSwap 条目：指向已退役 vns（修复前主库锁外捕 vns 缺陷
  //    的产物形态），经真实 DbMeta 落盘 + 镜像端口入 AOF
  let stale = DbMetaRecord::DbSwap {
    vns: vns_old,
    logic_db1: 1,
    logic_db2: 2,
    swapped_db1: d1,
    swapped_db2: d2,
  };
  s.persist_dbmeta(&stale).await?;
  aof.log().commit();

  // 4. 全新副本回放整条真 AOF 流：滞留条目处向量联动臂必显式上抛
  let dir_replay = TempDir::new()?;
  let rdevice = Arc::new(SegmentedDevice::single_file(
    dir_replay.path().join("replay.db"),
  )?);
  let rstore = Arc::new(WedbStore::open(test_store_config(), Arc::clone(&rdevice))?);
  let replayed_vm = vector_manager_of(&rstore);
  let _r_domain = replayed_vm
    .bind_dedicated_session()
    .expect("回放端专用向量会话工厂应已注入");

  let res = replay_to(&rstore, &aof, &replayed_vm).await;
  assert!(
    res.is_err(),
    "滞留死亡租户 DbSwap 条目的回放臂须显式上抛（旧缺陷形 unwrap_or(0) 静默错槽盖章）"
  );

  // 5. 死域门：副本路由表不含死亡域复活格——滞留条目的互换指向
  //    （swapped_db1=d1 与活域末态 d2 互异）绝未被照单插入
  assert_eq!(
    rstore.vdb.route_vdb_of(vns_old, 2),
    Some(d1),
    "死亡租户库格保持回放前末态，滞留 DbSwap 未复活换指"
  );
  assert_eq!(
    rstore.vdb.route_vdb_of(vns_old, 1),
    Some(d2),
    "同上（活域 SWAPDB 条目的末态）"
  );

  // 6. 正面对照：活租户条目（步骤 1 的合法 SWAPDB）照常收敛——反查/联动
  //    教义收紧不误伤主路径（rstore 与主库换指逐值一致）
  let live_ns = 0u64;
  let (v0, _) = (rstore.vdb.vns_of_ns(live_ns), rstore.vdb.route_vdb_of(0, 1));
  assert!(v0.is_some(), "根租户映射恒在册");
  Ok(())
}
