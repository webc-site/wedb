//! 启动期 DbMeta 映射重建就绪门禁与重建后代数推进测试
//!
//! 覆盖三件事（时序口径对标 libs/host/GarnetServer.cs:527-533 的
//! `Provider.RecoverAsync()` 同步完成后才 `servers[i].Start()`）：
//! - `WedbStore::open_shared` 返回的句柄即「映射面已就绪」凭据——重建在返回前
//!   于当前运行时内驱动到完成，调用方未让出一次执行权即可读到重建结果
//!   （历史实现 `drop(spawn(..))` 在 compio 下即抛即取消，重建从未落地）；
//! - 检查点重启后首屏会话命中的是磁盘既有虚拟 ID，不盲分配新号，
//!   `next_virtual_id` 不回退（doc/zh/db.md「无缝恢复」）；冷租户条款下
//!   非根域库级路由表零常驻，首访经严格会话挂起面 + resolve_context
//!   点查磁盘装载，命中既有映射绝不换号；
//! - 重建尾端代数推进与就绪门禁成对：代推进使已建立会话在下一次 `session_prefix`
//!   慢路径重解析，杜绝「新代数 + 旧映射」自洽导致的幽灵前缀。

use std::{
  fs::create_dir_all,
  sync::{Arc, atomic::Ordering::Relaxed},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wval::SessionPrefixBuf;

/// 与根域 (0, 0) 错开的逻辑命名空间/库编号
const LOGIC_NS: u64 = 7;
const LOGIC_DB: u64 = 3;

/// 测试用存储配置（与 store 套件同口径的小预算容量）
fn test_config() -> aok::Result<StoreConfig> {
  Ok(StoreConfig::new(2048, 64 * 1024, 16, 0.5)?)
}

/// 逻辑命名空间当前映射的虚拟命名空间 ID（未映射为 None）
fn mapped_vns(store: &Arc<WedbStore<SegmentedDevice>>, logic_ns: u64) -> Option<u64> {
  store.vdb.ns_map.pin().get(&logic_ns).copied()
}

/// 虚拟命名空间下逻辑库当前映射的虚拟库 ID（未映射为 None）
fn mapped_vdb(store: &Arc<WedbStore<SegmentedDevice>>, vns: u64, logic_db: u64) -> Option<u64> {
  store
    .vdb
    .db_routing
    .pin()
    .get(&vns)
    .and_then(|routing| routing.table.get(logic_db))
}

/// 门禁：`open_shared` 返回前已把映射重建驱动到完成（调用方零 await 即可观测）
///
/// 可观测判据取重建尾端的代数推进与根域初始化：重建任务一旦被即抛即取消，
/// `generation` 停在 `VirtualDbManager::new` 的初值 1；门禁下返回即为 2。
#[test]
fn open_shared_returns_after_rebuild_completion() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("gate.db"))?);
    let store = WedbStore::open_shared(test_config()?, device)?;

    // 未让出执行权（无 await、无轮询）即已就绪：代数由重建尾端推进
    assert_eq!(
      store.vdb.generation.load(Relaxed),
      2,
      "open_shared 返回时重建须已完成并换代（历史 fire-and-cancel 形态停在 1）"
    );
    // 重建首个动作是强制持久化根域 (0, 0)，且不虚增分配水位
    assert_eq!(
      store.vdb.next_virtual_id.load(Relaxed),
      1,
      "空日志重建不得分配新虚拟 ID"
    );
    assert_eq!(mapped_vns(&store, 0), Some(0));
    assert_eq!(mapped_vdb(&store, 0, 0), Some(0));

    // 就绪后建立的会话直接落在重建出的根域前缀上
    let session = store.new_session()?;
    assert_eq!(
      session.session_prefix().as_slice(),
      SessionPrefixBuf::new(0, 0).as_slice()
    );

    // 门禁形态的可行性实测：open_shared 在已运行的运行时内嵌套 block_on 驱动重建，
    // 空日志扫描不触 IO，故此处以同形态的刷盘（真实设备 IO + Group Commit 等待）
    // 验证嵌套驱动可完成、不死锁不 panic
    session.upsert(b"probe_k", b"probe_v").await?;
    let st = Arc::clone(&store);
    Runtime::with_current(|rt| rt.block_on(st.flush_all()))?;
    assert_eq!(session.read(b"probe_k").await?, Some(b"probe_v".to_vec()));

    drop(session);
    drop(store);
    OK
  })
}

/// 检查点重启：门禁内重建还原磁盘映射，首屏会话命中旧虚拟 ID 且零盲分配
#[compio::test]
async fn restart_rebuild_resolves_persisted_vid_and_keeps_watermark() -> Void {
  let dir = tempdir()?;
  let cpr_dir = dir.path().join("checkpoints");
  create_dir_all(&cpr_dir)?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("restart.db"))?);

  // 首进程：建逻辑 (7, 3) 并写数据（set_context 同批落 DbMeta 映射记录）
  let store = Arc::new(WedbStore::open(test_config()?, Arc::clone(&device))?);
  let session = store.new_session()?;
  session.set_context(LOGIC_NS, LOGIC_DB);
  session.upsert(b"restart_k", b"restart_v").await?;
  let old_vns = mapped_vns(&store, LOGIC_NS).expect("set_context 须建逻辑命名空间映射");
  let old_vdb = mapped_vdb(&store, old_vns, LOGIC_DB).expect("set_context 须建逻辑库映射");
  let allocated_before = store.vdb.next_virtual_id.load(Relaxed);
  let meta = store
    .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
    .await?;
  drop(session);
  drop(store);

  // 重启：恢复段（wcpr 恢复 → await 重建 → 交出句柄）还原映射面
  let recovered = Arc::new(WedbStore::recover(&cpr_dir, meta.token, Arc::clone(&device)).await?);
  assert_eq!(
    mapped_vns(&recovered, LOGIC_NS),
    Some(old_vns),
    "重建须还原磁盘逻辑命名空间映射（ns 标量基线），不得另起新号"
  );
  // 冷租户条款：非根域库级路由表零常驻（未装载），首访点查磁盘回建
  assert_eq!(
    mapped_vdb(&recovered, old_vns, LOGIC_DB),
    None,
    "冷库路由表零常驻，不得全量灌入内存"
  );
  // 分配水位只抬不回退，且严格高于磁盘最大已用编号
  let allocated_after = recovered.vdb.next_virtual_id.load(Relaxed);
  assert!(
    allocated_after > old_vns.max(old_vdb) && allocated_after >= allocated_before,
    "next_virtual_id 须抬到磁盘水位之上且不回退: before={allocated_before}, after={allocated_after}"
  );

  // 重启后首个会话（严格态）：冷库拒绝盲分配，点查装载命中磁盘既有 ID
  // 且零新分配（映射权威在磁盘，绝不换号）
  let first = recovered.new_session()?;
  first.set_strict_context(true);
  assert!(
    !first.set_context(LOGIC_NS, LOGIC_DB),
    "冷库严格会话拒绝盲分配（挂起面语义）"
  );
  let (vns, vdb) = recovered.resolve_context(LOGIC_NS, LOGIC_DB).await?;
  assert_eq!(
    (vns, vdb),
    (old_vns, old_vdb),
    "点查装载命中磁盘既有映射，不得另起新号"
  );
  assert!(first.set_context(LOGIC_NS, LOGIC_DB), "装载后重放物化");
  assert_eq!(
    first.session_prefix().as_slice(),
    SessionPrefixBuf::new(old_vns, old_vdb).as_slice(),
    "首屏会话前缀须落在磁盘既有映射上"
  );
  assert_eq!(
    recovered.vdb.next_virtual_id.load(Relaxed),
    allocated_after,
    "门禁就绪后解析不得盲分配新虚拟 ID"
  );
  // 旧前缀未成幽灵段：数据按原逻辑库可读
  assert_eq!(
    first.read(b"restart_k").await?,
    Some(b"restart_v".to_vec()),
    "重建后旧物理前缀数据必须按原逻辑库可读"
  );

  drop(first);
  drop(recovered);
  Ok(())
}

/// 代数与门禁成对：重建尾端换代后，已建立会话的缓存代落后并收敛回同一映射
#[compio::test]
async fn rebuild_tail_generation_bump_forces_session_reparse() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("reparse.db"))?);
  // 无门禁的嵌入式入口（`open` 不重建）：会话先于重建建立并缓存当代
  let store = Arc::new(WedbStore::open(test_config()?, device)?);
  let session = store.new_session()?;
  session.set_context(LOGIC_NS, LOGIC_DB);
  let cached_vns = session.active_vns.load(Relaxed);
  let cached_vdb = session.active_vdb.load(Relaxed);
  let gen_before = store.vdb.generation.load(Relaxed);
  let allocated_before = store.vdb.next_virtual_id.load(Relaxed);
  assert_eq!(
    session.last_generation.load(Relaxed),
    gen_before,
    "set_context 须缓存解析前的代"
  );

  // 宿主自行驱动重建（嵌入式/无运行时口径）：尾端必换代
  store.rebuild_vdb_async().await?;
  let gen_after = store.vdb.generation.load(Relaxed);
  assert_eq!(
    gen_after,
    gen_before + 1,
    "rebuild_vdb_async 尾端须推进代数，否则先建会话永不再刷新"
  );
  assert_ne!(
    session.last_generation.load(Relaxed),
    gen_after,
    "缓存代落后 ⇒ 下一次 session_prefix 必走慢路径重解析"
  );

  // 重解析收敛到同一映射（磁盘 DbMeta 记录即重建来源），且零盲分配
  assert_eq!(
    session.session_prefix().as_slice(),
    SessionPrefixBuf::new(cached_vns, cached_vdb).as_slice()
  );
  assert_eq!(
    store.vdb.next_virtual_id.load(Relaxed),
    allocated_before,
    "重解析命中既有映射，不得消耗新虚拟 ID"
  );
  assert_eq!(session.last_generation.load(Relaxed), gen_after);

  drop(session);
  drop(store);
  Ok(())
}
