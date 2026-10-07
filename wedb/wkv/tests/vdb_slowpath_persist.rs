#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 换代后 virtual_domain 慢路径重解析映射落盘锁（票
//! wkv-vdb-generation-swap-slow-path-blind-alloc-unpersisted-watermark-reuse）
//!
//! 缺陷形：`WedbStore::flush_namespace` 换号批只携 NsMap + 当时的 0x05，新
//! vns 路由表为空表；存量会话不重发 SELECT，其下一条写经 `virtual_domain`
//! 慢路径给 (new_vns, db) 盲分配虚号 N 并直接回 `OK`——映射不落盘、0x05 水位
//! 亦不抬升。正常重启即令已确认写永不可达（已确认写丢失），且水位未越 N 时
//! 重启后 N 被他逻辑库首取复用、旧前缀记录整体显形（跨域幽灵读）。
//!
//! 修复契约：
//! 1. `get_virtual_ids_with_created` 透传双 `created` 标志，慢路径命中新分配
//!    即按 set_context 同款 `[NsMap?, DbMap?, NextId]` safe-order 组批走
//!    `try_persist_dbmeta_sync` 单点（同步域零 await；根域 (0, 0) 恒不落）；
//! 2. created 全假（如 FLUSHDB 同 vns 格内换指，慢路径命中既有格）零新分配
//!    零落盘写放大。
//!
//! 注入体走真原语（`store.flush_namespace`/`store.flush_database` 内核含
//! lock_dbmeta 事务与原子批入账，非 mock）；重启面用检查点恢复入口
//! （`create_checkpoint` → `WedbStore::recover`，同 device 单趟扫描内 DbMeta
//! 重建）。核心断言全部取自磁盘权威面：`probe_db_mapping` 点查命中、重建后
//! 严格会话拒绝盲分配经 `resolve_context` 装载回同一物理域、0x05 水位经
//! rebuild 折叠后严格高于慢路径新号。
//! revert-proof：撤 `virtual_domain` 慢路径 persist 臂后，场景一「盘上映射
//! 命中」断言必转红（盘上无 0x02，resolve_context 另取新号亦红）。

use std::{
  fs::create_dir_all,
  sync::{Arc, atomic::Ordering::Relaxed},
};

use aok::Void;
use tempfile::tempdir;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

/// 与根域 (0, 0) 错开的逻辑命名空间/库编号（票面最小复现的非根上下文）
const LOGIC_NS: u64 = 5;
const LOGIC_DB: u64 = 1;
/// 重启后用于号复用观察的同租户另一逻辑库
const OTHER_DB: u64 = 2;

fn test_config() -> aok::Result<StoreConfig> {
  Ok(StoreConfig::new(2048, 64 * 1024, 16, 0.5)?)
}

/// 场景一主锁：FLUSHNS 换号 → 存量会话慢路径重解析盲分配 → 确认写 →
/// 检查点重启 → 盘上映射/水位/数据三面收敛
#[compio::test]
async fn flushns_slowpath_reparse_persists_mapping_and_watermark() -> Void {
  let dir = tempdir()?;
  let cpr_dir = dir.path().join("checkpoints");
  create_dir_all(&cpr_dir)?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("slowpath_persist.db"),
  )?);
  let (vns_new, vdb_slow);
  let cpr_token: u128;
  {
    let store = Arc::new(WedbStore::open(test_config()?, Arc::clone(&device))?);
    // 建档：set_context 同步批落 NsMap/DbMap/NextId，并写换号前键
    let s1 = store.new_session()?;
    assert!(s1.set_context(LOGIC_NS, LOGIC_DB), "非根租户上下文物化");
    s1.upsert(b"k1", b"pre-flush").await?;
    let (mk_vns, mk_vdb) = s1.virtual_domain();
    assert_eq!(mk_vns, store.vdb.vns_of_ns(LOGIC_NS).expect("建档 vns"));
    let probe0 = store.new_session()?;
    probe0.set_context(0, 0);
    assert_eq!(
      store.probe_db_mapping(&probe0, mk_vns, LOGIC_DB).await?,
      Some(mk_vdb),
      "set_context 同步批盘上可见（probe 形态基线）"
    );
    drop(probe0);

    // 真 FLUSHNS 内核换号：新 vns 落册、其路由表为空表（批不携任何 DbMap）
    let (new_vns, old_vns_opt) = store.flush_namespace(LOGIC_NS).await?;
    assert!(old_vns_opt.is_some(), "非首映射 FLUSHNS 必换出旧 vns");
    assert_ne!(new_vns, 0, "非根租户换号必落非零新 vns");

    // 主缺陷面：存量会话不重发 SELECT（上下文仍 (LOGIC_NS, LOGIC_DB)、缓存代
    // 落后），下一条写经 virtual_domain 慢路径重解析——现码此处对
    // (new_vns, LOGIC_DB) 盲分配新虚号
    let alloc_before = store.vdb.next_virtual_id.load(Relaxed);
    let (vns2, vdb2) = s1.virtual_domain();
    assert_eq!(vns2, new_vns, "慢路径解析到新代 vns");
    assert_eq!(vdb2, alloc_before, "慢路径确已发生新分配（取号即本水位值）");
    assert_eq!(store.vdb.next_virtual_id.load(Relaxed), alloc_before + 1);
    vns_new = vns2;
    vdb_slow = vdb2;

    // 修复臂：created 命中即 [DbMap, NextId] 批同步落盘——盘上映射必命中
    let probe = store.new_session()?;
    probe.set_context(0, 0);
    assert_eq!(
      store.probe_db_mapping(&probe, vns_new, LOGIC_DB).await?,
      Some(vdb_slow),
      "慢路径新分配映射必落盘（撤 persist 臂即此红：盘上无 0x02 记录）"
    );

    // 慢路径收尾的确认写（落新代物理域）
    s1.upsert(b"k2", b"post-flush-ack").await?;

    cpr_token = store
      .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
      .await?
      .token;
    drop(probe);
    drop(s1);
    drop(store);
  }

  // 常规重启：恢复段以盘上 DbMeta 为权威重建映射面
  let store2 = Arc::new(WedbStore::recover(&cpr_dir, cpr_token, Arc::clone(&device)).await?);

  // 0x05 水位经同批抬升：重建折叠后严格高于慢路径新号（未落则此红 =
  // 旧号复用撞号入口）
  assert!(
    store2.vdb.next_virtual_id.load(Relaxed) > vdb_slow,
    "重启分配水位不得回落至慢路径新号之下"
  );

  // 冷租户严格会话首访：盘上映射权威经点查装载回同一物理域，绝不另起新号
  let s3 = store2.new_session()?;
  s3.set_strict_context(true);
  assert!(
    !s3.set_context(LOGIC_NS, LOGIC_DB),
    "重启后库映射未常驻，严格会话拒绝盲分配（挂起面语义）"
  );
  let (rvns, rvdb) = store2.resolve_context(LOGIC_NS, LOGIC_DB).await?;
  assert_eq!(
    (rvns, rvdb),
    (vns_new, vdb_slow),
    "点查装载须命中慢路径落盘的同一物理域（撤 persist 臂即此红：盘上 miss 另取新号）"
  );
  assert!(s3.set_context(LOGIC_NS, LOGIC_DB), "装载后重放物化");

  // 已确认写随重启存活；换号前旧域数据按清库承诺不可读
  assert_eq!(
    s3.read(b"k2").await?,
    Some(b"post-flush-ack".to_vec()),
    "慢路径落域后的确认写不得随重启丢失"
  );
  assert_eq!(
    s3.read(b"k1").await?,
    None,
    "FLUSHNS 前旧域数据在新代逻辑库不可读"
  );

  // 号复用封堵回归：同租户他逻辑库重启后首取的号严格大于慢路径新号
  let s4 = store2.new_session()?;
  assert!(s4.set_context(LOGIC_NS, OTHER_DB), "他逻辑库上下文物化");
  let (_vns4, vdb4) = s4.virtual_domain();
  assert!(
    vdb4 > vdb_slow,
    "重启后他库首取号必严格高于慢路径新号（水位封堵旧号复用）"
  );
  assert_eq!(
    s4.read(b"k2").await?,
    None,
    "慢路径新代域数据不得在他逻辑库显形（跨域幽灵读封堵）"
  );

  drop(s3);
  drop(s4);
  drop(store2);
  Ok(())
}

/// 场景二零写放大回归：FLUSHDB 同 vns 格内换指（批已携 DbMap），存量会话
/// 慢路径重解析命中既有格——created 全假，零新分配、零额外落盘
#[compio::test]
async fn flushdb_same_cell_reparse_allocates_nothing() -> Void {
  let dir = tempdir()?;
  let store = Arc::new(WedbStore::open(
    test_config()?,
    Arc::new(SegmentedDevice::single_file(
      dir.path().join("slowpath_noop.db"),
    )?),
  )?);
  let s1 = store.new_session()?;
  assert!(s1.set_context(0, LOGIC_DB), "根租户库 1 上下文物化");
  let (r_vns, _) = store.flush_database(0, LOGIC_DB).await?;
  let (cell_vns, cell_vdb) = s1.virtual_domain();
  assert_eq!(cell_vns, r_vns, "根域库格换指同 vns");

  let alloc_before = store.vdb.next_virtual_id.load(Relaxed);
  // 强制回退缓存代令下一次解析走慢路径（模拟存量会话遇并发换代）
  s1.last_generation.store(0, Relaxed);
  let (vns2, vdb2) = s1.virtual_domain();
  assert_eq!((vns2, vdb2), (cell_vns, cell_vdb), "慢路径复用 flush 格");
  assert_eq!(
    store.vdb.next_virtual_id.load(Relaxed),
    alloc_before,
    "created 全假慢路径不得盲分配新号"
  );

  drop(s1);
  Ok(())
}
