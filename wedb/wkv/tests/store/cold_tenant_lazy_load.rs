//! 冷租户与冷库按需点查装载、路由快照空闲析构与死亡账本到期前缀回收
//! （doc/zh/db.md「冷租户按需加载与零全局常驻内存」承诺的集成验收）。
//!
//! 三条主线：
//! - 重启后冷库不装载（内存仅 ns 标量基线），首次访问点查磁盘 DbMeta 装载
//!   既有映射且绝不换号（换号即旧域数据判死丢失——本模块要消灭的核心缺陷）；
//! - 会话解绑引用归零后，GC 轮次空闲析构路由快照（内存归零），后续访问经
//!   点查装载回建，绑定期间快照绝不被析构；
//! - 死亡账本小根堆只弹到期前缀：未到期与截断线未越界条目保留。
//!
//! 自研依据: doc/zh/db.md 冷租户与冷库 0 内存常驻按需加载

use std::{
  sync::{Arc, atomic::Ordering::Relaxed},
  time::Duration,
};

use aok::{OK, Result, Void};
use compio::time::sleep;
use tempfile::tempdir;
use wbase::time::now_ticks;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{
  GcConfig, GcManager, StoreConfig, WedbStore,
  vdb::{GcDeadEntry, GcDeadLog, ROOT_VIRTUAL_ID},
};

/// 测试库配置（默认 GC 关闭，测试手动驱动 GcManager）
fn config() -> Result<StoreConfig> {
  Ok(StoreConfig::new(2048, 64 * 1024, 16, 0.5)?)
}

/// 空闲析构专用配置（期限 0 秒即期，默认关闭过期扫描）
fn idle_config() -> Result<StoreConfig> {
  let mut cfg = config()?;
  cfg.gc = GcConfig {
    route_idle_evict_secs: 0,
    ..GcConfig::default()
  };
  Ok(cfg)
}

/// 检查点恢复后的冷态断言：ns 标量基线在册、库级路由表零常驻
async fn assert_cold_after_recover(
  store: &Arc<WedbStore<SegmentedDevice>>,
  logic_ns: u64,
  vns0: u64,
  logic_db: u64,
) -> Void {
  assert_eq!(
    store.vdb.ns_map.pin().get(&logic_ns).copied(),
    Some(vns0),
    "ns 标量基线装载"
  );
  assert_eq!(
    store.vdb.db_routing.pin().len(),
    1,
    "仅根域快照常驻，租户路由表零常驻"
  );
  assert!(
    store.vdb.is_cold_db(logic_ns, logic_db),
    "冷库判定：租户在册而库映射未装载"
  );
  OK
}

/// 冷库点查装载：重启后路由表缺席（冷，零常驻），严格会话切换被拒（挂起面），
/// 点查磁盘装载既有映射不换号，数据原样可读
#[compio::test]
async fn test_cold_db_lazy_load_no_renumber() -> Void {
  let dir = tempdir()?;
  let db_path = dir.path().join("cold_tenant.db");
  let ckpt_dir = dir.path().join("checkpoints");

  let (vns0, vdb0, token) = {
    let store = Arc::new(WedbStore::open(
      config()?,
      Arc::new(SegmentedDevice::single_file(&db_path)?),
    )?);
    let session = store.new_session()?;
    // 非严格会话（内部语义）：映射创建即持久化 DbMeta（磁盘为映射权威）
    session.set_context(5, 3);
    let ids = store.vdb.get_virtual_ids(5, 3);
    session.upsert(b"cold:key", b"cold:value").await?;
    store.flush_all().await?;
    let meta = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;
    assert!(store.vdb.route_vdb_of(ids.0, 3).is_some(), "装载期路由在册");
    (ids.0, ids.1, meta.token)
  }; // 模拟停机：释放原 store 与 device

  // 恢复：重建只装载 ns 标量与死亡账本，库级路由表冷（零常驻）
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let store = Arc::new(WedbStore::recover(&ckpt_dir, token, device).await?);
  assert_cold_after_recover(&store, 5, vns0, 3).await?;

  // 严格会话（RESP 连接同形态）：未装载时拒绝物化（挂起面语义）
  let session = store.new_session()?;
  session.set_strict_context(true);
  assert!(
    !session.set_context(5, 3),
    "严格会话未装载上下文必须拒绝盲分配"
  );

  // 异步点查装载：命中磁盘既有映射，绝不换号
  let (vns, vdb) = store.resolve_context(5, 3).await?;
  assert_eq!((vns, vdb), (vns0, vdb0), "点查装载命中既有映射不换号");
  assert_eq!(store.vdb.route_vdb_of(vns, 3), Some(vdb0));

  // 重放上下文物化后原样读回磁盘数据
  assert!(session.set_context(5, 3), "装载后重放必须物化成功");
  assert_eq!(
    session.read(b"cold:key").await?.as_deref(),
    Some(b"cold:value".as_slice()),
    "装载域数据可读"
  );
  Ok(())
}

/// 路由快照空闲析构：解绑引用归零登记期限，GC 轮次摘除释放（内存归零）；
/// 绑定期间不被析构；析构后点查装载回建不换号，数据原样可读
#[compio::test]
async fn test_route_idle_evict_and_reload() -> Void {
  let dir = tempdir()?;
  let store = Arc::new(WedbStore::open(
    idle_config()?,
    Arc::new(SegmentedDevice::single_file(
      dir.path().join("idle_evict.db"),
    )?),
  )?);
  let mgr = GcManager::new(&store);

  // 绑定租户写入数据
  let (vns0, vdb0) = {
    let session = store.new_session()?;
    session.set_context(7, 0);
    let ids = store.vdb.get_virtual_ids(7, 0);
    session.upsert(b"idle:key", b"idle:value").await?;
    assert!(
      store.vdb.db_routing.pin().get(&ids.0).is_some(),
      "绑定期间快照在册"
    );
    // 绑定期间空闲析构被引用拦截（期限已满也不可析构）
    assert!(!store.vdb.evict_idle_route(ids.0), "绑定期间快照不得析构");
    ids
  }; // 会话 Drop：解绑引用归零，登记期限（0 秒即期）

  sleep(Duration::from_millis(5)).await;
  mgr.run_once().await?;
  assert!(
    store.vdb.db_routing.pin().get(&vns0).is_none(),
    "空闲析构后租户路由快照摘除，内存归零"
  );
  assert_eq!(store.vdb.db_routing.pin().len(), 1, "仅根域快照保留");

  // 析构后访问：严格会话挂起面拒绝 → 点查装载回建 → 不换号、数据可读
  let session = store.new_session()?;
  session.set_strict_context(true);
  assert!(!session.set_context(7, 0));
  let (vns, vdb) = store.resolve_context(7, 0).await?;
  assert_eq!((vns, vdb), (vns0, vdb0), "析构后点查装载不换号");
  assert!(session.set_context(7, 0));
  assert_eq!(
    session.read(b"idle:key").await?.as_deref(),
    Some(b"idle:value".as_slice())
  );
  Ok(())
}

/// 空闲析构与 set_context 交错不覆写（vdb-route-race-fix 定向用例）：
/// 冷检通过后、解析前的间隙内快照恰被 GC 摘除（留钩放大竞态窗口），
/// 严格会话必须回退异步点查——零盲分配、零落盘覆写，重放后既有映射
/// 原样不换号，旧域数据对用户绝不消失
#[compio::test]
async fn test_evict_between_cold_check_and_resolve_no_renumber() -> Void {
  let dir = tempdir()?;
  let store = Arc::new(WedbStore::open(
    idle_config()?,
    Arc::new(SegmentedDevice::single_file(
      dir.path().join("evict_race.db"),
    )?),
  )?);

  // 既有租户：非严格会话创建即持久化 DbMeta（磁盘权威），解绑归零登记即期期限
  let (vns0, vdb0) = {
    let session = store.new_session()?;
    session.set_context(7, 0);
    let ids = store.vdb.get_virtual_ids(7, 0);
    session.upsert(b"race:key", b"race:value").await?;
    ids
  };
  let next_id = store.vdb.next_virtual_id.load(Relaxed);

  // 留钩注入：冷检通过后、解析前的间隙内空闲析构摘除快照（refs 已归零必成功）
  let hook_store = Arc::clone(&store);
  *wkv::TEST_COLD_WINDOW_HOOK.lock() = Some(Box::new(move || {
    assert!(
      hook_store.vdb.evict_idle_route(vns0),
      "窗口内空闲析构应摘除"
    );
  }));
  let session = store.new_session()?;
  session.set_strict_context(true);
  assert!(
    !session.set_context(7, 0),
    "快照恰在冷检与解析之间被摘除必须回退异步解析，拒绝盲分配"
  );
  assert_eq!(
    store.vdb.next_virtual_id.load(Relaxed),
    next_id,
    "回退路径零盲分配"
  );
  assert!(
    store.vdb.db_routing.pin().get(&vns0).is_none(),
    "未发生空表回插盲分配"
  );

  // 协议层挂起面：点查装载既有映射不换号，重放物化成功、旧域数据原样可读
  let (vns, vdb) = store.resolve_context(7, 0).await?;
  assert_eq!((vns, vdb), (vns0, vdb0), "点查装载命中既有映射不换号");
  assert!(session.set_context(7, 0), "装载后重放必须物化成功");
  assert_eq!(
    session.read(b"race:key").await?.as_deref(),
    Some(b"race:value".as_slice()),
    "交错竞窗下旧域数据绝不丢失"
  );
  Ok(())
}

/// 死亡账本到期前缀：sweep 只回收「已到期且日志截断线已越界」的前缀，
/// 未到期与截断线未越界条目保留（扫描成本与到期前缀成正比，与账本总量无关）
#[compio::test]
async fn test_gc_dead_sweep_expired_prefix() -> Void {
  let dir = tempdir()?;
  let store = Arc::new(WedbStore::open(
    config()?,
    Arc::new(SegmentedDevice::single_file(
      dir.path().join("gc_dead_prefix.db"),
    )?),
  )?);
  let dead: &GcDeadLog = &store.vdb.gc_dead;
  let now = now_ticks();
  let entry = |expired_at: i64, tail: u64| GcDeadEntry {
    expired_at,
    tail_address: tail,
    vns: None,
  };
  // 注入三态：已到期可回收 / 未到期保留 / 已到期但截断线未越界保留
  dead.insert(9_001, entry(now - 1_000, 0));
  dead.insert(9_002, entry(now + 3_600_000, 0));
  dead.insert(9_003, entry(now - 1_000, u64::MAX));
  assert_eq!(dead.len(), 3);

  GcManager::new(&store).run_once().await?;

  assert!(dead.get(&9_001).is_none(), "到期且越界条目回收");
  assert!(
    dead.get(&9_002).is_some(),
    "未到期条目保留（小根堆到期前缀即停）"
  );
  assert!(
    dead.get(&9_003).is_some(),
    "截断线未越界条目保留（待紧缩推进后重弹）"
  );
  assert_eq!(dead.len(), 2);
  Ok(())
}

/// 根域 immortal：绑定/解绑与空闲析构绝不波及 (0, 0) 常驻快照
#[compio::test]
async fn test_root_domain_immortal() -> Void {
  let dir = tempdir()?;
  let store = Arc::new(WedbStore::open(
    idle_config()?,
    Arc::new(SegmentedDevice::single_file(
      dir.path().join("root_immortal.db"),
    )?),
  )?);
  assert!(!store.vdb.evict_idle_route(ROOT_VIRTUAL_ID), "根域拒释");
  assert!(store.vdb.db_routing.pin().get(&ROOT_VIRTUAL_ID).is_some());
  {
    let session = store.new_session()?;
    session.set_context(7, 0);
  }
  sleep(Duration::from_millis(5)).await;
  GcManager::new(&store).run_once().await?;
  assert!(
    store.vdb.db_routing.pin().get(&ROOT_VIRTUAL_ID).is_some(),
    "根域快照常驻不析构"
  );
  assert!(store.vdb.matches_logic_db(0, 0, 0), "根域路由完好");
  Ok(())
}
