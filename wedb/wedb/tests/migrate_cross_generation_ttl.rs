//! 迁移/复制驱动链跨代 TTL 幽灵回归锁（工单 wedb-migrate-cross-generation-ttl-ghost）
//!
//! 缺陷形：read_live_value 值读与 TTL 读系两独立 await，换号族（FLUSHDB/SWAPDB
//! 的 bump_generation）落两读之间时，逐 op 现解析的会话前缀把 TTL 读改指新代域
//! ——FLUSHDB 形拼出「值在旧代、TTL 归零」永久 0-TTL 幽灵帧（目标端回填门
//! >0 跳过，键永不消亡）；SWAPDB 形拼出邻库同名键真 TTL 串值帧（提前过期/
//! > 超期驻留双向）。主案修=单探针窗：每键探针头单次捕获 `session_prefix()`，
//! > 值读与 TTL 读并读该前缀（deviations §96 既定加固路落地，扩形登记 §118）。
//!
//! C# 对照面（票面测试验证点 5 skip 注记）：
//! libs/cluster/Server/Migration/MigrateScanFunctions.cs:Reader 整记录原子读
//! （expiration 随 RecordDataHeader 同读），结构上无两读窗，无可锁对形——本档
//! 为 rust 自研「换号教义 × 两 op 拼接」交叠面的内部契约锁。
//!
//! 锁面对应票面四条：
//! 1. `flushdb_between_reads_zero_ttl_ghost_locked_and_fixed`——FLUSHDB 形换代
//!    注入锁：修复前两独立 await 现形坐实（值读旧域真值、TTL 读落新空域归零，
//!    「值在而 TTL 归零」拼接形复现）；修复后 string/信封两臂帧 TTL 恒 = 值读
//!    同代真值（禁拼接形），换号于窗口边界照常生效（下一探针整键 Gone 跳发
//!    留痕），目标端经帧导入落真 TTL 与源端逻辑终态全等（非永久幽灵）；
//! 2. `swapdb_between_reads_never_borrows_swapped_in_domain_ttl`——SWAPDB 双库
//!    同名异 TTL 形锁：修复前邻库真 TTL 串值现形坐实、修复后帧 TTL 恒取自值读
//!    同域、邻库串值不可达（补源档自陈未做夹具之欠账）；
//! 3. `generation_bumps_within_probe_are_invisible`——前缀捕获单点原子性锁：
//!    探针窗内换代计数不跨探针生效（基线读与窗内双换代读全等、窗后重解析读
//!    亦全等——钉的是单点捕获、非整会话固着）；
//! 4. 零回归——既有族 `diskless_sync_ttl.rs::diskless_sync_preserves_ttl`
//!    （§96 宗一既有锁）本票不动，窄范围复跑取证。
//!
//! 注入体走 wkv 换号内核同款真原语（路由格成对换指 / bump_generation /
//! flush_db 换号段 / DbMetaRecord 原子批入账，与 §99 快照域钉锁夹具同源），
//! 非 mock；定序经 live_value.rs 探针读窗留钩（[`wkv::TEST_COLD_WINDOW_HOOK`]
//! 同族一次性回调，值读物化后、TTL 读前触发）。

use std::sync::{Arc, atomic::Ordering};

use async_lock::Mutex;
use parking_lot::Mutex as PlMutex;
use wbase::{
  convert::{
    expire_after_to_ticks, expire_at_milliseconds_to_ticks, unix_time_in_milliseconds_from_ticks,
  },
  time::now_ticks,
};
use wconn::record::{MigrateVal, MigrationFrame, MigrationRecord};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  migration::{
    chunk_reassembler::ChunkReassembler,
    frame_import::{FrameImport, import_migration_frames},
    migrate_driver::{LiveValue, TEST_LIVE_VALUE_READ_HOOK, read_live_value},
  },
};
use wkv::{DbMetaRecord, WedbStore};
use wnode::StorageSession;
use wtest_base::test_store_config;
use wval::KeyTag;

/// 留钩静态跨用例串行门（钩槽系进程级单例，用例并行会互相抢占武装，全程
/// 持锁串行；§99 快照域钉锁夹具同款纪律）
static CASE_GUARD: Mutex<()> = Mutex::new(());

const GHOST_STR: &[u8] = b"ghost:str";
const V_STR: &[u8] = b"ghost-value";
const GHOST_ENV: &[u8] = b"ghost:env";
/// 信封整值 = [内层标签][载荷]，标签 0x03 = Hash（可迁移类型）
const V_ENV: &[u8] = b"\x03env-payload";
const GHOST_SW: &[u8] = b"ghost:sw";
const V_SW_DB1: &[u8] = b"from-db1";
const V_SW_DB2: &[u8] = b"from-db2";
const GHOST_BUMP: &[u8] = b"ghost:bump";
const V_BUMP: &[u8] = b"bump-value";

/// 打开小预算测试存储（GC 关闭，与 diskless 族测试同款配置）
fn open_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  Arc::new(WedbStore::open(test_store_config(), device).unwrap())
}

/// 统一测试 TTL 毫秒（未来 60s，整毫秒格，帧值与落库 ticks 逐毫秒可全等）
fn ttl_ms() -> i64 {
  unix_time_in_milliseconds_from_ticks(now_ticks()) + 60_000
}

/// 逻辑库内写 string 键 + TTL ticks
async fn seed_string_ttl(
  store: &Arc<WedbStore<SegmentedDevice>>,
  db: u64,
  key: &[u8],
  val: &[u8],
  expire_ticks: i64,
) {
  let session = store.new_session().unwrap();
  assert!(session.set_context(0, db), "db{db} 上下文物化");
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage.upsert_string(key, val).await.unwrap();
  storage.expire_at_ticks(key, expire_ticks).await.unwrap();
}

/// 逻辑库内写可迁移信封键 + TTL ticks
async fn seed_envelope_ttl(
  store: &Arc<WedbStore<SegmentedDevice>>,
  db: u64,
  key: &[u8],
  env: &[u8],
  expire_ticks: i64,
) {
  let session = store.new_session().unwrap();
  assert!(session.set_context(0, db), "db{db} 上下文物化");
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage
    .upsert_tag(key, KeyTag::ObjectEnvelope, env)
    .await
    .unwrap();
  storage.expire_at_ticks(key, expire_ticks).await.unwrap();
}

/// FLUSHDB 内核真原语注入体（与 §99 快照域钉锁夹具 flushdb 注入同款：
/// vdb.flush_db 换号段 → [新映射, 旧域墓碑, 0x05 分配水位] 原子批入账）
fn flushdb_kernel(store: &Arc<WedbStore<SegmentedDevice>>, logic_db: u64) {
  let expired_at =
    expire_after_to_ticks(now_ticks(), store.config.gc.db_gc_reclaim_delay_secs as i64);
  let tail_address = store.tail_address();
  let (vns, new_vdb, old_vdb_opt) = store.vdb.flush_db(0, logic_db, expired_at, tail_address);
  let old_vdb = old_vdb_opt.expect("既有映射换出旧号");
  let session = store.new_session().expect("注入体解析会话");
  let degraded = session
    .try_persist_dbmeta_sync(&[
      Some(DbMetaRecord::DbMap {
        vns,
        logic_db,
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

/// SWAPDB 1 2 内核真原语注入体（与 §99 夹具同款：路由格成对换指 →
/// bump_generation → DbSwap 成对记录 + 双映射原子批入账）
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

/// 单键探针读帧（Migratable 判型折叠，异型即判负）
async fn read_frame(
  store: &Arc<WedbStore<SegmentedDevice>>,
  db: u64,
  key: &[u8],
) -> (MigrateVal, i64) {
  let session = store.new_session().unwrap();
  assert!(session.set_context(0, db), "db{db} 上下文物化");
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  match read_live_value(&storage, None, key).await.unwrap() {
    LiveValue::Migratable(val, expire) => (val, expire),
    LiveValue::Gone => panic!(
      "键 {} 应可迁移读帧，实得 Gone",
      String::from_utf8_lossy(key)
    ),
    LiveValue::TieredTree => panic!("键 {} 误判 TieredTree", String::from_utf8_lossy(key)),
    LiveValue::VectorSet => panic!("键 {} 误判向量集", String::from_utf8_lossy(key)),
    LiveValue::Unsupported(label) => panic!(
      "键 {} 误判不支持迁移: {label}",
      String::from_utf8_lossy(key)
    ),
  }
}

/// 留钩消费自锁：钩未被消费即定序失效（武装泄漏入下一用例形态，显式判死）
fn assert_hook_consumed() {
  assert!(
    TEST_LIVE_VALUE_READ_HOOK.lock().is_none(),
    "留钩须被探针值读与 TTL 读之间定序消费"
  );
}

/// 武装探针读窗留钩（一次性回调，生产零负担面见 live_value.rs 定义注）
fn arm_hook(hook: impl FnOnce() + Send + 'static) {
  *TEST_LIVE_VALUE_READ_HOOK.lock() = Some(Box::new(hook));
}

/// 票面验证点 1：FLUSHDB 形换代注入锁（修复前现形坐实 + 修复后同代断言 +
/// 目标端导入按真 TTL 消亡）
#[compio::test]
async fn flushdb_between_reads_zero_ttl_ghost_locked_and_fixed() {
  let _serial = CASE_GUARD.lock().await;

  // ===== 修复前形态现形坐实：两独立 await 之间换代 → TTL 读落新空域归零
  let neg = open_store("mcttl_flush_neg");
  seed_string_ttl(
    &neg,
    1,
    GHOST_STR,
    V_STR,
    expire_at_milliseconds_to_ticks(ttl_ms()),
  )
  .await;
  let old_vdb1 = neg.vdb.get_virtual_ids(0, 1).1;
  {
    let session = neg.new_session().unwrap();
    assert!(session.set_context(0, 1));
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    // op 1：值读落旧域真值（旧代码 read_string 与本夹具逐 op 现解析同形）
    let val = storage.read_string(GHOST_STR).await.unwrap();
    assert_eq!(val.as_deref(), Some(V_STR), "值读落旧域真值");
    // 换代落两读之间（FLUSHDB db1）
    flushdb_kernel(&neg, 1);
    // op 2：TTL 读无前缀捕获即逐 op 重解析 → 落新空域恒 None → 帧值 0
    let ttl = storage.batch.ttl_of(GHOST_STR).await.unwrap();
    assert_eq!(
      ttl, None,
      "修复前现形：TTL 读被换代扳向新空域（拼接即发 0-TTL 幽灵帧）"
    );
    // 拼接形「值在而 TTL 归零」的值半边与真 TTL 半边俱在旧域坐实
    let probe = neg.new_session().unwrap();
    probe.set_virtual_context(0, old_vdb1, 0, 1);
    let pb = probe.enter_batch();
    let ps = StorageSession::new_readonly(pb);
    assert_eq!(
      ps.read_string(GHOST_STR).await.unwrap().as_deref(),
      Some(V_STR),
      "旧域值仍在（幽灵帧的值半边）"
    );
    assert!(
      ps.batch.ttl_of(GHOST_STR).await.unwrap().is_some(),
      "旧域真 TTL 仍在（修复后单探针窗应读出该值）"
    );
  }

  // ===== 修复后：string 臂——单探针窗内 FLUSHDB 换代无感，帧 TTL = 同代真值
  let fix = open_store("mcttl_flush_fix");
  let expire_ms = ttl_ms();
  let expire_ticks = expire_at_milliseconds_to_ticks(expire_ms);
  seed_string_ttl(&fix, 1, GHOST_STR, V_STR, expire_ticks).await;
  seed_envelope_ttl(&fix, 2, GHOST_ENV, V_ENV, expire_ticks).await;
  let frame_expire;
  {
    let store = Arc::clone(&fix);
    arm_hook(move || flushdb_kernel(&store, 1));
    let (val, expire) = read_frame(&fix, 1, GHOST_STR).await;
    frame_expire = expire;
    assert_hook_consumed();
    assert!(matches!(val, MigrateVal::Str(_)), "string 域读出 string 值");
    assert_eq!(
      frame_expire, expire_ms,
      "禁「值在而 TTL 归零」拼接形：帧 TTL 须为值读同代真值"
    );
    // 换号于窗口边界照常生效：新探针（新会话重解析）落新空域整键 Gone 跳发留痕
    let session = fix.new_session().unwrap();
    assert!(session.set_context(0, 1));
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    assert!(
      matches!(
        read_live_value(&storage, None, GHOST_STR).await.unwrap(),
        LiveValue::Gone
      ),
      "换号对下一探针可见（旧代键不随新探针读出）"
    );
  }

  // ===== 修复后：信封臂——两读之间 FLUSHDB db2，帧 TTL 同为同代真值
  {
    let store = Arc::clone(&fix);
    arm_hook(move || flushdb_kernel(&store, 2));
    let (val, frame_expire) = read_frame(&fix, 2, GHOST_ENV).await;
    assert_hook_consumed();
    match val {
      MigrateVal::Env(env) => assert_eq!(env, V_ENV, "信封整值原样随帧"),
      _ => panic!("信封域应读出 Env 值"),
    }
    assert_eq!(frame_expire, expire_ms, "信封臂同样禁拼接形");
  }

  // ===== 目标端导入锁：同代帧经 frame_import 落真 TTL，键按真 TTL 消亡
  //（帧值 0 形会触发 :333 回填 >0 门跳过 → 永久 0-TTL 幽灵，断言即红）
  let target = open_store("mcttl_flush_target");
  let provider = ClusterProvider::new();
  provider.set_store(Arc::clone(&target));
  {
    let session = target.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    let chunks = PlMutex::new(ChunkReassembler::new());
    let ri = None;
    let import = FrameImport {
      provider: &provider,
      session: &session,
      storage: &storage,
      chunks: &chunks,
      ri: &ri,
      replace: true,
      vector_slot: 0,
      accept_domain_frames: false,
    };
    import_migration_frames(
      vec![MigrationFrame::Record(MigrationRecord::Str {
        key: GHOST_STR,
        val: V_STR,
        expire_unix_ms: frame_expire,
      })],
      &import,
    )
    .await
    .expect("同代帧导入应成功");
  }
  let back = read_frame(&target, 0, GHOST_STR).await;
  assert!(matches!(back.0, MigrateVal::Str(_)));
  assert_eq!(
    back.1, expire_ms,
    "目标端键携真 TTL、按真 TTL 消亡——与源端同代读出逻辑终态全等（非永久幽灵）"
  );
}

/// 票面验证点 2：SWAPDB 双库同名异 TTL 形锁（补源档自陈未做夹具之欠账）
#[compio::test]
async fn swapdb_between_reads_never_borrows_swapped_in_domain_ttl() {
  let _serial = CASE_GUARD.lock().await;
  let store = open_store("mcttl_swap");
  let t1_ms = ttl_ms();
  let t2_ms = t1_ms + 120_000;
  let t1_ticks = expire_at_milliseconds_to_ticks(t1_ms);
  let t2_ticks = expire_at_milliseconds_to_ticks(t2_ms);

  // ===== 修复前形态现形坐实：值读落 db1 域，TTL 读被换入域扳向邻库真 TTL
  seed_string_ttl(&store, 1, GHOST_SW, V_SW_DB1, t1_ticks).await;
  seed_string_ttl(&store, 2, GHOST_SW, V_SW_DB2, t2_ticks).await;
  {
    let session = store.new_session().unwrap();
    assert!(session.set_context(0, 1));
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    let val = storage.read_string(GHOST_SW).await.unwrap();
    assert_eq!(val.as_deref(), Some(V_SW_DB1), "值读落 db1 现势域");
    swap12_kernel(&store);
    let ttl = storage.batch.ttl_of(GHOST_SW).await.unwrap();
    assert_eq!(
      ttl,
      Some(t2_ticks),
      "修复前现形：TTL 读落换入域，邻库真 TTL 串值拼进帧（非零、任何重放不触及）"
    );
  }

  // ===== 修复后：两读之间注 SWAPDB，帧 TTL 恒取自值读同域、邻库串值不可达
  //（换后态下重播同名异 TTL 夹具：当前 db1=t3、当前 db2=t4）
  let t3_ms = t1_ms + 300_000;
  let t4_ms = t1_ms + 480_000;
  seed_string_ttl(
    &store,
    1,
    GHOST_SW,
    V_SW_DB1,
    expire_at_milliseconds_to_ticks(t3_ms),
  )
  .await;
  seed_string_ttl(
    &store,
    2,
    GHOST_SW,
    V_SW_DB2,
    expire_at_milliseconds_to_ticks(t4_ms),
  )
  .await;
  {
    let store_c = Arc::clone(&store);
    arm_hook(move || swap12_kernel(&store_c));
    let (val, expire) = read_frame(&store, 1, GHOST_SW).await;
    assert_hook_consumed();
    match val {
      MigrateVal::Str(v) => assert_eq!(v, V_SW_DB1, "值读落探针捕获域（db1 现势）"),
      _ => panic!("string 臂应读出 Str 值"),
    }
    assert_eq!(expire, t3_ms, "帧 TTL 恒取自值读同域");
    assert_ne!(expire, t4_ms, "邻库真 TTL 串值不可达");
  }
}

/// 票面验证点 3：前缀捕获单点原子性锁——探针窗内换代计数不跨探针生效
#[compio::test]
async fn generation_bumps_within_probe_are_invisible() {
  let _serial = CASE_GUARD.lock().await;
  let store = open_store("mcttl_bump");
  seed_string_ttl(
    &store,
    1,
    GHOST_BUMP,
    V_BUMP,
    expire_at_milliseconds_to_ticks(ttl_ms()),
  )
  .await;
  // 基线读（无换代介入）
  let baseline = read_frame(&store, 1, GHOST_BUMP).await;
  let gen_before = store.vdb.generation.load(Ordering::Acquire);
  // 探针窗内双纯换代（仅代数推进、映射不变）：值与 TTL 并读捕获前缀，全等基线
  {
    let store_c = Arc::clone(&store);
    arm_hook(move || {
      store_c.vdb.bump_generation();
      store_c.vdb.bump_generation();
    });
    let bumped = read_frame(&store, 1, GHOST_BUMP).await;
    assert_hook_consumed();
    assert_eq!(
      bumped, baseline,
      "换代计数不跨探针生效（单点捕获即换代无感）"
    );
  }
  // 钉的是单点捕获、非整会话固着：代数确已推进，窗后新探针重解析读亦全等
  assert!(
    store.vdb.generation.load(Ordering::Acquire) >= gen_before + 2,
    "换代确已发生"
  );
  assert_eq!(read_frame(&store, 1, GHOST_BUMP).await, baseline);
}
