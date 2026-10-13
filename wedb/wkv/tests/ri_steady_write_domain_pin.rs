#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! RI 稳态写臂链首域钉跨代撕裂回归锁（票
//! wkv-ri-steady-write-arm-generation-drift-cross-domain-ghost）
//!
//! 缺陷形：range_index_set / range_index_set_batch / range_index_del 为多
//! await 写链，链内逐点现解会话物理域（装载 → 锁内刷新 → 元记录回写 → AOF
//! 入账，del 臂另有排空面五取点）——FLUSHDB/FLUSHNS/SWAPDB 的 bump_generation
//! 与写臂零互斥（flush 仅持 lock_dbmeta），落窗中即撕裂：复合元记录落新代
//! 物理域成幽灵键（换号清库承诺破损，EXISTS=1 而树身份在旧代待延迟销毁）；
//! StoreEvent::RangeIndexWrite 事件域落新代而副本无此键（回放 NotFound 静默
//! 跳过）→ 主从发散；del 删空臂排空全取点被扳向新代脱靶 → 旧域元记录/树/
//! 旁表全漏清。已收口票 wkv-promote-ri-chain-mid-command-generation-tear
//! 射程仅 promote 与 RI.CREATE 发布长链，稳态写臂面未覆盖（本票补齐）。
//!
//! 修复契约（链首域钉 + 落盘前换代复核，对标 create/promote 已钉形态）：
//! 1. 三写臂链首单次解析 (代数基线, vns, vdb) 钉定三元组，链内锁内刷新
//!    （refresh_tiered_meta_with_prefix）、元记录回写
//!    （save_bftree_meta_stub_with_prefix）、排空面（drain 内核 pinned 直用）、
//!    AOF 事件域一律消费钉定值，禁逐点重解析；
//! 2. 落盘前换代复核：set/set_batch 树写前复核（此刻零树变零落盘直失败
//!    GenerationMoved）；del 实删已生效按 Swapped 分级（复用 promote 同款
//!    机制，先推进 WATCH 栅栏再上抛）；复核后的残余窗全链恒落钉定旧域，
//!    旧域随换号延迟销毁整体湮灭（tolerated 旧域泄漏口径，doc/zh/db.md 1.4）。
//! 3. 纯交改零行为差：无换代时链恒落同域，写读删终态与计数与既有契约一致。
//!
//! 注入体走 wkv 换号内核真原语（vdb.flush_db 换号段 → DbMeta 原子批入账，
//! 与 migrate_cross_generation_ttl.rs / promote_ri_create_domain_pin.rs
//! flushdb_kernel 同款，非 mock）；定序经 [`wkv::TEST_DOMAIN_PIN_HOOK`]
//! 一次性留钩（写臂取树条带独占写锁后、锁内刷新前触发——装载与树身份取点
//! 已收敛至钉定域，注入后刷新/复核/落盘的域一致性即被定向检验）。
//! 四场景置同一测试函数顺序执行：留钩槽系进程级单例，串行即免武装互踩。

// 留钩驱动测试（TEST_DOMAIN_PIN_HOOK 系 debug 专用门控符号，注入体
// flushdb_kernel 及其专属导入亦全为钩子服务）：release 随钩整文件剔除
// （wepoch/tests/epoch/shared_slot.rs 同款先例）
#![cfg(debug_assertions)]

#[path = "store_open.rs"]
mod store_open;

use std::sync::{Arc, atomic::Ordering};

use aok::{OK, Void};
use parking_lot::Mutex;
use store_open::{open_store_in, range_index_config};
use tempfile::tempdir;
use wbase::{convert::expire_after_to_ticks, time::now_ticks};
use wbftree::{StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::{
  DbMetaRecord, Error as WkvError, RangeIndexError, StoreEvent, StoreEventSink,
  TEST_DOMAIN_PIN_HOOK, WedbStore,
};
use wval::{KeyTag, NamespaceDbCodec};

/// set 场景键（逻辑 db 1）
const KEY_SET: &[u8] = b"swp:set";
/// set_batch 场景键（逻辑 db 1）
const KEY_BATCH: &[u8] = b"swp:batch";
/// del 场景键（逻辑 db 1）
const KEY_DEL: &[u8] = b"swp:del";
/// 纯回归场景键（逻辑 db 2）
const KEY_PURE: &[u8] = b"swp:pure";

/// 与 C# 测试一致的默认树调优（对标 tests/promote_ri_create_domain_pin.rs TUNE）
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 副本流域捕获条目 `(kind, ns, db, key, field)`：0 = RangeIndexWrite、
/// 1 = RangeIndexDrop
type EventLog = Mutex<Vec<(u8, u64, u64, Vec<u8>, Vec<u8>)>>;

/// FLUSHDB 内核真原语注入体（promote_ri_create_domain_pin.rs 同款：
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

/// 副本流域捕获 sink（只录 RI 写/删两族事件，其余直通）
fn ri_events_sink(log: Arc<EventLog>) -> StoreEventSink {
  fn capture(log: &EventLog, _ver: i64, _sid: i32, event: StoreEvent<'_>) -> wkv::Result<()> {
    match event {
      StoreEvent::RangeIndexWrite {
        ns,
        db,
        key,
        field,
        delete,
        ..
      } => {
        log
          .lock()
          .push((u8::from(delete), ns, db, key.to_vec(), field.to_vec()));
      }
      StoreEvent::RangeIndexDrop { ns, db, key } => {
        log.lock().push((2, ns, db, key.to_vec(), Vec::new()));
      }
      _ => {}
    }
    Ok(())
  }
  StoreEventSink::new(log, capture)
}

/// 换号后新代域解析（新会话走慢路径重解析，返回 (vns, new_vdb)）
fn resolve_new_domain(store: &Arc<WedbStore<SegmentedDevice>>, logic_db: u64) -> (u64, u64) {
  let s2 = store.new_session().expect("换代后重物化会话");
  assert!(s2.set_context(0, logic_db), "换代后上下文重物化");
  s2.virtual_domain()
}

/// 钉定域物理键构造（测试侧按编码内核直拼，与链首 session_tag_key_with_prefix
/// 字节恒等）
fn pinned_meta_key((vns, vdb): (u64, u64), key: &[u8]) -> wval::TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(vns, vdb, KeyTag::Meta, key)
}

/// 新代域零伪影三面断言：元记录缺席（清库承诺：新代无幽灵键）、树身份不
/// 在册（新代无幽灵树）、事件域不落新代（副本不收幽灵条目）
async fn assert_new_domain_clean(
  store: &Arc<WedbStore<SegmentedDevice>>,
  pinned: (u64, u64),
  logic_db: u64,
  key: &[u8],
  events: &EventLog,
) -> aok::Result<(u64, u64)> {
  let new_domain = resolve_new_domain(store, logic_db);
  assert_ne!(new_domain.1, pinned.1, "FLUSHDB 换号确已发生");
  {
    let probe = store.new_session()?;
    probe.set_virtual_context(new_domain.0, new_domain.1, 0, logic_db);
    assert!(
      probe.load_meta(key).await?.is_none(),
      "新代域禁幽灵元记录（撕裂形下 save 落新代即此处红）"
    );
  }
  assert!(
    store
      .range_index()
      .get_tree(&pinned_meta_key(new_domain, key))
      .is_none(),
    "新代域禁幽灵树注册"
  );
  assert!(
    !events
      .lock()
      .iter()
      .any(|&(_, n, d, ref k, _)| (n, d) == new_domain && k.as_slice() == key),
    "事件域禁落新代（副本收幽灵条目即主从发散入口）"
  );
  Ok(new_domain)
}

/// 场景一：FLUSHDB 落 set 链中 ⇒ 树写前复核直失败（GenerationMoved，
/// 零树变零落盘零 AOF）、新代域零伪影、旧域记录保持 flush 前原态（size=1，
/// 全链伪影恒落钉定旧域的 tolerated 基线）、旧代实况无 f2、重试按新域
/// 收敛（清库后该键不存在，重试答 NotFound）
async fn flushdb_mid_set_fails_closed() -> Void {
  let dir = tempdir()?;
  let store = open_store_in(
    &dir,
    "swp_set.db",
    range_index_config(&dir, 1024, 64 * 1024)?,
  )?;
  let s = store.new_session()?;
  assert!(s.set_context(0, 1), "db1 上下文物化");
  s.range_index_create(KEY_SET, StorageBackendType::Disk, TUNE)
    .await?;
  s.range_index_set(KEY_SET, b"field1", b"value1").await?;
  let pinned = s.virtual_domain();

  let log: Arc<EventLog> = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(ri_events_sink(Arc::clone(&log))));

  // 写臂取锁后、锁内刷新前注入 FLUSHDB（真原语，放大「换号落链中」竞态窗）
  let store2 = Arc::clone(&store);
  *TEST_DOMAIN_PIN_HOOK.lock() = Some(Box::new(move || flushdb_kernel(&store2, 1)));

  let err = s
    .range_index_set(KEY_SET, b"field2", b"value2")
    .await
    .expect_err("换代落链中须显式失败禁半代提交");
  assert!(
    err.to_string().contains("换号"),
    "错误面须为换代复核拒绝（GenerationMoved），实际 {err}"
  );
  assert!(
    !matches!(&err, RangeIndexError::Store(w) if matches!(w.as_ref(), WkvError::Swapped(_))),
    "set 零树变零落盘直失败，非 Swapped 分级，实际 {err:?}"
  );

  // 新代域零伪影（元记录 / 树身份 / 事件域三面）
  assert_new_domain_clean(&store, pinned, 1, KEY_SET, &log).await?;

  // 副本零 f2 写条目（复核先于 AOF 入账；f1 条目属建链期既有入账）
  assert!(
    !log
      .lock()
      .iter()
      .any(|&(k, .., ref f)| k == 0 && f == b"field2"),
    "复核失败臂先于 AOF 入账，禁半代 RangeIndexWrite 镜像"
  );

  // 旧域保持 flush 前原态：记录计数仍 1（save 未把 size=2 落任何域）、树内
  // 无 f2（树写前拦截零树变）——全链伪影恒落钉定旧域的 tolerated 基线
  {
    let probe = store.new_session()?;
    probe.set_virtual_context(pinned.0, pinned.1, 0, 1);
    assert_eq!(
      probe.load_meta(KEY_SET).await?.map(|m| m.size),
      Some(1),
      "旧域记录保持 flush 前原态（撕裂形下 size=2 落错域即此处红）"
    );
    assert!(
      !probe
        .range_index_get_with(KEY_SET, b"field2", |v| v.is_some())
        .await?,
      "旧域树禁 f2（树写前拦截零树变）"
    );
  }

  // 重试按新域收敛：清库后该键不存在，答 no such index
  let err = s
    .range_index_set(KEY_SET, b"field2", b"value2")
    .await
    .expect_err("换代后重试按新域收敛");
  assert!(matches!(err, RangeIndexError::NotFound), "实际 {err:?}");
  OK
}

/// 场景二：FLUSHDB 落 set_batch 链中 ⇒ 同型树写前复核直失败、批量零条目
/// 入账（主从零发散面）、新代域零伪影、旧域计数不变
async fn flushdb_mid_set_batch_fails_closed() -> Void {
  let dir = tempdir()?;
  let store = open_store_in(
    &dir,
    "swp_batch.db",
    range_index_config(&dir, 1024, 64 * 1024)?,
  )?;
  let s = store.new_session()?;
  assert!(s.set_context(0, 1), "db1 上下文物化");
  s.range_index_create(KEY_BATCH, StorageBackendType::Disk, TUNE)
    .await?;
  s.range_index_set(KEY_BATCH, b"field1", b"value1").await?;
  let pinned = s.virtual_domain();

  let log: Arc<EventLog> = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(ri_events_sink(Arc::clone(&log))));

  let store2 = Arc::clone(&store);
  *TEST_DOMAIN_PIN_HOOK.lock() = Some(Box::new(move || flushdb_kernel(&store2, 1)));

  let err = s
    .range_index_set_batch(KEY_BATCH, &[(b"field3", b"value3"), (b"field4", b"value4")])
    .await
    .expect_err("换代落链中须显式失败禁半代提交");
  assert!(
    err.to_string().contains("换号"),
    "错误面须为换代复核拒绝（GenerationMoved），实际 {err}"
  );

  assert_new_domain_clean(&store, pinned, 1, KEY_BATCH, &log).await?;

  assert!(
    !log
      .lock()
      .iter()
      .any(|&(k, .., ref f)| k == 0 && (f == b"field3" || f == b"field4")),
    "复核失败臂先于 AOF 入账，批量零条目镜像"
  );
  {
    let probe = store.new_session()?;
    probe.set_virtual_context(pinned.0, pinned.1, 0, 1);
    assert_eq!(
      probe.load_meta(KEY_BATCH).await?.map(|m| m.size),
      Some(1),
      "旧域记录保持 flush 前原态"
    );
  }
  OK
}

/// 场景三：FLUSHDB 落 del 链中 ⇒ 实删已生效按 Swapped 分级（对齐 promote
/// 换入后复核同款）、新代域零伪影、旧域记录保持 size=1（复核拦在回写前）、
/// 旧域树字段已实删（tolerated 无害幂等——旧域随换号延迟销毁整体湮灭）、
/// 副本零字段删条目零 Drop 条目（排空面未启动）
async fn flushdb_mid_del_swapped_grade() -> Void {
  let dir = tempdir()?;
  let store = open_store_in(
    &dir,
    "swp_del.db",
    range_index_config(&dir, 1024, 64 * 1024)?,
  )?;
  let s = store.new_session()?;
  assert!(s.set_context(0, 1), "db1 上下文物化");
  s.range_index_create(KEY_DEL, StorageBackendType::Disk, TUNE)
    .await?;
  s.range_index_set(KEY_DEL, b"field1", b"value1").await?;
  let pinned = s.virtual_domain();

  let log: Arc<EventLog> = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(ri_events_sink(Arc::clone(&log))));

  let store2 = Arc::clone(&store);
  *TEST_DOMAIN_PIN_HOOK.lock() = Some(Box::new(move || flushdb_kernel(&store2, 1)));

  let err = s
    .range_index_del(KEY_DEL, b"field1")
    .await
    .expect_err("换代落链中实删已生效须 Swapped 分级显式失败");
  let swapped_generation_moved = match &err {
    RangeIndexError::Store(w) => matches!(
      w.as_ref(),
      WkvError::Swapped(inner) if matches!(inner.as_ref(), WkvError::GenerationMoved)
    ),
    _ => false,
  };
  assert!(
    swapped_generation_moved,
    "del 复核失败面须为 Swapped(GenerationMoved) 分级，实际 {err:?}"
  );

  assert_new_domain_clean(&store, pinned, 1, KEY_DEL, &log).await?;

  // 旧域实况：字段已实删（树内容已变，Swapped 分级置脏真实反映）、元记录
  // 仍在且计数保持 1（复核拦在 size=0 基线回写前——回写落任何域均撕裂）
  {
    let probe = store.new_session()?;
    probe.set_virtual_context(pinned.0, pinned.1, 0, 1);
    assert!(
      !probe
        .range_index_get_with(KEY_DEL, b"field1", |v| v.is_some())
        .await?,
      "实删已生效（旧域树字段已消亡，tolerated 无害幂等）"
    );
    assert_eq!(
      probe.load_meta(KEY_DEL).await?.map(|m| m.size),
      Some(1),
      "复核拦在记录回写前（撕裂形下 size=0 基线落错域即此处红）"
    );
  }

  // 排空面未启动：副本零字段删条目、零整键 Drop 条目（排空被现解析扳向
  // 新代脱靶旧域漏清的缺陷形下，Drop 会落新代成幽灵条目）
  assert!(
    !log
      .lock()
      .iter()
      .any(|&(k, .., ref f)| k == 1 && f == b"field1"),
    "复核失败臂先于字段 AOF 入账"
  );
  assert!(
    !log
      .lock()
      .iter()
      .any(|&(k, .., ref k2)| k == 2 && k2 == KEY_DEL),
    "排空面未启动，零整键 Drop 镜像"
  );

  // 重试按新域收敛：清库后该键不存在
  let err = s
    .range_index_del(KEY_DEL, b"field1")
    .await
    .expect_err("换代后重试按新域收敛");
  assert!(matches!(err, RangeIndexError::NotFound), "实际 {err:?}");
  OK
}

/// 场景四：纯交改回归——无换代时三写臂恒落同域零行为差：set/set_batch
/// 计数增量正确、del 删空自愈照常（整键消亡 + 旁表注销 + Drop 入账）、
/// 事件域与记录落域同源
async fn pin_without_interleave_steady_write_unchanged() -> Void {
  let dir = tempdir()?;
  let store = open_store_in(
    &dir,
    "swp_pure.db",
    range_index_config(&dir, 1024, 64 * 1024)?,
  )?;
  let s = store.new_session()?;
  assert!(s.set_context(0, 2), "db2 上下文物化");
  let pinned = s.virtual_domain();

  let log: Arc<EventLog> = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(ri_events_sink(Arc::clone(&log))));

  s.range_index_create(KEY_PURE, StorageBackendType::Disk, TUNE)
    .await?;
  s.range_index_set(KEY_PURE, b"field1", b"value1").await?;
  let inserted = s
    .range_index_set_batch(KEY_PURE, &[(b"field2", b"value2"), (b"field3", b"value3")])
    .await?;
  assert_eq!(inserted, 2, "批量真实新增计数");
  assert_eq!(
    s.range_index_count(KEY_PURE).await?,
    3,
    "元记录计数为增量累积（O(1) RI.COUNT 契约）"
  );
  assert_eq!(
    s.range_index_get(KEY_PURE, b"field2").await?,
    Some(b"value2".to_vec()),
    "写读同域闭环"
  );

  // del 删空自愈：字段逐删至零即整键消亡（元记录墓碑 + 树注销 + Drop 入账）
  s.range_index_del(KEY_PURE, b"field1").await?;
  assert_eq!(s.range_index_count(KEY_PURE).await?, 2, "删后计数递减");
  s.range_index_del(KEY_PURE, b"field2").await?;
  s.range_index_del(KEY_PURE, b"field3").await?;
  assert!(
    matches!(
      s.range_index_count(KEY_PURE).await,
      Err(RangeIndexError::NotFound)
    ),
    "删空自愈整键消亡"
  );

  // 事件域与记录落域同源（全部落钉定域——撕裂形下事件域被扳向他域即红）
  let events = log.lock().clone();
  assert!(
    events
      .iter()
      .filter(|(k, ..)| *k <= 1)
      .all(|(_, n, d, ..)| (*n, *d) == pinned),
    "全部事件域须 = 会话域: {events:?}"
  );
  assert_eq!(
    events.iter().filter(|(k, ..)| *k == 0).count(),
    3,
    "字段写条目 3（f1 单点 + f2/f3 批量）"
  );
  assert_eq!(
    events.iter().filter(|(k, ..)| *k == 1).count(),
    3,
    "字段删条目 3"
  );
  assert_eq!(
    events.iter().filter(|(k, ..)| *k == 2).count(),
    1,
    "删空整键 Drop 条目 1"
  );
  OK
}

/// 四场景串行（留钩槽进程级单例，禁并行抢武装）
#[compio::test]
async fn domain_pin_locks_steady_write_arms_across_generation() -> Void {
  flushdb_mid_set_fails_closed().await?;
  flushdb_mid_set_batch_fails_closed().await?;
  flushdb_mid_del_swapped_grade().await?;
  pin_without_interleave_steady_write_unchanged().await?;
  OK
}
