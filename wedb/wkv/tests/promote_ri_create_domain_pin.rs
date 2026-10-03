//! 升阶 / RI.CREATE 链首域钉跨代撕裂回归锁（票 wkv-promote-ri-chain-mid-command-generation-tear）
//!
//! 缺陷形：promote_collection_to_bftree / range_index_create 为多 await 发布链，
//! 链内逐点 `virtual_domain` 现解析会话物理域——FLUSHDB/FLUSHNS/SWAPDB 的
//! bump_generation 落建树长窗中，下一取点即改指新代域，把树身份域、旁表登记域、
//! 元记录落域、副本流域撕裂：重启惰性回建按存根新代域现算树身份，文件不在该
//! 路径即空树静默清零（数据丢失）；旧域身份树文件无旁表登记无墓碑（孤儿永驻）；
//! 副本流载荷域停在 emit 时刻（主从发散）。
//!
//! 修复契约（链首域钉单点 + 落盘前换代复核，对标票面精炼执行方案）：
//! 1. 链首单次解析 (代数基线, vns, vdb) 钉定三元组，链内物理键构造经
//!    `session_tag_key_with_prefix` 显式前缀、register/detach 走三参内核直调，
//!    副本流载荷域 = 钉定域；
//! 2. 元记录落盘取点前复核全局代数：越过钉定基线即按既有 Swapped 分级失败臂
//!    逆序回滚（注销旁表 + delete_index 摘树 + compensate_stream_drop 显式域
//!    补偿），命令显式失败（[`wkv::Error::GenerationMoved`]）禁半代提交；
//! 3. 纯交改零行为差：无换代时链恒落同域，升阶结果与重启回建同帧。
//!
//! 注入体走 wkv 换号内核真原语（`vdb.flush_db` 换号段 → DbMeta 原子批入账，
//! 与 migrate_cross_generation_ttl.rs flushdb_kernel 同款，非 mock）；定序经
//! [`wkv::TEST_DOMAIN_PIN_HOOK`] 链首一次性留钩（钉定后、建树 await 前触发）。
//! 三场景置同一测试函数顺序执行：留钩槽系进程级单例，串行即免武装互踩。

// 留钩驱动测试（TEST_DOMAIN_PIN_HOOK 系 debug 专用门控符号，注入体
// flushdb_kernel 及其专属导入亦全为钩子服务）：release 随钩整文件剔除
// （wepoch/tests/epoch/shared_slot.rs 同款先例）
#![cfg(debug_assertions)]

#[path = "store_open.rs"]
mod store_open;

use std::{
  fs,
  fs::create_dir_all,
  path::Path,
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use aok::{OK, Void};
use compio::time::sleep;
use parking_lot::Mutex;
use store_open::{open_store_in, range_index_config};
use tempfile::tempdir;
use wbase::{convert::expire_after_to_ticks, time::now_ticks};
use wbftree::{ScanReturnField, StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::{
  CheckpointType, DbMetaRecord, Error as WkvError, RangeIndexError, StoreEvent, StoreEventSink,
  TEST_DOMAIN_PIN_HOOK, WedbStore,
};
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec};

/// 升阶场景键（逻辑 db 1）
const KEY_PROM: &[u8] = b"pin:prom";
/// RI.CREATE 场景键（逻辑 db 1）
const KEY_RI: &[u8] = b"pin:ri";
/// 纯交改回归场景键（逻辑 db 2）
const KEY_PURE: &[u8] = b"pin:pure";

/// 与 C# 测试一致的默认树调优（对标 tests/store/range_index.rs TUNE）
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 副本流域捕获条目 `(kind, ns, db, key)`：0 = RangeIndexStream、
/// 1 = RangeIndexDrop、2 = RangeIndexCreate
type EventLog = Mutex<Vec<(u8, u64, u64, Vec<u8>)>>;

/// FLUSHDB 内核真原语注入体（migrate_cross_generation_ttl.rs 同款：
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

/// 副本流域捕获 sink（只录 RI 三族事件，其余直通）
fn ri_events_sink(log: Arc<EventLog>) -> StoreEventSink {
  fn capture(log: &EventLog, _ver: i64, _sid: i32, event: StoreEvent<'_>) -> wkv::Result<()> {
    match event {
      StoreEvent::RangeIndexStream { ns, db, key, .. } => {
        log.lock().push((0, ns, db, key.to_vec()));
      }
      StoreEvent::RangeIndexDrop { ns, db, key } => {
        log.lock().push((1, ns, db, key.to_vec()));
      }
      StoreEvent::RangeIndexCreate { ns, db, key, .. } => {
        log.lock().push((2, ns, db, key.to_vec()));
      }
      _ => {}
    }
    Ok(())
  }
  StoreEventSink::new(log, capture)
}

/// 驱动释放消费并轮询等待树数据文件物理收敛（对标
/// promote_meta_save_failure_rollback::wait_file_gone 同款判据）
async fn wait_file_gone(path: &Path, store: &Arc<WedbStore<SegmentedDevice>>) -> Void {
  for _ in 0..2500 {
    store.drain_bftree_release(usize::MAX);
    if !path.exists() {
      return OK;
    }
    drop(store.new_session()?);
    sleep(Duration::from_millis(2)).await;
  }
  panic!("回滚删除未收敛，磁盘孤儿数据文件残留: {}", path.display());
}

/// 旁表在册判据（snapshot_bftree_domains 只读快照，登记面零影响）
fn side_table_holds(store: &WedbStore<SegmentedDevice>, key: &[u8]) -> bool {
  store
    .snapshot_bftree_domains()
    .into_iter()
    .any(|(_, _, keys)| keys.iter().any(|k| k.as_ref() == key))
}

/// 钉定域物理键构造（测试侧按编码内核直拼，与链首 session_tag_key_with_prefix
/// 字节恒等）
fn pinned_meta_key((vns, vdb): (u64, u64), key: &[u8]) -> wval::TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(vns, vdb, KeyTag::Meta, key)
}

/// 场景一：FLUSHDB 落升阶链中 ⇒ 命令显式失败（Swapped 分级 GenerationMoved）、
/// 三撕裂面零残留（树注销 / 文件回收 / 旁表零孤儿 / 钉定域零半代元记录）、
/// 副本流域 emit 与补偿 Drop 同落钉定域（副本终态与主等）；换代确已发生且重试
/// 按新域收敛
async fn flushdb_mid_promote_fails_closed() -> Void {
  let dir = tempdir()?;
  let store = open_store_in(
    &dir,
    "pin_prom.db",
    range_index_config(&dir, 1024, 64 * 1024)?,
  )?;
  let s = store.new_session()?;
  assert!(s.set_context(0, 1), "db1 上下文物化");
  let env_k = s.session_tag_key(KeyTag::ObjectEnvelope, KEY_PROM);
  s.upsert_tag(KEY_PROM, KeyTag::ObjectEnvelope, b"\x03env-snapshot")
    .await?;

  // 钉定基线域（断言基准）：链首解析的 (vns, vdb)
  let pinned = s.virtual_domain();
  let pinned_id = pinned_meta_key(pinned, KEY_PROM);
  let data_path = store.range_index().data_file_path_for_key(&pinned_id);

  // 副本流域捕获（先于升阶命令装配）
  let log: Arc<EventLog> = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(ri_events_sink(Arc::clone(&log))));

  // 链首域钉后立即注入 FLUSHDB（真原语，放大「换号落链中」竞态窗）
  let store2 = Arc::clone(&store);
  *TEST_DOMAIN_PIN_HOOK.lock() = Some(Box::new(move || flushdb_kernel(&store2, 1)));

  let err = s
    .promote_collection_to_bftree(
      KEY_PROM,
      GarnetObjectType::Hash,
      vec![(b"f1".to_vec(), b"v1".to_vec())],
      i64::MAX,
      false,
    )
    .await
    .expect_err("换代落链中须显式失败禁半代提交");
  assert!(
    matches!(err, WkvError::Swapped(_)),
    "换入已生效失败保持 Swapped 分级，实际 {err:?}"
  );
  assert!(
    err.to_string().contains("换号"),
    "错误面须为换代复核拒绝（GenerationMoved），实际 {err}"
  );

  // ── 三撕裂面收口：全链伪影恒落钉定域且零残留（孤儿永驻即本段断言红）──
  assert!(
    store.range_index().get_tree(&pinned_id).is_none(),
    "钉定域树身份不得残留注册表"
  );
  wait_file_gone(&data_path, &store).await?;
  assert!(
    !side_table_holds(&store, KEY_PROM),
    "旁表零孤儿（残留即换号取空后又回写死域条目）"
  );
  {
    // 钉定域直设探针：禁半代元记录（撕裂形下 meta 落新代、旧代存根悬空）
    let probe = store.new_session()?;
    probe.set_virtual_context(pinned.0, pinned.1, 0, 1);
    assert!(
      probe.load_meta(KEY_PROM).await?.is_none(),
      "钉定旧域禁半代元记录"
    );
  }

  // ── 副本终态与主等：流域 emit 与补偿 Drop 同落钉定域（补偿被重解析扳向新域
  //    落空、副本旧域幻影残留即本断言红）──
  let events = log.lock().clone();
  assert!(
    events
      .iter()
      .any(|&(k, n, d, ref kk)| k == 0 && (n, d) == pinned && kk == KEY_PROM),
    "RangeIndexStream 载荷域须 = 钉定域: {events:?}"
  );
  assert!(
    events
      .iter()
      .any(|&(k, n, d, ref kk)| k == 1 && (n, d) == pinned && kk == KEY_PROM),
    "补偿 RangeIndexDrop 须对准钉定发布域: {events:?}"
  );

  // ── 换代确已发生且对重试可见：新会话重解析落新域，重试升阶收敛 ──
  assert!(
    s.contains_key_raw(&env_k).await?,
    "回滚后键保持纯信封态（信封删除臂未达）"
  );
  let (_, new_vdb) = {
    let s2 = store.new_session()?;
    assert!(s2.set_context(0, 1), "换代后重物化");
    s2.virtual_domain()
  };
  assert_ne!(new_vdb, pinned.1, "FLUSHDB 换号确已发生");
  s.promote_collection_to_bftree(
    KEY_PROM,
    GarnetObjectType::Hash,
    vec![(b"f1".to_vec(), b"v1".to_vec())],
    i64::MAX,
    false,
  )
  .await
  .expect("换代后重试按新域收敛");
  let new_id = pinned_meta_key((pinned.0, new_vdb), KEY_PROM);
  assert!(
    store.range_index().get_tree(&new_id).is_some(),
    "重试升阶新树落新代域在册"
  );
  assert_eq!(
    s.load_meta(KEY_PROM).await?.map(|m| m.size),
    Some(1),
    "重试收尾元记录落新代域"
  );
  OK
}

/// 场景二：FLUSHDB 落 RI.CREATE 链中 ⇒ 同型显式失败（GenerationMoved）、
/// 刚建树回滚零孤儿、旁表零残留、AOF 零 RangeIndexCreate 入账（禁半代提交）
async fn flushdb_mid_ri_create_fails_closed() -> Void {
  let dir = tempdir()?;
  let store = open_store_in(
    &dir,
    "pin_ri.db",
    range_index_config(&dir, 1024, 64 * 1024)?,
  )?;
  let s = store.new_session()?;
  assert!(s.set_context(0, 1), "db1 上下文物化");
  let pinned = s.virtual_domain();
  let pinned_id = pinned_meta_key(pinned, KEY_RI);
  let data_path = store.range_index().data_file_path_for_key(&pinned_id);

  let log: Arc<EventLog> = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(ri_events_sink(Arc::clone(&log))));

  let store2 = Arc::clone(&store);
  *TEST_DOMAIN_PIN_HOOK.lock() = Some(Box::new(move || flushdb_kernel(&store2, 1)));

  let err: RangeIndexError = s
    .range_index_create(KEY_RI, StorageBackendType::Disk, TUNE)
    .await
    .expect_err("换代落链中须显式失败禁半代提交");
  assert!(
    err.to_string().contains("换号"),
    "错误面须为换代复核拒绝（GenerationMoved），实际 {err}"
  );

  assert!(
    store.range_index().get_tree(&pinned_id).is_none(),
    "钉定域树身份不得残留注册表（登记死亡域守卫 / 复核臂双重摘除）"
  );
  wait_file_gone(&data_path, &store).await?;
  assert!(
    !side_table_holds(&store, KEY_RI),
    "旁表零孤儿（撕裂形下登记落新代域即死域条目永驻）"
  );
  {
    let probe = store.new_session()?;
    probe.set_virtual_context(pinned.0, pinned.1, 0, 1);
    assert!(
      probe.load_meta(KEY_RI).await?.is_none(),
      "钉定旧域禁半代元记录"
    );
  }
  assert!(
    !log.lock().iter().any(|&(k, ..)| k == 2),
    "复核失败臂先于 AOF 入账，禁半代 RangeIndexCreate 镜像"
  );
  OK
}

/// 场景三：纯交改回归——无换代时链恒落同域零行为差，重启后元记录与树文件同域
/// 在位、惰性激活回建树内容非空（撕裂形下空树静默清零即本断言红）。
/// 重启面走引擎唯一的内容恢复入口：`create_checkpoint` 提交 +
/// [`WedbStore::recover`] 恢复（引擎无「裸开设备即续读日志」形态，日志地址窗口
/// 只能由检查点快照还原，对标 tests/store/dbmeta_layout.rs 同款先例）
async fn pin_without_interleave_restart_rebuild_non_empty() -> Void {
  let dir = tempdir()?;
  let cpr_dir = dir.path().join("checkpoints");
  create_dir_all(&cpr_dir)?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("pin_pure.db"),
  )?);
  let pinned;
  let token;
  {
    // 外持设备形态：检查点恢复段复用同一设备句柄重开（引擎无裸开设备续读
    // 形态，配置面与 open_store_in 收口同源）
    let store = Arc::new(WedbStore::open(
      range_index_config(&dir, 1024, 64 * 1024)?,
      Arc::clone(&device),
    )?);
    let s = store.new_session()?;
    assert!(s.set_context(0, 2), "db2 上下文物化");
    s.upsert_tag(KEY_PURE, KeyTag::ObjectEnvelope, b"\x04env-snapshot")
      .await?;
    s.promote_collection_to_bftree(
      KEY_PURE,
      GarnetObjectType::Hash,
      vec![
        (b"k1".to_vec(), b"v1".to_vec()),
        (b"k2".to_vec(), b"v2".to_vec()),
      ],
      i64::MAX,
      false,
    )
    .await
    .expect("无换代升阶照常成功");
    assert_eq!(
      s.load_meta(KEY_PURE).await?.map(|m| m.size),
      Some(2),
      "元记录计数为灌入批"
    );
    assert!(side_table_holds(&store, KEY_PURE), "升阶成功即登记换号旁表");
    pinned = s.virtual_domain();
    // 检查点提交：恢复扫描面须覆盖到元记录与 DbMeta 映射
    token = store
      .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
      .await?
      .token;
  }

  // 重启：检查点恢复（映射自磁盘 DbMeta 重建、range_index_dir 随 StoreMeta
  // 还原），元记录与树文件同钉定域在位
  let store = Arc::new(WedbStore::recover(&cpr_dir, token, device).await?);
  let s = store.new_session()?;
  assert!(s.set_context(0, 2), "重启后 db2 上下文物化");
  assert_eq!(s.virtual_domain(), pinned, "恢复须命中磁盘既有映射原域");
  let id = pinned_meta_key(pinned, KEY_PURE);
  let data_path = store.range_index().data_file_path_for_key(&id);
  assert!(data_path.exists(), "重启后树文件在位（钉定域路径）");
  assert!(
    fs::metadata(&data_path)?.len() > 0,
    "树文件非空（集合内容唯一持久副本）"
  );

  // 惰性激活回建非空树：分层集合面装载存根（升阶元记录 collection_type =
  // Hash，走 RI 面即 WrongType）→ acquire_tree_read 激活 → wbftree 扫描原语
  // 回流灌入批全量字段
  let (meta, mut stub) = s
    .load_collection_stub(KEY_PURE)
    .await?
    .expect("恢复后分层集合存根必在");
  assert_eq!(meta.size, 2, "恢复面元记录计数为灌入批");
  let tree = s
    .acquire_tree_read(KEY_PURE, &mut stub, None)
    .await
    .expect("恢复后首访须惰性开树回建");
  let mut fields = Vec::new();
  let n = tree
    .scan_with_count_callback(&[0], usize::MAX, ScanReturnField::Key, |k, _v| {
      fields.push(k.to_vec());
      true
    })
    .expect("回建树扫描成功");
  assert_eq!((n, fields.len()), (2, 2), "回建树计数为灌入批");
  assert!(
    fields.contains(&b"k1".to_vec()) && fields.contains(&b"k2".to_vec()),
    "回建树内容非空且与灌入批全等: {fields:?}"
  );
  OK
}

/// 三场景串行（留钩槽进程级单例，禁并行抢武装）
#[compio::test]
async fn domain_pin_locks_promote_chain_across_generation() -> Void {
  flushdb_mid_promote_fails_closed().await?;
  flushdb_mid_ri_create_fails_closed().await?;
  pin_without_interleave_restart_rebuild_non_empty().await?;
  OK
}
