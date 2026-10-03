//! RI.CREATE 建树窗跨型并发写穿透预检的落盘前复查收口回归（票
//! wkv-ri-create-cross-type-precheck-toctou-dual-state-coexist）
//!
//! 缺陷形：range_index_create 三域存在性预检（String 域 read / Meta 域
//! load_meta / 信封域 contains_key_raw）全部先于 create_bftree 的
//! spawn_blocking 长 await（含冷树回收重试重 I/O 窗），预检与提交（register
//! + save_bftree_meta_stub）之间零互斥零复查——并发 SET k v（字符串写路径
//!   不拒无 meta 键）或 HSET 族（信封物化）在窗内提交即双穿预检，终态为同一
//!   逻辑键 String/信封记录与 RI 元记录跨物理域并存：GET 答字符串值而
//!   TYPE/RI 族路由 Meta 域报 RangeIndex，双态应答自相矛盾；已 ACK 索引被
//!   后续字符串写静默销毁。C# RangeIndexOps.cs:RangeIndexCreate 经 RMW 落
//!   存储层，预检与提交同一记录锁窗（TsavoriteKV 记录 X 锁），跨型并发只有
//!   一方胜出，双态共存结构不可达。
//!
//! 修复契约（票面精炼执行方案 + 审核硬性订正）：save_bftree_meta_stub 落盘
//! 前最近处（紧随换代复核后）以钉定前缀重跑三域存在性预检（read_raw /
//! contains_key_raw 裸读消费钉定域，禁现解域），任一域新增存活记录即按既有
//! WrongType 臂显式失败并走既有 unregister + delete_index 回滚，不新造清理
//! 路径、不引入每键创建锁（跨条带锁序风险）。「复查后至落盘前」残余窗仍存：
//! 后至跨型写由后续 SET 族覆写清退内嵌 Meta 域收敛单态——本文件按收敛终态
//! 断言，不宣称全窗闭合。
//!
//! 注入体：RI.CREATE 建树窗停车注入钩子族（RI_CREATE_WINDOW_PAUSE_INJECT，
//! 一次性消费即复位，STUB_LOAD 停车-续跑握手同族）把建树闭包确定性停在
//! 建树返回后、闭包交还前；注入线程经独立 compio 运行时执行真实 SET 语义
//! 内核（`StoreSession::upsert`——wnode network_set 同一 String 域落笔与
//! 清退原语）与真实信封物化（upsert_tag ObjectEnvelope，
//! promote_ri_create_domain_pin 同款注入形态），全程真盘真原语，无 fake
//! mock、无概率性锤打。三场景串行（停车钩子进程级单例，禁并行抢武装）。

// 停车注入钩驱动测试（RI_CREATE_WINDOW_* 系 debug 专用门控钩子族，停车
// 助手与场景一/二全为其服务）：release 随钩整文件剔除
// （wepoch/tests/epoch/shared_slot.rs 同款先例）
#![cfg(debug_assertions)]

use std::{
  path::Path,
  result::Result as StdResult,
  sync::{Arc, atomic::Ordering},
  thread,
  time::{Duration, Instant},
};

#[path = "store_open.rs"]
mod store_open;

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use parking_lot::Mutex;
use store_open::{open_store_in, range_index_config};
use tempfile::tempdir;
use wbftree::{StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::{
  RI_CREATE_WINDOW_PAUSE_INJECT, RI_CREATE_WINDOW_PAUSED, RI_CREATE_WINDOW_RESUME, RangeIndexError,
  StoreEvent, StoreEventSink, WedbStore,
};
use wval::{KeyTag, NamespaceDbCodec};

/// 字符串域穿窗场景键（逻辑 db 1）
const KEY_STR: &[u8] = b"ct:win:str";
/// 信封域穿窗场景键（逻辑 db 1）
const KEY_ENV: &[u8] = b"ct:win:env";
/// 静默正路径场景键（逻辑 db 1）
const KEY_OK: &[u8] = b"ct:win:ok";

/// 与 C# 测试一致的默认树调优（对标 tests/store/range_index.rs TUNE）
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 副本流域捕获条目（RangeIndexCreate 入账键字节）
type EventLog = Mutex<Vec<Vec<u8>>>;

/// 钉定域物理键构造（测试侧按编码内核直拼，与链首 session_tag_key_with_prefix
/// 字节恒等）
fn pinned_meta_key((vns, vdb): (u64, u64), key: &[u8]) -> wval::TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(vns, vdb, KeyTag::Meta, key)
}

/// 副本流域捕获 sink（只录 RangeIndexCreate，其余直通）
fn ri_create_sink(log: Arc<EventLog>) -> StoreEventSink {
  fn capture(log: &EventLog, _ver: i64, _sid: i32, event: StoreEvent<'_>) -> wkv::Result<()> {
    if let StoreEvent::RangeIndexCreate { key, .. } = event {
      log.lock().push(key.to_vec());
    }
    Ok(())
  }
  StoreEventSink::new(log, capture)
}

/// 驱动释放消费并轮询等待树数据文件物理收敛（对标
/// promote_ri_create_domain_pin::wait_file_gone 同款判据）
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

/// 等待建树闭包停入建树窗（带死线防护：注入未被消费即快速红）
fn wait_window_paused() {
  let deadline = Instant::now() + Duration::from_secs(30);
  while !RI_CREATE_WINDOW_PAUSED.load(Ordering::Acquire) {
    if Instant::now() > deadline {
      panic!("建树闭包未在 30s 内停入建树窗（钩子未被消费，链路断裂）");
    }
    thread::sleep(Duration::from_millis(1));
  }
}

/// 窗内注入探针：装弹 → 主任务发起建树（建树闭包确定性停入建树窗，预检已
/// 放行、落盘未发生）→ 注入线程独立 compio 运行时执行真实跨型写 → 放行
/// 续跑 → 返回 RI.CREATE 应答。注入闭包在注入线程内以独立运行时真实落笔
/// （被停闭包驻阻塞线程池自旋，反应器核空闲，无互候面）
async fn create_with_window_inject(
  store: Arc<WedbStore<SegmentedDevice>>,
  key: &[u8],
  inject: impl FnOnce(Arc<WedbStore<SegmentedDevice>>) + Send + 'static,
) -> StdResult<(), RangeIndexError> {
  RI_CREATE_WINDOW_PAUSE_INJECT.store(true, Ordering::Release);
  let inj_store = Arc::clone(&store);
  let injector = thread::spawn(move || {
    wait_window_paused();
    inject(inj_store);
    RI_CREATE_WINDOW_RESUME.store(true, Ordering::Release);
  });
  let s = store.new_session().expect("建树会话");
  assert!(s.set_context(0, 1), "db1 上下文物化");
  let res = s
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await;
  injector.join().expect("注入线程不得 panic");
  res
}

/// 场景一：建树窗内并发 SET k v（真实 SET 语义内核 upsert：String 域落笔 +
/// TTL 清腿 + 信封/Meta 覆写清退探针，wnode network_set 同原语）⇒ RI.CREATE
/// 按既有 WrongType 臂显式失败；终态单态（仅字符串，Meta/信封域皆空）；既有
/// unregister + delete_index 回滚零孤儿（注册表 / 数据文件 / 旁表）；AOF 零
/// RangeIndexCreate 半代入账；同键重试按预检臂显式拒绝（失败收口诚实可见）
async fn set_in_window_fails_wrongtype_single_state() -> Void {
  let dir = tempdir()?;
  let store = open_store_in(
    &dir,
    "ct_win_str.db",
    range_index_config(&dir, 1024, 64 * 1024)?,
  )?;
  let log: Arc<EventLog> = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(ri_create_sink(Arc::clone(&log))));

  let err = create_with_window_inject(Arc::clone(&store), KEY_STR, |st| {
    let rt = Runtime::new().expect("注入运行时");
    rt.block_on(async {
      let s = st.new_session().expect("注入会话");
      assert!(s.set_context(0, 1), "注入会话 db1 上下文物化");
      s.upsert(KEY_STR, b"cross-type-str")
        .await
        .expect("窗内 SET 真实生效");
    });
  })
  .await
  .expect_err("跨型 SET 穿预检窗须显式失败禁双态落盘");
  assert!(
    matches!(err, RangeIndexError::WrongType),
    "须按既有 WrongType 臂显式失败，实际 {err:?}"
  );

  let s = store.new_session()?;
  assert!(s.set_context(0, 1), "断言会话 db1 上下文物化");
  // 终态单态：仅字符串值在位，RI 元记录与信封域皆空
  assert_eq!(
    s.read(KEY_STR).await?,
    Some(b"cross-type-str".to_vec()),
    "终态字符串值在位（注入写存活）"
  );
  assert!(s.load_meta(KEY_STR).await?.is_none(), "终态无 RI 元记录");
  let env_k = s.session_tag_key(KeyTag::ObjectEnvelope, KEY_STR);
  assert!(!s.contains_key_raw(&env_k).await?, "终态无信封记录");

  // 既有回滚零孤儿：注册表摘除、数据文件回收、旁表零残留
  let pinned = s.virtual_domain();
  let pinned_id = pinned_meta_key(pinned, KEY_STR);
  assert!(
    store.range_index().get_tree(&pinned_id).is_none(),
    "回滚后树身份不得残留注册表"
  );
  wait_file_gone(
    &store.range_index().data_file_path_for_key(&pinned_id),
    &store,
  )
  .await?;
  assert!(!side_table_holds(&store, KEY_STR), "旁表零孤儿");

  // AOF 零半代入账（复查失败臂先于 emit）
  assert!(
    !log.lock().iter().any(|k| k == KEY_STR),
    "失败臂禁 RangeIndexCreate 镜像入账"
  );
  // 停车钩子一次性消费即复位，不污染稳态链路
  assert!(
    !RI_CREATE_WINDOW_PAUSE_INJECT.load(Ordering::Acquire),
    "停车注入一次性消费即复位"
  );
  assert!(
    !RI_CREATE_WINDOW_PAUSED.load(Ordering::Acquire),
    "停车示信复位"
  );

  // 同键重试按预检臂显式拒绝（键已是字符串）——失败收口诚实可见
  let retry = s
    .range_index_create(KEY_STR, StorageBackendType::Disk, TUNE)
    .await
    .expect_err("重试须按预检臂显式拒绝");
  assert!(
    matches!(retry, RangeIndexError::WrongType),
    "重试按预检臂回 WrongType，实际 {retry:?}"
  );
  OK
}

/// 场景二：建树窗内并发 HSET 族信封物化（upsert_tag ObjectEnvelope 真实
/// 物化原语）⇒ 同型显式失败回 WrongType；终态单态（仅信封，String/Meta 域
/// 皆空）；回滚零孤儿同场景一
async fn envelope_in_window_fails_wrongtype_single_state() -> Void {
  let dir = tempdir()?;
  let store = open_store_in(
    &dir,
    "ct_win_env.db",
    range_index_config(&dir, 1024, 64 * 1024)?,
  )?;

  let err = create_with_window_inject(Arc::clone(&store), KEY_ENV, |st| {
    let rt = Runtime::new().expect("注入运行时");
    rt.block_on(async {
      let s = st.new_session().expect("注入会话");
      assert!(s.set_context(0, 1), "注入会话 db1 上下文物化");
      s.upsert_tag(KEY_ENV, KeyTag::ObjectEnvelope, b"\x03env-snapshot")
        .await
        .expect("窗内信封物化真实生效");
    });
  })
  .await
  .expect_err("跨型信封穿预检窗须显式失败禁双态落盘");
  assert!(
    matches!(err, RangeIndexError::WrongType),
    "须按既有 WrongType 臂显式失败，实际 {err:?}"
  );

  let s = store.new_session()?;
  assert!(s.set_context(0, 1), "断言会话 db1 上下文物化");
  // 终态单态：仅信封在位，字符串与 RI 元记录皆空
  let env_k = s.session_tag_key(KeyTag::ObjectEnvelope, KEY_ENV);
  assert!(
    s.contains_key_raw(&env_k).await?,
    "终态信封记录在位（注入写存活）"
  );
  assert!(s.read(KEY_ENV).await?.is_none(), "终态无字符串记录");
  assert!(s.load_meta(KEY_ENV).await?.is_none(), "终态无 RI 元记录");

  let pinned = s.virtual_domain();
  let pinned_id = pinned_meta_key(pinned, KEY_ENV);
  assert!(
    store.range_index().get_tree(&pinned_id).is_none(),
    "回滚后树身份不得残留注册表"
  );
  wait_file_gone(
    &store.range_index().data_file_path_for_key(&pinned_id),
    &store,
  )
  .await?;
  assert!(!side_table_holds(&store, KEY_ENV), "旁表零孤儿");
  OK
}

/// 场景三：静默正路径零回归——不装弹、无跨型写，干净键上建索引照常成功
/// （三域复查零干扰零误拒）；树在册、元记录落位、旁表登记、AOF 恰一条
/// Create 入账；索引可写可读（后续稳态写臂照常工作）
async fn quiescent_create_unaffected_by_recheck() -> Void {
  let dir = tempdir()?;
  let store = open_store_in(
    &dir,
    "ct_win_ok.db",
    range_index_config(&dir, 1024, 64 * 1024)?,
  )?;
  let log: Arc<EventLog> = Arc::new(Mutex::new(Vec::new()));
  assert!(store.set_event_sink(ri_create_sink(Arc::clone(&log))));

  assert!(
    !RI_CREATE_WINDOW_PAUSE_INJECT.load(Ordering::Acquire),
    "钩子默认必须关闭"
  );
  let s = store.new_session()?;
  assert!(s.set_context(0, 1), "db1 上下文物化");
  let pinned = s.virtual_domain();
  s.range_index_create(KEY_OK, StorageBackendType::Disk, TUNE)
    .await
    .expect("静默正路径建索引照常成功（复查零误拒）");

  let pinned_id = pinned_meta_key(pinned, KEY_OK);
  assert!(
    store.range_index().get_tree(&pinned_id).is_some(),
    "成功路径树身份在册"
  );
  assert_eq!(
    s.load_meta(KEY_OK).await?.map(|m| m.size),
    Some(0),
    "元记录落位（空索引计数 0）"
  );
  assert!(side_table_holds(&store, KEY_OK), "成功路径旁表登记在册");
  assert_eq!(
    log.lock().iter().filter(|k| k.as_slice() == KEY_OK).count(),
    1,
    "AOF 恰一条 RangeIndexCreate 入账"
  );

  // 稳态写臂照常：RI.SET 落字段（长度契约：field+value ≥ min_record_size 8）
  s.range_index_set(KEY_OK, b"field-01", b"value-01")
    .await
    .expect("建索引后稳态写照常");
  assert_eq!(
    s.range_index_get(KEY_OK, b"field-01").await?,
    Some(b"value-01".to_vec()),
    "索引可读"
  );
  OK
}

/// 三场景串行（停车钩子进程级单例，禁并行抢武装）
#[compio::test]
async fn cross_type_window_recheck_fails_closed_and_single_state() -> Void {
  set_in_window_fails_wrongtype_single_state().await?;
  envelope_in_window_fails_wrongtype_single_state().await?;
  quiescent_create_unaffected_by_recheck().await?;
  OK
}
