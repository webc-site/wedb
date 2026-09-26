//! 票 zcode-r167c-aoffail 回归：AOF 日志追加（emit_event）失败吞错冒泡三案
//!
//! 对标 C# 日志先行契约（GarnetLog.cs:Enqueue 故障沿调用栈上抛至 RESP 层报错，
//! Tsavorite core/Compaction/TsavoriteCompaction.cs:Compact 入队故障即中止紧缩）
//! 与 rust 侧 error.rs `AofEnqueue` 契约原文「主存写入已生效，AOF 缺条目，
//! 调用方须以错误拒绝该命令防主从发散」及 promote.rs / migration.rs 既有冒泡臂
//! 单一机制。注入面复用 rename_semantics.rs 同款真故障 sink（按事件类型裁决
//! Err，非假 mock）。覆盖：
//! 1. 案一 RangeIndex 面：RI.CREATE / RI.SET / RI.SET 批量 / RI.DEL 在对应
//!    事件入队失败时一律回 Err（禁吞错冒答成功），「已生效 + 镜像缺失」臂不
//!    伪装零副作用（树写与计数已生效可查，即契约的调用方纪律）；
//! 2. 案一 drain 面：`handle_bftree_drain_and_delete` 的 RangeIndexDrop 入账
//!    失败经 `Error::Swapped` 分级上抛（键已死、物理面已变更，分层臂 WATCH
//!    判据照常推进）；解除注错后重试闭环、Drop 恰一次入账；
//! 3. 案三 紧缩面：`WedbCompactionFunctions::on_dropped` 日志先行——
//!    RangeIndexDrop 入账失败即树分毫未动、记录保守保留（retained）、
//!    begin_address 不前移；下轮紧缩重试入账必达并闭环清退（Drop 恰一条）。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::Duration,
};

use aok::{OK, Void};
use parking_lot::Mutex;
use tempfile::TempDir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wbftree::{RangeIndexManager, StorageBackendType, TreeTuning};
use wcompact::CompactionType;
use wdev::SegmentedDevice;
use wkv::{Error as WkvError, RangeIndexError, StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec, TaggedKeyBuf};

/// 与 store/range_index.rs 套件一致的默认树调优
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 字段值定长 16B（满足 TUNE 的 8B 记录下限）
const VAL16: &[u8; 16] = b"vvvvvvvvvvvvvvvv";

/// 按事件类型注错的 AOF 入账故障上下文（未注错事件直通）
struct FaultCtx {
  fail_create: AtomicBool,
  fail_write: AtomicBool,
  fail_drop: AtomicBool,
  /// 成功入账的 RangeIndexDrop 事件用户键（恰一次入账断言用）
  drops: parking_lot::Mutex<Vec<Vec<u8>>>,
}

impl FaultCtx {
  fn new() -> Self {
    Self {
      fail_create: AtomicBool::new(false),
      fail_write: AtomicBool::new(false),
      fail_drop: AtomicBool::new(false),
      drops: Mutex::new(Vec::new()),
    }
  }
}

fn inject() -> WkvError {
  WkvError::AofEnqueue("注入：AOF 入账失败（测试）".into())
}

fn fault_handler(ctx: &FaultCtx, _ver: i64, _sid: i32, event: StoreEvent<'_>) -> wkv::Result<()> {
  match event {
    StoreEvent::RangeIndexCreate { .. } if ctx.fail_create.load(Ordering::Acquire) => {
      return Err(inject());
    }
    StoreEvent::RangeIndexWrite { .. } if ctx.fail_write.load(Ordering::Acquire) => {
      return Err(inject());
    }
    StoreEvent::RangeIndexDrop { key, .. } => {
      if ctx.fail_drop.load(Ordering::Acquire) {
        return Err(inject());
      }
      ctx.drops.lock().push(key.to_vec());
    }
    _ => {}
  }
  Ok(())
}

fn open_store(dir: &TempDir, name: &str) -> aok::Result<Arc<WedbStore<SegmentedDevice>>> {
  let mut config =
    StoreConfig::new(2048, 1024 * 1024, 16, 0.5)?.with_range_index_dir(dir.path().join("ri"));
  config.gc.enabled = false;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  Ok(Arc::new(WedbStore::open(config, device)?))
}

/// 树身份键 = 物理 Meta 键（默认会话域 (0, 0)）
fn meta_key(user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Meta, user_key)
}

/// 错误面须为 AofEnqueue 冒泡（RI 面经 RangeIndexError::Store 单点包装）
fn is_aof(err: &RangeIndexError) -> bool {
  matches!(err, RangeIndexError::Store(inner) if inner.to_string().contains("AOF"))
}

/// 案一：RI 增删改臂入账失败一律以错误拒绝命令；已生效臂不伪装零副作用
#[compio::test]
async fn ri_write_ops_reject_on_aof_enqueue_failure() -> Void {
  let dir = TempDir::new()?;
  let store = open_store(&dir, "ri_aof_fail.db")?;
  let ctx = Arc::new(FaultCtx::new());
  assert!(store.set_event_sink(StoreEventSink::new(Arc::clone(&ctx), fault_handler)));
  let session = store.new_session()?;

  // CREATE 入账失败 → 命令报错（禁吞错冒答成功）；已提交态不撤回（契约
  // error.rs:91「主存写入已生效」臂）：重试同键判重即证登记已落
  ctx.fail_create.store(true, Ordering::Release);
  let err = session
    .range_index_create(b"idx", StorageBackendType::Disk, TUNE)
    .await
    .expect_err("RangeIndexCreate AOF 入队失败必须上抛拒绝命令");
  assert!(is_aof(&err), "错误面须为 AofEnqueue 冒泡，实际 {err}");
  ctx.fail_create.store(false, Ordering::Release);
  let err = session
    .range_index_create(b"idx", StorageBackendType::Disk, TUNE)
    .await
    .expect_err("create 失败臂主存登记已生效，重试须判重而非重建");
  assert!(
    matches!(err, RangeIndexError::AlreadyExists),
    "已生效臂不得伪装零副作用，实际 {err}"
  );

  // SET 入账失败 → 命令报错；树写与计数已生效（AofEnqueue 契约「已生效 +
  // 镜像缺失」形态，调用方不得当零副作用处理）
  ctx.fail_write.store(true, Ordering::Release);
  let err = session
    .range_index_set(b"idx", b"f1", VAL16)
    .await
    .expect_err("RangeIndexWrite AOF 入队失败必须上抛拒绝命令");
  assert!(is_aof(&err));
  assert_eq!(
    session.range_index_get(b"idx", b"f1").await?.as_deref(),
    Some(VAL16.as_slice()),
    "已生效臂不伪装回滚：树写可见，镜像缺失经错误帧对客户端可见"
  );
  assert_eq!(
    session.range_index_count(b"idx").await?,
    1,
    "计数随写已生效"
  );

  // 批量写臂同样报错（树内容整体已生效后逐条入账，首条即败禁续吞——
  // 「已生效 + 镜像缺失」取数面全部可见）
  let batch = [
    (b"f2".as_slice(), VAL16.as_slice()),
    (b"f3".as_slice(), VAL16.as_slice()),
  ];
  let err = session
    .range_index_set_batch(b"idx", &batch)
    .await
    .expect_err("批量 RangeIndexWrite AOF 入队失败必须上抛拒绝命令");
  assert!(is_aof(&err));
  assert_eq!(
    session.range_index_count(b"idx").await?,
    3,
    "批量条目树写整体已生效"
  );

  // DEL 字段条目入账同样报错；解除注错后删除闭环
  ctx.fail_write.store(false, Ordering::Release);
  session.range_index_del(b"idx", b"f2").await?;
  assert_eq!(session.range_index_count(b"idx").await?, 2);
  OK
}

/// 案一 drain 面：RangeIndexDrop 入账失败经 Swapped 分级上抛；重试闭环、
/// Drop 恰一次入账（旧吞错形态为「冒答成功 + 副本永缺 Drop」，就此收口）
#[compio::test]
async fn ri_drain_drop_enqueue_failure_bubbles_and_retry_emits_once() -> Void {
  let dir = TempDir::new()?;
  let store = open_store(&dir, "ri_drain_fail.db")?;
  let ctx = Arc::new(FaultCtx::new());
  assert!(store.set_event_sink(StoreEventSink::new(Arc::clone(&ctx), fault_handler)));
  let session = store.new_session()?;

  session
    .range_index_create(b"drain", StorageBackendType::Disk, TUNE)
    .await?;
  session.range_index_set(b"drain", b"f1", VAL16).await?;

  // 删空自愈臂：drain 末位 RangeIndexDrop 入账失败 → 整命令报错
  ctx.fail_drop.store(true, Ordering::Release);
  let err = session
    .range_index_del(b"drain", b"f1")
    .await
    .expect_err("RangeIndexDrop AOF 入队失败必须上抛拒绝删空命令");
  assert!(is_aof(&err), "错误面须为 AofEnqueue 冒泡，实际 {err}");
  assert!(ctx.drops.lock().is_empty(), "入账失败轮副本必无 Drop 条目");

  // 解除注错后重试排空单点闭环：物理面幂等（meta 已墓碑、树已注销），
  // Drop 恰一次入账
  ctx.fail_drop.store(false, Ordering::Release);
  session
    .handle_bftree_drain_and_delete(b"drain", false)
    .await?;
  assert_eq!(&*ctx.drops.lock(), &[b"drain".to_vec()], "Drop 恰一次入账");
  OK
}

/// 案一 drain 失败分级：直调排空臂的入账失败为 Swapped（键已死、物理面已
/// 变更——分层臂 WATCH 判据据此保持置脏真实推进）
#[compio::test]
async fn ri_drain_drop_failure_is_swapped_graded() -> Void {
  let dir = TempDir::new()?;
  let store = open_store(&dir, "ri_drain_swapped.db")?;
  let ctx = Arc::new(FaultCtx::new());
  assert!(store.set_event_sink(StoreEventSink::new(Arc::clone(&ctx), fault_handler)));
  let session = store.new_session()?;

  session
    .range_index_create(b"swapped", StorageBackendType::Disk, TUNE)
    .await?;
  ctx.fail_drop.store(true, Ordering::Release);
  let err = session
    .handle_bftree_drain_and_delete(b"swapped", false)
    .await
    .expect_err("RangeIndexDrop AOF 入队失败必须上抛");
  assert!(
    matches!(err, WkvError::Swapped(ref inner) if inner.to_string().contains("AOF")),
    "墓碑与树注销已落盘后的入账失败须经 Swapped 分级，实际 {err:?}"
  );
  OK
}

/// 案三 紧缩面：on_dropped 日志先行——RangeIndexDrop 入账失败即树分毫未动、
/// 记录保守保留、begin_address 不前移；解除注错后下轮紧缩重试入账必达并
/// 闭环清退（副本端 Drop 恰一条，杜绝孤儿树永久泄漏）
#[compio::test]
async fn compact_drop_event_enqueue_failure_retains_and_retries_next_round() -> Void {
  let dir = TempDir::new()?;
  let store = open_store(&dir, "compact_drop_fail.db")?;
  let ctx = Arc::new(FaultCtx::new());
  assert!(store.set_event_sink(StoreEventSink::new(Arc::clone(&ctx), fault_handler)));
  let session = store.new_session()?;

  let key = b"h:compact_fail";
  session
    .promote_collection_to_bftree(
      key,
      GarnetObjectType::Hash,
      vec![(b"f1".to_vec(), b"v1".to_vec())],
      i64::MAX,
      false,
    )
    .await?;
  let tree_key = meta_key(key);
  let mgr: Arc<RangeIndexManager> = Arc::clone(store.range_index());
  assert!(mgr.get_tree(&tree_key).is_some(), "升阶后树实例必须在册");
  let data_path = mgr.data_file_path_for_key(&tree_key);
  assert!(data_path.exists());

  // 免 sleep 直写已过期 TTL → 紧缩判死
  session.put_ttl(key, now_ticks() - TICKS_PER_SECOND).await?;
  let tail = store.tail_address();
  store.shift_read_only_address(tail);
  let begin_before = store.begin_address();

  // 入账注错轮：Drop 入账失败 → 日志先行裁决——树不得注销、记录保留、
  // 截断位点不前移
  ctx.fail_drop.store(true, Ordering::Release);
  let stats = store.compact(tail, CompactionType::Scan).await?;
  assert!(
    stats.retained >= 1,
    "入账失败记录必须计入 retained 保守保留: {stats:?}"
  );
  assert!(
    mgr.get_tree(&tree_key).is_some(),
    "入账失败时树实例不得被注销（否则下轮无重发通道，副本永缺 Drop）"
  );
  assert!(data_path.exists(), "入账失败时数据文件不得物理释放");
  assert_eq!(
    store.begin_address(),
    begin_before,
    "清理事件未入账严禁前移截断位点"
  );
  assert!(ctx.drops.lock().is_empty(), "入账失败轮副本必无 Drop 条目");

  // 解除注错：下轮紧缩重试入账必达并闭环清退，Drop 恰一条
  ctx.fail_drop.store(false, Ordering::Release);
  let stats2 = store.compact(tail, CompactionType::Scan).await?;
  assert!(stats2.dead_dropped >= 1, "下轮重判清退必须成功: {stats2:?}");
  assert!(mgr.get_tree(&tree_key).is_none(), "重试轮须闭环注销树实例");
  thread::sleep(Duration::from_millis(50));
  assert!(!data_path.exists(), "重试轮须物理释放数据文件");
  assert_eq!(
    &*ctx.drops.lock(),
    &[key.to_vec()],
    "重试轮 Drop 恰一次入账（副本据此消亡 Meta 域）"
  );
  assert!(
    store.begin_address() > begin_before,
    "事件入账闭环后截断位点方可前移"
  );
  OK
}
