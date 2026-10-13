//! 紧缩清退安全垫三值保守回归：宿主存在性探查返 `Err` 严禁折成孤儿判死
//!
//! 对位 C# 紧缩面「不确定存活绝不宣告死亡」保守补拷臂（
//! libs/storage/Tsavorite/cs/src/core/Compaction/ConditionalCopyToTail.cs 的
//! NOTFOUND 臂，经 TsavoriteCompaction.cs:Compact → FindRecord.cs 链抵达）；
//! rust 侧 `WedbCompactionFunctions::on_dropped` 的无 TTL 旁路支消费
//! `host_exists_cooperative` 的三值契约（在场 / 确证缺席 / 探查不确定），第三档
//! `Err`（grow 迁移窗内核错误：溢出桶耗尽 / 环形 Cycle）必须原样上抛，借道
//! `wcompact::CompactRun::drop_dead` 既有「跳过摘槽 + 保守保留 + 截断回退」通道
//! 延至下轮重试。若把 `Err` 折成缺席，本轮即对仍存活的宿主入账伪
//! `RangeIndexDrop`（先入 AOF 并放射副本）并注销物理树（数据文件按
//! deleteFiles:true 回收），主从双侧不可逆毁数据。
//!
//! 注错钩 `wkv::HOST_EXISTS_ERR_INJECT` 为 `#[cfg(debug_assertions)]` 一次性
//! 消费点（生产构建不编译），与同函数既有 `wkv::ON_DROPPED_PAUSE_INJECT` 握手
//! 合用：清退链在安全垫之前停驻 → 测试窗内并发重建分层宿主（树在册、记录可读）
//! → 装载探查注错 → 放行安全垫，精确复现「宿主确在而本轮无从确证」的内核错误档。

use std::sync::{Arc, OnceLock};
// Duration/Ordering 仅 debug 注入钩用例消费，release 随用例剔除
#[cfg(debug_assertions)]
use std::{sync::atomic::Ordering, time::Duration};

use aok::{OK, Void};
#[cfg(debug_assertions)]
use compio::{runtime::spawn, time::sleep};
use parking_lot::Mutex;
use tempfile::TempDir;
use wcompact::CompactionType;
// 注错/停车钩族系 debug 专用门控符号，随注入用例 release 剔除
#[cfg(debug_assertions)]
use wkv::{HOST_EXISTS_ERR_INJECT, ON_DROPPED_PAUSE_INJECT, ON_DROPPED_PAUSED, ON_DROPPED_RESUME};
use wkv::{SerialLock, StoreEvent, StoreEventSink};
// GarnetObjectType 仅注错钩用例消费，随用例剔除（KeyTag/NamespaceDbCodec/
// TaggedKeyBuf 仅被同门的 meta_key 消费，一并剔除）
#[cfg(debug_assertions)]
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec, TaggedKeyBuf};

use crate::support::{open_store_in, range_index_config};

const PAGE: usize = 1024 * 1024;

/// `RangeIndexDrop` 入账留痕器（事件面判据：保守轮必须零条）
#[derive(Default)]
struct DropLedger {
  drops: Mutex<Vec<Vec<u8>>>,
}

fn ledger_handler(
  ctx: &DropLedger,
  _ver: i64,
  _sid: i32,
  event: StoreEvent<'_>,
) -> wkv::Result<()> {
  if let StoreEvent::RangeIndexDrop { key, .. } = event {
    ctx.drops.lock().push(key.to_vec());
  }
  Ok(())
}

/// 树身份键 = 物理 Meta 键（默认会话域 (0, 0)）
#[cfg(debug_assertions)]
fn meta_key(user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Meta, user_key)
}

/// 四枚调试钩均为进程级一次性位，用例起手显式复位杜绝跨用例残留装载
#[cfg(debug_assertions)]
fn reset_debug_hooks() {
  ON_DROPPED_PAUSE_INJECT.store(false, Ordering::SeqCst);
  ON_DROPPED_PAUSED.store(false, Ordering::SeqCst);
  ON_DROPPED_RESUME.store(false, Ordering::SeqCst);
  HOST_EXISTS_ERR_INJECT.store(false, Ordering::SeqCst);
}

/// 清退窗内宿主探查返 `Err`：本轮保守保留，宿主树与旁路记录分毫未动，
/// 零 `RangeIndexDrop` 入账，截断位点不前移；注错解除后下轮重试不误杀
// 注错钩用例：随 HOST_EXISTS_ERR_INJECT / ON_DROPPED_* 的 debug 门控剔除
#[cfg(debug_assertions)]
async fn host_probe_err_round_retains_candidate(comp_type: CompactionType, tag: &str) -> Void {
  let dir = TempDir::new()?;
  let mut config = range_index_config(&dir, 2048, PAGE)?;
  config.gc.enabled = false;
  let store = open_store_in(&dir, tag, config)?;
  let ledger = Arc::new(DropLedger::default());
  assert!(store.set_event_sink(StoreEventSink::new(Arc::clone(&ledger), ledger_handler)));
  let session = store.new_session()?;

  // 判死候选：无宿主无 TTL 的孤儿 ETag 旁路记录——`is_deleted` 的 KeyTag::Etag
  // 臂经宿主探查确证缺席（`Ok(false)`）判死，且该键无 TTL 旁路，`on_dropped`
  // 安全垫必走宿主存在性探查支（即本票三值裁决位）
  let key = b"k:host_probe_err";
  session.put_etag(key, 11).await?;
  let etag_key = session.etag_key(key);
  let tree_key = meta_key(key);
  assert!(
    store.index.load().find_tag(etag_key.as_slice()).is_some(),
    "前置：孤儿 ETag 记录必须挂载索引槽位"
  );

  let tail = store.tail_address();
  store.shift_read_only_address(tail);
  let begin_before = store.begin_address();

  // 一次性停驻钩：清退链在安全垫之前挂起，测试据此在窗内改动宿主态
  ON_DROPPED_PAUSE_INJECT.store(true, Ordering::SeqCst);

  let compact_store = Arc::clone(&store);
  let compact_task = spawn(async move { compact_store.compact(tail, comp_type).await });
  while !ON_DROPPED_PAUSED.load(Ordering::Acquire) {
    sleep(Duration::from_millis(10)).await;
  }

  // 窗内并发重建：同键升阶为分层集合（树在册 + 数据文件在场），宿主确在；
  // 随即装载探查注错，使安全垫这一探针返迁移内核错误——既拿不到在场凭证，
  // 也不是确证缺席，本轮唯一正确裁决是上抛保守保留
  session
    .promote_collection_to_bftree(
      key,
      GarnetObjectType::Hash,
      vec![(b"f1".to_vec(), b"v1".to_vec())],
      i64::MAX,
      false,
    )
    .await?;
  let mgr = store.range_index();
  assert!(
    mgr.get_tree(&tree_key).is_some(),
    "前置：窗内重建的分层宿主树必须在册"
  );
  let data_path = mgr.data_file_path_for_key(&tree_key);
  assert!(data_path.exists(), "前置：宿主数据文件必须在场");
  HOST_EXISTS_ERR_INJECT.store(true, Ordering::SeqCst);

  ON_DROPPED_RESUME.store(true, Ordering::Release);
  let stats = compact_task.await.unwrap()?;

  // 物理面保全判据先行（折损成孤儿即破坏链已落，注销与入账均不可逆）
  assert!(
    mgr.get_tree(&tree_key).is_some(),
    "宿主树不得因探查折损被注销（注销即物理树文件随 deleteFiles:true 回收，不可恢复）"
  );
  assert!(data_path.exists(), "宿主数据文件不得被物理释放");
  assert!(mgr.cache_reserved() > 0, "在册树页缓存预算必须仍在账");
  assert!(
    ledger.drops.lock().is_empty(),
    "事件面必须零伪清理事件入账（副本据此删除同一活对象）: {:?}",
    ledger.drops.lock()
  );
  // 保守通道判据（静默放行不破坏、但也不留痕，同样违规）
  assert!(
    stats.retained >= 1,
    "探查不确定的记录必须经上抛借道保守通道计入 retained: {stats:?}"
  );
  assert_eq!(
    stats.dead_dropped, 0,
    "探查不确定轮严禁记为清退成功: {stats:?}"
  );
  assert!(
    store.index.load().find_tag(etag_key.as_slice()).is_some(),
    "候选记录索引槽位必须保留，严禁误摘成悬挂槽"
  );
  assert_eq!(
    session.etag_of(key).await?,
    Some(11),
    "候选记录本轮仍须可读（保守保留非静默放行）"
  );
  assert!(
    session.contains_key(key).await?,
    "窗内重建的活宿主对本轮清退必须免疫"
  );
  assert_eq!(
    store.begin_address(),
    begin_before,
    "本轮无确证死亡，截断位点严禁前移"
  );

  // 下轮重试：探查不再注错，宿主确在 → 候选随宿主同活，绝不误杀
  let stats2 = store.compact(tail, comp_type).await?;
  assert_eq!(
    stats2.dead_dropped, 0,
    "下轮宿主在场，候选不得被清退: {stats2:?}"
  );
  assert!(mgr.get_tree(&tree_key).is_some(), "重试轮宿主树仍在册");
  assert!(
    ledger.drops.lock().is_empty(),
    "两轮累计仍须零伪 RangeIndexDrop"
  );
  assert_eq!(session.etag_of(key).await?, Some(11), "候选记录随轮保全");
  assert!(
    session.load_meta(key).await?.is_some(),
    "分层宿主路由域须完好可读"
  );
  OK
}

/// 真孤儿回归臂：探查确证缺席（`Ok(false)`）时破坏性清退链照常闭环，
/// 本票改动不得削弱既有清退能力
async fn host_probe_absent_round_still_evicts_orphan(comp_type: CompactionType, tag: &str) -> Void {
  let dir = TempDir::new()?;
  let mut config = range_index_config(&dir, 2048, PAGE)?;
  config.gc.enabled = false;
  let store = open_store_in(&dir, tag, config)?;
  let ledger = Arc::new(DropLedger::default());
  assert!(store.set_event_sink(StoreEventSink::new(Arc::clone(&ledger), ledger_handler)));
  let session = store.new_session()?;

  let key = b"k:host_absent";
  session.put_etag(key, 5).await?;
  let etag_key = session.etag_key(key);

  let tail = store.tail_address();
  store.shift_read_only_address(tail);

  let stats = store.compact(tail, comp_type).await?;
  assert!(
    stats.dead_dropped >= 1,
    "确证孤儿的 ETag 旁路记录须照常判死清退: {stats:?}"
  );
  assert_eq!(stats.retained, 0, "确证缺席轮不得计入保守保留: {stats:?}");
  assert!(
    store.index.load().find_tag(etag_key.as_slice()).is_none(),
    "清退链须摘除孤儿记录索引槽位"
  );
  assert_eq!(
    session.etag_of(key).await?,
    None,
    "孤儿旁路记录须随清退消亡"
  );
  assert!(
    ledger.drops.lock().is_empty(),
    "无分层宿主的孤儿清退不该入账 RangeIndexDrop"
  );
  OK
}

fn test_lock() -> &'static SerialLock {
  static LOCK: OnceLock<SerialLock> = OnceLock::new();
  LOCK.get_or_init(SerialLock::default)
}

// 注错钩用例：随 HOST_EXISTS_ERR_INJECT 的 debug 门控剔除
#[cfg(debug_assertions)]
#[compio::test]
async fn compact_lookup_host_probe_err_retains_live_host() -> Void {
  let _guard = test_lock().acquire().await;
  reset_debug_hooks();
  let res =
    host_probe_err_round_retains_candidate(CompactionType::Lookup, "hostprobe_err_lookup").await;
  reset_debug_hooks();
  res
}

// 注错钩用例：随 HOST_EXISTS_ERR_INJECT 的 debug 门控剔除
#[cfg(debug_assertions)]
#[compio::test]
async fn compact_scan_host_probe_err_retains_live_host() -> Void {
  let _guard = test_lock().acquire().await;
  reset_debug_hooks();
  let res =
    host_probe_err_round_retains_candidate(CompactionType::Scan, "hostprobe_err_scan").await;
  reset_debug_hooks();
  res
}

#[compio::test]
async fn compact_lookup_host_probe_absent_still_evicts_orphan() -> Void {
  let _guard = test_lock().acquire().await;
  #[cfg(debug_assertions)]
  reset_debug_hooks();
  let res =
    host_probe_absent_round_still_evicts_orphan(CompactionType::Lookup, "hostprobe_absent_lookup")
      .await;
  #[cfg(debug_assertions)]
  reset_debug_hooks();
  res
}

#[compio::test]
async fn compact_scan_host_probe_absent_still_evicts_orphan() -> Void {
  let _guard = test_lock().acquire().await;
  #[cfg(debug_assertions)]
  reset_debug_hooks();
  let res =
    host_probe_absent_round_still_evicts_orphan(CompactionType::Scan, "hostprobe_absent_scan")
      .await;
  #[cfg(debug_assertions)]
  reset_debug_hooks();
  res
}
