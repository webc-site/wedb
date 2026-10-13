//! RENAME 段三 dst ETag 旁路残留清退测试（对标 C# RENAME 记录级重写：
//! SET(newKey, in logRecord) 的 FieldInfo 硬编码 HasETag=false
//! （UnifiedStore/VarLenInputMethods.cs:147 GetUpsertFieldInfo →
//! LogRecord.TryCopyOptionals RemoveETag），new 记录不携带 ETag、dst 旧记录
//! 尾随 ETag 可选字段一体消亡）
//!
//! 覆盖：
//! - dst 曾为条件写键（旁路在场）：rename_range_index 内核单独收敛即清退 dst
//!   旧 etag（修复前：内核全函数零 etag 触点，claim 释放至调用方尾部清退间
//!   条件写比对残留错基线，且孤儿 ETag 被紧缩误判存活回拷永生）；
//! - 在场摘除恰发一次 EtagWrite(None)（写监听 KeyTag::Etag 快速分流 →
//!   Setwithetag(0) 条目 → 副本回放侧 del_etag 同调，回放终态无孤儿 etag）；
//! - 缺席臂幂等零写零入账（RENAME 到无 etag 的键不产任何 etag 事件）；
//! - 旧键随键 etag 由段五排空臂清退（handle_bftree_drain_and_delete
//!   keep_ttl=false → del_ttl+del_etag 成对，collection.rs 删键臂先例），内核
//!   不迁移（C# HasETag=false，随迁面由调用方 finish_rename_move 双臂先例承载）。
//!
//! 在 garnet 中的相对路径: libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAME

use std::sync::{Arc, Mutex};

use aok::{OK, Void};
use tempfile::tempdir;
use wbftree::{StorageBackendType, TreeTuning};
use wkv::{StoreConfig, StoreEvent, StoreEventSink, WedbStore};

use crate::support::open_store_in;

/// 与 range_index 模块测试一致的默认树调优：min_record=8 / max_record=1024 /
/// max_key_len=128
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 字段值定长 16B（满足 TUNE 的 8B 记录下限）
const VAL16: &[u8; 16] = b"vvvvvvvvvvvvvvvv";

/// EtagWrite 事件记录（键， etag）
type EtagLog = Arc<Mutex<Vec<(Vec<u8>, Option<i64>)>>>;

/// 全事件直通、仅按序记 EtagWrite 的 sink（不干扰建索引 / 迁移 / emit）
fn etag_recording_sink(log: EtagLog) -> StoreEventSink {
  StoreEventSink::new(log, |log, _ver, _aof_session_id, event| {
    if let StoreEvent::EtagWrite { key, etag, .. } = event {
      log.lock().expect("事件日志锁").push((key.to_vec(), etag));
    }
    Ok(())
  })
}

/// 构造挂载 RangeIndex 目录的会话层引擎
async fn open_store(
  dir: &tempfile::TempDir,
  name: &str,
) -> aok::Result<Arc<WedbStore<wdev::SegmentedDevice>>> {
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  open_store_in(dir, name, config)
}

/// 源键灌入纯 RI 索引（RI.CREATE + 逐条 RI.SET）
async fn fill_ri<D: wdev::Device>(
  session: &wkv::StoreSession<D>,
  key: &[u8],
  fields: &[&[u8]],
) -> aok::Result<()> {
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  for f in fields {
    session.range_index_set(key, f, VAL16).await?;
  }
  Ok(())
}

/// dst 曾为条件写键：内核段三清退 dst 旧 etag + 恰发一次 EtagWrite(None)
/// 入账（缺席零写零入账的在场臂）；旧键随键 etag 由段五排空臂清退、不迁移
#[compio::test]
async fn rename_clears_dst_etag_residue_and_emits_tombstone_entry() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_etag_residue.db").await?;
  let log: EtagLog = Arc::new(Mutex::new(Vec::new()));
  assert!(
    store.set_event_sink(etag_recording_sink(Arc::clone(&log))),
    "sink 注入应成功"
  );
  let session = store.new_session()?;

  // dst：String 记录 + 条件写 etag 旁路在场；src：RI 索引 + 随键 etag（内核
  // 段五排空臂清退面）
  session.upsert(b"dst", b"payload").await?;
  session.put_etag(b"dst", 7).await?;
  fill_ri(&session, b"src", &[b"f1", b"f2"]).await?;
  session.put_etag(b"src", 5).await?;
  assert_eq!(session.etag_of(b"dst").await?, Some(7));
  let base = log.lock().expect("事件日志锁").len();

  session.rename_range_index(b"src", b"dst").await?;

  // 迁移本体收敛：新键树态可数、旧键严格消失
  assert_eq!(session.range_index_count(b"dst").await?, 2);
  assert!(!session.range_index_exists(b"src").await?);

  // 内核单独收敛即清退 dst 旧 etag（修复前：残留 Some(7) 破坏条件写基线）
  assert_eq!(
    session.etag_of(b"dst").await?,
    None,
    "段三必须清退 dst 旧 etag 旁路残留（C# 记录级重写 HasETag=false）"
  );
  // 旧键随键 etag 由段五排空臂清退；内核不迁移（C# HasETag=false）
  assert_eq!(
    session.etag_of(b"src").await?,
    None,
    "段五排空臂须成对清退旧键随键 etag，且内核不得自行迁移"
  );

  // 入账闭环：在场摘除恰发一次 EtagWrite(None)（dst 清退臂 + src 排空臂各一），
  // 缺席键零事件——Setwithetag(0) 条目随 AOF 回放侧 del_etag 同调
  let entries = log.lock().expect("事件日志锁")[base..].to_vec();
  let tombs: Vec<(Vec<u8>, Option<i64>)> = entries
    .iter()
    .filter(|(_, etag)| etag.is_none())
    .cloned()
    .collect();
  assert_eq!(
    tombs,
    vec![(b"dst".to_vec(), None), (b"src".to_vec(), None)],
    "恰两次 etag 墓碑入账：段三 dst 清退臂先于段五 src 排空臂，此外零 etag 事件"
  );
  assert!(
    entries.iter().all(|(_, etag)| etag.is_none()),
    "内核不得迁移 etag（不得出现携带 Some 值的 EtagWrite）"
  );
  OK
}

/// 缺席臂幂等：RENAME 到无 etag 旁路的键零写零入账（不产任何 etag 事件）
#[compio::test]
async fn rename_over_absent_etag_emits_no_entry() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "rename_etag_absent.db").await?;
  let log: EtagLog = Arc::new(Mutex::new(Vec::new()));
  assert!(
    store.set_event_sink(etag_recording_sink(Arc::clone(&log))),
    "sink 注入应成功"
  );
  let session = store.new_session()?;

  fill_ri(&session, b"src", &[b"f1"]).await?;
  session.upsert(b"dst", b"payload").await?;
  let base = log.lock().expect("事件日志锁").len();

  session.rename_range_index(b"src", b"dst").await?;

  assert_eq!(session.range_index_count(b"dst").await?, 1);
  assert_eq!(session.etag_of(b"dst").await?, None);
  assert!(
    log.lock().expect("事件日志锁")[base..].is_empty(),
    "旁路缺席须零写零入账（哈希探针初筛幂等清退）"
  );
  OK
}
