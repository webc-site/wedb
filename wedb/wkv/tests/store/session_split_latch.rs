//! 扩容迁移窗口会话读写面回归（分裂协同先于探针/加闩铁律）
//!
//! 确定性装配 IN_PROGRESS_GROW 迁移窗（support::stage_resize），驱动真实
//! RMW 窗口、同步读、冷删除与旁路探针路径，锁定票面五缺陷点不变式：
//! 无闩位碰撞（加闩只落在 SPLIT_COMPLETED 桶）、无盲写覆盖（窗口内旧值可见）、
//! 无幽灵读未命中（探针不落未迁移新桶）、冷删除百分之百生效、旁路探针
//!（TTL/ETag/信封/元记录）扩容期不漏检。
//!
//! 自研依据: windex 增量分裂闩（C# 分裂语义参照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitBlocks）

use std::sync::atomic::Ordering;

use aok::Void;
use wbase::align::DEFAULT_SECTOR_SIZE;
use windex::SPLIT_COMPLETED;
use wkv::{StoreResult, store::ResizePhase};
use wval::KeyTag;

use crate::support::{
  config, finish_resize_window, open_store, split_status_of, split_status_of_hash, stage_resize,
};

/// RMW 原子窗口取闩前必完成本键分块协同（对标 InternalRMW.cs:67-72：
/// SplitBuckets 严格先于 FindOrCreateTagAndTryEphemeralXLock），窗口内旧值
/// 可见、读改写不回退覆盖
#[compio::test]
async fn rmw_window_splits_before_latch() -> Void {
  let env = open_store("rmw_window_split", config(64, DEFAULT_SECTOR_SIZE, 16)?)?;
  let store = env.store;
  let session = store.new_session()?;
  let key = b"ctr";
  session.upsert(key, b"1").await?;
  let rec_k = session.session_string_key(key);
  // 窗口用户键寻桶 scoped 口径（会话物理前缀种子，与窗口单点同构）
  let user_hash = whasher::scoped_hash(session.session_prefix().as_slice(), key);
  // 用户键与记录键异桶（两基并存结构，持闩期内层读写必落他桶，杜绝自锁互斥假阳性）
  assert_ne!(
    store.active_index().bucket_index_for_hash(user_hash),
    store.active_index().bucket_index_for_key(&rec_k)
  );

  stage_resize(&store, true, ResizePhase::InProgressGrow);
  let batch = session.enter_batch();
  let window = batch.try_rmw_window(key).expect("迁移窗口内取窗");
  // 铁律断言：取闩前目标分块必已协同迁移完成（协同与闩同 hash 单源），
  // 闩只落在 SPLIT_COMPLETED 桶
  assert_eq!(split_status_of_hash(&store, user_hash), SPLIT_COMPLETED);
  assert!(
    store.active_index().find_tag(&rec_k).is_some(),
    "协同后旧值记录必在新表可见（防窗口内查空盲写）"
  );
  // 窗口内读旧值 → 算新值 → 写回：旧值必须可见，杜绝计数回退式盲写覆盖
  assert!(matches!(
    batch.try_read_sync(key, |v| v.to_vec())?,
    StoreResult::Success(old) if &old == b"1"
  ));
  assert!(window.try_rmw_sync(b"2")?.is_ok());
  drop(window);
  drop(batch);
  finish_resize_window(&store);
  assert_eq!(session.read(key).await?, Some(b"2".to_vec()));
  Ok(())
}

/// 异步让核臂同款：入口先行协同，迁移错误显式上抛通道不破
#[compio::test]
async fn rmw_window_async_splits_before_latch() -> Void {
  let env = open_store(
    "rmw_window_split_async",
    config(64, DEFAULT_SECTOR_SIZE, 16)?,
  )?;
  let store = env.store;
  let session = store.new_session()?;
  let key = b"ctr_async";
  session.upsert(key, b"1").await?;
  let user_hash = whasher::scoped_hash(session.session_prefix().as_slice(), key);
  stage_resize(&store, true, ResizePhase::InProgressGrow);
  let batch = session.enter_batch();
  let window = batch.rmw_window(key).await?;
  assert_eq!(split_status_of_hash(&store, user_hash), SPLIT_COMPLETED);
  drop(window);
  drop(batch);
  finish_resize_window(&store);
  assert_eq!(session.read(key).await?, Some(b"1".to_vec()));
  Ok(())
}

/// 幽灵读回归：探针采样与读内核之间的跨阶段 TOCTOU 窗（growing 期采得未迁移
/// 新桶 None、随后扩容收尾翻 Rest、内核不再重探而直采陈旧 None）
///
/// 留钩仅在「采样缺席且 growing」的间隙内回调，于钩内确定性完成全量迁移并
/// 翻回 Rest。修复后探针必先协同，采样永不落未迁移空桶：钩子全程未触发且
/// 读必命中
#[compio::test]
async fn read_probe_no_stale_none_after_phase_flip() -> Void {
  let env = open_store("read_probe_ghost", config(64, DEFAULT_SECTOR_SIZE, 16)?)?;
  let store = env.store;
  let session = store.new_session()?;
  let key = b"ghost";
  session.upsert(key, b"payload").await?;
  stage_resize(&store, true, ResizePhase::InProgressGrow);
  let store_hook = store.clone();
  store
    .test_read_gap_hook
    .lock()
    .replace(Box::new(move || finish_resize_window(&store_hook)));
  // 修复形态：read_probe 协同先行，采样命中已迁移桶，钩子不触发
  assert!(matches!(
    session.try_read_sync(key, |v| v.to_vec())?,
    StoreResult::Success(v) if v == b"payload"
  ));
  assert!(
    store.test_read_gap_hook.lock().is_some(),
    "协同先行的探针在 growing 期绝不采得缺席（陈旧 None 直采即幽灵读）"
  );
  store.test_read_gap_hook.lock().take();
  finish_resize_window(&store);
  assert_eq!(session.read(key).await?, Some(b"payload".to_vec()));
  Ok(())
}

/// 扩容迁移窗内冷数据删除百分之百生效（copy-to-tail 内核候选收集前协同，
/// 杜绝未迁移新表假 Miss 吞删）
#[compio::test]
async fn cold_delete_during_grow_window_effective() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store("cold_delete_grow", config(64, page_size, 16)?)?;
  let store = env.store;
  let session = store.new_session()?;
  let cold_k = b"cold:to_delete";
  session.upsert(cold_k, b"cold_value").await?;
  let padding = vec![b'P'; 3900];
  session.upsert(b"padding", &padding).await?;
  store.flush_all().await?;
  store.shift_read_only_address(page_size as u64);
  store.shift_head_address(page_size as u64);

  stage_resize(&store, true, ResizePhase::InProgressGrow);
  assert!(session.delete(cold_k).await?, "迁移窗内冷删除禁被吞");
  finish_resize_window(&store);
  assert_eq!(session.read(cold_k).await?, None);
  Ok(())
}

/// 迁移窗内 SET 覆写清退对象信封旁域（meta_k/env_k 裸探旁路统一协同前置，
/// 杜绝信封幽灵记录令覆写键仍可被集合命令读到）
#[compio::test]
async fn set_overwrite_clears_envelope_during_grow_window() -> Void {
  let env = open_store("env_ghost_grow", config(64, DEFAULT_SECTOR_SIZE, 16)?)?;
  let store = env.store;
  let session = store.new_session()?;
  let key = b"typed";
  session
    .upsert_tag(key, KeyTag::ObjectEnvelope, b"stub")
    .await?;

  stage_resize(&store, true, ResizePhase::InProgressGrow);
  let batch = session.enter_batch();
  assert!(
    batch.try_upsert_sync(key, b"fresh")?.is_ok(),
    "同步快路径写回闭环（探针协同后信封在场走同步清退）"
  );
  assert_eq!(
    split_status_of(
      &store,
      &session.session_tag_key(KeyTag::ObjectEnvelope, key)
    ),
    SPLIT_COMPLETED
  );
  drop(batch);
  finish_resize_window(&store);
  assert_eq!(
    session
      .read_tag_with(key, KeyTag::ObjectEnvelope, |v| v.to_vec())
      .await?,
    None,
    "SET 覆写必清信封旁域（漏检即信封幽灵存活）"
  );
  assert_eq!(session.read(key).await?, Some(b"fresh".to_vec()));
  Ok(())
}

/// 迁移窗内 DEL 级联清 ETag 旁路记录（has_etag 探针收敛统一协同机制，
/// 杜绝扩容期漏检致 ETag 幽灵残留）
#[compio::test]
async fn del_cascades_etag_during_grow_window() -> Void {
  let env = open_store("etag_ghost_grow", config(64, DEFAULT_SECTOR_SIZE, 16)?)?;
  let store = env.store;
  let session = store.new_session()?;
  let key = b"withetag";
  session.upsert(key, b"v").await?;
  session.put_etag(key, 42).await?;

  stage_resize(&store, true, ResizePhase::InProgressGrow);
  let batch = session.enter_batch();
  assert!(matches!(batch.try_delete_sync(key)?, Ok(true)));
  assert_eq!(
    split_status_of(&store, &session.etag_key(key)),
    SPLIT_COMPLETED
  );
  drop(batch);
  finish_resize_window(&store);
  assert_eq!(
    session.etag_of(key).await?,
    None,
    "DEL 级联必清 ETag（旁路探针漏检即残影复活旧 etag）"
  );
  assert_eq!(session.read(key).await?, None);
  Ok(())
}

/// 迁移窗内 TTL 探针走全仓统一 is_growing 契约（废除私有 ptr_eq 陈旧判定后，
/// 探针前先协同、协同后重采样最新活跃索引）
#[compio::test]
async fn ttl_probe_unified_contract_during_grow_window() -> Void {
  let env = open_store("ttl_probe_grow", config(64, DEFAULT_SECTOR_SIZE, 16)?)?;
  let store = env.store;
  let session = store.new_session()?;
  let key = b"ttled";
  session.upsert(key, b"v").await?;
  session.put_ttl(key, 1 << 40).await?;
  let ttl_k = session.ttl_key(key);

  stage_resize(&store, true, ResizePhase::InProgressGrow);
  assert!(session.has_ttl_tag(key)?, "迁移窗内 TTL 在场禁漏检");
  assert_eq!(split_status_of(&store, &ttl_k), SPLIT_COMPLETED);
  finish_resize_window(&store);
  assert_eq!(session.ttl_of(key).await?, Some(1 << 40));
  // Rest 相位零协同直读活跃表（is_growing 契约下探针无分块状态残留）
  assert_eq!(
    store.resize.phase.load(Ordering::Acquire),
    ResizePhase::Rest as u8
  );
  assert!(session.has_ttl_tag(key)?);
  Ok(())
}
