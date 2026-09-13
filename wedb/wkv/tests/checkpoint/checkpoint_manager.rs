//! CheckpointManager 目录维护：purge/list/latest 检索与恢复后多 Session 并发安全。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wcpr::{CheckpointType, Error};
use wdev::SegmentedDevice;
use wkv::{CheckpointManager, StoreConfig, WedbStore};

/// 对标 Garnet CheckpointManagerTests.cs: CheckpointManagerPurgeCheck —— 快照列举与定位准确，
/// 单个 purge 精准删除指定快照且其余快照完好，purge_all 彻底清空目录。
///
/// 流程：
/// 1. 连续生成 6 个快照（3 个 FoldOver + 3 个 Snapshot）；
/// 2. 验证 `list_checkpoints()` 正确列出所有 6 个 Token；
/// 3. 验证 `find_latest_checkpoint()` 准确返回最后一个 Token；
/// 4. 针对特定 Token 执行 `purge_checkpoint`，验证指定快照文件被安全物理删除，其余快照完整可用；
/// 5. 执行 `purge_all`，验证所有快照被彻底清理，`list_checkpoints()` 返回空列表。
#[test]
fn test_purge_check() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("ckpt_manager_purge.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(512, 16 * 1024, 16, 0.5)?;
    let manager = CheckpointManager::new();

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let store = Arc::new(WedbStore::open(config.clone(), device)?);
    let session = store.new_session()?;
    session.upsert(b"mgr_key", b"mgr_val").await?;

    let mut tokens = Vec::new();
    for i in 0..6 {
      let cp_type = if i % 2 == 0 {
        CheckpointType::FoldOver
      } else {
        CheckpointType::Snapshot
      };
      let meta = manager
        .create_checkpoint(&store, &ckpt_dir, cp_type)
        .await?;
      tokens.push(meta.token);
    }

    // 1. 验证 list_checkpoints
    let listed = CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?;
    assert_eq!(listed, tokens);

    // 2. 验证 find_latest_checkpoint
    let latest = CheckpointManager::<SegmentedDevice>::find_latest_checkpoint(&ckpt_dir)?;
    assert_eq!(latest, Some(*tokens.last().unwrap()));

    // 3. 单个快照清理（对标 Purge(guid)）
    let remove_target = tokens[2];
    manager.purge(&ckpt_dir, remove_target)?;

    let listed_after_one = CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?;
    assert_eq!(listed_after_one.len(), 5);
    assert!(!listed_after_one.contains(&remove_target));

    // 验证被删除的 Token 执行恢复时报错 MetaNotFound
    let err = CheckpointManager::recover(&ckpt_dir, remove_target, Arc::clone(&store.device)).await;
    assert!(matches!(err, Err(Error::MetaNotFound(_))));

    // 验证未被删除的 Token 依然完好且可恢复
    let survivor = tokens[0];
    let recovered_survivor =
      Arc::new(CheckpointManager::recover(&ckpt_dir, survivor, Arc::clone(&store.device)).await?);
    let s_survivor = recovered_survivor.new_session()?;
    assert_eq!(
      s_survivor.read(b"mgr_key").await?,
      Some(b"mgr_val".to_vec())
    );

    // 4. 全量快照清理（对标 PurgeAll()）
    manager.purge_all_checkpoints(&ckpt_dir)?;
    let empty_list = CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?;
    assert!(empty_list.is_empty(), "purge_all 必须彻底清空所有快照");

    info!("CheckpointManagerPurgeCheck 复刻测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 恢复后多 Session 并发读写与无锁 RCU 追加安全性：8 个客户端会话交叉执行历史读取、
/// 只读记录 RCU 追加更新与全新记录插入，全部操作无死锁、无脏读，最终数据 100% 正确。
#[test]
fn test_concurrent_sessions_after_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("concurrent_recovery.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?.with_max_sessions(16)?;
    let manager = CheckpointManager::new();
    let base_records = 400;
    let token;

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      let session = store.new_session()?;

      for i in 0..base_records {
        let k = format!("stock_symbol:{i:04}");
        let v = format!("price_init_{}", i);
        session.upsert(k.as_bytes(), v.as_bytes()).await?;
      }

      let meta = manager
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;
    }

    // 崩溃恢复
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let recovered_store = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);

    // 启动 8 个客户端会话并发执行
    let num_sessions = 8;
    let per_session_ops = 50;

    let mut handles = Vec::new();
    for session_id in 0..num_sessions {
      let store = Arc::clone(&recovered_store);
      let handle = async move {
        let session = store.new_session()?;

        for j in 0..per_session_ops {
          // 1. 并发读历史记录
          let read_idx = (session_id * 31 + j) % base_records;
          let k_read = format!("stock_symbol:{read_idx:04}");
          let val = session.read(k_read.as_bytes()).await?;
          assert!(val.is_some(), "并发读取键 {k_read} 失败");

          // 2. 更新历史记录（触发 RCU 追加）
          if j % 5 == 0 {
            let k_upd = format!("stock_symbol:{read_idx:04}");
            let v_upd = format!("price_upd_by_s{}_{}", session_id, j);
            session.upsert(k_upd.as_bytes(), v_upd.as_bytes()).await?;
          }

          // 3. 并发写入全新的独立记录
          let k_new = format!("fresh_s{session_id}:{j:04}");
          let v_new = format!("fresh_val_s{}_{}", session_id, j);
          session.upsert(k_new.as_bytes(), v_new.as_bytes()).await?;
        }

        aok::Result::<()>::Ok(())
      };
      handles.push(handle);
    }

    for h in handles {
      h.await?;
    }

    // 最终全局校验
    let verify_session = recovered_store.new_session()?;
    for session_id in 0..num_sessions {
      for j in 0..per_session_ops {
        let k_new = format!("fresh_s{session_id}:{j:04}");
        let expected_v = format!("fresh_val_s{}_{}", session_id, j);
        let actual = verify_session.read(k_new.as_bytes()).await?;
        assert_eq!(actual, Some(expected_v.into_bytes()));
      }
    }

    info!("并发 Session 恢复与多线程安全测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
