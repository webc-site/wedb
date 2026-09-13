//! 边界条件：空存储快照恢复、尾页整页边界对齐与调用方处于纪元保护区时的免死锁。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wcpr::{CheckpointType, Error as CprError};
use wdev::SegmentedDevice;
use wkv::{CheckpointManager, StoreConfig, WedbStore};

/// 空存储引擎快照与恢复边界：从未写入数据的空 Store（tail == initial == 0x40）快照、
/// 恢复三区边界正确对齐，并向"空→有数据"二次快照恢复平滑过渡。
#[test]
fn test_empty_store_checkpoint_and_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("empty_store.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(512, 16 * 1024, 16, 0.5)?;
    let manager = CheckpointManager::new();

    let token;
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      assert_eq!(store.entry_count(), 0);
      assert_eq!(store.tail_address(), 0x40);

      let meta = manager
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;
      assert_eq!(meta.index_meta.entry_count, 0);
      assert_eq!(meta.hlog_meta.tail_address, 0x40);
    }

    // 从空快照恢复
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
      assert_eq!(recovered.entry_count(), 0);
      assert_eq!(recovered.tail_address(), 0x40);
      assert_eq!(recovered.begin_address(), 0x40);
      assert_eq!(recovered.head_address(), 0x40);

      let session = recovered.new_session()?;
      assert_eq!(session.read(b"non_exist").await?, None);

      // 写入新数据
      session.upsert(b"key1", b"val1").await?;
      assert_eq!(session.read(b"key1").await?, Some(b"val1".to_vec()));
      assert_eq!(recovered.entry_count(), 1);

      // 创建 Snapshot 二次快照
      let meta2 = manager
        .create_checkpoint(&recovered, &ckpt_dir, CheckpointType::Snapshot)
        .await?;
      let token2 = meta2.token;

      // 二次恢复
      let device2 = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let recovered2 = Arc::new(CheckpointManager::recover(&ckpt_dir, token2, device2).await?);
      let s2 = recovered2.new_session()?;
      assert_eq!(s2.read(b"key1").await?, Some(b"val1".to_vec()));
    }

    info!("空存储引擎快照与恢复边界测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 尾页正好跨满整页（offset == 0）边界对齐恢复：TailAddress 恰好对齐 PageSize 时
/// 恢复无需读取尾页预热，历史数据安全回读，新写入从新页第 0 偏移无缝追加。
#[test]
fn test_page_boundary_aligned_checkpoint_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("page_boundary.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let page_size = 4096;
    let config = StoreConfig::new(512, page_size, 16, 0.5)?;
    let manager = CheckpointManager::new();

    let token;
    let num_records = 63;

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      let session = store.new_session()?;

      // 初始地址为 0x40 (64B)。每个记录：Header(16B) + Key(16B) + Val(32B) = 64B。
      // 63 * 64B = 4032B。64B + 4032B = 4096B (精确对齐第 0 页尾部边界，进入第 1 页 offset=0)
      for i in 0..num_records {
        let k = format!("page_key_{i:07}");
        let v = format!("page_val_{i:07}_{}", "0".repeat(15));
        assert_eq!(k.len(), 16);
        assert_eq!(v.len(), 32);
        session.upsert_raw(k.as_bytes(), v.as_bytes()).await?;
      }

      assert_eq!(store.tail_address(), page_size as u64);

      let meta = manager
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;
      assert_eq!(meta.hlog_meta.tail_address, page_size as u64);
    }

    // 崩溃恢复
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
      let session = recovered.new_session()?;

      assert_eq!(recovered.tail_address(), page_size as u64);
      assert_eq!(recovered.head_address(), page_size as u64);

      // 回读全量历史数据（全部由底层磁盘驱动器提供）
      for i in 0..num_records {
        let k = format!("page_key_{i:07}");
        let expected_v = format!("page_val_{i:07}_{}", "0".repeat(15));
        let actual = session.read_raw(k.as_bytes()).await?;
        assert_eq!(
          actual,
          Some(expected_v.into_bytes()),
          "边界恢复后回读第 {i} 条记录失败"
        );
      }

      // 在新页（Page 1, offset 0 开始）追加新数据
      session
        .upsert_raw(b"new_page_item_01", b"new_page_item_01_val")
        .await?;
      assert_eq!(
        session.read_raw(b"new_page_item_01").await?,
        Some(b"new_page_item_01_val".to_vec())
      );

      // 修改历史只读数据（触发 RCU 追加至新页）
      let upd_k = format!("page_key_{:07}", 0);
      session.upsert(upd_k.as_bytes(), b"modified_val_0").await?;
      assert_eq!(
        session.read(upd_k.as_bytes()).await?,
        Some(b"modified_val_0".to_vec())
      );
    }

    info!("尾页整页边界对齐恢复测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 调用方处于纪元保护区时 fail-fast 拒绝：自持纪元使排空屏障永远无法完成，静默跳过
/// 将打开「数据页已刷盘但索引插入尚未提交」的丢失更新窗口。契约升级后
/// CheckpointManager 以 `CheckpointWhileEpochProtected` 类型化错误拒绝保护区内的发起
/// （零副作用、零残留可恢复视图），退出保护区后同一调用正常完成且崩溃恢复 100% 正确。
#[test]
fn test_checkpoint_under_epoch_protected_caller() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("epoch_protected_ckpt.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(512, 16 * 1024, 16, 0.5)?;
    let manager = CheckpointManager::new();
    let token;

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      let session = store.new_session()?;

      for i in 0..50 {
        let k = format!("prot_key:{i:03}");
        let v = format!("prot_val:{i:03}");
        session.upsert(k.as_bytes(), v.as_bytes()).await?;
      }

      // 人为让当前线程进入纪元保护区
      store.epoch.resume();
      assert!(
        store.epoch.this_instance_protected(),
        "当前调用方必须处于纪元保护状态"
      );

      // 在保护区内部发起 Checkpoint：必须 fail-fast 拒绝（类型化错误，零残留）
      let err = manager
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
        .await
        .expect_err("保护区内的 Checkpoint 发起必须被拒绝");
      assert!(
        matches!(err, CprError::CheckpointWhileEpochProtected),
        "必须返回 CheckpointWhileEpochProtected 类型化错误"
      );
      assert!(
        !ckpt_dir.exists()
          || CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?.is_empty(),
        "被拒绝的 Checkpoint 绝不能留下可恢复视图"
      );
      store.epoch.suspend();

      // 退出保护区后同一调用正常完成
      let meta = manager
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;
      assert_eq!(meta.index_meta.entry_count, 50);
    }

    // 崩溃恢复验证
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
      let session = recovered.new_session()?;
      assert_eq!(recovered.entry_count(), 50);

      for i in 0..50 {
        let k = format!("prot_key:{i:03}");
        let expected_v = format!("prot_val:{i:03}");
        assert_eq!(
          session.read(k.as_bytes()).await?,
          Some(expected_v.into_bytes())
        );
      }
    }

    info!("调用方处于纪元保护区 fail-fast 契约测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
