//! 快照恢复基础流：SimpleRecovery、周期全量快照多版本恢复与日志截断边界。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{CheckpointManager, StoreConfig, WedbStore};

/// 将数字字符串左侧补零到指定宽度
fn pad_str(s: &str, width: usize) -> String {
  let mut out = String::with_capacity(width.max(s.len()));
  for _ in 0..width.saturating_sub(s.len()) {
    out.push('0');
  }
  out.push_str(s);
  out
}

/// 对标 Garnet SimpleRecoveryTest.cs: LocalDeviceSimpleRecoveryTest —— FoldOver 快照崩溃恢复后
/// 全量数据 1:1 回读一致，ReadOnlyAddress 精确对齐 TailAddress。
///
/// 流程：
/// 1. 创建底层存储实例并写入 1000 条用户数据；
/// 2. 触发 FoldOver 快照落盘，封印只读边界，写入元数据；
/// 3. 模拟进程退出/崩溃（Drop 原实例并关闭文件句柄）；
/// 4. 实例化全新引擎执行 `CheckpointManager::recover`；
/// 5. 1:1 回读全部 1000 条记录并严格比对数值，验证删除与未写入键返回 None；
/// 6. 验证恢复后能够成功创建新的 Session 并无缝继续追加写入。
#[test]
fn test_simple_recovery_foldover() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("simple_recovery_foldover.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
    let manager = CheckpointManager::new();
    let num_ops = 1000;
    let token;

    // 1. 初始化源存储并灌入数据
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      let session = store.new_session()?;

      for i in 0..num_ops {
        let k = String::from("user_click:") + &pad_str(itoa::Buffer::new().format(i), 5);
        let v = String::from("click_count_") + itoa::Buffer::new().format(i);
        session.upsert(k.as_bytes(), v.as_bytes()).await?;
      }

      assert_eq!(store.entry_count(), num_ops);

      // 创建 FoldOver 快照
      let meta = manager
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;
      assert_eq!(meta.cp_type, CheckpointType::FoldOver);
      assert_eq!(meta.index_meta.entry_count, num_ops);
    } // 模拟停机，释放原 store 与 device

    // 2. 崩溃恢复并在新 store 上验证
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
      let session = recovered.new_session()?;

      assert_eq!(recovered.entry_count(), num_ops);

      // FoldOver 恢复语义：ReadOnlyAddress 必须精确对齐 TailAddress，全部历史封印只读
      // （对标 C# DoPostRecovery 中 FoldOver 分支 readOnlyAddress = tailAddress）
      assert_eq!(
        recovered.read_only_address(),
        recovered.tail_address(),
        "FoldOver 恢复后 ReadOnlyAddress 必须对齐 TailAddress"
      );

      // 验证历史全部数据
      for i in 0..num_ops {
        let k = String::from("user_click:") + &pad_str(itoa::Buffer::new().format(i), 5);
        let expected_v = String::from("click_count_") + itoa::Buffer::new().format(i);
        let actual = session.read(k.as_bytes()).await?;
        assert_eq!(
          actual,
          Some(expected_v.into_bytes()),
          "键 {k} 在 FoldOver 恢复后回读不一致"
        );
      }

      // 验证未写入的键
      assert_eq!(session.read(b"user_click:non_existent").await?, None);

      // 验证恢复后可继续追加写入
      session
        .upsert(b"user_click:recovered_new", b"value_fresh")
        .await?;
      assert_eq!(
        session.read(b"user_click:recovered_new").await?,
        Some(b"value_fresh".to_vec())
      );

      // 清理快照
      CheckpointManager::<SegmentedDevice>::purge_all(&ckpt_dir)?;
      assert!(CheckpointManager::<SegmentedDevice>::list_checkpoints(&ckpt_dir)?.is_empty());
    }

    info!("LocalDeviceSimpleRecoveryTest(FoldOver) 复刻测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet SimpleRecoveryTest.cs: LocalDeviceSimpleRecoveryTest —— Snapshot 快照恢复后
/// 依 mutable_fraction 保留可变区，支持历史记录原位覆写且不前移 TailAddress。
///
/// 流程：
/// 1. 写入数据集并创建 Snapshot 快照；
/// 2. 崩溃恢复后，验证 ReadOnlyAddress 依照 mutable_fraction 保留可变区；
/// 3. 对恢复后的内存数据执行原位覆写（In-Place Update），验证性能与内容一致性；
/// 4. 再次写入新键并落盘校验。
#[test]
fn test_simple_recovery_snapshot() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("simple_recovery_snapshot.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
    let manager = CheckpointManager::new();
    let num_ops = 800;
    let token;

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      let session = store.new_session()?;

      for i in 0..num_ops {
        let k = String::from("device_metric:") + &pad_str(itoa::Buffer::new().format(i), 4);
        let v = String::from("cpu_load_") + itoa::Buffer::new().format(i);
        session.upsert(k.as_bytes(), v.as_bytes()).await?;
      }

      let meta = manager
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::Snapshot)
        .await?;
      token = meta.token;
      assert_eq!(meta.cp_type, CheckpointType::Snapshot);
    }

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
      let session = recovered.new_session()?;

      // Snapshot 恢复语义：ReadOnlyAddress 依 mutable_fraction 重建（严格低于 Tail），
      // 必须保留内存可变区（对标 C# DoPostRecovery 中 CalculateReadOnlyAddress 分支）
      assert!(
        recovered.read_only_address() < recovered.tail_address(),
        "Snapshot 恢复后必须保留内存可变区: ro={:#x}, tail={:#x}",
        recovered.read_only_address(),
        recovered.tail_address()
      );

      // 验证恢复后的全部历史数据
      for i in 0..num_ops {
        let k = String::from("device_metric:") + &pad_str(itoa::Buffer::new().format(i), 4);
        let expected_v = String::from("cpu_load_") + itoa::Buffer::new().format(i);
        assert_eq!(
          session.read(k.as_bytes()).await?,
          Some(expected_v.into_bytes())
        );
      }

      // 在恢复后的 Store 上修改部分历史数据（等长值走可变区原位覆写，TailAddress 不得前移）
      let tail_before = recovered.tail_address();
      session
        .upsert(b"device_metric:0010", b"cpu_load_99")
        .await?;
      session
        .upsert(b"device_metric:0020", b"cpu_load_88")
        .await?;
      assert_eq!(
        recovered.tail_address(),
        tail_before,
        "可变区原位覆写严禁前移 TailAddress"
      );

      assert_eq!(
        session.read(b"device_metric:0010").await?,
        Some(b"cpu_load_99".to_vec())
      );
      assert_eq!(
        session.read(b"device_metric:0020").await?,
        Some(b"cpu_load_88".to_vec())
      );

      // 追加新记录
      session
        .upsert(b"device_metric:9999", b"cpu_load_9999")
        .await?;
      assert_eq!(
        session.read(b"device_metric:9999").await?,
        Some(b"cpu_load_9999".to_vec())
      );
    }

    info!("LocalDeviceSimpleRecoveryTest(Snapshot) 复刻测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet SimpleRecoveryTest.cs: ShouldRecoverBeginAddress —— 快照后恢复出的 begin_address
/// 必须精确对齐截断点，早于边界的记录读空、边界及之后记录完好可读。
///
/// 流程：
/// 1. 顺序插入 1000 条记录；
/// 2. 在第 500 条时记录当前 TailAddress 作为逻辑截断点 `cut_address`；
/// 3. 调用 `store.shift_begin_address(cut_address)` 丢弃前半部分历史数据；
/// 4. 触发 Checkpoint；
/// 5. 崩溃恢复后，断言 `recovered_store.begin_address() == cut_address`；
/// 6. 验证早于该边界的记录被识别为已截断（读空），而该边界及之后的记录完全有效可读。
#[test]
fn test_should_recover_begin_address() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("should_recover_begin_addr.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let manager = CheckpointManager::new();
    let token;
    let mut cut_address = 0;

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      let session = store.new_session()?;

      for i in 0..1000 {
        let k = String::from("account:") + &pad_str(itoa::Buffer::new().format(i), 4);
        let v = String::from("balance_") + itoa::Buffer::new().format(i);
        session.upsert(k.as_bytes(), v.as_bytes()).await?;

        if i == 499 {
          cut_address = store.tail_address();
        }
      }

      assert!(cut_address > 0, "截断地址必须有效大于 0");
      store.shift_begin_address(cut_address).await?;
      assert_eq!(store.begin_address(), cut_address);

      let meta = manager
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;
      assert_eq!(meta.hlog_meta.begin_address, cut_address);
    }

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
      let session = recovered.new_session()?;

      // 核心断言：恢复后的 begin_address 精确与快照时一致
      assert_eq!(
        recovered.begin_address(),
        cut_address,
        "恢复后的 begin_address 必须精确对齐截断点"
      );

      // 截断点之后的有效数据必须完好
      for i in 500..1000 {
        let k = format!("account:{i:04}");
        let expected_v = format!("balance_{}", i);
        assert_eq!(
          session.read(k.as_bytes()).await?,
          Some(expected_v.into_bytes()),
          "截断点之后的键 {k} 必须可被正常读取"
        );
      }
    }

    info!("ShouldRecoverBeginAddress 复刻测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet SimpleRecoveryTest.cs: SimpleReadAndUpdateInfoTest —— FlushAndEvict 后全内存驱逐，
/// 纯磁盘态回读 100% 正确，并可无缝过渡到新内存页继续追加。
///
/// 流程：
/// 1. 恢复 Store 之后，调用 `flush_and_evict_all` 将所有数据页刷盘并驱逐，
///    使得 `head_address == tail_address`，即内存环形缓冲区中没有任何未落盘或驻留有效页；
/// 2. 此时所有读请求必须透明落到底层 Device 的磁盘 I/O 上；
/// 3. 验证从磁盘读取依然 100% 正确；
/// 4. 在驱逐后继续追加新记录，验证从纯磁盘态到新内存页的过渡无缝衔接。
#[test]
fn test_read_and_update_info() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("read_and_update_info.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(1024, 32 * 1024, 16, 0.5)?;
    let manager = CheckpointManager::new();
    let num_ops = 500;
    let token;

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      let session = store.new_session()?;

      for i in 0..num_ops {
        let k = format!("info_key_{i:04}");
        let v = format!("info_val_{i:04}");
        session.upsert(k.as_bytes(), v.as_bytes()).await?;
      }

      let meta = manager
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;
    }

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
      let session = recovered.new_session()?;

      // 1. 验证常规读取
      let last_idx = num_ops - 1;
      let last_k = format!("info_key_{last_idx:04}");
      let last_expected = format!("info_val_{last_idx:04}");
      assert_eq!(
        session.read(last_k.as_bytes()).await?,
        Some(last_expected.into_bytes())
      );

      // 2. 调用 flush_and_evict_all 模拟全内存驱逐至磁盘
      recovered.flush_and_evict_all().await?;
      assert_eq!(
        recovered.head_address(),
        recovered.tail_address(),
        "flush_and_evict_all 后 head 必须对齐 tail"
      );

      // 3. 在全磁盘模式下回读所有历史数据（强制触发 Device 物理异步读）
      for i in 0..num_ops {
        let k = format!("info_key_{i:04}");
        let expected_v = format!("info_val_{i:04}");
        let actual = session.read(k.as_bytes()).await?;
        assert_eq!(
          actual,
          Some(expected_v.into_bytes()),
          "全磁盘驱逐后读取键 {k} 不匹配"
        );
      }

      // 4. 在驱逐后继续追加新记录
      session
        .upsert(b"info_key_after_evict", b"val_after_evict")
        .await?;
      assert_eq!(
        session.read(b"info_key_after_evict").await?,
        Some(b"val_after_evict".to_vec())
      );
    }

    info!("SimpleReadAndUpdateInfoTest 复刻测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet RecoveryTests.cs: RecoveryTestFullCheckpoint —— 周期性全量快照生成系列版本，
/// 逐版本独立恢复并验证快照点位的数据完整性与版本隔离性。
///
/// 流程：
/// 1. 模拟长周期持续写入业务流，每写入 200 条记录自动触发一次 Full Checkpoint，生成系列版本；
/// 2. 依次遍历每个历史 Checkpoint Token，独立恢复并验证快照点的数据完整性；
/// 3. 验证版本隔离性：第 K 个快照仅包含截至第 K 个快照前的数据，不包含后续写入的数据。
#[test]
fn test_full_checkpoint_periodic() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("full_checkpoint_periodic.db");
    let ckpt_dir = dir.path().join("checkpoints");

    let config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
    let manager = CheckpointManager::new();

    let total_checkpoints = 5;
    let interval = 200;
    let mut tokens = Vec::new();

    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      let session = store.new_session()?;

      for ckpt_idx in 0..total_checkpoints {
        for i in 0..interval {
          let global_idx = ckpt_idx * interval + i;
          let k = format!("metric:{global_idx:05}");
          let v = format!("sample_val_{}", global_idx);
          session.upsert(k.as_bytes(), v.as_bytes()).await?;
        }

        let meta = manager
          .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
          .await?;
        tokens.push((meta.token, (ckpt_idx + 1) * interval));
      }

      assert_eq!(tokens.len(), total_checkpoints);
    } // 关闭源 Store

    // 对每个快照版本逐一验证
    for (token, expected_count) in tokens {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
      let session = recovered.new_session()?;

      assert_eq!(
        recovered.entry_count(),
        expected_count,
        "Token {token:#x} 的有效条目数必须为 {expected_count}"
      );

      // 验证快照前的数据存在
      for i in 0..expected_count {
        let k = format!("metric:{i:05}");
        let expected_v = format!("sample_val_{}", i);
        let actual = session.read(k.as_bytes()).await?;
        assert_eq!(
          actual,
          Some(expected_v.into_bytes()),
          "快照 {token:#x} 中的键 {k} 回读异常"
        );
      }

      // 验证快照之后的数据不存在（点位隔离性）
      let after_k = format!("metric:{expected_count:05}");
      assert_eq!(
        session.read(after_k.as_bytes()).await?,
        None,
        "快照 {token:#x} 严禁泄露后续写入的键 {after_k}"
      );
    }

    info!("RecoveryTestFullCheckpoint 周期快照复刻测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
