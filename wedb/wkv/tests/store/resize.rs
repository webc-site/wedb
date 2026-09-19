//! 在线哈希索引动态扩容（Online Index Resize）专项集成测试

use std::{
  fs::create_dir_all,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
  },
  thread::{spawn, yield_now},
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use tempfile::tempdir;
use wbase::align::DEFAULT_SECTOR_SIZE;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use windex::{HashIndex, SPLIT_UNSTARTED, chunk_count};
use wkv::{StoreConfig, WedbStore, store::ResizePhase};

#[test]
fn test_online_index_grow_and_data_integrity() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("resize_test.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    // 初始 64 个桶
    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    // 写入 200 个不同的键值对
    for i in 0..200 {
      let key = format!("user:key:{i}");
      let val = format!("user:value:{i}");
      session.upsert(key.as_bytes(), val.as_bytes()).await?;
    }

    assert_eq!(store.active_index().size, 64);

    // 执行在线扩容：64 -> 128
    let grown = store.grow_index()?;
    assert!(grown, "grow_index 第一次翻倍应成功");
    assert_eq!(store.active_index().size, 128);

    // 验证所有已写入数据在扩容后完整无损
    for i in 0..200 {
      let key = format!("user:key:{i}");
      let expected_val = format!("user:value:{i}");
      let read_val = session.read(key.as_bytes()).await?;
      assert_eq!(read_val, Some(expected_val.into_bytes()));
    }

    // 扩容后继续写入 100 个新键并修改旧键
    for i in 200..300 {
      let key = format!("user:key:{i}");
      let val = format!("user:value:{i}");
      session.upsert(key.as_bytes(), val.as_bytes()).await?;
    }
    session.upsert(b"user:key:0", b"updated_val_0").await?;
    assert_eq!(
      session.read(b"user:key:0").await?,
      Some(b"updated_val_0".to_vec())
    );

    // 再次执行在线扩容：128 -> 256
    let grown2 = store.grow_index()?;
    assert!(grown2, "grow_index 第二次翻倍应成功");
    assert_eq!(store.active_index().size, 256);

    // 验证所有 300 个键的数据正确性
    assert_eq!(
      session.read(b"user:key:0").await?,
      Some(b"updated_val_0".to_vec())
    );
    for i in 1..300 {
      let key = format!("user:key:{i}");
      let expected_val = format!("user:value:{i}");
      let read_val = session.read(key.as_bytes()).await?;
      assert_eq!(read_val, Some(expected_val.into_bytes()));
    }

    OK
  })?;

  OK
}

#[test]
fn test_online_index_grow_checkpoint_recovery() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("resize_cpr.db");
    let cpr_dir = dir.path().join("checkpoints");
    create_dir_all(&cpr_dir)?;

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    // 初始 64 桶
    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
    let session = store.new_session()?;

    for i in 0..100 {
      let k = format!("cpr_k_{i}");
      let v = format!("cpr_v_{i}");
      session.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    // 扩容至 128
    store.grow_index()?;
    assert_eq!(store.active_index().size, 128);

    // 拍摄 CPR 检查点
    let meta = store
      .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
      .await?;
    let token = meta.token;

    // 从检查点恢复
    let recovered_store = Arc::new(WedbStore::recover(&cpr_dir, token, Arc::clone(&device)).await?);
    assert_eq!(
      recovered_store.active_index().size,
      128,
      "恢复后的索引表容量必须与快照时扩容后的新容量一致"
    );

    let rec_session = recovered_store.new_session()?;
    for i in 0..100 {
      let k = format!("cpr_k_{i}");
      let expected = format!("cpr_v_{i}");
      assert_eq!(
        rec_session.read(k.as_bytes()).await?,
        Some(expected.into_bytes())
      );
    }

    OK
  })?;

  OK
}

#[test]
fn test_concurrent_read_write_during_online_grow() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("resize_concurrent.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);

    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);

    let session1 = store.new_session()?;
    for i in 0..500 {
      let k = format!("base_key_{i}");
      let v = format!("base_val_{i}");
      session1.upsert(k.as_bytes(), v.as_bytes()).await?;
    }

    // 启动后台多会话并发读写与多次在线扩容
    let store_clone = Arc::clone(&store);
    let writer_task = spawn(move || -> aok::Result<()> {
      let rt_w = Runtime::new()?;
      rt_w.block_on(async {
        let s = store_clone.new_session()?;
        for i in 500..1000 {
          let k = format!("concurrent_k_{i}");
          let v = format!("concurrent_v_{i}");
          s.upsert(k.as_bytes(), v.as_bytes()).await?;
        }
        OK
      })?;
      OK
    });

    // 触发在线哈希扩容 64 -> 128 -> 256
    store.grow_index()?;
    store.grow_index()?;

    writer_task.join().unwrap()?;

    // 校验全部 1000 条记录在扩容后均可正常读取
    let verify_session = store.new_session()?;
    for i in 0..500 {
      let k = format!("base_key_{i}");
      let expected = format!("base_val_{i}");
      let val = verify_session.read(k.as_bytes()).await?;
      assert_eq!(val, Some(expected.into_bytes()));
    }
    for i in 500..1000 {
      let k = format!("concurrent_k_{i}");
      let expected = format!("concurrent_v_{i}");
      let val = verify_session.read(k.as_bytes()).await?;
      assert_eq!(val, Some(expected.into_bytes()));
    }

    assert_eq!(store.active_index().size, 256);

    OK
  })?;

  OK
}

/// 装配「相位已发布 InProgressGrow、活跃表尚未切换」的扩容中间态
///
/// 确定性装配（不依赖时序竞争）：发布 split_status / num_pending_chunks /
/// old_index 后置相位，活跃表保持旧表——精确复现 grow_index 中
/// `phase.store(InProgressGrow)` 与 `index.store(新表)` 之间的过渡窗。
fn stage_phase_published_index_unswitched(store: &WedbStore<SegmentedDevice>) {
  let old_index = store.active_index();
  let count = chunk_count(old_index.size);
  store.resize.split_status.store(Arc::new(
    (0..count)
      .map(|_| AtomicI64::new(SPLIT_UNSTARTED))
      .collect(),
  ));
  store
    .resize
    .num_pending_chunks
    .store(count, Ordering::Release);
  store.resize.old_index.store(Some(old_index));
  store
    .resize
    .phase
    .store(ResizePhase::InProgressGrow as u8, Ordering::Release);
}

/// 扩容态收尾回 REST（对齐 grow_index 完成清理）
fn teardown_resize_state(store: &WedbStore<SegmentedDevice>) {
  store.resize.old_index.store(None);
  store.resize.split_status.store(Arc::new(Vec::new()));
  store
    .resize
    .phase
    .store(ResizePhase::Rest as u8, Ordering::Release);
}

/// PREPARE_GROW 全事务屏障：相位停留 PrepareGrow 期间，新会话操作必须挂起；
/// 发布 InProgressGrow 后放行且数据完整（确定性：写线程 started 置位后仅剩
/// 同步路径到屏障，挂起窗口内若屏障失效 upsert 必然完成置位 done）
#[test]
fn test_prepare_grow_barrier_blocks_session_operations() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("barrier.db"))?);
    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let seed = store.new_session()?;
    seed.upsert(b"barrier_seed", b"seed_val").await?;

    // 独占进入 PrepareGrow：新会话操作自此必须挂起（对标 C# full barrier）
    store
      .resize
      .phase
      .compare_exchange(
        ResizePhase::Rest as u8,
        ResizePhase::PrepareGrow as u8,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .expect("无并发扩容时必须从 Rest 进入 PrepareGrow");

    let started = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let writer_store = Arc::clone(&store);
    let writer_started = Arc::clone(&started);
    let writer_done = Arc::clone(&done);
    let writer = spawn(move || -> aok::Result<()> {
      let rt_w = Runtime::new()?;
      rt_w.block_on(async {
        let s = writer_store.new_session()?;
        writer_started.store(true, Ordering::Release);
        s.upsert(b"barrier_key", b"barrier_val").await?;
        writer_done.store(true, Ordering::Release);
        OK
      })?;
      OK
    });

    while !started.load(Ordering::Acquire) {
      yield_now();
    }
    sleep(Duration::from_millis(50)).await;
    assert!(
      !done.load(Ordering::Acquire),
      "PrepareGrow 相位内会话操作必须被入口屏障挂起"
    );

    // 屏障期间完成排空（屏障挂起保证收敛）与新表构建，随后切表并发布相位：
    // 精确对齐 grow_index 的发布次序——先状态/old_index/新表、后相位
    let old_index = store.active_index();
    let count = chunk_count(old_index.size);
    store.resize.split_status.store(Arc::new(
      (0..count)
        .map(|_| AtomicI64::new(SPLIT_UNSTARTED))
        .collect(),
    ));
    store
      .resize
      .num_pending_chunks
      .store(count, Ordering::Release);
    store.resize.old_index.store(Some(Arc::clone(&old_index)));
    store
      .index
      .store(Arc::new(HashIndex::new(old_index.size * 2)?));
    store
      .resize
      .phase
      .store(ResizePhase::InProgressGrow as u8, Ordering::Release);

    let deadline = Instant::now() + Duration::from_secs(5);
    while !done.load(Ordering::Acquire) {
      assert!(
        Instant::now() < deadline,
        "屏障释放后挂起的会话操作必须完成"
      );
      yield_now();
    }
    writer.join().unwrap()?;

    // 数据完整性：种子键经协同迁移在新表可见，放行后的写入同样完整
    let verifier = store.new_session()?;
    assert_eq!(
      verifier.read(b"barrier_seed").await?,
      Some(b"seed_val".to_vec())
    );
    assert_eq!(
      verifier.read(b"barrier_key").await?,
      Some(b"barrier_val".to_vec())
    );

    teardown_resize_state(&store);
    OK
  })?;

  OK
}

/// 先置相位后切表的过渡窗：相位已发布而活跃表未切时，会话写入按旧表快照
/// 落地（同表防护不迁移不假推进），切表后随全量迁移整体搬移至新表
#[test]
fn test_phase_published_transition_window_writes_old_table_snapshot() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("transition.db"),
    )?);
    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);

    let old_index = store.active_index();
    stage_phase_published_index_unswitched(&store);
    assert!(store.is_growing(), "相位已发布即处于扩容态");

    let session = store.new_session()?;
    session.upsert(b"trans_key", b"trans_val").await?;

    // 同表防护：活跃表仍是迁移源自身，不得抢占分块也不得假标完成
    assert_eq!(
      store.resize.split_status.load()[0].load(Ordering::Acquire),
      SPLIT_UNSTARTED,
      "过渡窗内同表防护不得标记分块完成"
    );

    // 切表后手动全量迁移：过渡窗写入随旧表整体搬移至新表
    let new_index = Arc::new(HashIndex::new(old_index.size * 2)?);
    store.index.store(Arc::clone(&new_index));
    assert!(
      store.split_single_chunk(0, chunk_count(old_index.size), &old_index)?,
      "切表后分块迁移必须真实执行"
    );
    assert_eq!(store.resize.num_pending_chunks.load(Ordering::Acquire), 0);

    teardown_resize_state(&store);
    assert!(!store.is_growing());
    assert_eq!(
      session.read(b"trans_key").await?,
      Some(b"trans_val".to_vec()),
      "过渡窗写入必须随迁移在新表可见"
    );

    OK
  })?;

  OK
}

/// 分块分裂错误不得静默吞掉：溢出链自环触发 OverflowCycleDetected 时，
/// split_buckets 错误上抛、失败分块回滚 UNSTARTED 不递减待迁移计数，
/// grow_index 中止扩容、清理扩容态回 REST 并显式上抛
///
/// 环构造：空主桶挂载一个空溢出桶并令其自环——迁移遍历为纯步进，链遍历
/// 步数上限（MAX_CHAIN_STEPS）快速触达 Cycle 判定，无条目搬移放大。
#[test]
fn test_split_error_propagates_and_aborts_grow() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("cycle.db"))?);
    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);

    // 手工构造溢出链自环：主桶 0 -> 溢出桶（自环）。空桶纯步进让链遍历
    // 步数上限防御立即判定数据损坏，避免有条目环的搬移放大
    let cyclic_index = store.active_index();
    let ov_id = cyclic_index.overflow_pool.allocate()?;
    assert!(
      cyclic_index.bucket(0).set_overflow_index(ov_id),
      "空主桶必须能挂载溢出桶"
    );
    let ov = cyclic_index.overflow_pool.get(ov_id).expect("已分配溢出桶");
    assert!(ov.set_overflow_index(ov_id), "空溢出桶必须能自环");

    // 扩容中间态（已切表）下直接触发会话协同：错误必须上抛且分块状态回滚
    stage_phase_published_index_unswitched(&store);
    store
      .index
      .store(Arc::new(HashIndex::new(cyclic_index.size * 2)?));
    let hash = HashIndex::hash_key(b"cycle_probe");
    assert!(store.split_buckets(hash).is_err(), "环链错误必须上抛");
    assert_eq!(
      store.resize.split_status.load()[0].load(Ordering::Acquire),
      SPLIT_UNSTARTED,
      "失败分块必须回滚 UNSTARTED，绝不标记 SPLIT_COMPLETED"
    );
    assert_eq!(
      store.resize.num_pending_chunks.load(Ordering::Acquire),
      1,
      "失败分块不得递减待迁移计数"
    );
    teardown_resize_state(&store);

    // 回切带环表为活跃表，grow_index 真实路径：遇环中止扩容并显式上抛
    store.index.store(Arc::clone(&cyclic_index));
    assert!(
      store.grow_index().is_err(),
      "迁移遇环必须中止扩容并上抛错误"
    );
    assert!(!store.is_growing(), "中止后必须回到 REST 态");
    assert!(
      store.resize.old_index.load().is_none(),
      "中止后必须清理旧表句柄"
    );

    OK
  })?;

  OK
}
