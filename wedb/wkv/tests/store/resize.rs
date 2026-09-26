//! 在线哈希索引动态扩容（Online Index Resize）专项集成测试
//!
//! 自研依据: 增量索引在线扩容（C# 对应面 test/standalone/Garnet.test.extensions/IndexGrowthTests.cs 的索引增长语义）

use std::{
  fs::create_dir_all,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
    mpsc,
  },
  thread,
  thread::spawn,
  time::Duration,
};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use tempfile::tempdir;
use wbase::align::DEFAULT_SECTOR_SIZE;
use wcpr::{CheckpointType, CprStore, Error};
use wdev::SegmentedDevice;
use windex::{
  Error as WindexError, HashIndex, SPLIT_IN_PROGRESS, SPLIT_UNSTARTED, chunk_count,
  chunk_offset_for_hash,
};
use wkv::{
  Error as WkvError, StoreConfig, WedbStore,
  store::{ResizePhase, grow_index_blocking},
};

#[compio::test]
async fn test_online_index_grow_and_data_integrity() -> Void {
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
}

#[compio::test]
async fn test_online_index_grow_checkpoint_recovery() -> Void {
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

/// 扩容态收尾回 REST（对齐 grow_index 完成清理：先相位收口、后资源回收）
fn teardown_resize_state(store: &WedbStore<SegmentedDevice>) {
  store
    .resize
    .phase
    .store(ResizePhase::Rest as u8, Ordering::Release);
  store.resize.old_index.store(None);
  store.resize.split_status.store(Arc::new(Vec::new()));
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

    wtest_base::wait_yield_sync(
      || started.load(Ordering::Acquire),
      Duration::from_secs(5),
      "写线程必须启动",
    );
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

    wtest_base::wait_yield_sync(
      || done.load(Ordering::Acquire),
      Duration::from_secs(5),
      "屏障释放后挂起的会话操作必须完成",
    );
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

/// 分块分裂错误不得静默吞掉：溢出链自环触发 OverflowCycleDetected 时，
/// split_buckets 错误上抛、失败分块回滚 UNSTARTED 不递减待迁移计数，
/// grow_index 中止扩容、清理扩容态回 REST 并显式上抛
///
/// 环构造：空主桶挂载一个空溢出桶并令其自环——迁移遍历为纯步进，链遍历
/// 步数上限（MAX_CHAIN_STEPS）快速触达 Cycle 判定，无条目搬移放大。
#[compio::test]
async fn test_split_error_propagates_and_aborts_grow() -> Void {
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

  // 扩容中间态（严格状态机装配：split_status / old_index 就绪 → 先切表、
  // 后发相位，对齐 grow_index 发布次序）下直接触发会话协同：错误必须上抛
  // 且分块状态回滚
  let count = chunk_count(cyclic_index.size);
  store.resize.split_status.store(Arc::new(
    (0..count)
      .map(|_| AtomicI64::new(SPLIT_UNSTARTED))
      .collect(),
  ));
  store
    .resize
    .num_pending_chunks
    .store(count, Ordering::Release);
  store
    .resize
    .old_index
    .store(Some(Arc::clone(&cyclic_index)));
  store
    .index
    .store(Arc::new(HashIndex::new(cyclic_index.size * 2)?));
  store
    .resize
    .phase
    .store(ResizePhase::InProgressGrow as u8, Ordering::Release);
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
}

/// 验证 Truncate 推进 begin_address 后触发在线扩容：分裂迁移不将物理截断死条目
/// 双写进新表子桶（条目过滤），也不把低于 begin 的死回溯锚点插回对侧子桶
/// （trace_back 锚点过滤）
///
/// 确定性装配（不依赖时序竞态）：
/// 1. anchor 键先落小值（地址 A）→ dead 键落盘（地址 C > A）→ anchor 再落大值
///    （体积不符必走 RCU 追加，地址 B > C，索引 CAS A→B，记录 prev 链指向 A）；
/// 2. `shift_begin_address(B)`：begin 与 head 同步落 B（补刷先行，head 钳制到
///    已刷前缀后恰等于 B）——A、C 双双低于 begin 成物理死地址，B 处于内存可变区；
/// 3. grow_index：C 对应的索引条目被条目级 begin 门跳过；B 可解析走单侧插入，
///    其 prev 锚点 A 被 trace_back 的 begin 门拒收。
///
/// 证伪口径（修复前必红）：无门控时 C 按「冷磁盘记录」双写左右子桶、A 经
/// trace_back 兜底分支插回对侧子桶，`contains(C)` / `contains(A)` 即变红。
/// 本过滤为登记在案的分裂期防膨胀有意偏差（doc/zh/deviations.md「哈希分裂期按
/// begin 过滤死条目」）：C# SplitChunk 只门 HeadAddress、不做此过滤
#[compio::test]
async fn test_grow_skips_truncated_entries_and_dead_trace_back_anchor() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("resize_trunc.db"),
  )?);
  let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;

  // 索引槽位以物理键（含会话 ns/db 前缀）哈希落桶，探针必须同口径
  let phys_anchor = session.session_string_key(b"trunc_anchor");
  let phys_dead = session.session_string_key(b"trunc_dead");
  let big_val = vec![b'v'; 400];
  session.upsert(b"trunc_anchor", b"tiny_v1").await?;
  let anchor_v1_addr = store
    .active_index()
    .find_tag(phys_anchor.as_slice())
    .expect("首版条目已落索引");
  session.upsert(b"trunc_dead", b"dead_val").await?;
  let dead_addr = store
    .active_index()
    .find_tag(phys_dead.as_slice())
    .expect("dead 条目已落索引");
  assert!(anchor_v1_addr < dead_addr, "夹具地址单调前提");

  // 体积不符必走 RCU 追加：新址 B 严格高于 dead，prev 链锚回 A
  session.upsert(b"trunc_anchor", &big_val).await?;
  let anchor_v2_addr = store
    .active_index()
    .find_tag(phys_anchor.as_slice())
    .expect("RCU 换址后必须命中新版条目");
  assert!(
    anchor_v2_addr > dead_addr && anchor_v2_addr != anchor_v1_addr,
    "夹具：anchor 新版必须严格追加于链尾"
  );

  // 截断推进：begin=head=B，A、C 成物理死地址，B 留在内存可变区
  store.shift_begin_address(anchor_v2_addr).await?;
  assert_eq!(store.begin_address(), anchor_v2_addr);
  assert_eq!(store.head_address(), anchor_v2_addr);

  // 触发扩容迁移：64 → 128
  assert!(store.grow_index()?);
  let new_index = store.active_index();
  assert_eq!(new_index.size, 128);

  // 1. 条目过滤：索引仍指向死地址 C 的 dead 键不得迁进任何子桶
  let dead_cands = new_index.lookup_candidates(phys_dead.as_slice());
  assert!(
    !dead_cands.contains(dead_addr),
    "低于 begin 的死条目 {dead_addr} 严禁双写挤占新表槽位"
  );
  assert!(dead_cands.is_empty(), "死键在新表无任何候选残留");

  // 2. 锚点过滤：可解析活条目 B 单侧迁入，其低于 begin 的死锚点 A 不得插回对侧
  let anchor_cands = new_index.lookup_candidates(phys_anchor.as_slice());
  assert!(
    anchor_cands.contains(anchor_v2_addr),
    "活条目 {anchor_v2_addr} 必须迁入所属子桶"
  );
  assert!(
    !anchor_cands.contains(anchor_v1_addr),
    "死回溯锚点 {anchor_v1_addr} 严禁经 trace_back 兜底分支插回对侧子桶"
  );

  // 3. 防膨胀守恒：3 个键的迁移绝不触发新表溢出桶分配
  assert_eq!(
    new_index.overflow_pool.allocated_count(),
    0,
    "过滤死条目后不得有溢出桶伪分配"
  );

  // 4. 读路径闭环：活键读回最新版，死键 NOTFOUND（未借死索引条目复活）
  assert_eq!(
    session.read(b"trunc_anchor").await?,
    Some(big_val),
    "扩容后活键必须完好可读"
  );
  assert_eq!(
    session.read(b"trunc_dead").await?,
    None,
    "被截断的死键不得因分裂双写复活"
  );
  OK
}

/// 检查点临界期单槽互斥（扩容侧）：进入 Checkpoint 相位后 grow_index 必须被拒
/// 返回 Ok(false) 且不切表，重复进入检查点槽位亦被拒；退出复位后扩容正常执行
/// （对标 C# Tsavorite.cs:857 `GrowIndexAsync` 与 `TryInitiateFullCheckpoint`
/// 抢占同一 StateMachineDriver 槽位、被占即返回 false 的双向互斥）
#[compio::test]
async fn test_grow_index_rejected_during_checkpoint_slot() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("slot_grow.db"),
  )?);
  let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;
  for i in 0..50 {
    session
      .upsert(format!("slot_k_{i}").as_bytes(), b"slot_val")
      .await?;
  }

  // 真实宿主端口进入检查点临界区：CAS Rest -> Checkpoint
  store
    .enter_checkpoint()
    .expect("REST 态进入检查点临界区必须成功");
  assert_eq!(store.resize.phase(), ResizePhase::Checkpoint);

  // 扩容被拒：Rest→PrepareGrow CAS 天然失败，Ok(false) 即时返回，不切表
  assert!(!store.grow_index()?, "检查点临界期扩容必须被拒并返回 false");
  assert_eq!(store.active_index().size, 64, "被拒扩容不得切表");

  // 槽位被占：重复进入检查点临界区同样被拒
  assert!(
    store.enter_checkpoint().is_err(),
    "检查点槽位被占时二次进入必须失败"
  );

  // 退出复位：扩容恢复正常且数据完好
  store.exit_checkpoint();
  assert_eq!(store.resize.phase(), ResizePhase::Rest);
  assert!(store.grow_index()?, "检查点退出后扩容必须正常执行");
  assert_eq!(store.active_index().size, 128);
  assert_eq!(
    session.read(b"slot_k_0").await?,
    Some(b"slot_val".to_vec()),
    "临界期拒绝与复位后扩容均不得影响数据"
  );
  OK
}

/// 双重退出竞态回归（对标 C# StateMachineDriver.cs:164-173 空槽 CAS 抢占与
/// :345-362 单次清槽契约——清槽者只清自身持有的槽位，绝不踢出后来持有者）：
/// 成功路径显式 exit 复位 Rest 后、`CkptPhaseGuard` Drop 兜底二次 exit 前，
/// 并发 grow_index 经与 resize.rs 入口同款的 Rest→PrepareGrow CAS 抢占槽位，
/// 二次复位必须 no-op——旧无条件 store(Rest) 会把 PrepareGrow 打回 Rest 而
/// 扩容状态机仍在运行，`try_acquire_txn`/`barrier_enter` 的 PrepareGrow 拦截
/// 判据失效，事务钉旧表跨切表即丢写；两臂同测：phase 仍为 Checkpoint 时
/// Drop 复位正常生效，InProgressGrow 亦不被覆盖
#[compio::test]
async fn test_exit_checkpoint_double_reset_preserves_concurrent_grow_phase() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("dbl_reset.db"),
  )?);
  let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = WedbStore::open(config, device)?;

  // 临界区持有：CAS Rest→Checkpoint 成功
  store
    .enter_checkpoint()
    .expect("REST 态进入检查点临界区必须成功");
  assert_eq!(store.resize.phase(), ResizePhase::Checkpoint);

  // 成功路径显式退出复位 Rest（对位 create.rs 发布后 exit_checkpoint）
  store.exit_checkpoint();
  assert_eq!(
    store.resize.phase(),
    ResizePhase::Rest,
    "Checkpoint 相位下退出必须复位 Rest"
  );

  // 显式退出后、Drop 兜底前：并发 grow 抢占槽位（同款 Rest→PrepareGrow SeqCst CAS）
  assert!(
    store
      .resize
      .phase
      .compare_exchange(
        ResizePhase::Rest as u8,
        ResizePhase::PrepareGrow as u8,
        Ordering::SeqCst,
        Ordering::SeqCst,
      )
      .is_ok(),
    "Rest 态扩容抢占必须成功"
  );

  // Drop 兜底二次 exit：对扩容相位 no-op，不得打回 Rest
  store.exit_checkpoint();
  assert_eq!(
    store.resize.phase(),
    ResizePhase::PrepareGrow,
    "二次复位必须 no-op，保留并发 grow 的 PrepareGrow（清槽者只清自身持有的槽）"
  );
  assert!(
    store.is_growing(),
    "被打回 Rest 即屏障判据失效态，扩容态必须对外持续可见"
  );

  // InProgressGrow 窗（先切表后发相位之后）同样不被二次复位覆盖
  store
    .resize
    .phase
    .store(ResizePhase::InProgressGrow as u8, Ordering::SeqCst);
  store.exit_checkpoint();
  assert_eq!(
    store.resize.phase(),
    ResizePhase::InProgressGrow,
    "InProgressGrow 亦须保持不被打回 Rest"
  );
  // 扩容收尾发布 Rest（对位 grow_index 终态）
  store
    .resize
    .phase
    .store(ResizePhase::Rest as u8, Ordering::SeqCst);

  // 另一臂：Drop 路径 phase 仍为 Checkpoint 时正常复位（失败清场兜底语义）
  store
    .enter_checkpoint()
    .expect("槽位归还后再次进入必须成功");
  store.exit_checkpoint();
  assert_eq!(
    store.resize.phase(),
    ResizePhase::Rest,
    "phase 为 Checkpoint 时复位必须生效"
  );
  assert!(store.grow_index()?, "复位后扩容恢复正常（单槽契约闭环）");
  assert_eq!(store.active_index().size, 128);
  OK
}

/// 双向互斥（检查点侧）：PrepareGrow 准备窗（is_growing 扩义）与 Checkpoint
/// 临界期（闸门内 enter CAS）发起检查点都必须即时拒绝且零副作用；复位后正常
/// 创建，元数据 index_meta.size 与 store_meta.index_size 严格相等，重启恢复
/// 顺利通过（对标 C# IndexRecovery.cs 按快照 table_size 一致重建的前提）
#[compio::test]
async fn test_checkpoint_refused_in_prepare_grow_and_slot() -> Void {
  let dir = tempdir()?;
  let cpr_dir = dir.path().join("checkpoints");
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("dual_gate.db"),
  )?);
  let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
  let session = store.new_session()?;
  for i in 0..30 {
    session
      .upsert(format!("gate_k_{i}").as_bytes(), b"gate_val")
      .await?;
  }

  // PrepareGrow 准备窗：is_growing 扩义后对外可见，检查点入口拒绝且零副作用
  store
    .resize
    .phase
    .store(ResizePhase::PrepareGrow as u8, Ordering::Release);
  assert!(store.is_growing(), "PrepareGrow 准备期必须对外可见为扩容态");
  let err = store
    .create_checkpoint(&cpr_dir, CheckpointType::FoldOver)
    .await
    .expect_err("PrepareGrow 准备期发起检查点必须被拒绝");
  assert!(matches!(err, Error::Host(_)), "必须返回 Host 错误: {err}");
  assert!(!cpr_dir.exists(), "拒绝路径零副作用，不得创建检查点目录");
  store
    .resize
    .phase
    .store(ResizePhase::Rest as u8, Ordering::Release);

  // Checkpoint 临界期：闸门前 is_growing 放行，闸门内 enter CAS 失败拒绝
  store
    .enter_checkpoint()
    .expect("REST 态进入检查点临界区必须成功");
  let err = store
    .create_checkpoint(&cpr_dir, CheckpointType::FoldOver)
    .await
    .expect_err("检查点临界期发起检查点必须被拒绝");
  assert!(matches!(err, Error::Host(_)), "必须返回 Host 错误: {err}");
  assert!(!cpr_dir.exists(), "拒绝路径零副作用，不得创建检查点目录");

  // 复位后正常创建：元数据容量严格相等（撕裂修复的不变式）
  store.exit_checkpoint();
  assert_eq!(store.resize.phase(), ResizePhase::Rest);
  let meta = store
    .create_checkpoint(&cpr_dir, CheckpointType::FoldOver)
    .await?;
  assert_eq!(
    meta.index_meta.size, meta.store_meta.index_size,
    "索引快照容量与存储元数据容量必须严格相等"
  );
  assert_eq!(meta.store_meta.index_size, 64);

  // 重启恢复顺利通过
  let recovered = Arc::new(WedbStore::recover(&cpr_dir, meta.token, device).await?);
  assert_eq!(recovered.active_index().size, 64);
  let rec_session = recovered.new_session()?;
  for i in 0..30 {
    let k = format!("gate_k_{i}");
    assert_eq!(
      rec_session.read(k.as_bytes()).await?,
      Some(b"gate_val".to_vec())
    );
  }
  OK
}

/// 真实并发窗口：create_checkpoint 异步落盘期间 grow_index 必须被拒（有界轮询
/// 观测 Checkpoint 相位后立即触发扩容），检查点完成复位后扩容正常、容量翻倍
#[test]
fn test_concurrent_grow_rejected_during_live_checkpoint() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let cpr_dir = dir.path().join("checkpoints");
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("live_overlap.db"),
    )?);
    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;
    // 灌足数据令快照落盘窗口内多次 await 可观测（800 键 × 64B 值）
    let val = vec![b'v'; 64];
    for i in 0..800 {
      session
        .upsert(format!("live_k_{i}").as_bytes(), &val)
        .await?;
    }

    // 后台线程拍检查点（独立 compio 运行时）
    let store_bg = Arc::clone(&store);
    let cpr_bg = cpr_dir.clone();
    let ckpt = spawn(move || -> aok::Result<wcpr::CheckpointMeta> {
      let rt_c = Runtime::new()?;
      let meta = rt_c.block_on(async {
        store_bg
          .create_checkpoint(&cpr_bg, CheckpointType::Snapshot)
          .await
      })?;
      Ok(meta)
    });

    // 有界轮询观测检查点临界态
    wtest_base::wait_yield_sync(
      || store.resize.phase() == ResizePhase::Checkpoint,
      Duration::from_secs(10),
      "检查点必须进入 Checkpoint 临界态",
    );

    // 落盘窗口内并发扩容被拒（CAS 即时失败，零纪元交互）
    assert!(!store.grow_index()?, "检查点落盘窗口内扩容必须被拒");

    let meta = ckpt.join().unwrap()?;
    assert_eq!(
      meta.index_meta.size, meta.store_meta.index_size,
      "并发被拒下快照容量与元数据容量必须严格相等"
    );

    // 完成复位后扩容正常
    assert_eq!(
      store.resize.phase(),
      ResizePhase::Rest,
      "检查点完成必须复位"
    );
    assert!(store.grow_index()?, "复位后扩容必须正常执行");
    assert_eq!(store.active_index().size, 128);

    OK
  })?;

  OK
}

/// 真实并发扩容一致性：grow_index 切表与分块迁移期间，多会话线程高频对同批
/// 键发起读写，断言扩容收敛后——读恒最新版（无穿透写丢失/陈旧读）、记录
/// prev 链完整无断裂（非首版记录 prev_address 必指向前代）、索引最新槽位
/// 与记录键严格一致（无垃圾槽位挤占）
///
/// 证伪口径（修复前必红）：相位先发布而活跃表未切的过渡窗内，split_single_chunk
/// 同表旁路回滚 UNSTARTED 误判放行，会话对未迁移新桶裸写（prev_address=0），
/// 随后全量迁移重入同分块覆写历史条目——断链与陈旧读在链回溯与收敛校验现形
///
/// 装配：值长度逐轮严格递增强制 RCU 追加换址（杜绝原位更新混叠 prev 语义），
/// 初始 4096 桶 = 4 分块，迁移窗口内分块抢占/自旋与全量迁移真实并发
#[test]
fn test_concurrent_grow_no_dual_slot_or_chain_break() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("grow_race.db"),
    )?);
    let config = StoreConfig::new(4096, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);

    const KEYS: usize = 600;
    const WRITERS: usize = 4;
    const READERS: usize = 2;
    const ROUNDS: u64 = 50;
    // 值长度 = BASE + round，逐轮严格递增 → 每轮必走 RCU 追加换址
    const BASE: u64 = 16;

    let user_keys: Vec<String> = (0..KEYS).map(|i| format!("race_key_{i}")).collect();

    // 物理键（含会话 ns/db 前缀）预计算，供索引槽位级校验；预写首版杜绝并发读 NOTFOUND
    let seed_session = store.new_session()?;
    let phys_keys: Vec<Vec<u8>> = user_keys
      .iter()
      .map(|k| {
        seed_session
          .session_string_key(k.as_bytes())
          .as_slice()
          .to_vec()
      })
      .collect();
    for k in user_keys.iter() {
      seed_session
        .upsert(k.as_bytes(), &vec![b'v'; BASE as usize])
        .await?;
    }
    drop(seed_session);

    // 写线程：分区键 × ROUNDS 轮递增长度写（版本 = len - BASE 可精确还原）
    let mut writers = Vec::new();
    for w in 0..WRITERS {
      let store_w = Arc::clone(&store);
      let keys_w: Vec<Vec<u8>> = user_keys
        .iter()
        .enumerate()
        .filter(|(i, _)| i % WRITERS == w)
        .map(|(_, k)| k.as_bytes().to_vec())
        .collect();
      writers.push(spawn(move || -> aok::Result<()> {
        let rt_w = Runtime::new()?;
        rt_w.block_on(async {
          let s = store_w.new_session()?;
          for round in 1..ROUNDS {
            let val = vec![b'v'; (BASE + round) as usize];
            for k in &keys_w {
              s.upsert(k, &val).await?;
            }
          }
          OK
        })?;
        OK
      }));
    }

    // 读线程：全程轮询读，断言版本合法且单调不减（写推进下读允许滞后，绝不允许回退）
    let mut readers = Vec::new();
    for r in 0..READERS {
      let store_r = Arc::clone(&store);
      let keys_r = user_keys
        .iter()
        .enumerate()
        .filter(|(i, _)| i % READERS == r)
        .map(|(_, k)| k.as_bytes().to_vec())
        .collect::<Vec<_>>();
      readers.push(spawn(move || -> aok::Result<()> {
        let rt_r = Runtime::new()?;
        rt_r.block_on(async {
          let s = store_r.new_session()?;
          let mut last = vec![0u64; keys_r.len()];
          for _ in 0..ROUNDS * 4 {
            for (j, k) in keys_r.iter().enumerate() {
              if let Some(val) = s.read(k).await? {
                let ver = val.len() as u64 - BASE;
                assert!(ver < ROUNDS, "读出非法版本 {ver}（键 {j}）");
                assert!(val.iter().all(|&b| b == b'v'), "读出外来值污染");
                assert!(ver >= last[j], "版本回退 {ver} < {}", last[j]);
                last[j] = ver;
              }
            }
          }
          OK
        })?;
        OK
      }));
    }

    // 主线程连续两次在线扩容：4096 → 8192 → 16384，写读全程并发穿透迁移窗
    assert!(store.grow_index()?, "第一次扩容必须成功");
    assert!(store.grow_index()?, "第二次扩容必须成功");
    assert_eq!(store.active_index().size, 16384);

    for w in writers {
      w.join().unwrap()?;
    }
    for r in readers {
      r.join().unwrap()?;
    }

    let verifier = store.new_session()?;
    for (i, phys) in phys_keys.iter().enumerate() {
      // 1. 收敛读恒最新版：无穿透写丢失、无迁移覆写导致的陈旧读
      let latest = verifier
        .read(user_keys[i].as_bytes())
        .await?
        .unwrap_or_else(|| panic!("键 {i} 丢失"));
      assert_eq!(
        latest.len() as u64,
        BASE + ROUNDS - 1,
        "键 {i} 收敛读必须是最终版本"
      );

      // 2. prev 链完整性：链上可达版本集必为版本号的连续后缀
      //    （C# 一手：Helpers.cs:80-82「Can only elide the record if it is the tail of the
      //    tag chain ... and its PreviousAddress does not point to a valid record」+
      //    InternalUpsert.cs:322 新记录接管该前驱——脱钩只命中链首，每次至多截掉最新一代，
      //    历史只能从高端整段截断，链中不可能留缺口）
      let mut addr = store
        .active_index()
        .find_tag(phys)
        .unwrap_or_else(|| panic!("键 {i} 索引槽位丢失"));
      let mut steps = 0u64;
      loop {
        let rec = store.hlog.read_record(addr).await?;
        assert_eq!(
          rec.key()?,
          phys.as_slice(),
          "键 {i} 链上第 {steps} 代槽位记录键错位"
        );
        let ver = rec.value()?.len() as u64 - BASE;
        if steps == 0 {
          assert_eq!(ver, ROUNDS - 1, "键 {i} 槽位记录非最新版");
        }
        let prev = rec.prev_address()?;
        if prev == 0 {
          assert_eq!(
            steps + ver,
            ROUNDS - 1,
            "键 {i} prev 链有缺口（缺代断链或幽灵插入）：steps={steps} ver={ver}"
          );
          break;
        }
        steps += 1;
        assert!(steps <= ROUNDS, "键 {i} prev 链超长（疑似环或幽灵插入）");
        addr = prev;
      }
    }

    OK
  })?;

  OK
}

/// 迁移失败回滚出的 SPLIT_UNSTARTED 严禁对等待会话放行（修复前等待判据
/// `== SPLIT_IN_PROGRESS` 对回滚态放行，等待会话随即对未迁移新桶加闩/建槽）：
/// 1. 环形抢占路径：回滚分块被会话协同再次抢占，迁移再败错误原样上抛，
///    upsert 中止且新表无槽位（不得建槽）；
/// 2. 等待循环路径：目标分块被他人持锁（IN_PROGRESS）时等待者自旋，持锁者
///    错误回滚后等待者必须重入抢占而非放行（修复前此处 Ok 假完成）。
///
/// 确定性装配：单分块 64 桶表，主桶 0 挂自环溢出桶令迁移内核必败
/// （OverflowCycleDetected，同 test_split_error_propagates_and_aborts_grow 手法；
/// 票面点名的「新表溢出池耗尽」需 2^22 次池分配即 256MB 级内存，物理不可测，
/// 裁决改用同错误面的链环通道）。C# SplitIndex.cs:64-95 等待判据 `== 1` 的
/// 正确性由 SplitSingleBucket 状态单向 0→1→2 保证；rust 迁移内核引入错误回滚
/// 1→0 后，判据必须收紧为 `!= SPLIT_COMPLETED` 方能兑现同款不变量
/// （「等待退出即该分块迁移完成」）。
#[test]
fn test_rolledback_chunk_blocks_waiter_and_session_write() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("rollback_wait.db"),
    )?);
    let config = StoreConfig::new(64, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    // 主桶 0 挂自环溢出桶：分块 0（唯一分块）迁移内核必败
    let old_index = store.active_index();
    let ov_id = old_index.overflow_pool.allocate()?;
    assert!(
      old_index.bucket(0).set_overflow_index(ov_id),
      "空主桶必须能挂载溢出桶"
    );
    let ov = old_index.overflow_pool.get(ov_id).expect("已分配溢出桶");
    assert!(ov.set_overflow_index(ov_id), "空溢出桶必须能自环");

    // 扩容中间态装配（对齐 grow_index 发布次序：先状态/old_index/新表、后相位）
    let count = chunk_count(old_index.size);
    assert_eq!(count, 1, "64 桶表为单分块");
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
    let new_index = store.active_index();

    // 阶段 1：环形抢占路径——回滚态分块被会话协同抢占，迁移再败错误上抛，
    // upsert 中止且不得在新表建槽
    let phys = session.session_string_key(b"rollback_probe");
    let err = session
      .upsert(b"rollback_probe", b"v")
      .await
      .expect_err("未迁移分块上的写必须被协同错误拒绝");
    assert!(
      matches!(err, WkvError::Index(WindexError::OverflowCycleDetected)),
      "必须上抛迁移内核错误: {err}"
    );
    assert_eq!(
      store.resize.split_status.load()[0].load(Ordering::Acquire),
      SPLIT_UNSTARTED,
      "失败分块必须保持回滚态，绝不标记 COMPLETED"
    );
    assert!(
      new_index.find_tag(&phys).is_none(),
      "协同失败的会话严禁对未迁移新桶建槽"
    );

    // 阶段 2：等待循环路径——预置 IN_PROGRESS 模拟他线程持锁迁移，等待会话
    // 自旋；持锁者错误回滚 UNSTARTED 后，等待者必须重入抢占而非放行
    store.resize.split_status.load()[0].store(SPLIT_IN_PROGRESS, Ordering::Release);
    let waiter_store = Arc::clone(&store);
    let waiter_phys = phys.clone();
    let waiter = spawn(move || waiter_store.split_buckets(HashIndex::hash_key(&waiter_phys)));
    // 等待者只能自旋于 IN_PROGRESS（恒无 Ok 退出面），窗口后模拟持锁者失败回滚
    thread::sleep(Duration::from_millis(50));
    store.resize.split_status.load()[0].store(SPLIT_UNSTARTED, Ordering::Release);

    let waiter_err = waiter
      .join()
      .unwrap()
      .expect_err("修复前等待判据对回滚态放行，返回 Ok 假完成");
    assert!(
      matches!(
        waiter_err,
        WkvError::Index(WindexError::OverflowCycleDetected)
      ),
      "等待者重入抢占必须原样上抛迁移内核错误: {waiter_err}"
    );

    teardown_resize_state(&store);
    OK
  })?;

  OK
}

/// grow_index 有限返回：并发会话抢先迁移失败回滚 UNSTARTED 后，驱动线程尾段
/// pending 等待必须有限重扫重试（重试再败错误上抛中止扩容）；修复前对静默哈希
/// 区间永久自旋、grow_index 永不返回。
///
/// 确定性装配：32768 桶表（2 分块），环挂分块 1 的桶 20000；分块 0 灌 5000 键
/// 令其迁移耗时毫秒级，主线程以真实会话身份在相位发布后立即抢占分块 1（微秒级
/// 启动必先于驱动线程越过 chunk 0，抢先迁移必败回滚并上抛）。驱动线程越过分块 1
/// 进入尾段：修复后重扫抢占回滚分块 → 迁移再败错误上抛 → 有限 Err 收敛；
/// recv_timeout 看门狗防修复前挂死拖垮测试进程。
#[test]
fn test_grow_abort_bounded_on_rolledback_chunk() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("grow_rescan.db"),
    )?);
    let config = StoreConfig::new(32768, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
    let store = Arc::new(WedbStore::open(config, device)?);
    let session = store.new_session()?;

    // 环挂分块 1（桶 20000 ∈ [16384, 32767]）：分块 1 迁移必败，分块 0 无环
    let old_index = store.active_index();
    assert_eq!(chunk_count(old_index.size), 2, "32768 桶表为 2 分块");
    let ov_id = old_index.overflow_pool.allocate()?;
    assert!(
      old_index.bucket(20000).set_overflow_index(ov_id),
      "空主桶必须能挂载溢出桶"
    );
    let ov = old_index.overflow_pool.get(ov_id).expect("已分配溢出桶");
    assert!(ov.set_overflow_index(ov_id), "空溢出桶必须能自环");

    // 灌 5000 键拖慢分块 0 迁移，为并发会话抢先抢占分块 1 留出确定性窗口
    for i in 0..5000 {
      session
        .upsert(format!("rescan_seed_{i}").as_bytes(), b"seed_val")
        .await?;
    }

    // 驱动线程执行真实 grow_index，结果经 channel 回传（recv_timeout 看门狗防挂死）
    let grower_store = Arc::clone(&store);
    let (tx, rx) = mpsc::channel();
    let grower = spawn(move || {
      let _ = tx.send(grower_store.grow_index());
    });

    // 相位发布即新表就绪：并发会话立刻协同迁移落分块 1 的键（CAS 抢占，必败回滚）
    let probe = (0..1024)
      .map(|i| format!("rescan_probe_{i}"))
      .find(|k| chunk_offset_for_hash(HashIndex::hash_key(k.as_bytes()), old_index.mask) == 1)
      .expect("1024 个候选必命中分块 1");
    wtest_base::wait_yield_sync(
      || store.resize.phase() == ResizePhase::InProgressGrow,
      Duration::from_secs(10),
      "扩容必须进入 InProgressGrow 态",
    );
    assert_eq!(
      chunk_offset_for_hash(HashIndex::hash_key(probe.as_bytes()), old_index.mask),
      1,
      "探针键必须落在带环分块"
    );
    let split_res = store.split_buckets(HashIndex::hash_key(probe.as_bytes()));
    // 若在 InProgressGrow 期间抢先执行，必须失败回滚上抛；若驱动线程已先一步失败中止并清理扩容态（phase 恢复 Rest），则驱动端必已捕获错误
    if store.resize.phase() == ResizePhase::InProgressGrow {
      assert!(
        split_res.is_err(),
        "并发会话抢先迁移带环分块必须失败回滚并上抛"
      );
    } else {
      assert_eq!(
        store.resize.phase(),
        ResizePhase::Rest,
        "非 InProgressGrow 态扩容必须已收口复位"
      );
    }

    // 驱动线程有限重扫重试回滚分块，迁移再败错误上抛中止扩容（有限返回）
    let grown = rx
      .recv_timeout(Duration::from_secs(60))
      .expect("修复前驱动线程对静默回滚分块永久自旋，grow_index 挂死");
    assert!(
      grown.is_err(),
      "带环分块未迁，扩容必须显式中止而非假完成: {grown:?}"
    );
    assert!(!store.is_growing(), "中止后必须回到 REST 态");
    assert!(
      store.resize.old_index.load().is_none(),
      "中止后必须清理旧表句柄"
    );
    grower.join().unwrap();

    OK
  })?;

  OK
}

/// 扩容中止臂撕裂收口（票 zcode-r135c-rehash 案二）：中止先挂撕裂待重建
/// 标记再发布 Rest；标记存续期——未迁环后键在线读黑洞（旧表句柄已清、
/// 无协同通道）、索引检查点一律 Host 拒发且零副作用（宁试后重试不静默
/// 丢数，修复「中止臂先发 Rest 清 old_index + ensure_not_growing 只拦相位
/// → 撕裂空索引被落盘固化成永久数据丢失」）；rebuild_index_from_hlog 全量
/// 重放原地单调补齐收口——黑洞键复原至原记录地址、撕裂期在线新版本不被
/// 回放覆退（内核单调补齐守卫）、标记复位、检查点即时可发并干净恢复
#[compio::test]
async fn test_grow_abort_marks_torn_blocks_checkpoint_and_rebuild_heals() -> Void {
  let dir = tempdir()?;
  let cpr_dir = dir.path().join("checkpoints");
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("torn.db"))?);
  let config = StoreConfig::new(32768, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
  let session = store.new_session()?;

  let old_index = store.active_index();
  assert_eq!(chunk_count(old_index.size), 2, "32768 桶表为 2 分块");

  // 环挂分块 1 桶 20000：分块 1 迁移内核必败（同 test_split_error_propagates
  // 手法），环前段先迁后中止、环后段恒滞留——中止残面按位带分层
  let ov_id = old_index.overflow_pool.allocate()?;
  assert!(
    old_index.bucket(20000).set_overflow_index(ov_id),
    "空主桶必须能挂载溢出桶"
  );
  let ov = old_index.overflow_pool.get(ov_id).expect("已分配溢出桶");
  assert!(ov.set_overflow_index(ov_id), "空溢出桶必须能自环");

  // 夹具四键位带定位（物理键同口径：索引按 session 前缀物理键哈希落桶）
  let mut key_c = String::new(); // 分块 0：后台全迁，中止后照常可见
  let mut key_part = String::new(); // 分块 1 环前段：部分迁移后中止，可见
  let mut key_a = String::new(); // 分块 1 环后段：中止遗留黑洞
  let mut key_b = String::new(); // 同环后段：撕裂期在线重写（守卫核验）
  let mut phys_a: Vec<u8> = Vec::new();
  let mut phys_b: Vec<u8> = Vec::new();
  let mut i = 0usize;
  while key_c.is_empty() || key_part.is_empty() || key_a.is_empty() || key_b.is_empty() {
    let user = format!("torn_k_{i}");
    let phys = session
      .session_string_key(user.as_bytes())
      .as_slice()
      .to_vec();
    let bucket = (HashIndex::hash_key(&phys) as usize) & old_index.mask;
    // 位带互斥分派（桶 20000 为环桶本身，键落位会污染夹具，一律排除）
    if key_c.is_empty() && bucket < 16384 {
      key_c = user;
    } else if key_part.is_empty() && (16384..20000).contains(&bucket) {
      key_part = user;
    } else if key_a.is_empty() && bucket > 20000 {
      key_a = user.clone();
      phys_a = phys.clone();
    } else if key_b.is_empty() && bucket > 20000 {
      key_b = user;
      phys_b = phys;
    }
    i += 1;
    assert!(i < 100_000, "夹具四键位带必须在有限枚举内齐备");
  }
  assert!(
    !key_c.is_empty() && !key_part.is_empty() && !key_a.is_empty() && !key_b.is_empty(),
    "夹具键位带必须在有限枚举内齐备"
  );

  session.upsert(key_c.as_bytes(), b"v1").await?;
  session.upsert(key_part.as_bytes(), b"v1").await?;
  session.upsert(key_a.as_bytes(), b"v1").await?;
  session.upsert(key_b.as_bytes(), b"v1").await?;
  let addr_a = old_index.find_tag(&phys_a).expect("key_a 条目已落旧表索引");

  // 驱动线程真实 grow_index：分块 0 全迁、分块 1 触环中止（有限收口）
  let grower_store = Arc::clone(&store);
  let (tx, rx) = mpsc::channel();
  let grower = spawn(move || {
    let _ = tx.send(grower_store.grow_index());
  });
  let res = rx
    .recv_timeout(Duration::from_secs(60))
    .expect("扩容必须有限收口返回");
  assert!(
    matches!(
      res,
      Err(WkvError::Index(WindexError::OverflowCycleDetected))
    ),
    "带环分块未迁必须显式中止并上抛: {res:?}"
  );
  grower.join().unwrap();

  // 中止臂收口形制：相位回 Rest、旧表句柄已清、撕裂标记先于 Rest 置位
  assert!(!store.is_growing(), "中止后必须回到 REST 态");
  assert!(
    store.resize.old_index.load().is_none(),
    "中止后必须清理旧表句柄"
  );
  assert!(
    store.index_rebuild_pending(),
    "中止臂必须先挂撕裂待重建标记再发布 Rest"
  );
  assert_eq!(store.active_index().size, 65536);

  // 黑洞面真实：环后未迁键在线不可读（旧表已清、Rest 相无协同通道）；
  // 环前已迁键可见（证明活跃表是迁移后的新表而非回滚旧表）
  assert_eq!(
    session.read(key_a.as_bytes()).await?,
    None,
    "中止遗留未迁分块键在撕裂窗内必为读黑洞"
  );
  assert_eq!(session.read(key_c.as_bytes()).await?, Some(b"v1".to_vec()));
  assert_eq!(
    session.read(key_part.as_bytes()).await?,
    Some(b"v1".to_vec()),
    "环前段条目已迁入新表，中止不得回滚其可见性"
  );

  // 撕裂期在线重写 key_b（RCU 换址新版）：重建回放严禁覆退（单调补齐守卫）
  session.upsert(key_b.as_bytes(), b"v3-longer").await?;

  // 标记存续期索引检查点 Host 拒发、零副作用（宁试后重试不静默丢数）
  let err = store
    .create_checkpoint(&cpr_dir, CheckpointType::FoldOver)
    .await
    .expect_err("撕裂待重建标记存续期必须拒发索引快照");
  assert!(
    matches!(err, Error::Host(_)),
    "必须返回可重试 Host 错误: {err}"
  );
  assert!(!cpr_dir.exists(), "拒绝路径零副作用，不得创建检查点目录");

  // 在线收口重建：[begin, tail) 全量重放原地单调补齐，成功消标记
  store.rebuild_index_from_hlog().await?;
  assert!(!store.index_rebuild_pending(), "重建成功必须消撕裂标记");
  assert_eq!(
    session.read(key_a.as_bytes()).await?,
    Some(b"v1".to_vec()),
    "黑洞键必须经重放复原"
  );
  assert_eq!(
    store.active_index().find_tag(&phys_a),
    Some(addr_a),
    "复原槽位必须恰为原记录地址"
  );
  assert_eq!(
    session.read(key_b.as_bytes()).await?,
    Some(b"v3-longer".to_vec()),
    "单调补齐守卫：撕裂期在线新版本严禁被旧地址回放覆退"
  );
  assert!(
    store.active_index().find_tag(&phys_b).is_some(),
    "key_b 槽位重建后必须存在"
  );
  assert_eq!(
    session.read(key_part.as_bytes()).await?,
    Some(b"v1".to_vec()),
    "已迁条目守卫下逐字节不变"
  );

  // 消标记后检查点即时可发，恢复读出全夹具键（撕裂空索引不再被固化）
  let meta = store
    .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
    .await?;
  assert_eq!(meta.index_meta.size, 65536);
  let recovered = Arc::new(WedbStore::recover(&cpr_dir, meta.token, device).await?);
  let rec_session = recovered.new_session()?;
  for k in [&key_c, &key_part, &key_a] {
    assert_eq!(rec_session.read(k.as_bytes()).await?, Some(b"v1".to_vec()));
  }
  assert_eq!(
    rec_session.read(key_b.as_bytes()).await?,
    Some(b"v3-longer".to_vec())
  );
  OK
}

/// grow_index_blocking 中止收口驱动（票 zcode-r135c-rehash 案二执行位置臂）：
/// 扩容内核错误返回后回 reactor 线程即地驱动在线收口重建——错误原样上抛、
/// 标记即消、黑洞键复原；后续真实扩容（新表无环）正常翻倍；门口预检遗留
/// 标记时先重建收口再扩容（带撕裂表不继续长大）
#[compio::test]
async fn test_grow_index_blocking_heals_torn_and_preflight_rebuilds() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("blocking_torn.db"),
  )?);
  let config = StoreConfig::new(32768, DEFAULT_SECTOR_SIZE, 16, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;

  let old_index = store.active_index();
  let ov_id = old_index.overflow_pool.allocate()?;
  assert!(old_index.bucket(20000).set_overflow_index(ov_id));
  let ov = old_index.overflow_pool.get(ov_id).expect("已分配溢出桶");
  assert!(ov.set_overflow_index(ov_id), "空溢出桶必须能自环");

  let mut key_a = String::new();
  for i in 0..100_000 {
    let user = format!("blk_k_{i}");
    let phys = session
      .session_string_key(user.as_bytes())
      .as_slice()
      .to_vec();
    let bucket = (HashIndex::hash_key(&phys) as usize) & old_index.mask;
    if bucket > 20000 && bucket < 32768 {
      key_a = user;
      break;
    }
  }
  assert!(!key_a.is_empty(), "环后键位带必须在有限枚举内命中");
  session.upsert(key_a.as_bytes(), b"v1").await?;

  // 包装层中止臂：内核错误 → 撕裂标记 → 回 reactor 即地重建收口 → 错误原样上抛
  let res = grow_index_blocking(Arc::clone(&store)).await;
  assert!(
    matches!(
      res,
      Err(WkvError::Index(WindexError::OverflowCycleDetected))
    ),
    "扩容必须显式中止并原样上抛迁移内核错误: {res:?}"
  );
  assert!(
    !store.index_rebuild_pending(),
    "包装层必须在中止返回前即地驱动在线收口并消标记"
  );
  assert_eq!(store.active_index().size, 65536);
  assert_eq!(
    session.read(key_a.as_bytes()).await?,
    Some(b"v1".to_vec()),
    "黑洞键必须经包装层即地重建复原"
  );

  // 第二轮包装：活跃表无环，正常扩容 65536 → 131072
  assert!(grow_index_blocking(Arc::clone(&store)).await?);
  assert_eq!(store.active_index().size, 131072);

  // 确定性注入遗留标记（公开真值字段直置，模拟上一轮重建失败的门口态）：
  // 预检先重建收口（全在场记录守卫下逐字节 no-op）再照常扩容
  store.resize.index_torn.store(true, Ordering::SeqCst);
  assert!(grow_index_blocking(Arc::clone(&store)).await?);
  assert!(!store.index_rebuild_pending(), "门口重建成功必须消标记");
  assert_eq!(store.active_index().size, 262144);
  assert_eq!(
    session.read(key_a.as_bytes()).await?,
    Some(b"v1".to_vec()),
    "门口收口 + 后续扩容全链不得伤及数据"
  );
  OK
}
