//! 常驻页缓存总闸记账测试（本仓分层防 OOM 自定义面，C# 无对应）
//!
//! 验收契约（task/ing/wbftree-tree-cache-global-budget.md）：
//! 1. 超预算创建/升阶被 CacheBudgetExhausted 显式拒绝；
//! 2. 树清退后预算归还，后续创建/升阶可继续执行；
//! 3. cache_reserved 记账数与当前在线活跃树环容量和严格一致。
//!
//! 追加契约（task/done/wnode-ricreate-cachesize-unwind-budget-leak.md，返工
//! 对齐 task/ing/wbftree-ricreate-panicpremise-falsify-realign.md）：
//! 4. 建树实例化段失败（引擎 `Config::validate` 干净拒绝回 InvalidConfig；
//!    容量断言经 validate 前置在建树链上不可达，无 panic 面亦无 catch_unwind）
//!    不得滞留预留记账。
//!
//! 追加契约（task/ing/wkv-lazyrestore-ghost-tree-budget-grind.md）：
//! 5. 惰性恢复在途撞清库/换号（世代变更）：get_or_open_tree 注册前复查世代已变，
//!    必须回滚预留记账、弃置未注册树实例、返回 NotFound，cache_reserved 回归 0，
//!    注册表无幽灵条目，随后同名 create_bftree 成功。
//!
//! 自研依据: doc/zh/collection.md 升阶树页缓存预算（管理器/存根组件）

use std::{
  fs::remove_file,
  sync::{Arc, atomic::Ordering},
  thread,
};

use aok::{OK, Result};
use wbftree::{Error, RangeIndexManager, RangeIndexStub, StorageBackendType, TreeTuning};
#[cfg(debug_assertions)]
use wbftree::{
  GET_OR_OPEN_PAUSE_INJECT, GET_OR_OPEN_PAUSE_KEY_HASH, GET_OR_OPEN_PAUSED, GET_OR_OPEN_RESUME,
};
use whasher::fast_hash;

use super::common::ManagerEnvGuard;

/// 预算测试专用小调参（页环 64KiB，与 wkv RI 测试 TUNE 同量级）
const SMALL_TUNE: TreeTuning = TreeTuning {
  cache_size: 64 * 1024,
  min_record_size: 2,
  max_record_size: 1024,
  max_key_len: 32,
  leaf_page_size: 0,
};

/// 引擎拒绝调参：环 4KiB 低于内存后端（cache_only）的 4× 叶页下限，实例化段被
/// 引擎 `Config::validate` 显式拒绝（circular buffer size 检查）。不断言 panic：
/// 0.5.4/0.5.6 的 validate 容量比例判定（cache-only ≥ 4× 叶页、否则 ≥ 2×）严格
/// 强于 `CircularBuffer::new` 的 `capacity >= leaf_page_size + AllocMeta` 环断言
/// （circular_buffer/mod.rs:418），建树链上该断言不可达，引擎恒以
/// `Error::InvalidConfig` 干净拒绝
const REJECT_TUNE: TreeTuning = TreeTuning {
  cache_size: 4096,
  min_record_size: 2,
  max_record_size: 1024,
  max_key_len: 32,
  leaf_page_size: 4096,
};

/// 三环预算（65536 × 3）：恰好容纳三棵树
const THREE_RING_BUDGET: usize = 64 * 1024 * 3;

#[test]
fn test_budget_exhaustion_rejects_create_and_release_restores() -> Result<()> {
  let env = ManagerEnvGuard::new("budget_create");
  let manager = Arc::new(RangeIndexManager::with_epoch_and_budget(
    &env.ri_root.path,
    &env.cpr_root.path,
    None,
    THREE_RING_BUDGET,
  )?);

  // 1. 预算内逐棵创建：记账随在线树环容量和线性增长
  for i in 0..3 {
    let key = format!("idx_{i}");
    let tree = manager.create_bftree(key.as_bytes(), StorageBackendType::Disk, SMALL_TUNE)?;
    assert_eq!(tree.cache_bytes(), SMALL_TUNE.cache_size);
    assert_eq!(
      manager.cache_reserved(),
      (i + 1) * SMALL_TUNE.cache_size,
      "第 {} 棵树登记后记账须为环容量和",
      i + 1
    );
  }

  // 2. 超预算第 4 棵：显式拒绝且记账不变
  assert!(matches!(
    manager.create_bftree(b"idx_3", StorageBackendType::Disk, SMALL_TUNE),
    Err(Error::CacheBudgetExhausted)
  ));
  assert_eq!(manager.cache_reserved(), THREE_RING_BUDGET);

  // 3. 摘除一棵：配额归还，后续创建可继续
  assert!(manager.delete_index(b"idx_0")?);
  assert_eq!(manager.cache_reserved(), 2 * SMALL_TUNE.cache_size);
  let tree = manager.create_bftree(b"idx_3", StorageBackendType::Disk, SMALL_TUNE)?;
  assert_eq!(tree.cache_bytes(), SMALL_TUNE.cache_size);
  assert_eq!(manager.cache_reserved(), THREE_RING_BUDGET);

  // 4. 记账与在线树环容量和严格一致（验收指标 3）：在线树为 idx_1/idx_2/idx_3
  let live_sum: usize = ["idx_1", "idx_2", "idx_3"]
    .iter()
    .filter_map(|k| manager.get_tree(k.as_bytes()).map(|t| t.cache_bytes()))
    .sum();
  assert_eq!(manager.cache_reserved(), live_sum);

  // 5. 全量 dispose：注册表清空即配额归零
  manager.dispose();
  assert_eq!(manager.cache_reserved(), 0);
  OK
}

#[test]
fn test_zero_budget_unlimited() -> Result<()> {
  let env = ManagerEnvGuard::new("budget_unlimited");
  // 预算 0 = 不设限：仅记账不裁决（wbftree 独立使用形态）
  let manager =
    RangeIndexManager::with_epoch_and_budget(&env.ri_root.path, &env.cpr_root.path, None, 0)?;
  const TREE_COUNT: usize = 4;
  for i in 0..TREE_COUNT {
    let key = format!("idx_{i}");
    manager.create_bftree(key.as_bytes(), StorageBackendType::Disk, SMALL_TUNE)?;
  }
  assert_eq!(manager.cache_reserved(), TREE_COUNT * SMALL_TUNE.cache_size);
  OK
}

#[test]
fn test_scratch_gate_rejects_and_release_allows() -> Result<()> {
  let env = ManagerEnvGuard::new("budget_scratch");
  // 单环预算：一棵长驻树即占满
  let manager = Arc::new(RangeIndexManager::with_epoch_and_budget(
    &env.ri_root.path,
    &env.cpr_root.path,
    None,
    SMALL_TUNE.cache_size,
  )?);

  manager.create_bftree(b"resident", StorageBackendType::Disk, SMALL_TUNE)?;
  assert_eq!(manager.cache_reserved(), SMALL_TUNE.cache_size);

  // 长驻树占满预算：并发升阶 scratch 闸显式拒绝
  let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..16)
    .map(|i| (format!("k{i}").into_bytes(), format!("v{i}").into_bytes()))
    .collect();
  assert!(matches!(
    manager.build_collection_tree_snapshot(&entries, &SMALL_TUNE),
    Err(Error::CacheBudgetExhausted)
  ));
  assert_eq!(manager.cache_reserved(), SMALL_TUNE.cache_size);

  // 长驻树清退：配额归还，scratch 建树可行
  assert!(manager.delete_index(b"resident")?);
  assert_eq!(manager.cache_reserved(), 0);
  let (snap_path, count) = manager.build_collection_tree_snapshot(&entries, &SMALL_TUNE)?;
  assert_eq!(count, 16);
  assert!(snap_path.exists(), "scratch 建树产物快照必须存在");
  let _ = remove_file(&snap_path);
  // scratch 出口归还：建树成功后记账归零
  assert_eq!(manager.cache_reserved(), 0);
  OK
}

#[test]
fn test_instantiate_failure_releases_reservation() -> Result<()> {
  let env = ManagerEnvGuard::new("budget_instantiate_release");
  let manager = Arc::new(RangeIndexManager::with_epoch_and_budget(
    &env.ri_root.path,
    &env.cpr_root.path,
    None,
    SMALL_TUNE.cache_size,
  )?);

  // 环 4KiB 低于内存后端（cache_only）4× 叶页下限：引擎 Config::validate 于
  // 实例化段显式拒绝回 InvalidConfig——干净 Err，不触引擎断言 panic（validate
  // 的比例判定严格强于 CircularBuffer::new 的 leaf+AllocMeta 环断言，建树链上
  // 该断言不可达）
  let err = match manager.create_bftree(b"idx_reject", StorageBackendType::Memory, REJECT_TUNE) {
    Err(e) => e,
    Ok(_) => panic!("容量不足组合必须被引擎 validate 拒绝"),
  };
  assert!(
    matches!(err, Error::InvalidConfig(_)),
    "须为引擎配置拒绝，实得: {err:?}"
  );

  // 记账零滞留（追加契约 4）：Err 臂归还不得跳过——泄漏态下此处为 4096，
  // 脚本化重复失败创建即可磨穿总闸
  assert_eq!(manager.cache_reserved(), 0);

  // 归还后预算完好：正常创建不受滞留记账拖累
  let tree = manager.create_bftree(b"idx_ok", StorageBackendType::Memory, SMALL_TUNE)?;
  assert_eq!(tree.cache_bytes(), SMALL_TUNE.cache_size);
  assert_eq!(manager.cache_reserved(), SMALL_TUNE.cache_size);
  OK
}

/// 追加契约 6（task/ing/wbftree-cold-tree-cache-no-evict-recycle.md）：冷树回收
/// 形态的预算归还与懒重开闭环——dispose_tree_under_lock(delete_file=false)（C#
/// DisposeTreeUnderLock deleteFiles:false 对位，wkv 冷树回收内核唯一释放口）
/// 摘除在线树即归还页环预算、保留数据文件与条目消亡语义不同：新树创建成功后，
/// 冷键经 get_or_open_tree 从收口快照懒重开，数据逐字段无损、记账如实回账
#[test]
fn test_cold_detach_returns_budget_and_lazy_reopen_keeps_data() -> Result<()> {
  let env = ManagerEnvGuard::new("budget_cold_detach");
  // 两环预算装两棵树
  let manager = Arc::new(RangeIndexManager::with_epoch_and_budget(
    &env.ri_root.path,
    &env.cpr_root.path,
    None,
    SMALL_TUNE.cache_size * 2,
  )?);

  let tree_a = manager.create_bftree(b"cold_a", StorageBackendType::Disk, SMALL_TUNE)?;
  tree_a.insert(b"field", b"value");
  manager.create_bftree(b"cold_b", StorageBackendType::Disk, SMALL_TUNE)?;
  assert_eq!(manager.cache_reserved(), 2 * SMALL_TUNE.cache_size);

  // 预算耗尽：第三棵显式拒绝
  assert!(matches!(
    manager.create_bftree(b"cold_c", StorageBackendType::Disk, SMALL_TUNE),
    Err(Error::CacheBudgetExhausted)
  ));

  // 冷回收臂摘除 cold_a（delete_file=false）：预算回落、条目出册、数据文件保留
  assert!(manager.dispose_tree_under_lock(b"cold_a", false)?);
  assert_eq!(manager.cache_reserved(), SMALL_TUNE.cache_size);
  assert!(!manager.is_registered(b"cold_a"), "回收后注册表必须摘除");
  assert!(
    manager.data_file_path_for_key(b"cold_a").exists(),
    "懒恢复形态绝不删文件"
  );

  // 预算腾出：新树创建成功（回收 → 自愈闭环的注册表面）
  manager.create_bftree(b"cold_c", StorageBackendType::Disk, SMALL_TUNE)?;
  assert_eq!(manager.cache_reserved(), 2 * SMALL_TUNE.cache_size);

  // 冷键再读：get_or_open_tree 从收口快照懒重开，数据无损、强制预留回账
  let stub = RangeIndexStub::from_tuning(0, &SMALL_TUNE, StorageBackendType::Disk);
  let reopened = manager.get_or_open_tree(b"cold_a", &stub)?;
  assert_eq!(
    reopened.read(b"field").1.as_deref(),
    Some(b"value".as_slice())
  );
  assert_eq!(
    manager.cache_reserved(),
    3 * SMALL_TUNE.cache_size,
    "恢复树不拒绝、记账如实累加"
  );
  OK
}

#[cfg(debug_assertions)]
static INJECT_SERIALIZE: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
#[cfg(debug_assertions)]
struct InjectGuard;

#[cfg(debug_assertions)]
impl Drop for InjectGuard {
  fn drop(&mut self) {
    GET_OR_OPEN_PAUSE_INJECT.store(false, Ordering::SeqCst);
    GET_OR_OPEN_PAUSE_KEY_HASH.store(0, Ordering::SeqCst);
    GET_OR_OPEN_RESUME.store(true, Ordering::SeqCst);
    GET_OR_OPEN_PAUSED.store(false, Ordering::SeqCst);
  }
}

/// 惰性恢复在途撞清库/换号（世代变更）：get_or_open_tree 注册前复查世代已变，
/// 必须回滚预留记账、弃置未注册树实例、返回 NotFound，cache_reserved 回归 0，
/// 注册表无幽灵条目，随后同名 create_bftree 成功（追加契约 5）
#[cfg(debug_assertions)]
#[test]
fn test_lazyrestore_ghost_tree_generation_rollback() -> Result<()> {
  let _serialize_lock = INJECT_SERIALIZE.lock();
  let _guard = InjectGuard;

  let env = ManagerEnvGuard::new("budget_lazyrestore_ghost");
  let manager = Arc::new(RangeIndexManager::with_epoch_and_budget(
    &env.ri_root.path,
    &env.cpr_root.path,
    None,
    THREE_RING_BUDGET,
  )?);
  let key = b"ghost_victim";
  let key_hash = fast_hash(key);

  // 1. 创建一棵树，刷盘产生持久化快照，并卸载内存实例（保留磁盘数据文件供惰性恢复）
  let tree = manager.create_bftree(key, StorageBackendType::Disk, SMALL_TUNE)?;
  tree.insert(b"k1", b"v1");
  assert_eq!(manager.cache_reserved(), SMALL_TUNE.cache_size);

  let mut stub = RangeIndexStub::new(
    0,
    SMALL_TUNE.cache_size as u64,
    SMALL_TUNE.min_record_size as u32,
    SMALL_TUNE.max_record_size as u32,
    SMALL_TUNE.max_key_len as u32,
    4096,
    StorageBackendType::Disk,
  );
  manager.on_flush_address(key, &mut stub, 0x1000)?;
  assert!(manager.dispose_tree_under_lock(key, false)?);
  assert_eq!(manager.cache_reserved(), 0);

  // 预分阶段准备好 data.bftree（模拟冷态待惰性恢复状态）
  manager.pre_stage_and_register_pending(key, 0x1000)?;

  // 2. 启用故障注入钩子：限定目标键哈希，使 get_or_open_tree 在 open 完成并预留配额后暂停
  GET_OR_OPEN_RESUME.store(false, Ordering::SeqCst);
  GET_OR_OPEN_PAUSED.store(false, Ordering::SeqCst);
  GET_OR_OPEN_PAUSE_KEY_HASH.store(key_hash, Ordering::SeqCst);
  GET_OR_OPEN_PAUSE_INJECT.store(true, Ordering::SeqCst);

  let mgr_clone = Arc::clone(&manager);
  let stub_clone = stub;
  let handle = thread::spawn(move || mgr_clone.get_or_open_tree(key, &stub_clone));

  // 3. 等待 get_or_open_tree 完成 open 并在门前暂停
  while !GET_OR_OPEN_PAUSED.load(Ordering::Acquire) {
    thread::yield_now();
  }

  // 此时 open 已完成并强制预留了配额
  assert_eq!(manager.cache_reserved(), SMALL_TUNE.cache_size);

  // 4. 并发触发清库（对位 wkv flush_all_databases 的 range_index.clear_all()）
  manager.clear_all()?;

  // 5. 唤醒 get_or_open_tree 继续执行注册段
  GET_OR_OPEN_RESUME.store(true, Ordering::Release);

  // 6. 断言：恢复臂返回 NotFound、cache_reserved 回归 0、live_indexes 无该键条目
  let res = handle.join().unwrap();
  assert!(
    matches!(res, Err(Error::NotFound)),
    "世代已变，恢复臂必须返回 NotFound，实得: {:?}",
    res.as_ref().err()
  );
  assert_eq!(manager.cache_reserved(), 0, "回滚后已预留配额必须回归 0");
  let key_id = RangeIndexManager::key_id_of(key);
  assert!(
    manager.live_indexes().pin().get(&key_id).is_none(),
    "live_indexes 中不得残留幽灵树条目"
  );

  // 7. 随后同名 create_bftree 成功（无 IndexExists 幽灵占用）
  let new_tree = manager.create_bftree(key, StorageBackendType::Disk, SMALL_TUNE)?;
  assert_eq!(new_tree.cache_bytes(), SMALL_TUNE.cache_size);
  assert_eq!(manager.cache_reserved(), SMALL_TUNE.cache_size);

  OK
}
