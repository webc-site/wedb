use std::{fs, sync::Arc, thread, time::Duration};

use aok::{OK, Result};
use compio::{runtime::Runtime, time::sleep};
use wbase::base32::encode_u128;
use wbftree::{
  BfTreeInsertResult, BfTreeReadResult, Error, RangeIndexManager, RangeIndexStub,
  StorageBackendType,
};

use super::common::{ManagerEnvGuard, TUNE};

/// 测试 RangeIndexManager 协调数据持久化、快照与检查点恢复
#[test]
fn test_range_index_manager_lifecycle_and_checkpoint() -> Result<()> {
  let env = ManagerEnvGuard::new("root");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let key = b"garnet:rangeindex:testkey";

  // 1. 创建 BfTree 实例
  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;

  // 2. 写入数据
  assert_eq!(
    tree.insert(b"member_001", b"score_100"),
    BfTreeInsertResult::Success
  );
  assert_eq!(
    tree.insert(b"member_002", b"score_200"),
    BfTreeInsertResult::Success
  );

  let mut stub = RangeIndexStub::new(
    0,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );

  // 3. 刷盘 (唯一入口 on_flush_address：逻辑地址必带，工件为带地址命名)
  manager.on_flush_address(key, &mut stub, 0x1000)?;
  assert!(stub.is_flushed());
  assert!(
    manager
      .log_flush_path(&RangeIndexManager::base32_prefix_of(key), 0x1000)
      .exists()
  );

  // 4. 执行全局检查点快照
  let checkpoint_token = 12345u128;
  manager.snapshot_all_trees_to_dir(&env.cpr_root.path, checkpoint_token)?;

  let snap_path = RangeIndexManager::checkpoint_snapshot_path_in(
    &env.cpr_root.path,
    checkpoint_token,
    RangeIndexManager::key_id_of(key),
  );
  assert!(snap_path.exists());

  // 5. 模拟新进程从检查点全量恢复
  let new_manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  new_manager.recover_all_trees_from_dir(&env.cpr_root.path, checkpoint_token)?;

  let recovered_tree = new_manager.get_or_open_tree(key, &stub)?;
  let (res1, val1) = recovered_tree.read(b"member_001");
  assert_eq!(res1, BfTreeReadResult::Found);
  assert_eq!(val1, Some(b"score_100".to_vec()));

  let (res2, val2) = recovered_tree.read(b"member_002");
  assert_eq!(res2, BfTreeReadResult::Found);
  assert_eq!(val2, Some(b"score_200".to_vec()));

  OK
}

/// 测试带有逻辑地址的刷盘与日志截断
#[test]
fn test_range_index_manager_truncate() -> Result<()> {
  let env = ManagerEnvGuard::new("trunc");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let key = b"trunc_key";
  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;

  tree.insert(b"k1", b"v1");
  let mut stub = RangeIndexStub::new(
    0,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );

  // 在逻辑地址 0x100 处刷盘
  manager.on_flush_address(key, &mut stub, 0x100)?;
  let hash_prefix = RangeIndexManager::base32_prefix_of(key);
  let flush_path = manager.log_flush_path(&hash_prefix, 0x100);
  assert!(flush_path.exists());

  // 截断到 0x50 (不应删除)
  manager.on_truncate(0x50)?;
  assert!(flush_path.exists());

  // 截断到 0x200 (应删除)
  manager.on_truncate(0x200)?;
  assert!(!flush_path.exists());

  OK
}

/// 测试 get_or_open_tree 正确从磁盘恢复现有快照数据，而不是覆盖为空白新树
#[test]
fn test_get_or_open_tree_preserves_persisted_data() -> Result<()> {
  let env = ManagerEnvGuard::new("restore");
  let manager = Arc::new(RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap());
  let key = b"persist_key";

  // 1. 创建树并写入数据
  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree.insert(b"k1", b"v1"), BfTreeInsertResult::Success);

  // 2. 刷盘保存数据到磁盘 (逻辑地址 0x2000 处生成 flush 快照)
  let mut stub = RangeIndexStub::new(
    0,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );
  manager.on_flush_address(key, &mut stub, 0x2000)?;

  // 3. 卸载内存树 (模拟淘汰)
  assert!(manager.dispose_tree_under_lock(key, false).unwrap());

  // 4. 执行预分阶段 PreStage (从 flush 快照复制到 data.bftree)
  manager.pre_stage_and_register_pending(key, 0x2000)?;

  // 5. 通过 get_or_open_tree 惰性激活恢复
  let restored = manager.get_or_open_tree(key, &stub)?;
  let (res, val) = restored.read(b"k1");
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"v1".to_vec()));

  OK
}

/// 测试 delete_index 清理磁盘工作文件
#[test]
fn test_delete_index_cleans_disk_file() -> Result<()> {
  let env = ManagerEnvGuard::new("del");
  let manager = Arc::new(RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap());
  let key = b"key_to_delete";
  let _tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;

  let data_path = manager.data_file_path_for_key(key);
  assert!(data_path.exists());

  assert!(manager.delete_index(key).unwrap());
  assert!(!data_path.exists());

  OK
}

/// 测试并发重复创建同一 RangeIndex 时防御盲插并报错
#[test]
fn test_create_bftree_duplicate_prevention() -> Result<()> {
  let env = ManagerEnvGuard::new("dup");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let key = b"duplicate_key_test";

  let tree1 = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree1.insert(b"k1", b"v1"), BfTreeInsertResult::Success);

  let tree2_res = manager.create_bftree(key, StorageBackendType::Disk, TUNE);
  assert!(
    matches!(tree2_res, Err(Error::IndexExists)),
    "重复创建必须返回结构化 IndexExists 错误"
  );

  OK
}

/// 测试 CPR 检查点版本屏障设置、清除与树快照等待
#[test]
fn test_checkpoint_barrier_and_wait() -> Result<()> {
  let env = ManagerEnvGuard::new("barrier");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let key = b"barrier_test_key";

  let _tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;

  assert!(!manager.is_checkpoint_in_progress());
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    assert!(!manager.wait_for_tree_checkpoint_async(key).await.unwrap());
  });

  manager.set_checkpoint_barrier();
  assert!(manager.is_checkpoint_in_progress());

  // 清除屏障后恢复无等待
  manager.clear_checkpoint_barrier();
  assert!(!manager.is_checkpoint_in_progress());
  rt.block_on(async {
    assert!(!manager.wait_for_tree_checkpoint_async(key).await.unwrap());
  });

  OK
}

/// 冷树 (无在线实例) 刷盘：直接复制工作文件 data.bftree 为带地址刷盘快照
#[test]
fn test_on_flush_address_cold_tree_copies_data_file() -> Result<()> {
  let env = ManagerEnvGuard::new("cold");
  let manager = Arc::new(RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap());
  let key = b"cold_flush_key";

  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  tree.insert(b"k1", b"v1");

  // 卸载在线树后变为冷树 (data.bftree 保留在磁盘上)
  assert!(manager.dispose_tree_under_lock(key, false).unwrap());

  let mut stub = RangeIndexStub::new(
    0,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );
  manager.on_flush_address(key, &mut stub, 0x900)?;

  let hash_prefix = RangeIndexManager::base32_prefix_of(key);
  let flush_path = manager.log_flush_path(&hash_prefix, 0x900);
  assert!(stub.is_flushed());
  assert!(flush_path.exists());

  OK
}

/// 冷树工作文件缺失时刷盘必须保持未刷盘状态，且不得留下带地址刷盘工件
#[test]
fn test_on_flush_address_missing_data_file_keeps_stub_unflushed() -> Result<()> {
  let env = ManagerEnvGuard::new("cold_miss");
  let manager = Arc::new(RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap());
  let key = b"cold_missing_key";

  manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;

  // delete_index 同时注销在线树并删除工作文件 → 工作文件缺失
  assert!(manager.delete_index(key).unwrap());

  let mut stub = RangeIndexStub::new(
    0,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );
  manager.on_flush_address(key, &mut stub, 0xA00)?;

  assert!(!stub.is_flushed());
  let hash_prefix = RangeIndexManager::base32_prefix_of(key);
  let flush_path = manager.log_flush_path(&hash_prefix, 0xA00);
  assert!(!flush_path.exists());

  OK
}

/// 预分阶段源刷盘文件缺失时：不注册 pending 条目，后续 get_or_open_tree 显式报错
#[test]
fn test_pre_stage_missing_source_and_restore_error() -> Result<()> {
  let env = ManagerEnvGuard::new("prestage_miss");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let key = b"prestage_missing_key";

  // 无任何刷盘文件时 pre_stage 成功返回但不注册 pending 条目
  manager.pre_stage_and_register_pending(key, 0x1234)?;
  assert_eq!(manager.live_index_count(), 0);
  assert!(!manager.data_file_path_for_key(key).exists());

  // 磁盘后端无 data.bftree 且无刷盘快照 → get_or_open_tree 报错
  let stub = RangeIndexStub::new(
    0,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );
  assert!(manager.get_or_open_tree(key, &stub).is_err());

  OK
}

/// 刷盘快照文件名严格解析与单件预置：带符号等非法地址段的外来文件绝不会成为
/// 恢复来源 (预置只按存根源记录的精确地址取那一个文件，不做目录择优扫描)，
/// 也绝不被 on_truncate 误删
#[test]
fn test_flush_file_name_strict_parsing() -> Result<()> {
  let env = ManagerEnvGuard::new("fname");
  let manager = Arc::new(RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap());
  let key = b"strict_fname_key";

  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree.insert(b"k1", b"v1"), BfTreeInsertResult::Success);

  let hash_prefix = RangeIndexManager::base32_prefix_of(key);
  // 合法文件：地址段 0x0010，写入真实快照
  let good = manager.log_flush_path(&hash_prefix, 0x10);
  tree.cpr_snapshot(&good)?;
  // 非法文件：地址段带前导符号与非法字符
  let hostile = env
    .ri_root
    .join(format!("{}.+0000000000ff.flush.bftree", hash_prefix));
  fs::write(&hostile, b"bad")?;
  // 非法文件 2：13 字符非 Base32 编码
  let hostile2 = env
    .ri_root
    .join(format!("{}.{}.flush.bftree", hash_prefix, "z".repeat(13)));
  fs::write(&hostile2, b"bad2")?;

  // 注销在线树并清走工作文件：只剩非法命名的残件时冷读必须显式报错
  // (1:1 对标 C# RestoreTree 的 File.Exists(workingPath) 预置不变量，绝不回退到其他刷盘件)
  assert!(manager.dispose_tree_under_lock(key, false).unwrap());
  fs::remove_file(manager.data_file_path(&hash_prefix))?;
  let stub = RangeIndexStub::new(
    0,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );
  assert!(manager.get_or_open_tree(key, &stub).is_err());

  // 按精确源地址预置合法刷盘件后惰性恢复：从该件完整回读
  manager.pre_stage_and_register_pending(key, 0x10)?;
  let restored = manager.get_or_open_tree(key, &stub)?;
  assert_eq!(
    restored.read(b"k1"),
    (BfTreeReadResult::Found, Some(b"v1".to_vec()))
  );

  // on_truncate 只删除合法解析且地址低于阈值的文件，外来文件保持原样
  manager.on_truncate(0x20)?;
  assert!(!good.exists());
  assert!(hostile.exists());
  assert!(hostile2.exists());

  OK
}

/// 主端复制枚举（enumerate_checkpoint_snapshots）必须跳过非 26 字符 Base32
/// 前缀的外来快照文件
#[test]
fn test_replication_enumeration_skips_foreign_files() -> Result<()> {
  let env = ManagerEnvGuard::new("repl_foreign");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let key = b"repl_foreign_key";

  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree.insert(b"k1", b"v1"), BfTreeInsertResult::Success);

  let token = 12345u128;
  manager.snapshot_all_trees_to_dir(&env.cpr_root.path, token)?;

  let token_b32 = encode_u128(token);
  // 注入外来文件：非 Base32 stem、错长度 stem、错误后缀均应被跳过
  let snapshot_dir = env.cpr_root.join(token_b32.as_str()).join("rangeindex");
  fs::write(snapshot_dir.join("foreign.bftree"), b"junk")?;
  fs::write(
    snapshot_dir.join(format!("{}.bftree", "z".repeat(26))),
    b"junk",
  )?;
  fs::write(snapshot_dir.join("data.txt"), b"junk")?;

  let files = RangeIndexManager::enumerate_checkpoint_snapshots(&env.cpr_root.path, token)?;
  assert_eq!(files.len(), 1, "外来快照文件必须被跳过");
  let (key_id, path) = files[0].clone();
  assert_eq!(key_id, RangeIndexManager::key_id_of(key));
  assert_eq!(
    path,
    RangeIndexManager::checkpoint_snapshot_path_in(&env.cpr_root.path, token, key_id)
  );

  // token 目录不存在时返回空集（副本/主端无该检查点时的枚举契约）
  assert!(
    RangeIndexManager::enumerate_checkpoint_snapshots(&env.cpr_root.path, u128::MAX)?.is_empty()
  );

  OK
}

/// 测试 RangeIndexManager 检查点屏障异步等待 (使用 compio 异步定时器)
#[test]
fn test_manager_checkpoint_barrier_async_wait() -> Result<()> {
  let env = ManagerEnvGuard::new("barrier_alias");
  let manager = Arc::new(RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap());
  let key = b"barrier_alias_key";

  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree.insert(b"k1", b"v1"), BfTreeInsertResult::Success);

  // 设置屏障
  manager.set_checkpoint_barrier();
  assert!(manager.is_checkpoint_in_progress());

  // 在后台线程使用 compio sleep 清除屏障，验证 wait_for_tree_checkpoint_async 挂起等待不死锁
  let mgr_clone = Arc::clone(&manager);
  let handle = thread::spawn(move || {
    let rt = Runtime::new().expect("创建 compio 运行时失败");
    rt.block_on(async {
      sleep(Duration::from_millis(15)).await;
      mgr_clone.clear_checkpoint_barrier();
    });
  });

  // 屏障持有期间异步挂起等待清屏；后台清屏先行时短路返回，两种返回皆合法
  let rt = Runtime::new().unwrap();
  rt.block_on(manager.wait_for_tree_checkpoint_async(key))
    .unwrap();
  assert!(!manager.is_checkpoint_in_progress());

  handle.join().unwrap();
  OK
}

/// 检查点仅快照屏障设置时已存在的条目；屏障之后新注册的树被跳过
#[test]
fn test_checkpoint_skips_entries_registered_after_barrier() -> Result<()> {
  let env = ManagerEnvGuard::new("barrier_late");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();

  let key_a = b"barrier_early_key";
  manager.create_bftree(key_a, StorageBackendType::Disk, TUNE)?;

  // 屏障设置之后注册的新树：snapshot_pending 保持 false
  manager.set_checkpoint_barrier();
  assert!(manager.is_checkpoint_in_progress());
  let key_b = b"barrier_late_key";
  manager.create_bftree(key_b, StorageBackendType::Disk, TUNE)?;

  let token = 12345u128;
  // 返回实际生成快照文件的树数：屏障后注册的 key_b 被跳过，不计入。
  // 调用方预设的屏障归调用方所有：快照函数保留屏障时序不重设亦不清屏，
  // 清屏责任在调用方（wcpr 持至 flush 完成后统一解除）
  let count = manager.snapshot_all_trees_to_dir(&env.cpr_root.path, token)?;
  assert_eq!(count, 1);
  assert!(manager.is_checkpoint_in_progress());
  manager.clear_checkpoint_barrier();
  assert!(!manager.is_checkpoint_in_progress());

  let token_b32 = encode_u128(token);
  let snap_dir = env.cpr_root.join(token_b32.as_str()).join("rangeindex");
  let prefix_a = RangeIndexManager::base32_prefix_of(key_a);
  let prefix_b = RangeIndexManager::base32_prefix_of(key_b);
  assert!(snap_dir.join(format!("{}.bftree", prefix_a)).exists());
  assert!(!snap_dir.join(format!("{}.bftree", prefix_b)).exists());

  OK
}

/// 预置拷贝失败必须传播错误，绝不静默留下陈旧/半写的 data.bftree 供恢复
/// (1:1 对标 C# PreStageAndRegisterPending 的 File.Copy 异常传播)
#[test]
fn test_pre_stage_copy_failure_propagates() -> Result<()> {
  let env = ManagerEnvGuard::new("copyfail");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let key = b"copy_fail_key";

  let hash_prefix = RangeIndexManager::base32_prefix_of(key);
  // 仅放置刷盘快照，并把 data.bftree 位置占用为目录，迫使预置拷贝必然失败
  fs::write(manager.log_flush_path(&hash_prefix, 0x10), b"snapshot")?;
  fs::create_dir_all(manager.data_file_path(&hash_prefix))?;

  assert!(manager.pre_stage_and_register_pending(key, 0x10).is_err());
  assert_eq!(
    manager.live_index_count(),
    0,
    "拷贝失败不得注册 pending 条目"
  );

  OK
}

/// 迁移发布：快照文件换入数据路径、恢复注册、replace 语义与锁内原子性
/// （对应 PublishMigratedIndex 契约测试：调用方持条带写锁契约）
#[test]
fn test_publish_tree_from_snapshot_locked() -> Result<()> {
  let env = ManagerEnvGuard::new("publish");
  let manager = Arc::new(RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap());
  let key = b"publish_key";
  let key_hash = whasher::fast_hash(key);

  // 源树写入数据并快照导出 (模拟迁移发送端)，随后清理源
  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree.insert(b"k1", b"v1"), BfTreeInsertResult::Success);
  let temp1 = env.ri_root.join("publish_export_1.bftree");
  tree.cpr_snapshot(&temp1)?;
  assert!(manager.delete_index(key)?);

  // 全新 manager (模拟接收端)：replace=false 首次发布成功，数据完整回读
  let recv = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  {
    let _lock = recv.acquire_exclusive_for_delete(key_hash);
    let published = recv.publish_tree_from_snapshot_locked(key, &temp1, false)?;
    assert_eq!(
      published.read(b"k1"),
      (BfTreeReadResult::Found, Some(b"v1".to_vec()))
    );
    // 快照文件经 rename 消费，不再残留
    assert!(!temp1.exists());

    // 再次发布 replace=false → IndexExists
    let data_path = recv.data_file_path_for_key(key);
    let temp_dup = env.ri_root.join("publish_export_dup.bftree");
    fs::copy(&data_path, &temp_dup)?;
    assert!(matches!(
      recv.publish_tree_from_snapshot_locked(key, &temp_dup, false),
      Err(Error::IndexExists)
    ));
    // 拒绝发布不消费快照文件
    assert!(temp_dup.exists());

    // replace=true：旧树排空释放，文件原子换入，新树含全量数据
    assert_eq!(published.insert(b"k2", b"v2"), BfTreeInsertResult::Success);
    let temp2 = env.ri_root.join("publish_export_2.bftree");
    published.cpr_snapshot(&temp2)?;
    let republished = recv.publish_tree_from_snapshot_locked(key, &temp2, true)?;
    assert_eq!(
      republished.read(b"k1"),
      (BfTreeReadResult::Found, Some(b"v1".to_vec()))
    );
    assert_eq!(
      republished.read(b"k2"),
      (BfTreeReadResult::Found, Some(b"v2".to_vec()))
    );
    assert_eq!(recv.live_index_count(), 1, "replace 不得残留双条目");
  }

  OK
}

/// on_truncate 的回收边界：严格小于 new_begin_address 的带地址刷盘快照删除，
/// 等号位与更高地址保留
#[test]
fn test_manager_truncate_reclaims_flush_files() -> Result<()> {
  let env = ManagerEnvGuard::new("trunc_reclaim");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let key = b"trunc_reclaim_key";

  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree.insert(b"k1", b"v1"), BfTreeInsertResult::Success);

  let mut stub = RangeIndexStub::new(
    0,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );
  manager.on_flush_address(key, &mut stub, 0x100)?;
  manager.on_flush_address(key, &mut stub, 0x200)?;
  let hash_prefix = RangeIndexManager::base32_prefix_of(key);

  // 截断到 0x100：等号位保留
  manager.on_truncate(0x100)?;
  assert!(manager.log_flush_path(&hash_prefix, 0x100).exists());

  // 截断到 0x150：< 0x150 的删除，>= 0x150 的保留
  manager.on_truncate(0x150)?;
  assert!(!manager.log_flush_path(&hash_prefix, 0x100).exists());
  assert!(manager.log_flush_path(&hash_prefix, 0x200).exists());

  OK
}

/// pre_stage 对已存在条目不得覆盖 (1:1 对标 C# PreStageAndRegisterPending /
/// RegisterPending 的 TryAdd 语义)：条目已在线激活时到达的预分阶段请求必须保持
/// 原条目——无条件覆盖会把在线树条目换成 pending，同一数据文件被二次开打引擎实例
#[test]
fn test_pre_stage_preserves_existing_entry() -> Result<()> {
  let env = ManagerEnvGuard::new("pre_keep");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let key = b"prestage_keep_key";

  // 在线树 + 源刷盘快照就绪 (满足 pre_stage 的复制前提)
  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree.insert(b"k1", b"v1"), BfTreeInsertResult::Success);
  let hash_prefix = RangeIndexManager::base32_prefix_of(key);
  tree.cpr_snapshot(manager.log_flush_path(&hash_prefix, 0x30))?;

  // 在线条目存在时 pre_stage：TryAdd 失败必须保持原条目原样
  manager.pre_stage_and_register_pending(key, 0x30)?;
  assert_eq!(manager.live_index_count(), 1);
  assert!(Arc::ptr_eq(
    &manager.get_tree(key).expect("在线树不得被 pending 覆盖"),
    &tree
  ));

  // register_pending 同样不得覆盖：条目数与在线树保持不变
  assert!(!manager.register_pending(key));
  assert_eq!(manager.live_index_count(), 1);
  assert!(Arc::ptr_eq(&manager.get_tree(key).unwrap(), &tree));

  OK
}

/// 冷读只打开检查点预置的 data.bftree：检查点之前的旧世代带地址刷盘快照绝不
/// 参与恢复 (1:1 对标 C# RestoreTree 仅 File.Exists(workingPath) + RecoverFromCprSnapshot)
#[test]
fn test_cold_restore_never_consumes_stale_flush_files() -> Result<()> {
  let env = ManagerEnvGuard::new("recov_stub");
  let manager = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  let key = b"recovered_stub_key";
  let hash_prefix = RangeIndexManager::base32_prefix_of(key);
  let token = 12345u128;

  // 旧世代：写入 old_v 并刷出带地址快照；随后更新为 new_v 并产出检查点快照
  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree.insert(b"k1", b"old_v"), BfTreeInsertResult::Success);
  tree.cpr_snapshot(manager.log_flush_path(&hash_prefix, 0x10))?;
  assert_eq!(tree.insert(b"k1", b"new_v"), BfTreeInsertResult::Success);
  tree.cpr_snapshot(RangeIndexManager::checkpoint_snapshot_path_in(
    &env.cpr_root.path,
    token,
    RangeIndexManager::key_id_of(key),
  ))?;

  // 模拟进程重启：全新注册表 + 检查点全量恢复 (预置 data.bftree + pending 注册)
  let restored_mgr = RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap();
  restored_mgr.recover_all_trees_from_dir(&env.cpr_root.path, token)?;

  // wkv 恢复流程对检查点存根标记 mark_recovered_from_checkpoint 后惰性激活
  let mut stub = RangeIndexStub::new(
    0,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );
  stub.mark_recovered_from_checkpoint();
  let recovered = restored_mgr.get_or_open_tree(key, &stub)?;
  assert_eq!(
    recovered.read(b"k1"),
    (BfTreeReadResult::Found, Some(b"new_v".to_vec())),
    "冷读必须取检查点预置的 data.bftree，而非更早的刷盘快照"
  );

  OK
}

/// 空 ri_log_root 必须显式拒绝 (对标 C# 构造器 ArgumentException)：
/// create_dir_all("") 是静默 no-op，不拦截会让数据文件以相对路径散落 CWD
#[test]
fn test_manager_rejects_empty_root() -> Result<()> {
  assert!(matches!(
    RangeIndexManager::new("", "cpr"),
    Err(Error::InvalidArgument(_))
  ));
  OK
}

/// 同名键重建必须清理旧世代刷盘工件：前缀寻址恢复 (存根无逻辑地址) 无法区分
/// 世代，delete 后残留的带地址刷盘快照若不清理，会在新世代淘汰后的惰性
/// 恢复中把新世代工作文件覆盖回旧世代快照
#[test]
fn test_create_purges_old_generation_flush_artifacts() -> Result<()> {
  let env = ManagerEnvGuard::new("purge");
  let manager = Arc::new(RangeIndexManager::new(&env.ri_root.path, &env.cpr_root.path).unwrap());
  let key = b"purge_flush_key";
  let hash_prefix = RangeIndexManager::base32_prefix_of(key);

  // 旧世代：写入、淘汰 (delete_index 语义上保留刷盘快照) 后留下带地址工件
  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree.insert(b"old_k", b"old_v"), BfTreeInsertResult::Success);
  tree.cpr_snapshot(manager.log_flush_path(&hash_prefix, 0x100))?;
  assert!(manager.delete_index(key)?);

  // 同名重建：旧世代刷盘工件必须一并清理
  let tree2 = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(
    tree2.insert(b"new_k", b"new_v"),
    BfTreeInsertResult::Success
  );
  assert!(
    !manager.log_flush_path(&hash_prefix, 0x100).exists(),
    "重建必须清理旧世代带地址刷盘快照"
  );

  // 新世代淘汰后惰性恢复：绝不能复活旧世代数据
  assert!(manager.dispose_tree_under_lock(key, false)?);
  let stub2 = RangeIndexStub::new(
    0,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackendType::Disk,
  );
  let restored = manager.get_or_open_tree(key, &stub2)?;
  assert_ne!(
    restored.read(b"old_k"),
    (BfTreeReadResult::Found, Some(b"old_v".to_vec())),
    "惰性恢复不得回退到旧世代刷盘快照"
  );

  OK
}

/// 验证 RangeIndexManager 与 LightEpoch 协作：读者在纪元保护下安全访问，
/// delete_index 经 bump_current_epoch_action 延迟释放与删文件，
/// 读者退出纪元推进后底层树与文件方才真正释放 (1:1 对标 Garnet storeEpoch.BumpCurrentEpoch)
#[test]
fn test_range_index_manager_epoch_deferred_disposal() -> Result<()> {
  use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread::{spawn, yield_now},
  };

  use wepoch::LightEpoch;

  let env = ManagerEnvGuard::new("epoch_defer");
  let epoch = Arc::new(LightEpoch::new(16));
  let manager = Arc::new(RangeIndexManager::with_epoch(
    &env.ri_root.path,
    &env.cpr_root.path,
    Some(Arc::clone(&epoch)),
  )?);
  let key = b"epoch_deferred_key";

  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  assert_eq!(tree.insert(b"k1", b"v1"), BfTreeInsertResult::Success);

  let tree_clone = Arc::clone(&tree);
  let epoch_clone = Arc::clone(&epoch);
  let reader_entered = Arc::new(AtomicBool::new(false));
  let reader_can_exit = Arc::new(AtomicBool::new(false));
  let reader_read_ok = Arc::new(AtomicBool::new(false));
  let re_entered = Arc::clone(&reader_entered);
  let re_can_exit = Arc::clone(&reader_can_exit);
  let re_read_ok = Arc::clone(&reader_read_ok);

  let reader_handle = spawn(move || {
    let participant = epoch_clone.register().unwrap();
    let guard = participant.enter();
    re_entered.store(true, Ordering::Release);

    // 等待主线程发起 delete_index
    while !re_can_exit.load(Ordering::Acquire) {
      yield_now();
    }

    // 读者仍在纪元内，底层树尚未释放，读取必须成功
    let (res, val) = tree_clone.read(b"k1");
    if res == BfTreeReadResult::Found && val.as_deref() == Some(b"v1") && !tree_clone.is_disposed()
    {
      re_read_ok.store(true, Ordering::Release);
    }

    drop(guard);
  });

  while !reader_entered.load(Ordering::Acquire) {
    yield_now();
  }

  // 外部并发调用 delete_index：从 live_indexes 摘除，但由于后台读者处于纪元保护中，底层树尚未真正 dispose
  assert!(manager.delete_index(key)?);
  assert_eq!(manager.live_index_count(), 0);

  // 通知读者在纪元保护下执行点读
  reader_can_exit.store(true, Ordering::Release);
  reader_handle.join().unwrap();
  assert!(
    reader_read_ok.load(Ordering::Acquire),
    "读者在纪元保护期内读取必须成功且树未释放"
  );

  // 推进纪元触发延迟动作排空
  epoch.bump_current_epoch();
  epoch.drain();

  // 验证底层树已被延迟释放，文件被删除
  assert!(tree.is_disposed());
  let data_path = manager.data_file_path_for_key(key);
  assert!(!data_path.exists());

  OK
}

/// 收割线程持同一条带锁时的退让—重投闭环：延迟动作被条带写锁占用挡下时
/// 绝不阻塞（同线程自死锁），也不裁决删除，而是排队待释放驱动线程经
/// harvest_release_retries 重投同一内核后落地删除
#[test]
fn test_release_detached_defers_on_stripe_contention() -> Result<()> {
  use wepoch::LightEpoch;
  use whasher::fast_hash;

  let env = ManagerEnvGuard::new("epoch_contention");
  let epoch = Arc::new(LightEpoch::new(16));
  let manager = Arc::new(RangeIndexManager::with_epoch(
    &env.ri_root.path,
    &env.cpr_root.path,
    Some(Arc::clone(&epoch)),
  )?);
  let key = b"contended_release_key";
  let tree = manager.create_bftree(key, StorageBackendType::Disk, TUNE)?;
  let data_path = manager.data_file_path_for_key(key);
  assert!(data_path.exists(), "前置条件：数据文件在盘");

  let detached = manager.detach_tree(key, true).unwrap();
  // 模拟收割线程已持该条带写锁的上下文（detach 已完成，锁在此重新取得）
  let _stripe_lock = manager.acquire_exclusive_for_delete(fast_hash(key));
  manager.release_detached(detached);
  assert!(
    tree.is_disposed() && data_path.exists(),
    "旧引擎可先行弃置，但条带锁被占时 unlink 必须让位而非阻塞或删除"
  );

  // 退让批次由驱动线程重投：锁仍被持时再次退让，不丢条目也不落地
  assert_eq!(
    manager.harvest_release_retries(8),
    1,
    "让位批次须由 harvest_release_retries 交回同一释放内核"
  );
  assert!(data_path.exists(), "持锁期间重投仍应退让");

  drop(_stripe_lock);
  assert_eq!(manager.harvest_release_retries(8), 1);
  assert!(
    !data_path.exists(),
    "条带锁腾空后重投的批次须在世代复查下完成删除"
  );

  OK
}
