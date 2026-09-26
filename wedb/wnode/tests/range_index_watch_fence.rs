//! RangeIndex 写路径 WATCH 版本栅栏回归测试
//!
//! 缺陷（transpile 票：range-index 并发写共享读锁丢更新与 WATCH 漏推进）：
//! wkv range_index_set / range_index_set_batch 成功写入后完全漏调
//! bump_watch_version；range_index_del 仅删空臂推进，非删空实删不推进 →
//! WATCH 该索引的 MULTI/EXEC 在并发 RI.SET / RI.SETBATCH / RI.DEL 修改后
//! 仍判有效，脏提交，事务隔离破坏。
//!
//! 对标 C# 主存/对象域写钩子 functionsState.watchVersionMap.IncrementVersion
//!（garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:79/:100/:125/:200、
//! UpsertMethods.cs:48/:58/:68、DeleteMethods.cs:21/:30；
//! garnet/libs/server/Transaction/WatchVersionMap.cs:IncrementVersion）：
//! 任意用户键实写在存储层无条件推进观察者版本；rust 修复后 RI 三写臂在独占
//! 守卫释放后按同款判据（树内容实际变更）恰一次推进。
//!
//! 断言口径：
//! - RI.SET 新增 / 覆盖写、RI.SETBATCH（含仅覆盖批次）、RI.DEL 非删空实删、
//!   RI.DEL 删空自愈臂 → 版本恰一次推进，EXEC 中止（run 返回 false）；
//! - 纯读臂（RI.GET / RI.COUNT / RI.SCAN）与拒绝臂（InvalidKV / 不存在字段
//!   幂等无实删）零推进，EXEC 成功——对齐 C# 纯读 Read 面与拒绝臂不
//!   IncrementVersion；
//! - 删空自愈臂不双计（drain 物理键原语 + 本层显式推进合计恰一次）。

use std::{sync::Arc, time::Duration};

use aok::Void;
use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::{ScanReturnField, StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::storage::session::storage_session::version_map_watch_hook;
use wtxn::{TransactionManager, TxnKeyEntryComparison, TxnLockTable, WatchVersionMap};
use wval::SessionPrefixBuf;

type TestStore = WedbStore<SegmentedDevice>;

/// 与 store 层测试一致的默认树调优：min_record=8 / max_record=1024 / max_key_len=128
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 测试环境：真实引擎 + 共享版本表 + 引擎级写面钩子（与生产装配同径，
/// 对标 tiered_watch_fence.rs 的 Env 骨架）
struct Env {
  store: Arc<TestStore>,
  map: Arc<WatchVersionMap>,
  lock_table: TxnLockTable,
  _dir: tempfile::TempDir,
}

fn env(tag: &str) -> aok::Result<Env> {
  let dir = tempdir()?;
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&map))),
    "引擎级写面钩子应首次挂载"
  );
  Ok(Env {
    store,
    map,
    lock_table: TxnLockTable::new(),
    _dir: dir,
  })
}

/// 版本表读点（与 wtxn EXEC 校验同一哈希面：根域 scoped，写会话默认 (0,0)）
fn ver(env: &Env, key: &[u8]) -> u64 {
  env
    .map
    .read_version(TxnKeyEntryComparison::scoped_key_hash(root().as_slice(), key) as u64)
}

fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}

/// 新开登记单键 WATCH 的事务管理器（对标 RESP WATCH 后待 EXEC 的会话）
fn watch(env: &Env, key: &[u8]) -> TransactionManager {
  let mut txn = TransactionManager::new(env.lock_table.clone(), Arc::clone(&env.map), None);
  txn.watch(root().as_slice(), key);
  txn
}

/// EXEC 校验（版本变化返回 false = 中止提交，对标 C# WatchVersionMapValidate 失败回 nil）
fn exec(txn: &mut TransactionManager) -> bool {
  txn.run(root().as_slice(), false, false, Duration::ZERO)
}

/// RI.SET 新增与覆盖写、RI.SETBATCH、RI.DEL 非删空与删空臂全部推进栅栏、
/// EXEC 中止；纯读臂与拒绝臂零推进、EXEC 成功
#[test]
fn ri_write_arms_abort_watched_exec() -> Void {
  Runtime::new()?.block_on(async {
    let env = env("ri_watch_fence.db")?;
    let session = env.store.new_session()?;
    let key = b"watched_idx";
    session
      .range_index_create(key, StorageBackendType::Disk, TUNE)
      .await?;

    // 1. RI.SET 新增字段：恰一次推进，EXEC 中止
    let txn = watch(&env, key);
    let v0 = ver(&env, key);
    session.range_index_set(key, b"alpha", b"val_a").await?;
    assert_eq!(ver(&env, key), v0 + 1, "RI.SET 新增应恰一次推进");
    let mut txn = txn;
    assert!(!exec(&mut txn), "WATCH 后 RI.SET 修改，EXEC 必须中止");

    // 2. RI.SET 覆盖已有字段（is_new=false 但树值已改）：同样推进、中止
    let mut txn = watch(&env, key);
    let v0 = ver(&env, key);
    session.range_index_set(key, b"alpha", b"val_a2").await?;
    assert_eq!(ver(&env, key), v0 + 1, "覆盖写同样实改树内容，恰一次推进");
    assert!(!exec(&mut txn), "WATCH 后覆盖写，EXEC 必须中止");

    // 3. RI.SETBATCH：新增与纯覆盖批次均推进、中止
    let mut txn = watch(&env, key);
    session
      .range_index_set_batch(
        key,
        &[
          (b"bravo".as_slice(), b"val_b".as_slice()),
          (b"charlie".as_slice(), b"val_c".as_slice()),
        ],
      )
      .await?;
    assert!(!exec(&mut txn), "WATCH 后 RI.SETBATCH 修改，EXEC 必须中止");
    let mut txn = watch(&env, key);
    session
      .range_index_set_batch(key, &[(b"alpha".as_slice(), b"val_a3".as_slice())])
      .await?;
    assert!(!exec(&mut txn), "WATCH 后纯覆盖 SETBATCH 同样须中止");

    // 4. RI.DEL 非删空实删（size 3 → 2）：补齐漏推进的栅栏，EXEC 中止
    let mut txn = watch(&env, key);
    assert!(session.range_index_del(key, b"bravo").await?);
    assert!(!exec(&mut txn), "WATCH 后非删空 RI.DEL 修改，EXEC 必须中止");

    // 5. 拒绝臂零推进：InvalidKV 长度违约与不存在字段幂等无实删
    let v_before = ver(&env, key);
    assert!(session.range_index_set(key, b"k", b"v").await.is_err());
    session.range_index_del(key, b"ghost_field").await?;
    assert_eq!(
      ver(&env, key),
      v_before,
      "拒绝臂与零实删臂不得推进栅栏（禁双计/误杀）"
    );

    // 6. 纯读臂零推进：GET / COUNT / SCAN 不误杀 WATCH 事务
    let mut txn = watch(&env, key);
    assert_eq!(
      session.range_index_get(key, b"alpha").await?.as_deref(),
      Some(b"val_a3".as_slice())
    );
    assert_eq!(session.range_index_count(key).await?, 2);
    session
      .range_index_scan_stream(key, b"alpha", 10, ScanReturnField::Key, |_, _| true)
      .await?;
    assert!(exec(&mut txn), "纯读臂不得误杀 WATCH 事务，EXEC 必须成功");

    // 7. RI.DEL 删空自愈臂：恰一次推进（drain 物理键原语 + 本层显式收口
    // 合计不双计），EXEC 中止
    let mut txn = watch(&env, key);
    assert!(session.range_index_del(key, b"charlie").await?);
    let mut txn_after = {
      assert!(!exec(&mut txn), "WATCH 后删空自愈，EXEC 必须中止");
      watch(&env, key)
    };
    let v0 = ver(&env, key);
    assert!(session.range_index_del(key, b"alpha").await?);
    assert_eq!(ver(&env, key), v0 + 1, "删空臂合计恰一次推进，绝不双计");
    assert!(
      !session.range_index_exists(key).await?,
      "前置条件：末字段删除已触发删空自愈"
    );
    assert!(!exec(&mut txn_after), "WATCH 后被删空回收，EXEC 必须中止");

    aok::Result::<()>::Ok(())
  })?;
  aok::OK
}
