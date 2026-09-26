//! 迁移帧导入批量折叠证伪用例（fix-migrate-import-batch / N19）
//!
//! 对标 C# `libs/cluster/Session/RespClusterMigrateCommands.cs:Process`：C# 迁移
//! 接收是逐键 `if (replaceOption || !Exists(keySlice)) basicGarnetApi.SET(...)`
//! 的单条循环，全链无批量折叠机制。本仓对 `import_migration_frames` 的定向修复
//! 只在保持这一逐键语义的前提下，把存在门与 TTL 回填从「逐键多次独立异步调度」
//! 收敛为「同步快路径 + 循环前缀外提」，因此必须以「折叠不改写逐键落盘语义」为
//! 不变式证伪。
//!
//! 三条等价断言：
//! 1. 批量 == 逐条：一次 `import_migration_frames(frames)` 与逐帧各调一次
//!    `import_migration_frames(vec![frame])` 落库结果逐项相等（存在门/写序/TTL
//!    回填的折叠不改变终态）。
//! 2. 折叠 == 参照异步原语：导入落库（走 `probe_alive_with_prefix` +
//!    `put_ttl_sync`）与直接 `upsert_* + expire_at_ticks`（未折叠的异步慢路径
//!    参照）读回的值字节与 TTL 毫秒逐字节恒等——证伪「put_ttl_sync 裸写与
//!    expire_at 会话入口恒等直通落库值逐字节恒等」「毫秒换算 ticks 恒 16
//!    对齐」（§143 收口形，粗化唯命令边界单点）两处折叠论断。
//! 3. replace=false 存在门：预存键在批量与逐条下均被判存活跳过写入，值原样保留
//!    （对标 C# `!Exists` 跳写）。

use std::sync::Arc;

use parking_lot::Mutex;
use wbase::{
  convert::{expire_at_milliseconds_to_ticks, unix_time_in_milliseconds_from_ticks},
  time::now_ticks,
};
use wconn::record::{MigrateVal, MigrationFrame, MigrationRecord};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  migration::{
    chunk_reassembler::ChunkReassembler,
    frame_import::{FrameImport, import_migration_frames},
    migrate_driver::{LiveValue, read_live_value},
  },
};
use wkv::WedbStore;
use wnode::StorageSession;
use wtest_base::test_store_config;
use wval::KeyTag;

// 折叠语义对拍键值面：两 Str（一含 TTL 一不含）+ 两 Env（内层标签 Hash=3 /
// Set=4，各一含 TTL 一不含），覆盖两条写分支与 TTL 回填分支。
const K_STR_TTL: &[u8] = b"mig:s:ttl";
const V_STR_TTL: &[u8] = b"alpha-value";
const K_STR_NOTTL: &[u8] = b"mig:s:plain";
const V_STR_NOTTL: &[u8] = b"beta";
const K_ENV_TTL: &[u8] = b"mig:h:ttl";
// 信封整值 = [内层标签][载荷]，标签 0x03 = Hash
const V_ENV_TTL: &[u8] = b"\x03hash-payload";
const K_ENV_NOTTL: &[u8] = b"mig:set:plain";
// 标签 0x04 = Set
const V_ENV_NOTTL: &[u8] = b"\x04set-payload";

/// 打开小预算测试存储（GC 关闭，与集群会话测试同款）
fn open_store(tag: &str) -> Arc<WedbStore<SegmentedDevice>> {
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join(tag)).unwrap());
  Arc::new(WedbStore::open(test_store_config(), device).unwrap())
}

/// 一次迁移帧导入（MIGRATE 导槽链形态：拒域帧、无 RI 接收态、向量登记槽占位）
async fn run_import(
  store: &Arc<WedbStore<SegmentedDevice>>,
  provider: &Arc<ClusterProvider>,
  frames: Vec<MigrationFrame<'static>>,
  replace: bool,
) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  let chunks = Mutex::new(ChunkReassembler::new());
  let ri = None;
  let import = FrameImport {
    provider,
    session: &session,
    storage: &storage,
    chunks: &chunks,
    ri: &ri,
    replace,
    vector_slot: 0,
    accept_domain_frames: false,
  };
  import_migration_frames(frames, &import)
    .await
    .expect("迁移帧导入应成功");
}

/// 读回一个键的落盘值字节与 TTL 毫秒（不可迁移即判负）
async fn read_back(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> (Vec<u8>, i64) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  match read_live_value(&storage, None, key).await.unwrap() {
    LiveValue::Migratable(MigrateVal::Str(v), expire)
    | LiveValue::Migratable(MigrateVal::Env(v), expire) => (v, expire),
    _ => panic!("键 {} 应可迁移读回", String::from_utf8_lossy(key)),
  }
}

/// 批量帧集（借用 'static 常量，帧与逐条复用同一序列）
fn sample_frames(expire_ms: i64) -> Vec<MigrationFrame<'static>> {
  vec![
    MigrationFrame::Record(MigrationRecord::Str {
      key: K_STR_TTL,
      val: V_STR_TTL,
      expire_unix_ms: expire_ms,
    }),
    MigrationFrame::Record(MigrationRecord::Env {
      key: K_ENV_TTL,
      env: V_ENV_TTL,
      expire_unix_ms: expire_ms,
    }),
    MigrationFrame::Record(MigrationRecord::Str {
      key: K_STR_NOTTL,
      val: V_STR_NOTTL,
      expire_unix_ms: 0,
    }),
    MigrationFrame::Record(MigrationRecord::Env {
      key: K_ENV_NOTTL,
      env: V_ENV_NOTTL,
      expire_unix_ms: 0,
    }),
  ]
}

/// 参照路径：未折叠的逐键异步写原语（upsert + expire_at_ticks），复刻 C#
/// `replaceOption || !Exists` 后 `basicGarnetApi.SET` 的异步语义
async fn write_reference(store: &Arc<WedbStore<SegmentedDevice>>, expire_ms: i64) {
  let ticks = expire_at_milliseconds_to_ticks(expire_ms);
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  storage.upsert_string(K_STR_TTL, V_STR_TTL).await.unwrap();
  storage.expire_at_ticks(K_STR_TTL, ticks).await.unwrap();
  storage
    .upsert_tag(K_ENV_TTL, KeyTag::ObjectEnvelope, V_ENV_TTL)
    .await
    .unwrap();
  storage.expire_at_ticks(K_ENV_TTL, ticks).await.unwrap();
  storage
    .upsert_string(K_STR_NOTTL, V_STR_NOTTL)
    .await
    .unwrap();
  storage
    .upsert_tag(K_ENV_NOTTL, KeyTag::ObjectEnvelope, V_ENV_NOTTL)
    .await
    .unwrap();
}

/// 三存储读回逐项比对：值字节 + TTL 毫秒
async fn assert_stores_match(
  batch: &Arc<WedbStore<SegmentedDevice>>,
  per: &Arc<WedbStore<SegmentedDevice>>,
  reference: &Arc<WedbStore<SegmentedDevice>>,
) {
  for key in [K_STR_TTL, K_ENV_TTL, K_STR_NOTTL, K_ENV_NOTTL] {
    let b = read_back(batch, key).await;
    let p = read_back(per, key).await;
    let r = read_back(reference, key).await;
    assert_eq!(
      b,
      p,
      "批量与逐条落库终态不等价: {}",
      String::from_utf8_lossy(key)
    );
    assert_eq!(
      b,
      r,
      "折叠与参照异步原语落盘不等价: {}",
      String::from_utf8_lossy(key)
    );
  }
}

/// 证伪主用例：批量导入 == 逐条导入 == 参照异步原语（值字节 + TTL 逐字节对拍）
#[compio::test]
async fn migrate_import_batch_equals_per_record_and_reference() {
  // 未来 TTL：非 16 对齐毫秒值也须与参照异步路径逐字节恒等（tick 落库对拍）
  let expire_ms = unix_time_in_milliseconds_from_ticks(now_ticks()) + 60_000 + 7;
  let provider = ClusterProvider::new();

  let batch_store = open_store("mig_batch.db");
  let per_store = open_store("mig_per.db");
  let ref_store = open_store("mig_ref.db");
  provider.set_store(Arc::clone(&batch_store));

  // 1) 批量：单次折叠导入全部帧
  run_import(&batch_store, &provider, sample_frames(expire_ms), true).await;

  // 2) 逐条：每帧各调一次导入（无折叠，逐次进入 batch/纪元）
  for frame in sample_frames(expire_ms) {
    run_import(&per_store, &provider, vec![frame], true).await;
  }

  // 3) 参照：直接异步写原语
  write_reference(&ref_store, expire_ms).await;

  assert_stores_match(&batch_store, &per_store, &ref_store).await;

  // 帧→落盘值字节对拍：导入面存的就是帧内值（无二次编码/裁剪）
  assert_eq!(read_back(&batch_store, K_STR_TTL).await.0, V_STR_TTL);
  assert_eq!(read_back(&batch_store, K_ENV_TTL).await.0, V_ENV_TTL);
  assert_eq!(read_back(&batch_store, K_STR_NOTTL).await.0, V_STR_NOTTL);
  assert_eq!(read_back(&batch_store, K_ENV_NOTTL).await.0, V_ENV_NOTTL);
  // TTL 回填对拍：含 TTL 键读回原毫秒、无 TTL 键读回 0
  assert_eq!(read_back(&batch_store, K_STR_TTL).await.1, expire_ms);
  assert_eq!(read_back(&batch_store, K_ENV_TTL).await.1, expire_ms);
  assert_eq!(read_back(&batch_store, K_STR_NOTTL).await.1, 0);
  assert_eq!(read_back(&batch_store, K_ENV_NOTTL).await.1, 0);
}

/// 证伪存在门折叠：replace=false 下预存键在批量与逐条两形态均判存活跳过写入
#[compio::test]
async fn migrate_import_replace_false_skips_existing_batch_and_per_record() {
  let expire_ms = unix_time_in_milliseconds_from_ticks(now_ticks()) + 60_000;
  let provider = ClusterProvider::new();

  let batch_store = open_store("mig_gate_batch.db");
  let per_store = open_store("mig_gate_per.db");

  // 预存：K_STR_TTL 已有旧值、K_ENV_TTL 已有旧信封（无 TTL）
  for store in [&batch_store, &per_store] {
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    storage.upsert_string(K_STR_TTL, b"OLD").await.unwrap();
    storage
      .upsert_tag(K_ENV_TTL, KeyTag::ObjectEnvelope, b"\x03OLD")
      .await
      .unwrap();
  }
  provider.set_store(Arc::clone(&batch_store));

  // 仅迁移两预存键，replace=false：存在门须跳写（对标 C# !Exists）
  let gated = || {
    vec![
      MigrationFrame::Record(MigrationRecord::Str {
        key: K_STR_TTL,
        val: V_STR_TTL,
        expire_unix_ms: expire_ms,
      }),
      MigrationFrame::Record(MigrationRecord::Env {
        key: K_ENV_TTL,
        env: V_ENV_TTL,
        expire_unix_ms: expire_ms,
      }),
    ]
  };
  run_import(&batch_store, &provider, gated(), false).await;
  for frame in gated() {
    run_import(&per_store, &provider, vec![frame], false).await;
  }

  // 预存值原样保留、无 TTL（回填只作用于新写键，跳写键不回填）
  for store in [&batch_store, &per_store] {
    let s = read_back(store, K_STR_TTL).await;
    assert_eq!(s.0, b"OLD", "replace=false 预存 String 键不应被覆写");
    assert_eq!(s.1, 0, "跳写键不应被回填 TTL");
    let e = read_back(store, K_ENV_TTL).await;
    assert_eq!(e.0, b"\x03OLD", "replace=false 预存信封键不应被覆写");
    assert_eq!(e.1, 0, "跳写信封键不应被回填 TTL");
  }
}
