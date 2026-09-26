//! 后台降阶候选发现的登记表驱动面测试（task/ing/demote-candidate-domain-scan.md）
//!
//! 预筛改为直接遍历换号回收旁表登记键 + 逐键 load_meta 点读后，候选发现与主存
//! 日志规模解耦。覆盖口径：
//! 1. 升阶冷键在大量信封噪音记录在场时仍被发现并降阶，噪音信封键零入围零副作用；
//! 2. RI 登记键不入围：旁表同时登记 RangeIndex 与升阶集合，仅四族集合入候选，
//!    RI 键零触碰（重复创建仍报 AlreadyExists 即存证）；
//! 3. 注销闭环：降阶随树清退摘除旁表登记，后续轮零候选静默；
//! 4. 纯信封日志（零分层键）零候选：信封记录不得被误当降阶候选。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::{StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::{RangeIndexError, StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    objects::tiered_demote::{TieredDemoteStats, tiered_demote_round},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::version_map_watch_hook,
};
use wresp::command::RespCommand;
use wtxn::WatchVersionMap;
use wval::GarnetObjectType;

type TestStore = WedbStore<SegmentedDevice>;

/// 测试环境：真实引擎 + 引擎级写面钩子挂载共享版本表（与生产装配同径，
/// tiered_background_demote 同款）
struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  api: GarnetApi,
  _map: Arc<WatchVersionMap>,
  _dir: tempfile::TempDir,
}

fn env(tag: &str) -> Env {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&map))),
    "引擎级写面钩子应首次挂载"
  );
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  Env {
    rt: Runtime::new().unwrap(),
    store,
    api,
    _map: map,
    _dir: dir,
  }
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

/// 慢路径命令同步求值并回帧字节（tiered_background_demote 同款泵）
fn auto_exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 分层态判据：BfTree 元记录存根在册（wkv load_collection_stub 权威读）
fn is_tiered(env: &Env, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

/// 「分层态但双维齐低」冷键直接灌（wkv promote，同 tiered_background_demote
/// 跨域用例构造）：22000 字段远低于双低水位，无前台写触碰，只能由后台轮回收
fn promote_cold_hash(env: &Env, key: &[u8]) {
  let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..22000_usize)
    .map(|i| (format!("f{i}").into_bytes(), b"v".to_vec()))
    .collect();
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(
      key,
      GarnetObjectType::Hash,
      entries,
      i64::MAX,
      false,
    ))
    .unwrap();
}

/// 登记表发现主流程：RI 登记键 + 信封噪音在场时，升阶冷键仍被发现并降阶；
/// RI 键零触碰、噪音键零入围；降阶注销闭环后零候选静默
#[test]
fn registry_discovery_demotes_cold_key_and_excludes_ri_and_noise() {
  let env = env("tdd-registry.db");
  let mut s = session_with(&env);

  // 升阶冷键（登记面：集合升阶注册）
  promote_cold_hash(&env, b"cold");
  assert!(is_tiered(&env, b"cold"), "升阶冷键应已就位");

  // RI 登记键（登记面：RI.CREATE 注册）：四族白名单外，不得入围
  {
    let sess = env.store.new_session().unwrap();
    env
      .rt
      .block_on(sess.range_index_create(b"idx", StorageBackendType::Disk, TreeTuning::DEFAULT_RI))
      .unwrap();
  }

  // 信封噪音：2000 个小哈希（hlog 大量非 Meta 记录，旁表零登记）
  for i in 0..2000_usize {
    let k = format!("n{i}");
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Hset, &[k.as_bytes(), b"f", b"v"]),
      b":1\r\n"
    );
  }

  let stats = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(
    stats,
    TieredDemoteStats {
      candidates: 1,
      demoted: 1,
      aborted: 0
    },
    "升阶冷键唯一入围并降阶；RI 键与信封噪音键零入围"
  );
  assert!(!is_tiered(&env, b"cold"), "冷键元记录与树存根应释放");

  // RI 键零触碰：索引仍存活（重复创建报 AlreadyExists 即存证）
  {
    let sess = env.store.new_session().unwrap();
    assert_eq!(
      env
        .rt
        .block_on(sess.range_index_create(b"idx", StorageBackendType::Disk, TreeTuning::DEFAULT_RI))
        .unwrap_err(),
      RangeIndexError::AlreadyExists,
      "RI 键不得被降阶评估触碰或清除"
    );
  }

  // 冷键降阶后数据保真，形态回归内存信封
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"cold"]),
    b":22000\r\n"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"cold", b"f7"]),
    b"$1\r\nv\r\n"
  );

  // 注销闭环：旁表仅剩 RI 登记（非四族不入围），后续轮零候选静默
  let stats2 = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(stats2, TieredDemoteStats::default(), "降阶后零候选静默");
}

/// 纯信封日志零候选：零分层键时无论日志多长，轮次恒零候选零副作用
#[test]
fn envelope_only_log_yields_zero_candidates() {
  let env = env("tdd-envelope.db");
  let mut s = session_with(&env);

  for i in 0..500_usize {
    let k = format!("n{i}");
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Hset, &[k.as_bytes(), b"f", b"v"]),
      b":1\r\n"
    );
  }

  let stats = env.rt.block_on(tiered_demote_round(&env.store));
  assert_eq!(
    stats,
    TieredDemoteStats::default(),
    "信封键不得被误当降阶候选"
  );
}
