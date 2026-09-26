//! 删空自愈与偏差锁回归测试（工单 zcode-r40-delempty）
//!
//! 包含：
//! 1. 发现一（P1 代码级）：删空 drain 臂 Error::Swapped 失败窗键已死而 WATCH 版本推进收口
//!    - apply_rmw_post_operate 空对象臂失败分级：Swapped 推进 WATCH 版本并回错误帧、键已死、abort 事务；
//!    - sweep 臂与 apply 臂行为同构断言防回摆；
//!    - wkv delete() 与 upsert_tag 覆写清退臂同判据收口；
//! 2. 发现二（deviations.md 第 19 条）：C# InitialUpdater 缺键 HDEL/HPERSIST/LSET 幻键拒挂语义锁；
//! 3. 发现三（deviations.md 第 47 条）：读臂删空自愈（HEXPIRE 全字段即刻到期后 HLEN 回收）语义锁。

use std::{
  sync::{Arc, atomic::Ordering},
  thread::sleep,
  time::Duration,
};

use aok::Void;
use compio::runtime::Runtime;
use parking_lot::Mutex;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wbftree::DELETE_INDEX_FAIL_INJECT;
use wcol::types::member_ttl::encode_member;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::version_map_watch_hook,
};
use wresp::command::RespCommand;
use wtxn::{TransactionManager, TxnKeyEntryComparison, TxnLockTable, WatchVersionMap};
use wval::{GarnetObjectType, SessionPrefixBuf};

type TestStore = WedbStore<SegmentedDevice>;

/// 全局故障注入单测互斥锁（防并发测试抢占静态注入标志）
static INJECT_LOCK: parking_lot::Mutex<()> = Mutex::new(());

struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  map: Arc<WatchVersionMap>,
  api: GarnetApi,
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
    map,
    api,
    _dir: dir,
  }
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

fn auto_exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

fn watched(map: &Arc<WatchVersionMap>, key: &[u8]) -> TransactionManager {
  let mut txn = TransactionManager::new(TxnLockTable::new(), Arc::clone(map), None);
  txn.watch(SessionPrefixBuf::ROOT.as_slice(), key);
  txn
}

fn exec(txn: &mut TransactionManager) -> bool {
  txn.run(
    SessionPrefixBuf::ROOT.as_slice(),
    false,
    false,
    Duration::ZERO,
  )
}

fn ver(env: &Env, key: &[u8]) -> u64 {
  env
    .map
    .read_version(
      TxnKeyEntryComparison::scoped_key_hash(SessionPrefixBuf::ROOT.as_slice(), key) as u64,
    )
}

fn is_tiered(env: &Env, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

fn promote_hash(env: &Env, key: &[u8], entries: Vec<(&[u8], &[u8])>) {
  let sess = env.store.new_session().unwrap();
  let recs: Vec<(Vec<u8>, Vec<u8>)> = entries
    .into_iter()
    .map(|(f, v)| (f.to_vec(), encode_member(v, None)))
    .collect();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(key, GarnetObjectType::Hash, recs, i64::MAX, false))
    .unwrap();
}

fn promote_expired_hash(env: &Env, key: &[u8], count: usize) {
  let sess = env.store.new_session().unwrap();
  let past = now_ticks() - TICKS_PER_SECOND;
  let recs: Vec<(Vec<u8>, Vec<u8>)> = (1..=count)
    .map(|i| {
      (
        format!("e{i}").into_bytes(),
        encode_member(format!("v{i}").as_bytes(), Some(past)),
      )
    })
    .collect();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(key, GarnetObjectType::Hash, recs, past, false))
    .unwrap();
}

// -----------------------------------------------------------------------------
// 发现一（P1 代码级）：删空 drain 臂 Error::Swapped 失败窗版本推进测试
// -----------------------------------------------------------------------------

/// 测试 apply_rmw_post_operate 空对象臂在 drain 失败（Error::Swapped）时：
/// 1. 命令应答存储忙错误帧（-ERR ...）；
/// 2. 键已死（路由域元记录已墓碑化，is_tiered 为假，EXISTS 为 0）；
/// 3. WATCH 该键的并发事务 EXEC 被 abort，版本表已推进；
/// 4. 再次读取按键不存在回零。
#[test]
fn test_apply_post_operate_drain_swapped_bumps_watch_and_aborts_txn() -> Void {
  let _lock = INJECT_LOCK.lock();
  let env = env("apply-drain-swapped.db");
  let key = b"th1";
  promote_hash(&env, key, vec![(b"f1", b"v1")]);
  assert!(is_tiered(&env, key), "前置：键为分层态");
  assert_eq!(ver(&env, key), 0);

  let mut txn = watched(&env.map, key);
  let mut s = session_with(&env);

  DELETE_INDEX_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out = auto_exec(&env, &mut s, RespCommand::Hdel, &[key, b"f1"]);
  assert!(
    out.starts_with(b"-ERR "),
    "drain 失败必须回存储错误帧，实际: {}",
    String::from_utf8_lossy(&out)
  );

  assert_eq!(
    ver(&env, key),
    1,
    "Swapped 失败窗元记录墓碑已落盘，WATCH 版本必须已推进"
  );
  assert!(!exec(&mut txn), "WATCH 该键的事务因版本已推进必须被 abort");
  assert!(
    !is_tiered(&env, key),
    "键路由域已消亡（元记录墓碑先于树注销落盘）"
  );

  // 验证键读面确认消亡
  let out_exists = auto_exec(&env, &mut s, RespCommand::Exists, &[key]);
  assert_eq!(out_exists, b":0\r\n", "消亡键 EXISTS 应为 0");

  let out_hlen = auto_exec(&env, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(out_hlen, b":0\r\n", "消亡键 HLEN 应为 0");

  Ok(())
}

/// 验证 sweep 臂与 apply 臂行为同构（防回摆）：
/// sweep 臂（HLEN 到期剔空）与 apply 臂（HDEL 删空）在 DELETE_INDEX_FAIL_INJECT 下：
/// 均返回存储错误帧、均推进版本表、均 abort WATCH 事务、均确认键消亡。
#[test]
fn test_isomorphism_sweep_arm_and_apply_arm() -> Void {
  let _lock = INJECT_LOCK.lock();
  let env = env("isomorphism.db");

  // 1. sweep 臂
  let k_sweep = b"ksweep";
  promote_expired_hash(&env, k_sweep, 2);
  let mut txn_sweep = watched(&env.map, k_sweep);
  let mut s1 = session_with(&env);

  DELETE_INDEX_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out_sweep = auto_exec(&env, &mut s1, RespCommand::Hlen, &[k_sweep]);
  assert!(out_sweep.starts_with(b"-ERR "));
  assert_eq!(ver(&env, k_sweep), 1);
  assert!(!exec(&mut txn_sweep));
  assert!(!is_tiered(&env, k_sweep));
  assert_eq!(
    auto_exec(&env, &mut s1, RespCommand::Exists, &[k_sweep]),
    b":0\r\n"
  );

  // 2. apply 臂
  let k_apply = b"kapply";
  promote_hash(&env, k_apply, vec![(b"f1", b"v1")]);
  let mut txn_apply = watched(&env.map, k_apply);
  let mut s2 = session_with(&env);

  DELETE_INDEX_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out_apply = auto_exec(&env, &mut s2, RespCommand::Hdel, &[k_apply, b"f1"]);
  assert!(out_apply.starts_with(b"-ERR "));
  assert_eq!(ver(&env, k_apply), 1);
  assert!(!exec(&mut txn_apply));
  assert!(!is_tiered(&env, k_apply));
  assert_eq!(
    auto_exec(&env, &mut s2, RespCommand::Exists, &[k_apply]),
    b":0\r\n"
  );

  Ok(())
}

/// 测试 wkv StoreSession::delete 在 drain 失败（Error::Swapped）时推进 WATCH 版本
#[test]
fn test_wkv_delete_drain_swapped_bumps_watch() -> Void {
  let _lock = INJECT_LOCK.lock();
  let env = env("wkv-delete-swapped.db");
  let key = b"tdel";
  promote_hash(&env, key, vec![(b"f1", b"v1")]);
  assert!(is_tiered(&env, key));
  assert_eq!(ver(&env, key), 0);

  let mut txn = watched(&env.map, key);
  let sess = env.store.new_session().unwrap();

  DELETE_INDEX_FAIL_INJECT.store(true, Ordering::SeqCst);
  let res = env.rt.block_on(sess.delete(key));
  assert!(res.is_err(), "delete 在注销失败时上抛错误");
  assert_eq!(
    ver(&env, key),
    1,
    "wkv delete 在 Swapped 时必须推进 WATCH 版本"
  );
  assert!(!exec(&mut txn), "WATCH 事务必须被 abort");
  assert!(!is_tiered(&env, key), "元记录墓碑已生效，键已消亡");

  Ok(())
}

/// 测试 wkv StoreSession::upsert_tag（SET 覆写分层键）在 drain 失败（Error::Swapped）时推进 WATCH 版本
#[test]
fn test_wkv_upsert_tag_overwrite_drain_swapped_bumps_watch() -> Void {
  let _lock = INJECT_LOCK.lock();
  let env = env("wkv-upsert-swapped.db");
  let key = b"tset";
  promote_hash(&env, key, vec![(b"f1", b"v1")]);
  assert!(is_tiered(&env, key));
  assert_eq!(ver(&env, key), 0);

  let mut txn = watched(&env.map, key);
  let sess = env.store.new_session().unwrap();

  DELETE_INDEX_FAIL_INJECT.store(true, Ordering::SeqCst);
  let res = env.rt.block_on(sess.upsert(key, b"new_str_value"));
  assert!(res.is_err(), "upsert 在清退注销失败时上抛错误");
  // 写序收口（票 zcode-r34-writekernel 条目一）：String 域数据落笔重排于
  // meta drain 之前——失败窗内数据提交与元记录墓碑各为一次真实写效果，
  // WATCH 版本推进两次（数据 0→1、墓碑 1→2），比对旧时序「拒绝写、仅墓碑
  // 一次推进」更精确反映窗内实际变更面
  assert_eq!(
    ver(&env, key),
    2,
    "wkv upsert 在 Swapped 时必须推进 WATCH 版本（数据提交 + 元记录墓碑各一次）"
  );
  assert!(!exec(&mut txn), "WATCH 事务必须被 abort");
  assert!(!is_tiered(&env, key), "元记录墓碑已生效，分层态已死");

  Ok(())
}

// -----------------------------------------------------------------------------
// 发现二（deviations.md 第 19 条）：C# InitialUpdater 幻键拒挂语义锁
// -----------------------------------------------------------------------------

/// 缺键 HDEL / HPERSIST / LSET 拒建空对象幻键语义锁（对齐 Redis 恒不建键，不对齐 C# 挂空对象）
#[test]
fn missing_key_hdel_hpersist_lset_leaves_no_key() -> Void {
  let env = env("missing-key-no-phantom.db");
  let mut s = session_with(&env);

  // 1. HDEL 缺键
  let k_hdel = b"missing_hdel";
  let out_hdel = auto_exec(&env, &mut s, RespCommand::Hdel, &[k_hdel, b"field1"]);
  assert_eq!(out_hdel, b":0\r\n", "缺键 HDEL 应答 0");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[k_hdel]),
    b":0\r\n",
    "HDEL 缺键后键必须不存在"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Type, &[k_hdel]),
    b"+none\r\n",
    "HDEL 缺键后 TYPE 应为 none"
  );

  // 2. HPERSIST 缺键
  let k_hpersist = b"missing_hpersist";
  let out_hpersist = auto_exec(
    &env,
    &mut s,
    RespCommand::Hpersist,
    &[k_hpersist, b"FIELDS", b"1", b"field1"],
  );
  assert_eq!(out_hpersist, b"*1\r\n:-2\r\n", "缺键 HPERSIST 回 -2 数组");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[k_hpersist]),
    b":0\r\n",
    "HPERSIST 缺键后键必须不存在"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Type, &[k_hpersist]),
    b"+none\r\n",
    "HPERSIST 缺键后 TYPE 应为 none"
  );

  // 3. LSET 缺键
  let k_lset = b"missing_lset";
  let out_lset = auto_exec(&env, &mut s, RespCommand::Lset, &[k_lset, b"0", b"val"]);
  assert!(
    out_lset.starts_with(b"-ERR"),
    "LSET 缺键应报错，实际: {}",
    String::from_utf8_lossy(&out_lset)
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[k_lset]),
    b":0\r\n",
    "LSET 缺键后键必须不存在"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Type, &[k_lset]),
    b"+none\r\n",
    "LSET 缺键后 TYPE 应为 none"
  );

  Ok(())
}

// -----------------------------------------------------------------------------
// 发现三（deviations.md 第 47 条）：读臂删空自愈语义锁
// -----------------------------------------------------------------------------

/// HEXPIRE/HPEXPIRE 全字段到期后 HLEN 触发删空自愈（应答 :0 且整键回收 EXISTS 0）
#[test]
fn hexpire_all_expired_hlen_triggers_empty_deletion() -> Void {
  let env = env("read-arm-empty-delete.db");
  let mut s = session_with(&env);
  let key = b"hexpire_key";

  // 1. 初始化建 Hash 包含 2 个字段
  let out_hset = auto_exec(
    &env,
    &mut s,
    RespCommand::Hset,
    &[key, b"f1", b"v1", b"f2", b"v2"],
  );
  assert_eq!(out_hset, b":2\r\n");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[key]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Type, &[key]),
    b"+hash\r\n"
  );

  // 2. HPEXPIRE 设置全字段极短到期（50 毫秒）
  let out_expire = auto_exec(
    &env,
    &mut s,
    RespCommand::Hpexpire,
    &[key, b"50", b"FIELDS", b"2", b"f1", b"f2"],
  );
  assert_eq!(out_expire, b"*2\r\n:1\r\n:1\r\n", "全字段成功设置过期时间");

  // 等待全员到期
  sleep(Duration::from_millis(80));

  // 3. 执行 HLEN：读臂触发到期剔除与 REMOVE_KEY 整键回收自愈
  let out_hlen = auto_exec(&env, &mut s, RespCommand::Hlen, &[key]);
  assert_eq!(out_hlen, b":0\r\n", "全员到期 HLEN 应答 :0");

  // 4. 验证整键已被物理回收（EXISTS 0、TYPE none），对齐 Redis，杜绝空对象键存活
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[key]),
    b":0\r\n",
    "读臂删空自愈后键必须已消亡"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Type, &[key]),
    b"+none\r\n",
    "读臂删空自愈后 TYPE 必须为 none"
  );

  Ok(())
}

/// ZPEXPIRE 全成员到期后 ZCARD 触发删空自愈（应答 :0 且整键回收 EXISTS 0）
#[test]
fn zpexpire_all_expired_zcard_triggers_empty_deletion() -> Void {
  let env = env("read-arm-zcard-empty-delete.db");
  let mut s = session_with(&env);
  let key = b"zpexpire_key";

  // 1. 初始化建 ZSet 包含 2 个成员
  let out_zadd = auto_exec(
    &env,
    &mut s,
    RespCommand::Zadd,
    &[key, b"1.0", b"m1", b"2.0", b"m2"],
  );
  assert_eq!(out_zadd, b":2\r\n");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[key]),
    b":1\r\n"
  );

  // 2. ZPEXPIRE 设置全成员极短到期（50 毫秒）
  let out_zpexpire = auto_exec(
    &env,
    &mut s,
    RespCommand::Zpexpire,
    &[key, b"50", b"MEMBERS", b"2", b"m1", b"m2"],
  );
  assert_eq!(out_zpexpire, b"*2\r\n:1\r\n:1\r\n");

  // 等待全员到期
  sleep(Duration::from_millis(80));

  // 3. 执行 ZCARD：读臂触发到期剔除与整键回收自愈
  let out_zcard = auto_exec(&env, &mut s, RespCommand::Zcard, &[key]);
  assert_eq!(out_zcard, b":0\r\n", "全员到期 ZCARD 应答 :0");

  // 4. 验证整键已被物理回收
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[key]),
    b":0\r\n",
    "读臂删空自愈后键必须已消亡"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Type, &[key]),
    b"+none\r\n",
    "读臂删空自愈后 TYPE 必须为 none"
  );

  Ok(())
}
