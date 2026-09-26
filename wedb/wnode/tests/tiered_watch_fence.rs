//! 分层态（wbftree）集合写路径 WATCH 版本栅栏回归测试
//!
//! 缺陷：集合键升阶为 wbftree 分页分层态后，HSET/HDEL/SADD/SREM/ZADD/ZREM/
//! LPUSH/RPUSH 走引擎侧分层臂树内直写（树写漏斗 `tree_put` +
//! `save_bftree_meta_stub`），仅经 wkv 物理键原语落盘，绕过用户键写入口的
//! WATCH 版本推进 → WATCH 分层键的 MULTI/EXEC 在并发写后仍判有效，脏提交。
//!
//! 对标 C# 对象域写钩子 functionsState.watchVersionMap.IncrementVersion
//!（garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:79 与 :200
//! PostInitialUpdater/PostCopyUpdater、:100 InPlaceUpdater、:125 HasRemoveKey
//! 删空臂；UpsertMethods.cs:48/:58/:68；DeleteMethods.cs:21/:30）：C# 集合对象
//! 常驻对象域，任意写命令均经上述钩子推进；rust 分层臂是这些钩子在 wbftree 态
//! 的替代实现，栅栏在 `tiered_collection_ops::finish_tiered_arm` 单点补齐。
//!
//! 断言口径：分层写臂恰一次推进（懒降阶臂经 obj_save 用户键写入口，不得双计）；
//! 分层读臂与被拒臂（HSETNX 命中已存在 / ZADD NX 已存在 / ZADD 同分值零写 /
//! SADD 重复成员）零推进。
//!
//! 迁移双向与假阳性覆盖：真实阈值升阶（单条 HSET 越过 4MB 字节门槛）、迟滞死区
//! 内重灌回树、降阶回信封后的后续写入、仅物化不写数据（读侧物化 / 未命中删）
//! 必须 EXEC 成功、分层树删空自愈不留残留存根；未支持写命令（ZPOPMIN 等）
//! 穿透至 wcol 对象层单源后既回正确应答也推进栅栏（旧实现由有序集合兜底臂
//! 静默应答成整表数组，语义错且零推进）；HEXPIRE 族已树内原地化（不穿透，
//! 见 tiered_field_ttl.rs）。

use std::{sync::Arc, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcol::{
  SET_MEMBER_DUMMY_VALUE,
  types::{garnet_object::LIST_SEQ_BASE, member_ttl::encode_member},
};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  ReplayInput,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_store_callbacks::{
        ActiveDedicatedVectorSession, OwnedActiveVectorSession, WedbVectorStoreCallbacks,
      },
    },
  },
  service::StoreSwapSlot,
  storage::session::storage_session::{vector_version_watch_hook, version_map_watch_hook},
};
use wnode_test::drain_output;
use wresp::command::RespCommand;
use wtxn::{TransactionManager, TxnKeyEntryComparison, TxnLockTable, WatchVersionMap};
use wval::{GarnetObjectType, SessionPrefixBuf};
use wvector::Callbacks;

type TestStore = WedbStore<SegmentedDevice>;

/// 测试环境：真实引擎 + 共享版本表 + 引擎级写面钩子（与生产装配同径）
struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  map: Arc<WatchVersionMap>,
  /// 该引擎实例的锁表（对标 C# 事务管理器随 store 取 LockTable，非会话自造）
  lock_table: TxnLockTable,
  /// 写方执行域（独立于 WATCH 方的会话）
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
    lock_table: TxnLockTable::new(),
    api,
    _dir: dir,
  }
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

/// 慢路径命令同步求值并回帧字节（与 tiered_cmds_align 同款泵）
fn auto_exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 版本表读点（与 wtxn 校验同一哈希面：根域 scoped，与默认写会话前缀同源）
fn ver(env: &Env, key: &[u8]) -> u64 {
  env
    .map
    .read_version(TxnKeyEntryComparison::scoped_key_hash(root().as_slice(), key) as u64)
}

/// 缺省归属根域（写会话默认 (0,0) 与 RESP 执行域解析恒等）
fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}

/// 新开登记单键 WATCH 的事务管理器（对标 RESP WATCH 后待 EXEC 的会话；根域归属）
fn watch(env: &Env, key: &[u8]) -> TransactionManager {
  let mut txn = TransactionManager::new(env.lock_table.clone(), Arc::clone(&env.map), None);
  txn.watch(root().as_slice(), key);
  txn
}

/// EXEC 校验（失效返回 false，对标 C# Run 返回 false 回 nil 数组）
fn exec(txn: &mut TransactionManager) -> bool {
  txn.run(root().as_slice(), false, false, Duration::ZERO)
}

/// 就地升阶为分层态（免去 65,536 条灌水，与 apply_rmw_post_operate 升阶臂同函数）
fn promote(env: &Env, key: &[u8], obj_type: GarnetObjectType, entries: Vec<(Vec<u8>, Vec<u8>)>) {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(key, obj_type, entries, i64::MAX, false))
    .unwrap();
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(key))
      .unwrap()
      .is_some(),
    "键应处于 wbftree 分层态"
  );
}

fn promote_hash(env: &Env, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
    ],
  );
}

fn promote_set(env: &Env, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::Set,
    vec![(b"m1".to_vec(), encode_member(SET_MEMBER_DUMMY_VALUE, None))],
  );
}

fn promote_zset(env: &Env, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::SortedSet,
    vec![
      (b"m1".to_vec(), encode_member(&1.0f64.to_be_bytes(), None)),
      (b"m2".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
    ],
  );
}

fn promote_list(env: &Env, key: &[u8]) {
  let base = LIST_SEQ_BASE;
  promote(
    env,
    key,
    GarnetObjectType::List,
    vec![
      (base.to_be_bytes().to_vec(), encode_member(b"e1", None)),
      (
        (base + 1).to_be_bytes().to_vec(),
        encode_member(b"e2", None),
      ),
    ],
  );
}

/// RESP 帧编码（多命令流水线）
fn frame(cmds: &[&[&[u8]]]) -> Vec<u8> {
  let mut out = Vec::new();
  for c in cmds {
    out.extend_from_slice(format!("*{}\r\n", c.len()).as_bytes());
    for a in c.iter() {
      out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
      out.extend_from_slice(a);
      out.extend_from_slice(b"\r\n");
    }
  }
  out
}

/// 直填会话输入泵并取输出
fn pump(s: &mut RespServerSession, bytes: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(bytes);
  assert!(s.try_consume_messages().is_some(), "帧应完整消费");
  drain_output(s)
}

/// 判键是否处于 wbftree 分层态（元记录 + 树存根在位）
fn is_tiered(env: &Env, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

/// 分层写臂通用断言：WATCH 后该臂实际改动 → 版本恰推进 1 且 EXEC 必失效
fn assert_arm_bumps_once(env: &Env, key: &[u8], cmd: RespCommand, args: &[&[u8]], why: &str) {
  let mut s = session_with(env);
  let mut txn = watch(env, key);
  let before = ver(env, key);
  let out = auto_exec(env, &mut s, cmd, args);
  assert_eq!(
    before + 1,
    ver(env, key),
    "{why}：{cmd} 版本应恰推进一次，应答={out:?}"
  );
  assert!(
    !exec(&mut txn),
    "{why}：{cmd} 分层写后 WATCH 事务必须失效，应答={out:?}"
  );
}

/// 分层读臂 / 未命中臂通用断言：零推进，WATCH 事务仍有效
fn assert_arm_no_bump(env: &Env, key: &[u8], cmd: RespCommand, args: &[&[u8]], why: &str) {
  let mut s = session_with(env);
  let mut txn = watch(env, key);
  let before = ver(env, key);
  auto_exec(env, &mut s, cmd, args);
  assert_eq!(before, ver(env, key), "{why}：{cmd} 不得推进版本");
  assert!(exec(&mut txn), "{why}：{cmd} 后 WATCH 事务不得误杀");
}

/// 哈希分层写臂：新增字段 / 覆盖已有字段 / 删字段 / 原子计数 / 删空自愈
#[test]
fn tiered_hash_write_arms_invalidate_watch() {
  let env = env("tiered-watch-hash.db");
  let mut s = session_with(&env);
  promote_hash(&env, b"h");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    b":2\r\n",
    "前置：分层态 HLEN 自洽"
  );

  assert_arm_bumps_once(
    &env,
    b"h",
    RespCommand::Hset,
    &[b"h", b"f3", b"3"],
    "分层 HSET 新字段",
  );
  // 覆盖已有字段：无新增字段、仅树内值变更，C# InPlaceUpdater 实际改写口径
  assert_arm_bumps_once(
    &env,
    b"h",
    RespCommand::Hset,
    &[b"h", b"f1", b"v1x"],
    "分层 HSET 覆盖字段",
  );
  assert_arm_bumps_once(&env, b"h", RespCommand::Hdel, &[b"h", b"f2"], "分层 HDEL");
  // 原子计数：f3 持数字载荷，HINCRBY 实际改写树内值（非整数字段会落错误路径、
  // 零树内变更，本臂不成立）
  assert_arm_bumps_once(
    &env,
    b"h",
    RespCommand::Hincrby,
    &[b"h", b"f3", b"5"],
    "分层 HINCRBY",
  );
  // 删空自愈：此前存活字段为 f1 与 f3，一条 HDEL 全删 → 树清退 + 元记录删
  // （原缺陷同样漏推）
  assert_arm_bumps_once(
    &env,
    b"h",
    RespCommand::Hdel,
    &[b"h", b"f1", b"f3"],
    "分层 HDEL 至空",
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[b"h"]),
    b":0\r\n",
    "删空后键应严格消失"
  );
  assert!(!is_tiered(&env, b"h"), "删空后不得残留元记录 / 树存根");
}

/// 哈希分层读臂与未命中写臂：不得误杀 WATCH
#[test]
fn tiered_hash_read_and_rejected_arms_keep_watch_valid() {
  let env = env("tiered-watch-hash-noop.db");
  promote_hash(&env, b"h");

  assert_arm_no_bump(&env, b"h", RespCommand::Hget, &[b"h", b"f1"], "分层 HGET");
  assert_arm_no_bump(&env, b"h", RespCommand::Hlen, &[b"h"], "分层 HLEN");
  assert_arm_no_bump(
    &env,
    b"h",
    RespCommand::Hgetall,
    &[b"h"],
    "分层 HGETALL 物化读",
  );
  assert_arm_no_bump(
    &env,
    b"h",
    RespCommand::Hdel,
    &[b"h", b"absent"],
    "分层 HDEL 未命中",
  );
  // HSETNX 命中已存在字段：应答 0、零写入
  assert_arm_no_bump(
    &env,
    b"h",
    RespCommand::Hsetnx,
    &[b"h", b"f1", b"zz"],
    "分层 HSETNX 已存在",
  );
}

/// 集合 / 有序集合 / 列表分层写臂（SADD/SREM/ZADD/ZREM/LPUSH/RPUSH/LPOP）
#[test]
fn tiered_set_zset_list_write_arms_invalidate_watch() {
  let env = env("tiered-watch-colls.db");

  promote_set(&env, b"s");
  assert_arm_bumps_once(&env, b"s", RespCommand::Sadd, &[b"s", b"m2"], "分层 SADD");
  // 重复成员判空必须紧跟 SADD 之后：SREM 起已改走整值写回面，一旦跌回降阶
  // 水位即就地懒降阶退出分层态，其后的 SADD 落在信封面（C# 整值重写恒推进），
  // 再断言「树内容未变即零推进」就不是同一条臂了
  assert_arm_no_bump(
    &env,
    b"s",
    RespCommand::Sadd,
    &[b"s", b"m2"],
    "分层 SADD 重复成员",
  );
  assert_arm_bumps_once(&env, b"s", RespCommand::Srem, &[b"s", b"m1"], "分层 SREM");

  promote_zset(&env, b"z");
  assert_arm_bumps_once(
    &env,
    b"z",
    RespCommand::Zadd,
    &[b"z", b"2", b"m3"],
    "分层 ZADD 新成员",
  );
  assert_arm_bumps_once(
    &env,
    b"z",
    RespCommand::Zadd,
    &[b"z", b"3", b"m1"],
    "分层 ZADD 改分值",
  );
  // 同分值且旧记录无挂 TTL：树写零动作（C# SortedSetObjectImpl.cs:163 同分支
  // TryRemoveExpiration 幂等零动作，-0.0/+0.0 等值面不得覆写树内位模式）→ 零推进
  assert_arm_no_bump(
    &env,
    b"z",
    RespCommand::Zadd,
    &[b"z", b"3", b"m1"],
    "分层 ZADD 同分值",
  );
  assert_arm_no_bump(
    &env,
    b"z",
    RespCommand::Zadd,
    &[b"z", b"NX", b"9", b"m1"],
    "分层 ZADD NX 已存在",
  );
  assert_arm_bumps_once(&env, b"z", RespCommand::Zrem, &[b"z", b"m2"], "分层 ZREM");

  promote_list(&env, b"l");
  assert_arm_bumps_once(
    &env,
    b"l",
    RespCommand::Lpush,
    &[b"l", b"head"],
    "分层 LPUSH",
  );
  assert_arm_bumps_once(
    &env,
    b"l",
    RespCommand::Rpush,
    &[b"l", b"tail"],
    "分层 RPUSH",
  );
  assert_arm_bumps_once(&env, b"l", RespCommand::Lpop, &[b"l"], "分层 LPOP");
  assert_arm_no_bump(
    &env,
    b"l",
    RespCommand::Lrange,
    &[b"l", b"0", b"-1"],
    "分层 LRANGE 读",
  );
}

/// 懒降阶臂（物化回信封 + 树清退）恰一次推进：原实现显式推进与 obj_save
/// 用户键写入口重复计两次
#[test]
fn tiered_demote_writeback_bumps_exactly_once() {
  let env = env("tiered-watch-demote.db");
  promote_list(&env, b"l");
  let mut s = session_with(&env);
  let mut txn = watch(&env, b"l");
  let before = ver(&env, b"l");
  // LTRIM 无树内臂 → 物化装载 + 分层感知写回（obj_save + 树清退）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Ltrim, &[b"l", b"0", b"1"]),
    b"+OK\r\n"
  );
  assert_eq!(
    before + 1,
    ver(&env, b"l"),
    "懒降阶写回必须恰推进一次（不得与 obj_save 双计）"
  );
  assert!(!exec(&mut txn), "降阶写回后 WATCH 事务必须失效");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[b"l"]),
    b":1\r\n",
    "降阶后键仍在（信封接管，数据不丢）"
  );
}

/// 端到端 RESP：WATCH 分层键 → MULTI 入队 → 他会话分层写 → EXEC 必回 *-1
#[test]
fn multi_exec_on_watched_tiered_key_returns_nil_after_tiered_write() {
  let env = env("tiered-watch-multi-exec.db");
  promote_hash(&env, b"h");

  // WATCH 方：WATCH h + MULTI + 入队 HSET
  let mut watcher = session_with(&env);
  watcher.attach_transaction_components(Arc::clone(&env.map), env.lock_table.clone());
  assert_eq!(
    pump(
      &mut watcher,
      &frame(&[
        &[b"WATCH", b"h"],
        &[b"MULTI"],
        &[b"HSET", b"h", b"fw", b"vw"]
      ])
    ),
    b"+OK\r\n+OK\r\n+QUEUED\r\n"
  );

  // 并发方：分层写臂改动同一键
  let mut writer = session_with(&env);
  assert_eq!(
    auto_exec(&env, &mut writer, RespCommand::Hset, &[b"h", b"fc", b"vc"]),
    b":1\r\n",
    "分层 HSET 应正常应答"
  );

  // EXEC：版本失配 → nil 数组（原缺陷：栅栏未推进，EXEC 照常提交 → 脏提交）
  assert_eq!(
    pump(&mut watcher, &frame(&[&[b"EXEC"]])),
    b"*-1\r\n",
    "分层写后 EXEC 必须失效"
  );
}

/// 对照面：并发仅读分层键时 EXEC 正常提交（证明失配源于栅栏推进而非恒失败）
#[test]
fn multi_exec_commits_when_concurrent_tiered_read_only() {
  let env = env("tiered-watch-multi-exec-read.db");
  promote_hash(&env, b"h");

  let mut watcher = session_with(&env);
  watcher.attach_transaction_components(Arc::clone(&env.map), env.lock_table.clone());
  assert_eq!(
    pump(
      &mut watcher,
      &frame(&[&[b"WATCH", b"h"], &[b"MULTI"], &[b"PING"]])
    ),
    b"+OK\r\n+OK\r\n+QUEUED\r\n"
  );

  let mut reader = session_with(&env);
  assert_eq!(
    auto_exec(&env, &mut reader, RespCommand::Hget, &[b"h", b"f1"]),
    b"$2\r\nv1\r\n"
  );
  assert_eq!(
    auto_exec(&env, &mut reader, RespCommand::Hgetall, &[b"h"]),
    b"*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"
  );
  assert_eq!(ver(&env, b"h"), 0, "分层读面不得推进版本");

  assert_eq!(
    pump(&mut watcher, &frame(&[&[b"EXEC"]])),
    b"*1\r\n+PONG\r\n",
    "并发纯读后 EXEC 必须提交"
  );
}

/// 真实阈值两条迁移臂的栅栏覆盖（不经 promote 助手捷径）：
/// 1. 单条 HSET 越过 wcol::TIERED_PROMOTE_BYTES（4MB）→ 升阶 wcol→wbftree，
///    升阶臂（export_entries + promote_collection_to_bftree 仅物理键原语）
///    由 apply_rmw_post_operate 显式推进恰一次；
/// 2. 升阶后的分层态原生写臂树内直写 → 由 finish_tiered_arm 推进恰一次；
/// 3. 规模仍高于降阶阈值时的 HDEL → 迟滞死区内重灌回树（drain + promote），
///    同样恰一次推进，且数据真实删除。
#[test]
fn real_threshold_promotion_and_repump_bump_exactly_once() {
  let env = env("tiered-watch-real-promote.db");
  let mut s = session_with(&env);

  // 8000 字段 × 600B ≈ 4.8MB：仅靠字节判据跨过升阶门槛（对标
  // collection_adaptive_tiering 的按字节升阶用例，避开 65,536 计数门槛）
  let value = vec![b'v'; 600];
  let fields: Vec<Vec<u8>> = (0..8000_usize)
    .map(|i| format!("f{i}").into_bytes())
    .collect();
  let mut args: Vec<&[u8]> = vec![b"h"];
  for f in &fields {
    args.push(f.as_slice());
    args.push(&value);
  }

  let mut txn = watch(&env, b"h");
  let before = ver(&env, b"h");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hset, &args),
    b":8000\r\n",
    "前置：单条 HSET 灌 8000 字段"
  );
  assert!(is_tiered(&env, b"h"), "超字节门槛应升阶为 wbftree 分层态");
  assert_eq!(
    before + 1,
    ver(&env, b"h"),
    "升阶迁移命令恰推进一次（升阶臂显式推进，无第二处）"
  );
  assert!(!exec(&mut txn), "升阶后 WATCH 事务必须失效");

  // 升阶后原生臂：新 WATCH → 树内 HSET → 恰一次推进（旧实现此处零推进）
  assert_arm_bumps_once(
    &env,
    b"h",
    RespCommand::Hset,
    &[b"h", b"fx", b"vy"],
    "升阶后分层 HSET",
  );
  assert!(is_tiered(&env, b"h"), "迟滞死区内的小改动不得被误降阶");

  // 重灌臂：删一个字段后规模仍远超降阶阈值 → drain + 重灌回树，恰一次推进
  let mut txn2 = watch(&env, b"h");
  let before2 = ver(&env, b"h");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hdel, &[b"h", b"f0"]),
    b":1\r\n",
    "前置：分层 HDEL 命中"
  );
  assert_eq!(
    before2 + 1,
    ver(&env, b"h"),
    "分层重灌（迁移）命令恰推进一次，树清退不双计"
  );
  assert!(!exec(&mut txn2), "重灌后 WATCH 事务必须失效");
  assert!(is_tiered(&env, b"h"), "重灌后仍处分层态");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"f0"]),
    b"$-1\r\n",
    "重灌后 f0 应真实消失（迁移不丢改动）"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"fx"]),
    b"$2\r\nvy\r\n",
    "重灌后前一分层写入的字段仍在（迁移不丢数据）"
  );
}

/// 降阶回 wcol 信封后的后续写入仍推进栅栏（(b) 面）：
/// 未支持写命令（ZPOPMIN）物化 → 对象层单源真实删改 → 信封写回 + 树清退
/// 恰一次推进（HEXPIRE 族已树内原地化不再穿透，见 tiered_field_ttl.rs）；
/// 此后信封态读写与旧路径同口径，读面零推进
#[test]
fn demoted_key_writes_still_invalidate_watch() {
  let env = env("tiered-watch-after-demote.db");
  promote_zset(&env, b"z");
  let mut s = session_with(&env);

  let mut txn = watch(&env, b"z");
  let before = ver(&env, b"z");
  // ZPOPMIN 删 1 剩 1：物化对象层删除后跌回迟滞死区 → 懒降阶信封写回
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zpopmin, &[b"z", b"1"]),
    b"*2\r\n$2\r\nm1\r\n$1\r\n1\r\n",
    "穿透 ZPOPMIN 应走对象层单源删最小分值成员"
  );
  assert_eq!(
    before + 1,
    ver(&env, b"z"),
    "降阶写回恰推进一次（obj_save 用户键写入口，树清退不双计）"
  );
  assert!(!exec(&mut txn), "降阶写回后 WATCH 事务必须失效");
  assert!(!is_tiered(&env, b"z"), "小集合降阶后应回到 wcol 信封态");

  // 降阶回信封后：后续写入仍被 WATCH 感知
  assert_arm_bumps_once(
    &env,
    b"z",
    RespCommand::Zadd,
    &[b"z", b"9", b"m9"],
    "降阶后信封 ZADD",
  );
  assert_arm_no_bump(
    &env,
    b"z",
    RespCommand::Zscore,
    &[b"z", b"m9"],
    "降阶后信封 ZSCORE",
  );
}

/// (c) 仅迁移 / 仅物化不写数据不得触发假阳性：
/// - 读侧物化（未支持读命令 HTTL）零推进，且键仍留在分层态（读不得顺手降阶）；
/// - 未命中写命令（HDEL 不存在字段）零推进；
/// - 上述之后 EXEC 正常提交
#[test]
fn read_side_materialize_and_noop_write_keep_watch_valid() {
  let env = env("tiered-watch-no-false-positive.db");
  promote_hash(&env, b"h");
  let mut s = session_with(&env);

  let mut txn = watch(&env, b"h");
  let before = ver(&env, b"h");
  // HTTL 非分层原生臂 → 读侧物化求值：应答为长度 1 的整数数组，
  // 而非旧兜底臂的 HGETALL 形态（*4）
  let out = auto_exec(
    &env,
    &mut s,
    RespCommand::Httl,
    &[b"h", b"FIELDS", b"1", b"f1"],
  );
  assert!(
    out.starts_with(b"*1\r\n:"),
    "HTTL 应回逐字段整数数组，实际={out:?}"
  );
  // 未命中删：removed=0，元记录不写回
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hdel, &[b"h", b"absent"]),
    b":0\r\n"
  );
  assert_eq!(before, ver(&env, b"h"), "物化读与未命中删不得推进版本");
  assert!(
    is_tiered(&env, b"h"),
    "读侧物化不得改变分层态（仅迁移不写数据）"
  );
  assert!(exec(&mut txn), "仅物化读 + 未命中删之后 WATCH 事务不得误杀");
}

/// (d) 迁移途中的空删语义：分层树最后一次删空 → 树清退 + 元记录删 +
/// 随键 TTL 一并回收，恰一次推进；键严格消失且不留残留存根；同键重建正常
#[test]
fn empty_delete_on_tiered_key_bumps_once_and_leaves_no_residue() {
  let env = env("tiered-watch-empty-del.db");
  promote_hash(&env, b"h");
  let mut s = session_with(&env);

  let mut txn = watch(&env, b"h");
  let before = ver(&env, b"h");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hdel, &[b"h", b"f1", b"f2"]),
    b":2\r\n",
    "前置：一次 HDEL 删空两字段"
  );
  assert_eq!(
    before + 1,
    ver(&env, b"h"),
    "删空恰推进一次（树清退与元记录删不双计）"
  );
  assert!(!exec(&mut txn), "删空后 WATCH 事务必须失效");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[b"h"]),
    b":0\r\n",
    "删空后键应严格消失"
  );
  assert!(!is_tiered(&env, b"h"), "删空后不得残留元记录 / 树存根");
  // 同键重建走信封路径，栅栏口径不变
  assert_arm_bumps_once(
    &env,
    b"h",
    RespCommand::Hset,
    &[b"h", b"g1", b"w1"],
    "删空后同键重建 HSET",
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hget, &[b"h", b"g1"]),
    b"$2\r\nw1\r\n",
    "重建后数据自洽"
  );
}

/// 未支持写命令穿透后的真实删改与栅栏推进（原哈希 / 有序集合兜底臂把
/// ZPOPMIN 应答成整表 ZRANGE 形态且零删改零推进）
#[test]
fn tiered_unsupported_write_ops_materialize_and_bump_once() {
  let env = env("tiered-watch-passthru.db");
  promote_zset(&env, b"z");
  let mut s = session_with(&env);
  // m2 已在树（分值 2.0）：同分值 ZADD 走零写臂（C# SortedSetObjectImpl.cs:163
  // TryRemoveExpiration 幂等零动作）→ 树内容不变、零推进，与写臂衔接段
  assert_arm_no_bump(
    &env,
    b"z",
    RespCommand::Zadd,
    &[b"z", b"2", b"m2"],
    "分层 ZADD 同分值",
  );

  let mut txn = watch(&env, b"z");
  let before = ver(&env, b"z");
  let out = auto_exec(&env, &mut s, RespCommand::Zpopmin, &[b"z"]);
  assert!(
    out.starts_with(b"*2\r\n$2\r\nm1\r\n"),
    "ZPOPMIN 应答首个弹出成员 m1，实际={out:?}"
  );
  assert_eq!(before + 1, ver(&env, b"z"), "分层 ZPOPMIN 恰推进一次");
  assert!(!exec(&mut txn), "ZPOPMIN 后 WATCH 事务必须失效");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zcard, &[b"z"]),
    b":1\r\n",
    "弹出后仅剩 m2（旧兜底臂零删改）"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Zscore, &[b"z", b"m1"]),
    b"$-1\r\n",
    "m1 应真实移除"
  );

  // 弹空：删空自愈臂恰一次推进，键严格消失
  let mut txn2 = watch(&env, b"z");
  let before2 = ver(&env, b"z");
  let out2 = auto_exec(&env, &mut s, RespCommand::Zpopmin, &[b"z"]);
  assert!(
    out2.starts_with(b"*2\r\n$2\r\nm2\r\n"),
    "第二次 ZPOPMIN 应弹 m2，实际={out2:?}"
  );
  assert_eq!(before2 + 1, ver(&env, b"z"), "弹空恰推进一次");
  assert!(!exec(&mut txn2), "弹空后 WATCH 事务必须失效");
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Exists, &[b"z"]),
    b":0\r\n",
    "弹空后键应消失"
  );
  // 键已不存在：再弹零推进（未走写漏斗，不得误杀 WATCH）
  assert_arm_no_bump(&env, b"z", RespCommand::Zpopmin, &[b"z"], "缺失键 ZPOPMIN");
}

/// RENAME 分层键的双侧栅栏：新键整迁移窗口恰一次推进——wkv 内核段三单点
///（dst 残留清退触碰前推进，覆盖清退 + 新键元记录裸原语 upsert_raw 全程；
/// 调用方显式补推即同键双计，原缺陷：同命令同键推进两次）+ 旧键恰一次推进
///（finish_rename_move 的 delete_string 降级臂，树排空不双计）
#[test]
fn rename_tiered_key_bumps_both_sides_exactly_once() {
  let env = env("tiered-watch-rename.db");
  promote_hash(&env, b"h");

  let mut txn_new = watch(&env, b"h2");
  let mut txn_old = watch(&env, b"h");
  let before_new = ver(&env, b"h2");
  let before_old = ver(&env, b"h");

  let mut s = session_with(&env);
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Rename, &[b"h", b"h2"]),
    b"+OK\r\n",
    "前置：分层键 RENAME 慢路径应正常闭环"
  );

  // 新键恰一次推进（wkv 内核段三单点），旧键恰一次推进（delete_string 降级臂，
  // 树排空不双计）
  assert_eq!(
    before_new + 1,
    ver(&env, b"h2"),
    "RENAME 新键应恰推进一次（段三内核单点，调用方不补推）"
  );
  assert_eq!(
    before_old + 1,
    ver(&env, b"h"),
    "RENAME 旧键应恰推进一次（降级臂单点，树排空不双计）"
  );
  assert!(
    !exec(&mut txn_new),
    "RENAME 覆写新键后 WATCH 新键的事务必须失效"
  );
  assert!(
    !exec(&mut txn_old),
    "RENAME 删旧键后 WATCH 旧键的事务必须失效"
  );

  // 迁移后数据自洽：新键树态在位、旧键严格消失
  assert!(is_tiered(&env, b"h2"), "RENAME 后新键应处于 wbftree 分层态");
  assert!(
    !is_tiered(&env, b"h"),
    "RENAME 后旧键不得残留元记录 / 树存根"
  );
}

/// 向量域 WATCH 测试环境（生产同径装配：独立存储会话回调 + 登记表管理器 +
/// 写漏斗 WATCH 推进钩子注入，service.rs from_parts 同一装配位）。返回环境
/// 与管理器，供 AOF 重放臂直调（RENAME 重放回归用）
fn vector_watch_env(tag: &str) -> (Env, Arc<VectorManager>) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&map))),
    "引擎级写面钩子应首次挂载"
  );
  // 回调无状态装配（生产同形态）：命令面绑连接会话，直调臂经专用会话工厂自备会话
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new())),
  ));
  let fs = Arc::clone(&store);
  vm.attach_dedicated_session_factory(Arc::new(move || {
    fs.new_session()
      .ok()
      .map(OwnedActiveVectorSession::new)
      .map(ActiveDedicatedVectorSession::from_bound)
  }));
  // 向量写面以物理前缀寻址登记表，版本轨推进经生产同款换算钩子单点入逻辑域
  // 槽（service.rs 装配同径；空置换槽回落在握引擎）
  vm.set_watch_bump(vector_version_watch_hook(
    Arc::clone(&store),
    StoreSwapSlot::new(),
    Arc::clone(&map),
  ));
  let api: GarnetApi = Arc::new(
    StoreGarnetApi::new(store.new_session().unwrap()).with_vector_manager(Arc::clone(&vm)),
  );
  let env = Env {
    rt: Runtime::new().unwrap(),
    store,
    map,
    lock_table: TxnLockTable::new(),
    api,
    _dir: dir,
  };
  (env, vm)
}

/// 向量写漏斗 WATCH 推进（agy 轮5 条3 + r6-data 条3）：VADD/VREM/VSETATTR
/// 登记表变更与向量 RENAME 双键不经 wkv 用户键写入口，WATCH 版本由
/// VectorManager 写漏斗内部收口（装配期注入，同引擎级 set_watch_hook 形态；
/// 对标 C# 向量写经 Unified RMW 成功钩子无条件 IncrementVersion、RENAME 向量臂
/// DELETE(old) 与 SET(newKey) 双键各推）。旧实现向量域零推进 → WATCH 向量键
/// 永不夭折，MULTI/EXEC 隔离承诺对向量键静默失效
#[test]
fn vector_write_and_rename_old_key_invalidate_watch() {
  // VADD 慢路径 insert 链 webc-diskann Handle::block_on 需当前线程挂 compio
  // runtime（thread-per-core 每次阻塞调用解析本线程 runtime），整体置于上下文内
  Runtime::new().unwrap().block_on(async {
    let (env, _vm) = vector_watch_env("vector-watch.db");

    let mut s = session_with(&env);
    let vec4: Vec<u8> = [0.5f32; 4].iter().flat_map(|v| v.to_le_bytes()).collect();

    // VADD 成功：登记面变更 → 恰一次推进 + EXEC 夭折
    assert_arm_bumps_once(
      &env,
      b"vk",
      RespCommand::Vadd,
      &[b"vk", b"FP32", &vec4, b"elem1"],
      "向量 VADD",
    );

    // VSETATTR 成功：属性写入 → 恰一次推进 + EXEC 夭折
    assert_arm_bumps_once(
      &env,
      b"vk",
      RespCommand::Vsetattr,
      &[b"vk", b"elem1", b"attr"],
      "向量 VSETATTR",
    );

    // VREM 成功：删除 → 恰一次推进 + EXEC 夭折
    assert_arm_bumps_once(
      &env,
      b"vk",
      RespCommand::Vrem,
      &[b"vk", b"elem1"],
      "向量 VREM",
    );

    // 缺席元素 VREM / 缺失键 VSETATTR：零变更不得误杀
    assert_arm_no_bump(
      &env,
      b"vk",
      RespCommand::Vrem,
      &[b"vk", b"absent"],
      "缺席元素 VREM",
    );

    // 重建集合后 RENAME：旧键 DELETE(old) 推进在登记表迁移内恰一次；新键
    // SET(newKey) 语义位恒推（agy r6-data 条 3，对标 C# UnifiedStoreOps.cs
    // SET(newKey) 全 4 case 恒推 + UpsertMethods PostInitialWriter 无条件
    // IncrementVersion）——dst 缺席臂 delete_string 未命中墓碑同向一推 + 迁移
    // 成功一推 = 两推，与 C# DELETE 墓碑 + SET 双推对齐
    let vec1: Vec<u8> = [1.0f32; 4].iter().flat_map(|v| v.to_le_bytes()).collect();
    assert_eq!(
      auto_exec(
        &env,
        &mut s,
        RespCommand::Vadd,
        &[b"vk2", b"FP32", &vec1, b"e"]
      ),
      b":1\r\n",
      "前置：第二向量集应创建成功"
    );
    let mut txn_old = watch(&env, b"vk2");
    let before_old = ver(&env, b"vk2");
    let mut txn_new = watch(&env, b"vk3");
    let before_new = ver(&env, b"vk3");
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Rename, &[b"vk2", b"vk3"]),
      b"+OK\r\n",
      "前置：向量 RENAME 慢路径应正常闭环"
    );
    assert_eq!(
      before_old + 1,
      ver(&env, b"vk2"),
      "向量 RENAME 旧键应恰推进一次（登记表迁移内 DELETE(old) 收口）"
    );
    assert_eq!(
      before_new + 2,
      ver(&env, b"vk3"),
      "向量 RENAME 新键应推进两次（dst 清退一推 + SET(newKey) 语义位一推，C# 双推对齐）"
    );
    assert!(!exec(&mut txn_old), "向量 RENAME 后 WATCH 旧键事务必须失效");
    assert!(!exec(&mut txn_new), "向量 RENAME 后 WATCH 新键事务必须失效");
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Exists, &[b"vk2"]),
      b":0\r\n",
      "RENAME 后旧键应严格消失"
    );
  })
}

/// 零到期仅水位前移不得误杀 WATCH（r7-my 条 3）：水位滞留旧值（HPERSIST 后
/// 残低旧水位的等价形态，promote 水位入参直灌）时 HCOLLECT 零到期零树写，
/// 仅元记录水位前移——与四族计数臂 Swept{0} 不置脏不推进同口径，WATCH 事务
/// 不得夭折。旧实现把水位前移判为 changed 假推进版本，纯 FOLLOW-UP 内部收敛
/// 误夭折并发 WATCH
#[test]
fn collect_watermark_only_advance_keeps_watch_valid() {
  let env = env("tiered-watch-collect-watermark.db");
  // 灌树水位刻意残低：now > 水位必开扫（common.rs 越线门 `<=` 收口），
  // 但两成员均存活（m1 无 TTL、m2 挂远期 TTL）→ expired = 0、仅水位前移
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(
      b"h",
      GarnetObjectType::Hash,
      vec![
        (b"m1".to_vec(), encode_member(b"v1", None)),
        (
          b"m2".to_vec(),
          encode_member(b"v2", Some(now_ticks() + 60 * TICKS_PER_SECOND)),
        ),
      ],
      now_ticks() - 1000,
      false,
    ))
    .unwrap();

  assert_arm_no_bump(
    &env,
    b"h",
    RespCommand::Hcollect,
    &[b"h"],
    "HCOLLECT 零到期仅水位前移",
  );

  // 数据面自洽：零到期不得误删成员、计数恒精确
  let mut s = session_with(&env);
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Hlen, &[b"h"]),
    b":2\r\n",
    "零到期水位前移不得误删成员"
  );
}

/// 向量 RENAME 的 AOF 重放臂双键 WATCH 推进（r7-data 条 2）：C# 副本重放
/// RENAME 经 SET(newKey)+DELETE(oldKey) 写钩子恒 IncrementVersion
///（UnifiedStoreOps.cs RENAME 主流程，重放与交互共路径无豁免），rust 常规键
/// 重放臂（upsert_tag / expire_at_ticks / persist_key）同推进——旧实现
/// replay_vector_set_rename 直迁登记表零推进，副本上 WATCH 任一键的事务在
/// 重放 RENAME 后 EXEC 不夭折
#[test]
fn vector_rename_replay_invalidates_watch_on_both_keys() {
  let (env, vm) = vector_watch_env("vector-watch-replay.db");
  // AOF 重放臂直调（replay_vector_set_add / replay_vector_set_rename）的
  // 执行域绑定：本执行域自持专用会话，持至用例结束（单任务段许可形态）
  let _vector_domain = vm
    .bind_dedicated_session()
    .expect("专用向量会话工厂应已注入");
  let prefix = SessionPrefixBuf::ROOT.as_slice();

  // 重放 VADD 条目先行（副本语义：登记表未经命令面，按 AOF 条目重建；
  // 10 参布局对齐 VectorAofSink 合成条目：dims/reduceDims/valueType/values/
  // element/quantizer/buildEF/attributes/numLinks/distanceMetric）
  let vec4: Vec<u8> = [0.5f32; 4].iter().flat_map(|v| v.to_le_bytes()).collect();
  let add_input = ReplayInput {
    cmd: RespCommand::Vadd,
    flags: 0,
    sub_id: 0,
    obj_type: 0,
    arg1: 0,
    arg2: 0,
    arg3: 0,
    args: vec![
      4u32.to_le_bytes().to_vec(),
      0u32.to_le_bytes().to_vec(),
      1u32.to_le_bytes().to_vec(),
      vec4,
      b"e1".to_vec(),
      1u32.to_le_bytes().to_vec(),
      200u32.to_le_bytes().to_vec(),
      Vec::new(),
      8u32.to_le_bytes().to_vec(),
      2u32.to_le_bytes().to_vec(),
    ],
  };
  // 重放口 async 化后经块内收割（同步测试体内单段 block_on,无嵌套重入）
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    vm.replay_vector_set_add(prefix, b"vk", 0, &add_input)
      .await
      .expect("重放 VADD 应重建登记表");
  });

  // 双键 WATCH 登记 → 重放 RENAME 条目（args = [旧名]，条目键 = 新名）
  let mut txn_old = watch(&env, b"vk");
  let mut txn_new = watch(&env, b"vk2");
  let before_old = ver(&env, b"vk");
  let before_new = ver(&env, b"vk2");
  let rename_input = ReplayInput {
    cmd: RespCommand::Rename,
    flags: 0,
    sub_id: 0,
    obj_type: 0,
    arg1: 0,
    arg2: 0,
    arg3: 0,
    args: vec![b"vk".to_vec()],
  };
  rt.block_on(async {
    vm.replay_vector_set_rename(prefix, b"vk2", &rename_input)
      .await
      .expect("重放 RENAME 应迁移登记表");
  });

  assert_eq!(
    before_old + 1,
    ver(&env, b"vk"),
    "重放 RENAME 旧键应恰推进一次（DELETE(old) 语义位）"
  );
  assert_eq!(
    before_new + 1,
    ver(&env, b"vk2"),
    "重放 RENAME 新键应恰推进一次（SET(newKey) 语义位）"
  );
  assert!(!exec(&mut txn_old), "重放 RENAME 后 WATCH 旧键事务必须夭折");
  assert!(!exec(&mut txn_new), "重放 RENAME 后 WATCH 新键事务必须夭折");

  // 登记表真迁移：新名在位、旧名摘除（推进伴随真实迁移，非空转）
  assert!(
    vm.read_stored_index(prefix, b"vk2").is_some(),
    "重放后新名登记应在位"
  );
  assert!(
    vm.read_stored_index(prefix, b"vk").is_none(),
    "重放后旧名登记应摘除"
  );
}

/// swapnum（FLUSHDB 换号）窗内向量登记臂对在途 WATCH 必夭折（票
/// task/ing/wtxn-watch-version-slot-freeze-after-swapnum 验证点 5「回放写/
/// 分层写/向量登记」改判臂换号后语义如常）：向量写漏斗以登记表物理前缀寻址，
/// bump 经生产装配同款 vector_version_watch_hook 换算落版本轨逻辑 (0,0) 槽——
/// 换号后同逻辑键 VADD 与在途 WATCH 冻结槽恒命中，EXEC 中止（修复前形态：
/// 换号后 bump 漂新代物理槽，向量键乐观锁在换号窗静默失效）
#[test]
fn vector_write_after_swapnum_aborts_inflight_watch() {
  Runtime::new().unwrap().block_on(async {
    let (env, _vm) = vector_watch_env("vector-swap.db");
    let mut s = session_with(&env);
    let vec1: Vec<u8> = [1.0f32; 4].iter().flat_map(|v| v.to_le_bytes()).collect();
    auto_exec(
      &env,
      &mut s,
      RespCommand::Vadd,
      &[b"vk", b"FP32", &vec1, b"e1"],
    );
    let mut txn = watch(&env, b"vk");
    env
      .rt
      .block_on(env.store.flush_database(0, 0))
      .expect("FLUSHDB 换号");
    auto_exec(
      &env,
      &mut s,
      RespCommand::Vadd,
      &[b"vk", b"FP32", &vec1, b"e2"],
    );
    assert!(
      !exec(&mut txn),
      "换号后向量登记写必须推进逻辑轨槽使在途 WATCH 中止"
    );
  });
}

/// swapnum（FLUSHDB 换号）后分层集合写臂语义如常（票
/// task/ing/wtxn-watch-version-slot-freeze-after-swapnum 验证点 5 六型之
/// 「分层写」）：换号后新代域重建 wbftree 分层态并登记 WATCH，分层穿透写
/// （ZADD 对象层回写）bump 必落版本轨逻辑 (0,0) 槽——EXEC 校验同槽命中必
/// 中止，且换号后分层写恰一次推进该逻辑槽（槽位域不越代、不从新物理代
/// 另起）。原缺陷：分层写面 bump 漂入新代物理域种子，跨换号窗与换代后
/// 在途 WATCH 的槽位失联，分层键乐观锁在换号窗静默失效（回放/GC/降阶
/// 直设臂同构，其形状另由 watch_version_regression direct_set_domain 钉）
#[test]
fn tiered_write_after_swapnum_aborts_inflight_watch() {
  let env = env("tiered-watch-swapnum.db");
  let mut s = session_with(&env);
  // 先换号后重建分层态：写臂全程落新代物理域，钉「换号后」而非「跨换号窗」
  // 形态（跨窗冻结面已由 upsert/向量/回放型钉死）
  env
    .rt
    .block_on(env.store.flush_database(0, 0))
    .expect("FLUSHDB 换号");
  promote_zset(&env, b"zs");
  assert!(is_tiered(&env, b"zs"), "重建后应处 wbftree 分层态");

  let mut txn = watch(&env, b"zs");
  let before = ver(&env, b"zs");
  auto_exec(&env, &mut s, RespCommand::Zadd, &[b"zs", b"9", b"m9"]);
  assert!(
    !exec(&mut txn),
    "换号后分层集合写入必须使在途 WATCH 中止（分层写型改判臂不冻结）"
  );
  assert_eq!(
    ver(&env, b"zs"),
    before + 1,
    "换号后分层写 bump 恰推进逻辑轨槽一次（不越代另起新槽）"
  );
}
