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
//! 分层读臂与被拒臂（HSETNX 命中已存在 / ZADD NX 已存在 / SADD 重复成员）零推进。
//!
//! 迁移双向与假阳性覆盖：真实阈值升阶（单条 HSET 越过 4MB 字节门槛）、迟滞死区
//! 内重灌回树、降阶回信封后的后续写入、仅物化不写数据（读侧物化 / 未命中删）
//! 必须 EXEC 成功、分层树删空自愈不留残留存根；未支持写命令（ZPOPMIN 等）
//! 穿透至 wcol 对象层单源后既回正确应答也推进栅栏（旧实现由有序集合兜底臂
//! 静默应答成整表数组，语义错且零推进）；HEXPIRE 族已树内原地化（不穿透，
//! 见 tiered_field_ttl.rs）。

use std::{mem::take, sync::Arc, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wcol::{
  SET_MEMBER_DUMMY_VALUE,
  types::{garnet_object::LIST_SEQ_BASE, member_ttl::encode_member},
};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::version_map_watch_hook,
};
use wnode_test::drain_output;
use wresp::command::RespCommand;
use wtxn::{TransactionManager, TxnKeyEntryComparison, TxnLockTable, WatchVersionMap};
use wval::GarnetObjectType;

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
  s.output.clear();
  env.api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = env.rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 版本表读点（与 wtxn 校验同一哈希面）
fn ver(env: &Env, key: &[u8]) -> u64 {
  env
    .map
    .read_version(TxnKeyEntryComparison::key_hash(key) as u64)
}

/// 新开登记单键 WATCH 的事务管理器（对标 RESP WATCH 后待 EXEC 的会话）
fn watch(env: &Env, key: &[u8]) -> TransactionManager {
  let mut txn = TransactionManager::new(env.lock_table.clone(), Arc::clone(&env.map), None);
  txn.watch(key);
  txn
}

/// EXEC 校验（失效返回 false，对标 C# Run 返回 false 回 nil 数组）
fn exec(txn: &mut TransactionManager) -> bool {
  txn.run(false, false, Duration::ZERO)
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
    &[b"z", b"2", b"m2"],
    "分层 ZADD 新成员",
  );
  assert_arm_bumps_once(
    &env,
    b"z",
    RespCommand::Zadd,
    &[b"z", b"3", b"m1"],
    "分层 ZADD 改分值",
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
  assert_arm_bumps_once(
    &env,
    b"z",
    RespCommand::Zadd,
    &[b"z", b"2", b"m2"],
    "分层 ZADD 补成员",
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
