//! 分层集合读臂扫描 Err 上抛回归（本票收口核心）
//!
//! 机制：tiered 树内读臂曾经 `let _ =` 吞掉 `scan_with_count_callback` 的
//! `Err`（底层 range_index 迭代失败 / wbftree scan_callback catch_unwind
//! 兜底），把存储故障折成空集/截断应答——r6 红期间 HGETALL 双端返 `*0`
//! 无任何报错即此形态。收口后全族读臂经 [`wnode` tiered_collection_ops
//! common::scan_count] 单一门上抛 `Err(())`，由慢路径存储错误漏斗统一闭环
//! 成 RESP 错误帧（RESP_ERR_SLOW_PATH_STORAGE）。
//!
//! 注入形态：wbftree 测试钩子 [`wbftree::SCAN_FAIL_INJECT`]（一次性，消费即
//! 复位）模拟下一次扫描初始化失败，逐臂断言「错误帧而非空集/截断」。
//!
//! 对标 C#：对象域迭代无失败面，存储层异常（GarnetException）一律以错误
//! 帧上抛，无「迭代出错返空集合」形态（libs/server/Storage/Session/
//! ObjectStore/Common.cs 的 ObjectScan 链路）。

use std::{
  mem::take,
  sync::{Arc, atomic::Ordering},
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::SCAN_FAIL_INJECT;
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
use wresp::command::RespCommand;
use wtxn::WatchVersionMap;
use wval::GarnetObjectType;

type TestStore = WedbStore<SegmentedDevice>;

struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  api: GarnetApi,
  _dir: tempfile::TempDir,
}

fn env(tag: &str) -> Env {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::new(WatchVersionMap::new(
      1 << 10
    )))),
    "引擎级写面钩子应首次挂载"
  );
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  Env {
    rt: Runtime::new().unwrap(),
    store,
    api,
    _dir: dir,
  }
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

/// 慢路径命令同步求值并回帧字节（与 tiered_field_ttl 同款泵）
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

/// 手工升阶（entries 与 `IGarnetObject::export_entries` 同构；`next_expiry`
/// 为灌入批最早到期水位，`i64::MAX` = 无成员挂 TTL，`0` = 水位已越过）
fn promote(
  env: &Env,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
  next_expiry: i64,
) {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(key, obj_type, entries, next_expiry, false))
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

fn is_tiered(env: &Env, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

/// 分层 hash 三字段（f1/f2/f3）
fn promote_hash3(env: &Env, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
    i64::MAX,
  );
}

/// 分层 set 三成员
fn promote_set3(env: &Env, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::Set,
    vec![
      (b"m1".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m2".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m3".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
    ],
    i64::MAX,
  );
}

/// 分层 zset 双成员（m1=1.0 / m2=2.0）
fn promote_zset2(env: &Env, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::SortedSet,
    vec![
      (b"m1".to_vec(), encode_member(&1.0f64.to_be_bytes(), None)),
      (b"m2".to_vec(), encode_member(&2.0f64.to_be_bytes(), None)),
    ],
    i64::MAX,
  );
}

/// 分层 list 三元素（序号自 LIST_SEQ_BASE 连续排布）
fn promote_list3(env: &Env, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::List,
    vec![
      (
        (LIST_SEQ_BASE).to_be_bytes().to_vec(),
        encode_member(b"e1", None),
      ),
      (
        (LIST_SEQ_BASE + 1).to_be_bytes().to_vec(),
        encode_member(b"e2", None),
      ),
      (
        (LIST_SEQ_BASE + 2).to_be_bytes().to_vec(),
        encode_member(b"e3", None),
      ),
    ],
    i64::MAX,
  );
}

/// 注入扫描 Err 并断言 RESP 错误帧（一次性钩子：置真 → 命令 → 消费复位）
fn assert_scan_err_frame(
  env: &Env,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
  label: &str,
) {
  SCAN_FAIL_INJECT.store(true, Ordering::SeqCst);
  let out = auto_exec(env, s, cmd, args);
  let text = String::from_utf8_lossy(&out);
  assert!(
    out.starts_with(b"-ERR "),
    "{label} 必须回存储错误帧，实际应答: {text}"
  );
}

/// HGETALL 慢路径（水位未越过，Below 分支流式直出扫描）：错误帧而非 `*0`
#[test]
fn hgetall_scan_err_is_error_frame() {
  let env = env("scan-err-hgetall.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Hgetall, &[b"h"], "HGETALL");
  // 钩子一次性消费即复位：同一命令恢复正常应答（3 对 map）
  let out = auto_exec(&env, &mut s, RespCommand::Hgetall, &[b"h"]);
  assert!(
    out.starts_with(b"%3\r\n") || out.starts_with(b"*6\r\n"),
    "复位后 HGETALL 应正常，实际: {}",
    String::from_utf8_lossy(&out)
  );
}

/// HKEYS 读臂：错误帧而非 `*0`
#[test]
fn hkeys_scan_err_is_error_frame() {
  let env = env("scan-err-hkeys.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Hkeys, &[b"h"], "HKEYS");
}

/// HVALS 读臂：错误帧而非 `*0`
#[test]
fn hvals_scan_err_is_error_frame() {
  let env = env("scan-err-hvals.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Hvals, &[b"h"], "HVALS");
}

/// HLEN 计数校正臂（水位越过触发到期扫描）：错误帧而非伪计数；
/// 水位用 0（now_ticks 恒大于 0）强制走 sweep 扫描
#[test]
fn hlen_sweep_scan_err_is_error_frame() {
  let env = env("scan-err-hlen.db");
  promote(
    &env,
    b"h",
    GarnetObjectType::Hash,
    vec![(b"f1".to_vec(), encode_member(b"v1", None))],
    0,
  );
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Hlen, &[b"h"], "HLEN");
  // 出账失败键不得消亡、数据不得固化丢失
  assert!(is_tiered(&env, b"h"), "HLEN 出账失败键应保持分层态");
}

/// ZCARD 计数校正臂（水位越过触发到期扫描）：同 HLEN 口径
#[test]
fn zcard_sweep_scan_err_is_error_frame() {
  let env = env("scan-err-zcard.db");
  promote(
    &env,
    b"z",
    GarnetObjectType::SortedSet,
    vec![(b"m1".to_vec(), encode_member(&1.0f64.to_be_bytes(), None))],
    0,
  );
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Zcard, &[b"z"], "ZCARD");
  assert!(is_tiered(&env, b"z"), "ZCARD 出账失败键应保持分层态");
}

/// SMEMBERS 读臂：错误帧而非 `~0`
#[test]
fn smembers_scan_err_is_error_frame() {
  let env = env("scan-err-smembers.db");
  promote_set3(&env, b"s");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Smembers, &[b"s"], "SMEMBERS");
}

/// SRANDMEMBER 读臂（随机起点扫描闭包）：错误帧而非空集
#[test]
fn srandmember_scan_err_is_error_frame() {
  let env = env("scan-err-srandmember.db");
  promote_set3(&env, b"s");
  let mut s = session_with(&env);
  assert_scan_err_frame(
    &env,
    &mut s,
    RespCommand::Srandmember,
    &[b"s", b"3"],
    "SRANDMEMBER",
  );
}

/// LRANGE 读臂：错误帧而非 `*0`
#[test]
fn lrange_scan_err_is_error_frame() {
  let env = env("scan-err-lrange.db");
  promote_list3(&env, b"l");
  let mut s = session_with(&env);
  assert_scan_err_frame(
    &env,
    &mut s,
    RespCommand::Lrange,
    &[b"l", b"0", b"-1"],
    "LRANGE",
  );
}

/// LINDEX 读臂：错误帧而非 null（存储故障不得伪装成键缺失）
#[test]
fn lindex_scan_err_is_error_frame() {
  let env = env("scan-err-lindex.db");
  promote_list3(&env, b"l");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Lindex, &[b"l", b"0"], "LINDEX");
}

/// LPUSH 写臂头端序号定位（list_head_seq）：错误帧且零写入——
/// 静默回落基准序号会把新元素覆盖到既有元素上（数据销毁）
#[test]
fn lpush_head_seq_scan_err_is_error_frame() {
  let env = env("scan-err-lpush.db");
  promote_list3(&env, b"l");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Lpush, &[b"l", b"n1"], "LPUSH");
  // 钩子复位后 push 正常：长度 4（失败臂零写入）
  let out = auto_exec(&env, &mut s, RespCommand::Lpush, &[b"l", b"n1"]);
  assert_eq!(out, b":4\r\n", "复位后 LPUSH 应成功且长度 4");
}

/// RPUSH 写臂头端序号定位：同 LPUSH 口径
#[test]
fn rpush_head_seq_scan_err_is_error_frame() {
  let env = env("scan-err-rpush.db");
  promote_list3(&env, b"l");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Rpush, &[b"l", b"n1"], "RPUSH");
}

/// ZRANGE 全量读臂（zset_scan_select 流式内核）：错误帧而非空数组
#[test]
fn zrange_scan_err_is_error_frame() {
  let env = env("scan-err-zrange.db");
  promote_zset2(&env, b"z");
  let mut s = session_with(&env);
  assert_scan_err_frame(
    &env,
    &mut s,
    RespCommand::Zrange,
    &[b"z", b"0", b"-1"],
    "ZRANGE",
  );
}

/// ZCOUNT 读臂（计数窗口）：错误帧而非 0
#[test]
fn zcount_scan_err_is_error_frame() {
  let env = env("scan-err-zcount.db");
  promote_zset2(&env, b"z");
  let mut s = session_with(&env);
  assert_scan_err_frame(
    &env,
    &mut s,
    RespCommand::Zcount,
    &[b"z", b"-inf", b"+inf"],
    "ZCOUNT",
  );
}

/// ZRANK 点读定位（tree_member_score）：错误帧而非 null——
/// 扫描失败与「成员不存在」必须分态
#[test]
fn zrank_score_scan_err_is_error_frame() {
  let env = env("scan-err-zrank.db");
  promote_zset2(&env, b"z");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Zrank, &[b"z", b"m1"], "ZRANK");
}

/// ZLEXCOUNT 读臂（zset_lex_select 断点趟）：错误帧而非 0
#[test]
fn zlexcount_scan_err_is_error_frame() {
  let env = env("scan-err-zlexcount.db");
  promote_zset2(&env, b"z");
  let mut s = session_with(&env);
  assert_scan_err_frame(
    &env,
    &mut s,
    RespCommand::Zlexcount,
    &[b"z", b"-", b"+"],
    "ZLEXCOUNT",
  );
}

/// HSCAN 树内游标扫描（exec_tiered_scan）：错误帧而非 `[0, []]`——
/// 存储故障不得伪装成「扫描完毕」
#[test]
fn hscan_scan_err_is_error_frame() {
  let env = env("scan-err-hscan.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Hscan, &[b"h", b"0"], "HSCAN");
}

/// 物化降级通道（tiered_materialize_blob 四臂）：HDEL 穿透物化时扫描 Err
/// 必须中止（fail-fast 不写回）——旧行为静默截断成空对象会触发删空自愈
/// 把整键销毁（数据丢失）。失败后键与数据完好
#[test]
fn materialize_scan_err_aborts_without_data_loss() {
  let env = env("scan-err-materialize.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Hdel, &[b"h", b"f1"], "HDEL");
  // 键未销毁、数据未丢：复位后 HGETALL 仍返 3 对
  assert!(is_tiered(&env, b"h"), "物化失败键不得被删空自愈回收");
  let out = auto_exec(&env, &mut s, RespCommand::Hgetall, &[b"h"]);
  assert!(
    out.starts_with(b"%3\r\n") || out.starts_with(b"*6\r\n"),
    "物化失败后数据应完好，实际: {}",
    String::from_utf8_lossy(&out)
  );
}

/// 未注入时全链路无错误帧（防钩子误常驻）：HGETALL 正常返 3 对 map
#[test]
fn no_inject_no_error_frame() {
  let env = env("scan-err-clean.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);
  assert!(!SCAN_FAIL_INJECT.load(Ordering::SeqCst), "钩子默认必须关闭");
  let out = auto_exec(&env, &mut s, RespCommand::Hgetall, &[b"h"]);
  assert!(
    !out.starts_with(b"-"),
    "未注入不应有错误帧，实际: {}",
    String::from_utf8_lossy(&out)
  );
  assert!(
    out.windows(2).any(|w| w == b"f1") && out.windows(2).any(|w| w == b"v3"),
    "HGETALL 应含全部字段，实际: {}",
    String::from_utf8_lossy(&out)
  );
}
