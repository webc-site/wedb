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

use std::sync::{Mutex, atomic::Ordering};

use wbftree::SCAN_FAIL_INJECT;
use wcol::{
  SET_MEMBER_DUMMY_VALUE,
  types::{garnet_object::LIST_SEQ_BASE, member_ttl::encode_member},
};
use wnode::resp::resp_server_session::RespServerSession;
use wnode_test::{TestEnv, session_with, tiered_env};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

/// 慢路径命令同步求值并回帧字节（与 tiered_field_ttl 同款泵）
fn auto_exec(
  env: &TestEnv,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 手工升阶（entries 与 `IGarnetObject::export_entries` 同构；`next_expiry`
/// 为灌入批最早到期水位，`i64::MAX` = 无成员挂 TTL，`0` = 水位已越过）
fn promote(
  env: &TestEnv,
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

fn is_tiered(env: &TestEnv, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

/// 分层 hash 三字段（f1/f2/f3）
fn promote_hash3(env: &TestEnv, key: &[u8]) {
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
fn promote_set3(env: &TestEnv, key: &[u8]) {
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
fn promote_zset2(env: &TestEnv, key: &[u8]) {
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
fn promote_list3(env: &TestEnv, key: &[u8]) {
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

/// 全文件测试串行门：[`SCAN_FAIL_INJECT`] 是进程级一次性静态，cargo test 默认
/// 并行线程会互偷注入——他人测试偷走本测试的注入 ⇒ 本测试回正常应答误判红，
/// 反向同理（干净 HEAD 上全量跑约四成概率间歇红）。各测试体全程持锁串行，
/// 测试总时长 0.3s 量级，无并行收益可损失（测试无调度碰运气口径）
static INJECT_SERIALIZE: Mutex<()> = Mutex::new(());

/// 注入扫描 Err 并断言 RESP 错误帧（一次性钩子：置真 → 命令 → 消费复位）
fn assert_scan_err_frame(
  env: &TestEnv,
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
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-hgetall.db");
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
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-hkeys.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Hkeys, &[b"h"], "HKEYS");
}

/// HVALS 读臂：错误帧而非 `*0`
#[test]
fn hvals_scan_err_is_error_frame() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-hvals.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Hvals, &[b"h"], "HVALS");
}

/// HLEN 计数校正臂（水位越过触发到期扫描）：错误帧而非伪计数；
/// 水位用 0（now_ticks 恒大于 0）强制走 sweep 扫描
#[test]
fn hlen_sweep_scan_err_is_error_frame() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-hlen.db");
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
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-zcard.db");
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
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-smembers.db");
  promote_set3(&env, b"s");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Smembers, &[b"s"], "SMEMBERS");
}

/// SRANDMEMBER 读臂（随机起点扫描闭包）：错误帧而非空集
#[test]
fn srandmember_scan_err_is_error_frame() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-srandmember.db");
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
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-lrange.db");
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

/// LINDEX 读臂：错误帧而非 null（存储故障不得伪装成键缺失）；直写改造
/// （task/ing/wnode-lindex-tmpvec）后 Err 出口须撤净半成品帧——整个应答
/// 恰一错误帧，首个 \r\n 即帧尾，无批量字符串残帧尾随
#[test]
fn lindex_scan_err_is_error_frame() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-lindex.db");
  promote_list3(&env, b"l");
  let mut s = session_with(&env);
  SCAN_FAIL_INJECT.store(true, Ordering::SeqCst);
  // 取 idx=1：命中前须经一条 skip 分支，残帧判定覆盖 skip 与直写两形态
  let out = auto_exec(&env, &mut s, RespCommand::Lindex, &[b"l", b"1"]);
  let text = String::from_utf8_lossy(&out);
  assert!(
    out.starts_with(b"-ERR "),
    "LINDEX 必须回存储错误帧，实际应答: {text}"
  );
  let frame_end = out
    .windows(2)
    .position(|w| w == b"\r\n")
    .map(|p| p + 2)
    .expect("错误帧须有行终止符");
  assert_eq!(
    frame_end,
    out.len(),
    "LINDEX Err 应答必须撤帧后恰一错误帧（无残帧尾随），实际: {text}"
  );
  // 撤帧后二次命令恢复正常（钩子一次性消费复位）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Lindex, &[b"l", b"1"]),
    b"$2\r\ne2\r\n",
    "复位后 LINDEX 1 应精确回 e2 批量串帧"
  );
}

/// LINDEX 读臂直写改造（task/ing/wnode-lindex-tmpvec）应答逐字节全等回归：
/// 正/负索引、两端越界、缺失键应答帧与改造前逐字节一致；空列表语义由
/// 缺失键承接（分层态 size=0 元记录被 is_live 挡掉，正常不可达）
#[test]
fn lindex_response_matrix_byte_exact() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-lindex-matrix.db");
  promote_list3(&env, b"l");
  let mut s = session_with(&env);
  let bulk = |v: &str| format!("${}\r\n{v}\r\n", v.len()).into_bytes();
  let null = b"$-1\r\n".to_vec();
  // 正索引：0 首条即命中直写；1 走 skip 分支后命中直写
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Lindex, &[b"l", b"0"]),
    bulk("e1"),
    "正索引 0 帧逐字节"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Lindex, &[b"l", b"1"]),
    bulk("e2"),
    "正索引 1（skip 分支）帧逐字节"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Lindex, &[b"l", b"2"]),
    bulk("e3"),
    "尾端正索引帧逐字节"
  );
  // 负索引折算
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Lindex, &[b"l", b"-1"]),
    bulk("e3"),
    "负索引 -1 帧逐字节"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Lindex, &[b"l", b"-3"]),
    bulk("e1"),
    "负索引 -3（折算到头端）帧逐字节"
  );
  // 越界（正端出窗 / 负端折算出窗）
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Lindex, &[b"l", b"3"]),
    null,
    "正越界回 null 帧（RESP2 逐字节）"
  );
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Lindex, &[b"l", b"-4"]),
    null,
    "负越界折算出窗回 null 帧（RESP2 逐字节）"
  );
  // 空列表语义（缺失键）：null 帧逐字节
  assert_eq!(
    auto_exec(&env, &mut s, RespCommand::Lindex, &[b"nope", b"0"]),
    null,
    "缺失键（空列表语义）回 null 帧（RESP2 逐字节）"
  );
}

/// LPUSH 写臂头端序号定位（list_head_seq）：错误帧且零写入——
/// 静默回落基准序号会把新元素覆盖到既有元素上（数据销毁）
#[test]
fn lpush_head_seq_scan_err_is_error_frame() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-lpush.db");
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
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-rpush.db");
  promote_list3(&env, b"l");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Rpush, &[b"l", b"n1"], "RPUSH");
}

/// ZRANGE 全量读臂（zset_scan_select 流式内核）：错误帧而非空数组
#[test]
fn zrange_scan_err_is_error_frame() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-zrange.db");
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
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-zcount.db");
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
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-zrank.db");
  promote_zset2(&env, b"z");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Zrank, &[b"z", b"m1"], "ZRANK");
}

/// ZLEXCOUNT 读臂（zset_lex_select 断点趟）：错误帧而非 0
#[test]
fn zlexcount_scan_err_is_error_frame() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-zlexcount.db");
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
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-hscan.db");
  promote_hash3(&env, b"h");
  let mut s = session_with(&env);
  assert_scan_err_frame(&env, &mut s, RespCommand::Hscan, &[b"h", b"0"], "HSCAN");
}

/// 物化降级通道（tiered_materialize_blob 四臂）：HDEL 穿透物化时扫描 Err
/// 必须中止（fail-fast 不写回）——旧行为静默截断成空对象会触发删空自愈
/// 把整键销毁（数据丢失）。失败后键与数据完好
#[test]
fn materialize_scan_err_aborts_without_data_loss() {
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-materialize.db");
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
  let _inject_gate = INJECT_SERIALIZE.lock().unwrap();
  let env = tiered_env("scan-err-clean.db");
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
