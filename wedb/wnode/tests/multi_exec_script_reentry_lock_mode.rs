//! EXEC 重放段脚本重入的锁器模式回归（票 wlua-multi-exec-replay-script-lockmode-inherit）
//!
//! 缺陷背景：收口前 garnet_api::exec 选型点对 EXEC 重放段（含脚本重入共享会话
//! 的 redis.call）无条件按 `txn_state == Running` 下传 `Transactional` 让闩，而
//! rust 无 C# 内嵌 processor（C# 脚本命令恒走 basicApi ephemeral 自取闩，见
//! doc/zh/deviations.md §139），脚本触碰事务锁集外的键即零闩读—算—写盲写，与
//! 他连接同键并发必丢更新。收口后选型点按本命令键窗对本事务持锁桶域的会合判定
//! 选型：域内让闩复用（声明键路径绝不自撞）、域外 Basic 自取闩（未声明键与他
//! 连接经同一份 windex 桶闩互斥）。
//!
//! 锁具形仿 `rmw_key_concurrency.rs` 的 Transactional 让闩用例补脚本重入回归：
//! - `undeclared_key_concurrent_*` / `declared_key_concurrent_*`：双线程压测，
//!   事务连接每轮 MULTI;EVAL;EXEC 整批一次泵入并以闭泵臂闭环（挂起/降级臂全部
//!   驱动完毕，无 unclosed 竞态），只统计**整帧回执吻合**的已回执轮次；他连接
//!   同键裸 INCR 同样按整数回执计数。断言「存储终值 == 两侧已回执数之和」——
//!   收口前未声明键臂按构造即丢写（零闩盲写覆写他连接已回执自增）；声明键臂
//!   同时钉死「锁集内让闩不自撞挂死」判据（若误改成脚本恒 Basic 自取，同桶
//!   已持闩自等将致错误帧/挂死，整帧吻合率断言与终值守恒双双破裂）。
//! - `mixed_key_shapes_serial_exact`：单线程确定性串行判据——同一脚本混触声明
//!   键与未声明键，逐值精确断言（无调度器赌注），并钉死「EXEC 应答整帧字节形」
//!   不受选型改造扰动。

use std::{str::from_utf8, sync::Arc, thread};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};
use wtest_base::{open_test_store, resp_frame as frame};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 并发连接数（事务臂 + 裸写臂双线程）与每连接轮次（250 同键争用足以在收口前
/// 稳定复现丢更新，量级对齐 rmw_key_concurrency）
const ITERS: usize = 250;

/// 在既有 store 上开一条 enable_lua 独立连接（生产装配路径，脚本重入共享会话）
///
/// 事务组件按产线同款两段式装配显式挂接（对标 provider 装配期
/// `inject_dependencies` 单点；仓内 MULTI/EXEC e2e 夹具同此口径——
/// `transaction_tests` / `wtxn_exec_lock_async_retry` 均经
/// `attach_transaction_components` 接线，未挂接会话 MULTI 按未接线回
/// unknown command 哨兵）
fn lua_consumer_on(store: &Arc<WedbStore<SegmentedDevice>>) -> RespSessionConsumer {
  let mut c = RespSessionConsumer::new(
    1,
    RespServerSessionOptions {
      enable_lua: true,
      ..RespServerSessionOptions::default()
    },
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  c.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  c
}

/// 多命令整批往返：一次泵入后以闭泵臂把慢路径 / 脚本挂起全部驱动完毕
/// （挂起态不留到下轮——每轮要么完整回执要么终态错误帧，竞态闭环）
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, frames: &[Vec<u8>]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  for f in frames {
    scratch.extend_from_slice(f);
  }
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = c.try_consume_messages_into(&mut resp);
  rt.block_on(wnode_test::drive_pending_parks_consumer(c, &mut resp));
  resp
}

/// `:N\r\n` 整数回执解析
fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

/// `$N\r\n<payload>\r\n` 批量回执解析
fn reply_bulk(resp: &[u8]) -> Option<Vec<u8>> {
  let rest = resp.strip_prefix(b"$")?;
  let end = rest
    .windows(2)
    .position(|w| w == b"\r\n")
    .filter(|&i| i + 2 <= rest.len())?;
  let len: usize = from_utf8(&rest[..end]).ok()?.parse().ok()?;
  let payload = rest.get(end + 2..)?.get(..len)?;
  Some(payload.to_vec())
}

/// 单命令往返取终值（断言面）
fn get_value(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Vec<u8> {
  let rt = Runtime::new().unwrap();
  let mut c = lua_consumer_on(store);
  reply_bulk(&roundtrip(&rt, &mut c, &[frame(&[b"GET", key])])).unwrap_or_default()
}

/// 构造「MULTI; EVAL script numkeys [key…]; EXEC」整批帧
fn multi_eval_frames(script: &[u8], argv: &[&[u8]]) -> Vec<Vec<u8>> {
  let mut eval = vec![b"EVAL".as_slice(), script];
  eval.extend(argv.iter().copied());
  vec![frame(&[b"MULTI"]), frame(&eval), frame(&[b"EXEC"])]
}

/// 解析「+OK; +QUEUED; *1 :N」整帧形态，吻合回脚本内自增回执值 N
/// （不吻合 = 本轮出现降级错误帧/半途应答，按「未回执」处置）
fn exec_single_int_reply(resp: &[u8]) -> Option<i64> {
  resp
    .strip_prefix(b"+OK\r\n+QUEUED\r\n*1\r\n")
    .and_then(reply_int)
}

/// 事务臂脚本 INCR（未声明键）× 他连接同键裸 INCR：终值必须等于两侧已回执数之和
/// （收口前脚本臂零闩盲写，必覆掉他连接已回执自增）
#[test]
fn multi_exec_script_undeclared_key_concurrent_no_lost_update() {
  let (_dir, store) = open_test_store("mvl-undeclared.db").unwrap();
  let key = b"mvl:undeclared";
  let script = b"return redis.call('INCR', 'mvl:undeclared')";

  // 事务臂：每轮 MULTI;EVAL;EXEC，脚本触碰未声明键（不在事务锁集）
  let store_a = Arc::clone(&store);
  let txn_arm = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    let mut c = lua_consumer_on(&store_a);
    (0..ITERS)
      .filter(|_| {
        exec_single_int_reply(&roundtrip(&rt, &mut c, &multi_eval_frames(script, &[b"0"])))
          .is_some()
      })
      .count()
  });

  // 裸写臂：他连接同键 INCR（收口后即同锁内存上的真正互斥对手方）
  let store_b = Arc::clone(&store);
  let raw_arm = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    let mut c = lua_consumer_on(&store_b);
    (0..ITERS)
      .filter(|_| reply_int(&roundtrip(&rt, &mut c, &[frame(&[b"INCR", key])])).is_some())
      .count()
  });

  let txn_acked = txn_arm.join().expect("事务臂线程无 panic");
  let raw_acked = raw_arm.join().expect("裸写臂线程无 panic");
  assert!(
    txn_acked > 0 && raw_acked > 0,
    "两臂都须有已回执更新，否则用例无覆盖（事务臂 {txn_acked} / 裸写臂 {raw_acked}）"
  );

  let final_value: i64 = String::from_utf8(get_value(&store, key))
    .unwrap()
    .parse()
    .expect("INCR 终值必须是十进制整数");
  assert_eq!(
    final_value,
    (txn_acked + raw_acked) as i64,
    "脚本重入未声明键并发同键 INCR 丢更新：终值 {final_value} ≠ 已回执总数 {}",
    txn_acked + raw_acked
  );
}

/// 事务臂脚本经 KEYS[1] 声明触键（在事务锁集内，让闩复用不挂死）× 他连接同键
/// 裸 SET 基线读回校验：声明键臂必须整帧回执且终值非零（自撞误取闩即错误帧/
/// 挂死，整帧吻合即证让闩复用生效）
#[test]
fn multi_exec_script_declared_key_concurrent_no_self_collision() {
  let (_dir, store) = open_test_store("mvl-declared.db").unwrap();
  let key = b"mvl:declared";
  let script = b"return redis.call('INCR', KEYS[1])";

  let store_a = Arc::clone(&store);
  let txn_arm = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    let mut c = lua_consumer_on(&store_a);
    (0..ITERS)
      .filter(|_| {
        exec_single_int_reply(&roundtrip(
          &rt,
          &mut c,
          &multi_eval_frames(script, &[b"1", key]),
        ))
        .is_some()
      })
      .count()
  });

  // 他连接读侧：同键 GET 不得被持锁窗挂死（EXEC 提交放闩后即可读）
  let store_b = Arc::clone(&store);
  let read_arm = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    let mut c = lua_consumer_on(&store_b);
    (0..ITERS)
      .filter(|_| roundtrip(&rt, &mut c, &[frame(&[b"GET", key])]).starts_with(b"$"))
      .count()
  });

  let txn_acked = txn_arm.join().expect("事务臂线程无 panic");
  let read_ok = read_arm.join().expect("读侧线程无 panic");
  assert!(
    txn_acked > 0,
    "声明键事务臂必须有已回执轮次（整帧吻合率为 0 即让闩复用失效）"
  );
  assert!(read_ok > 0, "读侧必须读到 bulk 应答（挂死/错误帧即零吻合）");

  let final_value: i64 = String::from_utf8(get_value(&store, key))
    .unwrap()
    .parse()
    .expect("声明键终值必须是十进制整数");
  assert_eq!(
    final_value, txn_acked as i64,
    "声明键路径终值 {final_value} ≠ 事务臂已回执自增数 {txn_acked}（让闩窗内被覆写即丢值）"
  );
}

/// 单线程确定性串行判据：同一脚本混触声明键与未声明键，逐值精确断言，
/// 并钉死 EXEC 应答整帧字节形不受选型改造扰动
#[test]
fn multi_exec_script_mixed_key_shapes_serial_exact() {
  let (_dir, store) = open_test_store("mvl-mixed.db").unwrap();
  let rt = Runtime::new().unwrap();
  let mut c = lua_consumer_on(&store);

  // 基值：声明键 10、未声明键 100
  assert_eq!(
    roundtrip(&rt, &mut c, &[frame(&[b"SET", b"mvl:decl", b"10"])],),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[frame(&[b"SET", b"mvl:undecl", b"100"])],),
    b"+OK\r\n"
  );

  // 脚本对声明键 INCR、对未声明键 SET 固定值、并回显两值之和（判定顺序确定）
  let resp = roundtrip(
    &rt,
    &mut c,
    &multi_eval_frames(
      b"local a = redis.call('INCR', KEYS[1]) local b = redis.call('GET', 'mvl:undecl') redis.call('SET', 'mvl:undecl', 'side-' .. a) return {a, b}",
      &[b"1", b"mvl:decl"],
    ),
  );
  // 期望 *1 → *2 数组：{11, "100"}
  assert_eq!(
    resp, b"+OK\r\n+QUEUED\r\n*1\r\n*2\r\n:11\r\n$3\r\n100\r\n",
    "混触声明/未声明键的 EXEC 应答整帧字节形漂移（选型改造即回归点）"
  );

  assert_eq!(get_value(&store, b"mvl:decl"), b"11");
  assert_eq!(get_value(&store, b"mvl:undecl"), b"side-11");

  // 第二轮（脚本未声明键的 SET 落在上轮脚本写后的值上）：仍须逐值精确
  let resp = roundtrip(
    &rt,
    &mut c,
    &multi_eval_frames(
      b"local a = redis.call('INCR', KEYS[1]) return redis.call('STRLEN', 'mvl:undecl') + a",
      &[b"1", b"mvl:decl"],
    ),
  );
  assert_eq!(
    resp, b"+OK\r\n+QUEUED\r\n*1\r\n:19\r\n",
    "第二轮混触应答漂移（side-11 长 7 + decl 递增至 12 → 19）"
  );
}
