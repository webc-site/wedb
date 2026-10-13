#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 慢臂两装载间让渡窗键消亡承接回归（票
//! wnode-object-scan-degrade-vanish-race-folded-to-slow-path-storage-error）
//!
//! 缺陷形：慢臂 `slow::object_scan` 的 ObjLoad::Degrade 承接臂把
//! `exec_tiered_scan` 的 `Ok(false)`（装载即键不存在 / 非分层态，头注契约
//! 「调用方维持既有路径」）折成 `Err(())`，沿 err_frame! 单源出
//! -ERR slow path storage error——慢臂内两次装载之间（obj_load_typed 异步
//! Meta 探测命中返回 Degrade 之后、exec_tiered_scan 内
//! load_collection_stub_for_read 之前隔 await 让渡点）他连接任务插入并发
//! DEL / FLUSHALL / 懒降阶写，纯只读扫描即对客户端突发存储错误帧，违背
//! C# ObjectScan 三态收口（OK / NOTFOUND / WRONGTYPE，无存储错误帧通道，
//! libs/server/Resp/Objects/SharedObjectCommands.cs:ObjectScan）。
//!
//! 修法判据（本文件锁定）：Ok(false) 按契约回环重装载一次，键态至此已定：
//! Missing → `[0, 空数组]`（C# NOTFOUND）；Present（懒降阶落信封）→ 对象层
//! operate 出等价内存态扫描帧；WrongType → 装载核既有 WRONGTYPE 帧。
//!
//! 窗内注入形态：wkv 停车注入钩子 `STUB_LOAD_PAUSE_INJECT`（一次性，消费即
//! 复位，对标 wbftree GET_OR_OPEN_PAUSE_INJECT 停车-续跑握手）把读连接任务
//! 确定性停在两装载间让渡窗内，宿主线程经独立连接注入**真实** DEL / HDEL
//! 懒降阶 / 删后改型写后放行——被停装载续跑即如实见到注入后键态，全程真
//! 存储真命令，无 fake mock、无概率性锤打。

// 全文件围绕 debug-only 注入钩构造，release 整文件剔除
#![cfg(debug_assertions)]
use std::{
  sync::{Arc, atomic::Ordering},
  thread,
  time::{Duration, Instant},
};

use compio::runtime::Runtime;
use wcol::{SET_MEMBER_DUMMY_VALUE, types::member_ttl::encode_member};
use wkv::{STUB_LOAD_PAUSE_INJECT, STUB_LOAD_PAUSED, STUB_LOAD_RESUME};
use wnode_test::{TestEnv, conn_on_store, promote_env_zset2 as promote_zset2, tiered_env};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

/// 集合升层（[`wnode_test::promote`] 环境适配，永不过期形）
fn promote(
  env: &TestEnv,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
) {
  wnode_test::promote(&env.rt, &env.store, key, obj_type, entries)
}

/// 分层 hash 三字段（f1/f2/f3）
fn promote_hash3(env: &wnode_test::TestEnv, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::Hash,
    vec![
      (b"f1".to_vec(), encode_member(b"v1", None)),
      (b"f2".to_vec(), encode_member(b"v2", None)),
      (b"f3".to_vec(), encode_member(b"v3", None)),
    ],
  );
}

/// 分层 set 三成员
fn promote_set3(env: &wnode_test::TestEnv, key: &[u8]) {
  promote(
    env,
    key,
    GarnetObjectType::Set,
    vec![
      (b"m1".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m2".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
      (b"m3".to_vec(), SET_MEMBER_DUMMY_VALUE.to_vec()),
    ],
  );
}

/// C# NOTFOUND 帧形：`[0, 空数组]` 逐字节
fn scan_not_found_frame() -> Vec<u8> {
  b"*2\r\n$1\r\n0\r\n*0\r\n".to_vec()
}

/// 解析 SCAN 族应答帧 → (游标, 条目字节列表)，非法帧直接 panic（帧合法性门）
fn parse_scan(frame: &[u8]) -> (i64, Vec<Vec<u8>>) {
  let text = String::from_utf8_lossy(frame);
  let mut parts = text.split("\r\n");
  assert_eq!(parts.next(), Some("*2"), "外层应为 *2: {text}");
  let cursor_hdr = parts.next().unwrap();
  assert!(cursor_hdr.starts_with('$'), "游标 bulk 头: {cursor_hdr}");
  let cursor: i64 = parts.next().unwrap().parse().unwrap();
  let arr_hdr = parts.next().unwrap();
  assert!(arr_hdr.starts_with('*'), "条目数组头: {arr_hdr}");
  let n: usize = arr_hdr[1..].parse().unwrap();
  let mut items = Vec::with_capacity(n);
  for _ in 0..n {
    let hdr = parts.next().unwrap();
    assert!(hdr.starts_with('$'), "条目 bulk 头: {hdr}");
    let len: usize = hdr[1..].parse().unwrap();
    let val = parts.next().unwrap().as_bytes().to_vec();
    assert_eq!(val.len(), len);
    items.push(val);
  }
  (cursor, items)
}

/// 等待读连接任务停入两装载间让渡窗（带死线防护：注入未被消费即快速红）
fn wait_stub_paused(label: &str) {
  let deadline = Instant::now() + Duration::from_secs(30);
  while !STUB_LOAD_PAUSED.load(Ordering::Acquire) {
    if Instant::now() > deadline {
      panic!("{label} 读连接未在 30s 内停入存根装载窗（钩子未被消费，链路断裂）");
    }
    thread::sleep(Duration::from_millis(1));
  }
}

/// 两装载窗确定性探针：升阶键上发起 SCAN（读连接停入窗内）→ 宿主注入并发
/// 键态变更 → 放行续跑 → 返回 SCAN 应答帧。注入闭包在宿主线程经独立连接
/// 执行真实命令
fn window_probe(
  env: &wnode_test::TestEnv,
  cmd: RespCommand,
  args: &[&[u8]],
  inject: impl FnOnce(&wnode_test::TestEnv) + Send + 'static,
) -> Vec<u8> {
  // 装夹后装弹：promote 链路自身消费 load_collection_stub，须在其完成后武装
  STUB_LOAD_PAUSE_INJECT.store(true, Ordering::Release);
  let store = Arc::clone(&env.store);
  let (cmd, args_owned): (RespCommand, Vec<Vec<u8>>) =
    (cmd, args.iter().map(|a| a.to_vec()).collect());
  let reader = thread::spawn(move || {
    let rt = Runtime::new().unwrap();
    let mut c = conn_on_store(&store);
    rt.block_on(async {
      let views: Vec<&[u8]> = args_owned.iter().map(|a| a.as_slice()).collect();
      c.exec(cmd, &views).await
    })
  });
  wait_stub_paused("window_probe");
  inject(env);
  STUB_LOAD_RESUME.store(true, Ordering::Release);
  reader.join().expect("读连接线程不得 panic")
}

/// 宿主线程经独立连接执行单命令并阻塞闭环（注入用真实命令面）
fn host_exec(env: &wnode_test::TestEnv, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let mut c = conn_on_store(&env.store);
  env.rt.block_on(c.exec(cmd, args))
}

/// 窗内注入 DEL（顶层 fn 形态：零捕获闭包满足注入面 'static 约束）
fn window_del(env: &wnode_test::TestEnv, key: &[u8]) {
  assert_eq!(
    host_exec(env, RespCommand::Del, &[key]),
    b":1\r\n",
    "窗内注入 DEL 应真实生效"
  );
}

/// 窗内并发 DEL：三域 SCAN 应答均为 C# NOTFOUND 帧 `[0, 空数组]` 逐字节，
/// 绝不折 -ERR slow path storage error（修复前红：Ok(false) 折 Err 出错误帧）
#[test]
fn vanish_del_in_window_answers_not_found_frame() {
  let env = tiered_env("scan-reload-del.db");
  promote_hash3(&env, b"h");
  promote_set3(&env, b"s");
  promote_zset2(&env, b"z");

  let out = window_probe(&env, RespCommand::Hscan, &[b"h", b"0"], |env| {
    window_del(env, b"h")
  });
  assert_eq!(
    out,
    scan_not_found_frame(),
    "HSCAN 窗内键消亡应答须为 NOTFOUND 帧，实际: {}",
    String::from_utf8_lossy(&out)
  );

  let out = window_probe(&env, RespCommand::Sscan, &[b"s", b"0"], |env| {
    window_del(env, b"s")
  });
  assert_eq!(
    out,
    scan_not_found_frame(),
    "SSCAN 窗内键消亡应答须为 NOTFOUND 帧，实际: {}",
    String::from_utf8_lossy(&out)
  );

  let out = window_probe(&env, RespCommand::Zscan, &[b"z", b"0"], |env| {
    window_del(env, b"z")
  });
  assert_eq!(
    out,
    scan_not_found_frame(),
    "ZSCAN 窗内键消亡应答须为 NOTFOUND 帧，实际: {}",
    String::from_utf8_lossy(&out)
  );
}

/// 窗内并发懒降阶写（HDEL 触发物化降阶落信封）：重装载走 Present 臂，应答
/// 为等价内存态扫描帧（剩余字段成对、游标归零），绝不折存储错误帧——本应
/// 正确应答的数据面不得丢
#[test]
fn lazy_demote_in_window_answers_memory_state_frame() {
  let env = tiered_env("scan-reload-demote.db");
  promote_hash3(&env, b"h");

  let out = window_probe(&env, RespCommand::Hscan, &[b"h", b"0"], |env| {
    assert_eq!(
      host_exec(env, RespCommand::Hdel, &[b"h", b"f1"]),
      b":1\r\n",
      "窗内注入 HDEL 应真实生效并触发懒降阶"
    );
  });
  assert!(
    !out.starts_with(b"-ERR "),
    "懒降阶窗内 HSCAN 不得折存储错误帧: {}",
    String::from_utf8_lossy(&out)
  );
  let (cursor, items) = parse_scan(&out);
  assert_eq!(cursor, 0, "内存态三字段单页应游标归零");
  assert_eq!(items.len(), 4, "剩余两字段应成对出件（成员+值）");
  let pairs: Vec<(Vec<u8>, Vec<u8>)> = items
    .chunks(2)
    .map(|p| (p[0].clone(), p[1].clone()))
    .collect();
  for (f, v) in &pairs {
    let expect_v = format!("v{}", String::from_utf8_lossy(f).trim_start_matches('f')).into_bytes();
    assert_eq!(v, &expect_v, "字段-值配对偏离: {:?}", (f, v));
    assert!(f == b"f2" || f == b"f3", "剩余字段应为 f2/f3，实际: {f:?}");
  }
}

/// 窗内删后改型（DEL + SET 字符串）：重装载走 String 域反探，应答为装载核
/// 既有 WRONGTYPE 帧（三态收口之一），同样不出存储错误帧
#[test]
fn type_swap_in_window_answers_wrongtype_frame() {
  let env = tiered_env("scan-reload-typeswap.db");
  promote_set3(&env, b"s");

  let out = window_probe(&env, RespCommand::Sscan, &[b"s", b"0"], |env| {
    assert_eq!(host_exec(env, RespCommand::Del, &[b"s"]), b":1\r\n");
    assert_eq!(
      host_exec(env, RespCommand::Set, &[b"s", b"str"]),
      b"+OK\r\n"
    );
  });
  assert!(
    out.starts_with(b"-WRONGTYPE"),
    "窗内删后改型应答须为既有 WRONGTYPE 帧，实际: {}",
    String::from_utf8_lossy(&out)
  );
  assert!(
    !out.starts_with(b"-ERR "),
    "不得折存储错误帧: {}",
    String::from_utf8_lossy(&out)
  );
}

/// 静默对照（无注入）：分层态 SCAN 正常出件，证明停车钩子一次性消费即复位、
/// 不污染稳态链路；应答含全部成员且游标归零
#[test]
fn quiescent_tiered_scan_unaffected_by_hook() {
  let env = tiered_env("scan-reload-quiescent.db");
  promote_set3(&env, b"s");
  assert!(
    !STUB_LOAD_PAUSE_INJECT.load(Ordering::Acquire),
    "钩子默认必须关闭"
  );
  let mut c = conn_on_store(&env.store);
  let out = env.rt.block_on(c.exec(RespCommand::Sscan, &[b"s", b"0"]));
  assert!(
    !out.starts_with(b"-"),
    "静默态不应有任何错误帧，实际: {}",
    String::from_utf8_lossy(&out)
  );
  let (cursor, items) = parse_scan(&out);
  assert_eq!(cursor, 0);
  assert_eq!(items.len(), 3, "三成员单页全量出件");
}
