#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 慢臂装载窗键消亡承接回归（票
//! wnode-slow-load-degrade-reload-vanish-busy-folded-to-storage-error）
//!
//! 缺陷形两处：
//! 1. 读臂装载公共体 slow_load_eval 的 Degrade 承接臂把物化装载 None（键在
//!    两探针 await 让渡窗内被并发 DEL / 懒降阶摘 Meta / FLUSHDB 换号）折成
//!    Err(())，沿 err_frame! 单源出 -ERR slow path storage error；且装载走
//!    门禁形态 load_stub 把 MigrationBusy 一并折 Err——读面忙拒面扩大，
//!    违背 try_tiered_arm 头注在册验收「并发升阶 / 物化的自迁移开窗期间
//!    LRANGE/SCARD 等轮询读不得折存储错误帧」与 tiered_materialize_blob
//!    头注契约「Ok(None) 调用方维持既有装载路径」。
//! 2. 写回面封窗装载核 load_typed_sealed 把 sealed 物化 Ok(None)（claim 窗内
//!    键消亡）折 Err(())——违背 tiered_materialize_blob_sealed 头注契约
//!    「调用方落信封通道按 Missing 新建承接」（run_async_rmw 物化臂同位
//!    break 'materialize 已是守约消费形态）。
//!
//! 修法判据（本文件锁定）：读臂 Degrade 承接改走读臂装载单点
//! load_collection_stub_for_read（MigrationBusy 回退装载快照）+ None 重跑
//! 一次 obj_load_typed（Missing → on_missing 短路帧、WrongType → 装载核既有
//! 帧、Present → 信封求值；再度 Degrade 维持 Err 折算防忙时报假缺失）；
//! sealed None 映射 SealedLoad::Missing 交既有 Missing 臂——对齐 C# 对象层
//! OK/NOTFOUND/WRONGTYPE 三态无存储错误帧通道
//!（libs/server/Storage/Session/ObjectStore/Common.cs:ReadObjectStoreOperation）。
//!
//! 窗内注入形态：读臂用 wkv 停车注入钩子 STUB_LOAD_PAUSE_INJECT（一次性，
//! 消费即复位）把读连接任务确定性停在装载让渡窗内；写臂用 STUB_WIN_*
//! 三元组（仅 claim 在册的持窗者装载消费——封窗物化臂窗内停车，先例同
//! STUB_LOAD 形制），宿主经独立连接注入**真实** DEL / SET / SADD / FLUSHDB
//! 后放行——被停装载续跑即如实见到注入后键态，全程真存储真命令，无 fake
//! mock、无概率性锤打。

// 全文件围绕 debug-only 注入钩构造，release 整文件剔除
#![cfg(debug_assertions)]
use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use compio::runtime::Runtime;
use wcol::{SET_MEMBER_DUMMY_VALUE, types::member_ttl::encode_member};
use wkv::{
  STUB_LOAD_PAUSE_INJECT, STUB_LOAD_PAUSED, STUB_LOAD_RESUME, STUB_WIN_LOAD_PAUSE_INJECT,
  STUB_WIN_LOAD_PAUSED, STUB_WIN_LOAD_RESUME,
};
use wnode::{
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::garnet_api::{GarnetApi, StoreGarnetApi},
};
use wnode_test::{Conn, TestEnv, conn_on_store, session_on, tiered_env};
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

/// 等待读连接任务停入装载让渡窗（带死线防护：注入未被消费即快速红）
fn wait_paused(paused: &AtomicBool, label: &str) {
  let deadline = Instant::now() + Duration::from_secs(30);
  while !paused.load(Ordering::Acquire) {
    if Instant::now() > deadline {
      panic!("{label} 读连接未在 30s 内停入装载窗（钩子未被消费，链路断裂）");
    }
    thread::sleep(Duration::from_millis(1));
  }
}

/// 装载窗确定性探针：武装停车钩子 → 读连接发起命令停入窗内 → 宿主注入并发
/// 键态变更 → 放行续跑 → 返回应答帧。注入闭包在宿主线程经独立连接执行真实
/// 命令
fn window_probe(
  env: &wnode_test::TestEnv,
  cmd: RespCommand,
  args: &[&[u8]],
  inject: impl FnOnce(&wnode_test::TestEnv) + Send + 'static,
) -> Vec<u8> {
  // 装夹后装弹：promote 链路自身消费装载探测，须在其完成后武装
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
  wait_paused(&STUB_LOAD_PAUSED, "window_probe");
  inject(env);
  STUB_LOAD_RESUME.store(true, Ordering::Release);
  reader.join().expect("读连接线程不得 panic")
}

/// claim 持窗者装载窗探针（写回面封窗物化臂）：停车点为封窗后的未门禁装载
/// 域读之前，宿主窗内注入 FLUSHDB 换号后续跑装载即落入新代空域
fn win_window_probe(
  env: &wnode_test::TestEnv,
  cmd: RespCommand,
  args: &[&[u8]],
  inject: impl FnOnce(&wnode_test::TestEnv) + Send + 'static,
) -> Vec<u8> {
  STUB_WIN_LOAD_PAUSE_INJECT.store(true, Ordering::Release);
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
  wait_paused(&STUB_WIN_LOAD_PAUSED, "win_window_probe");
  inject(env);
  STUB_WIN_LOAD_RESUME.store(true, Ordering::Release);
  reader.join().expect("读连接线程不得 panic")
}

/// 宿主线程经独立连接执行单命令并阻塞闭环（注入用真实命令面）
fn host_exec(env: &wnode_test::TestEnv, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let mut c = conn_on_store(&env.store);
  env.rt.block_on(c.exec(cmd, args))
}

/// 漏斗面宿主连接：装配常驻 SingleDatabaseManager（无 AOF 形态，对标 C#
/// !EnableAOF）——FLUSHDB 清库唯一漏斗经常驻 manager 换号，管理面未装配即
/// 显式回 checkpoint channel not configured（garnet_api/slow/admin.rs 漏斗门禁）
fn conn_on_store_funnel(env: &wnode_test::TestEnv) -> Conn {
  let cp_dir = env._dir.path().join("cp-funnel");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&env.store),
    Arc::clone(&env.store.device),
    cp_dir.clone(),
    None,
  ));
  let api: GarnetApi = Arc::new(
    StoreGarnetApi::new(env.store.new_session().unwrap())
      .with_database_manager(Arc::new(SingleDatabaseManager::new(cp_dir, db))),
  )
  .into();
  let s = session_on(&api);
  Conn { api, s }
}

/// 窗内注入 FLUSHDB 专用宿主执行（漏斗面连接，真实换号）
fn host_exec_funnel(env: &wnode_test::TestEnv, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let mut c = conn_on_store_funnel(env);
  env.rt.block_on(c.exec(cmd, args))
}

/// 窗内并发 DEL（顶层 fn 形态：零捕获闭包满足注入面 'static 约束）
fn window_del(env: &wnode_test::TestEnv, key: &[u8]) {
  assert_eq!(
    host_exec(env, RespCommand::Del, &[key]),
    b":1\r\n",
    "窗内注入 DEL 应真实生效"
  );
}

/// 解析 RESP 数组帧 → 条目字节列表（非法帧直接 panic，帧合法性门）
fn parse_bulk_array(frame: &[u8], expect_n: usize) -> Vec<Vec<u8>> {
  let text = String::from_utf8_lossy(frame);
  let mut parts = text.split("\r\n");
  let arr_hdr = parts.next().unwrap();
  assert!(arr_hdr.starts_with('*'), "应答应为数组帧: {text}");
  let n: usize = arr_hdr[1..].parse().unwrap();
  assert_eq!(n, expect_n, "数组基数须为 {expect_n}，实际 {n}: {text}");
  let mut items = Vec::with_capacity(n);
  for _ in 0..n {
    let hdr = parts.next().unwrap();
    assert!(hdr.starts_with('$'), "条目 bulk 头: {hdr}");
    let len: usize = hdr[1..].parse().unwrap();
    let val = parts.next().unwrap().as_bytes().to_vec();
    assert_eq!(val.len(), len);
    items.push(val);
  }
  items
}

/// 窗内并发 DEL：装载窗内键消亡，纯读命令应答为 on_missing 短路帧
///（SMEMBERS → `*0` 空数组、HMGET → null 数组），绝不折 -ERR slow path
/// storage error——C# NOTFOUND 三态收口，无存储错误帧通道
#[test]
fn read_arm_window_del_answers_missing_frame() {
  let env = tiered_env("slowload-reload-del.db");
  promote_set3(&env, b"s");
  promote_hash3(&env, b"h");

  let out = window_probe(&env, RespCommand::Smembers, &[b"s"], |env| {
    window_del(env, b"s")
  });
  assert_eq!(
    out,
    b"*0\r\n".to_vec(),
    "SMEMBERS 窗内键消亡应答须为空数组帧，实际: {}",
    String::from_utf8_lossy(&out)
  );

  let out = window_probe(&env, RespCommand::Hmget, &[b"h", b"f1"], |env| {
    window_del(env, b"h")
  });
  assert_eq!(
    out,
    b"*1\r\n$-1\r\n".to_vec(),
    "HMGET 窗内键消亡应答须为 null 数组帧，实际: {}",
    String::from_utf8_lossy(&out)
  );
}

/// 窗内删后改型（DEL + SET 字符串）：装载承接走 String 域反探，应答为装载核
/// 既有 WRONGTYPE 帧（三态收口之一），同样不出存储错误帧
#[test]
fn read_arm_window_type_swap_answers_wrongtype_frame() {
  let env = tiered_env("slowload-reload-typeswap.db");
  promote_set3(&env, b"s");

  let out = window_probe(&env, RespCommand::Smembers, &[b"s"], |env| {
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
}

/// 窗内删后信封重建（DEL + SADD 落内存信封）：装载承接走 Present 臂对象层
/// 求值，应答为信封内存态成员帧（本可正确交付的数据面不得丢）
#[test]
fn read_arm_window_envelope_recreate_answers_members() {
  let env = tiered_env("slowload-reload-recreate.db");
  promote_set3(&env, b"s");

  let out = window_probe(&env, RespCommand::Smembers, &[b"s"], |env| {
    assert_eq!(host_exec(env, RespCommand::Del, &[b"s"]), b":1\r\n");
    assert_eq!(
      host_exec(env, RespCommand::Sadd, &[b"s", b"mX", b"mY"]),
      b":2\r\n",
      "窗内注入 SADD 应真实生效并落内存信封"
    );
  });
  assert!(
    !out.starts_with(b"-"),
    "窗内信封重建 SMEMBERS 不得出任何错误帧: {}",
    String::from_utf8_lossy(&out)
  );
  let mut items = parse_bulk_array(&out, 2);
  items.sort();
  assert_eq!(
    items,
    vec![b"mX".to_vec(), b"mY".to_vec()],
    "应答须为重建信封的两成员"
  );
}

/// 写回面封窗物化窗内 FLUSHDB 换号（claim 持有者装载落入新代空域 → sealed
/// 物化 Ok(None) 键消亡态）：SPOP 按 Missing 短路承接应答 nil，绝不折
/// -ERR slow path storage error（修复前红：Ok(None) 折 Err 出错误帧）
#[test]
fn write_arm_window_flushdb_answers_nil() {
  let env = tiered_env("slowload-reload-flushdb.db");
  promote_set3(&env, b"s");

  let out = win_window_probe(&env, RespCommand::Spop, &[b"s"], |env| {
    assert_eq!(
      host_exec_funnel(env, RespCommand::Flushdb, &[]),
      b"+OK\r\n",
      "窗内注入 FLUSHDB 应真实换号"
    );
  });
  assert_eq!(
    out,
    b"$-1\r\n".to_vec(),
    "封窗物化窗内键消亡 SPOP 应答须为 nil（Missing 短路承接），实际: {}",
    String::from_utf8_lossy(&out)
  );
}

/// 静默对照（无注入）：分层键纯读 / 装载型写臂正常出件，证明停车钩子一次性
/// 消费即复位、不污染稳态链路
#[test]
fn quiescent_tiered_keys_unaffected_by_hooks() {
  let env = tiered_env("slowload-reload-quiescent.db");
  promote_set3(&env, b"s");
  promote_hash3(&env, b"h");

  assert!(!STUB_LOAD_PAUSE_INJECT.load(Ordering::Acquire));
  assert!(!STUB_WIN_LOAD_PAUSE_INJECT.load(Ordering::Acquire));

  let mut c = conn_on_store(&env.store);
  let out = env.rt.block_on(c.exec(RespCommand::Smembers, &[b"s"]));
  assert!(
    !out.starts_with(b"-"),
    "静默态不应有任何错误帧，实际: {}",
    String::from_utf8_lossy(&out)
  );
  let mut items = parse_bulk_array(&out, 3);
  items.sort();
  assert_eq!(
    items,
    vec![b"m1".to_vec(), b"m2".to_vec(), b"m3".to_vec()],
    "三成员全量出件"
  );

  // HRANDFIELD 无 count：随机单字段 bulk 帧（分层树内流式抽样臂）
  let out = env.rt.block_on(c.exec(RespCommand::Hrandfield, &[b"h"]));
  assert!(
    !out.starts_with(b"-"),
    "静默态 HRANDFIELD 不应出错误帧，实际: {}",
    String::from_utf8_lossy(&out)
  );

  // SPOP 静默：封窗物化 Present 臂弹出单成员 bulk 帧
  let out = env.rt.block_on(c.exec(RespCommand::Spop, &[b"s"]));
  assert!(
    !out.starts_with(b"-"),
    "静默态 SPOP 不应出错误帧，实际: {}",
    String::from_utf8_lossy(&out)
  );
}
