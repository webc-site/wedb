//! Lua 四旋钮配置的 wnode 端到端真接线用例（票 wlua-lua-options-config-disconnect）。
//!
//! 打通「NodeArgs 配置 → attach.rs `From<&NodeArgs>` 单点投影 LuaOptions →
//! wlua 消费链」全链，消灭 attach 恒 `..default()` 的假旋钮形态：
//! 1. 投影面：`RespServerSessionOptions::from(&node)` 逐字段核对内存模式/限额/
//!    日志模式/沙箱白名单/超时（对照 C# Options.cs:1029 五参数整体装配）。
//! 2. 消费面：Tracked + 限额配置驱动真实 wkv 库跑大分配 EVAL，断言配额错误帧
//!    （对齐 wlua lib.rs allocator_quota_stops_runaway_script 语义的生产链端到端）；
//!    LuaLoggingMode::Disable 形态 redis.log 回错误帧、Silent 形态静默放行。

use std::{future::Future, sync::Arc};

use compio::runtime::Runtime;
use wconf::{ConfigFileArgs, NodeArgs};
use wlua::{LuaLoggingMode, LuaMemoryManagementMode};
use wnode::resp::{
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::drain_output;
use wtest_base::open_test_store;

type TestStore = Arc<wkv::WedbStore<wdev::SegmentedDevice>>;

fn block_on<F: Future>(fut: F) -> F::Output {
  Runtime::new().unwrap().block_on(fut)
}

/// RESP 数组帧
fn resp(parts: &[&[u8]]) -> Vec<u8> {
  let mut buf = format!("*{}\r\n", parts.len()).into_bytes();
  for p in parts {
    buf.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
    buf.extend_from_slice(p);
    buf.extend_from_slice(b"\r\n");
  }
  buf
}

/// EVAL script numkeys keys... argv...
fn eval_parts(script: &str, keys: &[&[u8]], argv: &[&[u8]]) -> Vec<Vec<u8>> {
  let mut parts = vec![
    b"EVAL".to_vec(),
    script.as_bytes().to_vec(),
    keys.len().to_string().into_bytes(),
  ];
  parts.extend(keys.iter().map(|k| k.to_vec()));
  parts.extend(argv.iter().map(|a| a.to_vec()));
  parts
}

/// 用给定的会话装配选项（含配置投影而来的 lua_options）开一条真实存储会话
fn session_with_opts(store: &TestStore, options: RespServerSessionOptions) -> RespServerSession {
  let session = store.new_session().unwrap();
  let mut s = RespServerSession::new(1, options);
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(session)));
  s
}

/// 消费一条命令并取回响应（redis.call 重入闭环驱动，对位 lua_script_tests）
fn cmd(s: &mut RespServerSession, parts: Vec<Vec<u8>>) -> Vec<u8> {
  let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
  s.recv_buffer.extend_from_slice(&resp(&refs));
  let mut resp_buf = Vec::new();
  s.try_consume_messages();
  s.take_output_into(&mut resp_buf);
  block_on(wnode_test::drive_pending_parks(s, &mut resp_buf, true));
  s.output.extend_from_slice(&resp_buf);
  drain_output(s)
}

/// 大分配脚本：短时限内海量分配，Tracked 配额须先于超时中断（对齐 wlua
/// lib.rs allocator_quota_stops_runaway_script 的脚本源）
const RUNAWAY_SCRIPT: &str = "local t = {} for i = 1, 100000 do t[i] = ('x'):rep(64) end return #t";

/// 投影面：NodeArgs 四旋钮经 attach 单点装配逐字段进 LuaOptions（不再恒 default）
#[test]
fn test_node_args_projects_all_four_lua_knobs() {
  let node = NodeArgs::from_args_iter([
    "wedb",
    "--enable-lua",
    "--lua-memory-management-mode=managed",
    "--lua-script-memory-limit=4mb",
    "--lua-logging-mode=disable",
    "--lua-allowed-functions=redis.call,cjson.encode",
    "--lua-script-timeout-ms=3000",
  ])
  .expect("四旋钮合法配置须启动");
  let opts = RespServerSessionOptions::from(&node);

  assert!(opts.enable_lua);
  assert_eq!(
    opts.lua_options.memory_mode,
    LuaMemoryManagementMode::Managed
  );
  assert_eq!(opts.lua_options.lua_memory_limit_bytes, 4 * 1024 * 1024);
  assert_eq!(opts.lua_options.log_mode, LuaLoggingMode::Disable);
  assert_eq!(
    opts.lua_options.allowed_functions,
    vec!["redis.call".to_string(), "cjson.encode".to_string()]
  );
  assert_eq!(opts.lua_options.timeout_millis, 3000);
  // 超时非无限 + enable_lua → 超时管理器随装配建起（既有链路口径不变）
  assert!(opts.lua_timeout_manager.is_some());
}

/// 缺省态投影零漂移：不传四旋钮即落 C# 生效默认（Native / 0 / Enable / 空）
#[test]
fn test_default_projection_is_not_fake_knob() {
  let node = NodeArgs::from_args_iter(["wedb"]).expect("默认配置");
  let opts = RespServerSessionOptions::from(&node);
  assert_eq!(
    opts.lua_options.memory_mode,
    LuaMemoryManagementMode::Native
  );
  assert_eq!(opts.lua_options.lua_memory_limit_bytes, 0);
  assert_eq!(opts.lua_options.log_mode, LuaLoggingMode::Enable);
  assert!(opts.lua_options.allowed_functions.is_empty());
  assert_eq!(opts.lua_options.timeout_millis, 0);
}

/// 消费面（生产链端到端）：Tracked + 限额配置驱动真实库，大分配 EVAL 触配额
/// 错误帧，配额内小脚本照常执行（证配置真抵达 wlua 分配器防线，非只存字段）
#[test]
fn test_tracked_memory_limit_stops_runaway_script_e2e() {
  let (_dir, store) = open_test_store("lua-opts-quota.db").expect("open test store");
  let node = NodeArgs::from_args_iter([
    "wedb",
    "--enable-lua",
    "--lua-memory-management-mode=tracked",
    "--lua-script-memory-limit=1mb",
  ])
  .expect("Tracked + 1mb 合法");
  let mut s = session_with_opts(&store, RespServerSessionOptions::from(&node));

  let out = cmd(&mut s, eval_parts(RUNAWAY_SCRIPT, &[], &[]));
  let text = String::from_utf8_lossy(&out);
  assert!(
    out.starts_with(b"-") && text.to_ascii_lowercase().contains("memory"),
    "大分配脚本须被配额拦截并回内存错误帧: {text}"
  );

  // 配额内小脚本照常完成（证非一刀切失败，配额真实生效）
  let out = cmd(&mut s, eval_parts("return 'ok'", &[], &[]));
  assert_eq!(out, b"$2\r\nok\r\n");
}

/// 消费面：日志模式经配置真下传 redis.log——Disable 回错误帧、Silent 静默放行
#[test]
fn test_logging_mode_configured_end_to_end() {
  let (_dir, store) = open_test_store("lua-opts-log.db").expect("open test store");

  let disable_node =
    NodeArgs::from_args_iter(["wedb", "--enable-lua", "--lua-logging-mode=disable"])
      .expect("disable 合法");
  let mut s = session_with_opts(&store, RespServerSessionOptions::from(&disable_node));
  let out = cmd(
    &mut s,
    eval_parts("redis.log(redis.LOG_WARNING, 'x') return 1", &[], &[]),
  );
  assert_eq!(
    out, b"-ERR redis.log(...) disabled in Garnet config\r\n",
    "Disable 档 redis.log 须回禁用错误帧"
  );

  let silent_node = NodeArgs::from_args_iter(["wedb", "--enable-lua", "--lua-logging-mode=silent"])
    .expect("silent 合法");
  let mut s = session_with_opts(&store, RespServerSessionOptions::from(&silent_node));
  let out = cmd(
    &mut s,
    eval_parts("redis.log(redis.LOG_WARNING, 'x') return 1", &[], &[]),
  );
  assert_eq!(out, b":1\r\n", "Silent 档 redis.log 静默成功、脚本续跑");
}
