//! 脚本层 pcall/xpcall 吞超时抢占回归测试（zcode-r22-wlua P0）
//!
//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:RequestTimeout 的
//! C# 对位面：看门狗到期以 LuaHookMask.Count 每指令钩子重抛超时，脚本层
//! pcall 吞不断。rust 侧超时抢占落在 VM safepoint 中断回调
//! （在 garnet 中的相对路径:libs/server/Lua/LuaRunner.Functions.cs:RequestTimeout
//! 的承接体，state.rs:interrupt_trampoline）。
//!
//! 缺陷形态（修复前红证）：vendored Luau（build.rs LUA_USE_LONGJMP）脚本层
//! pcall 经 lua_pcall → luaD_pcall（lapi.cpp:1189、ldo.cpp:825）自立保护点，
//! 截获超时 lua_error 的 longjmp 后，防重入哨兵悬挂 true（唯一复位点在宿主
//! pcall_n），同 run 内后续全部 safepoint 被挡下——票面用例
//! `pcall(function() while true do end end)`（吞错返回，超时不上抛）及其
//! 尾部死循环变体（后续 safepoint 全被挡，永久挂死）吞掉超时抢占。
//! 修复后哨兵删除，超时错误在剩余每个指令 safepoint
//! （LOP_CALL/CALLFB/RETURN/JUMPBACK/FORNLOOP…VM_INTERRUPT）持续重抛，
//! 脚本层 pcall 吞超时形态消失。
//!
//! 触发口径与生产一致（杜绝「入场即抛」先于脚本层保护点建立的假绿）：真
//! LuaTimeoutManager 专属看门狗线程驱动，run 开始 arm 短截止，激活必落在
//! 死循环执行期（脚本层 pcall 保护帧内）。
//!
//! 断言口径（票面执行方案 3）：超时预算内返回脚本超时错误、worker 不挂死；
//! 全部挂死候选断言自带 recv_timeout 护栏（LuaState/LuaRunner 非 Send，
//! VM 自持专属线程执行），绝不无限等待拖走整轮。

use std::{
  sync::{Arc, mpsc::channel},
  thread,
  time::Duration,
};

use wbase::time::now_ms_i64;
use wlua::{Deadline, LuaOptions, LuaRunner, LuaState, LuaTimeoutManager};

/// 超时错误文案（state.rs TIMEOUT_ERROR 常量段子串，对位既有测试
/// timeout_interrupt.rs 的字面判定口径）。
const TIMEOUT_TEXT: &str = "exceeded configured timeout";

/// 脚本超时预算（毫秒）：tick = 超时/10（timeout.rs TIMEOUT_DIVISIONS），
/// 死循环执行期内必越过激活点。
const TIMEOUT_MILLIS: i64 = 100;

/// 挂死护栏：修复后超时错误在每个指令 safepoint 重抛，返回 ≈ 超时预算量级；
/// 吞掉抢占则挂死至此判红（票面禁止真挂死整轮）。
const HANG_GUARD: Duration = Duration::from_secs(10);

/// 看门狗驱动口径的守护执行（LuaState 原生形态）：专属 manager 线程到期
/// CAS 激活（生产 tick），VM 线程 arm → 装载 → 宿主 pcall → disarm
/// （commands.rs try_execute_script 的装挂对齐），回传（是否报错, 应答文本）。
/// 超时预算内未回车 = 吞掉抢占挂死，判红而不留无限等待。
fn run_guarded(source: &str) -> (bool, String) {
  let source = source.to_owned();
  let manager = Arc::new(LuaTimeoutManager::new(TIMEOUT_MILLIS));
  let registration = manager.register();
  manager.start();
  let (tx, rx) = channel::<(bool, String)>();
  thread::spawn(move || {
    let mut state = LuaState::new();
    state.hook_shared_deadline(registration.shared_deadline());
    registration.arm(now_ms_i64(), TIMEOUT_MILLIS);
    state.load_string(&source).expect("load_string 失败");
    let (is_err, text) = match state.pcall(0) {
      Ok(()) => (false, "ok:宿主 pcall 成功返回".to_owned()),
      Err(err) => (true, err.to_string()),
    };
    registration.disarm();
    let _ = tx.send((is_err, text));
  });
  let res = rx.recv_timeout(HANG_GUARD);
  manager.dispose();
  res.unwrap_or_else(|_| panic!("脚本吞掉超时抢占：worker 挂死（超过 {HANG_GUARD:?} 未返回）"))
}

/// 票面原文用例（执行方案 3）：`pcall(function() while true do end end)`。
/// 修复后：脚本层 pcall 吞下首个超时错误后，chunk 收尾 RETURN safepoint
/// 再次重抛并越过保护点上抛宿主——断言超时错误上抛且 worker 不被挂死。
/// 修复前红证：哨兵悬挂后宿主 pcall 以 Ok 返回（超时被吞，无错误上抛）。
#[test]
fn ticket_case_bare_pcall_timeout_surfaces() {
  let (is_err, text) = run_guarded("pcall(function() while true do end end)");
  assert!(
    is_err,
    "超时错误应上抛宿主 pcall，实际宿主成功返回: {text:?}"
  );
  assert!(text.contains(TIMEOUT_TEXT), "上抛的应是超时错误: {text:?}");
}

/// 票面发现一段原文用例：pcall 吞超时后尾部再挂死循环
/// `pcall(function() while true do end end) while true do end`。
/// 修复前红证：首个超时 longjmp 被脚本层 pcall 截获后哨兵悬挂，挡下全部
/// 后续 safepoint（死循环回边 JUMPBACK 不再中断），尾部死循环吞掉抢占 →
/// worker 永久挂死（由 HANG_GUARD 判红）。
/// 修复后：超时错误在尾部死循环的每个回边 safepoint 重抛并上抛宿主。
#[test]
fn ticket_case_pcall_then_deadloop_surfaces() {
  let (is_err, text) = run_guarded("pcall(function() while true do end end) while true do end");
  assert!(
    is_err,
    "超时错误应上抛宿主 pcall，实际宿主成功返回: {text:?}"
  );
  assert!(text.contains(TIMEOUT_TEXT), "上抛的应是超时错误: {text:?}");
}

/// 同口径覆盖 xpcall（票面执行方案 3）：消息处理器吞下超时 longjmp 后，
/// 尾部死循环仍须被每个 safepoint 抢占重抛。
#[test]
fn ticket_case_xpcall_then_deadloop_surfaces() {
  let (is_err, text) = run_guarded(
    "xpcall(function() while true do end end, function(m) return m end) while true do end",
  );
  assert!(
    is_err,
    "超时错误应上抛宿主 pcall，实际宿主成功返回: {text:?}"
  );
  assert!(text.contains(TIMEOUT_TEXT), "上抛的应是超时错误: {text:?}");
}

/// 嵌套脚本层保护臂（票面「嵌套臂回归用例」rust 可达形态）：双层 pcall
/// 逐层吞超时，每层吞下的下个 safepoint 即再次重抛，最终上抛宿主。
#[test]
fn ticket_case_nested_pcall_arms_surfaces() {
  let (is_err, text) =
    run_guarded("pcall(function() pcall(function() while true do end end) end) while true do end");
  assert!(
    is_err,
    "超时错误应上抛宿主 pcall，实际宿主成功返回: {text:?}"
  );
  assert!(text.contains(TIMEOUT_TEXT), "上抛的应是超时错误: {text:?}");
}

/// EVAL 沙箱形态（票面 `EVAL "..." 0` 原文的执行面对位）：经 LuaRunner
/// 沙箱装载（loader.rs 默认放行 pcall/xpcall），同一超时装挂下超时错误
/// 上抛为宿主错误、worker 不挂死。
#[test]
fn eval_case_pcall_deadloop_surfaces() {
  let source = "pcall(function() while true do end end) while true do end";
  let manager = Arc::new(LuaTimeoutManager::new(TIMEOUT_MILLIS));
  let registration = manager.register();
  manager.start();
  let (tx, rx) = channel::<(bool, String)>();
  thread::spawn(move || {
    let options = LuaOptions::default();
    let mut runner =
      LuaRunner::with_options(&options, source.as_bytes(), "0.0.0.0").expect("runner 构造失败");
    let mut out = Vec::new();
    runner.compile_for_runner(&mut out).expect("编译失败");
    runner.hook_shared_deadline(registration.shared_deadline());
    registration.arm(now_ms_i64(), TIMEOUT_MILLIS);
    let res = runner.run_for_runner(None, None);
    registration.disarm();
    let text = match &res {
      Ok(obj) => format!("ok:{obj:?}"),
      Err(err) => err.clone(),
    };
    let _ = tx.send((res.is_err(), text));
  });
  let res = rx.recv_timeout(HANG_GUARD);
  manager.dispose();
  let (is_err, text) =
    res.unwrap_or_else(|_| panic!("EVAL 吞掉超时抢占：worker 挂死（超过 {HANG_GUARD:?} 未返回）"));
  assert!(is_err, "EVAL 超时错误应上抛宿主，实际成功返回: {text:?}");
  assert!(text.contains(TIMEOUT_TEXT), "上抛的应是超时错误: {text:?}");
}

/// 对照面：截止槽空闲（未 arm，deadline=0）时脚本层 pcall 吞普通错误语义
/// 不受扰动（pcall 捕获错误返回 false，宿主 pcall 成功应答数值）。
#[test]
fn plain_script_pcall_still_catches_errors() {
  let mut state = LuaState::new();
  state.hook_shared_deadline(Arc::new(Deadline::new(0)));
  state
    .load_string("local ok = pcall(function() error('boom') end) return ok and 1 or 7")
    .unwrap();
  state.pcall(0).unwrap();
  assert_eq!(state.check_number(-1), Some(7.0));
}
