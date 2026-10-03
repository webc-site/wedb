//! Lua VM safepoint 超时中断闭环测试
//!
//! 对标 test/standalone/Garnet.test.scripting/LuaScriptTests.cs:IntentionalTimeout
//! （超时报错文案 + 之后脚本继续正常执行）；safepoint 零时钟采样形态对标
//! C# LuaRunner.ResetTimeout/RequestTimeout 的「未激活零钩子原生执行」。

use std::sync::{Arc, atomic::Ordering};

use wbase::time::now_ms_i64;
use wlua::{Deadline, LuaState, TIMEOUT_TRIGGERED};

/// 紧凑循环跑完并校验累加结果（每轮回边都触发 safepoint 中断回调）。
fn run_tight_loop(state: &mut LuaState) {
  state
    .load_string("local s = 0 for i = 1, 1000000 do s = s + i end return s")
    .unwrap();
  state.pcall(0).unwrap();
  assert_eq!(state.check_number(-1), Some(500_000_500_000.0));
  state.clear_stack();
}

/// 激活哨兵 → 下个 safepoint（循环回边）即抛超时错误；disarm 清槽后同一
/// VM 恢复正常执行（IntentionalTimeout 的 Timeout 段报错 + Safe 段返回）。
#[test]
fn sentinel_slot_interrupts_then_disarm_recovers() {
  let mut state = LuaState::new();
  // 看门狗 tick CAS 成功后的槽形态（单调毫秒读数不入槽，激活即哨兵）。
  let slot = Arc::new(Deadline::new(TIMEOUT_TRIGGERED));
  state.hook_shared_deadline(slot.clone());

  state
    .load_string("local i = 0; while true do i = i + 1 end")
    .unwrap();
  let err = state.pcall(0).unwrap_err();
  assert!(
    err.to_string().contains("exceeded configured timeout"),
    "应抛超时错误: {err:?}"
  );
  state.clear_stack();

  // disarm（C# SetCookie(0) 形态）：同 VM 后续脚本正常跑完。
  slot.store(0, Ordering::Release);
  state.load_string("return 42").unwrap();
  state.pcall(0).unwrap();
  assert_eq!(state.check_number(-1), Some(42.0));
  state.clear_stack();
}

/// safepoint 零时钟采样：截止槽为任意正值（远期未到期、早已过期的陈旧
/// 单调读数）都不触发中断——计时判决权在看门狗 tick，回调只认哨兵。
#[test]
fn safepoint_positive_deadline_never_interrupts_without_watchdog() {
  let mut state = LuaState::new();
  // 已设未到期：远期单调截止。
  let slot = Arc::new(Deadline::new(now_ms_i64() + 3_600_000));
  state.hook_shared_deadline(slot.clone());
  run_tight_loop(&mut state);

  // 早已过期的陈旧正值（1 早被任何单调 now 越过）：轮询式实现（读时钟
  // 比对截止）会即刻误杀，哨兵式实现必须放行跑完。
  slot.store(1, Ordering::Release);
  run_tight_loop(&mut state);
}
