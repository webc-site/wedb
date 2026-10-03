//! run 域堆栈归零守卫与 GC 步进回归测试
//! 对标 garnet/libs/server/Lua/LuaRunner.cs:1560-1563（RunCommon finally
//! ExpectLuaStackEmpty）与 Redis evalGenericCommand 收尾
//! lua_settop(0) + lua_gc(LUA_GCSTEP,1)。
//!
//! 三例：
//! 1. 执行报错/preamble 异常后 get_top 恒为 0；
//! 2. 连续复用 runner 不受前次残留影响；
//! 3. LuaTrackedAllocator 限额下 gc_step 释放临时垃圾，避免伪 OOM。

use wlua::{
  LuaOptions, LuaRunner, LuaState, LuaTrackedAllocator, RespObject, ScriptApiError, ScriptingApi,
};
use wresp::command::RespCommand;

/// 最小记录型会话（session 口清栈断言用）。
#[derive(Default)]
struct NullSession {
  protocol_version: u8,
}

impl ScriptingApi for NullSession {
  fn dispatch_resp(&mut self, _request: &[u8], response: &mut Vec<u8>) {
    response.extend_from_slice(b"+OK\r\n");
  }
  fn get(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>, ScriptApiError> {
    Ok(None)
  }
  fn set(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), ScriptApiError> {
    Ok(())
  }
  fn resp_protocol_version(&self) -> u8 {
    self.protocol_version
  }
  fn update_resp_protocol_version(&mut self, version: u8) {
    self.protocol_version = version;
  }
  fn parse_resp_command_buffer(&mut self, _buffer: &[u8]) -> Option<RespCommand> {
    None
  }
  fn check_acl_permissions(&self, _command: RespCommand) -> bool {
    true
  }
}

fn runner(source: &str) -> LuaRunner {
  let opts = LuaOptions::default();
  LuaRunner::with_options(&opts, source.as_bytes(), "0.0.0.0").unwrap()
}

/// 编译 + run_for_runner，返回执行结果；不断言栈态。
fn compile_and_run(
  runner: &mut LuaRunner,
  keys: Option<Vec<Vec<u8>>>,
  argv: Option<Vec<Vec<u8>>>,
) -> Result<RespObject, String> {
  let mut out = Vec::new();
  runner.compile_for_runner(&mut out)?;
  runner.run_for_runner(keys, argv)
}

/// 用例 1：执行报错与正常执行的全部退出路径，栈必归零。
#[test]
fn stack_empty_after_all_exit_paths() {
  // runner 口：脚本运行时 error → run_common 异常臂退出
  {
    let mut r = runner("error('boom')");
    let err = compile_and_run(&mut r, None, None).unwrap_err();
    assert!(err.contains("boom"), "unexpected err: {err}");
    assert_eq!(r.stack_top(), 0, "脚本错误退出后栈未归零");
  }

  // runner 口：正常退出
  {
    let mut r = runner("return 1");
    assert_eq!(
      compile_and_run(&mut r, None, None).unwrap(),
      RespObject::Integer(1)
    );
    assert_eq!(r.stack_top(), 0, "正常退出后栈未归零");
  }

  // session 口：脚本错误经 RESP error 帧退出
  {
    let mut r = runner("error('boom')");
    let mut out = Vec::new();
    assert!(r.compile_for_session(&mut out));
    out.clear();
    let mut session = NullSession::default();
    r.run_for_session(&[b"0"], &mut session, &mut out);
    assert_eq!(&out[..4], b"-ERR", "应写出错误帧: {out:?}");
    assert_eq!(r.stack_top(), 0, "session 错误退出后栈未归零");
  }

  // session 口：numkeys 与实际键数不符（preamble 截断路径）也要归零
  {
    let mut r = runner("return #KEYS");
    let mut out = Vec::new();
    assert!(r.compile_for_session(&mut out));
    out.clear();
    let mut session = NullSession::default();
    r.run_for_session(&[b"3", b"k1"], &mut session, &mut out);
    assert_eq!(r.stack_top(), 0, "preamble 截断路径退出后栈未归零");
  }
}

/// 用例 2：报错执行与不同 KEYS/ARGV 规模交错复用，结果不受残留影响。
#[test]
fn runner_reuse_unaffected_by_residue() {
  let mut r = runner(
    "if ARGV[1] == 'fail' then error('boom') end \
     return {#KEYS, #ARGV, KEYS[1] or '-', ARGV[1] or '-'}",
  );
  let mut out = Vec::new();
  r.compile_for_runner(&mut out).unwrap();

  let k = |s: &str| s.as_bytes().to_vec();
  let expect = |res: RespObject, nk: i64, na: i64, first_key: &str, first_argv: &str| {
    assert_eq!(
      res,
      RespObject::Array(vec![
        RespObject::Integer(nk),
        RespObject::Integer(na),
        RespObject::BulkString(k(first_key)),
        RespObject::BulkString(k(first_argv)),
      ])
    );
  };

  let res = r
    .run_for_runner(
      Some(vec![k("k1"), k("k2")]),
      Some(vec![k("a1"), k("a2"), k("a3")]),
    )
    .unwrap();
  expect(res, 2, 3, "k1", "a1");
  assert_eq!(r.stack_top(), 0);

  // 中途报错的执行不得污染下一次
  let e = r
    .run_for_runner(Some(vec![k("bad")]), Some(vec![k("fail")]))
    .unwrap_err();
  assert!(e.contains("boom"), "unexpected err: {e}");
  assert_eq!(r.stack_top(), 0, "交错报错退出后栈未归零");

  // 规模收缩（触发 reset_keys_and_argv）后旧槽位必须清空
  let res = r
    .run_for_runner(Some(vec![k("z1")]), Some(vec![k("b1")]))
    .unwrap();
  expect(res, 1, 1, "z1", "b1");
  assert_eq!(r.stack_top(), 0);

  // 规模扩张（触发 try_recreate_keys/argv）后仍精确
  let res = r
    .run_for_runner(
      Some(vec![k("m1"), k("m2"), k("m3"), k("m4"), k("m5"), k("m6")]),
      Some(vec![k("c1"), k("c2")]),
    )
    .unwrap();
  expect(res, 6, 2, "m1", "c1");
  assert_eq!(r.stack_top(), 0);

  // 空参数
  let res = r.run_for_runner(None, None).unwrap();
  expect(res, 0, 0, "-", "-");
  assert_eq!(r.stack_top(), 0);
}

/// 用例 3：LuaTrackedAllocator 限额下 gc_step 释放不可达垃圾，恢复配额余量。
///
/// 直接对 state 层驱动：先用一块不可达垃圾把 used_bytes 顶到配额附近，
/// 反复 gc_step 推进增量回收至周期完成；回收后再次大分配应成功。
/// 若 gc_step 未接真实 Luau LUA_GCSTEP（常量或 FFI 错接），垃圾不回收，
/// used_bytes 维持高位，紧随的大分配被配额拒绝 → pcall Err，本例失败。
#[test]
fn gc_step_frees_dead_garbage_under_tracked_quota() {
  const LIMIT: usize = 512 * 1024;
  let mut st = LuaState::with_allocator(LuaTrackedAllocator::new(LIMIT));

  // ~400KB 局部垃圾：块返回后即不可达（未清扫前仍计入 used_bytes）。
  st.load_buffer(
    b"local a = {} for i = 1, 4000 do a[i] = string.rep('x', 100) end",
    "@junk",
  )
  .unwrap();
  st.pcall(0).unwrap();
  assert_eq!(st.get_top(), 0);

  // 推进增量 GC 至周期完成（GCSTEP 返回 1 = 周期收尾）。垃圾对象可能在
  // 上一周期标记期仍可达（黑染），需连驱数个周期确保清扫释放。
  for cycle in 0..3 {
    let mut guard = 0;
    while st.gc_step(256) != 1 {
      guard += 1;
      assert!(guard < 10_000, "gc_step 周期 {cycle} 未在合理步数内完成");
    }
  }

  // 回收后配额余量恢复：再次分配 ~150KB 应成功。
  st.load_buffer(b"return #string.rep('y', 150000)", "@alloc")
    .unwrap();
  st.pcall(0)
    .expect("gc_step 回收后仍无法分配（垃圾未释放，伪 OOM）");
  assert_eq!(st.get_top(), 1);
  st.clear_stack();
}
