use aok::{Error, OK, Result, Void};
use gxhash::HashSet;
use log::info;
use wlua::{DEFAULT_REDIS_VERSION, LuaLoggingMode, LuaMemoryManagementMode, LuaRunner, RespObject};

fn run_script(source: &[u8]) -> Result<RespObject> {
  let mut runner = LuaRunner::new(
    LuaLoggingMode::default(),
    LuaMemoryManagementMode::Native,
    None,
    HashSet::default(),
    source.to_vec(),
    false,
    DEFAULT_REDIS_VERSION,
  )
  .map_err(Error::msg)?;

  let mut out = Vec::new();
  if !runner.compile_for_session(&mut out) {
    return aok::err!("{}", String::from_utf8_lossy(&out));
  }

  runner.run_for_runner(None, None).map_err(Error::msg)
}

#[test]
fn test_rawset_normal_table() -> Void {
  info!("测试 rawset 修改普通 table：正常写入属性并返回该 table");

  let script = r#"
    local t = { a = 1 }
    local ret = rawset(t, 'b', 2)
    if ret ~= t then
        error('rawset must return the modified table', 0)
    end
    if t.b ~= 2 then
        error('property b was not set correctly', 0)
    end

    -- 验证绕过 __newindex 元方法
    local meta_invoked = false
    local with_meta = setmetatable({}, {
        __newindex = function()
            meta_invoked = true
        end
    })
    local ret2 = rawset(with_meta, 'key', 'val')
    if ret2 ~= with_meta or with_meta.key ~= 'val' or meta_invoked then
        error('rawset should bypass __newindex and return table', 0)
    end

    return 'OK'
  "#
  .as_bytes();

  let res = run_script(script)?;
  assert_eq!(res, RespObject::BulkString(b"OK".to_vec()));

  OK
}

#[test]
fn test_rawset_readonly_table() -> Void {
  info!("测试 rawset 修改标记为 __readonly 的只读 table 抛出错误");

  // 1. 测试直接对自定义带有 __readonly 的 table 调用 rawset
  let script1 = r#"
    local t = { __readonly = true }
    rawset(t, 'a', 1)
  "#
  .as_bytes();
  let res1 = run_script(script1);
  assert!(res1.is_err());
  let err_msg1 = res1.unwrap_err().to_string();
  assert!(
    err_msg1.contains("Attempt to modify a readonly table"),
    "错误信息应包含 'Attempt to modify a readonly table'，实际为: {err_msg1}"
  );

  // 2. 测试对沙箱内置只读全局表（如 redis）调用 rawset
  let script2 = r#"
    rawset(redis, 'custom_fn', 123)
  "#
  .as_bytes();
  let res2 = run_script(script2);
  assert!(res2.is_err());
  let err_msg2 = res2.unwrap_err().to_string();
  assert!(
    err_msg2.contains("Attempt to modify a readonly table"),
    "错误信息应包含 'Attempt to modify a readonly table'，实际为: {err_msg2}"
  );

  // 3. 在 Lua 内部使用 pcall 捕获 rawset 对只读表的报错
  let script3 = r#"
    local t = { __readonly = true }
    local ok, err = pcall(rawset, t, 'k', 'v')
    if ok then
        error('rawset on readonly table should have failed', 0)
    end
    return err
  "#
  .as_bytes();
  let res3 = run_script(script3)?;
  match res3 {
    RespObject::BulkString(bytes) => {
      let err_str = String::from_utf8_lossy(&bytes);
      assert!(
        err_str.contains("Attempt to modify a readonly table"),
        "捕获的错误信息应包含 'Attempt to modify a readonly table'，实际为: {err_str}"
      );
    }
    other => return aok::err!("意外的响应类型: {other:?}"),
  }

  OK
}
