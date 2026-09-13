//! LuaRunner 集成测试
//! 对标 test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs

use std::{iter::once, str};

use gxhash::HashSet;
use wlua::{LuaLoggingMode, LuaMemoryManagementMode, LuaOptions, LuaRunner, RespObject};

const EXPORTED_FUNCS: &[(&str, &[&str])] = &[
  (
    "bit",
    &[
      "tobit", "tohex", "bnot", "bor", "band", "bxor", "lshift", "rshift", "arshift", "rol", "ror",
      "bswap",
    ],
  ),
  ("cjson", &["encode", "decode"]),
  ("cmsgpack", &["pack", "unpack"]),
  (
    "math",
    &[
      "abs",
      "acos",
      "asin",
      "atan",
      "atan2",
      "ceil",
      "cos",
      "cosh",
      "deg",
      "exp",
      "floor",
      "fmod",
      "frexp",
      "huge",
      "ldexp",
      "log",
      "log10",
      "max",
      "min",
      "modf",
      "pi",
      "pow",
      "rad",
      "random",
      "randomseed",
      "sin",
      "sinh",
      "sqrt",
      "tan",
      "tanh",
    ],
  ),
  ("os", &["clock"]),
  (
    "redis",
    &[
      "call",
      "pcall",
      "error_reply",
      "status_reply",
      "sha1hex",
      "log",
      "LOG_DEBUG",
      "LOG_VERBOSE",
      "LOG_NOTICE",
      "LOG_WARNING",
      "setresp",
      "set_repl",
      "REPL_ALL",
      "REPL_AOF",
      "REPL_REPLICA",
      "REPL_SLAVE",
      "REPL_NONE",
      "replicate_commands",
      "breakpoint",
      "debug",
      "acl_check_cmd",
      "REDIS_VERSION",
      "REDIS_VERSION_NUM",
    ],
  ),
  (
    "string",
    &[
      "byte", "char", "find", "format", "gmatch", "gsub", "len", "lower", "match", "rep",
      "reverse", "sub", "upper",
    ],
  ),
  ("struct", &["pack", "unpack", "size"]),
  ("table", &["concat", "insert", "maxn", "remove", "sort"]),
];

fn new_runner(source: &str) -> LuaRunner {
  let opts = LuaOptions::default();
  LuaRunner::with_options(&opts, source.as_bytes(), false, "0.0.0.0").unwrap()
}

fn runner_with_allowed(allowed: &[&str], source: &str) -> LuaRunner {
  let opts = LuaOptions {
    log_mode: LuaLoggingMode::Silent,
    allowed_functions: allowed.iter().map(|s| (*s).to_string()).collect(),
    ..Default::default()
  };
  LuaRunner::with_options(&opts, source.as_bytes(), false, "0.0.0.0").unwrap()
}

fn compile_and_run(runner: &mut LuaRunner) -> Result<RespObject, String> {
  let mut out = Vec::new();
  runner.compile_for_runner(&mut out)?;
  runner.run_for_runner(None, None)
}

fn resp_as_str(resp: &RespObject) -> &str {
  match resp {
    RespObject::BulkString(b) => str::from_utf8(b).expect("valid utf-8 bulk string"),
    RespObject::SimpleString(s) => s.as_str(),
    RespObject::Null => "nil",
    other => panic!("expected string resp object, got {other:?}"),
  }
}

fn assert_is_nil(resp: &RespObject, msg: &str) {
  assert_eq!(resp_as_str(resp), "nil", "{msg}");
}

fn assert_is_not_nil(resp: &RespObject, msg: &str) {
  assert_ne!(resp_as_str(resp), "nil", "{msg}");
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:CannotRunUnsafeScript
#[test]
fn cannot_run_unsafe_script() {
  // Try to load an assembly
  {
    let mut runner = new_runner("luanet.load_assembly('mscorlib')");
    let err = compile_and_run(&mut runner).unwrap_err();
    assert!(
      err.contains("attempt to index nil with 'load_assembly'")
        || err.contains("attempt to index a nil value")
        || err.contains("luanet"),
      "Unexpected error: {err}"
    );
  }

  // Try to call a OS function
  {
    let mut runner = new_runner("os = require('os'); return os.time();");
    let err = compile_and_run(&mut runner).unwrap_err();
    assert!(
      err.contains("attempt to call a nil value") || err.contains("require"),
      "Unexpected error: {err}"
    );
  }

  // Try to execute the input stream
  {
    let mut runner = new_runner("dofile();");
    let err = compile_and_run(&mut runner).unwrap_err();
    assert!(
      err.contains("attempt to call a nil value") || err.contains("dofile"),
      "Unexpected error: {err}"
    );
  }

  // Try to call a windows executable
  {
    let mut runner = new_runner("require \"notepad\"");
    let err = compile_and_run(&mut runner).unwrap_err();
    assert!(
      err.contains("attempt to call a nil value") || err.contains("require"),
      "Unexpected error: {err}"
    );
  }

  // Try to call an OS function
  {
    let mut runner = new_runner("os.exit();");
    let err = compile_and_run(&mut runner).unwrap_err();
    assert!(
      err.contains("attempt to call a nil value") || err.contains("exit"),
      "Unexpected error: {err}"
    );
  }

  // Try to include a new .net library
  {
    let mut runner = new_runner("import ('System.Diagnostics');");
    let err = compile_and_run(&mut runner).unwrap_err();
    assert!(
      err.contains("attempt to call a nil value") || err.contains("import"),
      "Unexpected error: {err}"
    );
  }
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:LoadAndLoadStringNotExposed
#[test]
fn load_and_load_string_not_exposed() {
  // load and loadstring are not exposed in the sandbox
  {
    let mut runner = new_runner("return type(load)");
    let result = compile_and_run(&mut runner).unwrap();
    assert_eq!(resp_as_str(&result), "nil");
  }

  {
    let mut runner = new_runner("return type(loadstring)");
    let result = compile_and_run(&mut runner).unwrap();
    assert_eq!(resp_as_str(&result), "nil");
  }

  // Calling load should error since it's nil
  {
    let mut runner = new_runner("local f = load('return io') return f()");
    let err = compile_and_run(&mut runner).unwrap_err();
    assert!(
      err.contains("attempt to call a nil value"),
      "Unexpected error: {err}"
    );
  }

  // Calling loadstring should error since it's nil
  {
    let mut runner = new_runner("local f = loadstring('return io') return f()");
    let err = compile_and_run(&mut runner).unwrap_err();
    assert!(
      err.contains("attempt to call a nil value"),
      "Unexpected error: {err}"
    );
  }

  // io, debug, and other dangerous modules remain inaccessible
  {
    let mut runner = new_runner("return type(io)");
    let result = compile_and_run(&mut runner).unwrap();
    assert_eq!(resp_as_str(&result), "nil");
  }

  {
    let mut runner = new_runner("return type(debug)");
    let result = compile_and_run(&mut runner).unwrap();
    assert_eq!(resp_as_str(&result), "nil");
  }

  // Internal garnet_* functions should not be reachable from sandbox
  {
    let mut runner = new_runner("return type(garnet_loadstring)");
    let result = compile_and_run(&mut runner).unwrap();
    assert_eq!(resp_as_str(&result), "nil");
  }

  {
    let mut runner = new_runner("return type(garnet_call)");
    let result = compile_and_run(&mut runner).unwrap();
    assert_eq!(resp_as_str(&result), "nil");
  }
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:CanLoadScript
#[test]
fn can_load_script() {
  // Code with error
  {
    let mut runner = new_runner("local;");
    let mut out = Vec::new();
    let err = runner.compile_for_runner(&mut out).unwrap_err();
    assert!(
      err.starts_with("Compilation error:"),
      "Unexpected compile error: {err}"
    );
    assert!(err.contains(';'));
  }

  // Code without error
  {
    let mut runner = new_runner("local list; list = 1; return list;");
    let mut out = Vec::new();
    runner.compile_for_runner(&mut out).unwrap();
  }
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:CanRunScript
#[test]
fn can_run_script() {
  let args = vec![b"arg1".to_vec(), b"arg2".to_vec(), b"arg3".to_vec()];

  // Run code without errors
  {
    let mut runner = new_runner("local list; list = ARGV[1] ; return list;");
    let mut out = Vec::new();
    runner.compile_for_runner(&mut out).unwrap();
    let res = runner.run_for_runner(None, Some(args)).unwrap();
    assert_eq!(res, RespObject::BulkString(b"arg1".to_vec()));
  }

  // Run code with errors
  {
    let mut runner = new_runner("local list; list = ; return list;");
    let mut out = Vec::new();
    let err = runner.compile_for_runner(&mut out).unwrap_err();
    assert!(
      err.starts_with("Compilation error:"),
      "Unexpected compile error: {err}"
    );
    assert!(err.contains(';'));
  }
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:KeysAndArgsCleared
#[test]
fn keys_and_args_cleared() {
  let mut runner = new_runner("return { KEYS[1], ARGV[1], KEYS[2], ARGV[2] }");
  let mut out = Vec::new();
  runner.compile_for_runner(&mut out).unwrap();

  let res1 = runner
    .run_for_runner(
      Some(vec![b"hello".to_vec(), b"world".to_vec()]),
      Some(vec![b"fizz".to_vec(), b"buzz".to_vec()]),
    )
    .unwrap();
  assert_eq!(
    res1,
    RespObject::Array(vec![
      RespObject::BulkString(b"hello".to_vec()),
      RespObject::BulkString(b"fizz".to_vec()),
      RespObject::BulkString(b"world".to_vec()),
      RespObject::BulkString(b"buzz".to_vec()),
    ])
  );

  let res2 = runner
    .run_for_runner(Some(vec![b"abc".to_vec()]), Some(vec![b"def".to_vec()]))
    .unwrap();
  assert_eq!(
    res2,
    RespObject::Array(vec![
      RespObject::BulkString(b"abc".to_vec()),
      RespObject::BulkString(b"def".to_vec()),
    ])
  );

  let res3 = runner
    .run_for_runner(
      Some(vec![b"012".to_vec(), b"345".to_vec()]),
      Some(vec![b"678".to_vec()]),
    )
    .unwrap();
  assert_eq!(
    res3,
    RespObject::Array(vec![
      RespObject::BulkString(b"012".to_vec()),
      RespObject::BulkString(b"678".to_vec()),
      RespObject::BulkString(b"345".to_vec()),
    ])
  );
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:RedisLogDisabled
#[test]
fn redis_log_disabled() {
  let opts = LuaOptions {
    log_mode: LuaLoggingMode::Disable,
    ..Default::default()
  };
  let mut runner = LuaRunner::with_options(
    &opts,
    b"redis.log(redis.LOG_WARNING, 'foo')",
    false,
    "0.0.0.0",
  )
  .unwrap();
  let err = compile_and_run(&mut runner).unwrap_err();
  assert_eq!(err, "ERR redis.log(...) disabled in Garnet config");
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:RedisLogSilent
#[test]
fn redis_log_silent() {
  let opts = LuaOptions {
    log_mode: LuaLoggingMode::Silent,
    ..Default::default()
  };
  let mut runner = LuaRunner::with_options(
    &opts,
    b"redis.log(redis.LOG_WARNING, 'foo')",
    false,
    "0.0.0.0",
  )
  .unwrap();
  let res = compile_and_run(&mut runner);
  assert!(res.is_ok());
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:AllowedFunctions
#[test]
fn allowed_functions() {
  let global_funcs: &[&str] = &[
    "xpcall",
    "tostring",
    "setmetatable",
    "next",
    "assert",
    "tonumber",
    "rawequal",
    "getmetatable",
    "rawset",
    "pcall",
    "coroutine",
    "type",
    "_G",
    "select",
    "unpack",
    "gcinfo",
    "pairs",
    "rawget",
    "ipairs",
    "_VERSION",
    "error",
  ];

  // Check the supported globals
  {
    let source = r#"
      local ret = {}
      for k, v in pairs(_G) do
        table.insert(ret, k)
      end
      return ret
    "#;
    let mut all_runner = new_runner(source);
    let res = compile_and_run(&mut all_runner).unwrap();
    let RespObject::Array(items) = res else {
      panic!("expected array, got {res:?}");
    };
    let all_defined: HashSet<&str> = items
      .iter()
      .map(resp_as_str)
      .filter(|x| *x != "__readonly")
      .collect();

    let mut expected: HashSet<&str> = global_funcs.iter().copied().collect();
    for (key, _) in EXPORTED_FUNCS {
      expected.insert(*key);
    }

    let missing: Vec<_> = expected.difference(&all_defined).copied().collect();
    let extra: Vec<_> = all_defined
      .difference(&expected)
      .copied()
      .filter(|x| *x != "ARGV" && *x != "KEYS" && *x != "redis")
      .collect();

    assert!(missing.is_empty(), "Missing globals: {:?}", missing);
    assert!(extra.is_empty(), "Extra globals: {:?}", extra);

    for &global_func in global_funcs {
      if matches!(global_func, "type" | "_G" | "_VERSION") {
        continue;
      }

      let everything_except: Vec<&str> = expected
        .iter()
        .copied()
        .filter(|s| *s != global_func)
        .collect();

      let mut without_runner =
        runner_with_allowed(&everything_except, &format!("return type({global_func})"));
      assert_is_nil(
        &compile_and_run(&mut without_runner).unwrap(),
        &format!("Global {global_func} available when it shouldn't have been"),
      );

      let mut with_runner = runner_with_allowed(
        &["type", global_func],
        &format!("return type({global_func})"),
      );
      assert_is_not_nil(
        &compile_and_run(&mut with_runner).unwrap(),
        &format!("Global {global_func} not available when it should have been"),
      );
    }
  }

  // Check for the supported Lua functions which are under names in globals
  for (func_group, funcs) in EXPORTED_FUNCS {
    // Check all expected funcs are in the table
    {
      let source = format!(
        r#"local ret = {{ }}
           for k, v in pairs({func_group}) do
             table.insert(ret, k)
           end
           return ret"#
      );
      let mut runner = runner_with_allowed(&[func_group, "pairs", "table.insert"], &source);
      let res = compile_and_run(&mut runner).unwrap();
      let RespObject::Array(items) = res else {
        panic!("expected array, got {res:?}");
      };
      let defined: HashSet<&str> = items
        .iter()
        .map(resp_as_str)
        .filter(|x| *x != "__readonly")
        .collect();

      let missing: Vec<_> = funcs
        .iter()
        .copied()
        .filter(|f| !defined.contains(*f))
        .collect();
      assert!(
        missing.is_empty(),
        "Missing funcs in {func_group}: {:?}",
        missing
      );
    }

    // Check all expected funcs have type != nil
    {
      let all_types = funcs
        .iter()
        .map(|x| format!("type({func_group}.{x})"))
        .collect::<Vec<_>>()
        .join(", ");

      let mut runner =
        runner_with_allowed(&[func_group, "type"], &format!("return {{ {all_types} }}"));
      let defined = compile_and_run(&mut runner).unwrap();
      let RespObject::Array(items) = defined else {
        panic!("expected array, got {defined:?}");
      };
      for (i, for_func) in funcs.iter().enumerate() {
        let func_type = resp_as_str(&items[i]);
        assert_ne!(
          func_type, "nil",
          "{func_group}.{for_func} is not defined when it should be"
        );
      }
    }

    // Check NOT including top level group causes all functions to be unavailable
    {
      let other_groups: Vec<&str> = EXPORTED_FUNCS
        .iter()
        .map(|(g, _)| *g)
        .filter(|g| *g != "_G" && *g != *func_group)
        .chain(once("type"))
        .collect();

      let mut runner = runner_with_allowed(&other_groups, &format!("return type({func_group})"));
      assert_is_nil(
        &compile_and_run(&mut runner).unwrap(),
        &format!("{func_group} is defined when it should not be"),
      );
    }

    // Check allowing just the one func
    for func in *funcs {
      let single = format!("{func_group}.{func}");
      let mut runner = runner_with_allowed(
        &[&single, "type"],
        &format!("return type({func_group}.{func})"),
      );
      assert_is_not_nil(
        &compile_and_run(&mut runner).unwrap(),
        &format!("{func_group}.{func} is not defined when it should be"),
      );
    }

    // Check that disallowing just the one func in the group works
    for func in *funcs {
      let others: Vec<String> = funcs
        .iter()
        .filter(|x| **x != *func)
        .map(|x| format!("{func_group}.{x}"))
        .collect();

      let type_statement = if !others.is_empty() {
        format!("type({func_group}.{func})")
      } else {
        format!("type({func_group})")
      };

      let mut allowed_list: Vec<&str> = others.iter().map(|s| s.as_str()).collect();
      allowed_list.push("type");

      let mut runner = runner_with_allowed(&allowed_list, &format!("return {type_statement}"));
      assert_is_nil(
        &compile_and_run(&mut runner).unwrap(),
        &format!("{func_group}.{func} is defined when it should not be"),
      );
    }
  }
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:InternalFunctionsIgnoredInAllowedFunctions
#[test]
fn internal_functions_ignored_in_allowed_functions() {
  let opts = LuaOptions {
    log_mode: LuaLoggingMode::Silent,
    allowed_functions: vec!["tostring".to_string(), "garnet_call".to_string()],
    ..Default::default()
  };
  let mut all_runner =
    LuaRunner::with_options(&opts, b"return tostring(garnet_call)", false, "0.0.0.0").unwrap();
  let res = compile_and_run(&mut all_runner).unwrap();
  assert_eq!(resp_as_str(&res), "nil");
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:Issue1165
#[test]
fn issue_1165() {
  let opts = LuaOptions {
    memory_mode: LuaMemoryManagementMode::Native,
    lua_memory_limit_bytes: 10 * 1024 * 1024,
    log_mode: LuaLoggingMode::Enable,
    ..Default::default()
  };
  let mut runner = LuaRunner::with_options(&opts, b"return 1", false, "0.0.0.0").unwrap();
  let mut out = Vec::new();
  runner.compile_for_runner(&mut out).unwrap();
  let keys = vec![b"key".to_vec(); 9];
  let argv = vec![b"argv".to_vec(); 9];
  let res = runner.run_for_runner(Some(keys), Some(argv));
  assert!(res.is_ok());
}

/// test/standalone/Garnet.test.scripting/LuaScriptRunnerTests.cs:NeedsDisposeCheck
#[test]
fn needs_dispose_check() {
  for mode in [
    LuaMemoryManagementMode::Native,
    LuaMemoryManagementMode::Tracked,
    LuaMemoryManagementMode::Managed,
  ] {
    for limit in [None, Some(1024 * 1024)] {
      if limit.is_some() && mode == LuaMemoryManagementMode::Native {
        continue;
      }
      let opts = LuaOptions {
        memory_mode: mode,
        lua_memory_limit_bytes: limit.unwrap_or(0) as i64,
        log_mode: LuaLoggingMode::Silent,
        ..Default::default()
      };
      let mut runner = LuaRunner::with_options(&opts, b"return 1", false, "0.0.0.0").unwrap();
      let mut out = Vec::new();
      runner.compile_for_runner(&mut out).unwrap();
      let _ = runner.run_for_runner(Some(vec![]), Some(vec![]));
      assert!(!runner.needs_dispose());
    }
  }
}

/// test/standalone/Garnet.test.scripting/LuaScriptTests.cs:ReadOnlyGlobalTables
#[test]
fn read_only_global_tables() {
  let illegals = [
    "_G", "bit", "cjson", "cmsgpack", "math", "os", "redis", "string", "struct", "table",
  ];
  for illegal in illegals {
    let script = format!("table.insert({}, 'foo')", illegal);
    let mut runner = new_runner(&script);
    let mut out = Vec::new();
    runner.compile_for_runner(&mut out).unwrap();
    let res = runner.run_for_runner(Some(vec![]), Some(vec![]));
    assert!(res.is_err(), "modifying {} should fail", illegal);
    let err_str = res.unwrap_err();
    assert!(
      err_str.contains("Attempt to modify a readonly table"),
      "error for {} should contain 'Attempt to modify a readonly table', got: {}",
      illegal,
      err_str
    );
  }

  let legals = ["KEYS", "ARGV"];
  for legal in legals {
    let script = format!("table.insert({}, 'fizz'); return {}[1];", legal, legal);
    let mut runner = new_runner(&script);
    let res = compile_and_run(&mut runner).unwrap();
    assert_eq!(resp_as_str(&res), "fizz");
  }
}

/// 全内存模式下复杂表与字符串操作测试
#[test]
fn all_memory_modes_table_operations() {
  for mode in [
    LuaMemoryManagementMode::Native,
    LuaMemoryManagementMode::Tracked,
    LuaMemoryManagementMode::Managed,
  ] {
    let opts = LuaOptions {
      memory_mode: mode,
      lua_memory_limit_bytes: if mode == LuaMemoryManagementMode::Native {
        0
      } else {
        2 * 1024 * 1024
      },
      log_mode: LuaLoggingMode::Silent,
      ..Default::default()
    };
    let script = br#"
      local t = {}
      for i = 1, 100 do
        table.insert(t, "item_" .. tostring(i))
      end
      return #t
    "#;
    let mut runner = LuaRunner::with_options(&opts, script, false, "0.0.0.0").unwrap();
    let res = compile_and_run(&mut runner).unwrap();
    assert_eq!(res, RespObject::Integer(100));
    assert!(!runner.needs_dispose());
  }
}
