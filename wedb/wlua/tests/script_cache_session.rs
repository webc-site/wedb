//! 会话脚本缓存集成测（r7 自 `wlua/src/cache.rs` 内联测模块迁入，
//! 依赖面全 pub：SessionScriptCache / LuaScriptHandle / RunnerCreateOptions /
//! LuaTimeoutManager / TimeoutRegistration / ScriptHashKey / LuaRunner）。

use std::sync::{Arc, atomic::Ordering};

use wbase::time::now_ms_i64;
use wlua::{
  LuaLoggingMode, LuaMemoryManagementMode, LuaRunner, LuaScriptHandle, LuaTimeoutManager,
  RunnerCreateOptions, SHA1_HEX_LEN, ScriptHashKey, SessionScriptCache,
};

fn runner_options() -> RunnerCreateOptions {
  RunnerCreateOptions {
    mem_mode: Some(LuaMemoryManagementMode::Native),
    mem_limit_bytes: None,
    log_mode: Some(LuaLoggingMode::Silent),
    allowed_functions: None,
    redis_version: "0.0.0".to_string(),
  }
}

#[test]
fn load_get_and_digest() {
  let mut cache = SessionScriptCache::default();
  let script = b"return 1";
  let hash = SessionScriptCache::get_script_digest(script);
  let options = runner_options();
  let mut handle = None;
  let mut out = Vec::new();
  assert!(
    cache
      .try_get_or_create_runner_from_source(script, &hash, &mut handle, &options, &mut out)
      .is_some()
  );
  assert!(
    cache.try_get_runner(&hash).is_some(),
    "装载后应命中会话缓存"
  );

  // 摘要确定性。
  let hash2 = SessionScriptCache::get_script_digest(script);
  assert!(hash.equals(&hash2));
}

#[test]
fn timeout_assembly_and_unregister_on_clear() {
  let mut cache = SessionScriptCache::default();
  let hash = SessionScriptCache::get_script_digest(b"return redis.call('PING')");
  assert!(cache.try_get_runner(&hash).is_none(), "未装载脚本不得命中");

  // 超时装配链：manager 注入 → 首装载注册 → arm/disarm 截止可见。
  let manager = Arc::new(LuaTimeoutManager::new(300));
  let mut cache = SessionScriptCache::default();
  cache.set_timeout_manager(Arc::clone(&manager));
  assert_eq!(manager.active_count(), 0);

  let options = runner_options();
  let mut handle = None;
  let mut out = Vec::new();
  let digest = SessionScriptCache::get_script_digest(b"return 1");
  let Some((runner, _)) = cache.try_get_or_create_runner_from_source(
    b"return 1",
    &digest,
    &mut handle,
    &options,
    &mut out,
  ) else {
    panic!("装载失败: {out:?}");
  };
  let _ = runner;
  // 首装载成功即注册（C# TryLoad 尾部）。
  assert_eq!(manager.active_count(), 1);

  // arm：截止 ≈ now + timeout（单调时钟，界检查）。
  let arm_at = now_ms_i64();
  let Some((registration, timeout_millis)) = cache.timeout_handle() else {
    panic!("超时登记句柄应存在");
  };
  assert_eq!(timeout_millis, 300);
  let Some(runner) = cache.try_get_runner(&digest) else {
    panic!("runner 应命中");
  };
  runner.hook_shared_deadline(registration.shared_deadline());
  registration.arm(arm_at, timeout_millis);
  // 截止经登记项可见（原 manager.active_deadlines 枚举面是测试脚手架，已删）：
  // arm 写的就是换挂给 runner 的那个共享槽。
  assert_eq!(registration.deadline(), arm_at + 300);
  assert_eq!(
    registration.shared_deadline().load(Ordering::Acquire),
    arm_at + 300
  );

  // disarm：截止清 0。
  registration.disarm();
  assert_eq!(registration.deadline(), 0);
  assert_eq!(registration.shared_deadline().load(Ordering::Acquire), 0);

  // Clear：注销登记。
  cache.clear();
  assert_eq!(manager.active_count(), 0);

  // Drop：再次装载注册后，drop 注销。
  let mut out = Vec::new();
  let mut handle = None;
  let digest = SessionScriptCache::get_script_digest(b"return 2");
  assert!(
    cache
      .try_get_or_create_runner_from_source(b"return 2", &digest, &mut handle, &options, &mut out)
      .is_some()
  );
  assert_eq!(manager.active_count(), 1);
  drop(cache);
  assert_eq!(manager.active_count(), 0);
}

#[test]
fn script_hex_key_roundtrip() {
  let digest = SessionScriptCache::get_script_digest(b"return 7");
  let from_hex = ScriptHashKey::from_hex(digest.as_str().as_bytes()).unwrap();
  assert!(digest.equals(&from_hex));
  assert!(ScriptHashKey::from_hex(b"zz").is_none());
}

#[test]
fn lua_script_handle_lifecycle() {
  let handle = LuaScriptHandle::new(b"return 1".to_vec());
  assert_eq!(handle.script_data(), b"return 1");
  assert!(!handle.is_disposed());
  handle.dispose();
  assert!(handle.is_disposed());
}

#[test]
fn load_runner_init_failure_writes_no_frame() {
  // Native + 限额矛盾直构（绕开 LuaOptions 归一路径）：new 返 Err，
  // try_get_or_create_runner_from_source 仅日志留痕后返 None——out 零字节、缓存与句柄零变更
  // （对位 C# catch 臂：LogError 后返 false，dcurr 无写）。
  let options = RunnerCreateOptions {
    mem_mode: Some(LuaMemoryManagementMode::Native),
    mem_limit_bytes: Some(1_048_576),
    ..Default::default()
  };
  let mut cache = SessionScriptCache::default();
  let hash = SessionScriptCache::get_script_digest(b"return 1");
  let mut handle = None;
  let mut out = Vec::new();
  assert!(
    cache
      .try_get_or_create_runner_from_source(b"return 1", &hash, &mut handle, &options, &mut out)
      .is_none()
  );
  assert!(out.is_empty(), "构造失败不得写应答帧: {out:?}");
  assert!(cache.try_get_runner(&hash).is_none(), "缓存零变更");
  assert!(handle.is_none());
}

#[test]
fn runner_new_native_limit_error_message() {
  // 钉 runner/mod.rs 矛盾臂文案（C# 参考树无此句，rust 侧自设口径）。
  let options = RunnerCreateOptions {
    mem_mode: Some(LuaMemoryManagementMode::Native),
    mem_limit_bytes: Some(1_048_576),
    ..Default::default()
  };
  // LuaRunner 无 Debug，unwrap_err 不可用，以 let-else 取 Err。
  let Err(err) = LuaRunner::new(b"return 1".to_vec(), &options) else {
    panic!("Native + 限额矛盾应构造失败");
  };
  assert!(
    err.to_string().contains("native memory management"),
    "构造失败文案漂移: {err}"
  );
}

#[test]
fn digest_to_hex() {
  let digest = [0xabu8; 20];
  let key = ScriptHashKey::new(&digest);
  assert_eq!(key.as_str(), &"ab".repeat(20));
  assert_eq!(key.as_str().len(), SHA1_HEX_LEN);

  // sha1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709
  assert_eq!(
    SessionScriptCache::get_script_digest(b"").as_str(),
    "da39a3ee5e6b4b0d3255bfef95601890afd80709"
  );
}

#[test]
fn equals_matches_content() {
  let a = ScriptHashKey::new(&[1u8; 20]);
  let b = ScriptHashKey::new(&[1u8; 20]);
  let c = ScriptHashKey::new(&[2u8; 20]);
  assert!(a.equals(&b));
  assert!(!a.equals(&c));
}
