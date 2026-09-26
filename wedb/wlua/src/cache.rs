//! 会话脚本缓存：SHA1 摘要 → 已编译函数的会话级映射
//! （对标 libs/server/Lua/SessionScriptCache.cs:SessionScriptCache）。

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use wbase::map::{HashMap, HashSet};

use crate::{
  hash_key::ScriptHashKey,
  loader::LuaRunnerLoader,
  options::{LuaLoggingMode, LuaMemoryManagementMode},
  runner::LuaRunner,
  timeout::{LuaTimeoutManager, TimeoutRegistration},
};

/// 共享脚本句柄（对标 libs/server/Lua/LuaScriptHandle.cs:LuaScriptHandle）。
///
/// 跟踪全局共享脚本的生命周期；句柄销毁（Dispose）即向会话级缓存
/// 传播"该脚本需被丢弃"的信号。
pub struct LuaScriptHandle {
  /// 是否已被销毁。
  disposed: AtomicBool,
  /// 关联脚本的源码（或预编译形态）。
  script_data: Vec<u8>,
}

impl LuaScriptHandle {
  /// libs/server/Lua/LuaScriptHandle.cs:LuaScriptHandle（构造）
  pub fn new(script_data: Vec<u8>) -> Arc<Self> {
    Arc::new(Self {
      disposed: AtomicBool::new(false),
      script_data,
    })
  }

  /// libs/server/Lua/LuaScriptHandle.cs:IsDisposed
  ///
  /// 为 true 时，凡由该句柄支撑的缓存都应被丢弃。
  pub fn is_disposed(&self) -> bool {
    self.disposed.load(Ordering::Relaxed)
  }

  /// libs/server/Lua/LuaScriptHandle.cs:ScriptData
  ///
  /// 关联脚本的源码。
  pub fn script_data(&self) -> &[u8] {
    &self.script_data
  }

  /// libs/server/Lua/LuaScriptHandle.cs:Dispose
  pub fn dispose(&self) {
    self.disposed.store(true, Ordering::Relaxed);
  }
}

/// runner 构造选项（C# 自 StoreWrapper.serverOptions 逐字段下传的集合）。
#[derive(Clone, Default)]
pub struct RunnerCreateOptions {
  /// 内存管理模式。
  pub mem_mode: Option<LuaMemoryManagementMode>,
  /// 内存限制（字节）。
  pub mem_limit_bytes: Option<usize>,
  /// redis.log 行为。
  pub log_mode: Option<LuaLoggingMode>,
  /// 允许导出函数集（空 = 默认集）。
  pub allowed_functions: Option<HashSet<String>>,
  /// redis 版本号全局量。
  pub redis_version: String,
}

/// 会话脚本缓存条目（C# (LuaRunner, LuaScriptHandle) 元组）。
pub(crate) struct ScriptCacheEntry {
  /// 已编译脚本的执行器。
  pub runner: LuaRunner,
  /// 支撑该条目的共享句柄。
  pub handle: Arc<LuaScriptHandle>,
}

/// 会话超时装配（C# SessionScriptCache 的 timeoutManager +
/// timeoutRegistration 字段形态）。
struct SessionTimeout {
  /// 进程级超时管理器（服务装配期注入；专属看门狗线程驱动）。
  manager: Arc<LuaTimeoutManager>,
  /// 本会话登记（首个脚本装载成功时建立——C# TryLoad 尾部注册）。
  registration: Option<Arc<TimeoutRegistration>>,
}

/// 会话脚本缓存。
#[derive(Default)]
pub struct SessionScriptCache {
  /// 已编译脚本：摘要 → (runner, 共享句柄)。
  script_cache: HashMap<ScriptHashKey, ScriptCacheEntry>,
  /// 正在运行的脚本（引用计数语义：Start/Stop 配对）。
  running: HashMap<ScriptHashKey, u32>,
  /// 挂起中脚本（协程化挂起协议：redis.call 命中阻塞/慢路径时登记；
  /// 续跑口 [`Self::suspended_key`] 读，完成臂 [`Self::clear_suspended`] 清）。
  suspended: Option<ScriptHashKey>,
  /// 超时装配（None = 未启用；C# timeoutManager == null 形态）。
  timeout: Option<SessionTimeout>,
}

impl Drop for SessionScriptCache {
  /// libs/server/Lua/SessionScriptCache.cs:Dispose
  ///
  /// C# Dispose = Clear() + scratchBufferNetworkSender.Dispose() +
  /// processor.Dispose()；rust 会话缓存无内嵌 RespServerSession 与
  /// ScratchBuffer 发送器（脚本 redis.call 经 ScriptingApi 直达宿主会话），
  /// 注销超时登记（Clear 的 timeoutRegistration?.Dispose() 落点）是唯一
  /// 需显式收尾的面，其余字段随结构体 Drop 自然释放。
  fn drop(&mut self) {
    if let Some(t) = self.timeout.take()
      && let Some(registration) = &t.registration
    {
      t.manager.remove(registration);
    }
  }
}

impl SessionScriptCache {
  /// 装配超时管理器（C# 构造注入 timeoutManager；仅首次生效，重复注入
  /// 保留既有登记）。
  pub fn set_timeout_manager(&mut self, manager: Arc<LuaTimeoutManager>) {
    if self.timeout.is_none() {
      self.timeout = Some(SessionTimeout {
        manager,
        registration: None,
      });
    }
  }

  /// libs/server/Lua/SessionScriptCache.cs:StartRunningScript
  pub fn start_running_script(&mut self, hash: &ScriptHashKey) {
    *self.running.entry(*hash).or_insert(0) += 1;
  }

  /// libs/server/Lua/SessionScriptCache.cs:StopRunningScript
  pub fn stop_running_script(&mut self, hash: &ScriptHashKey) {
    if let Some(count) = self.running.get_mut(hash) {
      *count = count.saturating_sub(1);
      if *count == 0 {
        self.running.remove(hash);
      }
    }
  }

  /// 是否有脚本正在运行。
  pub fn is_running(&self, hash: &ScriptHashKey) -> bool {
    self.running.contains_key(hash)
  }

  /// 登记挂起中脚本（run 返回挂起态时由执行入口调用）。
  pub(crate) fn note_suspended(&mut self, hash: &ScriptHashKey) {
    self.suspended = Some(*hash);
  }

  /// 挂起中脚本的键（None = 无挂起；续跑入口 [`crate::LuaCommands::
  /// continue_execute_script`] 据此取回 runner）。
  pub(crate) fn suspended_key(&self) -> Option<ScriptHashKey> {
    self.suspended
  }

  /// 清除挂起登记（脚本完成/出错臂调用）。
  pub(crate) fn clear_suspended(&mut self) {
    self.suspended = None;
  }

  /// 当前超时登记句柄（run 装挂所需：登记项 + 服务级超时值）。
  ///
  /// C# StartRunningScript/StopRunningScript 的会话面取用形态：调用方
  /// （commands.rs try_execute_script）先取本句柄再借 runner，规避
  /// session_cache 与 runner 的双重可变借用。
  pub fn timeout_handle(&self) -> Option<(Arc<TimeoutRegistration>, i64)> {
    self.timeout.as_ref().and_then(|t| {
      t.registration
        .clone()
        .map(|registration| (registration, t.manager.timeout_millis()))
    })
  }

  /// libs/server/Lua/SessionScriptCache.cs:TryGetFromDigest
  ///
  /// 取摘要对应的 runner；全局句柄已销毁时按 C# 语义从会话缓存移除。
  pub fn try_get_runner(&mut self, digest: &ScriptHashKey) -> Option<&mut LuaRunner> {
    if self
      .script_cache
      .get(digest)
      .is_some_and(|entry| entry.handle.is_disposed())
    {
      // If the global cache has been invalidated, remove from the session cache
      self.script_cache.remove(digest);
      return None;
    }
    self
      .script_cache
      .get_mut(digest)
      .map(|entry| &mut entry.runner)
  }

  /// 执行期直取既有会话 runner（对标 C#
  /// libs/server/Lua/LuaCommands.cs 的 RunScriptForSession 直接持
  /// TryLoad/TryGetFromDigest 出参 runner 的传递形态）。
  ///
  /// 刻意不做 is_disposed 清退判定：句柄失效清退统一由检索期
  /// [`Self::try_get_runner`] 承接，保证败方句柄 dispose 后当次请求
  /// 仍以本地 runner 正常应答、仅下次检索清退（C# TryEVAL 时序契约）。
  pub fn get_runner_mut(&mut self, digest: &ScriptHashKey) -> Option<&mut LuaRunner> {
    self
      .script_cache
      .get_mut(digest)
      .map(|entry| &mut entry.runner)
  }

  /// libs/server/Lua/SessionScriptCache.cs:TryLoad
  ///
  /// 编译脚本并载入会话缓存；命中即复用。必要时返回新建的共享句柄供
  /// 调用方登记进全局缓存（`digest_on_heap` 对标形态）。
  /// 失败时错误以 RESP error 写入 `out` 并返回 None。
  /// 构造失败仅日志留痕，out 不写错误帧（对位 C# catch 臂形态）。
  pub fn try_load_runner(
    &mut self,
    source: &[u8],
    digest: &ScriptHashKey,
    global_handle: &mut Option<Arc<LuaScriptHandle>>,
    options: &RunnerCreateOptions,
    out: &mut Vec<u8>,
  ) -> Option<(&mut LuaRunner, Option<Arc<LuaScriptHandle>>)> {
    // 会话缓存命中（句柄失效的由移除分支承接）：
    // C# TryGetFromDigest 命中即 ref 句柄置为既有会话句柄；调用方
    // （TryEVAL 的 sessionScriptHandle != globalScriptHandle 分支）在全局
    // 缺失该句柄时上升登记——以 created 的 Some 承接该上升形态。
    if let Some(entry) = self.script_cache.get_mut(digest) {
      if !entry.handle.is_disposed() {
        let promoted = Arc::clone(&entry.handle);
        let created = global_handle.take().map_or_else(
          || Some(promoted),
          |existing| {
            *global_handle = Some(existing);
            None
          },
        );
        *global_handle = Some(Arc::clone(&entry.handle));
        return Some((&mut entry.runner, created));
      }
      self.script_cache.remove(digest);
    }

    // CompileSource：luau 无 string.dump，编译在装载时进行，源码直存。
    let compiled_source = LuaRunnerLoader::compile_source(source);

    // runner 构造失败（VM/空间/模式矛盾类 Err）：对位 C# TryLoad 的
    // catch (Exception ex) 臂（SessionScriptCache.cs:227 LogError），
    // 错误文案只进日志不写应答面，返 None 维持上游「当次无应答」形态。
    let mut runner = match LuaRunner::new(compiled_source.clone(), options) {
      Ok(runner) => runner,
      Err(err) => {
        log::error!("During Lua script loading, an unexpected exception: {err}");
        return None;
      }
    };

    // If compilation fails, an error is written out
    if !runner.compile_for_session(out) {
      return None;
    }

    // C# luaScriptHandle ??= new(compiledSource)：ref 句柄缺失时新建并回填。
    // 调用方仅在句柄与全局既有句柄不同时登记（TryEVAL != 分支）：
    // 传入既有句柄 → 无需登记；新建 → 上升登记。
    let (handle, created) = match global_handle.take() {
      Some(existing) => {
        *global_handle = Some(Arc::clone(&existing));
        (existing, None)
      }
      None => {
        let fresh = LuaScriptHandle::new(compiled_source);
        *global_handle = Some(Arc::clone(&fresh));
        (Arc::clone(&fresh), Some(fresh))
      }
    };
    self.script_cache.insert(
      *digest,
      ScriptCacheEntry {
        runner,
        handle: Arc::clone(&handle),
      },
    );

    // On first script load, register for timeout notifications
    //
    // We don't do this for every session because not every session will run scripts
    // （C# TryLoad 尾部：timeoutManager != null && timeoutRegistration == null）
    if let Some(t) = &mut self.timeout
      && t.registration.is_none()
    {
      t.registration = Some(t.manager.register());
    }

    let entry = self.script_cache.get_mut(digest)?;
    Some((&mut entry.runner, created))
  }

  /// libs/server/Lua/SessionScriptCache.cs:Remove
  ///
  /// 从会话缓存移除脚本（不销毁句柄：不影响全局缓存）。
  pub fn remove_runner(&mut self, key: &ScriptHashKey) {
    self.script_cache.remove(key);
  }

  /// libs/server/Lua/SessionScriptCache.cs:Clear
  pub fn clear(&mut self) {
    // Intentionally NOT disposing the script handles (global cache semantics)
    self.script_cache.clear();
    self.running.clear();
    // C# Clear：注销超时登记（timeoutRegistration?.Dispose() + 置空），
    // 下次脚本装载时重新注册。
    if let Some(t) = &mut self.timeout
      && let Some(registration) = t.registration.take()
    {
      t.manager.remove(&registration);
    }
  }

  /// libs/server/Lua/SessionScriptCache.cs:GetScriptDigest
  ///
  /// 计算脚本 SHA1 摘要键。
  pub fn get_script_digest(script: &[u8]) -> ScriptHashKey {
    use sha1_smol::Sha1;
    let mut hasher = Sha1::new();
    hasher.update(script);
    let digest = hasher.digest().bytes();

    ScriptHashKey::new(&digest)
  }

  /// 已缓存脚本数（SCRIPT EXISTS 路径）。
  pub fn len(&self) -> usize {
    self.script_cache.len()
  }

  /// 是否为空。
  pub fn is_empty(&self) -> bool {
    self.script_cache.is_empty()
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{Arc, atomic::Ordering};

  use wbase::time::now_ms_i64;

  use super::{LuaScriptHandle, SessionScriptCache};
  use crate::{
    hash_key::ScriptHashKey,
    options::{LuaLoggingMode, LuaMemoryManagementMode},
    runner,
    timeout::LuaTimeoutManager,
  };

  fn runner_options() -> crate::RunnerCreateOptions {
    crate::RunnerCreateOptions {
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
        .try_load_runner(script, &hash, &mut handle, &options, &mut out)
        .is_some()
    );
    assert_eq!(cache.len(), 1);
    assert!(cache.try_get_runner(&hash).is_some());

    // 摘要确定性。
    let hash2 = SessionScriptCache::get_script_digest(script);
    assert!(hash.equals(&hash2));
  }

  #[test]
  fn running_lifecycle_and_timeout_assembly() {
    let mut cache = SessionScriptCache::default();
    let hash = SessionScriptCache::get_script_digest(b"return redis.call('PING')");
    cache.start_running_script(&hash);
    assert!(cache.is_running(&hash));
    cache.stop_running_script(&hash);
    assert!(!cache.is_running(&hash));
    assert!(cache.is_empty());

    // 超时装配链：manager 注入 → 首装载注册 → arm/disarm 截止可见。
    let manager = Arc::new(LuaTimeoutManager::new(300));
    let mut cache = SessionScriptCache::default();
    cache.set_timeout_manager(Arc::clone(&manager));
    assert_eq!(manager.active_count(), 0);

    let options = runner_options();
    let mut handle = None;
    let mut out = Vec::new();
    let digest = SessionScriptCache::get_script_digest(b"return 1");
    let Some((runner, _)) =
      cache.try_load_runner(b"return 1", &digest, &mut handle, &options, &mut out)
    else {
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
        .try_load_runner(b"return 2", &digest, &mut handle, &options, &mut out)
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
    // try_load_runner 仅日志留痕后返 None——out 零字节、缓存与句柄零变更
    // （对位 C# catch 臂：LogError 后返 false，dcurr 无写）。
    let options = crate::RunnerCreateOptions {
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
        .try_load_runner(b"return 1", &hash, &mut handle, &options, &mut out)
        .is_none()
    );
    assert!(out.is_empty(), "构造失败不得写应答帧: {out:?}");
    assert_eq!(cache.len(), 0);
    assert!(handle.is_none());
  }

  #[test]
  fn runner_new_native_limit_error_message() {
    // 钉 runner/mod.rs 矛盾臂文案（C# 参考树无此句，rust 侧自设口径）。
    let options = crate::RunnerCreateOptions {
      mem_mode: Some(LuaMemoryManagementMode::Native),
      mem_limit_bytes: Some(1_048_576),
      ..Default::default()
    };
    // LuaRunner 无 Debug，unwrap_err 不可用，以 let-else 取 Err。
    let Err(err) = runner::LuaRunner::new(b"return 1".to_vec(), &options) else {
      panic!("Native + 限额矛盾应构造失败");
    };
    assert!(
      err.to_string().contains("native memory management"),
      "构造失败文案漂移: {err}"
    );
  }
}
