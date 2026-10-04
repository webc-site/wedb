//! 会话脚本缓存：SHA1 摘要 → 已编译函数的会话级映射
//! （对标 libs/server/Lua/SessionScriptCache.cs:SessionScriptCache）。

use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};

use wbase::map::{HashMap, HashSet};

use crate::{
  commands::LuaCommands,
  hash_key::ScriptHashKey,
  loader::LuaRunnerLoader,
  options::{LuaLoggingMode, LuaMemoryManagementMode, LuaOptions},
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
#[derive(Default)]
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

impl RunnerCreateOptions {
  /// [`LuaOptions`] 五字段下传的单点拼装：EVAL 装配链路（
  /// [`crate::commands::LuaSessionContext`]）与 [`crate::LuaRunner::with_options`]
  /// 共用同一构造点，杜绝双处拼装漂移。
  pub fn from_lua_options(options: &LuaOptions, redis_version: &str) -> Self {
    Self {
      mem_mode: Some(options.memory_mode),
      mem_limit_bytes: options.get_memory_limit_bytes(),
      log_mode: Some(options.log_mode),
      allowed_functions: if options.allowed_functions.is_empty() {
        None
      } else {
        Some(options.allowed_functions.iter().cloned().collect())
      },
      redis_version: redis_version.to_string(),
    }
  }
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

  /// libs/server/Lua/SessionScriptCache.cs:TryGetOrCreateRunnerFromSource
  ///
  /// 编译源码文本并载入会话缓存；命中即复用。必要时返回新建的共享句柄供
  /// 调用方登记进全局缓存（`digest_on_heap` 对标形态）。
  /// 失败时错误以 RESP error 写入 `out` 并返回 None。
  /// 构造失败仅日志留痕，out 不写错误帧（对位 C# catch 臂形态）。
  ///
  /// C# #2138 另拆 FromCachedScript / FromGeneratedBytecode 两口承接字节码
  /// 缓存生命周期；rust 无字节码缓存——全局句柄存源码文本，EVALSHA 命中
  /// 全局缓存路径同样经本口装载，「只信内部产物」语义由双层门承接：
  /// 入口前置门（[`LuaRunnerLoader::try_compile_source`]）+ 装载门
  /// （`LuaState::load_buffer` 的 text-only 检查），外部字节码无任何入口。
  pub fn try_get_or_create_runner_from_source(
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

    // C# TryCompileSourceAndCreateRunner：编译前置门——外部字节码 / NUL
    // 在构造 VM 前即拒（免为垃圾输入白分配 VM），错误经编译错误单点
    // （LuaCommands::write_lua_compilation_error）以 RESP error 写出。
    let compiled_source = match LuaRunnerLoader::try_compile_source(source) {
      Ok(source) => source,
      Err(error) => {
        LuaCommands::write_lua_compilation_error(out, error);
        return None;
      }
    };

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
}
