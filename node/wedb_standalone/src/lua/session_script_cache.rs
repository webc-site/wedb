//! 会话脚本缓存：SHA1 摘要 → 已编译函数的会话级映射
//! （对标 libs/server/Lua/SessionScriptCache.cs:SessionScriptCache）。

use std::{cell::Cell, mem, sync::Arc};

use gxhash::{HashMap, HashSet};

use super::{lua_options::LuaLoggingMode, lua_runner::LuaRunner, script_hash_key::ScriptHashKey};

/// SHA1 十六进制长度（兼容关键常量，C# SessionScriptCache.SHA1Len）。
pub const SHA1_LEN: usize = 40;

/// 共享脚本句柄（对标 libs/server/Lua/LuaScriptHandle.cs:LuaScriptHandle）。
///
/// 跟踪全局共享脚本的生命周期；句柄销毁（Dispose）即向会话级缓存
/// 传播"该脚本需被丢弃"的信号。
pub struct LuaScriptHandle {
  /// 是否已被销毁。
  disposed: Cell<bool>,
  /// 关联脚本的源码（或预编译形态）。
  script_data: Vec<u8>,
}

impl LuaScriptHandle {
  /// libs/server/Lua/LuaScriptHandle.cs:LuaScriptHandle（构造）
  pub fn new(script_data: Vec<u8>) -> Arc<Self> {
    Arc::new(Self {
      disposed: Cell::new(false),
      script_data,
    })
  }

  /// libs/server/Lua/LuaScriptHandle.cs:IsDisposed
  ///
  /// 为 true 时，凡由该句柄支撑的缓存都应被丢弃。
  pub fn is_disposed(&self) -> bool {
    self.disposed.get()
  }

  /// libs/server/Lua/LuaScriptHandle.cs:ScriptData
  ///
  /// 关联脚本的源码。
  pub fn script_data(&self) -> &[u8] {
    &self.script_data
  }

  /// libs/server/Lua/LuaScriptHandle.cs:Dispose
  pub fn dispose(&self) {
    self.disposed.set(true);
  }
}

/// runner 构造选项（C# 自 StoreWrapper.serverOptions 逐字段下传的集合）。
#[derive(Clone, Default)]
pub struct RunnerCreateOptions {
  /// 内存限制（字节）。
  pub mem_limit_bytes: Option<usize>,
  /// redis.log 行为。
  pub log_mode: Option<LuaLoggingMode>,
  /// 允许导出函数集（空 = 默认集）。
  pub allowed_functions: Option<HashSet<String>>,
  /// 事务模式。
  pub txn_mode: bool,
  /// redis 版本号全局量。
  pub redis_version: String,
}

/// 会话脚本缓存条目（C# (LuaRunner, LuaScriptHandle) 元组）。
pub struct ScriptCacheEntry {
  /// 已编译脚本的执行器。
  pub runner: LuaRunner,
  /// 支撑该条目的共享句柄。
  pub handle: Arc<LuaScriptHandle>,
}

/// 会话脚本缓存。
#[derive(Default)]
pub struct SessionScriptCache {
  /// 已编译脚本：摘要 → (runner, 共享句柄)。
  script_cache: HashMap<ScriptHashKey, ScriptCacheEntry>,
  /// 脚本源码登记（EVAL 登记面：digest → 源码）。
  scripts: HashMap<ScriptHashKey, Vec<u8>>,
  /// 正在运行的脚本（引用计数语义：Start/Stop 配对）。
  running: HashMap<ScriptHashKey, u32>,
  /// 关联的用户句柄（ACL 场景）。
  user_handle: Option<u64>,
  /// 超时请求标记（脚本超时中断后置位）。
  timeout_requested: bool,
}

impl SessionScriptCache {
  /// libs/server/Lua/SessionScriptCache.cs:SetUserHandle
  pub fn set_user_handle(&mut self, user_handle: Option<u64>) {
    self.user_handle = user_handle;
  }

  /// 关联的用户句柄。
  pub fn user_handle(&self) -> Option<u64> {
    self.user_handle
  }

  /// libs/server/Lua/SessionScriptCache.cs:StartRunningScript
  pub fn start_running_script(&mut self, hash: &ScriptHashKey) {
    *self.running.entry(hash.clone()).or_insert(0) += 1;
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

  /// libs/server/Lua/SessionScriptCache.cs:RequestTimeout
  ///
  /// 标记超时请求（当前运行脚本应在下一检查点中断）。
  pub fn request_timeout(&mut self) {
    self.timeout_requested = true;
  }

  /// 消费超时请求标记。
  pub fn take_timeout_requested(&mut self) -> bool {
    mem::take(&mut self.timeout_requested)
  }

  /// 摘要取脚本源码（内部源码登记面）。
  pub fn try_get_from_digest(&self, hash: &ScriptHashKey) -> Option<&Vec<u8>> {
    self.scripts.get(hash)
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

  /// 登记脚本源码（内部登记面）。
  pub fn try_load(&mut self, hash: &ScriptHashKey, script: &[u8]) -> bool {
    self
      .scripts
      .entry(hash.clone())
      .or_insert_with(|| script.to_vec());
    true
  }

  /// libs/server/Lua/SessionScriptCache.cs:TryLoad
  ///
  /// 编译脚本并载入会话缓存；命中即复用。必要时返回新建的共享句柄供
  /// 调用方登记进全局缓存（`digest_on_heap` 对标形态）。
  /// 失败时错误以 RESP error 写入 `out` 并返回 None。
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
    let compiled_source = LuaRunner_LoaderProxy::compile_source(source);

    let mut runner = LuaRunner::new(
      options.log_mode.unwrap_or_default(),
      options.mem_limit_bytes,
      options.allowed_functions.clone().unwrap_or_default(),
      compiled_source.clone(),
      options.txn_mode,
      &options.redis_version,
    )
    .ok()?;

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
      digest.clone(),
      ScriptCacheEntry {
        runner,
        handle: Arc::clone(&handle),
      },
    );

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
    self.scripts.clear();
    self.running.clear();
  }

  /// libs/server/Lua/SessionScriptCache.cs:TrySwapDatabaseSessions
  ///
  /// SWAPDB 场景：会话缓存与数据库解耦，脚本集合保持不变。
  pub fn try_swap_database_sessions(&mut self, _old_db: i32, _new_db: i32) -> bool {
    true
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
    self.script_cache.len().max(self.scripts.len())
  }

  /// 是否为空。
  pub fn is_empty(&self) -> bool {
    self.script_cache.is_empty() && self.scripts.is_empty()
  }
}

/// CompileSource 代理（避免 loader ↔ runner 模块反向耦合）。
struct LuaRunner_LoaderProxy;

impl LuaRunner_LoaderProxy {
  fn compile_source(source: &[u8]) -> Vec<u8> {
    super::lua_runner__loader::LuaRunner_Loader::compile_source(source)
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::{LuaScriptHandle, RunnerCreateOptions, SessionScriptCache};
  use crate::lua::script_hash_key::ScriptHashKey;

  #[test]
  fn load_get_and_digest() {
    let mut cache = SessionScriptCache::default();
    let script = b"return 1";
    let hash = SessionScriptCache::get_script_digest(script);
    assert!(cache.try_load(&hash, script));
    assert_eq!(cache.try_get_from_digest(&hash).unwrap(), &script.to_vec());
    assert_eq!(cache.len(), 1);

    // 摘要确定性。
    let hash2 = SessionScriptCache::get_script_digest(script);
    assert!(hash.equals(&hash2));
  }

  #[test]
  fn running_lifecycle_and_timeout() {
    let mut cache = SessionScriptCache::default();
    let hash = SessionScriptCache::get_script_digest(b"return redis.call('PING')");
    assert!(cache.try_load(&hash, b"return redis.call('PING')"));
    cache.start_running_script(&hash);
    assert!(cache.is_running(&hash));
    cache.request_timeout();
    assert!(cache.take_timeout_requested());
    cache.stop_running_script(&hash);
    assert!(!cache.is_running(&hash));
    cache.set_user_handle(Some(42));
    assert_eq!(cache.user_handle(), Some(42));
    // swap 语义：脚本保留。
    assert!(cache.try_swap_database_sessions(0, 1));
    assert_eq!(cache.len(), 1);
  }

  #[test]
  fn try_load_runner_compiles_and_reuses() {
    let mut cache = SessionScriptCache::default();
    let source = b"return 'ok'";
    let digest = SessionScriptCache::get_script_digest(source);
    let mut out = Vec::new();
    let mut global = None;
    let options = RunnerCreateOptions::default();

    {
      let (runner, created) = cache
        .try_load_runner(source, &digest, &mut global, &options, &mut out)
        .expect("首次装载成功");
      assert!(created.is_some());
      assert!(runner.source().starts_with(b"return 'ok'"));
    }
    // 二次命中复用 runner；全局缺失时会话句柄上升返回（C# promote 形态）。
    let mut global = None;
    let mut out = Vec::new();
    let (_, created) = cache
      .try_load_runner(source, &digest, &mut global, &options, &mut out)
      .expect("二次装载命中");
    assert!(created.is_some(), "应上升会话句柄供全局缓存登记");
    assert!(Arc::ptr_eq(
      created.as_ref().unwrap(),
      global.as_ref().unwrap()
    ));
    assert_eq!(cache.len(), 1);

    // 全局句柄就位后再次命中 → 不再返回新句柄。
    let mut global = created;
    let mut out = Vec::new();
    let (_, created) = cache
      .try_load_runner(source, &digest, &mut global, &options, &mut out)
      .expect("三次装载命中");
    assert!(created.is_none());
    assert_eq!(cache.len(), 1);
  }

  #[test]
  fn try_load_runner_writes_error_on_bad_source() {
    let mut cache = SessionScriptCache::default();
    let source = b"return ]]";
    let digest = SessionScriptCache::get_script_digest(source);
    let mut out = Vec::new();
    let mut global = None;
    let options = RunnerCreateOptions::default();
    assert!(
      cache
        .try_load_runner(source, &digest, &mut global, &options, &mut out)
        .is_none()
    );
    assert!(out.starts_with(b"-"));
  }

  #[test]
  fn handle_dispose_invalidates_session_entry() {
    let mut cache = SessionScriptCache::default();
    let digest = SessionScriptCache::get_script_digest(b"return 7");
    let mut out = Vec::new();
    let mut global = None;
    let options = RunnerCreateOptions::default();
    let (_, created) = cache
      .try_load_runner(b"return 7", &digest, &mut global, &options, &mut out)
      .expect("装载成功");
    let handle = created.expect("新建句柄");
    assert!(!handle.is_disposed());
    handle.dispose();
    assert!(cache.try_get_runner(&digest).is_none());
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
}
