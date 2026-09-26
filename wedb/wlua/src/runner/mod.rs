//! 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:LuaRunner
//!
//! Lua 脚本执行器。C# 以 KeraLua C 指针 + UnmanagedCallersOnly trampoline
//! 承接宿主回调；Rust 以 wlua 的 C 蹦床（catch_unwind + thread-local
//! 上下文）承接同语义。会话上下文经 thread-local [`HostShared`] 指针传递
//! （对标 LuaRunnerTrampolines.CallbackContext）。
//!
//! C# LuaRunner 为 partial 类，跨 LuaRunner.Functions.cs / LuaRunner.Loader.cs /
//! LuaRunner.Strings.cs 分文件；Rust 侧对应 runner（本域）、functions、loader、strings。
//!
//! C# TryProbeSupport 动态库探测
//! （在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:TryProbeSupport）
//! 在 Rust 侧无对应形态：luau 经静态链接编译期内建，不存在运行时库探测失败路径。
//!
//! 域划分：本模块承接执行器构造与核心入口；宿主上下文与回调守卫见
//! `host`，RESP 输出通道与响应转 Lua 值见 `resp_convert`，
//! 编译与执行见 `executor`。

use std::{error::Error, fmt, ptr};

use wbase::map::HashSet;

use crate::{
  LuaState,
  cache::RunnerCreateOptions,
  functions::LuaRunnerFunctions,
  limited_allocator::LuaLimitedManagedAllocator,
  loader::LuaRunnerLoader,
  managed_allocator::LuaManagedAllocator,
  options::{LuaMemoryManagementMode, LuaOptions},
  sys,
  tracked_allocator::LuaTrackedAllocator,
};

/// 调用脚本出现意外响应形态时的错误文案（本域两处复用）。
pub(super) const ERR_UNEXPECTED_RESPONSE: &[u8] = b"ERR Unexpected error response";
/// 脚本执行内部异常的兜底错误文案（本域两处复用）。
pub(super) const ERR_LUA_INVOKE_FAILED: &[u8] =
  b"ERR An error occurred while invoking a Lua script";

/// 脚本运行错误的 RESP 前缀。
pub(super) const ERR_PREFIX: &[u8] = b"ERR ";

/// 沙箱初始 KEYS/ARGV 数组容量（C# InitialKeysCapacity/InitialArgvCapacity）。
const INITIAL_KEYS_CAPACITY: usize = 5;
const INITIAL_ARGV_CAPACITY: usize = 5;

/// Lua 脚本执行器。
///
/// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:LuaRunner
pub struct LuaRunner {
  /// 注册表引用：sandbox_env。
  sandbox_env_registry_index: i32,
  /// 注册表引用：load_sandboxed。
  load_sandboxed_registry_index: i32,
  /// 注册表引用：reset_keys_and_argv。
  reset_keys_and_argv_registry_index: i32,

  /// 允许导出函数集。
  allowed_functions: HashSet<String>,
  /// 脚本源码。
  source: Vec<u8>,

  /// 宿主共享态（堆定址，回调经 thread-local 指针访问）。
  host: Box<HostShared>,

  /// KEYS 数组容量。
  keys_arr_capacity: usize,
  /// ARGV 数组容量。
  argv_arr_capacity: usize,
  /// 当前 KEYS 长度。
  key_length: usize,
  /// 当前 ARGV 长度。
  argv_length: usize,

  /// VM。
  state: LuaState,

  /// 运行期协程线程（lua_newthread 产物；仅 run 窗口非空——线程值以主栈
  /// 栈位锚定，挂起窗口跨同步首段/续段存活，finish_run 清栈收锚归空）。
  script_thread: *mut sys::lua_State,

  /// 续段 resume 的参数个数（挂起应答转换值个数，push 后由
  /// [`LuaRunner::continue_session`] 消费）。
  pending_resume_args: usize,
}

// SAFETY: LuaRunner 独占持有底层的 Luau 虚拟机及其所有环境，不跨线程并发共享，满足跨线程转移所有权的 Send 要求。
unsafe impl Send for LuaRunner {}

/// 构造失败（对标 GarnetException 文案）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LuaRunnerInitError(pub String);

impl fmt::Display for LuaRunnerInitError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(&self.0)
  }
}

impl Error for LuaRunnerInitError {}

mod executor;
mod host;
mod resp_convert;
// HostShared 经 `functions` 门面的宿主回调签名可达，保留 pub 声明；
// 其余宿主内部符号仅 crate 内可见。
pub use host::HostShared;
pub(crate) use host::{HostFn, ScriptSessionPtr};
pub use resp_convert::{RespObject, RespOut};
pub(crate) use resp_convert::{lua_wrapped_error_view, process_resp_response_view};

impl LuaRunner {
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:LuaRunner（构造）
  ///
  /// 建 KEYS/ARGV 全局表、注册宿主函数族、灌入 redis 版本全局量，
  /// 执行 loader block 并引用沙箱关键对象。
  ///
  /// 散参入 [`crate::RunnerCreateOptions`] 聚合承接。
  pub fn new(source: Vec<u8>, options: &RunnerCreateOptions) -> Result<Self, LuaRunnerInitError> {
    let log_mode = options.log_mode.unwrap_or_default();
    let mem_limit_bytes = options.mem_limit_bytes;
    let mem_mode = options.mem_mode.unwrap_or(LuaMemoryManagementMode::Native);
    let allowed_functions = options.allowed_functions.clone().unwrap_or_default();
    let redis_version = &options.redis_version;
    // 对标 libs/server/Lua/LuaStateWrapper.cs:50-70 的 4 种内存模式分支
    let mut state = match mem_mode {
      LuaMemoryManagementMode::Native => {
        if mem_limit_bytes.is_some() {
          return Err(LuaRunnerInitError(
            "Memory limit cannot be set when using native memory management".into(),
          ));
        }
        LuaState::new()
      }
      LuaMemoryManagementMode::Tracked => {
        LuaState::with_allocator(LuaTrackedAllocator::new(mem_limit_bytes.unwrap_or(0)))
      }
      LuaMemoryManagementMode::Managed => match mem_limit_bytes {
        Some(limit) => LuaState::with_allocator(LuaLimitedManagedAllocator::new(limit)),
        None => LuaState::with_allocator(LuaManagedAllocator::default()),
      },
    };

    // KEYS / ARGV 全局表（显式容量，后续按需重建）。
    state.create_table(INITIAL_KEYS_CAPACITY, 0);
    if !state.set_global(b"KEYS") {
      return Err(LuaRunnerInitError(
        "Insufficient space in Lua VM for KEYS".into(),
      ));
    }
    state.create_table(INITIAL_ARGV_CAPACITY, 0);
    if !state.set_global(b"ARGV") {
      return Err(LuaRunnerInitError(
        "Insufficient space in Lua VM for ARGV".into(),
      ));
    }

    // Lua 5.1 兼容 + 运行时库 + redis 接口（宿主实现）。
    Self::register(&mut state, b"garnet_atan2", LuaRunnerFunctions::atan2)?;
    Self::register(&mut state, b"garnet_cosh", LuaRunnerFunctions::cosh)?;
    Self::register(&mut state, b"garnet_frexp", LuaRunnerFunctions::frexp)?;
    Self::register(&mut state, b"garnet_ldexp", LuaRunnerFunctions::ldexp)?;
    Self::register(&mut state, b"garnet_log10", LuaRunnerFunctions::log10)?;
    Self::register(&mut state, b"garnet_pow", LuaRunnerFunctions::pow)?;
    Self::register(&mut state, b"garnet_sinh", LuaRunnerFunctions::sinh)?;
    Self::register(&mut state, b"garnet_tanh", LuaRunnerFunctions::tanh)?;
    Self::register(&mut state, b"garnet_maxn", LuaRunnerFunctions::maxn)?;
    Self::register(
      &mut state,
      b"garnet_loadstring",
      LuaRunnerFunctions::load_string,
    )?;

    Self::register(
      &mut state,
      b"garnet_cjson_encode",
      LuaRunnerFunctions::c_json_encode,
    )?;
    Self::register(
      &mut state,
      b"garnet_cjson_decode",
      LuaRunnerFunctions::c_json_decode,
    )?;
    Self::register(
      &mut state,
      b"garnet_bit_tobit",
      LuaRunnerFunctions::bit_to_bit,
    )?;
    Self::register(
      &mut state,
      b"garnet_bit_tohex",
      LuaRunnerFunctions::bit_to_hex,
    )?;
    // garnet_bitop implements bnot, bor, band, xor, etc. but isn't directly exposed
    Self::register(&mut state, b"garnet_bitop", LuaRunnerFunctions::bitop)?;
    Self::register(
      &mut state,
      b"garnet_bit_bswap",
      LuaRunnerFunctions::bit_bswap,
    )?;
    Self::register(
      &mut state,
      b"garnet_cmsgpack_pack",
      LuaRunnerFunctions::c_msg_pack_pack,
    )?;
    Self::register(
      &mut state,
      b"garnet_cmsgpack_unpack",
      LuaRunnerFunctions::c_msg_pack_unpack,
    )?;
    Self::register(
      &mut state,
      b"garnet_struct_pack",
      LuaRunnerFunctions::struct_pack,
    )?;
    Self::register(
      &mut state,
      b"garnet_struct_unpack",
      LuaRunnerFunctions::struct_unpack,
    )?;
    Self::register(
      &mut state,
      b"garnet_struct_size",
      LuaRunnerFunctions::struct_size,
    )?;
    Self::register(&mut state, b"garnet_call", LuaRunnerFunctions::garnet_call)?;
    Self::register(&mut state, b"garnet_sha1hex", LuaRunnerFunctions::sha1_hex)?;
    Self::register(&mut state, b"garnet_log", LuaRunnerFunctions::log)?;
    Self::register(
      &mut state,
      b"garnet_acl_check_cmd",
      LuaRunnerFunctions::acl_check_command,
    )?;
    Self::register(&mut state, b"garnet_setresp", LuaRunnerFunctions::set_resp)?;
    Self::register(
      &mut state,
      b"garnet_unpack_trampoline",
      LuaRunnerFunctions::unpack_trampoline,
    )?;
    Self::register(&mut state, b"garnet_load", LuaRunnerFunctions::load_chunk)?;

    let mut runner = Self {
      sandbox_env_registry_index: -1,
      load_sandboxed_registry_index: -1,
      reset_keys_and_argv_registry_index: -1,
      allowed_functions,
      source,
      host: Box::new(HostShared::new(log_mode)),
      keys_arr_capacity: INITIAL_KEYS_CAPACITY,
      argv_arr_capacity: INITIAL_ARGV_CAPACITY,
      key_length: 0,
      argv_length: 0,
      state,
      script_thread: ptr::null_mut(),
      pending_resume_args: 0,
    };

    // redis 版本全局量。
    runner.state.push_buffer(redis_version.as_bytes());
    if !runner.state.set_global(b"garnet_REDIS_VERSION") {
      return Err(LuaRunnerInitError(
        "Insufficient space in Lua VM for redis version global".into(),
      ));
    }

    let redis_version_num = Self::redis_version_num(redis_version);
    runner.state.push_integer(redis_version_num);
    if !runner.state.set_global(b"garnet_REDIS_VERSION_NUM") {
      return Err(LuaRunnerInitError(
        "Insufficient space in Lua VM for redis version number global".into(),
      ));
    }

    // loader block：构建沙箱环境与关键函数。
    let loader_block = LuaRunnerLoader::prepare_loader_block_bytes(&runner.allowed_functions);
    if runner
      .state
      .load_buffer(loader_block.as_bytes(), "@loader_block")
      .is_err()
    {
      return Err(LuaRunnerInitError("Could not initialize Lua VM".into()));
    }
    if runner.state.pcall_n(0, usize::MAX).is_err() {
      let err_msg = runner.state.known_string_to_buffer(-1).map_or_else(
        || "No error provided".to_string(),
        |buff| String::from_utf8_lossy(&buff).into_owned(),
      );
      return Err(LuaRunnerInitError(format!(
        "Could not initialize Lua sandbox state: {err_msg}"
      )));
    }

    // 引用沙箱关键对象。
    runner.sandbox_env_registry_index = runner.ref_global(
      b"sandbox_env",
      "Insufficient space in VM for sandbox_env ref",
    )?;
    runner.load_sandboxed_registry_index = runner.ref_global(
      b"load_sandboxed",
      "Insufficient space in VM for load_sandboxed ref",
    )?;
    runner.reset_keys_and_argv_registry_index = runner.ref_global(
      b"reset_keys_and_argv",
      "Insufficient space in VM for reset_keys_and_argv ref",
    )?;

    debug_assert!(runner.state.expect_lua_stack_empty());
    Ok(runner)
  }

  /// 取全局对象入栈并写入注册表引用（引用失败即沙箱初始化失败）
  fn ref_global(&mut self, name: &[u8], err: &str) -> Result<i32, LuaRunnerInitError> {
    if !self.state.get_global(name) {
      return Err(LuaRunnerInitError(err.into()));
    }
    let index = self.state.try_ref();
    if index <= 0 {
      return Err(LuaRunnerInitError(err.into()));
    }
    Ok(index)
  }

  /// Options 构造重载 (LuaRunner(options))
  pub fn with_options(
    options: &LuaOptions,
    source: &[u8],
    redis_version: &str,
  ) -> Result<Self, LuaRunnerInitError> {
    Self::new(
      source.to_vec(),
      &RunnerCreateOptions {
        mem_mode: Some(options.memory_mode),
        mem_limit_bytes: options.get_memory_limit_bytes(),
        log_mode: Some(options.log_mode),
        allowed_functions: Some(options.allowed_functions.iter().cloned().collect()),
        redis_version: redis_version.to_string(),
      },
    )
  }

  /// C# `Version.Parse` 的 major<<16|minor<<8|build 折算。
  fn redis_version_num(redis_version: &str) -> i64 {
    let mut parts = redis_version
      .split('.')
      .map(|p| p.parse::<i64>().unwrap_or(0).clamp(0, u8::MAX as i64));
    let major = parts.next().unwrap_or(0);
    let minor = parts.next().unwrap_or(0);
    let build = parts.next().unwrap_or(0);
    (major << 16) | (minor << 8) | build
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:Register（构造内局部函数）
  ///
  /// 注册宿主函数为全局；闭包即蹦床：参数压栈镜像 → 执行 → 取回结果。
  /// 末尾清空镜像栈，对标 Lua C 调用帧丢弃语义（仅保留返回值）。
  fn register(
    state: &mut LuaState,
    name: &[u8],
    function: HostFn,
  ) -> Result<(), LuaRunnerInitError> {
    let registration = state.register_host_fn::<HostShared>(name, function);
    if !registration {
      return Err(LuaRunnerInitError(format!(
        "Insufficient space in VM for {} global",
        String::from_utf8_lossy(name)
      )));
    }
    Ok(())
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:NeedsDispose
  ///
  /// 运行中遭遇致命损伤或 VM 分配器在 infallible 区域发生应急逃逸时为 true，应在最近时机重建 runner。
  pub fn needs_dispose(&self) -> bool {
    self.host.needs_dispose || self.state.needs_dispose()
  }

  /// 脚本源码。
  pub fn source(&self) -> &[u8] {
    &self.source
  }

  /// 在 garnet 中的相对路径:libs/server/Lua/LuaStateWrapper.cs:StackTop
  ///
  /// VM 当前栈高观测口（run 收尾守卫保证执行退出后恒为 0）。
  pub fn stack_top(&self) -> usize {
    self.state.get_top()
  }
}
