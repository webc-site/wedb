//! wlua：Luau C API 直连薄封装。
//!
//! 以 vendored 官方 Luau（luau0-src，cc 编译静态链接）为底座，提供：
//! - [`LuaState`]：栈操作/表/注册表引用/pcall/装载的 C API 语义层；
//! - [`ILuaAllocator`]：`lua_Alloc` 直挂的自定义分配器（内存配额）；
//! - VM safepoint 中断回调（脚本超时钩子）；
//! - 宿主函数注册（C 蹦床 + catch_unwind，panic 不跨 FFI）。
//!
//! 对标 garnet libs/server/Lua 的绑定层；RESP 值编解码留在会话域。

#![cfg_attr(docsrs, feature(doc_cfg))]
mod allocator;
mod api;
mod cache;
mod commands;
mod context;
mod error;
pub mod functions;
#[doc(hidden)]
pub mod functions_struct;
mod hash_key;
mod limited_allocator;
pub mod loader;
mod managed_allocator;
mod options;
mod runner;
mod state;
#[doc(hidden)]
pub mod strings;
mod sys;
mod timeout;
mod tracked_allocator;

pub use allocator::{ILuaAllocator, LuaAllocator};
pub use api::{ScriptApiError, ScriptingApi};
pub use cache::{LuaScriptHandle, RunnerCreateOptions, SessionScriptCache};
pub use commands::{LuaCommands, LuaSessionContext, StoreScriptCache};
#[doc(hidden)]
pub use context::{clear_callback_context, set_callback_context};
pub use error::{Error, Result};
pub use hash_key::{SHA1_HEX_LEN, ScriptHashKey};
pub use limited_allocator::LuaLimitedManagedAllocator;
pub use managed_allocator::LuaManagedAllocator;
pub use options::{LuaLoggingMode, LuaMemoryManagementMode, LuaOptions};
pub use runner::{LuaRunner, RespObject};
pub use state::{Deadline, LuaState};
pub use strings::ConstantStrings;
pub use sys::{LUA_ERRERR, LUA_ERRMEM, LUA_ERRRUN, LUA_ERRSYNTAX, LUA_OK, LUA_YIELD};
pub use timeout::{LuaTimeoutManager, TIMEOUT_TRIGGERED};
pub use tracked_allocator::LuaTrackedAllocator;
