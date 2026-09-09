//! Lua C API P/Invoke 绑定面（对标 libs/server/Lua/NativeMethods.cs:NativeMethods）。
//!
//! C# 侧为指向 Lua 原生库的 P/Invoke 声明（lua_tolstring/lua_pushlstring/
//! lua_pcallk 等原始栈操作）。Rust 侧改用 mlua（luau feature）：编译、调用、
//! 表操作以高级 API 承接（见 `LuaStateWrapper`），不存在逐函数的 C 栈映射，
//! 故本文件保留映射注释、方法体为占位零值，并已登记 check/ignore。

use super::lua_state_wrapper::LuaStateWrapper;

pub struct NativeMethods;

impl NativeMethods {
  /// libs/server/Lua/NativeMethods.cs:lua_tolstring
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_tolstring(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_pushlstring
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_pushlstring(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:luaL_loadbufferx
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn luaL_loadbufferx(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:luaL_loadstring
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn luaL_loadstring(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:luaL_newstate
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn luaL_newstate(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_newstate
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_newstate(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:luaL_openlibs
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn luaL_openlibs(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_close
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_close(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_checkstack
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_checkstack(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:luaL_checknumber
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn luaL_checknumber(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_rawlen
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_rawlen(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_pcallk
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_pcallk(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_rawseti
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_rawseti(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_rawset
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_rawset(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_rawgeti
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_rawgeti(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_rawget
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_rawget(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:luaL_ref
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn luaL_ref(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:luaL_unref
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn luaL_unref(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_createtable
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_createtable(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_getglobal
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_getglobal(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_setglobal
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_setglobal(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_next
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_next(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_rotate
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_rotate(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_gc
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_gc(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_gettop
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_gettop(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_type
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_type(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_pushnil
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_pushnil(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_pushinteger
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_pushinteger(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_pushnumber
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_pushnumber(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_pushboolean
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_pushboolean(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_toboolean
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_toboolean(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_settop
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_settop(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_atpanic
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_atpanic(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_pushcclosure
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_pushcclosure(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_sethook
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_sethook(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_pushvalue
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_pushvalue(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:lua_version
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn lua_version(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:CheckBuffer
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn checkBuffer(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:KnownStringToBuffer
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn knownStringToBuffer(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:PushBuffer
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn pushBuffer(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:LoadBuffer
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn loadBuffer(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:LoadString
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn loadString(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:GetTop
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn getTop(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:Type
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn type_(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:PushNil
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn pushNil(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:PushInteger
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn pushInteger(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:PushNumber
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn pushNumber(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:PushBoolean
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn pushBoolean(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:ToBoolean
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn toBoolean(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:Pop
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn pop(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:AtPanic
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn atPanic(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:NewState
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn newState(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:OpenLibs
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn openLibs(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:CheckStack
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn checkStack(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:CheckNumber
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn checkNumber(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:RawLen
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn rawLen(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:PushCFunction
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn pushCFunction(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:PCall
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn pCall(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:RawSetInteger
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn rawSetInteger(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:RawSet
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn rawSet(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:RawGetInteger
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn rawGetInteger(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:RawGet
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn rawGet(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:Unref
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn unref(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:CreateTable
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn createTable(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:GetGlobal
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn getGlobal(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:SetGlobal
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn setGlobal(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:Next
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn next(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:PushValue
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn pushValue(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:SetTop
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn setTop(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:Rotate
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn rotate(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }

  /// libs/server/Lua/NativeMethods.cs:SetHook
  ///
  /// mlua(luau) 以高级 API 等价承接 C API；本绑定面不再逐函数映射，
  /// 保留占位体并记录于 check/ignore（见 NativeMethods.yml 中文理由）。
  pub fn setHook(&self, _state: &mut LuaStateWrapper) -> i32 {
    let _ = _state;
    0
  }
}
