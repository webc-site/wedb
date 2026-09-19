Lua 关闭门去重：单一门限 + 逐字对标 C# 文案

背景（对照 C#）
C# 只有一道 Lua 启用门 LuaCommands.cs:CheckLuaEnabled，判据读 serverOptions.EnableLua，
未启用经 TryWriteError 单点回 RESP_ERR_LUA_DISABLED（CmdStrings.cs:215，逐字
"ERR This instance has Lua scripting support disabled"）。EVAL/EVALSHA/SCRIPT 五入口同调此一处。

rust 现状：两道互斥门
1. 会话层 wedb/wnode/src/resp/resp_server_session.rs run_lua_command 用
   session_script_cache.take() 的 None 分支当门（该 Option 仅在 enable_lua 时创建，None≡未启用），
   但回自造文案 "ERR Lua is disabled."（全仓无常量，违反一处定义，字节与 C# 不同）。
2. wlua 层 wedb/wlua/src/commands.rs check_lua_enabled 持正确逐字文案，但判据 ctx.lua_enabled
   在唯一构造点被写死 true，五个调用点（try_evalsha/try_eval/network_script_exists/
   network_script_flush/network_script_load）恒真短路 → 死臂。

选型：方案 (a)——删死臂，会话门为唯一权威门
让 wlua 的门可达需把 LuaSessionContext.session_cache 由 &mut 改为 Option 并改所有命令体，
只为未启用态多走一遍空分派，改动过大且会话仍需分派路径。
会话 run_lua_command 是所有 Lua 命令的单一入口，其 None 分支判据即 enable_lua（缓存存在⇔启用），
与 C# 单布尔同构，故取此一处为唯一门，删除 wlua 死臂。

改动清单
1. wedb/wresp/src/cmd_strings.rs 新增常量 RESP_ERR_LUA_DISABLED，逐字对标 C#，一处定义。
2. wedb/wlua/src/commands.rs 删 check_lua_enabled 函数、LuaSessionContext.lua_enabled 字段、五处调用点。
3. wedb/wnode/src/resp/resp_server_session.rs 删 ctx 里 lua_enabled: true 写死；
   None 分支改 self.abort_error_message(cs::RESP_ERR_LUA_DISABLED)，注释保留
   libs/server/Lua/LuaCommands.cs:CheckLuaEnabled 映射（供 check.js 识别）。
4. wedb/wnode/tests/resp_server_session_tests.rs eval_disabled_rejects 断言改为 C# 逐字文案。

验收
cargo check --workspace --all-targets 绿、无警告、无残留旧字符串/字段引用。
