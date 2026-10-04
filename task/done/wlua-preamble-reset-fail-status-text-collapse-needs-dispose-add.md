甄别结论：通过（2026-09-29 主控甄别，定级 P3——executor.rs:277-280/:312-316 两失败臂一律 PARAMETER_RESET_FAILED_OTHER 且加码 needs_dispose=true；C# LuaRunner.Functions.cs 两失败臂按 LuaStatus 分流四文案且 preamble 臂无 NeedsDispose 置位（唯一置位点 LuaStateWrapper.cs:771 allocator 域）。修复：补三常量逐字节对齐+状态出参口+四文案分流单源；pcall 出参口勿破坏既有签名；needs_dispose 二选一禁并存）

审核结论：通过（2026-09-29 甲轮35-A，P3 级）。两失败臂恒回 OTHER+置 needs_dispose、state.rs:147-171 状态码丢弃、C# 四文案分臂+无 NeedsDispose 置位、finish_execute_script 消费面全复核成立。方案 3 二选一边界清楚：默认摘除对齐 C# 或登记刻意偏差，禁并存。

原票面：
脚本 preamble 参数重置失败错误文案塌缩恒 Other 且 rust 加码 needs_dispose 未登记

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# TryResetParameters（libs/server/Lua/LuaRunner.cs）对 reset_keys_and_argv 的 PCall 失败保留原始 LuaStatus（failingStatus 出参回传），两处 preamble 失败臂（libs/server/Lua/LuaRunner.Functions.cs 的 UnsafeRunPreambleForSession 与 UnsafeRunPreambleForRunner）按状态 switch 分流四种文案：ErrSyntax 回 "Resetting parameters to Lua script failed: Syntax"、ErrMem 回 ": Memory"、ErrRun 回 ": Runtime"、其余回 ": Other"（常量源 libs/server/Resp/CmdStrings.cs 的 LUA_parameter_reset_failed_memory/syntax/runtime/other）。失败臂只回错误帧，不置 NeedsDispose，runner 留在会话缓存下次复用。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧 try_reset_parameters（wedb/wlua/src/runner/executor.rs）经 LuaState::pcall 执行重置函数，pcall 失败载荷只携错误文本串（wedb/wlua/src/state.rs 的 pcall/pcall_n 丢弃底层 lua_pcall 状态码），Error 枚举（wedb/wlua/src/error.rs）无状态维度。两处 preamble 失败臂（run_preamble_for_session_slice 与 run_preamble_for_runner，同文件）一律返回 PARAMETER_RESET_FAILED_OTHER；wedb/wlua/src/strings.rs 仅存 OTHER 一常量，Memory/Syntax/Runtime 三常量缺失。且两处失败臂均额外置 host.needs_dispose = true，C# 对应路径无此置位。

3. 逻辑危害确证（实际危害）
其一，错误帧文案契约分叉：managed 内存模式配限额（lua-memory-limit-bytes，下限 1KiB）下 runner 复用缩参（前次 KEYS/ARGV 更大触发重置臂）遇 ErrMem 时，C# 回 ": Memory" 而 rust 恒回 ": Other"，EVAL/SCRIPT LOAD 编译后执行的用户可观测帧面字节不等。其二，needs_dispose 加码系未登记行为分叉：rust 置位致失败后 runner 被 finish_execute_script 移出会话缓存、下次重编译，C# 复用原 runner；两种收口各能自洽，但现状既未对齐 C# 也未登记为刻意偏差，属无裁决的分叉面。

涉及代码：
rust 文件与函数：
wedb/wlua/src/runner/executor.rs:LuaRunnerExecutor::run_preamble_for_session_slice
wedb/wlua/src/runner/executor.rs:LuaRunnerExecutor::run_preamble_for_runner
wedb/wlua/src/runner/executor.rs:LuaRunnerExecutor::try_reset_parameters
wedb/wlua/src/state.rs:LuaState::pcall
wedb/wlua/src/error.rs:Error
wedb/wlua/src/strings.rs:ConstantStrings::PARAMETER_RESET_FAILED_OTHER

对应 c# 文件与函数：
libs/server/Lua/LuaRunner.cs:LuaRunner.TryResetParameters
libs/server/Lua/LuaRunner.Functions.cs:LuaRunnerFunctions.UnsafeRunPreambleForSession
libs/server/Lua/LuaRunner.Functions.cs:LuaRunnerFunctions.UnsafeRunPreambleForRunner
libs/server/Resp/CmdStrings.cs:LUA_parameter_reset_failed_memory

精炼执行方案：
1. wedb/wlua/src/strings.rs 补 PARAMETER_RESET_FAILED_MEMORY/SYNTAX/RUNTIME 三常量，文本逐字节对齐 CmdStrings.cs 四常量。
2. LuaState 层为 pcall 增加状态出参口（或 pcall_n 返回底层 lua_pcall 状态码），try_reset_parameters 失败载荷改携状态；两处 preamble 失败臂按 C# switch 形分流四文案。
3. needs_dispose 置位随本票裁决：默认按 C# 摘除（reset 失败 runner 留缓存复用）；若判 rust 侧重置中途断裂必须弃 runner，则改登记为刻意偏差并注记回指本票，二选一禁止并存。
4. 测试：状态到文案映射收纯函数加表驱动断言（四状态全覆盖）；wlua/tests/script_cache_session.rs 旁补 preamble 失败臂回归（Misuse 注入路径或状态注入口），断言帧文案与 runner 去留判据。

终态注记（2026-09-29 执行子代理收口）：
合入哈希 5250eaf（merge --no-ff，特性提交 a2f0439）。收口形态：
1. sys.rs 补 LUA_ERRRUN/LUA_ERRSYNTAX/LUA_ERRMEM/LUA_ERRERR 四状态码常量（lua_State 枚举序，与 C# KeraLua.LuaStatus 同序），lib.rs 导出为 resume/pcall_status 状态码 pub API 语义配套。
2. state.rs 新增 LuaState::pcall_status(nargs) -> Result<(), c_int> 状态出参口（失败载荷即底层 lua_pcall 状态码，pcall/pcall_n 既有签名未动）；lua_pcall 直调收编 raw_pcall 单点。
3. strings.rs 补 PARAMETER_RESET_FAILED_MEMORY/SYNTAX/RUNTIME 三常量，逐字节对齐 CmdStrings.cs:545-548。
4. executor.rs：try_reset_parameters 失败载荷改携状态码（push_ref 失效防御臂折算 LUA_ERRRUN，对齐 C# 压 nil 后调 nil 的真实行为）；新增 LuaRunner::parameter_reset_failed(status) 单源分流（ErrSyntax→Syntax/ErrMem→Memory/ErrRun→Runtime/其余→Other），两 preamble 失败臂共用。
5. needs_dispose 裁决=对齐 C# 撤除：两失败臂 host.needs_dispose = true 置位删除，HostShared.needs_dispose 死字段删除，LuaRunner::needs_dispose 收敛 state 单源（置位唯一在 allocator 域，对齐 LuaStateWrapper.cs:771）；reset 失败 runner 留会话缓存复用。
6. tests/preamble_reset_failed_status.rs：四状态→四文案表驱动全覆盖 + pcall_status 底层状态真值锚（error() → ErrRun）+ reset 臂触发（KEYS 收缩）后 runner 复用与 needs_dispose 终态断言。
worktree 内 cargo check --all-targets 全 workspace 零警告零错误。
