终态：合入 22ccb3a（经 496863a ff 并 dev），settle_script_call 可让渡判据单点（回调线程 == host.script_thread 且 lua_isyieldable）+ take_script_yield(can_yield) 携带就地取消，不可让渡处挂起命令零执行、redis.call 经沙箱 error_wrapper_r1 报 ERR redis.call cannot block inside a coroutine or C-call boundary（可 pcall 捕获），cancel_script_suspend 单点收口四处手写取消，wnode/tests/lua_script_tests.rs 新增三形态测试全绿。

甄别结论：通过（定级 P1：默认沙箱确定性触发，BLPOP 元素静默丢失 + 本连接永久挂死）

甄别复核（执行席现码亲验，2026-10-04）：
1. rust 锚全成立：redis.rs:671 settle_script_call 取 take_script_yield 后无条件 state.script_yield(1)；executor.rs:473 run_common 派生协程并记 script_thread；lua.rs:69 park_script_suspend 搬空泵可见槽；lua.rs:295 run_lua_command 收尾只查 take_blocked_wait/take_slow_wait 不查 script_suspend；commands.rs:413 continue_execute_script 的 suspended_key()? 早退还 None 即应答丢弃；consume.rs:182 泵 has_script_suspend 进 resume_suspended_script。
2. Luau 源码锚亲验（luau0-src 0.21.0+luau736 本地 registry 可达）：ldo.cpp:805-806 lua_yield 判 nCcalls>baseCcalls 即 luaG_runerror；ldo.cpp:820 lua_isyieldable；ldo.cpp:686 resume_start 置 co->baseCcalls=++nCcalls（用户协程顶层 C 帧可让渡，让渡标记经 lcorolib.cpp:50 auxresume 的 lua_xmove 泄漏回脚本）；ltablib.cpp:27/45 sort 比较器走 lua_call（C 边界不可让渡）。
3. C# 锚全成立：LuaRunner.Functions.cs:3313 TryConsumeMessages 同步收割；ListCommands.cs:282 / SortedSetCommands.cs:1582 AsyncUtils.BlockingWait；Loader.cs:323 DefaultAllowedFunctions 含 coroutine；LuaRunner.cs:512 LuaWrappedError。
4. 查重：task/todo/ing/done/reject 四池与 task/refactor-backlog.md 无同判据票（backlog 命中 suspend 为 wepoch try_suspend 删码条目，不同根因）；doc/zh 下无 deviations.md，无在册偏差条目可并案。
5. 方案合规：可让渡判据单点 + 让渡点就地取消，script_thread 迁移入 HostShared 单一真源（回调窗口仅可达 host），取消口收口三处手写重复，无双重兜底。

审核结论：通过

审核核实：
1. 真实性成立：wedb/wlua/src/functions/redis.rs:settle_script_call 取标记后无条件 state.script_yield(1)，无脚本线程比对、无 lua_isyieldable 校验（sys.rs 未绑定该函数）；回调 state 为 context.rs:host_trampoline 以调用线程 l 构造的 view，用户协程内即协程 co。
2. 沙箱可达：wedb/wlua/src/loader.rs:default_allowed_functions 含 "coroutine"/"table"/"string"，对位 garnet/libs/server/Lua/LuaRunner.Loader.cs:327。
3. Luau 语义核实（luau0-src 0.21.0+luau736）：lcorolib.cpp:auxresume 调 lua_resume(co, L, narg)，co 内 C 函数可让渡，标记值经 coroutine.wrap 交还脚本；ltablib.cpp 比较器走 lua_call 递增 nCcalls，ldo.cpp:806 lua_yield 抛 "attempt to yield across metamethod/C-call boundary"（build.rs 为 longjmp 形态，错误照常抛出）。
4. 孤儿链路核实：wedb/wlua/src/commands.rs:finish_execute_script 清 suspended；wnode lua.rs:run_lua_command 收尾只查 take_blocked_wait/take_slow_wait；consume.rs 见 has_script_suspend 进 resume_suspended_script，先 await blocked.resolve()/slow.resolve() 真实执行，再 continue_execute_script 因 suspended_key() 为 None 直接返回，应答丢弃，BLPOP 弹出元素丢失成立。
5. C# 对照：garnet/libs/server/Lua/LuaRunner.Functions.cs:3313 TryConsumeMessages 同步重入，ListCommands.cs:282 AsyncUtils.BlockingWait 内联收割，任意调用点 redis.call 返回真实应答。
6. 查重：task/todo、task/reject、.forks/sync-2026-10-02 无同题提案（.forks 中 script-suspend 相关两项为泵存活探测与 EXEC 重放残留，不同根因）。
7. 方案修订：原方案「ScriptingApi 新增取消口 + 脚本完成后残留闸」为双重兜底，精简为让渡点单处闭环（见下方审核优化版执行方案第 5 步依据）；script_thread 迁移而非镜像，取消口并入既有 take_script_yield 不扩 trait 面。

redis.call 在脚本自建协程或 C 调用边界内命中阻塞/慢路径时让渡错位，挂起体脱离脚本成孤儿，脚本结束后被泵驱动执行且应答丢弃（BLPOP 弹出元素永久丢失）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# redis.call 为同步 C 回调：LuaRunner.Functions.cs:ProcessCommandFromScripting 尾部 respServerSession.TryConsumeMessages 重入内嵌 processor，脚本内 BLPOP/BLMOVE 等阻塞命令经 ListCommands.cs / SortedSetCommands.cs 的 AsyncUtils.BlockingWait(itemBroker.GetCollectionItemAsync(...)) 在回调栈上内联收割，应答齐后才返回 Lua。无论调用点位于脚本主函数、脚本自建 coroutine、table.sort 比较器、string.gsub 回调还是元方法内，redis.call 返回值恒为该命令真实应答，命令副作用与应答一一对应。默认沙箱导出 coroutine（LuaRunner.Loader.cs 默认允许集含 "coroutine"），用户协程内 redis.call 是合法用法。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   rust 协程化：执行器仅为脚本主函数派生一条协程线程（wedb/wlua/src/runner/executor.rs:LuaRunner::run_common 的 state.new_thread + resume），挂起协议假定 redis.call 恒在该线程的可让渡帧上执行。
   链路：wedb/wnode/src/resp/resp_server_session/lua.rs:RespScriptingApi::dispatch_resp 命中 take_blocked_wait / take_slow_wait 即 park_script_suspend——挂起体移出泵可见槽、存入 self.script_suspend 并置 script_yield_tag；随后 wedb/wlua/src/functions/redis.rs:LuaRunnerFunctions::settle_script_call 取走标记，无条件 state.script_yield(1)（sys::lua_yield）。该处不校验当前 lua_State 是否为 runner.script_thread，也不校验 lua_isyieldable：
   a. 脚本自建协程（coroutine.wrap / coroutine.resume 内调 redis.call）：Luau lua_resume(co, from=L) 令 co.baseCcalls = ++nCcalls，co 内 C 函数可让渡，lua_yield 挂起的是用户协程 co 而非脚本线程。coroutine.wrap 把让渡标记（整数 1=Blocked / 2=Slow）当作 redis.call 结果交还脚本，脚本继续执行并正常完成——run_for_session 返回 None，wedb/wlua/src/commands.rs:LuaCommands::finish_execute_script 清 suspended，但会话 script_suspend 仍为 Some。
   b. C 调用边界内（table.sort 比较器、string.gsub 回调、__index/__eq 等元方法经 luaD_call 递增 nCcalls）：luau0-src ldo.cpp:lua_yield 判 nCcalls 大于 baseCcalls 抛 "attempt to yield across metamethod/C-call boundary"，脚本出错（或被 pcall 吞掉后继续），挂起体同样滞留 script_suspend。
   run_lua_command 收尾不变式只清 take_blocked_wait / take_slow_wait（泵可见槽，已被 park 搬空），不检查 script_suspend；process_messages 循环尾见 script_suspend.is_some() 停止消费；wedb/wnode/src/net/handler/drive/consume.rs 泵见 has_script_suspend 即进入 resume_suspended_script：先 await blocked.resolve() / slow.resolve() 真实执行挂起命令，再调 continue_execute_script——suspended_key() 已为 None 直接返回 None，应答被丢弃。

3. 逻辑危害确证
   数据丢失：EVAL "local f=coroutine.wrap(function() return redis.call('BLPOP',KEYS[1],0) end) return f()" 1 q，q 为空时 EVAL 立即回 :1（让渡标记泄漏为脚本值，契约分叉），随后本连接被孤儿 BLPOP 挂住；他连接 LPUSH q v 后孤儿 BLPOP 弹走 v、应答丢弃——已确认写入的元素静默消失，任何客户端都收不到。
   状态脱节：挂起期 process_messages 恒返 0，本连接后续流水线命令全被阻至孤儿等待结束（timeout=0 即永久挂死直至断连）；冷键慢路径写命令（SET 等）在脚本应答发出后才落地，脚本观察值与实际执行序倒置；C 边界场景脚本已回错误，命令却在事后执行，客户端据错误重试即重复写。
   可达性：用户脚本可确定性触发（默认沙箱即可），无需竞态；C# 同脚本行为正确，属协程化改造引入的漏项。

涉及代码：
rust 文件与函数：
wedb/wlua/src/functions/redis.rs:LuaRunnerFunctions::settle_script_call
wedb/wlua/src/functions/redis.rs:LuaRunnerFunctions::process_command_from_scripting
wedb/wlua/src/runner/executor.rs:LuaRunner::run_common
wedb/wlua/src/runner/host.rs:HostShared
wedb/wlua/src/api.rs:ScriptingApi
wedb/wlua/src/state.rs:LuaState::script_yield
wedb/wnode/src/resp/resp_server_session/lua.rs:RespScriptingApi::dispatch_resp
wedb/wnode/src/resp/resp_server_session/lua.rs:RespServerSession::park_script_suspend
wedb/wnode/src/resp/resp_server_session/lua.rs:RespServerSession::run_lua_command
wedb/wnode/src/resp/resp_server_session/lua.rs:RespServerSession::resume_suspended_script

对应 c# 文件与函数：
garnet/libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting
garnet/libs/server/Resp/Objects/ListCommands.cs:ListBlockingPop（AsyncUtils.BlockingWait 内联收割）
garnet/libs/common/AsyncUtils.cs:BlockingWait
garnet/libs/server/Lua/LuaRunner.Loader.cs:DefaultAllowedFunctions（coroutine 默认导出）

精炼执行方案（审核优化版，单机制：可让渡判据 + 让渡点就地取消，一处闭环）：
1. 脚本线程指针单一真源：把 wedb/wlua/src/runner/mod.rs:LuaRunner 的 script_thread 字段整体迁入 wedb/wlua/src/runner/host.rs:HostShared（不新增镜像字段，executor.rs 的 run_common 写入、finish_run 清空、continue_session / abort_suspended 判空全部改读 self.host.script_thread），回调窗口经 host 直接字段访问。
2. sys 补绑定：wedb/wlua/src/sys.rs 声明 lua_isyieldable（luau0-src ldo.cpp:820 已导出，语义 nCcalls 小于等于 baseCcalls）；wedb/wlua/src/state.rs 加薄封装 is_yieldable。
3. 让渡判据单点：wedb/wlua/src/functions/redis.rs:settle_script_call 先算 can_yield = (state.raw() == host.script_thread 且 state.is_yieldable())，再把 can_yield 传入既有 take_script_yield（签名改为 take_script_yield(&mut self, can_yield: bool) -> Option<i32>，同步改 api.rs trait、host.rs vtable 与 ScriptSessionRef，不新增 trait 方法）。标记为 None 原样返回；can_yield 为真走原 lua_yield 臂；为假走既有 lua_wrapped_error_view 错误臂（与 PLEASE_SPECIFY_REDIS_CALL 同口径，不发起 lua_error 长跳转），文案新增常量 wedb/wlua/src/strings.rs:ConstantStrings（如 "ERR redis.call cannot block inside a coroutine or C-call boundary"）。
4. 会话侧取消口单点：wedb/wnode/src/resp/resp_server_session/lua.rs 抽 RespServerSession::cancel_script_suspend（take script_suspend，blocked.abort()，slow drop，script_yield_tag 置 None）；RespScriptingApi::take_script_yield 在 can_yield 为假且标记命中时调用它后返回标记（挂起命令零执行）；run_lua_command 入口兜底复位与 resume_suspended_script 缓存缺失臂改调同一函数，消除三处手写取消重复。
5. 不加收尾残留闸：原方案第 3 步（脚本完成后再查 script_suspend）删除。依据：park_script_suspend 与 settle_script_call 在同一 C 回调内同步配对（dispatch_resp 返回即 settle，快路径 get/set 与 fallback 三路全部收敛于 settle_script_call），修复后 settle 只有两种结局——脚本线程可让渡则挂起（phase Some，泵合法续跑），否则就地取消；script_suspend 为 Some 与脚本挂起严格等价，事后闸属双重兜底。
6. 偏差登记：C# 内联 BlockingWait 可在任意调用点收割，rust 受第 12 条运行时纪律约束（禁内联驱动调度器）在非脚本线程/C 边界内无法让渡，改回确定性错误；偏差说明写入 settle_script_call 文档注释（C# 对位行与偏差根因同处，不另建文档）。
7. 测试验证点（wedb/wnode/tests 新增集成测试文件，短测试）：
   a. EVAL "local f=coroutine.wrap(function() return redis.call('BLPOP',KEYS[1],0) end) return f()" 1 q（q 空）→ 回步骤 3 错误帧；同连接紧随 PING 立即回 PONG；他连接 LPUSH q v 后 LLEN q 为 1。
   b. table.sort 比较器内 redis.call('BLPOP',KEYS[1],0)（q 空）→ 错误帧，后续命令可用，LPUSH 后元素仍在。
   c. pcall 包裹 a 的协程调用后脚本继续 redis.call('SET',KEYS[2],'x') 并返回 → SET 生效、错误被脚本捕获、连接无挂起。
   d. 脚本主函数直接 BLPOP 挂起/续跑回归：既有挂起续跑测试全绿（他连接 LPUSH 后 EVAL 回真实元素）。
