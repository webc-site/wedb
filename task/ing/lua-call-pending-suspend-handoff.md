lua redis.call 重入面对慢路径与阻塞两类挂起零承接：脚本命令拿不到应答、应答错插网络流

来源：next/glm.data.md 条 8（该文件已被并发分拣波消费删除，原文转抄存于
/Users/z/git/db/wedb/next/lua-redis-call-pending-suspend.md，本档按当下主仓代码重新取证）。
取证基线：主仓 /Users/z/git/db/wedb，分支 dev，
HEAD a7402c4（bb06827、6311510 两轮复核：本档全部取证文件未被期间提交改动，锚点未位移）。

结论

脚本内 `redis.call` 的重入消费循环只认「输出水位让渡」一种续消费信号，
对慢路径挂起（pending_slow）与阻塞挂起（pending_block）两态完全无承接，
导致三类客户端可见后果：脚本内命令回内部错误而非应答；后续每条 redis.call 连锁拿空应答；
EVAL 返回后残留的挂起 future 被网络泵 resolve，把一帧不属于任何客户端命令位置的应答
直接写进出网流，破坏 pipelining 的应答-命令对齐。第三类是协议流污染，危害高于第一类。

现状

重入面：/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:2690
`struct RespScriptingApi`，:2709 `dispatch_resp` —— :2718-2731 的循环只有
`try_consume_messages()` 与 `take_output_watermark_yield()` 两个分支，
无 `take_slow_wait()` / `take_blocked_wait()`（两取件在 :715、:721，
`resolve_blocked_wait_into` :727）；应答收束为 :2732 `response.extend_from_slice(&session.output)`。
`get` / `set` 两个快路径特例（:2737、:2747）内部同样只走 `dispatch_resp`，同一缺陷。
脚本窗口无挂起防护：:2502-2521 `run_lua_command` 构造 `RespScriptingApi(&mut *self)` 并驱动
`LuaCommands::try_eval`，全程不检查、不清空 pending_slow / pending_block；
会话析构面 :779-784 才处理两态。

挂起侧：命令降级即挂 pending_slow ——
/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/mod.rs:509 判 `Ok(false)`、
:561-566 `session.pending_slow = Some(SlowWait::for_command(..))`；
消费循环遇挂起即停：入口门 /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:1131-1133
（`pending_block.is_some() || pending_slow.is_some()` → `return 0`）、
批内 break :1260-1263。于是 `dispatch_resp` 拿到的 session.output 不含该命令应答，
空输入喂给应答转换器即走错误分支：
/Users/z/git/db/wedb/wedb/wlua/src/runner/resp_convert.rs:592-595 与 :867-870
`log::error!("Unexpected response, this should never happen")` → `UNEXPECTED_ERROR`。
EVAL 收尾后，残留 future 由网络泵独占消费点
/Users/z/git/db/wedb/wedb/wnode/src/net/handler/drive.rs:176-181
`take_slow_wait()` → `resolve().await` → 应答字节 `extend_from_slice` 进本轮出网缓冲，
插在与脚本应答之后，客户端侧无对应命令槽。

复现：`SET k v` → `DEBUG FLUSHANDEVICT` → `EVAL "return redis.call('GET', KEYS[1])" 1 k`。
GET 确有降级（慢路径臂存在：
/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:165 `C::Get`），
快路径冷键即 `UserRead::Deferred` → `Ok(false)`。

C# 参考

/Users/z/git/db/wedb/garnet/libs/server/Lua/LuaRunner.Functions.cs:3137
`ProcessCommandFromScripting`（:319、:325 两处挂载，分别走
transactionalGarnetApi 与 basicGarnetApi），尾部 :3305
`_ = respServerSession.TryConsumeMessages(request.ptr, request.length);`、
:3307 `var response = scratchBufferNetworkSender.GetResponse();` —— 同步重入、同步取答，
不存在「重入返回但命令仍挂起」的形态；C# 存储层的磁盘 pending 由同步上下文就地闭环
（CompletePending 系，例见
/Users/z/git/db/wedb/garnet/libs/server/AOF/AofProcessor.cs:625、:674、:756 与
/Users/z/git/db/wedb/garnet/libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs:61-63
`CompletePendingWithOutputs(wait: true)`）。grep C# 全域亦无「脚本内禁阻塞命令」的门
（LuaRunner.Functions.cs 零 BLPOP/blocking 判定），故 lua 面阻塞命令的语义也要能在
重入内闭环，不能一拒了之。

修法

原则：`dispatch_resp` 返回前，本次重入内产生的挂起必须已被 resolve 并把应答写进
response，绝不允许把 pending 留回会话状态。

1. 慢路径态：循环内补 `take_slow_wait()` 承接——取到 future 后同步驱动到完成
   （`SlowWait::resolve()` 已是 async 口，drive.rs:177 同款调用），把应答字节并入本轮
   response 后继续消费脚本余下命令。驱动 future 的同步入口必须复用
   /Users/z/git/db/wedb/task/ing/block-on-single-source.md 收敛后的单点
   （现状四份复抄：/Users/z/git/db/wedb/wedb/wnode/src/resp/vector/vector_store_callbacks.rs:47、
   /Users/z/git/db/wedb/wedb/wnode/src/resp/acl_store.rs:37 等），
   严禁在 resp_server_session.rs 新开第五份 block_on。
2. 阻塞态：同样补 `take_blocked_wait()` 承接，走 :727 `resolve_blocked_wait_into` 的等价
   闭环（应答并入 response，不写会话出网缓冲）。若裁决「脚本内阻塞命令应报错」，
   必须先核 C# 该场景的真实应答形态再定，不接受顺手造新错误文案。
3. 收尾不变式：`run_lua_command` 脚本窗口关闭处（:2526-2530，摘位图 :2528、
   脚本缓存挂回 :2529）加断言式防御——
   窗口退出时 pending_slow / pending_block 必为 None，非 None 即写明错误并上报，
   杜绝残留 future 被 drive.rs 泵当成自己的挂起命令 resolve（这一条是防协议流错插的
   最后一道闸，必须落地，不接受只靠 1/2 的隐含约定）。
4. 与 /Users/z/git/db/wedb/task/ing/slow-path-string-key-admin-arms.md 无先后依赖但同向：
   本单让脚本面拿到应答，那单让慢路径真有臂可落；两单若相邻落地，
   lua 面 SETNX/TTL 的闭环才真正成立。

优先级

污染扩散 + 功能缺口（先于纯文案类打磨）：客户端可见的协议流错位与内部错误外泄，
且污染面是整个连接的应答序，不是单条命令。

交叉引用

1. /Users/z/git/db/wedb/task/ing/block-on-single-source.md（同步驱动单点，本单必须转调）。
2. /Users/z/git/db/wedb/task/ing/slow-path-string-key-admin-arms.md（慢路径缺臂，同域）。
3. task/ing/resp-server-session-file-split.md（认领前在
   /Users/z/git/db/wedb/next/resp-server-session-file-split.md，拟把
   `struct RespScriptingApi` / `impl ScriptingApi` 段 :2684+ 拆出本文件；
   本单与该段同域，宜先落本单逻辑、由拆分单整块搬迁，或拆分先行后本单按新文件重定位）。
4. /Users/z/git/db/wedb/task/ing/zset-aggregate-member-ttl-filter.md 与本单同为脚本可读到的
   聚合语义票，不同文件，无冲突。
5. 并发分拣波把本条原文逐字转抄为
   /Users/z/git/db/wedb/next/lua-redis-call-pending-suspend.md（无 HEAD 复核、无修法细化），
   主题与本单同一；派单以本单为载体，勿双花。

验收

1. `SET k v` → `DEBUG FLUSHANDEVICT` → `EVAL "return redis.call('GET', KEYS[1])" 1 k`
   返回真实值；同脚本连发多条 redis.call（含降级与非降级混合）逐条应答正确。
2. 脚本内 `redis.call('BLPOP', k, 0)` 形态有确定且与 C# 一致的结论（应答或错误），
   且 EVAL 之后连接出网流零孤儿帧：pipelining 后续命令应答逐条对齐
   （可在 /Users/z/git/db/wedb/wedb/wnode/tests/lua_script_tests.rs 与
   net_pump_consume_tests.rs 同型用例上扩展断言）。
3. 全链路 grep：`resp_server_session.rs` 不再出现第五份 block_on；
   dispatch_resp 返回路径上 pending_slow / pending_block 恒 None（有断言或防御臂）。
4. cargo check 零告警（禁写 allow）。
