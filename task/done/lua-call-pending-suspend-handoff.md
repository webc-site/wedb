lua redis.call 重入面对慢路径与阻塞两类挂起零承接：脚本命令拿不到应答、应答错插网络流

认领：本档自 next/ 移入 task/ing/，取证基线主仓 dev HEAD 82e5d7f（票面原基线
a7402c4，锚点已按当下代码重定位）。分支 fix-lua-pending-handoff（/tmp/fork 同名
worktree）。

甄别结论

票面逐条对 C# 与当下 rust 复核，全部成立，予以实施。补充一条同域缺陷（见
「新增取证」），一并落地。

C# 侧事实复核：LuaRunner.Functions.cs:3137 ProcessCommandFromScripting 尾部
:3305 同步重入 TryConsumeMessages、:3307 取应答，磁盘 pending 与阻塞等待都在
调用栈上闭环（ListCommands.cs:284、:374、:913 的阻塞命令一律 AsyncUtils
.BlockingWait 内联），故 C# 不存在「重入返回而命令仍挂起」的形态；libs/server/
Lua 全域 grep 无「脚本内禁阻塞命令」的门（零 BLPOP/blocking 判定），故脚本内
阻塞命令的应答也要在重入内闭环，不做拒绝。

rust 侧事实复核（文件行号按 82e5d7f）

1. 重入面只认水位让渡：resp/resp_server_session.rs:2744 struct RespScriptingApi、
   :2764 dispatch_resp 的循环只有 try_consume_messages 与
   take_output_watermark_yield（:2769）两分支，无 :729 take_blocked_wait /
   :735 take_slow_wait 承接；get/set 两个快路径特例（:2790、:2803）内部同走
   dispatch_resp，同一缺陷。
2. 会话消费循环遇挂起即停：入口门 :1152（pending_block/pending_slow 任一非
   None 即 return 0）、批内 break :1291，于是 dispatch_resp 冲出的应答不含该
   命令应答。
3. 空应答即内部错误外泄：wlua/src/runner/resp_convert.rs:600 process_single_
   resp_term_view 首字节缺失分支与 :873 default_resp_term_view 均
   log::error!("Unexpected response, this should never happen") → UNEXPECTED_ERROR。
4. 残留挂起体被网络泵误收：net/handler/drive.rs:186 take_slow_wait → resolve
   → 应答字节并进出网缓冲，位置在脚本应答之后，客户端侧无对应命令槽。
5. 脚本窗口无挂起防护：:2529 run_lua_command 全程不检查、不清空两态；仅
   :782 dispose 处理。
6. 降级确有臂可落（复现成立）：resp/garnet_api/mod.rs:504 判 Ok(false)、
   :504-508 挂 SlowWait::for_command；resp/garnet_api/slow.rs:165 C::Get 慢路径
   臂存在，冷键 GET 必降级。

新增取证（同一重入面的另两类客户端可见后果，与票面同源）

1. 水位让渡丢应答：dispatch_resp 续消费前未冲出 session.output，而
   try_consume_messages_body 入口 :1044 经 enter_and_get_response_object 清空
   输出缓冲，故单条脚本命令应答达 OUTPUT_WATERMARK_BYTES 即整段凭空丢失，
   转换器拿到空应答（与票面第 3 点同一错误出口，但触发不需挂起态）。
2. 外层批字节被覆写：dispatch_resp 覆写会话接收窗与游标，而 C# 切的是内嵌
   processor（SessionScriptCache.cs:64 new RespServerSession，自带独立接收缓冲
   与 ScratchBufferNetworkSender）的窗口，外层批字节不受扰动。rust 重入共享
   会话后未复原，导致同批 EVAL 之后未消费的命令凭空消失（原注释「与 C# 指针
   切换行为一致」的判断有误，已改）、EVAL 之前已产出的应答被 Lua 转换器误读
   成本条 redis.call 的应答，MULTI/EXEC 回退重解析所读的排队字节同样被覆写。

修法（已落地形态）

原则与票面一致：dispatch_resp 返回前，本次重入产生的挂起必须已 resolve 并把
应答写进 response；脚本窗口关闭时会话窗口状态（接收窗·游标·挂起态）原样复位。

1. dispatch_resp 消费序改为网络泵同构：消费 → 水位让渡先 take_output_into
   冲出再续消费 → 取 blocked/slow 就地驱动至完成、应答经泵同款并入口写入
   response。同步驱动复用 wbase::future::blocking_wait 单点（task/ing/
   block-on-single-source.md 已收敛合入，HEAD 里 vector_store_callbacks.rs、
   acl_store.rs、txn_proc_view.rs 同源转调），未新开第五份 block_on。
2. 慢路径应答并入收束为 RespServerSession::resolve_slow_wait_into 一枚口，与
   既有 resolve_blocked_wait_into 同形态（先冲出已累积应答、再并入挂起体应答、
   出向量按同一口径入账），drive.rs 原内联 extend 改调该口，消除第二份实现。
3. run_lua_command 脚本窗口：mem::take 换出外层 output 与 recv_buffer、保存
   三游标，窗口关闭原样挂回并复位 output_watermark_yield，零拷贝，等价 C#
   内嵌 processor 的缓冲隔离。
4. 收尾不变式：脚本窗口关闭处对两态做取走式防御，非 None 即 log::error 写明
   并按 dispose 同口径取消（blocked.abort / drop future），杜绝残留挂起体被泵
   当本连接挂起 resolve——防协议流错插的最后一道闸。
5. 脚本内阻塞命令采「与 C# 一致」结论：就地驱动到取到元素/超时，不造新错误
   文案；无经纪注入时仍走既有立即可取降级路径（park_broker_wait 返回 false）。

验收

1. 冷键脚本内 GET 返回真实值，同脚本降级与非降级混发逐条正确：
   tests/lua_script_tests.rs:eval_redis_call_cold_key_slow_path_handoff。
2. 同批 PING + EVAL(冷键) + PING + SET 应答逐条对齐、EVAL 帧后命令不丢、
   批收尾无挂起残留：eval_pipelined_batch_reply_alignment。
3. 脚本内 BLPOP 在重入内取到元素且弹出生效：eval_script_blpop_blocked_wait_
   handoff。
4. resp_server_session.rs 无新增 block_on 复刻；dispatch_resp 返回路径上
   pending_slow/pending_block 恒 None（有防御臂）。
5. cargo check 零告警（禁写 allow）。

交叉引用

1. next/slow-path-string-key-admin-arms.md（慢路径缺臂，同向无先后依赖：本单
   让脚本面拿到应答，那单让慢路径真有臂可落）。
2. 票面原引 task/ing/block-on-single-source.md 已合入并归档删除，其收敛口即
   wbase::future::blocking_wait，本单直接转调。
3. 票面原引 next/resp-server-session-file-split.md 已不在 next/（并发分拣消
   费）；该段（RespScriptingApi / impl ScriptingApi）后续整块搬迁时随迁。
