归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 aa1655a（P1），收口形态：EXEC 重放窗一态——无 AUTH 形 HELLO 经 commit_hello_state_and_write_reply 同步快臂直出应答 map（等形独立同帧），携 AUTH/语法错形维持围栏、ACL 族回专属文案；顺带收口隐性 P0（park_cold_context_load 围栏帧直写移出态 self.output 必被 exec() restore 覆没，改写入参 output 缓冲＋文案按域参数化），§58d 册内追记，新锁 exec_replay_hello_acl_txn_gate 207 行＋§58a 两断言随收口更新。续排注：票面 core/pump 锚行号漂移已按现码执行注记。

甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P1
核验记录：C# 亲验——TxnRespCommands.cs NetworkSKIP :131-184 只拒 WATCH 族/SELECT 异库/SWAPDB/DEBUG 门，无 HELLO/ACL 拒臂（现读亲见）；RespCommandsInfo.json:2104-2108 HELLO Flags "Fast, Loading, NoAuth, NoScript, Stale, AllowBusy" 无 NoMulti 亲验。rust 亲验——core.rs:1483-1498 预筛 `cmd == Auth || Hello || is_acl_command` 且 txn_state != None 即写错误帧 return、err 选择器仅两值（非 Auth 一律 RESP_ERR_HELLO_IN_TXN_UNSUPPORTED，cmd_strings.rs:409-411 文案亲见）、pump.rs:57-63 事务窗围栏同样复用 HELLO 文案——扩大化拒绝臂现码原样无灭失；与 §58a 在册裁决（中止面恰等于携 AUTH 组）矛盾成立。查重：§58 全册无 ACL 族登记；四池零同轴（done wcpr-hlog「回放窗」他轴）。架构：无 AUTH HELLO 抽同步快臂（无存储点查、不停车，守事务窗零停泊红线）、ACL 专属文案+§58 追记、pump 文案调用方传入，一态收口无第二机制；三锁测试+既有 hello_cold_park 族回归闭环。格式：纯文本、双侧齐全。定级 P1：EXEC 应答数组内正常命令收错误元素且三域共用错位文案，对外契约分叉+内部裁决面自相矛盾，生产可达。

审核结论：通过（修复级；core.rs:1483-1498 预筛扩大化与 ACL 共用 HELLO 文案、network_skip 仅携 AUTH 组中止的自陈与重放窗拒无 AUTH 形自相矛盾、C# NetworkSKIP 无 HELLO/ACL 拒臂且双侧 RespCommandsInfo Flags 一致无 NoMulti、process_hello_command_state 空 username 路径全仓零 await/点查（撕裂顾虑仅适用携 AUTH 形）——全部亲验属实；§58 全册无 ACL 登记、各池无同轴活跃票）。审核注记：wresp JSON 非逐字节同源（AUTH 已加 NoMulti）本轴无碍；既有 hello 测试文件实名为 hello_cold_park_pipeline_frames.rs。

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. core.rs 预筛臂收敛：Hello 且 parse_hello_args Ok 且无 auth 选项组时经同步快臂（抽无存储 sync 路径或首 poll 完成）直出 HELLO map（语法错形维持既有错误路径），携 AUTH 形维持拒——与 §58a「中止面恰等于携 AUTH 组」裁决面收口为一态。
2. err 选择器三值化：ACL 族回专属文案（采文案分支，不采排队期补中止——EXECABORT 更毁契约），并在 deviations §58 追记「ACL 重放窗拒」偏差条目注明理由。
3. pump.rs:57-63 SELECT 冷库围栏文案由调用方传入，SELECT 臂回 SELECT_IN_TXN 族帧，杜绝三域共用 HELLO 文案。
4. 测试验证点：wnode/tests 三锁——MULTI+HELLO 3（无 AUTH）+EXEC 应答数组含正常 HELLO map；MULTI+ACL LIST+EXEC 元素不含 "ERR HELLO" 字样且为 ACL 专属帧；同库 SELECT 冷库窄窗（装配期卸载映射后重放）回 SELECT 专属帧；既有 hello_cold_park_pipeline_frames/acl_tests/requirepass_test 保持绿。

MULTI 排队合法的 HELLO（无 AUTH）与 ACL 十子命令在 EXEC 重放窗被分派漏斗停车预筛一刀切拒绝，ACL 命令回 HELLO 文案（与 deviations §58a 裁决面自相矛盾）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 中 HELLO 未声明 NoMulti（wresp/RespCommandsInfo.json HELLO 条目 Flags 为 "Fast, Loading, NoAuth, NoScript, Stale, AllowBusy"，与 garnet/libs/resources/RespCommandsInfo.json 同源），AllowedInTxn 为 true；ACL 十子命令（ACL|LIST/ACL|WHOAMI 等）Flags 亦无 NoMulti。事务排队期 NetworkSKIP（garnet/libs/server/Transaction/TxnRespCommands.cs:105-204）只拒 WATCH/SELECT 异库/SWAPDB/DEBUG 门，无 HELLO 与 ACL 拒臂，两者均落 +QUEUED 排队。EXEC 启动后（TxnRespCommands.cs:63-74 endReadHead 回退 txnStartHead）重放窗 TxnState.Running 直通 ProcessBasicCommands（RespServerSession.cs:662-666），HELLO 经 RespServerSession.cs:1090 `RespCommand.HELLO => NetworkHELLO()`、ACL 族经 AdminCommands.cs:65-69 分派各自执行，NetworkHELLO 与 ACL 十处理器全仓零 TxnState 门（grep 亲验），正常应答写入 EXEC 应答数组。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   rust 排队准入面与 C# 同构：wedb/wnode/src/resp/txn_resp_commands.rs:network_skip 中 HELLO 仅携 AUTH 选项组时中止（§58a 裁决面「中止面恰等于可解析出合法 AUTH 选项组之形集」，码内 :306-319 注释自陈），无 AUTH 的 HELLO 与 ACL 族（allowed_in_txn=true）均落排队得 +QUEUED。EXEC 重放（Running 态游标回退重解析，wedb/wnode/src/resp/resp_server_session/txn.rs:41-44 直通 process_basic_commands）时分派链穿透至尾部 wedb/wnode/src/resp/resp_server_session/core.rs:dispatch_via_garnet_api（:1483-1498）：停车预筛 `cmd == RespCommand::Hello || is_acl_command(cmd)` 命中且 `self.txn_state != TxnState::None`（Running 成立）即写错误帧 return——err 选择器只区分 Auth 与非 Auth，HELLO 与 ACL 族十命令一律共用 RESP_ERR_HELLO_IN_TXN_UNSUPPORTED（"ERR HELLO is currently unsupported inside a transaction."，wresp/src/cmd_strings.rs:409-411）。该臂本意是「事务窗严禁停车挂起」的围栏（防 pending_auth_acl 撕裂重放），实现却把不停车即可执行或本应另案处置的命令全部扩大化拒绝。另 SELECT 冷库窄窗围栏同病：pump.rs:park_cold_context_load（wedb/wnode/src/resp/resp_server_session/pump.rs:57-63）事务窗内弃置 cold_ctx 时同样写 RESP_ERR_HELLO_IN_TXN_UNSUPPORTED，SELECT 场景文案错位同族。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   无 panic 与数据损坏，EXEC 应答数组元素数与 operation_cnt 一致、事务仍闭环复位；危害为对外契约分叉与内部一致性破坏：客户端 MULTI 内发 ACL LIST（或 HELLO 3 不携 AUTH）时 C# 返回正常应答数组元素，rust 返回错误元素且 ACL 命令收到 "ERR HELLO is currently unsupported inside a transaction." 的错位文案；同时与 deviations §58a 已裁决的「中止面恰等于携 AUTH 组形集」直接矛盾——无 AUTH 的 HELLO 按 §58a 应与普通命令同形重放，现码围栏扩大化把裁决面撕开。生产可达：HELLO 协议协商与 ACL 查询命令均为客户端库常规命令，MULTI 内混发即触发。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/resp_server_session/core.rs:RespServerSession:dispatch_via_garnet_api（:1483-1498 事务窗拒绝臂与 err 选择器）
wedb/wnode/src/resp/txn_resp_commands.rs:TransactionManager:network_skip（HELLO 无 AUTH 组落排队臂 :306-319）
wedb/wnode/src/resp/resp_server_session/txn.rs:RespServerSession:process_transactional_command（Running 直通 :41-44）
wedb/wnode/src/resp/garnet_api/mod.rs:is_acl_command（ACL 十命令谓词 :611-625）与 StoreGarnetApi:exec_auth_acl（Hello/ACL 异步臂，普通窗口合法执行面）
wedb/wnode/src/resp/resp_server_session/pump.rs:RespServerSession:park_cold_context_load（SELECT 冷库事务窗围栏文案 :57-63）
wedb/wresp/src/cmd_strings.rs:RESP_ERR_HELLO_IN_TXN_UNSUPPORTED（:409-411）

对应 c# 文件与函数：
garnet/libs/server/Transaction/TxnRespCommands.cs:NetworkSKIP（:105-204，无 HELLO/ACL 拒臂）
garnet/libs/server/Resp/RespServerSession.cs:ProcessMessages（:662-666 Running 直通）与 ProcessOtherCommands（:1090 `RespCommand.HELLO => NetworkHELLO()`）
garnet/libs/server/Resp/AdminCommands.cs:ProcessAdminCommands（:65-69 ACL 族分派，无事务门）
garnet/libs/server/Resp/BasicCommands.cs:NetworkHELLO（无 TxnState 门）
garnet/libs/resources/RespCommandsInfo.json:HELLO/ACL 条目（Flags 无 NoMulti，双侧同源拷贝）

精炼执行方案：
1. 事务窗拒绝臂 err 选择器分命令收敛：is_acl_command(cmd) 命中时回 ACL 族专属文案（如 "ERR ACL commands are not allowed inside a transaction" 或对齐 §58a 先例改在排队期中止，二选一由审核按「对 C# 契约破坏最小」裁定并在 deviations 补登该偏差）；SELECT 冷库围栏（park_cold_context_load）换用 SELECT_IN_TXN 族文案，杜绝三域共用 HELLO 文案。
2. 无 AUTH 的 HELLO 重放窗执行面修复：process_hello_command_state 在无 AUTH 选项组时无存储点查需求（协议版本回显/server 名/客户端名均为会话元数据），评估在 dispatch_via_garnet_api 事务窗内对无 AUTH 形走同步快臂直通应答（不停车不点查），与 §58a「中止面恰等于携 AUTH 组」裁决面对齐；若评估认定须保持拒绝，则回排队期补中止使两面一致，并在 §58 追记勘误（现两态矛盾必须收敛为一态）。
3. 测试验证点：wnode/tests 新增锁测三形态——MULTI + HELLO 3（无 AUTH）+ EXEC 应答数组含正常 HELLO map（或按 2 的裁定收敛形态断言）；MULTI + ACL LIST + EXEC 不出现 "ERR HELLO" 文案；MULTI + SELECT 同库 + EXEC 冷库窄窗（装配期卸载映射后重放）文案为 SELECT 专属帧；既有 hello_test/acl_tests/requirepass_test 保持绿。

合入哈希：1c38ea8 收口形态：并行支 fix-execreplay 与 aa1655a 波零语义差收口——沙箱独立实现三锁验毕后全量取 dev 侧（重复锁面 txn_exec_replay_hello_acl_fence.rs 删除），仅存 txn_resp_commands.rs 排队/重放两面互为补集注记三行，合入后 check/锁面复验全绿
