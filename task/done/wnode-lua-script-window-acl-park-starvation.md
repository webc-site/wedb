归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 6db82e2＋fmt 补稳 38766f2（P2），收口形态：dispatch_resp 环尾无副作用判据（acl_refresh_park_needed×空应答）写专属改权错误帧、文案单源 ScriptApiError::ACL_CHANGED_TEXT，快路新增 AclChanged 形态（host.rs vtable 随动）、acl_check_cmd 经 acl_mount_stale 预门改错，三消费面统一确定性窗内改权 Lua 错误；游标回退保存储零执行、pending_acl_refresh 不 take、EVAL 收尾泵即时刷新；§159 登记（顺编避开册尾 §158）。src 净 +88（行为收口口径已随提交申报）、锁测 +249 四例真驱动零假 mock、回归含 exec_replay §58d 保持绿。遗留申报两条（快路陈旧 bool 撤权方向早回 NOPERM、逐 call 协程重放全对齐另立票）不阻收口。

甄别结论：通过（甄别席 zc-fix-r25-乙，2026-09-26）定级 P2
核验记录：C# 亲验——ACLCommands.cs:226 `while (!userHandle.TrySetUser(...))` 共享句柄 CAS 换引现读亲见；LuaRunner.Functions.cs 逐条 redis.call 现判臂（acl_check_cmd/快路）锚在案，契约形态「改权后下一条即按新规则裁决」属实。rust 亲验——lua.rs dispatch_resp 窗口循环（:491-517 现读）仅水位/阻塞/慢路三让渡臂、无 ACL 刷新臂；core.rs:970-978 Parked 臂游标回退即 break（对脚本面坍缩为空应答连锁挡回）；redis.rs:606-611 空应答折 nil 亲见；acl_check_cmd 纯 bool 位图陈旧裁决面属实；auth.rs:96 acl_refresh_park_needed 现读判据可复用——现码无修复合入。审核订正（§97 折叠规则引用、忌 take 取走 pending 标志）已在票顶承接。查重：deviations 无脚本窗 ACL 条目；五池零同轴（reject wnode-blocked-wait 系 pubsub 排空轴他域、ing wlua 配置票正交）。架构：方案 1 以确定性错误中断收口、严禁窗口内同步 await 点查、逐 call 协程重放另立票评估——完全符合 compio thread-per-core 禁忌纪律（无 inline 驱动调度器），无权限逃逸已核。格式：纯文本、双侧齐全。定级 P2：韧性与契约面（nil 污染出错值、假协议文案误导排障、acl_check_cmd 反向裁决），无数据损坏无逃逸，依 r25 审核席判不升级。

审核结论：通过（r25 审核席，定级 P2；三形 fail-closed 无撤权续跑逃逸、假违规仅中断脚本不断连，无须升级。全锚亲验属实：lua.rs:491-517 窗循环无刷新臂、core.rs:970-978 Parked 空应答、redis.rs:606-607 折 nil、快路空应答落 Protocol 假违规、acl_check_cmd/auth.rs 纯读陈旧位图、AOF 回放 bump 可达、现码无窗内逐 call 现取通路（预门现读但只停车不刷新）；C# ACLCommands.cs:226 共享句柄 CAS + LuaRunner.Functions.cs 逐条 fresh 属实。五池无脚本窗 ACL 既裁、deviations 无同条、在办 wlua 配置票正交）。审核订正：Protocol 折叠规则系 §97 非 §111、§98 引注漂移；方案忌用 take 取走 pending 标志（会致 EVAL 收尾泵不再即时刷新），改无副作用判据。

审核裁定执行方案（供 fix 直接消费，替代文末原方案）：
1. dispatch_resp 消费环尾以无副作用判据（复用 acl_refresh_park_needed 现读）命中窗内停车时向 response 写专属 -ERR 帧，fallback 折 nil 与快路假违规统一收口为确定性 Lua 错误，游标已回退保存储零执行。
2. frame_and_acl_check 同判据成立时不回陈旧 bool，改报同文案错误，杜绝 acl_check_cmd 误报 true。
3. 订正 auth.rs:113/:509-511 口径注释；deviations 新条登记（Protocol 面正确引用 §97）。
4. 测试（仿 acl_tests.rs:98 harness）：fallback bump 后第二条断错误帧且键未落且 EVAL 收尾泵刷新；GET/SET 快路断非 PROTOCOL_TEXT；acl_check_cmd 断不复报 true；lua_script_tests 回归绿。
5. 逐 call 协程重放全对齐 C# 的更大改造另立票评估，本票不做。

脚本窗口内 ACL 改权停车饿死：并发改权后在飞脚本后续 redis.call 拿 nil 或假协议违规文案中断，与 C# 每条 redis.call 即时新句柄收敛分叉

问题分析：
1. Garnet 契约对齐：C# 的 ACL SETUSER 在共享 UserHandle 上 CAS 换引（garnet/libs/server/Resp/ACLCommands.cs:NetworkAclSetUser :226 while (!userHandle.TrySetUser(newUser, currentUser))，garnet/libs/server/ACL/UserHandle.cs:TrySetUser :49-56 Interlocked.CompareExchange）。会话与脚本面每次鉴权都经该共享句柄现读最新 User 引用：直令面 RespServerSession.cs:ProcessMessages :653 每命令 CheckACLPermissions（AdminCommands.cs:CheckACLPermissions :124-148，:139 读 _userHandle.User.CanAccessCommand）；脚本面 redis.call 无论回退重入 TryConsumeMessages（内嵌同会话门链，rust 侧 lua.rs:467-471 注释亦自陈对标 C# ProcessCommandFromScripting 尾部 TryConsumeMessages）还是存储 API 直连快路，均在调用点逐条现判（garnet/libs/server/Lua/LuaRunner.Functions.cs :2879/:2959/:2993 acl_check_cmd 与 fallback 解析臂、:3169 SET 快路、:3221 GET 快路）。即契约形态为：改权落在脚本执行中段，下一条 redis.call 即以新规则裁决——放行即照常执行、撤权即 NOPERM，脚本其余部分正常推进。
2. 工程现状确证：rust 侧句柄为会话本地挂载（Arc 快照），跨连接改权收敛唯一通道是命令入口预门（auth.rs:acl_refresh_park_needed :96-107 代数比较）停车后由网络泵 await 点查刷新（auth.rs:refresh_acl_mount_if_stale :114 起；admin_commands.rs:check_acl_permissions :143-148 置 pending_acl_refresh；drive.rs :237-249 泵刷新臂）。脚本窗口为同步上下文（wlua VM C 函数栈上），泵刷新臂不可达：每条 redis.call 经 lua.rs:dispatch_resp（:464-518）重入 core.rs:process_messages，确实再过同一预门，但代数一经落后即落 Parked 臂（core.rs:970-978：游标回退本命令起点、break），窗口循环仅备水位/阻塞/慢路三让渡臂、无刷新臂，停车对脚本面坍缩为「本条空应答且后续每条 redis.call 连锁挡回直至脚本结束」——此连锁形态恰为 lua.rs:487-489 注释自陈知晓的 hazard。三消费面各出异象：其一，fallback 形（redis.rs:dispatch_scripting_command_fallback :580-615）空应答经 :606-611 is_empty 折成 0 返回值，redis.call 回 nil，脚本以 nil 续跑，产出错值或撞 Lua 运行时算术错；其二，SET/GET 快路（redis.rs:418-498/:502-576，经 lua.rs:528-544 同样重入 dispatch_resp）空应答经 wresp/src/read.rs:parse_bulk_reply :482-498 / parse_simple_reply :503-509 落 Malformed，ScriptApiError::Protocol 按 §111 在册折叠规则上抛（redis.rs:487-493/:573-576），脚本以「协议违规」误导性文案整体中断——并发改权（含 AOF/repl 回放 ACL 条目亦 bump 代数，aof_processor_store_ops.rs:112-120/:412-424）被伪装成协议故障；其三，redis.acl_check_cmd（redis.rs:frame_and_acl_check :312-341，:338 走 lua.rs:561-563 纯 bool 位图，不重入门链）在落后挂载上恒给陈旧裁决（撤权后仍回 true），与 C# :2993 现读新句柄反向分叉。auth.rs:113 与 :509-511 自陈「经外层脚本命令预门刷新后的同一句柄判定，与直令面同口径、无第二套判据」——该断言仅覆盖 EVAL 入口前改权，入口后窗口内 bump 不可刷新，口径承诺在窗口面为假。
3. 逻辑危害确证：无权限逃逸（快路 bool 预检虽用陈旧快照放行，实际执行仍被 dispatch_resp 重入的 Parked 臂挡回、零陈旧执行），危害为韧性与契约面：其一，任何并发 ACL 写（运维 SETUSER/DELUSER、副本 AOF 回放条目）命中在飞脚本即致该脚本后续全部命令静默失效——fallback 形以 nil 污染脚本逻辑并向客户端返回基于 nil 的错值，快路形以假协议违规文案中断，两侧均与 C#「下一条 redis.call 即按新规则正常执行/NOPERM」可观测分叉；其二，acl_check_cmd 在陈旧挂载上给出与引擎真源相反的布尔裁决，脚本以其分权限的分支逻辑双侧反向；其三，错误文案错位（改权并发伪装为协议违规）误导排障。生产可达：长脚本（含纯 Lua 计算循环 × 偶发 redis.call）与策略同步期的 SETUSER 批量回放同刻即触发。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/resp_server_session/core.rs:RespServerSession:process_messages（门链 :970-978 Parked 臂回退即断）
wedb/wnode/src/resp/admin_commands.rs:RespServerSession:check_acl_permissions（:143-148 预门停车置位）
wedb/wnode/src/resp/resp_server_session/auth.rs:acl_refresh_park_needed（:96-107）、refresh_acl_mount_if_stale（:114 起，仅泵 await）、口径注释（:113、:509-511）
wedb/wnode/src/resp/resp_server_session/lua.rs:RespScriptingApi:dispatch_resp（:464-518 窗口循环无刷新让渡臂）、get/set 特例（:528-544）、ScriptingApi::check_acl_permissions（:561-563 纯 bool）
wedb/wnode/src/net/handler/drive.rs:drive_loop（:237-249 刷新臂仅命令边界可达；:275-285 脚本续跑臂在其后）
wedb/wlua/src/functions/redis.rs:process_command_from_scripting（:618-637）、try_fast_path_set/get（:418-498/:502-576）、dispatch_scripting_command_fallback（:580-615，:606-611 空应答折 nil）、frame_and_acl_check（:312-341，acl_check_cmd 布尔面）、settle_script_call（:644-654）
wedb/wresp/src/read.rs:parse_bulk_reply（:482-498）、parse_simple_reply（:503-509）
wedb/wnode/src/aof/aof_processor_store_ops.rs:store_upsert/store_delete ACL 臂（:112-120/:412-424，bump 源含回放）

对应 c# 文件与函数：
garnet/libs/server/Resp/RespServerSession.cs:ProcessMessages（:653 每命令现读共享句柄，脚本重入同门）
garnet/libs/server/Resp/AdminCommands.cs:CheckACLPermissions（:124-148，:139 现读 _userHandle.User）
garnet/libs/server/Resp/ACLCommands.cs:NetworkAclSetUser（:226 共享句柄 CAS 换引）
garnet/libs/server/ACL/UserHandle.cs:TrySetUser（:49-56）
garnet/libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting（:2879/:2959/:2993 fallback 与 acl_check_cmd 逐条现判、:3169 SET 快路、:3221 GET 快路）

精炼执行方案：
1. 窗口 Parked 收口为确定性显式中断（最小闭环，先落地）：dispatch_resp 消费段感知本轮 ACL 停车（try_consume 后 take_pending_acl_refresh 命中且无让渡标记）时，不再以空应答出窗，改向 response 写专属错误帧（文案如 -ERR ACL configuration changed during script execution, please retry the script），三消费面随之统一以 Lua 错误中断当前脚本；杜绝 fallback 形「nil 静默续跑出错值」与快路形「假协议违规文案」两态扰乱。该形态相对 C# 逐条 fresh 收敛的差距（并发改权中止在飞脚本 vs C# 续跑）按在册化流程登记 deviations（架构根因：会话本地挂载 + 同步窗口不可 await，与 §98 会话本地句柄裁决同谱）。
2. 同窗口面顺带修正：frame_and_acl_check（acl_check_cmd）在同一停车判据（acl_refresh_park_needed）成立时不复用陈旧布尔，改回同文案错误（不谎报陈旧裁决）；auth.rs:113 与 :509-511「与直令面同口径、无第二套判据」注释按窗口实况订正（口径仅在命令边界闭环成立，窗口内为中止语义）。
3. 完整对齐备选（评估后择一，不在本票强推）：为 ScriptYieldTag 增 ACL 刷新形，窗口停车即 park_script_suspend 让渡（游标已回退，命令不丢），drive.rs 既有「刷新臂 :237 先于续跑臂 :275」次序天然承接刷新，resume 侧需补「同点重跑当前 C 函数、不喂转换值」协程语义令重放的 redis.call 以刷新后挂载 fresh 重评，达 C# 逐条收敛；若重放语义评估不可行，回退方案 1 收口。严禁给窗口内同步 await 点查开闸（存储点查禁同步收割红线）。
4. 测试验证点：wnode/tests 新增锁测三形态（泵 harness 仿 acl_tests.rs:98 刷新臂循环）——其一，ACL 档已认证会话 EVAL，脚本内两条 fallback redis.call，第一条消费后经 AclStore 写出口 bump 代数（模拟跨连接 SETUSER），断言第二条拿确定性「规则已变」错误帧、脚本中断、存储面零执行（键未落）、EVAL 收尾后泵刷新完成、下一笔外层命令按新权限 fresh 裁决；其二，脚本内 redis.call('GET')/redis.call('SET') 快路形同 bump，断言错误文案非 PROTOCOL_TEXT 协议违规形；其三，redis.acl_check_cmd 形在 bump 后不复报陈旧 true。既有 acl_tests/requirepass_test/lua_script_tests/redis_call_fast_path 保持绿（§111 Protocol 折叠语义仅收缩至真协议损伤面）。
