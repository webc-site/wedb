park_exec_lock_wait 让渡重驱协议在脚本窗失效，redis.call 命令静默丢弃

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# LuaRunner 内嵌独立 processor 会话（脚本内 redis.call SELECT 不穿透外层连接域），且事务锁获取为网络线程内联自旋无让渡协议——「EXEC 重放窗内脚本换前缀 + 桶闩争用让渡」组合在 C# 结构性不可达，无对位分支。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
MULTI 内 EVAL 的 EXEC 重放段（TxnState::Running）脚本内 redis.call SELECT 热库直改 active_db_id（array_commands/mod.rs:410 热臂无事务围栏）→ 会话前缀异于 EXEC 展开锚 → 后续 redis.call 存储命令经 exec（garnet_api/exec.rs:70）txn_prefix_needs_rearm 真 → reexpand_for_generation_swap 桶闩争用返 false（transaction_manager.rs:405）→ park_exec_lock_wait（txn.rs:416-422）登记 pending_slow 空应答让渡体 + pending_rearm=true → 脚本窗 dispatch_resp（lua.rs:721-725）劫持让渡体入 script_suspend → 内层 process_messages（consume.rs:360-364）把 pending_rearm 消费在脚本请求缓冲游标上（重驱落空）→ resume_suspended_script 驱动让渡体得空应答 → 空拍回填 nil（违反 lua.rs:224「redis.call 返回值不得空拍回填」自陈契约；连带违反 txn.rs:414「单次尝试+执行器让步直至成功」与 exec.rs:69「整个事务重放期间始终持桶闩」）→ 读命令回幻象 nil、写命令丢失而事务照常 TxnCommit。
关联事实：task/done/wtxn-exec-relock-retry-stale-generation-prefix.md 未覆盖脚本角；c13-1 确认席（2026-10-05）定位。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
窄窗（MULTI+EVAL+脚本内 SELECT 热库+换代桶闩争用四条件叠加）但机制性成立：脚本内写命令静默丢失且事务提交语义照常，破坏原子性声明；读命令幻象 nil。

涉及代码：
rust 文件与函数：
wnode/src/resp/resp_server_session/txn.rs:park_exec_lock_wait（:416-422）
wnode/src/resp/garnet_api/exec.rs:exec（:69-70 rearm 判定）
wnode/src/resp/resp_server_session/lua.rs:dispatch_resp 让渡劫持臂（:721-725）与 resume 回填（:246-255）
wtxn/src/transaction_manager.rs:reexpand_for_generation_swap（:405 争用返 false）
wnode/src/resp/array_commands/mod.rs:network_select 热臂（:410 无事务围栏）
对应 c# 文件与函数：
garnet/libs/server/Lua/LuaRunner.cs（内嵌 processor 独立会话形态）
garnet/libs/server/Transaction/TransactionManager.cs:Run（内联自旋无让渡）

精炼执行方案：
1. 裁决修复方向（三选一）：a) 脚本窗内 park_exec_lock_wait 改挂起重驱语义——让渡体驱动完成后 resume 重放当前 redis.call（需区分「空应答重驱型」与「已应答型」挂起体，脚本挂起协议加面）；b) EXEC 重放窗内 redis.call SELECT 热臂加围栏回错误帧（前缀永不异锚，改脚本内 SELECT 语义，需对照内嵌 processor 形态评估用户可见面）；c) 脚本窗内 rearm 争用即中止脚本回错误帧（失败关闭，窗窄破坏性小）。
2. 落修复 + 回归测试（MULTI+EVAL+SELECT+换代争用四条件复现）。
3. 同族排查：pending_rearm 协议在其它非命令游标消费面有无同型错位。
