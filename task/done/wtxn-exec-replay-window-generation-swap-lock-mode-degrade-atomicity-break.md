终态注记: 已合入 main（commit: b0e09fc）。收口形态：TransactionManager 记录 EXEC 展开期物理前缀快照 lock_prefix；重放段现前缀异于锚前缀时触发 reexpand_for_generation_swap 对新增桶增量 try 闩，争用走 Contended 慢臂重驱，严禁降级 Basic 临时闩；新增 tests/exec_replay_generation_swap_lock.rs 跨代原子性与隔离回归测试。

甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-H，P2 级）。EXEC 重放窗内物理域换号（队内 FLUSHDB 或慢臂挂起窗并发换号）后，余下重放命令锁模式降级 SessionLocking::Basic 逐命令临时闩事实确证，破坏事务原子性与无他连接穿插契约。执行席遵照：TransactionManager 记录 EXEC 展开期物理前缀快照，重放段现前缀异于锚前缀时按现前缀增量补算哈希并入 key_entries 并在新增桶增量 try 闩，争用走 Contended 慢臂重驱，禁止降级 Basic。

原票面：
EXEC 重放窗内物理域换号后余下重放命令锁模式降级 Basic 逐命令临时闩，事务隔离与原子性破口（C# 换号不改锁身份恒免疫）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# 排队期 LockKeys（TxnRespCommands.cs:197 → TxnKeyManager.cs:82）即把键哈希经 TxnKeyEntries.AddKey（TxnKeyEntry.cs:84-106，GetKeyHash 为 store 裸键哈希）冻结进事务上下文，EXEC（NetworkEXEC → Run）按排序计划持锁直至 Commit/Reset 全程不放。C# FLUSHDB 可入队（无 no_multi 旗，AllowedInTxn 恒真），重放段经 ProcessMessages 的 Running 直通（RespServerSession.cs:664-666 → ProcessBasicCommands(transactionalApi)）真执行；而 C# FlushDatabase（libs/server/Databases/DatabaseManagerBase.cs:301-311）仅 ShiftBeginAddress 截日志、不换锁表身份，逻辑键到哈希映射跨清库恒稳——事务重放中途执行队内 FLUSHDB，既持桶闩与后续命令键哈希仍同一身份，他连接对同键的写在事务提交前恒被阻，事务原子性（EXEC 期间无他连接命令穿插）保全。
2. 工程现状确证（Rust 实现路径与代码缺陷）：rust 锁轨哈希含物理域代际（scoped_hash 前缀种子，wtxn/src/txn_key_entry_comparison.rs:46，前缀取 StoreSession::session_prefix 真值含换号态）；FLUSHDB/FLUSHALL 经 O(1) 秒级换号清库（wkv/src/vdb/flush.rs:11/49 swap_out + bump_generation，换号仅持 lock_dbmeta 串行锁、无 txn-active 守卫）。EXEC 起点一次性展开：register_run_preamble（wtxn/src/transaction_manager.rs:312-343）按当时物理前缀把排队裸键与 WATCH 键现算入 key_entries 并持桶闩，此后不再重算。队内 FLUSHDB 重放可达：process_transactional_command Running 直通（wnode/src/resp/resp_server_session/txn.rs:41-43）→ flush_db 回 Ok(false) 降级慢路径（wnode/src/resp/basic_commands/mod.rs:865-877）→ flush_command_slow 换号执行段（wnode/src/resp/garnet_api/slow.rs:1205-1258，尾端 set_active_db 刷新会话现域）→ 自此会话 session_prefix 代际已推进。余下重放命令的锁器选型点（wnode/src/resp/garnet_api/mod.rs:681-707 exec）调 txn_locks_cover_cmd（wnode/src/resp/resp_server_session/txn.rs:212-226）：按**现**前缀 scoped_key_hash 对 key_entries 持桶判 covers_user_keys（wtxn/src/txn_key_entry.rs:298-315，桶下标对拍）——现代际哈希落在旧代注册集外恒判不过 → SessionLocking::Basic（wkv/src/session/rmw_window.rs:218-223）逐命令自取本键桶闩、命令返回即 Drop 放闩（RmwWindow Drop，rmw_window.rs:251-258），事务对余下命令不再持任何跨命令闩。触发面两支：a) 确定性触发——队内 FLUSHDB/FLUSHALL（无 no_multi，排队臂不拒）排在前、写命令排在后；b) 竞态触发——重放段慢臂挂起让渡窗（compio 泵 probe_race）内他连接 FLUSHDB/FLUSHNS/SWAPDB 插入 bump_generation。文档把 covers 判不过的 Basic 自取闩框为「保守让调用窗口自取闩」（txn.rs:210-211 注），但对 Running 事务该降级非保守而是隔离丢失面。
3. 逻辑危害确证（并发正确性破口）：连接 A `MULTI; FLUSHDB; SET k v; SET k2 v2; EXEC`，重放段换号至代际 G+1 后 SET k/SET k2 均降级 Basic：连接 B 对 k 的 SET/GET 可在 A 的 SET k 与 SET k2 之间穿插生效（A 仅在每命令窗口内持闩），事务「EXEC 期间无他连接命令穿插」契约破口；终态可见 B 值覆盖 A 事务内写，或 A 事务读到他连接中途写——对照 C# 同场景 B 恒被事务闩阻至提交。次生面：TxnStart/TxnCommit 围栏向量按 EXEC 起点旧代哈希路由（transaction_manager.rs:480-516 compute_sublog_access_vector），换号后余下写的 AOF 记录按现代际哈希路由，多子日志拓扑下事务标记向量可能漏覆盖实际承载子日志，恢复期组协调错路由。严重度 P2：可观测的并发正确性破口，非纯性能；与在册 §115（WATCH 键并锁一臂）及 task/done/wtxn-multi-queued-lock-hash-stale-across-generation（排队期冻结哈希、EXEC 起点一次性重展开收口）均不同窗——该票收口 EXEC 起点单次展开，本票是展开之后重放窗内的换代，登记面无先例。

涉及代码：
rust 文件与函数：
wedb/wtxn/src/transaction_manager.rs:register_run_preamble（EXEC 起点一次性展开）、compute_sublog_access_vector
wedb/wtxn/src/txn_key_entry.rs:TxnKeyEntries::covers_user_keys（现域判桶）
wedb/wnode/src/resp/resp_server_session/txn.rs:txn_locks_cover_cmd（session_prefix 现取）
wedb/wnode/src/resp/garnet_api/mod.rs:GarnetApiFace::exec（push_session_locking 选型点）
wedb/wkv/src/session/rmw_window.rs:SessionLocking、RmwWindow（Basic 逐命令自取闩即放）
wedb/wnode/src/resp/basic_commands/mod.rs:flush_db（Ok(false) 慢路径降级）
wedb/wnode/src/resp/garnet_api/slow.rs:flush_command_slow（换号执行段）
wedb/wkv/src/vdb/flush.rs:VirtualDbManager::flush_db、swap_db（bump_generation）

对应 c# 文件与函数：
garnet/libs/server/Transaction/TxnRespCommands.cs:NetworkEXEC、NetworkSKIP
garnet/libs/server/Transaction/TxnKeyManager.cs:LockKeys、SaveKeyEntryToLock
garnet/libs/server/Transaction/TxnKeyEntry.cs:TxnKeyEntries.AddKey（GetKeyHash 裸键哈希）
garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase（仅截日志不换锁身份）
garnet/libs/server/Resp/RespServerSession.cs:ProcessMessages（Running → ProcessBasicCommands(transactionalApi) 直通）

精炼执行方案：
1. TransactionManager 增 EXEC 展开期物理前缀锚（register_run_preamble 记 lock_prefix 快照），run_exec/commit 间可读。
2. 重放段换号重展开单机制：garnet_api::exec 锁器选型点判 Running 且现前缀异于锚前缀时，不降级 Basic，改按 txn_keys 裸键以现前缀补算哈希并入 key_entries、按既有排序归并计划对**新增桶增量** try 闩（旧代已持桶不重复取、随 commit/reset 逆序释放不变），补锁争用走既有 ExecRun::Contended 慢臂让步重驱；TxnStart/Commit 围栏向量随补算哈希并入。
3. 测试验证点：a) MULTI 内 FLUSHDB+双 SET，他连接并发写同键，断言 EXEC 提交前被阻、终态等于事务串行结果；b) 重放慢臂窗内他连接 FLUSHDB 插入，余下重放命令仍持现代际桶闩至提交；c) covers 判据换号前后真值翻转锁测；d) txn_queue_lockset_residual / transaction_tests / watch_version_regression 全绿。
