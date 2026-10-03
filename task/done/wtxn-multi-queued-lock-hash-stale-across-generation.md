终态注记（Done）：
- 实施完成：
  1. `TxnKeysBuffer` 扩形支持记录 `SmallVec<[LockType; 16]>`，实现 `push(key, lock_type)`（自动升级排他锁与去重）、`is_read_only()`、`iter_with_lock()`。
  2. `save_key_entry_to_lock` 移除欺骗性 `prefix` 参数，排队期与单机/集群模式统一记录裸键与锁类型至 `txn_keys`，零冻结物理代际哈希条目。
  3. `register_run_preamble` 单点收敛：在 EXEC / run 展开期，统一按当前执行时刻最新的物理前缀 `lock_prefix` 现算哈希展开填入 `key_entries`，并在展开期置位 `perform_writes`，使排队写与 WATCH 键同一套前缀代际绑定机制，彻底杜绝换代失效。
  4. 测试全面覆盖：在 `watch_version_regression.rs` 增补排队写跨 FLUSHDB 换代加锁回归用例 `queued_write_cross_flushdb_locks_on_new_generation_bucket`；更新全部相关模块的单测与集成测试；修复测试中 `Result` 类型别名冲突与冗余注解。
- 验证结论：`cargo check --workspace --tests` 编译通过，零错误零警告。

审核结论：通过，严重度 P2 维持

分席勘误与确证（供 fix 直接消费）：
- 引注订正：C# 亦在排队期落锁（TxnRespCommands.cs:197 → TxnKeyManager.cs:82 → TxnKeyEntry.cs:88），非「仅 EXEC 才取哈希」；但 C# 免疫的真因是 GetKeyHash=store 裸键哈希不含库代际，且 FlushDatabase（libs/server/Databases/DatabaseManagerBase.cs:301）仅截日志不换锁身份，冻结哈希跨代恒稳。
- 换号闸确无：keyspace.rs:134-142、flush.rs:26-34 换号仅持 lock_dbmeta 串行锁，无 txn-active 守卫；MULTI 排队→EXEC 窗确可被他连接 FLUSHDB/SWAPDB 插入 bump_generation。
- §115 仅裁 WATCH 臂，排队命令臂系残留真缝，非重复立案；与 keybucket-desync 邻票正交。

优化执行方案：
1 network_queued 期 lock_keys 改为把（裸键字节, lock_type）记入扩形 TxnKeysBuffer，并单化现仅 cluster 才登记的半截形态。
2 EXEC 于 register_run_preamble 内 save_lock_hashes 之后按当前 lock_prefix 统一重展开排队键入 key_entries；perform_writes 判据移至展开期置位。
3 验证点：a) 排队含跨键命令（RENAME/MSET）换代后 EXEC 持现代桶闩、他连接同键写阻至提交；b) 换代后 sublog 访问向量按现域路由（AOF 回放对拍）；c) 非换代常规 MULTI 零开销回归（复用缓冲不劣化）；d) covers 判据在 EXEC 后重放段恢复 Transactional 让闩；e) DISCARD/ABORT 路径缓冲正确清空。

MULTI 排队命令锁轨哈希在排队期冻结物理代际，换号窗内 EXEC 隔离失效

问题分析：
1. Garnet 契约对齐（C# 原型行为）
C# 排队期（MULTI 内）命令不执行，仅缓存字节；SaveKeyEntryToLock/LockKeys 的真实调用点在命令执行/重放路径（MainStoreOps、ObjectStore 各 op、AofReplayCoordinator、CustomTransactionProcedure），即 EXEC 重放时才由各命令 op 现取哈希。键哈希在 TxnKeyEntry.AddKey（garnet/libs/server/Transaction/TxnKeyEntry.cs:88-90）内以 comparison.UnifiedTransactionalContext.GetKeyHash 当场派生。且 C# FlushDatabase（libs/server/Storage/Session/ObjectStore/../DatabaseManagerBase.cs:301）不改库或锁表身份，逻辑键到哈希映射跨清库恒稳，故 C# 形态无「排队哈希过期」问题。

2. 工程现状确证（Rust 实现路径）
rust 走双轨：WATCH 键锁轨在 EXEC 现算——TransactionManager::register_run_preamble（wedb/wtxn/src/transaction_manager.rs:348）以入参 lock_prefix（EXEC 时刻物理前缀）经 watch_container.save_lock_hashes（:358）对裸键重展开并入 key_entries（此臂已登记 doc/zh/deviations.md §115，锁面 wedb/wnode/tests/watch_version_regression.rs 的 swapnum_exec_locks_current_physical_domain 仅覆盖 WATCH 键一臂）。
但排队命令的锁 entry 在排队期即冻结：network_queued 于 write_queued 之前调 self.lock_keys（wedb/wnode/src/resp/txn_resp_commands.rs:330-333），TxnKeyManager::lock_keys（wedb/wtxn/src/txn_key_manager.rs:61）取排队时 session_prefix（:69），save_key_entry_to_lock（:41）当场以 TxnKeyEntryComparison::scoped_key_hash(prefix, key)（:44-45）算出哈希存入 key_entries。EXEC 期 register_run_preamble 不重算这批排队键。EXEC 重放各命令仅以 txn_locks_cover_cmd（wedb/wnode/src/resp/resp_server_session/txn.rs:212）拿当前 session_prefix（:224）比对 key_entries.covers_user_keys（:225）。
rust 自研的 O(1) 秒级换号清库（既定改良，见 task/review.md 板块 4.2）会在 FLUSHDB/FLUSHNS/SWAPDB 时 bump_generation（wedb/wkv/src/vdb/flush.rs:32/71/100、vdb/manager.rs:124/143），使物理前缀代际改变。

3. 逻辑危害确证
场景：连接 A 执行 MULTI; SET k1 v; SET k2 v; 排队期 k1/k2 锁哈希按代际 G 冻结；在 A 发出 EXEC 前，连接 B 对本库执行 FLUSHDB/SWAPDB 触发 bump_generation 至代际 G+1。A 的 EXEC 取闩落在按 G 算的旧代桶（保护的是已退役空域），而重放写命令按现值路由落在 G+1 现代桶；txn_locks_cover_cmd 判不过（现域哈希不在冻结集内），wkv 窗据此逐命令自取 ephemeral 闩，两重放命令之间闩即释放，连接 B 的写可在间隙穿插——EXEC 原子隔离被打破，多键中间态对外可读可写。compute_sublog_access_vector（transaction_manager.rs:531-533 依 key_entries.key_hashes 路由）亦按旧代哈希算 TxnStart/Commit 围栏向量，多子日志回放错路由。C# 形免疫（无代际、哈希运行期现取）。
严重度 P2：可观测的并发正确性破口，非纯性能。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/txn_resp_commands.rs:network_queued（lock_keys 调用点 :330-333）
wedb/wtxn/src/txn_key_manager.rs:lock_keys / save_key_entry_to_lock（:61/:41）
wedb/wtxn/src/transaction_manager.rs:register_run_preamble（仅 WATCH 臂 :348/:358）
wedb/wnode/src/resp/resp_server_session/txn.rs:txn_locks_cover_cmd（现域比对 :212/:225）

对应 c# 文件与函数：
libs/server/Transaction/TxnKeyManager.cs:LockKeys / SaveKeyEntryToLock
libs/server/Transaction/TxnKeyEntry.cs:TxnKeyEntries.AddKey / GetKeyHash
libs/server/Storage/Session/DatabaseManagerBase.cs:FlushDatabase

精炼执行方案：
1. 排队期锁登记不再冻结哈希，改存裸键字节 + lock_type（复用现有 TxnKeysBuffer 形态；is_read_only/perform_writes 判据不动）。
2. EXEC 期与 WATCH 键同点（register_run_preamble 内、save_lock_hashes 之后）按当前物理前缀一次性把排队裸键重展开进 key_entries，与 §115 收为同一单机制（非双轨）。
3. 测试验证点：在 swapnum 族补「排队 SET 臂」——换代后 EXEC 应持现代桶闩、并发写被阻至提交；并回归 txn_keybucket_scope_mutex 族与 watch_version_regression 全族。
