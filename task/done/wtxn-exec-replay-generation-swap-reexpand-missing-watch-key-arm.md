锁定注记（2026-10-01 r8 波主控，基线 7397ed1；wtxn 只读甄别席候选 + 主控现码亲验复跑全实，行号以本注记为准）：
- rust 病灶现位：transaction_manager.rs:361 reexpand_for_generation_swap（函数体 361-377）——
  new_entries 仅由 self.txn_keys.iter_with_lock() 构造（:363-369），全程不触 watch_container；
  WATCH 键入锁集的唯一站点是 register_run_preamble（:321-356，:334-336 save_lock_hashes(lock_prefix) → add_key Shared），
  而该前置由 run_exec 的 exec_lock_armed 门（:271-277）只跑一次，重放窗换代后不再重跑。
- 调用点现位：garnet_api/mod.rs:692 → txn.rs:245-254 传入新物理前缀；scoped_key_hash 锁轨种子口径见 :348。
- C# 对位：TxnWatchedKeysContainer.cs:97-108 SaveKeysToLock → TransactionManager.cs:470-521 Run 臂 AddKey →
  TxnKeyEntry.cs:88-104 GetKeyHash(裸键) 入集并持至 Commit/UnlockAllKeys；C# 锁轨无代际种子，
  故「换代即锁桶身份变化须全量键重展开」为 rust 双轨制（§115）自有义务，判据落仓内自陈契约。
- 前案边界（查重已核）：done/wtxn-exec-replay-window-generation-swap-lock-mode-degrade-atomicity-break.md
  方案原文只收口 txn_keys 一臂（「按 txn_keys 裸键补算哈希」），WATCH 臂未入；
  done/wtxn-multi-queued-lock-hash-stale-across-generation.md 裁的是排队键臂；§115 登记的是 EXEC 起点臂双轨口径。本票为新漏臂，非并案。
- 禁触域（同侪在途）：wedb/wedb/src/server/replication/**、wedb/wnode/src/resp/mod.rs、
  wedb/wnode/src/resp/resp_server_session/mod.rs、wedb/wnode/src/storage/session/common/ttl_sync.rs；
  本票只动 wedb/wtxn/src/transaction_manager.rs 与其 tests/。

审核结论：通过（2026-10-01 主控亲验立案；P2。触发前提需 standalone 集群关 + WATCH 非空键 + MULTI 内阻塞命令把 EXEC 挂进重放窗 + 窗内 SWAPDB/FLUSHDB 类换号，四件齐备方可达；后果为 WATCH 栅栏在他连接侧失效的脏读窗，危害有界但语义破口真实）

EXEC 重放窗换代重展开只补排队键漏 WATCH 键臂，被监视键新代桶无闩他连接可即时改写

问题分析：
1. Garnet 契约对齐：C# WATCH 键在 Run 起手经 SaveKeysToLock 全量入 keyEntries 并持锁至事务终局（TransactionManager.cs:470-521），
   锁轨身份与物理代际无关，故不存在「换代后监视键锁集需重算」的义务面；rust 采 §115 双轨制，
   锁轨哈希以 scoped_key_hash(物理前缀, 裸键) 现算（transaction_manager.rs:348），SWAPDB/FLUSHDB 换号后
   同一裸键落在不同桶——重展开若不重算 WATCH 键，旧代闩对新代写完全不具阻挡力。
2. 工程现状：reexpand_for_generation_swap（:361）只遍历 txn_keys（:363-369），WATCH 键不在 txn_keys 内
   （cluster 关时更是从不并入：txn_resp_commands.rs:173-183 的 save_keys_to_key_list 推入仅 cluster_enabled 分支），
   其入锁集只发生在 register_run_preamble（:334-336），而该臂被 run_exec 的 exec_lock_armed 门（:271-272）
   挡为一次性——重放窗内换代时 armed 已为真，永不重跑。故新代桶集合内不含被监视键。
3. 逻辑危害确证：最小复现（standalone）——A: WATCH w（w 有值）; MULTI; BLPOP empty 0（EXEC 挂入重放窗，
   w 以旧代哈希持 Shared 闩）；B: SWAPDB 0 1；B: SET w new2（命中新代桶，本事务无闩在此桶，即时成功并 bump 版本）；
   A: 重放续跑，BLPOP 得手后队列内 GET w 读到 new2。WATCH 版本校验只在取锁后一次性执行
   （finish_run_postlock，:283 起），换代后不复核，栅栏名存实亡。C# 同场景 B 的写会被持闩阻塞到 A 终局。定 P2。

涉及代码：
rust 文件与函数：
wedb/wtxn/src/transaction_manager.rs:361 reexpand_for_generation_swap（漏臂点，new_entries 构造域仅 txn_keys）
wedb/wtxn/src/transaction_manager.rs:321 register_run_preamble（WATCH 键入锁集单点，:334-336）
wedb/wtxn/src/transaction_manager.rs:271 run_exec（exec_lock_armed 一次性门，使前置不重跑）

对应 c# 文件与函数：
libs/server/Transaction/TxnWatchedKeysContainer.cs:97 SaveKeysToLock
libs/server/Transaction/TransactionManager.cs:470 Run（AddKey 持锁至终局）
libs/server/Transaction/TxnKeyEntry.cs:88 GetKeyHash（C# 锁轨无代际种子的对位证据）

精炼执行方案：
1. reexpand_for_generation_swap 内构造 new_entries 处并入 WATCH 臂：以 new_prefix 调
   watch_container.save_lock_hashes(new_prefix)，条目按 LockType::Shared、恒不置 perform_writes
   （与 register_run_preamble 非 internal 路径完全同口径，一处定义两处共用，禁另造第二套哈希算法），
   与 txn_keys 条目一并走 try_lock_incremental_entries（旧代已持桶由该增量语义自然去重）
2. 只动 transaction_manager.rs 一个函数；禁改 run_exec 门控位语义、禁触 wnode resp 面与 §115 双轨口径注释
3. 锁测：扩 wedb/wtxn/tests/exec_replay_generation_swap_lock.rs 补 WATCH 臂用例——
   旧代入锁后 reexpand 至新前缀，断言新代桶上他连接 try_lock_exclusive 失败（即监视键确已持闩）；
   revert-proof：撤臂后该断言转红
4. 验证面：cargo check -q -p wtxn --all-targets 与 cargo nextest run -p wtxn 定向

---

## 终态注记
- **合入哈希**：`f48873b`（cherry-pick 自 `928b1c1`）
- **收口形态**：
  1. 在 `wedb/wtxn/src/transaction_manager.rs` 的 `reexpand_for_generation_swap` 构造 `new_entries` 时并入 `watch_container.save_lock_hashes(new_prefix)`，条目按 `LockType::Shared`、`perform_writes=false` 统一口径接入，与 `txn_keys` 一并走增量锁桶展开。
  2. 在 `wedb/wtxn/tests/exec_replay_generation_swap_lock.rs` 补充锁表回归用例，确证换代重展开后新代桶持有 Shared 锁、他连接独占加锁被拒；完成 revert-proof 检验（撤 WATCH 臂测试 100% 转红）。
- **门禁验证**：`cargo check -p wtxn --all-targets` 与 `cargo test -p wtxn` 全部通过。

## 主控全量复核（反证式审计，2026-10-01）

- **落点与票面同形**：`wtxn/src/transaction_manager.rs::reexpand_for_generation_swap` 在 `txn_keys` 展开之前先走
  `watch_container.save_lock_hashes(new_prefix)` 构 `TxnKeyEntry::new(hash, routing_hash, LockType::Shared)`，
  与 `register_run_preamble` 非 internal 臂**同源同口径**（同一 `save_lock_hashes` 单点、恒 Shared、不置 `perform_writes`），
  票面「一处定义两处共用、禁另造第二套哈希算法」守住；票面「禁改 `run_exec` 门控位语义」亦守住（本次 diff 未触 `run_exec`）。
- **增量去重与旧闩保留**：新条目一律经既有 `key_entries.try_lock_incremental_entries` 入集，票面点名的
  「旧代已持桶由增量语义自然去重」形态未变；`with_capacity(self.txn_keys.len())` 少算了 WATCH 臂条数，
  `SmallVec<[TxnKeyEntry; 8]>` 自动增长，非缺陷（容量提示，非上界）。
- **旧代闩泄漏面**（本票唯一真实风险）：重展开后旧代桶闩仍由本事务持有、至 `commit`/`reset` 的
  `unlock_all_keys` 才释放。新册 `test_watch_key_reexpand_on_generation_swap_holds_new_bucket_latch`
  已把「重展开后旧代闩保持」与「提交后新旧两代闩均释放」双双断言在内，即该泄漏路径有锁测覆盖，非未验面。
  旧代桶属退役物理域（无新写入），多持一段窗不构成对活租户的额外阻塞，判定可接受、不再开票。
- **反证 #5（撤臂三例全转红）**：席沙箱（`.forks/audit-itembroker` @ `f48873b`，私有 target）删除该 3 行臂后单跑新册：
  `…_holds_new_bucket_latch` 红于册内「新代桶上他连接 try_lock_exclusive 失败（确已持闩）」断言、
  `…_both_reexpanded_on_generation_swap` 与 `…_contention_rolls_back_and_preserves_old_locks` 同批转红（3 FAIL / 3 例），
  证伪「注释式覆盖」。撤改 `git checkout --` 归还后复跑 3/3 绿。日志 `.bench_run/audit-replay.log`、
  `.bench_run/audit-replay-nc.log`。
- **交付面统计**：`f48873b` 仅 `transaction_manager.rs` +5/-1 与新册 +217（净 +221，属补锁测的被授权增行）；
  `c4fb8de`/`739d5a2` 为归档与 fmt 补笔，零源码语义改动。
