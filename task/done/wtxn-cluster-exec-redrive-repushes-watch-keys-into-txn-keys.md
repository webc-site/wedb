锁定注记（2026-10-01 r8 波主控，基线 7397ed1；wtxn 只读甄别席候选 + 主控现码亲验复跑全实，行号以本注记为准）：
- rust 现位：txn_resp_commands.rs:173-183 集群臂 `if !self.is_exec_lock_armed() { for key in watch_container.save_keys_to_key_list() { txn_keys.push(key, Shared) } }`
  确在 :205 run_exec 之前；armed 位在 run_exec 内 barrier 成功时才置真（transaction_manager.rs:271-277），
  Contended 分支（:210-215 park_exec_lock_wait）不置位 ⇒ 每一重驱轮再推一遍全量 WATCH 键。
  txn_keys 推入无去重（wtxn/src/txn_keys_buffer.rs:94 顺序追加）。
- 每轮还附带代价：verify_cluster_txn_keys（txn.rs:394-432 区段，调用点 txn_resp_commands.rs:185-193）吃整份膨胀键集重算槽校验。
- C# 无对位：NetworkEXEC（libs/server/Transaction/TxnRespCommands.cs:40-100）同步取锁一次成功即入锁集，
  无 Contended 重驱形态，SaveKeysToKeyList 一次调用一次消费；判据落仓内自陈契约——
  txn_resp_commands.rs:202-204 注释明言「争用则键集原样保留、登记既有唯一慢臂（单次让步 + 重驱本 EXEC）」，
  即重驱轮须幂等，而 :174 的 armed 门与 push 时序错位。
- 前案边界（查重已核）：done/wtxn-queued-keys-push-dedup-quadratic-scan.md 裁的是排队键 push 去重（性能面），
  与本「重驱轮 WATCH 键重复推入」不同臂；task 池 grep exec_lock_armed / save_keys_to_key_list 零命中。
- 禁触域（同侪在途）：wedb/wedb/src/server/replication/**、wedb/wnode/src/resp/mod.rs、
  wedb/wnode/src/resp/resp_server_session/mod.rs；本票只动 transaction_manager.rs（位 + reset）与 txn_resp_commands.rs 门判两处及其 tests/。

审核结论：通过（2026-10-01 主控亲验立案；P3。影响面为单连接内存/CPU 随排空轮次线性退化，正确性不破（Shared 闩重复 add_key 幂等），故不上抬）

集群 EXEC 取闩争用每轮重驱都把 WATCH 键重复推入 txn_keys，排空窗内键集无界膨胀

问题分析：
1. Garnet 契约对齐：C# EXEC 无「争用让核重驱」这一层，SaveKeysToKeyList 只在 Run 起手消费一次
   （TxnRespCommands.cs:40-100 同步取锁）；rust 为重驱幂等另设 armed 门，但该门只覆盖取锁前置、
   未覆盖键集并入这一侧，等价义务漏半臂。
2. 工程现状：并入门判 is_exec_lock_armed()（txn_resp_commands.rs:174）读的是「本轮是否已成功起锁」，
   而 push 动作在起锁之前；Contended 时 armed 仍 false（run_exec 于 transaction_manager.rs:273 直接返回 Contended，
   :276 置位不可达），下一轮 rearm 重驱再次进入同一分支重复 push。
3. 逻辑危害确证：集群开 + WATCH N 键 + MULTI 后 EXEC 首撞屏障（同 worker 他事务持闩）⇒ 每次重驱 txn_keys
   长度 +N；屏障排空窗可持续整段慢臂周期（§115 慢臂登记面），长窗下同一连接键集线性膨胀，
   且每轮 verify_cluster_txn_keys 代价随轮次放大（O(轮次×N) 重复哈希与槽查）。纯资源退化不涉正确性，定 P3。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/txn_resp_commands.rs:174 集群臂 WATCH 键并入（幂等门与 push 时序错位点）
wedb/wtxn/src/transaction_manager.rs:271 run_exec（armed 置位时机）
wedb/wtxn/src/txn_keys_buffer.rs:94 push（无去重的顺序追加）

对应 c# 文件与函数：
libs/server/Transaction/TxnRespCommands.cs:40 NetworkEXEC（同步取锁一次性消费，无对位缺陷，语义对账锚）

精炼执行方案：
1. 幂等门改判独立位：TransactionManager 增 watch_merged_into_txn_keys（随 reset 清除），
   :174 改判该位、push 循环后置真——即「并入过」与「起锁成」两事分离，杜绝重驱轮重复推入；
   禁改用 armed 位打补丁（armed 语义是取锁前置，混用会破 §115 双轨自陈口径）
2. 只动上述两处 + 位定义与 reset；禁触 resp/mod.rs 派发面与慢臂 park 机制
3. 锁测：wtxn/tests 补集群形态连续 Contended 重驱 3 轮的用例，断言 txn_keys.len() 不随轮次增长
   （WATCH 键只计一份）；revert-proof：撤独立位后断言转红
4. 验证面：cargo check -q -p wtxn -p wnode --all-targets 与定向 nextest

---

## 终态注记
- **合入哈希**：`8ed5630`（cherry-pick 自 `9370603`）
- **收口形态**：
  1. 在 `TransactionManager`（`wedb/wtxn/src/transaction_manager.rs`）引入独立标志 `watch_merged_into_txn_keys`（在 reset 时复位），解耦 WATCH 键并入与起锁状态（`exec_lock_armed`）。
  2. `wedb/wnode/src/resp/txn_resp_commands.rs:174` 集群臂改判 `!watch_merged_into_txn_keys` 并在 push 循环后置为 true，杜绝屏障与取锁争用重驱轮次重复追加 WATCH 键致 `txn_keys` 线性膨胀。
  3. 在 `wedb/wtxn/tests/exec_run_async_arm.rs` 与 `wedb/wnode/tests/transaction_tests.rs` 补充连续 3 轮争用重驱锁测，确证 `txn_keys` 长度与内容不膨胀，revert-proof 检验有效。
- **门禁验证**：`cargo check -p wtxn -p wnode --all-targets` 通过，`wtxn` 与 `transaction_tests` 全部单测通过。

## 主控全量复核（反证式审计，2026-10-01）

- **落点与票面同形**：`wtxn/src/transaction_manager.rs` 新增独立位 `watch_merged_into_txn_keys`（文档锚 C#
  `TxnRespCommands.cs:40 SaveKeysToKeyList` 单次消费契约），构造与 `reset()` 同步复位；
  `wnode/src/resp/txn_resp_commands.rs` 集群臂由 `!is_exec_lock_armed()` 改判该位并在推入后置 true——
  与票面「并入门控与起锁态解耦」逐字对应，未顺带改 `run_exec` 的 armed 语义。
- **复位面穷举（本票唯一风险位）**：`watch_merged_into_txn_keys` 只在 `network_exec` 集群臂置位，
  而 EXEC 的四条出口全部经 `TransactionManager::reset()` 清零——提交（`commit`）、
  槽校验失败与中止应答（`reset_txn_none`）、`finish_run_postlock` 的两条 `false` 臂（WATCH 版本冲突 /
  TxnStart 入队失败，均在内部先 `self.reset()` 再回 `ExecRun::Aborted`，故 `end_session_txn` 不清管理器不属漏项）、
  DISCARD（`reset_txn_none`）。剩余唯一「清 `txn_keys` 不清新位」站点是 `network_unwatch`，
  但其前置 `state == TxnState::None` 只在上述 reset 之后可达，新位此时恒已为 false——**无泄漏路径**。
- **反证 #4（撤解耦必转红）**：席沙箱内把判据回改成 `!self.is_exec_lock_armed()` 单跑
  `wnode::transaction_tests network_exec_barrier_contended_retry_watch_keys_do_not_inflate` →
  转红于 `transaction_tests.rs:1556`（连续 3 轮重驱键集随轮次增长）；其余 14 例全绿，`wtxn::exec_run_async_arm` 4/4 仍绿
  （该册只测位语义，不测重驱膨胀）。撤改后 `git checkout --` 归还，沙箱除 `.cargo/config.toml` target 覆盖外干净。
- **沙箱复跑**（`--all-features`，日志 `.bench_run/audit-wtxn-redrive.log`）：`exec_run_async_arm` 4/4、
  `transaction_tests` 22/22，无 skipped、无 error。
