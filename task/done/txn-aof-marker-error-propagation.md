AOF 事务标记入队失败改错误透传（承接 next/qcode.db.md 条 25，MED）

现状
- wedb/wtxn/src/transaction_manager.rs:67 trait TxnAofLog，:77 enqueue_txn、:85 enqueue_stored_proc
  签名返回 ()（无错误通道）。
- wedb/wnode/src/aof/garnet_log/mod.rs:343/:362 GarnetLog impl：底层返回 Err 时
  :356/:379 仅 `log::error!` 吞掉（注释自述「trait 签名 ()：wtxn 事务状态机无法回滚已开的标记序」）。
- 底层 wedb/wnode/src/aof/garnet_log/single_log_branch.rs:399 enqueue_txn、:307 enqueue_stored_proc
  实际返回 waof::Result（有真实失败面：背压/入队错误）。
- 恢复裁决 wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs:266
  add_or_replay_transaction_operation 仅在 TxnCommit 时提交事务组，无 Commit 的组随回放结束静默丢弃。
- 调用面 transaction_manager.rs:412 run 侧 TxnStart、:427 commit 侧 TxnCommit、:588 log_proc。
- 危害链：commit 的 TxnCommit 入队失败被吞 → 主库事务已生效且 EXEC 已 ACK → AOF 尾部残留
  TxnStart + 组内操作而无 TxnCommit → 恢复回放静默丢组 → 已确认事务重启后消失（持久性缺口）。

C# 参考
- garnet/libs/server/Transaction/TransactionManager.cs:390-400 Commit：直接
  `appendOnlyFile.Log.EnqueueTxn(AofEntryType.TxnCommit, ...)`，无 try/catch，异常沿 EXEC 响应传播；
  :513-516 附近 TxnStart 同形态（run 侧）。
- garnet/libs/server/AOF/GarnetLog.cs:1051 EnqueueTxn 底层 log.Enqueue 失败抛异常。

优先级
功能缺口（已确认事务的重启后持久性缺失）。

方向
- trait TxnAofLog::enqueue_txn/enqueue_stored_proc 返回类型改为 Result（wtxn 域错误或
  waof::Result<()> 透传）。
- GarnetLog impl 去掉 log::error! 吞错，改透传底层 waof::Result。
- wtxn run/commit/log_proc 传播失败：commit 失败时 EXEC 响应返回错误（对齐 C# 异常传播），
  run 失败按现有 lock 失败路径 reset 返回 false；同步修订 trait 注释中「签名 ()」的自述。
- 验收：注入入队失败（背压闸门拒绝）后 EXEC 返回错误；AOF 中无「有 TxnStart 无 TxnCommit」
  的残缺组；客户端可感知未确认。

串行依赖与撞车
- 与 task/ing/txn-aof-marker-session-wiring.md 同域异面：该票拥有会话装配注入（session_dependencies.rs /
  service.rs session_dependencies / attach_transaction_components，把 aof_log 由恒 None 改注入真实
  GarnetLog），不改 trait 形态；本票只改 trait TxnAofLog 签名（wtxn 域）与 GarnetLog impl 吞错点
  （garnet_log/mod.rs），两单文件不交叉。
- 前置：本票「入队失败面」在会话注入真实后端后方为生产可达（注入缺席时 enqueue_txn 生产零调用），
  与 session-wiring 票及其前置 next/wtxn-aof-log-dyn-backend.md（分支 wave6-a-wtxn-aof-log，trait 去 <L>
  泛型收敛）协同排期；后端形态收敛定名以该前置条为准，勿双改 trait 形态。

细化方案（2026-09-19 实现前核实，行号以当前 dev 为准）
- 甄别通过：dyn-backend 已落地，trait 已收敛 `Arc<dyn TxnAofLog>` 单一 dyn 形态
  （transaction_manager.rs:270），本票只改方法返回类型，不动 trait 装配形态。
  吞错点仍在 mod.rs:361/:384；底层 single_log_branch.rs:267/:359 返回 waof::Result<i64>。
  C# 参考核实无误：TransactionManager.cs:395/:516 直接入队无 try/catch，
  GarnetLog.cs:1051 EnqueueTxn 底层 Enqueue 失败抛异常。
- 错误类型选型：wtxn 加 waof workspace 依赖，trait 两方法返回 waof::Result<()>。
  理由：TxnAofLog 是 AOF 日志抽象，错误面即日志入队错误，waof::Error 一处定义
  零包装透传（rust_review「依赖库错误 transparent 转发」）；wtxn→waof 无循环依赖。
- wtxn 侧传播：
  - enqueue_txn_marker / log_proc 返回 waof::Result<()>（内部 ? 上抛）。
  - run：TxnStart 入队失败 → 对齐现有 lock 失败收尾（reset + !internal_txn 时
    watch_container.reset）→ 返回 false。EXEC 层并入既有 null-array 未确认路径
    （C# 异常的 bool 投影；TxnStart 未落盘，无残缺组产生）。
  - commit → 返回 waof::Result<()>；TxnCommit 入队失败同样先做完整状态收尾
    （watch reset + reset 释放锁）再返回 Err——rust 无 C# session GC 兜底，
    显式收尾防锁泄漏，客户端经 EXEC 响应感知未确认。
  - run_transaction_proc：log_proc 失败 → 跳过 commit、ran=false（现有 !ran
    分支已保证 reset + finalize 照跑，对齐 C# 异常跳过 Commit + finally Finalize）；
    commit 失败 → ran=false（主段已生效但客户端收 error，对齐 C#）。
  - TransactionGuard::dispose：Drop 语义无法传播，失败 log::error! 兜底
    （internal 提升路径，生产暂零显式调用面，仅测试用）。
- wnode 侧：txn_resp_commands.rs network_exec Running 态收尾（:162）commit 失败 →
  session.write_error(RESP_ERR_TRANSACTION_FAILED)，其余收尾照做。
  GarnetLog impl 去掉两处 log::error! 吞错与「trait 签名 ()」自述注释，改
  .map(|_| ()) 透传。
- 不越界：恢复裁决 aof_replay_coordinator.rs:266 不动（丢无 Commit 组是恢复侧
  安全方向裁决，本票消灭产生侧残缺组）；session_dependencies 接线归姊妹票。
- 测试面核实：wnode/tests 均直调底层 GarnetLog::enqueue_txn/enqueue_stored_proc
  （aof_replay.rs:626、aof_stored_proc_replay.rs:75），签名不变，无破坏。


