甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-E，P2 级）。EXEC 重放段挂起被 CLIENT KILL 或对端断连竞速废弃时，RespServerSession::dispose 对已落 TxnStart 的事务不投任何终结符收口事实确证，导致 AOF 产生孤儿残组、副本组缓冲滞留、重启后丢可见写。执行席遵照：dispose 与泵三臂 Disposed 废弃时对 Running 事务补投显式 TxnAbort 终结符，接通 waof 既有 AofEntryType::TxnAbort(0x22) 与协调器已闭环的 TxnAbort 弃组分支。

原票面：
EXEC 重放段挂起被终止广播或对端断连竞速废弃时事务无终结符收口：TxnStart 已入队而 TxnCommit 永不再达，恢复期整组静默弃置令已生效写在重启后丢失、主副本活态分叉

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# 的 EXEC 重放（libs/server/Transaction/TxnRespCommands.cs NetworkEXEC，回退光标逐条重放排队命令后 txnManager.Commit）全程在单网络线程内同步执行，批中途不可被外部打断——CLIENT KILL 经 GarnetTcpNetworkSender.cs:TryClose 的 socket.Close() 只令后续读失败，对正在执行的重放与内联 BlockingWait（libs/server/Resp/Objects/ListCommands.cs:283 自注 Must block as we're on the network thread）无中断能力，重放必达 Commit、AOF 事务组 TxnStart..TxnCommit 恒闭合。恢复侧 AofReplayCoordinator.cs 的 activeTxns 对无提交终结符的残组仅在恢复末尾整组弃置，从不半组重放——两端合起来 C# 不存在「已生效写落入弃置组」的形态。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）：rust 泵把 EXEC 重放（Running 直通）中命令的挂起交网络泵竞速驱动——排队 KEYS/冷键降级置 pending_slow、空键 BLPOP 经 blocking.rs 的 txn_direct 臂照常 park_broker_wait（挂起窗=持闩窗为票面明注的刻意形态）、排队 EVAL 内挂起置 script_suspend。三臂（阻塞/慢/脚本）在 drive.rs 的 probe_race 三路竞速中，终止广播（CLIENT KILL/停机令牌）或对端 FIN/RST 胜出即 RaceEnd::Disposed → break 'drive 丢弃执行体，重放中途废弃。收场尾巴 process_stream → NetworkHandler::dispose → RespServerSession::dispose（core.rs）只就地取消挂起体、摘订阅、关集群切面，对 Running 态事务不 commit、不投任何终结符；锁面无泄漏——TransactionManager::drop → reset → unlock_all_keys 兜底释放，屏障票据同步注销。但 finish_run_postlock 已把 TxnStart 落入 AOF（perform_writes 且 aof 在位时），重放前缀写命令的存储写已生效、组内 AOF 条目已入队，而 TxnCommit（TransactionManager::commit）只由重放遍末尾再次消费 EXEC 令牌触达，废弃后永不再达。恢复/副本侧 AofReplayCoordinator::add_or_replay_transaction_operation 对无 TxnCommit 的 active 组整组静默弃置（组缓冲随上下文消亡），TxnAbort 臂在协调器与 waof（AofEntryType::TxnAbort = 0x22）均现成却无任何生产写入方（wtxn TxnEntryType 仅 TxnStart/TxnCommit 两值）。
3. 逻辑危害确证（并发/数据丢失等实际危害）：a) 重启丢可见写——重放前缀写已在共享存储生效，恢复期弃组即丢弃，重启前已生效的数据重启后消失；b) 主副本活态分叉——副本经同一 aof_processor 组缓冲直到 TxnCommit 才 process_transaction_group 重放，废弃后主库有数据而副本永缺，直至重启才双方归零；c) 副本协调器 active 组滞留——无显式终结符，组缓冲与 session_id 键位滞留至重启（每废弃一笔事务一份）；d) 残余排队命令不再执行，与 C# 必达全组提交的批内不可中断语义相悖；e) 平滑关停复用同一终止令牌打断在途重放，放大为关停期批量残组，与审查维度「平滑关停在途排空」承诺相抵。

涉及代码：
rust 文件与函数：
wedb/wnode/src/net/handler/drive.rs: NetworkHandler::drive_loop（阻塞臂/慢臂/脚本臂 RaceEnd::Disposed → break 'drive 三处）、probe_race
wedb/wnode/src/resp/resp_server_session/core.rs: RespServerSession::dispose
wedb/wtxn/src/transaction_manager.rs: TransactionManager::finish_run_postlock（TxnStart 入队点）、TransactionManager::commit、impl Drop for TransactionManager、TxnEntryType
wedb/wnode/src/resp/objects/list_commands/blocking.rs: list_blocking_pop、list_blocking_move、list_blocking_pop_push、list_blocking_pop_multiple（txn_direct 重放段挂经纪臂，挂起可达性锚）
wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs: AofReplayCoordinator::add_or_replay_transaction_operation（TxnCommit/TxnAbort 臂与组缓冲）
wedb/waof/src/aof/entry_type.rs: AofEntryType::TxnAbort（现成无写入方）

对应 c# 文件与函数：
garnet/libs/server/Transaction/TxnRespCommands.cs: NetworkEXEC（同步重放 → Commit 必达）
garnet/libs/server/Resp/RespServerSession.cs: ProcessMessages（批内单线程执行不可中断）、Send（waitForAofBlocking 阻塞形态）
garnet/libs/server/Resp/Objects/ListCommands.cs: ListBlockingPop（BlockingWait 网络线程内联）
garnet/libs/common/Networking/GarnetTcpNetworkSender.cs: TryClose（KILL 关套接字不打断在途批）
garnet/libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs: activeTxns 组处置（未闭合组恢复末尾弃置）

精炼执行方案：
1. 泵三臂 Disposed 收场与 RespServerSession::dispose 增一道事务收口单点：txn_state == Running 且 perform_writes 且 aof 在位时，先经既有 enqueue_txn 通道投递显式终结符再复位——wtxn TxnEntryType 增 TxnAbort 变体并接通 waof AofEntryType::TxnAbort 既有判别值与协调器既有 TxnAbort 臂（恢复/副本即弃组），随后走现行 reset（锁释放与屏障注销路径零改动）；无 AOF 或只读事务直接 reset
2. 终结符语义取 TxnAbort 不取 TxnCommit：半组提交会把已废弃的尾命令钉成永久缺失且令副本重放半组（原子性破坏更深）；TxnAbort 与恢复期现行为同结果但显式化，同时解副本组滞留。重启丢可见写为废弃语义的既定代价，在收口单点注释钉明「重放废弃组 = 刻意弃置面」防后续误改
3. 测试验证点：开 AOF，MULTI + SET k v + BLPOP 空键 0 + EXEC，另一连接对同键 LPUSH 前 CLIENT KILL 前一连接，断言 aof 尾部落 TxnAbort、锁表清空、副本组缓冲释放无滞留；重启恢复同日志不报非法事务流、无残组；CLIENT UNBLOCK 与超时闭环（正常 commit 路径）回归不变；transaction_tests / txn_queue_lockset_residual / list_blocking_cold_wait 全绿

---

终态注记（2026-09-30 执行席收口，接续前序中断代理）：

- 合入哈希：merge 4f48b75（--no-ff fix-exec-abort 入 dev，基点 e645b00，dev 侧基线 5a41ed8）
- 收口形态：事务收口单点 `wtxn::TransactionManager::finish_abandoned`——Running 态经既有 `enqueue_txn_marker`（与 `commit` 同一入队内核）补投显式 `TxnEntryType::TxnAbort` 终结符，接通 waof 既有 `AofEntryType::TxnAbort`(0x22) 判别值与协调器既有弃组臂，随后走现行 `reset`；`traits::MessageConsumerFace::finish_abandoned_txn` 为转接口（默认空操作），单点实现在 `RespServerSession::finish_abandoned_txn`。双入口经状态门去重：泵三臂 `RaceEnd::Disposed`（drive.rs 阻塞/慢/脚本臂废弃 instant 先行）与会话 `dispose` 漏斗（QUIT/EOF/协议违规/致命断连/停机排空全部退出路径汇聚）共调一处，非 Running 恒零动作、不重复投递；无 AOF 或未落 TxnStart 只读事务直复位。锁释放与屏障注销路径零改动（`reset`→`unlock_all_keys`+票据注销）。
- 收口单点注释钉版「重放废弃组 = 刻意弃置面（严禁改判为补提交）」，取 Abort 不取 Commit：半组提交把已废弃尾命令钉成永久缺失且令副本重放半组，原子性破坏更深；重启丢可见写为废弃语义既定代价，对标 C# 恢复侧 `AofReplayCoordinator` 对未闭合组仅恢复末尾整组弃置、从不半组重放（C# 无此废弃面：NetworkEXEC 单网络线程内联重放必达 Commit）。
- 新增真链路集成测试 `wedb/wnode/tests/exec_replay_race_abandon_txn_abort.rs`（5 例，严禁假 mock）：真 socket CLIENT KILL 竞速废弃落 TxnAbort+锁闩释放+真协调器组缓冲滞留/弃组观测；真重启恢复同日志不报非法事务流、弃组不污染后继完整组；臂口双调用状态门去重+dispose 漏斗补投；CLIENT UNBLOCK 与超时闭环正常 commit 路径零废弃终结符回归；无 AOF 直复位。收口单测须 compio 运行时上下文驱动挂起等待面（同步单消费替身在运行时内消费，复用同一全局串行锁避免多运行时并发 SIGABRT）。
- 定向验证：transaction_tests(17)/txn_queue_lockset_residual(8)/list_blocking_cold_wait(2)/本票新增(5)全绿，workspace cargo check 零告警。
- 风险点：合并回主目录时 `wtxn/src/transaction_manager.rs` 与并行会话在途改动（`SublogAccess` derive 去 `Default`，异区不重叠）相撞——已先恢复至 HEAD 干净态吃进本票 merge，再将该一行还原为未提交工作树改动，并行席间产物零丢失；共享 target 目录多代理互锁下须 touch wtxn 源码强制重建才能刷新陈旧 rlib 指纹（已在 worktree 复现并规避）。
