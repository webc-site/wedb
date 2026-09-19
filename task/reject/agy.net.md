来源：next/agy.net.md（review-net 待办 20 条，分拣于 2026-09-19 晚间波，主仓 dev 现刻代码取证）
本档案登记本文件判定不成立的 8 条，逐条附拒绝理由与代码/C# 证据。其余 12 条为「并入既有载体」6 条、「已落地/事实不再成立」3 条、「成立待做」3 组（见 task/ing/）。

## 条 1 execute_checkpoint_recv 阻塞网络 reactor 线程（主张改用 SlowWait、移除 blocking_wait）

拒绝理由：违反 1:1 对标，且对 blocking_wait 的事实判断错误。
- 事实：blocking_wait 在 compio 运行时线程上即 Runtime::try_current().block_on(f)，挂起窗口内让位本运行时的其他任务与 I/O driver，不冻结同核其他连接（/Users/z/git/db/wedb/wedb/wbase/src/future.rs:64-67 实现，:51-53 分工注释自陈「生产全部调用点走 Runtime::block_on，挂起窗口内让位给本运行时的其他任务与 I/O driver」）。
- C# 对位：本检查点接收三面（NetworkClusterSnapshotData / NetworkClusterSendCheckpointMetadata / NetworkClusterSendCheckpointFileSegment）在 C# 是纯同步直落盘——分别直调 recvCheckpointHandler.ProcessSnapshotData / ProcessMetadata / ProcessFileSegment 后回 RESP_OK，全链无 await、无线程让渡（garnet/libs/cluster/Session/RespClusterReplicationCommands.cs:304-418）。C# 同文件里用 BlockingWait 的是另三面（:104 Replicate、:282 InitiateReplicaSync、:501 AttachSync），rust 已按 SlowWait 承载（replication.rs:604、:782、:832、:877）。
- 结论：把该路径改成 SlowWait 挂起会让 rust 比 C# 多一层慢路径编排，属自造优化；票面「阻塞 reactor 导致同核心其他并发连接被暂停」的前提不成立。
- 取证位：/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/replication.rs:619-649（execute_checkpoint_recv 单点收割，仅 blocking_wait 一处）。

## 条 2 NodeConnection initialize_async 失败后不复位 initialized（主张失败即复位以自动重连）

拒绝理由：rust 与 C# 逐字同构，票面要求的是 C# 原版没有的重连复位机制。
- C# 对位：garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:107-114 InitializeAsync —— `if (initialized != 0 || Interlocked.CompareExchange(ref initialized, 1, 0) != 0) return default;` 后置位再 `gc.ReconnectAsync()`；全文件仅构造期 :96 一处把 initialized 归零，TryGossip 的失败回退段 ResetCts()（:309-327）也不复位该标志。即 C# 同样是「首连失败即不再重连」。
- rust 取证：/Users/z/git/db/wedb/wedb/wedb/src/server/gossip/node_connection.rs:120-132（同构双检 + compare_exchange 后置 connect_async）；门面侧另有 is_connected 短路（/Users/z/git/db/wedb/wedb/wedb/src/client.rs:116-118）。
- 结论：加「失败复位 + 下轮重连」属新增架构与自造优化，超出转写需求。

## 条 8 CheckpointStore wait_for_replicas 纯自旋（主张加超时熔断或 Event 通知）

拒绝理由：C# 同函数即无超时的线程让渡自旋，rust 一比一，票面把它误读为无让渡空转。
- rust 取证：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/checkpoint_store.rs:4 `use std::thread::yield_now`，:94-103 wait_for_replicas 内 `while !entry.try_suspend_readers() { yield_now(); }` —— 是 OS 线程让步（std::thread::yield_now），非 busy spin 占满时间片，也非未 await 的空 future。
- C# 对位：garnet/libs/cluster/Server/Replication/CheckpointStore.cs:62-71 WaitForReplicas `while (!curr.TrySuspendReaders()) Thread.Yield();`，同样无超时、无事件唤醒。
- 结论：加超时/Event 属新增第二套等待机制，与 1:1 对标冲突。

## 条 13 GarnetClient 构造锚点挂到 facade 注入器（主张移除 wedb/wedb/src/client.rs set_tls 上的构造锚点）

拒绝理由：票面所指锚点不存在，判据被 check.js 重复定义扫描否证。
- 事实：/Users/z/git/db/wedb/wedb/wedb/src/client.rs 内 `.cs:` 锚点全为 GarnetClientExtensions / GarnetClientClusterCommands / Migrate / Replication 族（:165、:179、:219、:237、:254、:268、:296、:310、:342、:378、:426、:441、:482、:520），无一处 libs/client/GarnetClient.cs:GarnetClient；wconn 侧 set_tls 的说明文字也已写成「（GarnetClient 构造器）」不含 `.cs:` 锚点（/Users/z/git/db/wedb/wedb/wconn/src/client.rs:98-106）。
- 复核手段：按 js/check.js:304-335 dupDefFind 同规则（CS_REF_REGEX + 仅函数级文档注释）对全仓扫描，libs/client/GarnetClient.cs:GarnetClient 未进重复定义组（wconn/src/client.rs:19 为 struct 文档、:44 为字段文档、error.rs:60 为枚举变体文档，均不在 fn_doc_li 口径内）。
- 结论：无缺陷可修，票面路径与判据双双不成立。

## 条 17 can_access_key 转发层与本体同名并存、共用同一锚点（主张转发侧撤锚）

拒绝理由：两级锚点各指不同 C# 文件，且 C# 本身就是三级同名转发链，rust 为 1:1 映射。
- C# 对位：garnet/libs/cluster/Server/Migration/MigrationManager.cs:152-153 `public bool CanAccessKey(...) => migrationTaskStore.CanAccessKey(...)`；MigrateSessionTaskStore.cs:221；MigrateSessionKeyAccess.cs:35 —— 三层同名，非「一个锚点两处挂」。
- rust 取证：migration_manager.rs:116 挂 libs/cluster/Server/Migration/MigrationManager.cs:CanAccessKey，migrate_session.rs:170 挂 MigrateSessionKeyAccess.cs:CanAccessKey，migrate_session_task_store.rs:128 挂 MigrateSessionTaskStore.cs:CanAccessKey，三级齐平；dup 扫描无该键重复组。
- 结论：撤掉任一级锚点反而丢映射覆盖。另记：cluster_manager_slot_gate.rs:463 的散文裸名 `MigrateSessionKeyAccess.cs:CanAccessKey` 不注册为函数锚点，无需处理。

## 条 18 ConsumerRegistry install_global 只在 NodeService::from_parts，直构 GarnetServer 即取到 None

拒绝理由：该「第二启动路径」在生产代码里不存在，票面担心的 None 只在手写测试 provider 处出现且已被调用侧判空消化。
- rust 取证：ConsumerRegistry 唯一生产构造与安装点即 from_parts（/Users/z/git/db/wedb/wedb/wnode/src/service.rs:1020-1021），而三个装配口全部收敛于它（:1001 open_with_config、:1213、:1271 open_from_args）；生产启动序列 server.rs:229-241 的 provider 由 `assemble` 产出，唯一生产 SessionProviderFace 实现是 StorageSessionProvider（service.rs:1540）——src 全域再无第二实现（其余实现全在 tests/）。
- 消费侧本即按 Option 语义处理：resp/client_commands.rs:48、:139、:463 与 info_provider.rs:236 均 `let Some(..) else` 兜空，garnet_api/slow.rs:57 用 is_some_and。
- 结论：把安装点上移到 GarnetServer::new 需为 trait 增口，属预防性架构改动，C# 侧是 GarnetServerTcp 实例字段 activeHandlers（无进程级静态、无「双路径」问题），不存在该复杂度。

## 条 19 MIGRATE KEYS 超大单记录分块中途失败需「重连并复位远端槽位」

拒绝理由：C# 无此恢复编排；接收端重组器为连接级、随会话析构，票面「孤儿流数据滞留」不成立。
- C# 对位：garnet/libs/cluster/Server/Migration/MigrateSessionKeys.cs:67-71 TransmitKeysAsync 失败即 `return false`，无重连、无远端复位；MigrateSessionCommonUtils.cs 的 CompletePending 处理的是本地待回收态，不涉及重连语义。
- rust 取证：/Users/z/git/db/wedb/wedb/wedb/src/server/migration/migrate_driver/keys.rs:545-579（`?` 上抛即整批失败），ChunkReassembler 挂在 ClusterSession 实例上（/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/mod.rs:72 `pub(super) chunk_reassembler: Arc<Mutex<ChunkReassembler>>`，:94 随会话 `Arc::default()` 构造），连接销毁即释放，且本体已有 reset 复位口（/Users/z/git/db/wedb/wedb/wedb/src/server/migration/chunk_reassembler.rs:50-55，对标 ChunkedRecordReassembler.cs:Reset）。
- 结论：新增「重连 + 远端槽位复位」是 C# 没有的第三套恢复机制。（顺带登记：ChunkReassembler::reset 目前生产零调用，属死码判读范畴，与本票主张不同题，留给设计域盘点票处理。）

## 条 20 failover replica 长会话「泛型抹平」风险（主张核对差异并补注释与边界单测）

拒绝理由：两类会话在 rust 已按 C# 一文件一类分件、方法逐一对应，未抹平；票面无具体缺陷，属泛化核查型主张。
- 文件对位（rust vs C#）：primary_failover_session.rs:85/113/165/179（probe_replica_sync、wait_for_first_replica_sync_async、initiate_replica_take_over_async、begin_async_primary_failover_async）对 garnet/libs/cluster/Server/Failover/PrimaryFailoverSession.cs:15/31/88/104 四个方法；replica_failover_session.rs:58/89/94/165/214/289/324/336/375 对 ReplicaFailoverSession.cs:32/63/70/117/187/261/309/316。
- 共用半边对标 C# 基类：C# FailoverSession.cs 自身即两类共用基类（:60-83 按 isReplicaSession 分叉建 clients、failoverTimeout == default 归一 600 秒 + failoverDeadline），rust 基件 failover_session.rs:166-173 的 race_abort 只承接 C# `WaitAsync(timeout, cts.Token)` 的取消半边，超时半边按 C# 同口径留外层 timeout。
- 差异已有用例钉住：failover_session.rs:188-196 zero_failover_timeout_normalizes_to_six_hundred_seconds。
- 结论：无「抹平」事实，不立核查票。
