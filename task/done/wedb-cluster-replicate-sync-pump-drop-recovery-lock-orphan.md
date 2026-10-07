终态：合入 6d8a97dd（主目录 dev 经 merge 提交 2b3f2be0 承载），ReplicateSyncGuard（assembly.rs，同 EpochDrainGuard 先例形：执行体首装配 + 正常臂 disarm + Drop spawn 补跑 finish_replica_sync）收口于 queue_try_replicate_sync 暴露面单点，恢复锁释放/角色复位/拒死链解除三面断言 + revert 摘守卫转红验证（tests/replicate_sync_pump_drop_orphan.rs 三用例全绿）。

甄别结论：通过（定级 P2：条件触发但节点砖死无自愈，唯一出路进程重启）

甄别要点（现码复跑）：
1. 真实性双侧全中。C# 锚：RespClusterReplicationCommands.cs:101-103 与 ReplicaOfCommand.cs:88-94 网络线程内联 BlockingWait（注释自陈 cannot avoid blocking）、ReplicaDiskbasedSync.cs:42 TryAddReplicaAsync、:188-196 catch(TryResetReplica)、:197-214 finally EndRecovery 必达。rust 锚：replica_of.rs:86 与 replication.rs:252 挂 pending_slow、queue_try_replicate_sync（replication.rs:102-114）无守卫、consume.rs:259 慢臂 probe_race、race.rs:104/:138/:153/:158 Disposed 丢弃（票面 274 为 consume.rs Disposed 分支行号）、assembly.rs:313/:367-374/:421-451 全中、try_add_replica_async 现码 cluster_manager_worker_state.rs:130-201 四项登记（:171 begin_recovery(ClusterReplicate)、:177-181 角色翻转+bump、:186 suspend、:194 驱动全拆、:196 flush_config）、recovery.rs:44 非 NoRecovery 即拒、拒死链三入口（replica_of.rs:35、failover/replica_failover_session.rs:183——票面路径 replication/ 系模块定位小偏差以现码为准、cluster_provider/replication.rs:328/:252）亲验。
2. 非重复非灭失：四池无同判据票；todo 姊妹票 wnode-flush-slowpath-pump-drop 判据为 FLUSH 换号域 AOF 广播发散（不同函数不同危害）；failover 承判票（done）收口接管臂排空弃值，与本票泵丢弃面互补不重叠；deviations.md 不在现树（代码注释引用为历史残留）。未灭失：queue_try_replicate_sync 现码确无守卫。
3. 幂等前提验讫：try_reset_replica 在 Primary 态幂等无害（cluster_manager.rs:603-613）、resume_primary_tasks 角色守卫（assets.rs:156-159）、NoRecovery 起点 end_recovery 状态矩阵拒变仅 error 留痕（recovery.rs:76-99，assembly.rs:310-312 注释自陈同形态）——取锁前丢弃补跑无害。
4. 先例形态验讫：EpochDrainGuard（cluster_session/failover.rs:406-448）Drop spawn 补跑 + disarm + Runtime::try_current 兜底留痕，本票同形收口零新机制。

REPLICAOF/CLUSTER REPLICATE 慢路径执行体被泵丢弃后恢复锁与副本角色滞留，复制面拒死无自愈通路

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 两处发起臂均在网络线程内联 BlockingWait 驱动同步发起链：CLUSTER REPLICATE 为 libs/cluster/Session/RespClusterReplicationCommands.cs:103，REPLICAOF 为 libs/cluster/Session/ReplicaOfCommand.cs:90-93（:88 注释自陈 "Cannot avoid blocking here we're on the network thread"）。网络线程阻塞期间连接收场事件（FIN/RST/客户端超时拆连）滞留内核缓冲，发起链不可能被中途撤销：TryReplicateDiskbasedSyncAsync（libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs，:42 await TryAddReplicaAsync 取 BeginRecovery 恢复锁）的收尾为 :188-194 catch（TryResetReplica + StartReplicaTasks）与 :197-208 finally EndRecovery，异常与正常两路都必达——恢复锁、角色翻转、任务挂起三面登记恒被回滚或推进到终态，绝不滞留半途态。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 前台臂经 queue_try_replicate_sync 挂 pending_slow 慢路径（wedb/wedb/src/server/cluster_session/replica_of.rs:86 与 replication.rs:252），网络泵以 probe_race 三路竞速驱动（wedb/wnode/src/net/handler/drive/consume.rs:259、race.rs）。终止广播（CLIENT KILL/停机令牌）、对端 EOF、dead-conn 任一胜出即 RaceEnd::Disposed，执行体随竞速败侧丢弃（race.rs:104/138/153/274）。执行体链 replicate_sync_async（wedb/wedb/src/server/replication/assembly.rs:313）第一步 try_add_replica_async（wedb/wedb/src/server/cluster_manager_worker_state.rs:155-200）同步完成四项管理面登记后才进入 await 窗：begin_recovery(ClusterReplicate) 恢复锁（recovery.rs:32，仅 NoRecovery/ReadRole 起点可取）、make_replica_of 角色翻转 + flush_config 持久化、suspend_primary_tasks（GC/周期任务停摆）、aof_sync_driver_store.reset()（旧推流驱动全拆）。此后全部 await 窗暴露于泵丢弃面：纪元排空 bump_and_wait_for_epoch_transition_async（上限 cluster_node_timeout，默认 60 秒）、recover_replication 建连与 INITIATE 往返（repl_attach_timeout）。任意一点丢弃即 finish_replica_sync（assembly.rs:421-451，catch 回滚臂 + finally release_attach_recovery 单点）永不可达。Background 臂（CLUSTER REPLICATE 缺省 ASYNC）spawn(detach) 之前的 try_add 与纪元等待段同面暴露。该链已防裸 return Err 与 panic（assembly.rs:367-374 注释自陈「裸 Err 即恢复锁永持、角色已翻 REPLICA 却不复位，后续一切恢复/重连被 ERR_RECOVERY_LOCK 拒死无自愈通路」，supervise panic 臂补跑 finish_replica_sync），唯独未防泵侧取消——同构面先例 network_cluster_fail_stop_writes 已挂 EpochDrainGuard 补跑（cluster_session/failover.rs:164-187），本链无对应守卫。滞留后果链：recovery_status 恒 ClusterReplicate → is_recovering/cannot_stream_aof 恒真 → REPLICAOF NO ONE（replica_of.rs:35 begin_recovery(ReplicaOfNoOne) 仅 NoRecovery 起点可取）、FAILOVER 接管（replica_failover_session.rs:183 begin_recovery(ClusterFailover)）、启动臂 start_replication_attach（cluster_provider/replication.rs:328）与 ensure_replication 重连臂全部 CannotAcquireRecoveryLock 拒绝；节点以副本态无流滞留，主任务挂起、旧推流驱动已拆，唯一出路是进程重启（恢复状态为内存态）。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
触发面为常规运维路径：发起 REPLICAOF/CLUSTER REPLICATE 的连接在纪元排空窗或建连窗断开、CLIENT KILL、停机排空，任一命中即节点被砖——恢复锁永持、角色持久化为副本、GC 与周期提交停摆、副本读面背后无流。集群视角下该节点槽位仍有属主但永久脱管，需人工重启恢复。竞态票推演链：连接断开（EOF）→ probe_race Ok(0) → RaceEnd::Disposed → slow.resolve() future 随 probe_race 帧丢弃 → replicate_sync_async 在 await 点展开 → finish_replica_sync 不可达 → begin/end_recovery 状态矩阵永久停在 ClusterReplicate。不可复现原因：丢弃点分布 Across 秒级窗且依赖连接终止事件与链内 await 的交错，集成测试可以 future drop 注入确定性构造，但生产表现为低频滞留而非即时可见错误。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/assembly.rs:replicate_sync_async
wedb/wedb/src/server/replication/assembly.rs:finish_replica_sync
wedb/wedb/src/server/replication/assembly.rs:release_attach_recovery
wedb/wedb/src/server/cluster_manager_worker_state.rs:try_add_replica_async
wedb/wedb/src/server/cluster_session/replica_of.rs:network_replicaof
wedb/wedb/src/server/cluster_session/replication.rs:queue_try_replicate_sync
wedb/wedb/src/server/cluster_session/replication.rs:network_cluster_replicate
wedb/wedb/src/server/replication/replication_manager/recovery.rs:begin_recovery
wedb/wedb/src/server/replication/replication_manager/recovery.rs:end_recovery
wedb/wnode/src/net/handler/drive/race.rs:probe_race（RaceEnd::Disposed 丢弃执行体）
wedb/wnode/src/net/handler/drive/consume.rs:NetworkHandler::drive_loop（慢臂竞速）
参照守卫先例：wedb/wedb/src/server/cluster_session/failover.rs:EpochDrainGuard

对应 c# 文件与函数：
garnet/libs/cluster/Session/RespClusterReplicationCommands.cs:NetworkClusterReplicate（:103 BlockingWait）
garnet/libs/cluster/Session/ReplicaOfCommand.cs:NetworkTryREPLICAOF（:90-93 BlockingWait）
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:TryReplicateDiskbasedSyncAsync（:42 TryAddReplicaAsync、:188-194 catch、:197-208 finally EndRecovery）
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:TryAddReplicaAsync（:204-230 恢复锁生命周期）
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:TryReplicateDisklessSyncAsync（:185-194 finally EndRecovery）

精炼执行方案：
1. 单机制收口（对标 failover.rs:EpochDrainGuard 先例：Drop 内取消收口 + disarm 幂等共存）：queue_try_replicate_sync 的 SlowWait 执行体内、try_replicate_sync_async 返回后 disarm 一枚取消赎回守卫（持 Arc<ClusterProvider> + ReplicateSyncOptions）。正常臂零变化：disarm 后守卫退场无操作。
2. 守卫 Drop（未 disarm，即执行体被泵丢弃或会话收口丢弃）在 compio runtime 上下文 spawn(detach) 独立任务补跑 finish_replica_sync(&provider, opts, Err(Sync("replicate sync wait dropped")))：判败走 try_reset_replica + resume_primary_tasks 回滚臂，finally 臂 release_attach_recovery 释放恢复锁——与 assembly.rs:409-417 panic 臂同一收尾单点，零新机制。补跑幂等性论证：仅当 try_add_replica_async 已成功返回（持锁事实成立）守卫才具赎回义务，故守卫装配点放在 try_replicate_sync_async 成功路径内部紧随其后不可行（链内装配即错过取锁前丢弃——取锁前丢弃本就零登记无需赎回），以「执行体起点装配 + 正常臂 disarm + Drop 补跑」为唯一形态；Runtime::try_current 缺席兜底臂仅留痕（同 EpochDrainGuard 先例）。严禁另起第二套按 recovery_status 判据的独立清扫器。
3. 测试验证点：集成用例——装配集群后直接构造 SlowWait 执行 queue_try_replicate_sync（target 指向不可达端点以拖长建连窗），在未来体 pending 后 drop 之，断言 recovery_status 回 NoRecovery、local_node_role 回 Primary、primary tasks 恢复、REPLICAOF NO ONE 可再次成功；对照正常成功链与失败回滚链行为零变化（disarm 生效，无重复收尾日志）。
