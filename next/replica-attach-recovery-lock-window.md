副本 attach 恢复锁窗口坍缩到配置翻转瞬间：CheckpointRecoveredAtReplica 不可达、CannotStreamAOF 防线与恢复互斥双失

来源：next/glm.net.md 条 7（该文件已分拣清空删除）。逐句按主仓当下代码与 C# 复核后判定成立待做。
取证基线：主仓 /Users/z/git/db/wedb，行号按符号定位。
载体唯一性：并发拆条在 next/ 下留了一份本条原文照抄的壳（basename
replica-attach-recovery-lock-window.md，仅加「优先级：高」头、无取证订正），其认领后的目标路径
与本文件同名，git mv 会直接覆盖本文件的判定与修法。以本文件为唯一载体：派单前先核对壳与本文件，
壳剪掉再开工，勿双花。

结论

C# 的副本 attach 全程持有恢复锁：TryAddReplicaAsync 成功路径不释放，一直握到 attach 体的 finally。
rust 把释放提前到了 try_add_replica_async 的尾段，锁窗口只剩「配置翻转那一瞬」，
此后 INITIATE 往返、检查点传送、引擎置换全程状态是 NoRecovery。三个后果同时成立：
副本恢复收尾的 EndRecovery(CheckpointRecoveredAtReplica) 每次都被状态矩阵判非法并打一条
error 日志（该状态在正常 REPLICAOF/CLUSTER REPLICATE 链上不可达）；cannot_stream_aof 在整个
传送与置换窗口恒 false，恢复中拒收 AOF 帧的防线形同虚设；attach 窗口内并发的第二次
REPLICAOF / CLUSTER FAILOVER 的 begin_recovery 可以成功，而 C# 由恢复锁天然互斥。

现状

- 提前释放：/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_manager_worker_state.rs:125-183
  `try_add_replica_async` 在 :151-156 `begin_recovery(RecoveryStatus::ClusterReplicate, upgrade_lock)`
  之后，配置翻转、挂主端任务、清推流驱动、flush_config，最后 :179-181
  `rm.end_recovery(RecoveryStatus::NoRecovery, false)` 就地释放，函数返回即无锁。
- 唯一调用点在 attach 骨架内：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/assembly.rs:220-227
  （`if opts.try_add_replica` 臂），随后 :229-231 纪元等待、:233-249 才发起 attach 体并交
  `finish_replica_sync` 收尾；:266-277 的 finally 臂注释自述「rust 的 try_add_replica_async 尾段已自行
  EndRecovery(NoRecovery) 释放非升级锁……此刻无锁可释，重复转换会被 end_recovery 状态矩阵判非法」，
  即当前实现明确知道自己握不住锁，只把升级臂降级回 ReadRole（:272-276）。
- 不可达状态与必打错误日志：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_diskbased_sync.rs:176
  与 /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_diskless_sync.rs:204 的
  `rm.end_recovery(RecoveryStatus::CheckpointRecoveredAtReplica, false)` 落在
  /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replication_manager.rs:534-535
  的 `RecoveryStatus::NoRecovery => false` 分支上，恒走 :556-557 的
  `error!("Invalid state change ...")`。全仓 grep `end_recovery(RecoveryStatus::CheckpointRecoveredAtReplica`
  的调用点只有这两个加若干单测，也就是说普通 diskbased / diskless attach 每次完成必打一条
  Invalid state change 错误日志；该状态只在单测里可达
  （/Users/z/git/db/wedb/wedb/wedb/tests/cluster_replication.rs:101-105、
  /Users/z/git/db/wedb/wedb/wedb/tests/replication_manager.rs:202-209 都是先手工
  `begin_recovery(ClusterReplicate/InitializeRecover)` 再转），生产链上只有 upgrade_lock 臂经
  finish_replica_sync 降级到 ReadRole 后（:551 `ReadRole => true`）才碰巧合法，行为随入口分裂。
  同文件 replica_diskbased_sync.rs:57-61 的文档注释「调用方 CLUSTER REPLICATE 已
  begin_recovery(ClusterReplicate)」与实况矛盾（调用方此刻早已 end 到 NoRecovery）。
- 拒收 AOF 帧的防线失效：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replication_manager.rs:455-467
  `is_recovering`（NoRecovery/ReadRole 皆假）与 `cannot_stream_aof`，消费点
  /Users/z/git/db/wedb/wedb/wedb/src/server/replication/cluster_replication_session.rs:252-258
  （命中即 ResourceBusy 断流）。由于窗口内恒 NoRecovery，半开的旧 AofSyncTask 连接或主端异常时序
  可以让 AOF 帧在副本引擎置换前后落盘并重放进即将被换出的旧存储；C# 在此窗口拒收断流、触发重同步。
- 并发互斥失效：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replication_manager.rs:490-513
  `begin_recovery` 的非升级臂 :502-508 只在当前态非 NoRecovery 时拒绝，
  因此 attach 窗口内并发的 CLUSTER FAILOVER 或二次 REPLICAOF 都能取到锁，
  C# 则由握住的恢复锁返回 `CannotAcquireRecoveryLock`
  （/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_manager_worker_state.rs:155 同源错误）。

C# 参考

- /Users/z/git/db/wedb/garnet/libs/cluster/Server/ClusterManagerWorkerState.cs:155-230
  `TryAddReplicaAsync`：:204 `BeginRecovery(RecoveryStatus.ClusterReplicate, upgradeLock)` 成功后
  直接 `FlushConfig(); return (true, default)`（:226-230），锁不释；只有 CAS 更新失败的
  重试路径才 EndRecovery（:218-224）。
- /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/PrimaryOps/PrimarySync.cs:68-107
  `TryBeginDiskbasedSyncAsync` 返回的后台任务要等 `session.SendCheckpointAsync()` 完成
  （检查点传送 + 副本恢复回报位点）才对副本的 INITIATE 回 +OK，故副本侧的 INITIATE await
  覆盖到「主端全流程结束」。
- /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:181-186
  副本 `ExecuteClusterInitiateReplicaSync` 的 await，:197-208 其 finally 才
  `EndRecovery(ReadRole, downgradeLock: true)` / `EndRecovery(NoRecovery, false)`（发起段最后执行），
  :359-363 恢复点自己的 finally `EndRecovery(CheckpointRecoveredAtReplica, false)` 先于此发生
  （此刻 curr 仍是 ClusterReplicate，按矩阵合法）。diskless 同形：
  /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs 的
  发起段 finally 与恢复点 finally 同一对偶。
- 矩阵与防线：/Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/ReplicationManager.cs:406-460
  EndRecovery（:417-418 NoRecovery 起点即 `throw new GarnetException`，:420 起 ClusterReplicate 可转
  CheckpointRecoveredAtReplica）与 :49 `CannotStreamAOF => IsRecovering && currentRecoveryStatus != CheckpointRecoveredAtReplica`。
  即 C# 的时序是 ClusterReplicate（传送/恢复全程，拒流 + 互斥）→ CheckpointRecoveredAtReplica
  （放行 AOF 流但锁仍持）→ NoRecovery / ReadRole。

修法

1. 主修（对齐 C# 时序）：删掉
   /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_manager_worker_state.rs:179-181 尾段的
   `end_recovery(NoRecovery)`，成功路径握锁返回，与 C# :204-230 同形。失败路径不需要补释放：
   rust 的 try_add_replica_async 没有 C# 的 CAS 重试环（:158-163 是一次性 `write()` 改配置），
   取锁之后的分支只有 flush 与 reset，没有中途 return Err 的路径；若实现中出现新的提前返回，
   必须按 C# :218-224 的形态在返回前 end_recovery，别裸退。
2. 释放点搬到 attach 体的收尾：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/assembly.rs:253-278
   `finish_replica_sync` 的 finally 臂按 C# 的 if/else 一次做全 ——
   upgrade_lock 臂 `end_recovery(ReadRole, downgrade_lock: true)`（现 :275 已是此形），
   其余臂 `end_recovery(NoRecovery, false)`。关键约束：只有本次调用真的取过锁才允许 end，
   即释放要以 `opts.try_add_replica`（或 try_add_replica_async 返回的「本次握锁」标志）为条件；
   `try_add_replica == false` 的启动恢复臂进入 finish_replica_sync 时 curr 本就是 NoRecovery，
   无条件补一次 end 会把「每次 attach 打一条 Invalid state change」换成另一处同样的噪声，
   这是搬释放点时最容易踩的坑，判据写进该函数注释。
   background 臂（:236-247）在 spawn 体内走同一收尾，语义与 C# 的 forceAsync 一致，不需要额外补。
3. 防线随之复活，逐项核对而不是假定：release 之后 replica_diskbased_sync.rs:176 与
   replica_diskless_sync.rs:204 的 `end_recovery(CheckpointRecoveredAtReplica, false)`
   才落在 curr=ClusterReplicate 上按矩阵合法转换，cannot_stream_aof
   （replication_manager.rs:461-467）在 ClusterReplicate 段为 true、在
   CheckpointRecoveredAtReplica 段为 false，与 C# :49 的三态语义逐一对齐。
   同窗口的 begin_recovery 互斥（:502-508）也据此恢复拒并发。
4. 注释订正（与代码同批，不留旧说法）：replica_diskbased_sync.rs:57-73 的六步叙述里
   「调用方 CLUSTER REPLICATE 已 begin_recovery(ClusterReplicate)」改为实况；
   assembly.rs:266-277 的「此刻无锁可释」段整段重写为「锁由 try_add_replica_async 握到本收尾」；
   cluster_manager_worker_state.rs:164-177 段关于「翻转后、attach 前清驱动」的说明按新的持锁窗口
   复核措辞。replica_diskbased_sync.rs:91-95 与 replica_diskless_sync.rs:173-177 的
   「与 try_add_replica_async 尾的挂起互为幂等双保险」一句在尾段不再 end 锁后仍成立（挂起面不动），
   核对后保留。
5. 若确有理由维持两段拆分（不推荐）：则必须在副本侧收到恢复指令的入口
   （/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/replication.rs:770 的 diskbased 恢复调用点、
   :835 的 diskless 恢复调用点）先 `begin_recovery(ClusterReplicate, false)` 再进恢复体，
   使 :176/:204 的转换合法、恢复窗口重新握锁，并同步订正第 4 步的注释。
   两条路只能择一，禁止「尾段仍 end、入口再 begin」的三不管形态。
6. 测试：/Users/z/git/db/wedb/wedb/wedb/tests/checkpoint_import.rs 现有三处
   `begin_recovery(RecoveryStatus::ClusterReplicate, false)` 手工前置（:352、:512、:650）
   要按新时序复核 —— 若用例走的是完整 attach 链，前置 begin 应变为多余甚至冲突；
   另在 /Users/z/git/db/wedb/wedb/wedb/tests/replication_pipeline.rs（:314-329 已有 begin/end
   矩阵用例）补两条断言：attach 全流程中 `cannot_stream_aof()` 在恢复窗口为真、
   收尾后为假；以及 `CheckpointRecoveredAtReplica` 在生产入口可达（判据是错误日志计数为 0，
   可用 log 捕获断言而不是靠肉眼）。

优先级

功能缺口，且伴随一个不可达状态与每次 attach 必打的错误日志（属可机械验证的落地判据）。
类别上排在死代码、重复架构、污染扩散之后，但它是本文件里后果最重的一条：
防线失效直接指向「AOF 帧重放进将被换出的旧存储」这类数据面事故，建议与
task/ing/replication-history-flush-torn-write.md 同批处理。

交叉引用

- 同一 attach 链、同一 replication_manager.rs 的落盘互斥缺口：
  task/ing/replication-history-flush-torn-write.md（两单文件域重叠，须串行或同批）。
- 主端推流驱动与 AOF 直推时序：task/ing/aof-driver-register-pre-transfer.md、
  task/ing/wait-for-commit-chain.md，本单不改推流注册与位点推进，只改恢复窗口。
- diskless 全量同步的 flush 语义另有一单：task/ing/diskless-full-sync-flush-all.md，
  勿把两单的时序改动混进同一提交。
