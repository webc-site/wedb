归档注记：合入 5c00d96c，PARTIAL 补 BEGIN_REPLICA_RECOVER 往返+残段按 mask 补应用+回传位点钳制挂流，同应用核单机制

甄别结论：通过（甄别席 J3，2026-09-27，定级 P1——默认生产形态一次断连即永久漏应用段；b02 后行号漂移已勘误，判据不灭失）。重点复验 b02 重叠面：replica_sync_session.rs 现码 :250 if matches!(strategy, ResyncStrategy::FullResync { .. }) 才走 send_checkpoint_and_recover，begin_replica_recover_async（:636）仅存在于全量链内，PartialResync 臂无任何恢复往返——核心判据 b02 大改后依然成立。现码勘误：initiate_replica_sync 漏应用臂实位 :250（票面 :256）、diskless try_begin_aof_sync 实位 :306（票面 :363-410）、recover_replication 应答面「成功仅记日志」实位 :167 起。mask 死计算亲证：replica_diskbased_sync.rs:41 注释自陈「rust 主端恒发 0」、:117 非 0 恒拒，negotiate_resync :872/:930/:943 计算无消费臂。C# 三锚亲验：ReplicaSyncSession.cs:178 ExecuteClusterBeginReplicaRecover 无条件往返 + syncFromAofAddress 挂驱动、ReplicaDiskbasedSync.cs:305-317 ReplayAOF+Log.Initialize、GarnetAppendOnlyFile.cs:205-221 min(rep_tail,committed)+repl_offset2 钳。派沙箱席 c01b。

审核结论：通过（P1 真案，危害链坐实非虚报。C# 强制验伪通过：DiskbasedReplication/ReplicaSyncSession.cs:178 ExecuteClusterBeginReplicaRecover 确在 skipLocalMainStoreCheckpoint 分支外无条件执行、:199 回传 syncFromAofAddress 挂驱动；ReplicaDiskbasedSync.cs:307-317 ReplayAOF+Log.Initialize 钳位属实；GarnetAppendOnlyFile.cs:208-221 min(rep_tail,committed) 与票述一致。rust initiate_replica_sync:250 仅 FullResync 臂走 send_checkpoint_and_recover、PartialResync 直挂协商位点（=rep_tail）、recover_replication:256 成功仅记日志属实。漏应用链验真：生产装配重放资产在场、replay_hook=None、maxLag 默认 -1，applied=M 落后尾 N 为常态；断连 dispose 终止背景重放线程（replica_replay_driver.rs:99-108），残段 [M,N) 留 wal 未应用；新驱动自 previous≈N 起扫不回补 M，协商取 rep_tail 非 replication_offset 主端不重推——「已落盘未应用」非「已应用」，虚报反证不成立；位点 M 跳 N、检查点覆盖认定污染、§162 重启回补「先缺后跳」闭合。replay_aof_mask 死计算属实（发送恒 0、接收拒非 0、无消费）。diskless try_begin_aof_sync:363-410 回传位点挂驱动系不对称先例。查重净：§116/§162/§44/§88 正交，idx 界票异轴）

整理执行方案（审核席订正版，供 fix 消费）：
1 唯一主轨：PartialResync 补轻量 recover 往返、以回传位点挂驱动，与 diskless 同形收口
2 mask 支取「真消费 replay_aof_mask 并扩 replay 语义」，弃「降级 FullResync」支免留双轨歧义
3 fix 时同步订正 deviations §162「无新增数据危害面」措辞回指本票窗口
4 锁测：断连残段 [M,N) 重挂后终态双侧对账（INCR/APPEND 类非幂等命令锁）

磁盘链 PartialResync 接续省去对位钳制回放往返，副本 wal 已落盘未应用段永久漏应用且钳位场景必发散死循环

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 磁盘臂主端在 startAofSync 之前无条件发 ExecuteClusterBeginReplicaRecover 往返（即便 skipLocalMainStoreCheckpoint 即部分重同步、不发快照）：先经 ComputeAofSyncReplayAddress 算出 replayAOFMap 与接续位点 replay_until（= min(副本 wal 尾, 主端已提交)，failover 臂再钳 repl_offset2），副本 TryReplicaDiskbasedRecovery 以 ReplayAOF(replayUntil) 把本地 wal 中检查点覆盖线之后、尚未应用进存储的残留段补应用，再 Log.Initialize(beginAddress, recoveredReplicationOffset) 把本地日志尾回滚对齐授予位点，应答该位点；主端以副本回传位点挂推流驱动，源码注释自陈 "start streaming from that address in order not to introduce duplicate insertions"。即「副本上报的是落盘尾、存储应用位可落后」这一非幂等重放窗（INCR/APPEND/LPUSH 类同代条目，版本门 is_old/new_version 判不等故拦不住、副本位点门亦不启用）恰由该往返单点收口。本仓无盘臂 rust 已同形落地：try_begin_aof_sync 经 ATTACH_SYNC 恢复握手以副本回传位点 sync_from 为推流起点，注释自证「保证首个 APPENDLOG 记录帧与副本日志尾严格衔接（否则必命中 divergent 断流）」；磁盘臂两形态非同构。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 磁盘臂：initiate_replica_sync 仅在 matches!(strategy, FullResync) 时调 send_checkpoint_and_recover，且该臂 replayAOFMap 恒传 0、以快照覆盖位点直推（全量臂前提成立：副本 safe_initialize(covered, covered) 回滚后主端从 covered 重推，无漏）；PartialResync 分支无任何恢复往返，直接 start_aof_sync 以协商 sync_start（negotiate_resync 算出的 replay_until，默认即副本上报 wal 尾 rep_tail）挂 AofSyncDriver。副本侧 recover_replication 对 INITIATE 应答「成功仅记日志」，无本地补回放/回滚臂；主端从 rep_tail 起推流，副本 process_primary_stream 收帧后 initialize_background_replay_task(previous_address=rep_tail)，新驱动 run_replay_loop 自 rep_tail 起扫，断连时刻应用位点 M（背景重放 applied 语义，默认 aof_replay_max_lag_bytes=-1 节流禁用、M<尾 N 为写负载常态）至 N 之间已落盘未应用的残留段 [M,N) 再无任何链路应用：该段不进存储、复制位点批末直接 M 跳 N（假消费推进主端截断线与副本自身检查点覆盖认定），主端 AOF 截断后 [M,N) 全域消失，主从永久发散；副本重启时启动面 replay_aof 又从本地 wal 回补该段，终态先缺后跳、非单调。次生面：当 committed 钳或 deviations §116 在码激活的 repl_offset2 钳使 replay_until < 副本尾，主端无从回滚副本日志（往返缺失），首帧必命中 process_primary_stream divergent 判定 → 副本致命断流 → ensure_replication 按轮重试同位点同结果，无退全量收敛臂。另 negotiate_resync 计算的 replay_aof_mask 在磁盘链为死值（发送面恒 0、副本 network_cluster_begin_replica_recover → try_replica_diskbased_recovery 对非 0 恒拒绝违约）。查重：五池与 deviations 仅 §116（钳位判据本身）与 wedb-repl-appendlog-sublog-idx-range-guard-missing（下标界）沾边，PartialResync 钳制往返缺失、[applied,tail) 残留段漏应用面无一登记；task/done/replica-offset-semantics 登记的是退化装配掉电窗，非本重放窗。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
默认生产形态（磁盘复制 + 背景重放）下一次瞬时断连重挂即可令副本存储永久缺失一段已落盘写（非幂等命令终态不可补救、无告警），并经位点假推进污染主端截断线与副本检查点覆盖认定，发散随时间固化；failover 重挂钳位场景为持续 divergent 断连重连风暴，复制面停摆。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/replica_sync_session.rs:initiate_replica_sync/start_aof_sync/transmit_checkpoint（BeginReplicaRecover 仅全量臂、mask 恒 0）
wedb/wedb/src/server/replication/replication_manager.rs:negotiate_resync（replay_until/replay_aof_mask 死计算无消费臂）
wedb/wedb/src/server/replication/assembly.rs:recover_replication（应答面只记日志）
wedb/wedb/src/server/replication/cluster_replication_session.rs:process_primary_stream（新驱动自帧 previous 起扫 + divergent 判定）
wedb/wedb/src/server/replication/replica_replay_task.rs:run_replay_loop（aof_floor 空、扫描起点=授予位点）
wedb/wedb/src/server/replication/replica_diskbased_sync.rs:try_replica_diskbased_recovery（replay_aof_map!=0 恒拒）
对照收口先例：wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs:try_begin_aof_sync（副本回传位点挂驱动）

对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:SendCheckpointAsync（ComputeAofSyncReplayAddress + ExecuteClusterBeginReplicaRecover 无条件往返 + syncFromAofAddress 挂驱动）
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:TryReplicaDiskbasedRecovery（ReplayAOF + Log.Initialize 回滚钳位 + 回传位点）
garnet/libs/server/AOF/GarnetAppendOnlyFile.cs:ComputeAofSyncReplayAddress

精炼执行方案：
1 磁盘臂对位 C#：PartialResync 亦经 begin_replica_recover_async 往返（副本侧扩一轻量钳制形，不发快照帧），副本以协商授予位点为界把本地 wal「应用位点~日志尾」残留段经既有记录应用链（run_replay_loop 同应用体/版本闸）补应用进存储，随即 wal.safe_initialize 对齐授予位点并回传；主端以副本回传位点（非本地协商值）挂 AofSyncDriver，与 diskless 臂 try_begin_aof_sync 同形态收口单机制
2 replay_aof_mask 三态判据一并收敛：副本侧钳制回放臂真消费该 mask（对位 C#），或磁盘链删 negotiate 死计算并将「sync_start < 副本尾不可回滚」场景显式降级 FullResync（消除 divergent 重连风暴），二择一不留双轨
3 测试验证点：默认磁盘复制 + maxLag=-1 形态下构造 INCR/LPUSH 落盘后人为断连（tail>applied），重挂 PartialResync 完成后断言副本存储终态含 [applied,tail) 全部条目且 INFO 位点连续无跳变；钳位 replay_until<副本尾场景断言单轮协商内收敛（不触发 divergent 断流循环）；无盘臂与全量臂既测不回退
