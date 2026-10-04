归档注记：合入 1ff3cee7，非主承接拒绝谓词+一跳环判，失败沿既有回滚复位，副本接收面不过门

甄别结论：通过（甄别席 J3，2026-09-27，定级 P1——互指环双主互推乒乓、分片无可写主）。三门亲验：cluster_manager_worker_state.rs:139 MigrateToMyself/:146 AlreadyReplica/:156 ReplicateTargetNotPrimary，皆只裁直接自指与本地已知套娃，无环判据；:187 flush_config 持久化互指配置坐实重启固化路径。承接面亲验：network_cluster_initiate_replica_sync（:592 起）函数体零本端角色门，:54 origin_node_role: NodeRole::Replica 系记录来方角色非本端判据；start_replication_attach（cluster_provider/replication.rs:290 起）按盘上 role==Replica 径行发起。C# 同源零门亲证：ClusterManagerWorkerState.cs:155 TryAddReplicaAsync、PrimarySync.cs:58 TryBeginDiskbasedSyncAsync 承接面无反向校验。todo/ing 池角色门关键词仅本票（primary_task_role_gate 测试系副本写面已入库，正交）。派沙箱席 c01b。

审核结论：通过（P1 真案。①承接链确无本端角色门：network_cluster_initiate_replica_sync（cluster_session/replication.rs:592 仅资产在位+端点反查+去重册）、attach_sync Replica 支（:881→replica_diskless_sync.rs:246 直入册）、diskbased 体全链零 local_node_role 判；主端资产 boot.rs:182→assembly.rs:85 点亮即双角色一律装配；APPENDLOG 面 is_replica+validate_primary_id（cluster_replication_session.rs:182/234）环态互自洽反成放行。②重启前提坐实：try_add_replica_async:187 flush_config 持久 role+replica_of_node_id（make_replica_of mod.rs:597 serializer 往返锁定），start_replication_attach（cluster_provider/replication.rs:290）TryAddReplica:false 绕三门按盘上互指配置径行发起，无竞态亦固化。③乒乓坐实：副本接收面 wal.safe_initialize 按对端位点改基（cluster_replication_session.rs:303、replica_diskless_sync.rs:196），同端作主又按改基位点供流，divergent 断流（:311）→ensure_replication 重试成环；截断线取全驱动最小值钉死（aof_sync_driver.rs:459-476）。④C# 同源属实不豁免（TryAddReplicaAsync:155-200 三门、PrimarySync.cs 承接零门）；套娃至多一层属实。⑤五池与 deviations 零同面，§44/§88/failover 圈正交）

整理执行方案（审核席订正版，供 fix 消费）：
1 主收口：承接面单谓词角色门，文案取 ReplicateTargetNotPrimary 族、经既有失败回滚臂收敛，零新机制
2 翻转臂一跳回溯并入 try_add_replica_async:136-158 既有读锁段内完成，勿开第二判点
3 夹具测试三点照票面

REPLICAOF 互指二节点环无环检测：翻转臂与 attach 承接面均不验「请求方/目标链含己身」，A↔B 互指成环后分片无可写主且位点乒乓断连

问题分析：
1. 契约与自研防线完备性核查：本仓拓扑面在册防线共四处——cluster_manager_worker_state.rs::try_add_replica_async 三门（direct self：local_node_id==node_id 拒 MigrateToMyself；AlreadyReplica；get_node_role_from_node_id 非 Primary 拒 ReplicateTargetNotPrimary）、replica_sync_session.rs 命令侧 AOF 门、cluster_replication_session.rs::validate_primary_id（APPENDLOG init 帧要求来流 origin==本端 primary）、cluster_provider/replication.rs::ensure_replication 第 3 步（活跃会话须来自其 primary）。逐一核验：全部只裁「直接自指」与「本地已知的套娃」，对间接环 A→B 且 B→A 无任何判据。deviations.md 全文零环检测/cycle 登记（grep 自指/乒乓/cycle 零命中本面），非在册裁决。对照 garnet C#：ClusterManagerWorkerState.cs::TryAddReplicaAsync 与 ReplicaOfCommand.cs:NetworkTryREPLICAOF 同形三门，PrimarySync.cs:TryBeginDiskbasedSyncAsync 承接面同样零反向校验（SyncMetadata.currentPrimaryReplicationId 全链仅日志消费），属 C# 同源缺陷——按本席规矩不豁免。
2. 工程现状确证：成环两条可达路径。路径一（竞态窗口）：B REPLICAOF A 翻转即 bump 本位 epoch 落盘，gossip 收敛前 A 本地视 B 仍为 Primary，A 执行 REPLICAOF B 三门全过（REPLICAOF 为 Force:true，AlreadyReplica/槽位门旁路）；此后双方各持久化互指配置。路径二（重启固化，无需竞态）：双方持久配置互指后，start_replication_attach（cluster_provider/replication.rs:290-348）各按本端配置发起 attach，而主端承接面 network_cluster_initiate_replica_sync / network_cluster_attach_sync（cluster_session/replication.rs:592/881）仅验「请求方节点 id 在册 + endpoint 反查成功」，无本端角色门——主端资产在 boot 期 AOF 门控点亮即双角色一律装配（wire_replication_data_plane 无角色分支，副本态照常在册承接），互指双方当场坐实互推流。副本写面角色门（primary_task_role_gate 同族判据）使环内双方均拒写：该分片从此无可写主，INFO replication 两节点恒报 role:slave 且互指。
3. 逻辑危害确证：位点面双主互推互为对方流源——本端 wal 被对端流覆写衔接（process_primary_stream 判 tail!=currentAddress 即 Divergent 断流；fast_aof_truncate 臂 safe_initialize 按对端位点重置本地地址空间），而本端作为主端授予对端的又取本端刚被改基的位点，两侧授予互相矛盾，断流→ensure_replication/重挂→再断流乒乓，唯一止血为人工 REPLICAOF NO ONE 双拆。资源面：环内两驱动在册钳制 safe_truncate 线与背压闸门（AofSyncDriverStore::safe_truncate_aof 取全驱动 previous_address 最小值），失联乒乓期主写面（环外升主的节点）截断线被钉。套娃深度面核验：C→B→A 链在命令面已被 ReplicateTargetNotPrimary 封死（至多一层），唯独环面无此等位防线。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/cluster_manager_worker_state.rs:try_add_replica_async
wedb/wedb/src/server/cluster_session/replication.rs:network_cluster_initiate_replica_sync/network_cluster_attach_sync
wedb/wedb/src/server/cluster_provider/replication.rs:start_replication_attach/ensure_replication
wedb/wedb/src/server/replication/cluster_replication_session.rs:validate_primary_id/process_primary_stream

对应 c# 文件与函数：
garnet/libs/cluster/Server/ClusterManagerWorkerState.cs:TryAddReplicaAsync
garnet/libs/cluster/Session/ReplicaOfCommand.cs:NetworkTryREPLICAOF
garnet/libs/cluster/Server/Replication/PrimaryOps/PrimarySync.cs:TryBeginDiskbasedSyncAsync

精炼执行方案：
1 承接面角色门单点收口（单一谓词，非新机制）：network_cluster_initiate_replica_sync 与 network_cluster_attach_sync（其 origin_node_role==Replica 的主端支）入口补 local_node_role()!=Primary 即拒（-ERR 文案对齐 ReplicateTargetNotPrimary 族，经既有发起侧失败回滚臂收敛）；本仓套娃契约「至多一层」下 Replica 态节点永远不该为他人供流，与接管链升主后角色已翻 Primary 的合法再挂载零冲突
2 翻转臂一跳环判：try_add_replica_async 校验段补「沿本地配置视图自 target 的 primary_id 链回溯命中 local_node_id 即拒环」（链长有界 ≤ worker 数，视图陈旧期漏检由步骤 1 承接面门兜底，双点同谓词一条环判据）
3 测试验证点：双节点夹具互指（gossip 停摆窗内互 REPLICAOF）断言次路 initiate 被 -ERR 拒且发起侧回滚为 Primary；持久互指配置重启回归（start_replication_attach 臂）断言一方显式失败不再乒乓；合法 A→B 单套娃与接管升主后副本重挂不回退
