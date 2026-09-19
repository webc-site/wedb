复制同步超时口径对齐 C#：ReplicaSyncTimeout(5s) 与 ReplicaAttachTimeout(60s) 两旋钮分工

来源：next/replication-timeout-knob-alignment.md（已立单核销）。
取证基线：主仓 dev；C# 逐处复核后成立，非误报。

C# 事实（已复核）
- libs/server/Servers/GarnetServerOptions.cs:420 ReplicaSyncTimeout = 5s，
  :425 ReplicaAttachTimeout = 60s；libs/host/defaults.conf:355/358 同值。
- attach 级：ReplicaOps/ReplicaDiskbasedSync.cs:180/186 与
  ReplicaOps/ReplicaDisklessSync.cs:171/174 均以
  WaitAsync(GetTimeSpan(REPL_ATTACH_TIMEOUT)) 限时，该配置项经
  libs/server/Config/RuntimeServerConfig.cs:253 回填 ReplicaAttachTimeout。
- 帧级：PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:140 快照逐帧
  驱动取 ReplicaSyncTimeout，:181-184 ExecuteClusterBeginReplicaRecover 亦
  WaitAsync(ReplicaSyncTimeout)；DiskbasedReplication/FileTransmitSource.cs:45/60
  逐块 WaitAsync(timeout)。diskless 扇出flush 限时同取
  DisklessReplication/ReplicaSyncSession.cs:143 WaitAsync(ReplicaSyncTimeout)。
- 主端恢复帧：PrimaryOps/AofOperations/AofSyncDriver.cs ExecuteAttachSyncAsync
  为裸 Task 无显式限时（rust 设有界上界属加严，不改语义）。

改动（wedb/wedb/src/server/replication/）
1. replica_wire.rs：REPLICA_SYNC_TIMEOUT（5s）提 pub(crate) 并订正文档注释
   （覆盖建连 + 帧级/RPC 应答，对应 C# GarnetServerOptions.cs:420）；
   新增 REPL_ATTACH_TIMEOUT（60s）单点常量，对应 GarnetServerOptions.cs:425。
2. assembly.rs recover_replication 第 4 步：initiate_replica_sync 应答限时由
   误取 cluster_node_timeout 改 REPL_ATTACH_TIMEOUT（C#
   ReplicaDiskbasedSync.cs:182-186），订正失真注释（原自称
   WaitAsync(clusterTimeout)）。
3. replica_diskless_sync.rs replica_diskless_attach：execute_cluster_attach_sync
   硬编码 30s 改 REPL_ATTACH_TIMEOUT（C# ReplicaDisklessSync.cs:173-174），
   注释同步。
4. diskless_replication/replica_sync_session.rs begin_aof_sync：主端恢复帧
   wire.attach_sync 硬编码 30s 改 REPL_ATTACH_TIMEOUT 保留有界上界，注释
   声明 C# 该帧无限时、rust 加严。
5. replica_sync_session.rs（diskbased）transmit_checkpoint：逐帧限时由
   cluster_node_timeout 改 REPLICA_SYNC_TIMEOUT（C# ReplicaSyncSession.cs:140）；
   begin_replica_recover_async 补 wait_async(REPLICA_SYNC_TIMEOUT)（C# :184）。
6. snapshot_transmission.rs send_snapshot_data：注释口径由
   「cluster_node_timeout 的单帧粒度」订正为 ReplicaSyncTimeout(5s)。
7. diskless_replication/replication_snapshot_iterator.rs：删除局部
   SYNC_FRAME_TIMEOUT(30s)，三处停等限时单点复用 REPLICA_SYNC_TIMEOUT（C#
   DisklessReplication/ReplicaSyncSession.cs:143 与
   ReplicationSyncManager.cs:318-340 帧级均取 ReplicaSyncTimeout）。

边界：不建 wconf 配置面（两旋钮入配置转写另计）；不触碰任何建连限时机制
（与 task/reject/replication-connect-timeout.md 划界）；cluster_node_timeout
语义专留节点失联判定。

验收：cargo check 零错误零警告。
