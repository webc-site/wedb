aof-driver-register-pre-transfer 已完成并合并

来源 next/aof-driver-register-pre-transfer.md。取证基线 dev HEAD 306d32da；合并时 HEAD 4c1bb619。

## 结论
把副本同步驱动注册提前到快照/记录流传送之前，两条链（diskbased 与 diskless）均在传送开始前
以驱动起始位点钉住安全截断线，避免传送窗口内 FastAofTruncate 越过覆盖位导致后续 try_add 被拒、
attach 以 "Failed trying to try update replication task" 收场、diskless 侧 data_loss_check 拒绝。

## C# 对位
- diskless：garnet/libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:240-255
  pauseAofTruncation while 环，TryAddReplicationDrivers start=Log.BeginAddress 先于流式快照，:254 二次更新。
- diskbased：garnet/.../DiskbasedReplication/ReplicaSyncSession.cs:303 AcquireCheckpointEntryAsync 内
  TryAddReplicationDriver 先于快照下发，:199 恢复往返后以授予位点二次更新。

## 实现（rust，agent 提交 88ce745c，base 14200dac；本仓合并 25147da3）
- wedb/wedb/src/server/replication/replica_sync_session.rs（diskbased）：向 send_checkpoint_and_recover
  透传 local_node_id；在快照流+恢复往返之前先建 pin driver（start=检查点覆盖下界）并 try_add，失败即摘除；
  既有 attach_replica_wire 保留为二次更新。
- wedb/wedb/src/server/replication/diskless_replication/replication_sync_manager.rs：引入 AofSyncDriver；
  run_snapshot_fanout 前在 Log.BeginAddress 批量 try_add_replication_drivers 带截断重试环；
  main_streaming_snapshot_driver 清扫失败会话的 pin，防泄漏 pin 永久阻塞截断。
- wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs：仅注释，
  说明 begin_aof_sync 的 try_add 是二次更新。

## 合并前甄别
dev 合并前 initiate_replica_sync 顺序仍为 send_checkpoint_and_recover 后才 attach_replica_wire
（:103 try_add 在 attach 内 = 传送后），即缺口仍在、非 fleet 重复实现；fleet 自 14200dac 起未触碰
本 3 文件，三方合并且无冲突。合并后 cargo check -p wedb --all-targets 复核。
