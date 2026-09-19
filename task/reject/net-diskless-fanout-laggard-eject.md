裁决：不成立（指控与实现不符：rust 扇出已是逐会话独立超时 + 失败即摘除；且 C# 扇出本体就是 lockstep 停等设计，「更快剥离」是 C# 没有的优化）
来源：next/agy.net.md 条 9。核销 2026-09-19。

一句话结论：fan_out_send 对批内每个会话并发发送、每会话独立 REPLICA_SYNC_TIMEOUT(5s) 超时、
任一会话失败/超时/被拒立即 set_status(Failed) 摘除、其余会话继续——指控的「缺乏单副本熔断与剥离」
不存在；而「慢副本在超时窗口内拖慢整批」正是 C# lockstep（缓冲齐步走）的原始设计。

逐条核销
1. rust 实测：wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs:84-117
   fan_out_send：join_all 逐会话并发，:95-99 每会话独立 timeout(REPLICA_SYNC_TIMEOUT, execute_cluster_sync)，
   :108-110 失败即 s.set_status(SyncStatus::Failed) 摘除，:113-115 全批失败才中止。REPLICA_SYNC_TIMEOUT=5s
   （replica_wire.rs:248，对标 C# ReplicaSyncSession.cs:140/:184 WaitAsync 默认口径）。
2. C# 实测：garnet/libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSnapshotIterator.cs:159-178
   FanOutRecordSpan 与 :188-215 FanOutChunk——「Fan a whole record span out to all active sessions in
   lockstep ... a full buffer flushes all and retries」（:156-158 官方注释），满者 SetFlushTask 后
   BlockingWait(WaitForFlushAsync) 等齐，失败会话由 WaitForFlushAsync 内 Sessions[i].Failed → null 摘除。
   停等等齐是 C# 明确设计前提（缓冲齐步走保证分块对齐），慢副本在 flush 失败/超时前同样拖住 C# 整批。
3. rust 注释 :80-83 已自标该对标关系（SetFlushTask 失败收敛 + Sessions[i].Failed 置 null）。
4. 要求的「单帧更短超时即时熔断」无 C# 对标物，属自造优化，与 transpile 1:1 原则冲突；如对 5s 值有
   异议，正确路径是把 C# 的 ReplicaSyncTimeout 配置面转写过来（replica_wire.rs:246-247 注释已声明该
   缺口属配置转写范畴），而非另造剥离算法。
