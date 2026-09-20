# 副本 ThrottlePrimary 背压生产会话接线与位点校验

来源：next/zcode-r5-repl.md 问题 3

## 问题

process_primary_stream 将 ThrottlePrimary 挂起体写入共享 ClusterReplicationSession.pending_throttle，
但生产副本连接走 ClusterSession 切面，网络泵 drive.rs 仅检查会话自身的 pending_slow，
导致 pending_throttle 槽位无人消费，挂起体被逐帧覆写。
aof-replay-max-lag-bytes 配置完全失效，maxLag==0 锁步与 >0 滞后阻塞均不生效，
连带折叠的 syncReplay 位点一致性校验一并失效。

## 涉及路径

- wedb/wedb/src/server/replication/cluster_replication_session.rs
- wedb/wedb/src/server/cluster_session/replication.rs
- wedb/wnode/src/net/handler/drive.rs
- libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplaySession.cs

## 解决建议

1. 将 throttle 等待体经 ClusterSession 挂入网络泵可轮询的挂起槽位。
2. 恢复 maxLag==0 时批前 replication_offset==tail 位点校验逻辑。
3. 增加对应背压等待与锁步校验的单元测试。
