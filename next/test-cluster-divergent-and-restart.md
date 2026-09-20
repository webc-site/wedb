# 复制流重启与分叉历史集成测试对标

来源：next/zcode-r2-test.md 缺口 1。

## 问题
C# ClusterReplicationBaseTests.cs 覆盖了：
- `ClusterSRPrimaryRestart`：主节点重启后副本经检查点重新挂载；
- `ClusterSRNoCheckpointRestartSecondary`：副本无检查点重启后重新对齐；
- `ClusterDivergentReplicasTest`：分叉历史拒绝与重对齐。

## 目标
在 `wedb/tests/` 中扩充主备重启与分叉重放场景，锁住主从拓扑在各种重启和位点偏移状态下的稳定对齐与恢复。

## 验收
1. cargo check -p wedb --tests 0 error 0 warning。
2. 相关测试运行通过。
