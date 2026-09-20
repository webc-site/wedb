# 主端 AOF 背压闸门生产装配接线

来源：next/zcode-r5-repl.md 问题 4

## 问题

CONFIG SET aof-sync-max-lag-bytes 经 config_owner 热调 gate.set_budget 点亮闸门，
但 AofSyncDriverStore.attach_backpressure 在生产装配路径未被调用，
导致发布链中的 backpressure 恒为 None，shipped_watermark 恒为 i64::MAX。
慢副本拖主保护机制不可达。

## 涉及路径

- wedb/wedb/src/server/replication/aof_sync_driver.rs
- wedb/wedb/src/server/replication/assembly.rs
- wedb/wedb/src/server/cluster_provider/assets.rs
- wedb/wnode/src/aof/garnet_log/single_log_branch.rs
- libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs
- libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs

## 解决建议

1. 在 set_aof 装配点同步调用 attach_backpressure，注入 aof.backpressure()。
2. 接通发布端后验证 AnyStalled 与 ADVANCE_TIME 脉冲解锁逻辑。
3. 增加慢副本背压门控与解冻测试。
