# wnode 根门面 re-export 单一入口纪律闭环

来源：next/zcode-r5-api.md 问题 4

## 问题

wnode 存在根门面与深路径穿透混用的情况：
wedb 中部分代码引用 wnode::storage::session::storage_session::StorageSession，而根路径 wnode::StorageSession 已存在；
同时 ReplicaCheckpointHook、SlowWait、VectorManager 等跨 crate 稳定符号根门面未收，导致被迫深路径穿透。

## 涉及路径

- wedb/wnode/src/lib.rs
- wedb/wnode/src/aof/mod.rs
- wedb/wedb/src/server/migration/migrate_session_range_index.rs
- wedb/wedb/src/server/replication/cluster_replication_session.rs
- wedb/wedb/src/server/replication/assembly.rs
- wedb/wedb/src/server/migration/migrate_session_vector_set.rs

## 解决建议

1. 将 ReplicaCheckpointHook、SlowWait、VectorManager 等稳定符号上提至 wnode 模块根 re-export。
2. wedb 侧统一改用根路径引用，杜绝深层路径穿透引用。
