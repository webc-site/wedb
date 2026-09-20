# 迁移路径墓碑键无锁覆盖补充

来源：next/zcode-r2-test.md 缺口 3。

## 问题
C# `ClusterMigrateSlotWithTombstones`（test/cluster/Garnet.test.cluster.migrate/ClusterMigrateTests.cs）测试了带墓碑（已删除键残留）的槽位迁移时，目标端不会错误复活已删除键，也不会因 tombstone 投毒。
rust 当前在 `wedb/tests/cluster_migration.rs` 覆盖了对象信封、chunk 重排、TTL、权限与恢复等，但唯独缺少带墓碑键的迁移用例。

## 目标
在 `wedb/tests/cluster_migration.rs` 中增加单测：
- 在待迁移 slot 写入若干键，并使用 DEL 删除其中部分（产生墓碑记录）；
- 触发 CLUSTER MIGRATE / MIGRATE 执行该 slot 迁移；
- 验证：存活的键迁移至目标端且值正确，已删除的墓碑键在目标端不存在，且不报任何迁移或解析错误。

## 验收
1. cargo check -p wedb --tests 0 error 0 warning。
2. wedb/tests/cluster_migration.rs 新增测试通过。
