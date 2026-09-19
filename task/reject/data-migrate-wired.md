拒绝：MIGRATE「有名无实/发了必错/无执行体」指控不实，不另立项

来源：next/agy.data.md 条 9、next/muse.data.md 条 2（同题合并）
结论：不成立（取证不实：MIGRATE 有完整执行体，且为集群槽/键迁移语义，非缺失）

引证（rust 链路完整）：
- wedb/wnode/src/resp/parser/command_table.rs:132 线名 MIGRATE → RespCommand::Migrate
- wedb/wnode/src/resp/resp_server_session.rs:1707 将 Migrate 与 Failover/Replicaof 一道路由集群通道（对标 C# 集群命令分派形态）
- wedb/wedb/src/server/cluster_session/mod.rs:239 `RespCommand::Migrate => self.network_try_migrate(args, output, slot)` 有承接臂
- wedb/wedb/src/server/cluster_session/migrate.rs:270 network_try_migrate 完整实现：支持 Redis MIGRATE host port key destination-db timeout [COPY] [REPLACE] [AUTH/AUTH2] [KEYS] [SLOTS] [SLOTSRANGE] 参数形状，非占位
- C# 对标：garnet/libs/cluster/Session/MigrateCommand.cs NetworkTryMIGRATE（顶层分派 ClusterSession.cs:110），RespServerSession 无 MIGRATE 分派（grep 零命中）——两边同为集群会话语义，rust 未缺
- 两档声称「raw.rs/slow.rs 无 C::Migrate 臂 → 落入未知命令报错」的前提即错：MIGRATE 不经 raw/slow 分派，在会话层已提前路由集群通道

「注释写清 MIGRATE=集群迁移」的动作不再单独立项：migrate.rs:253-269 文档注释已完整写明参数形状、C# 对标与差异说明。
