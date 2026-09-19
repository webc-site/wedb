INFO replication 主端 slaveN 行改走 RoleInfo 投影与 C# ToString 同形，端点 ip/port 对外可见

来源：next/info-replication-slave-line-endpoint.md（glm.net 第 1 条）。取证基线：主仓
/Users/z/git/db/wedb 分支 dev，行号按当下代码复核。

结论
票面成立，且票内两处对原报的更正经 C# 源码核实无误。C# 主端 slaveN 行是
RoleInfo.ToString()（garnet/libs/server/Cluster/RoleInfo.cs:44-47 的
ip=..,port=..,state=..,offset=..,lag=..,sequenceNumber=..），由
garnet/libs/cluster/Server/ClusterProvider.cs:263-266 逐项 ToString 入列；sequenceNumber
在 C# 的 INFO 取数面 AofSyncDriverStore.GetReplicaInfo
（garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:177-207）
确实未赋值，打印默认零值（default AofAddress 的 ToString 无条件先写 addresses[0]，即 0），
「max send 时间戳」属 AofSyncTask 的 ADVANCETIME 脉冲面。rust 侧此前 INFO 段绕开
get_primary_info 已有的 RoleInfo 投影（端点反查在
wedb/wedb/src/server/cluster_provider.rs:get_primary_info 内经
get_worker_address_from_node_id 完成），直出内部 node_id，监控按 ip=/port= 解析读不到端点。

改动
1. wedb/wnode/src/role_info.rs：RoleInfo 实现 Display，唯一格式化出口逐字段对标
   garnet/libs/server/Cluster/RoleInfo.cs:ToString；sequence_number 字段文档注释点名
   C# GetReplicaInfo 未赋值、INFO 输出默认零值的口径；新增单测
   metrics_line_matches_csharp_tostring 锁定行形态（含端点未知时空 ip、port 兜底）。
   对标 C# RoleInfo.cs:ToString。
2. wedb/wedb/src/server/cluster_provider.rs:get_replication_info 主端分支改消费
   self.get_primary_info() 的 Vec<RoleInfo> 投影逐项 to_string 入列，删除手写
   node_id=/state=/offset=/lag= 的 format! 字面量与 INFO 段第二处
   aof_sync_driver_store.get_replica_info 直连；ReplicaRoleInfo
   （wedb/wedb/src/server/replication/aof_sync_driver.rs 的 get_replica_info 产出）
   保持内部身份不再对外渲染。node_id= 不保留，对外字段以 C# 逐字段对位为准。
   对标 garnet/libs/cluster/Server/ClusterProvider.cs:GetReplicationInfo 主端分支。

不变面
ROLE 命令（wedb/wnode/src/resp/admin_commands.rs:network_role）逐字节不变：其消费
RoleInfo 结构化字段而非格式化出口；get_primary_info 投影逻辑未动，端点反查仍单点。
全仓无既有测试断言旧 node_id= 形态（grep 仅此一处渲染点），无 wmetric 断言需改。

验证
cargo check --workspace --all-features 零 error 零 warning；
cargo test -p wnode --lib metrics_line_matches_csharp_tostring 通过。
（--all-targets 下 wnode/tests/tls_test.rs 有 4 处 futures_util/compio read trait
歧义 E0034，为该测试在 all-features 组合下的既有问题，与本单两文件改动无交集。）
