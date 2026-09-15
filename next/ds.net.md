# ds.net 待办

8. [P2] 巨型文件与超长函数拆分
   位置：wedb/wedb/src/server/cluster_session.rs（2116 行，process_cluster_commands:841-1676 单函数 836 行）；cluster_config.rs 1683 行；cluster_provider.rs 1131 行；replication/replication_manager.rs 1090 行
   对标：garnet/libs/cluster/Session/RespClusterBasicCommands.cs、RespClusterSlotManagementCommands.cs、RespClusterMigrateCommands.cs、RespClusterReplicationCommands.cs、RespClusterFailoverCommands.cs
   问题：单函数混合五类命令分发，慢路径 helper 虽已独立（:1940/:1964/:1993/:2013/:2043）但主分发体仍超阈值。
   改法：process_cluster_commands 按 C# 五文件切成五个 impl 块（Rust 同类型跨文件多 impl）；cluster_provider 的 INFO 统计段与资产注入段分文件。

21. [P2] ignore 面拆块与理由修正
    位置：js/check/ignore/cluster.yml:497-684（单块约 190 行共用一句笼统理由）、js/check/ignore/server.yml:39-40
    对标：garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/**（未实现）；garnet/libs/cluster/Server/Replication/ReplicationNetworkBufferSettings.cs（等价实现在 wedb/wedb/src/server/replication/network_buffer.rs:67/:147）；garnet/libs/cluster/Session/SlotVerifiedState.cs、TransferOption.cs（等价枚举 slot_verify.rs、migration_manager.rs）；garnet/libs/cluster/Session/ClusterKeyIterationFunctions.cs（count/get/del keys in slot 已实现）
    问题：未实现项（DiskbasedReplication 检查点传输族、ChunkedRecordReassembler、MigrateSession 对象/RangeIndex 迁移族、GarnetServerNode.GetMostRecentConfig）理由未写「未实现」；已实现等价物条目未删；server.yml 的 ServerTcpNetworkHandler 整文件忽略与 wnode/src/net/handler.rs:3 自述对标冲突；MigrateSessionKeyAccess 块内已实现的 CanAccessKey（migrate_session.rs:123）与被忽略的 WaitForConfigPropagationAsync 混装。
    改法：按「未实现 / 已实现等价 / C# 专属不可移植 / 注释形态受限」拆至少四块改写理由，已实现条目补标准注释后由 check.js 自动淘汰。

22. [P2] 注释路径勘误与补注
    位置：wedb/wedb/src/server/cluster_session.rs:1162/:1194（NetworkClusterCountKeysInSlot / NetworkClusterGetKeysInSlot 误标 ClusterCommands.cs，应改 RespClusterSlotManagementCommands.cs）、:1940/:1964/:1993（count_keys_in_slot_slow / get_keys_in_slot_slow / del_keys_in_slots_slow 的 /// 非标准「libs/….cs:函数」格式，check.js 不认，对应 ignore 条目滞留）、:2013/:2043（cluster_flush_all_slow / cluster_migrate_slow 缺 C# 对标注释）；wedb/wedb/src/server/replication/driver_registry.rs:25（通用 DriverRegistry 误标 AofSyncDriverStore.cs:AofSyncDriverStore）
    对标：garnet/libs/cluster/Session/RespClusterSlotManagementCommands.cs、garnet/libs/cluster/Session/RespClusterMigrateCommands.cs
    问题：错路径与类型面误指被 check.js 盲区（普通 // 计入覆盖、重复检测只遍历 function_item）掩盖。
    改法：按正确 C# 路径改写并补齐标准 ///，使对应 ignore 条目被 check.js 自动淘汰。
