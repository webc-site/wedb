# 轮8 网络协议 / 共识 gossip / 主从同步复制 / 槽位迁移 审查报告

审查范围与方法
对照 garnet/libs/cluster 与 garnet/libs/server/Replication 及 rust 侧 wedb/wnode、wedb/wedb/src/server/。
检查网络驱动、集群协议、共识合并、复制同步、槽位迁移的实现完整性、逻辑正确性、不变量维护与符号映射。

1. CLUSTER RESET (SOFT 与 HARD) 槽位键判定短路与清库绕过多库
具体问题：
1) cluster_reset_slow 宏 run_readonly_storage_slow! 构造的 StorageSession 默认绑死在 (ns=0, db=0)。
2) try_reset 内调用 storage.has_keys_in_slots(&slots) 时，has_keys_in_slots 使用 self.session_slot()（槽位 0）与传入的槽位集合匹配（!slots.contains(&slot) 即返回 Ok(false)）。若节点负责的槽位不包含槽位 0，判定直接短路返回无键，导致即便分配的槽位中存有大量用户键，SOFT / HARD 复位依然放行并清空槽位映射；即便包含槽位 0，也只探测了 db 0，其他 active_db 的槽位键完全漏判。
3) HARD 复位分支 (!soft)，cluster_reset_slow 仅调用 storage.delete_all_user_keys().await，该方法底层仅对当前会话所在的单一数据库 (0, 0) 执行 flush_database，其他 active_db > 0 的数据完全未被清除，且对只读会话执行删除破坏了只读约束。C# 对应逻辑为 clusterProvider.FlushDB(true)，直接截断全仓底层存储日志。
rust 文件与函数：
wedb/wedb/src/server/cluster_session/basic.rs: cluster_reset_slow (:126)
wedb/wedb/src/server/cluster_manager_worker_state.rs: try_reset (:71)
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs: has_keys_in_slots (:216)
wedb/wnode/src/storage/session/common/db_admin_functions.rs: delete_all_user_keys (:32)
c# 对应文件与函数：
libs/cluster/Server/ClusterManagerWorkerState.cs: TryReset
libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs: HasKeysInSlotsScan
libs/cluster/Session/RespClusterBasicCommands.cs: NetworkClusterReset
libs/cluster/Server/ClusterProvider.cs: FlushDB

2. DELKEYSINSLOT / DELKEYSINSLOTRANGE 仅删除 String 键且破坏只读会话约束
具体问题：
1) for_each_db_in_slot 创建的是 StorageSession::new_readonly(batch)。但在 del_keys_in_slots_slow 中，该只读会话被传给 delete_slot_keys 并执行删除写操作，破坏了 new_readonly 契约。
2) delete_slot_keys 实现仅调用 self.string_keys_snapshot() 获取 String 键并逐一调用 self.delete_string(&key)，完全忽略了复合对象（Hash, List, Set, ZSet）以及 RangeIndex 索引树。在 C# 中，DeleteSlotKeys 通过 unifiedBasicContext 快照遍历主存储与对象存储，并调用统一的 storageSession.DELETE。当前 rust 实现会导致属于待删槽位的对象和树结构数据完全残留在节点中。
rust 文件与函数：
wedb/wedb/src/server/cluster_session/slot_mgmt.rs: for_each_db_in_slot (:107)
wedb/wedb/src/server/cluster_session/slot_mgmt.rs: del_keys_in_slots_slow (:171)
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs: delete_slot_keys (:255)
c# 对应文件与函数：
libs/cluster/Server/ClusterManagerSlotState.cs: DeleteKeysInSlots
libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs: DeleteSlotKeys

3. 槽位迁移与键迁移 DELETING 阶段 RangeIndex 删键被 MigrationBusy 拦截
具体问题：
1) 当 !spec.copy_option 迁移成功进入 DELETING 阶段后，slots.rs 与 keys.rs 均使用 storage.delete_string(key) 清理源节点数据。对复合对象，虽可通过降级走异步 delete，但在概念与抽象上混淆了 String 删除与通用用户键删除。
2) 在 migrate_range_index_keys_async 中，RangeIndex 键迁移完毕后调用 storage.delete_string(key)。但该键在迁移开始时已被 store.range_index.claim_key_for_migration 标记为 claimed；delete_string 降级到 collection::delete 时触发 self.store.range_index.migration_claimed(&meta_k) 检测，直接返回 Err(Error::MigrationBusy)，导致源端 RangeIndex 删除必定失败并打印错误日志，造成源端残留废弃索引树与孤儿元数据。C# 对应方法为 migrateOperation.DeleteRangeIndex。
rust 文件与函数：
wedb/wedb/src/server/migration/migrate_driver/slots.rs: execute_slots_migration (:250)
wedb/wedb/src/server/migration/migrate_driver/keys.rs: execute_keys_migration (:913)
wedb/wedb/src/server/migration/migrate_session_range_index.rs: migrate_range_index_keys_async (:144)
wedb/wkv/src/session/collection.rs: delete (:43)
c# 对应文件与函数：
libs/cluster/Server/Migration/MigrateOperation.cs: DeleteKeys
libs/cluster/Server/Migration/MigrateOperation.cs: DeleteRangeIndex
libs/cluster/Server/Migration/MigrateSession.RangeIndex.cs: MigrateRangeIndexKeysAsync

5. check.js 符号重复定义：Gossip 连接仓重复锚定 GetOrAddAsync
具体问题：
get_or_add_entry 为内部实现元组返回函数，get_or_add 为公开接口包装，二者同时使用了完全相同的文档注释锚点，导致符号门禁 check.js 报告重复定义。
rust 文件与函数：
wedb/wedb/src/server/gossip/connection_store.rs: ConnectionStore::get_or_add_entry (:128)
wedb/wedb/src/server/gossip/connection_store.rs: ConnectionStore::get_or_add (:172)
c# 对应文件与函数：
libs/cluster/Server/Gossip/GarnetClusterConnectionStore.cs: GetOrAddAsync

6. check.js 符号重复定义：测试文件越界主张生产对位方法 NeedToFullSync
具体问题：
测试用例在文档注释中直接使用了生产代码对位锚点格式，而非测试映射规范，导致与生产实现冲突，被 check.js 判定为同一 C# 生产符号在两个 Rust 文件中重复声明。
rust 文件与函数：
wedb/wedb/src/server/replication/replication_manager.rs: ReplicationManager::diskless_resync_strategy (:896)
wedb/wedb/tests/replication_manager.rs: test_diskless_resync_strategy_need_full_sync_conditions (:461)
c# 对应文件与函数：
libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs: NeedToFullSync

7. ReplicaSyncSessionTaskStore 缺失结构化方法注释锚点
具体问题：
replica_sync_task_store.rs 的结构体头标注了 C# 文件路径，但内部核心方法 try_add、try_remove、clear 仅在自然语言注释中提及 C# 方法，缺少单行标准锚点格式（TryAddReplicaSyncSession 等），在 check.js 扫描中被列为实现缺失项。
rust 文件与函数：
wedb/wedb/src/server/replication/replica_sync_task_store.rs: try_add (:33)
wedb/wedb/src/server/replication/replica_sync_task_store.rs: try_remove (:43)
wedb/wedb/src/server/replication/replica_sync_task_store.rs: clear (:50)
c# 对应文件与函数：
libs/cluster/Server/Replication/PrimaryOps/ReplicaSyncSessionTaskStore.cs: TryAddReplicaSyncSession, TryRemove, Clear, Dispose, GetNumSessions, IsFirst

8. Gossip 封禁列表清理缺少 Expired 判据结构化锚点
具体问题：
cluster_manager.rs:cleanup_ban_list 在注释正文中提到了 C# Gossip.cs 的 Expired，但缺少标准的单行文档锚点，导致 check.js 报告 Gossip.cs:Expired 仅为词元提及。
rust 文件与函数：
wedb/wedb/src/server/cluster_manager.rs: cleanup_ban_list (:656)
c# 对应文件与函数：
libs/cluster/Server/Gossip/Gossip.cs: Expired

9. 检查点下发 SendCheckpointAsync 文档注释路径前缀截断
具体问题：
注释写为 ReplicaSyncSession.cs:SendCheckpointAsync，缺失了 libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ 完整路径前缀，导致 check.js 将其归入未完整文档化的词元提及。
rust 文件与函数：
wedb/wedb/src/server/replication/replica_sync_session.rs: initiate_replica_sync (:115)
c# 对应文件与函数：
libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs: SendCheckpointAsync
