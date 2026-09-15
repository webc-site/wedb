# cluster-domain-orphans

来源：next/design.md 条 4（集群域零引用孤儿甄别，21 项）。逐项对照 garnet C# 核实后：4 项跳过保留（别的待办认领）、3 项补链收敛、14 组删除。

## 跳过保留（别的待办认领，不动）

1. args.rs:51 cluster_config_path、cluster_manager.rs:93 init_local → next/glm.md 条 1（集群拓扑落盘 nodes.conf，flush_config 落盘 + 启动恢复 init_local(recover_config=true)）
2. replication_manager.rs:205 get_sublog_replication_offset、replica_replay_driver.rs:66 set_replayed_offset → next/net.md 条 2 / next/ds.net.md 条 3（副本 AOF 位点语义，位点上报挂存储应用完成事件）

## 补链收敛（3 项）

1. failover_timeout_reached 接线
   对标：garnet/libs/cluster/Server/Failover/FailoverSession.cs:27（FailoverTimeout 属性）、ReplicaFailoverSession.cs:97（`if (FailoverTimeout)`）
   改动：wedb/src/server/failover/failover_session.rs:371 `now >= self.failover_deadline` 内联判定收敛为 `self.failover_timeout_reached()`，孤儿入口即生产接线
2. get_worker_info_for_gossip 接线
   对标：garnet/libs/cluster/Server/ClusterConfig.cs:1014 GetWorkerInfoForGossip、Gossip/Gossip.cs:384（InitConnectionsAsync 消费）
   改动：wedb/src/server/gossip/gossip_manager.rs gossip_step_async 第 2 步（直遍 workers 内联收集）改调 ClusterConfig::get_worker_info_for_gossip，循环内保留封禁与自己过滤
3. set_seq_reset_hook 删间接层、reset_sequence_number_generator 直达
   对标：garnet/libs/cluster/Server/Failover/ReplicaFailoverSession.cs:154（failover 接管后 storeWrapper.appendOnlyFile.ResetSequenceNumberGenerator）
   现状：cluster_provider.rs:446 hook 无人注入，reset_sequence_number_generator（failover_session.rs:415 已调用）空转；rust 已有 GarnetAppendOnlyFile::reset_sequence_number_generator（wnode/src/aof/garnet_append_only_file.rs:238，wnode 恢复路径已用）
   改动：cluster_provider.rs reset_sequence_number_generator 改为 `try_aof().reset_sequence_number_generator()` 直达委托；删 seq_reset_hook 字段与 set_seq_reset_hook

## 删除（14 组，删后 js/check/ignore 登记）

1. gossip_session.rs 整文件：GossipSession::handle_gossip 与 cluster_session.rs:211 network_cluster_gossip（CLUSTER GOSSIP 命令臂，C# RespClusterBasicCommands.cs:NetworkClusterGossip 对应）重复实现；gossip/mod.rs 去注册
2. cluster_manager.rs:73 unsafe_set_config：C# ClusterManager.cs:29 UnsafeSetConfig 为 benchmark-only（C# 注释 NOTE: Unsafe! DO NOT USE），生产零调用；rust cfg(test,bench) 门控下零引用
3. cluster_config.rs:378 is_migrating_slot：C# 唯一消费点 MigrateCommand.cs:212（顶层 MIGRATE sketch 构建，属迁移发起端）；rust 顶层 MIGRATE 发起臂未立项（遗留记录）；slot_verify 用 get_state 判定系 C# WaitForSlotToStabalize 语义非重复定义。is_importing_slot 有消费（cluster_migrate_slow）保留
4. cluster_config.rs:551 get_slot_count_for_state：get_info 已用 slot_state_counts 单遍扫描承接 C# GetSlotCountForState 全部四个状态计数需求（cluster_manager.rs:222），两套并存
5. cluster_manager_worker_state.rs:137 list_replicas：C# 本身是对 GetReplicas 的一行透传；rust CLUSTER REPLICAS 臂（cluster_session.rs:1371）已直接消费 get_replica_ids 承接
6. args.rs:44 cluster_bus_port + cluster_port 字段 + CLUSTER_BUS_PORT_OFFSET 常量 + wbase/src/hash_slot.rs:36 DEFAULT_BUS_PORT_OFFSET：C# 无独立 bus 端口概念（gossip 走业务端口 CLUSTER GOSSIP 命令），rust 发明物
7. cluster_config.rs:194 local_node_endpoint：C# 仅两处 clientName 日志标识（AofSyncTask.cs:141、GarnetServerNode.cs:94）；rust client 名已用 node_id 承接辨识语义（replica_wire.rs:203）
8. cluster_config.rs:211 get_local_node_replica_ids：C# 全仓（生产+测试）零调用
9. cluster_config.rs:307 get_remote_node_ids：C# 全仓零调用
10. cluster_config.rs:359 get_host_name_from_node_id：get_slots_info/append_formatted_slot_info 同循环已持 Worker 引用直取 hostname（cluster_config.rs:1419/1430），getter 会造成双查找
11. cluster_config.rs:520 get_replica_endpoints：C# 全仓零调用
12. cluster_config.rs:582 get_worker_node_id_from_address_or_hostname：C# 唯一消费者 MigrateCommand.cs:116/131（顶层 MIGRATE 发起端）；rust REPLICAOF/FAILOVER 按 C# 原样用 get_worker_node_id_from_address（ReplicaOfCommand.cs:71、FailoverCommand.cs:78）
13. cluster_session.rs:194 set_replicating（连带 is_replicating getter 与 is_replicating 字段）：C# IsReplicating 置位点在 APPENDLOG 握手（RespClusterReplicationCommands.cs:218）；rust 以 replication_manager.has_active_replication_stream（重放驱动注册表）为权威状态面（cluster_provider.rs:250 消费），会话级标志位是被替代机制残留，全仓无 getter 消费者
14. aof_sync_driver_store.rs:311 assert_does_not_exist：C# ReplicaSyncSession.cs:299 调用点语义（旧驱动已终止校验 + Add）由 rust attach_replica_wire 的 try_remove 置换语义显式替代（replica_sync_session.rs:50，注释已声明对标）

## 验收口径

1. ./clippy.sh 零警告（禁 allow）
2. ./test.sh 全量通过
3. bun ./js/check.js 无新增缺失；删除符号在 js/check/ignore/ 对应 yml 登记（文件 + 函数名 + 原因）
4. 全仓 grep 确认删除符号引用清零

## 验证结果

- 分支 w1-cluster-orphans（fork 后 4 commit，已合并回 dev 并删除）
- ./clippy.sh 零警告（无 allow）
- ./test.sh 全量通过：wedb 工作区 1992 项 + regress 2 项，0 失败
- bun ./js/check.js 无输出（0 缺失 0 重复）；删除符号在 js/check/ignore/libs_cluster_Server.yml 与 libs_server_Cluster.yml 登记
- 甄别修正两处（对照 C# 后与意见预设不同）：
  1. is_migrating_slot 选删除而非收敛：slot_verify 的 Migrating 判定走 get_state（C# WaitForSlotToStabalize 语义），非 IsMigratingSlot 重复定义；C# 唯一消费点 MigrateCommand.cs:212 属顶层 MIGRATE 发起端，rust 未立项该命令臂
  2. get_worker_info_for_gossip 选补链而非删除：C# Gossip.cs:384 生产在用，rust gossip_step_async 第 2 步内联重写收敛为调用该函数
- 遗留（超出本任务范围，记录不动）：顶层 MIGRATE 发起命令臂缺失（C# MigrateCommand.cs，ClusterSession partial，含 sketch 构建与 GetWorkerNodeIdFromAddressOrHostname 消费），与 next/net.md 条 3「六集群命令无臂」同类，建议另行立项
