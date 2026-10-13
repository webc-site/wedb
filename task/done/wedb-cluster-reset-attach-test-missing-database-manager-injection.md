终态：合入 cea59891，provider_with_role 单源补 set_database_manager（形态对齐 boot.rs:264 与 cluster_resp_session.rs:929-948 先例，rig 无 AOF 短路安全），附带修正 replica_receive_checkpoint_retry.rs:133 旧 None 臂恒真伪断言（|taken| !taken 改 |taken| taken，对标 C# TakeOnDemandCheckpointAsync 相等即拍），目标测试与 provider_with_role 全部调用方 15 册抽验绿；gossip_manager::test_meet_from_replica_propagates_across_three_nodes SIGABRT 经 stash 对照确证为 dev HEAD 基线既有红，与本票正交，待另立票。

甄别结论：通过（P3 测试装配回归）。双侧锚亲验成立：rust 侧 cluster_flush_all_slow（replication.rs:141-144）None 臂回 RESP_ERR_SLOW_PATH_STORAGE、boot.rs:264 生产恒注入、cluster_resp_session.rs:929-948 在册测试先例、node_storage::provider_with_role 现码无 manager 注入、diskless_sync_kick.rs:49 expect 断言位、issue_flush_all_async（replica_sync_session.rs:271-272）非 OK 即 Sync(resp) 判败链闭环；C# 侧 ReplicaSyncSession.cs:51 IssueFlushAllAsync 与 SingleDatabaseManager.cs:369 FlushAllDatabases 在册。deviations 无覆盖条目，dev HEAD（6c8de48f 立项 commit）现码无修复，判据未灭失。

cluster_reset_during_attach 重入簇全量同步失败：open_node 装配缺 database_manager 注入，flush 漏斗改造后 CLUSTER FLUSHALL 回 ERR slow path storage error

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# SingleDatabaseManager.FlushAllDatabases 为 storeWrapper 常驻组件（StoreWrapper 构造期注入，无缺席形态），CLUSTER FLUSHALL（RespClusterReplicationCommands.cs:654 storeWrapper.FlushAllDatabases）与全量同步 IssueFlushAllAsync（ReplicaSyncSession.cs）恒可达该漏斗，测试装配与生产装配同构（TestUtils.CreateGarnetServer 构造 StoreWrapper 即含 manager）。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
task/done/wnode-flush-slowpath-pump-drop-skip-aof-broadcast-divergence（合入 ce345a61）把 CLUSTER FLUSHALL 慢路径从 provider.try_store() 直调 store.flush_all_databases() 改为 provider.try_database_manager() 漏斗（wedb/src/server/cluster_session/replication.rs:135-150 cluster_flush_all_slow，None 臂回 RESP_ERR_SLOW_PATH_STORAGE）。生产链恒注入：boot.rs:264 set_database_manager；测试装配两处在册先例：wedb/tests/cluster_resp_session.rs:925-947（注释自陈「与 boot.rs 集群注入段同形」）、cluster_flushall_ns_caller_gate.rs:79。但 wedb/tests/cluster_reset_during_attach.rs 用 open_node 起服（:139 附近）未注入 manager——CLUSTER RESET HARD 取消在途 attach 后重入簇，副本端处理主端 CLUSTER FLUSHALL 复位帧走 try_database_manager() 得 None，回 -ERR slow path storage error，主端 issue_flush_all_async（diskless_replication/replica_sync_session.rs:270）以 Sync("FAILED:cluster flushall failed: ERR slow path storage error") 判败，测试 wedb/tests/common/diskless_sync_kick.rs:49 断言「复位后的节点必须能立即重新入簇」abort（SIGABRT）。改造前旧形态不依赖 manager（try_store 直调），故为 ce345a61 引入的装配面回归。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
无生产危害（生产链 boot.rs:264 恒注入）。危害面为测试装配回归 + 门禁瘫痪：./test.sh fail-fast 于 1236/5478，其后 4242 用例被截断。同类面排查：grep 所有经 open_node / 测试装配起服且触达 manager 漏斗（cluster_flush_all_slow、flush_all_databases、flush_namespace 慢臂）的测试，一次收口防逐个爆雷。

执行方案：
1. wedb_test 的 open_node（或 cluster_reset_during_attach 装配段）补 set_database_manager 注入，形态对齐 boot.rs:264 集群注入段与 cluster_resp_session.rs:925-947 先例；若 open_node 为公共装配，评估全部调用方零回归（nopass 形态 manager 与 store 同源构造）。
2. 同类面排查：grep 测试目录 open_node 调用方中会触达 CLUSTER FLUSHALL / FLUSH 族慢路径漏斗者，确认注入后行为不变；发现同缺陷一并修。
3. 验证：cargo test -p wedb --test cluster_reset_during_attach 全绿；受影响 open_node 调用方抽验绿。

涉及代码：
rust 文件与函数：
wedb/wedb_test/src/node.rs:open_node（装配位）
wedb/tests/cluster_reset_during_attach.rs:cluster_reset_hard_cancels_in_flight_diskless_attach（:137/:139/:333）
wedb/tests/common/diskless_sync_kick.rs:try_full_sync（:49 断言位）
wedb/src/server/cluster_session/replication.rs:cluster_flush_all_slow（:135-150 漏斗依赖）
wedb/src/server/boot.rs:264（生产链注入先例）
wedb/tests/cluster_resp_session.rs:925-947（测试注入先例）
csharp 文件与函数：
garnet/libs/cluster/Server/Replication/ReplicaOps/DisklessReplication/ReplicaSyncSession.cs:IssueFlushAllAsync
garnet/test/cluster/TestUtils.cs（装配恒含 manager 契约先例）
