复制域出站客户端三处缺 TLS 与缓冲池透传

问题分析：
1. Garnet 契约对齐：C# 复制域全部出站客户端建连四锚均带 TlsOptions?.TlsClientOptions（ReplicaDiskbasedSync.cs:112、ReplicaDisklessSync.cs:149、ReplicaSyncSession.cs:103、AofSyncTask.cs:138）。
2. 工程现状确证：rust 侧 apply_tls! 全仓仅 4 调用点（replica_sync_session.rs 两处、failover_session.rs:44、gossip/node_connection.rs:73）；以下三处 GarnetClient 建连无 TLS，扇出处还缺 set_network_pool：replication/assembly.rs:222-227（INITIATE_REPLICA_SYNC 发起）、replica_diskless_sync.rs:115-120（ATTACH_SYNC attach）、diskless_replication/replication_sync_manager.rs:340-345（无盘扇出客户端）。
3. 危害确证：集群 TLS 开启时此三条链为明文出站，集群凭据（cluster_username/password 经 with_auth）走明文通道；缓冲池缺失则扇出高并发下绕过统一收发预算。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/assembly.rs:222
wedb/wedb/src/server/replication/replica_diskless_sync.rs:115
wedb/wedb/src/server/replication/diskless_replication/replication_sync_manager.rs:340
对应 c# 文件与函数：
garnet/libs/cluster/Server/ReplicaDiskbasedSync.cs:112（BeginSendingAofSyncFileToReplica 建链）
garnet/libs/cluster/Server/ReplicaDisklessSync.cs:149
garnet/libs/cluster/Server/ReplicaSyncSession.cs:103

精炼执行方案：
1. 复制域抽单点出站客户端构造（with_auth+set_network_pool+apply_tls!），三处改走
2. 与 refactor-r5 的 C3 [D1] 建链同构收口（establish_replica_stream）合流执行
3. 测试验证点：开启 TLS 的集群装配用例断言三链 ClientSslStream 生效

## 销号注记（2026-09-28 主控）
立案已由 task/done/wedb-replication-egress-clients-missing-tls.md 收口
（合并 6389c39f：三链 apply_tls + tests/replication_egress_tls_gate.rs 对拍册）。
本 issue 的两项前置核查结论：`set_network_pool` 三处在先合入已在位（本票仅补 TLS 面）；
票面 `PrimaryOps/DiskbasedReplication/` 路径系笔误，现树实位见收口记录。
