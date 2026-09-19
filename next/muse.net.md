review-net 待办：网络协议 共识 同步 迁移

check.js 现状
check.js 重复 20 组，实现缺失仅 3 项且都不在 net 域内。net 域零缺失，问题集中在锚点复挂与少量重复实现待收敛。
缺失三项原文：libs/common/RespWriteUtils，libs/server/Lua/NativeMethods，libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable

1. SingleKeySlotVerify 锚点复挂
问题：同一 C# 函数被挂到状态机本体和迭代门评两个不同函数，check.js 报重复。evaluate 侧是挂起扩展，不是单键状态机。
rust：wedb/wedb/src/server/cluster_manager_slot_gate.rs fn evaluate_iterative_key_gate
对应 C#：libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs fn NetworkIterativeSlotVerify
保留：wedb/wedb/src/server/slot_verify.rs fn single_key_slot_verify 对 libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs fn SingleKeySlotVerify
动作：只改前者注释锚点。

2. GarnetClientSession 构造锚点一挂四处
问题：构造语义被挂到两个注入器加一个自由函数，set 与 resolve 并非构造。
rust：wedb/wconn/src/session.rs fn GarnetClientSession::new 保留；wedb/wconn/src/session.rs fn set_network_pool，wedb/wconn/src/client.rs fn set_network_pool，wedb/wconn/src/network/pump.rs fn resolve_network_pool 去构造锚点
对应 C#：libs/client/ClientSession/GarnetClientSession.cs fn GarnetClientSession；libs/cluster/Server/Migration/MigrationManager.cs prop GetNetworkPool
动作：非构造只留回退说明。

3. ConnectAsync 锚点挂到 TLS 握手子步骤
问题：ClientTlsConfig::connect 只是 AuthenticateAsClientAsync 等价物，被复挂 ConnectAsync 全量，与 connect_async 撞车。
rust：wedb/wconn/src/tls.rs fn ClientTlsConfig::connect 去 ConnectAsync 锚点
对应 C#：libs/client/GarnetClient.cs fn ConnectAsync 保留给 wedb/wconn/src/client.rs fn GarnetClient::connect_async
动作：只改注释。

4. GarnetClient 构造锚点挂到 facade 注入器
问题：wconn 主体已挂 new，wedb facade 的 set_tls 又挂同一构造，主从不分。
rust：wedb/wedb/src/client.rs fn GarnetClient::set_tls 去锚点；保留 wedb/wconn/src/client.rs fn GarnetClient::new
对应 C#：libs/client/GarnetClient.cs fn GarnetClient
动作：只改 facade 注释。

5. StartAsync 锚点挂到 finally 子动作
问题：consumer_finish 只是 done.Set 收尾，被复挂 StartAsync 全量，与宿主任务撞车。
rust：wedb/wpubsub/src/subscribe_broker.rs fn SubscribeBroker::consumer_finish 去 StartAsync 锚点，改挂 Dispose 收口；保留 wedb/wnode/src/service.rs fn spawn_pubsub_consume_task
对应 C#：libs/server/PubSub/SubscribeBroker.cs fn StartAsync fn Initialize fn Dispose
动作：只改注释。

6. HandleNewConnection 锚点挂到容量门子步骤
问题：try_acquire_connection 只是 Increment 加 limit 门子步骤，被复挂全量，与 process_stream 撞车。
rust：wedb/wnode/src/servers/consumer_registry.rs fn ConsumerRegistry::try_acquire_connection 缩小锚点或去锚点；保留 wedb/wnode/src/net/handler/drive.rs fn NetworkHandler::process_stream
对应 C#：libs/server/Servers/GarnetServerTcp.cs fn HandleNewConnection
动作：只改注释。另核对 HandleAcceptError 退避是否在 server.rs accept 循环对齐。

7. GetSslServerAuthenticationOptions 一挂三处
问题：两工厂加一装配单点同挂一键，check.js 报重复。
rust：wedb/wnode/src/tls/config.rs fn server_config 保留；fn ServerTlsConfig::from_pem_files 与 fn ServerTlsConfig::from_der 去该锚点改挂构造说明
对应 C#：libs/server/TLS/GarnetTlsOptions.cs fn GetSslServerAuthenticationOptions
动作：只改注释。出站 wedb/wconn/src/tls.rs fn ClientTlsConfig::new 对 GetSslClientAuthenticationOptions 单挂不变。

8. TLS PEM 解析两套完全重复
问题：load_certs 与 load_private_key 在 wnode 与 wconn 各写一份逐行同形，违反一处定义。SKILL 要求 rustls-pemfile 单一解析栈。
rust：wedb/wnode/src/tls/config.rs fn load_certs fn load_private_key；wedb/wconn/src/tls.rs fn load_certs fn load_private_key
对应 C#：libs/server/TLS/GarnetTlsOptions.cs fn GetCertificateIssuer 相关载入段
动作：下沉到 wbase 或 wconf 共用，两侧留薄包装。只审查不改。

9. can_access_key 转发层与本体同名并存
问题：manager 侧只是遍历转发，会话侧才是 MigrateSessionKeyAccess 本体，易误读为重复。
rust：wedb/wedb/src/server/migration/migration_manager.rs fn MigrationManager::can_access_key；wedb/wedb/src/server/migration/migrate_session.rs fn MigrateSession::can_access_key
对应 C#：libs/cluster/Server/Migration/MigrateSessionKeyAccess.cs fn CanAccessKey
动作：转发侧去锚点或注明转发，或内联调用方。

10. ConsumerRegistry 进程级安装链路不一致
问题：install_global 只在 NodeService::from_parts 内调用，GarnetServer 直装路径无安装，CLIENT 经 global 直取在该路径取空。
rust：wedb/wnode/src/service.rs fn NodeService::from_parts 内 registry.install_global；wedb/wnode/src/server.rs fn start_server_monitor 只装 monitor；wedb/wnode/src/resp/client_commands.rs 消费点 ConsumerRegistry::global
对应 C#：libs/server/Servers/GarnetServerBase.cs activeHandlers 实例直持；libs/server/Servers/GarnetServerTcp.cs fn TryCreateMessageConsumer fn DisposeMessageConsumer
动作：统一安装到 ServerBootstrap 装配单点，或 GarnetServer 构造内安装并补回归。

11. gossip meet 超时与版本门口径分散
问题：版本先验后反序列化与 C# 一致，但建连超时口径散在 manager 与 node_connection 两层。
rust：wedb/wedb/src/server/gossip/gossip_manager.rs fn GossipManager::try_meet_async；wedb/wedb/src/server/gossip/node_connection.rs fn NodeConnection::try_meet_async fn try_gossip_async
对应 C#：libs/cluster/Server/Gossip/Gossip.cs fn TryMeetAsync；libs/cluster/Server/Gossip/GarnetServerNode.cs 建连超时；libs/cluster/Server/Gossip/GarnetClientExtensions.cs 帧编解码
动作：超时与版本门注释收敛一处。

12. sync_transport 自定义扩展需显式声明
问题：两流式共用件 C# 无同名，文件头已声明不挂锚点是对的，但调用方两链各写编排易被补错锚点。
rust：wedb/wedb/src/server/sync_transport.rs fn transmit_range_index_stream fn transmit_vector_set_frames
对应 C#：libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs fn WriteOrSendRecordAsync 系列；libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs 快照链
动作：保持无锚点，调用方注明自定义扩展。

13. failover replica 长会话核对泛型抹平风险
问题：rust 已按 manager session 拆分，但竞速 abort 共用 race_abort 泛型，需核对 C# 主副两类差异是否被抹平。
rust：wedb/wedb/src/server/failover/replica_failover_session.rs 全文件；wedb/wedb/src/server/failover/failover_session.rs fn race_abort；wedb/wedb/src/server/failover/failover_manager.rs fn try_start_replica_failover fn try_start_primary_failover
对应 C#：libs/cluster/Server/Failover/ReplicaFailoverSession.cs；libs/cluster/Server/Failover/PrimaryFailoverSession.cs；libs/cluster/Server/Failover/FailoverManager.cs fn TryStartReplicaFailover fn TryStartPrimaryFailover
动作：逐臂核对超时与 abort 差异并补注释。
