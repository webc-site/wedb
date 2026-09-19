review-net 待办：网络协议 共识 同步 迁移

1. execute_checkpoint_recv 阻塞网络 reactor 线程
问题：execute_checkpoint_recv 内部使用 blocking_wait 同步等待检查点数据与快照分块处理，阻塞了 compio 单核 reactor 线程，导致同核心其他并发连接被暂停；而同文件的其它慢命令（如 network_cluster_begin_replica_recover、network_cluster_initiate_replica_sync 等）均使用 SlowWait 挂入 pending_slow 异步让渡。
rust：wedb/wedb/src/server/cluster_session/replication.rs fn execute_checkpoint_recv fn network_cluster_snapshot_data fn network_cluster_send_checkpoint_metadata fn network_cluster_send_checkpoint_file_segment
对应 C#：libs/cluster/Session/RespClusterReplicationCommands.cs fn NetworkClusterSnapshotData fn NetworkClusterSendCheckpointMetadata fn NetworkClusterSendCheckpointFileSegment
动作：重构 execute_checkpoint_recv 改用 SlowWait 挂入 pending_slow 异步等待，移除 blocking_wait，避免阻塞 reactor 线程。

2. NodeConnection initialize_async 失败后连接重试失效
问题：initialize_async 在调用 client.connect_async().await 前直接通过 compare_exchange 将 initialized 标为 true。若首次建连因瞬时网络错误或对端未启动失败，initialized 保持 true，后续 gossip 轮次再次调用时直接跳过，导致节点永久失联无法自动重连。
rust：wedb/wedb/src/server/gossip/node_connection.rs fn NodeConnection::initialize_async
对应 C#：libs/cluster/Server/Gossip/GarnetServerNode.cs fn InitializeAsync fn TryGossip
动作：若 connect_async 失败，将 initialized 复位为 false，允许后续 gossip 周期重新发起建连。

3. try_parse_slots 奇数槽位区间参数静默截断
问题：try_parse_slots 的 range 模式使用 args.as_chunks::<2>().0 迭代区间，未校验 args 长度是否为偶数，剩余奇数参数在 as_chunks.1 中被静默忽略。在未在外层做偶数校验的命令（如 CLUSTER SETSLOTSRANGE）中，奇数尾参数不会触发解析报错而是直接丢弃。
rust：wedb/wedb/src/server/cluster_session/slot_mgmt.rs fn ClusterSession::try_parse_slots fn ClusterSession::network_cluster_set_slots_range
对应 C#：libs/cluster/Session/ClusterCommands.cs fn TryParseSlots
动作：在 try_parse_slots 的 range 分支首行增加偶数校验，奇数长度直接返回 Err(SlotParseError::NotInteger)（对齐 C# RESP_ERR_INVALID_SLOT）。

4. TLS PEM 解析两套完全重复
问题：load_certs 与 load_private_key 在 wnode 和 wconn 各自实现了一份，逻辑与依赖完全雷同，违反一处定义与去重规范。
rust：wedb/wnode/src/tls/config.rs fn load_certs fn load_private_key；wedb/wconn/src/tls.rs fn load_certs fn load_private_key
对应 C#：libs/server/TLS/GarnetTlsOptions.cs fn GetCertificateIssuer 证书载入段
动作：下沉抽离到公共基础模块（如 wbase::tls），两侧保留薄封装。

5. ClusterProvider 职责堆叠体量过大亟待拆分
问题：cluster_provider.rs 单文件超过 1700 行，主 impl 独占 900 余行，混杂了依赖装配注入槽、运行期配置旋钮、复制编排判定链（含 ensure_replication 七步链）、检查点与快照恢复面等四类不同职责，堆叠导致字段读写链路核对困难。
rust：wedb/wedb/src/server/cluster_provider.rs struct ClusterProvider 及主 impl
对应 C#：libs/cluster/Server/ClusterProvider.cs
动作：按 C# 职责切分到 cluster_provider/ 目录：mod.rs（核心骨架与句柄）、assets.rs（装配注入槽）、flags.rs（运行期旋钮与布尔标志）、replication.rs（复制编排链）、checkpoint.rs（检查点与快照恢复）。

6. GarnetClient 仅支持 TCP 缺少 Unix Domain Socket 支持
问题：服务端已支持 Unix Domain Socket 监听（ServerEndpoint::Unix），但客户端 wconn::GarnetClient 仅硬编码使用 TcpStream::connect，OutStream 仅包含 Tcp 与 Tls 两臂，无法通过 UDS 进行本机高性能进程间通信。
rust：wedb/wconn/src/client.rs fn GarnetClient::connect_async；wedb/wconn/src/network/stream.rs enum OutStream enum ReadHalf enum WriteHalf
对应 C#：libs/client/GarnetClient.cs fn ConnectAsync
动作：在 OutStream 与读写半句柄中增加 Unix 变体，并在 connect_async 识别 unix 路径发起 UDS 连接。

7. mTLS 必选客户端证书在宽松模式下判定缺口
问题：当配置开启 client_cert_required 时，若未提供 issuer_cert 走 AnyClientCert，需要核验 rustls WebPkiClientVerifier 在特定配置下是否会放行空客户端证书请求，防止 mTLS 降级为单向 TLS。
rust：wedb/wnode/src/tls/config.rs fn server_config fn client_verifier struct AnyClientCert
对应 C#：libs/server/TLS/GarnetTlsOptions.cs fn ValidateClientCertificateCallback prop ClientCertificateRequired
动作：核验 ClientCertVerifier 实现，确保 client_auth_mandatory 恒真并在握手阶段拦截无证书连接，补齐全链路回归测试。

8. CheckpointStore wait_for_replicas 纯自旋让渡
问题：wait_for_replicas 采用 while !entry.try_suspend_readers() { yield_now(); } 纯自旋让渡，在慢客户端或长读者场景下会占用 CPU 核心自旋，缺少超时熔断或事件驱动唤醒。
rust：wedb/wedb/src/server/replication/checkpoint_store.rs fn CheckpointStore::wait_for_replicas
对应 C#：libs/cluster/Server/Replication/CheckpointStore.cs fn WaitForReplicas
动作：为读者等待增加超时退出机制或基于 Event 的异步通知，避免无界 CPU 空转。

9. Diskless Sync 快照扇出单副本慢挂拖慢整批
问题：run_snapshot_fanout 向批次内所有副本广播全量快照帧时，若其中单个副本网络拥塞或慢挂，缺乏针对单副本的即时熔断与剥离，会导致整个批次的所有健康副本被连带降速。
rust：wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs fn run_snapshot_fanout
对应 C#：libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSnapshotIterator.cs fn StreamSnapshotData
动作：对每个副本发送通道施加单帧独立超时，单个副本发送超时立即标记失败移出当前同步批次，保障健康副本的同步流水线。

10. SingleKeySlotVerify 锚点与迭代门复挂
问题：同一 C# 函数 NetworkIterativeSlotVerify 被同时挂到 evaluate_iterative_key_gate 和 network_iterative_slot_verify，check.js 报重复定义。evaluate 侧为挂起扩展，不是单键状态机。
rust：wedb/wedb/src/server/cluster_manager_slot_gate.rs fn ClusterManager::evaluate_iterative_key_gate；wedb/wedb/src/server/cluster_session/slot_verify.rs fn ClusterSession::network_iterative_slot_verify
对应 C#：libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs fn NetworkIterativeSlotVerify
动作：保留 wedb/wedb/src/server/cluster_session/slot_verify.rs 的锚点，移除前者锚点并改为内部辅助说明。

11. GarnetClientSession 构造锚点一挂四处
问题：C# 构造函数 GarnetClientSession 被同时挂到 GarnetClientSession::new 以及 set_network_pool、resolve_network_pool 等方法上，造成锚点冲突且触发 check.js 重复警告。
rust：wedb/wconn/src/session.rs fn GarnetClientSession::new；wedb/wconn/src/session.rs fn GarnetClientSession::set_network_pool；wedb/wconn/src/client.rs fn GarnetClient::set_network_pool；wedb/wconn/src/network/pump.rs fn resolve_network_pool
对应 C#：libs/client/ClientSession/GarnetClientSession.cs fn GarnetClientSession；libs/cluster/Server/Migration/MigrationManager.cs prop GetNetworkPool
动作：仅保留 session.rs new 的构造锚点，其余方法移除构造锚点并改挂 GetNetworkPool 回退说明。

12. ConnectAsync 锚点挂到 TLS 握手子步骤
问题：ClientTlsConfig::connect 仅执行出站 TLS 握手，被误挂 ConnectAsync 全量连接锚点，与 GarnetClient::connect_async 冲突。
rust：wedb/wconn/src/tls.rs fn ClientTlsConfig::connect；wedb/wconn/src/client.rs fn GarnetClient::connect_async
对应 C#：libs/client/GarnetClient.cs fn ConnectAsync
动作：移除 ClientTlsConfig::connect 上的 ConnectAsync 锚点，保留给 GarnetClient::connect_async。

13. GarnetClient 构造锚点挂到 facade 注入器
问题：wedb facade 的 GarnetClient::set_tls 又挂了 GarnetClient 构造锚点，与 wconn 的 GarnetClient::new 冲突。
rust：wedb/wedb/src/client.rs fn GarnetClient::set_tls；wedb/wconn/src/client.rs fn GarnetClient::new
对应 C#：libs/client/GarnetClient.cs fn GarnetClient
动作：移除 facade 的构造锚点，保留 wconn 构造锚点。

14. HandleNewConnection 锚点误挂容量门子步骤
问题：ConsumerRegistry::try_acquire_connection 仅为计数递增与容量校验门子步骤，被误挂 HandleNewConnection 全量连接处理锚点，与 process_stream 冲突。
rust：wedb/wnode/src/servers/consumer_registry.rs fn ConsumerRegistry::try_acquire_connection；wedb/wnode/src/net/handler/drive.rs fn NetworkHandler::process_stream
对应 C#：libs/server/Servers/GarnetServerTcp.cs fn HandleNewConnection
动作：移除 try_acquire_connection 上的锚点，保留 process_stream 的主锚点。

15. GetSslServerAuthenticationOptions 锚点重复挂载
问题：ServerTlsConfig::from_der 与 server_config 两处同时挂载了同一个 C# 选项生成函数锚点。
rust：wedb/wnode/src/tls/config.rs fn ServerTlsConfig::from_der fn server_config
对应 C#：libs/server/TLS/GarnetTlsOptions.cs fn GetSslServerAuthenticationOptions
动作：保留 server_config 主装配单点的锚点，移除 from_der 上的重复锚点。

16. SubscribeBroker StartAsync 锚点挂到收尾子步骤
问题：consumer_finish 仅为订阅消费任务完成后的清理收尾步骤，被误挂 StartAsync 锚点，与 spawn_pubsub_consume_task 冲突。
rust：wedb/wnode/src/service.rs fn spawn_pubsub_consume_task；wedb/wpubsub/src/subscribe_broker.rs fn SubscribeBroker::consumer_finish
对应 C#：libs/server/PubSub/SubscribeBroker.cs fn StartAsync fn Dispose
动作：移除 consumer_finish 的 StartAsync 锚点，改挂 Dispose 收口说明。

17. can_access_key 转发层与本体同名并存
问题：MigrationManager::can_access_key 仅为对 active session 的遍历转发层，而 MigrateSession::can_access_key 才是实际判断槽位和 sketch 状态的本体，两者同名且均使用同一锚点。
rust：wedb/wedb/src/server/migration/migration_manager.rs fn MigrationManager::can_access_key；wedb/wedb/src/server/migration/migrate_session.rs fn MigrateSession::can_access_key
对应 C#：libs/cluster/Server/Migration/MigrateSessionKeyAccess.cs fn CanAccessKey
动作：转发侧移除锚点并注明转发说明，本体保留 CanAccessKey 锚点。

18. ConsumerRegistry 进程级安装链路不一致
问题：install_global 仅在 NodeService::from_parts 内部调用，若直接构造 GarnetServer 启动，全局注册表未被安装，导致在直接启动路径下依赖 ConsumerRegistry::global() 的管理命令取到 None。
rust：wedb/wnode/src/service.rs fn NodeService::from_parts；wedb/wnode/src/server.rs fn start_server_monitor；wedb/wnode/src/resp/client_commands.rs
对应 C#：libs/server/Servers/GarnetServerBase.cs activeHandlers 实例直持；libs/server/Servers/GarnetServerTcp.cs fn TryCreateMessageConsumer
动作：将 ConsumerRegistry 全局安装收敛到 ServerBootstrap 或 GarnetServer 构造单点，确保任何启动路径均能正确获取活跃连接句柄。

19. MIGRATE KEYS 超大单记录切块在途异常清理
问题：在 transmit_keys 中，超限单记录使用 send_chunked_record 逐块分批发送，若中间块遭遇网络异常或远端超时，当前直接返回 Err 并中断，对端重组器可能滞留未拼接完毕的孤儿流数据。
rust：wedb/wedb/src/server/migration/migrate_driver/keys.rs fn transmit_keys fn send_payload_and_wait
对应 C#：libs/cluster/Server/Migration/MigrateSessionKeys.cs fn TransmitKeysAsync；libs/cluster/Server/Migration/MigrateSessionCommonUtils.cs fn CompletePending
动作：当分块传输发生错误时，在 recover 恢复流程中显式重连并复位远端槽位状态，确保接收端断连丢弃残存 stream。

20. failover replica 长会话核对泛型抹平风险
问题：Rust 实现中主从两类 failover 会话通过基类组合共用 race_abort，需逐项核对 C# 中 PrimaryFailoverSession 与 ReplicaFailoverSession 在超时预算、状态回滚与 abort 逻辑上的差异，杜绝抹平导致状态机行为分歧。
rust：wedb/wedb/src/server/failover/replica_failover_session.rs；wedb/wedb/src/server/failover/primary_failover_session.rs；wedb/wedb/src/server/failover/failover_session.rs fn race_abort；wedb/wedb/src/server/failover/failover_manager.rs fn try_start_replica_failover fn try_start_primary_failover
对应 C#：libs/cluster/Server/Failover/ReplicaFailoverSession.cs；libs/cluster/Server/Failover/PrimaryFailoverSession.cs；libs/cluster/Server/Failover/FailoverManager.cs fn TryStartReplicaFailover fn TryStartPrimaryFailover
动作：核对主从会话两端超时处理与状态流转细节，补齐针对 abort 竞态与超时的差异注释与边界单测。
