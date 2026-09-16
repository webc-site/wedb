[补全] 完善 RESP 协议解析和命令路由支持 RESP3
c#: garnet/libs/server/Resp/Parser/RespCommand.cs:TryReadCommand() 和 SessionParseState.cs
rust: wedb/wresp/src/read.rs:15 (各读取函数需升级) 或 wedb/wresp/src/session_parse_state.rs:12 (缺乏高层解析路由逻辑)
现状: wresp 的 read.rs 有基础类型读取，但缺乏高层 TryReadCommand 解析、RESP3 完整类型映射及路由机制，目前多为低级占位。
方案: 在 `wedb/wresp/src/` 中实现一套流式解析并暴露命令路由入口，使用 Rust 静态 enum（结合 match 或过程宏分发）代替动态 Hash 字典，实现 RESP3 完整支持和状态机读取，杜绝运行时查字典。

[补全] 实现集群节点 Gossip 协议序列化与状态交换
c#: garnet/libs/cluster/Gossip/GossipManager.cs:ProcessGossipMessage()
rust: wedb/wedb/src/server/gossip/gossip_manager.rs:28 (GossipManager 缺少序列化及周期逻辑)
现状: 缺少节点间 Gossip 协议消息体序列化和状态周期交换的实现逻辑，当前只有空白结构。
方案: 在 `wedb/wedb/src/server/gossip/gossip_manager.rs` 中，用 bitcode 库实现 Gossip 消息格式序列化，并新增后台异步循环，定时执行 PULL/PUSH 从已知端点获取和下发集群信息。

[补全] 完善集群高可用主从切换机制
c#: garnet/libs/cluster/Failover/FailoverManager.cs:TryStartFailover()
rust: wedb/wedb/src/server/failover/failover_manager.rs:31 (FailoverManager 逻辑缺失)
现状: 缺乏基于多数派的故障检测、心跳超时判定、以及完整的 Leader Election 选举逻辑。
方案: 完善 `failover_manager.rs`，实现主节点宕机检测的心跳定时器。从节点在超时后发起携带自身日志偏移的投票请求（Epoch/Term），在收到集群多数派确认后执行状态机，切换为 Leader。

[补全] 补齐集群节点元数据状态和心跳维护
c#: garnet/libs/cluster/Server/ClusterManagerWorkerState.cs:ClusterManager.LocalNodeState
rust: wedb/wnode/src/cluster_session.rs:缺失 或 wedb/wnode/src/role_info.rs:缺失
现状: wnode 缺少维护当前节点身份（角色、哈希槽映射、纪元）并向全网广播心跳的常驻机制。
方案: 在 `wnode` crate 中创建节点状态管理器模块。使用并发哈希字典 `papaya + gxhash` 维护全网节点映射；后台拉起心跳任务不断向对端发送本地的槽位和配置纪元。

[优化] 借鉴 C# 零拷贝优化网络层内存分配
c#: garnet/libs/common/Networking/GarnetTcpNetworkSender.cs:SendResponse()
rust: wedb/wconn/src/net/stream.rs (需检查具体连接流读写实现)
现状: 当前网络请求的读写缓冲处理中存在过多的数据拷贝。
方案: 在 `wedb/wconn` 中引入 `Bytes`/`BytesMut` 预分配缓冲池，读入数据后通过零成本切割出 `&[u8]` 切片供解析。写路径上聚拢零散缓冲片通过 vectored I/O (Writev) 直接发送，彻底消除应用深拷贝。

[优化] 协议解析引入 DFA/FSM 流式解析
c#: garnet/libs/server/Resp/Parser/SessionParseState.cs:ReadCommand()
rust: wedb/wresp/src/session_parse_state.rs:12 (未实现 DFA 流式读取)
现状: 现有协议解析可能有反复扫描缓冲或 `Vec<u8>` 分配问题，时间复杂度非最优 O(N)。
方案: 将 `wresp` 的解析逻辑彻底重构成确定的单次正向遍历状态机。不再拷贝提取原始字符串，只记录字节的偏移区间，利用生命周期生成 `&[u8]` 直接送入执行引擎。

[修复] 校验主从复制同步并发逻辑
c#: garnet/libs/cluster/Replication/ReplicationManager.cs:ProcessReplicaSyncRequest()
rust: wedb/wedb/src/server/replication/replication_manager.rs:59 (ReplicationManager 存在并发时序隐患)
现状: 断网重连后的 AOF 偏移检查与日志传输存在并发冲突，缺少时序屏障栅栏机制。
方案: 在 `replication_manager.rs` 重连处理段，先使用 parking_lot 锁死 AOF 发送状态；正确对齐历史偏移或判定需进行全量复制后，再挂起并开启异步日志传输循环。

[修复] 确保槽位迁移机制的健壮性与回滚能力
c#: garnet/libs/cluster/Migration/MigrationManager.cs:StartMigration()
rust: wedb/wedb/src/server/migration/migration_manager.rs:16 (MigrationManager 迁移补偿与回滚缺失)
现状: 槽位数据在迁移失败或网络掉线时缺乏状态异常处理及回滚恢复保障。
方案: 引入块传输的状态检查与重试机制。如果在传输过程中链接中断或抛错，启动后台清理任务丢弃半成品 Chunk，并通过日志回滚恢复原本的主属权状态。

[拆分] 解耦 ClusterManager 臃肿逻辑
c#: garnet/libs/cluster/Server/ClusterManager.cs
rust: wedb/wedb/src/server/cluster_manager.rs (全文件超35KB)
现状: `cluster_manager.rs` 极其庞大，包含了所有工作节点管理、槽状态等聚合操作，严重违背单一职责和低耦合原则。
方案: 依据职责将 `wedb/wedb/src/server/cluster_manager.rs` 拆解为 `cluster_manager_slot_state.rs`（负责槽映射）、`cluster_manager_worker_state.rs`（负责工作进程元数据管理）。

[去重] 统一提取槽位校验逻辑
c#: garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:IterativeSlotVerify() 等
rust: wedb/wedb/src/server/cluster_session.rs:1175、wedb/wnode/src/cluster_session.rs:188、wedb/wtxn/src/txn_slot_verify.rs:23、wedb/wedb/src/server/slot_verify.rs:432
现状: 迭代式槽校验（NetworkIterativeSlotVerify/WriteCachedSlotVerificationMessage）在多个 crate 内被多处复制粘贴重复定义。
方案: 提取并保留一份在 `wnode/src/slot_verify.rs` 或者专门的公共库下，暴露泛型 Trait 或者独立函数供 `wtxn` 和 `wedb` 等引用；删除其它复制复写的文件行，实现一处定义多处调用。

[清理] 清除重构及测试产生的废弃旧代码
c#: garnet/ 众多旧协议废弃支持
rust: js/check/ignore、全局旧代码残留
现状: 存在为跑通初期测试而写的无效假代码，且未在 JS 检查配置中忽略对应 C# 函数。
方案: 执行 ` unused.sh` 及 `clippy.sh`，主动删减虚设的结构体。修改 `js/check/ignore/` yaml 列表，跳过由于不需要实现而不再需要的 C# 函数的校验检查，保证架构轻量清晰。
