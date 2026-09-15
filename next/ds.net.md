# ds.net 待办

1. [P1] 六个集群命令注册而无命令臂，全量同步检查点链路缺失
   位置：wedb/wnode/src/resp/parser/resp_command.rs:392（ATTACH_SYNC）、:395（BEGIN_REPLICA_RECOVER）、:435（SEND_CKPT_FILE_SEGMENT）、:438（SEND_CKPT_METADATA）、:445（SNAPSHOT_DATA）、:447（SYNC）；消费端 wedb/wedb/src/server/cluster_session.rs:1671 兜底回 ERR，全文件无这些臂
   对标：garnet/libs/cluster/Session/ClusterCommands.cs:127-172
   问题：解析层与 COMMAND 目录已注册，执行层无臂成幽灵命令；C# 全量同步三段握手（INITIATE_REPLICA_SYNC → ATTACH_SYNC → SYNC/SEND_CKPT 检查点流）rust 只有首段 + AOF 直推，非空库副本无法达成一致。
   改法：与检查点传输流一并立项补臂；短期先从 resp_commands_info_data.rs 与 COMMAND 目录摘除六项，消除幽灵注册。

2. [P1] 配置纪元全会话静止机制被删空
   位置：wedb/wedb/src/server/cluster_provider.rs:371-374（bump_and_wait_for_epoch_transition_async = bump + yield_now）；wedb/wnode/src/cluster_session.rs ClusterSessionFace 全文无 epoch 快照方法；调用点 wedb/wedb/src/server/failover/failover_session.rs:262/:402/:419
   对标：garnet/libs/cluster/Server/ClusterProvider.cs:366-391、garnet/libs/cluster/Session/ClusterSession.cs:185-196、garnet/libs/server/Resp/RespServerSession.cs:490/:576
   问题：在途会话可持旧 config 完成槽位判定与写入，failover / setslot / replicaof 的配置过渡非原子。
   改法：ClusterSessionFace 增批次级 epoch 快照（u64 原子，批首尾置取），bump_and_wait 轮询活跃会话快照至追平（带 cluster_node_timeout_ms 上限）。

3. [P1] 副本 AOF 位点推进超前于重放应用
   位置：wedb/wedb/src/server/replication/cluster_replication_session.rs:247（enqueue_raw）、:263-274（consume_direct 空回调）、:276-278（set_sublog_replication_offset）
   对标：garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:137/148/164
   问题：确认语义从 replayed 降级为 enqueued，掉电窗口内主端认为副本已确认而存储层尚未应用。
   改法：位点上报改挂「存储应用完成」事件，或文档化 enqueued 语义并同步调整主端 data_loss_check 口径。

4. [P1] gossip 增量判定以 epoch 替代配置版本
   位置：wedb/wedb/src/server/gossip/gossip_manager.rs:236-238（last_sent_epoch != epoch 才发送）、:256（成功后记账）
   对标：garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:162-178
   问题：配置内容变化而 epoch 未变时增量判定漏发，gossip 收敛依赖全量兜底。
   改法：判定键改配置版本号（与 try_peek_version 同源），成功后记账。

5. [P1] INFO commandstats/gossip/bufferpool/checkpoint 段恒空
   位置：wedb/wnode/src/resp/info_provider.rs:57（command_stats_monitor: false 硬编码）、:94-95（command_stats 恒空）、:147/:152/:162（keyspace/gossip/buffer_pool/checkpoint 空实现）；wedb/wnode/src/resp/resp_server_session.rs:934-935（command_error_written 置位后无消费）
   对标：garnet/libs/server/Resp/RespServerSession.cs:683-716、garnet/libs/cluster/Server/ClusterProvider.cs:272-333
   问题：InfoProvider 接口面已布好但全部返回空，观测面空转，无 per-command 维度。
   改法：补 CommandStats 挂会话主循环三出口计数；集群侧把 gossip/迁移/复制 manager 既有内部计数导出为 MetricsItem。

6. [P2] wconn 应答消费滞留，TCP 合包可致命令永挂
   位置：wedb/wconn/src/network.rs:276-277（内层 while !queue.is_empty() 先 stream.read 后解析；:254 orphan_error_reply 仅兜队列空时的滞留 -ERR）
   对标：garnet/libs/client/ClientSession/GarnetClientSession.cs:Execute
   问题：一次 read 读到多条应答、队列命令数少于应答数时剩余应答滞留 read_buf；下一条命令的应答已在缓冲仍先阻塞等新 socket 数据。
   改法：泵循环先排空 read_buf 中已有完整应答再等新数据；补逐帧应答静默端点的合包形态测试。

7. [P2] MOVED/ASK 端点偏好硬编码 Ip，配置面半接线
   位置：wedb/wedb/src/server/cluster_session.rs:139（redirect_slot）、:825（SlotVerifyRequest.pref_type 写死 Ip）；wedb/wedb/src/server/cluster_config.rs:479-491（get_endpoint_from_slot 已有 Hostname 分支）；全仓无 preferred-endpoint 配置键
   对标：garnet/libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs、garnet/libs/server/Servers/ServerOptions.cs（ClusterPreferredEndpointType）
   问题：hostname 部署的集群客户端被重定向到 IP 端点，IP 不可达时重定向失效。
   改法：preferred endpoint 类型落本地节点配置（默认 Ip），两处构造点取配置值；或声明不支持该形态并固定输出 Ip。

8. [P2] 巨型文件与超长函数拆分
   位置：wedb/wedb/src/server/cluster_session.rs（2116 行，process_cluster_commands:841-1676 单函数 836 行）；cluster_config.rs 1683 行；cluster_provider.rs 1131 行；replication/replication_manager.rs 1090 行
   对标：garnet/libs/cluster/Session/RespClusterBasicCommands.cs、RespClusterSlotManagementCommands.cs、RespClusterMigrateCommands.cs、RespClusterReplicationCommands.cs、RespClusterFailoverCommands.cs
   问题：单函数混合五类命令分发，慢路径 helper 虽已独立（:1940/:1964/:1993/:2013/:2043）但主分发体仍超阈值。
   改法：process_cluster_commands 按 C# 五文件切成五个 impl 块（Rust 同类型跨文件多 impl）；cluster_provider 的 INFO 统计段与资产注入段分文件。

9. [P2] 异步重放模式与 ThrottlePrimary 未接线
   位置：wedb/wedb/src/server/replication/replica_replay_driver.rs（should_throttle_primary 仅测试引用）；ADVANCE_TIME 接收面 wedb/wedb/src/server/cluster_session.rs:570/:593（signal_time_advance）下游无消费者
   对标：garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:293-341
   问题：AofReplayMaxLagBytes > 0 的背景重放与主端背压整链缺失，默认配置不触发，属功能缺口。
   改法：接线背景重放任务 + 主端节流查询；或配置面禁用该形态并删死接口。

10. [P2] FastAofTruncate 断点重对齐缺失
    位置：wedb/wedb/src/server/replication/cluster_replication_session.rs:235-242（tail != current_address 一律 Divergent 断流）
    对标：garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplaySession.cs:54-72
    问题：C# 检测跳页/超长跳过时 SafeInitialize 重对齐并推进位点，rust 容错弱于 C#。
    改法：补跳过重对齐分支，或明确依赖上层 resync 并文档化。

11. [P2] 迁移停等无超时、参数与结果吞没
    位置：wedb/wedb/src/server/migration/migrate_driver.rs:259-262/:311-314/:341-344（STABLE 回滚三元组复制三份）、:355/:361/:365（let _ = 吞返回值）；migrate_session.rs:23（timeout 死字段）
    对标：garnet/libs/cluster/Server/Migration/ClusterMigrateDriver.cs、garnet/libs/cluster/Session/RespClusterMigrateCommands.cs（TryRecoverFromFailureAsync）
    问题：批次停等无 timeout 目标挂起任务永挂；批次失败时远端已导入批次不回滚。
    改法：停等加超时；提取 STABLE 回滚辅助函数；接失败 recover 路径。

12. [P2] MEET 响应未验配置版本即反序列化，失败残留临时连接
    位置：wedb/wedb/src/server/gossip/gossip_manager.rs:98（直接 from_byte_array，gossip 路径有 :263 try_peek_version 防护）、:115（仅成功取得 target_id 才 try_remove，空应答/失败/超时路径残留临时连接）
    对标：garnet/libs/cluster/Server/Gossip/Gossip.cs:196-221
    问题：残留连接被 broadcast_gossip_async 继续遍历，向未知节点 gossip。
    改法：MEET 响应先 peek version 再反序列化；created 连接统一 dispose。

14. [P2] handle_aof_commit_mode 缺配置门控
    位置：wedb/wnode/src/resp/parser/resp_command.rs:729（调用点）、:939-947（函数体无条件执行）
    对标：garnet/libs/server/Resp/Parser/RespCommand.cs:1206-1208
    问题：每条命令无条件维护 wait_for_aof_blocking，AOF 关闭时标志仍置位，语义漂移隐患。
    改法：调用点补 EnableAOF && WaitForCommit 等价门控。

15. [P2] ensure_replication 心跳刷新点偏移
    位置：wedb/wedb/src/server/cluster_provider.rs:229/:232（节流通过即 update_last_primary_sync_time）
    对标：garnet/libs/cluster/Server/Replication/ReplicationManager.cs:38-40
    问题：健康副本也刷新，LastPrimarySyncSeconds 表征不出真实失联时长。
    改法：刷新点改到建立同步时，与 C# 对齐。

16. [P2] 双消费形态长期并存
    位置：wedb/wnode/src/resp/resp_server_session.rs:753（try_consume_messages 批次拷贝）、:794（try_consume_pending scratch 持久游标）
    对标：garnet/libs/server/Resp/RespServerSession.cs:TryConsumeMessages（单一形态）
    问题：生产泵走 scratch，拷贝形态仅服务回退与测试，双轨维护成本。
    改法：长期收敛到 scratch 单形态，测试改走 scratch 入口。

17. [P2] 主端 --recover 后复制位点不回填复制域
    位置：wedb/wedb/src/main.rs:101（open_recovered_with_aof，重放结果仅日志）；wedb/wedb/src/server/replication/replication_manager.rs:451（store_recovered_safe_aof_address 注入口无生产调用）
    对标：garnet/libs/cluster/Server/Replication/ReplicationManager.cs:537-563
    问题：--recover 重启后、首批新写入推进位点前，gossip 广播位点与 failover data-loss 判定基线均为初始值。
    改法：宿主装配完成后按 recover_aof 结果回填 rm。

18. [P2] 复制历史恢复生产装配下空转，门控语义偏离 C#
    位置：wedb/wedb/src/server/replication/replication_manager.rs:93（with_options(1, None)）、:146-157（recover_replication_history / flush_config 空转）；wedb/wedb/src/server/cluster_provider.rs:130/:137（ReplicationManager::new()）
    对标：garnet/libs/cluster/Server/Replication/ReplicationManager.cs:159-166
    问题：replication.conf 持久化面未接线，主端重启即丢 replid 历史，副本被迫全量重同步；构造期无条件 recover_or_init 与 C# Recover && fileSize > 0 门控相反。
    改法：装配传入 checkpoint/config 目录接通持久化；构造期恢复按 --recover 门控。

19. [P2] AOF StoreRMW 重放未知命令 warn 后吞没
    位置：wedb/wnode/src/aof/aof_processor.rs:1291-1302（落空分支 log::warn! 后 return Ok(())）
    对标：garnet/libs/server/AOF/AofProcessor.cs:StoreRMW
    问题：写入端新增 RMW 编码而重放端漏配时静默丢恢复数据；现状写入端条件 SET 族已固化为盲写 upsert，无实际缺口，风险在演化失配。
    改法：改返回 Err，或以编译期穷尽匹配绑定写入端与重放端命令集。

20. [P2] wresp WithLengthHeader 族对空数字按半包挂起
    位置：wedb/wresp/src/read.rs:106-110（零数字降级失败的刻意差异）、:367/:399/:431（try_read_{i32,i64,u64}_with_length_header，当前无生产调用点）
    对标：garnet/libs/common/RespReadUtils.cs:TryReadInt64Safe
    问题：载荷完整但 digits_read==0 时上层按「字节不足」半包永久等待，后续接线易误用。
    改法：补「载荷完整但 digits_read==0 → Err」终态，或文档标注不可用于不可信输入。

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
