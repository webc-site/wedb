# net 待办

1. [P1] 配置纪元过渡缺全会话静止
   位置：wedb/wedb/src/server/cluster_provider.rs:371-374（bump_and_wait_for_epoch_transition_async = bump + yield_now 即返 true）
   位置：wedb/wnode/src/cluster_session.rs:63（ClusterSessionFace 全 trait 无 epoch 快照方法）
   位置：wedb/wedb/src/server/failover/failover_session.rs:262/:402/:419（调用点）；wedb/wedb/src/server/cluster_session.rs:373/:1101/:1153/:1591（直接 bump 不等待）
   对标：garnet/libs/cluster/Server/ClusterProvider.cs:366 BumpAndWaitForEpochTransitionAsync、garnet/libs/cluster/Session/ClusterSession.cs:185-196、garnet/libs/server/Resp/RespServerSession.cs:490/:576（批首 Acquire / finally Release）
   问题：C# 配置过渡（failover/setslot/replicaof/failstopwrites）前自旋等待所有活跃会话 LocalCurrentEpoch 追上；rust 在途会话可持旧 config 完成槽位判定与写入，配置过渡非原子，current_epoch() 无生产消费者。
   改法：ClusterSessionFace 增批次级 epoch 快照（u64 原子，消费首尾置取），bump_and_wait 轮询活跃会话快照直至追平（cluster_node_timeout_ms 封顶），复刻 C# 静止语义。

2. [P1] 副本 AOF 位点推进超前于重放应用
   位置：wedb/wedb/src/server/replication/cluster_replication_session.rs:247（enqueue_raw 落盘）→ :263（consume_direct 空回调）→ :277（set_sublog_replication_offset，注释自认「先记流式落盘位点」）
   对标：garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:137/148/164（记录应用进存储后才推进位点）、ReplicaReplaySession.cs:86-96
   问题：确认语义从 replayed 降级为 enqueued；掉电窗口内主端误认副本已确认，副本存储层可能尚未应用。
   改法：位点上报改挂「存储应用完成」事件（replay coordinator 应用后回推）；或文档化 enqueued 语义并同步调整主端 data_loss_check 口径。

3. [P1] 六个集群命令注册而无执行臂
   位置：wedb/wnode/src/resp/parser/resp_command.rs:392/:395/:435/:438/:445/:447（ATTACH_SYNC/BEGIN_REPLICA_RECOVER/SEND_CKPT_FILE_SEGMENT/SEND_CKPT_METADATA/SNAPSHOT_DATA/SYNC）
   位置：wedb/wedb/src/server/cluster_session.rs:1671（`_ =>` 统一回 unknown subcommand，全文件无这些判别臂）
   对标：garnet/libs/cluster/Session/ClusterCommands.cs:127-172
   问题：解析层与 COMMAND 目录已注册，执行层无臂，幽灵命令；即 C# 全量同步三段握手（INITIATE_REPLICA_SYNC → ATTACH_SYNC → SYNC/SEND_CKPT 检查点流）rust 只实现首段 + AOF 直推，非空库副本无法达成一致。
   改法：与检查点传输流一并立项补臂；短期先从命令目录摘除六项消除幽灵注册（或对齐 C# DisableClusterCheckpointFromFileProvider 配置形态）。

4. [P1] INFO commandstats/gossip/bufferpool/checkpoint 段恒空
   位置：wedb/wnode/src/resp/info_provider.rs:57（command_stats_monitor: false 硬编码）、:94（command_stats 恒 Vec::new）、:99/:146/:151/:156（keyspace/gossip/buffer_pool/checkpoint 空实现）
   位置：wedb/wnode/src/resp/resp_server_session.rs:934-936（command_error_written 置位后仅清位，无 per-command 计数）
   对标：garnet/libs/server/Resp/RespServerSession.cs:683-716（IncrementCalls/IncrementFailed/IncrementRejected，CommandStatsMonitor 门控）、garnet/libs/cluster/Server/ClusterProvider.cs:272-333（GetCheckpointInfo/GetGossipStats/GetBufferPoolStats）
   问题：InfoProvider 接口面已布好但全部返回空，观测面空转；会话侧仅聚合总量，无 per-command 维度。
   改法：补 CommandStats（命令判别 → calls/failed/rejected 三计数）挂会话，主循环三出口分别计数；集群侧把 gossip/迁移/复制 manager 既有内部计数导出为 MetricsItem。

5. [P2] 异步重放模式与 ThrottlePrimary 未接线
   位置：wedb/wedb/src/server/replication/replica_replay_driver.rs:133（should_throttle_primary 仅测试引用 :172-173）、:114/:126（signal_time_advance/applied_pulse_sequence_number 无下游消费）
   位置：wedb/wedb/src/server/cluster_session.rs:593（ADVANCE_TIME 接收面 signal_time_advance）
   对标：garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:293-341（AofReplayMaxLagBytes > 0 时 InitializeBackgroundReplayTask + ThrottlePrimary 背压）
   问题：异步重放与背压整链缺失，脉冲消费无下游；默认配置不触发，属功能缺口非 bug。
   改法：接后台重放任务与节流回推。

6. [P2] FastAofTruncate 断点重对齐缺失
   位置：wedb/wedb/src/server/replication/cluster_replication_session.rs:233-241（tail != current_address 一律 Divergent 报错断流）
   对标：garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplaySession.cs:54-72（跳页/超长跳过时 SafeInitialize 重对齐并推进位点）
   问题：断点容错弱于 C#，可直接补分支的场景也走断流重同步。
   改法：补跳过重对齐分支；或明确依赖上层 resync 并文档声明。

7. [P2] 迁移停等无超时、返回值吞没
   位置：wedb/wedb/src/server/migration/migrate_driver.rs:302-305（批次 ACK 停等无 timeout，目标挂起任务永挂）、:259/:311/:341（STABLE 回滚三元组复制三份）、:355/:361/:365（完成哨兵/NODE/relinquish_ownership 返回值 let _ = 吞没）
   位置：wedb/wedb/src/server/migration/migrate_session.rs:23（MigrateTaskSpec.timeout 死字段）
   对标：garnet/libs/cluster/Server/Migration/MigrationDriver.cs:71 TryRecoverFromFailureAsync（批次失败 recover 路径）
   问题：目标节点挂起则迁移任务永挂；批次失败时远端已导入批次不回滚。
   改法：停等加超时；STABLE 回滚提取辅助函数；接 recover 语义，至少让哨兵与 relinquish 失败显式留痕。

8. [P2] MEET 响应未验配置版本、失败残留临时连接
   位置：wedb/wedb/src/server/gossip/gossip_manager.rs:69-140（try_meet_async；:98 直接 from_byte_array；:113 仅成功且取得 target_id 才 try_remove，空应答/失败/超时分支均残留）
   对标：garnet/libs/cluster/Server/Gossip/Gossip.cs:196-221（dispose created 连接）；gossip 路径已有 :263 try_peek_version 防护可复用
   问题：残留临时连接（"address:port" 键）会被 broadcast_gossip_async 继续遍历，向未知节点 gossip；MEET 响应不验配置版本。
   改法：MEET 各失败路径统一 try_remove；复用 try_peek_version 先验版本再反序列化。

10. [P2] handle_aof_commit_mode 缺配置门控
    位置：wedb/wnode/src/resp/parser/resp_command.rs:729（调用点）、:939-950（函数体）
    对标：garnet/libs/server/Resp/Parser/RespCommand.cs:1206-1208（EnableAOF && WaitForCommit 才调）
    问题：每条命令无条件维护 wait_for_aof_blocking，AOF 关闭时标志仍置位，靠消费点自我豁免，语义漂移隐患。
    改法：调用点补 EnableAOF && WaitForCommit 等价门控。

11. [P2] ensure_replication 心跳刷新点偏移
    位置：wedb/wedb/src/server/cluster_provider.rs:232（节流通过即 update_last_primary_sync_time）
    对标：garnet/libs/cluster/Server/Replication/ReplicationManager.cs:38-40（仅建立同步时调用）
    问题：健康副本也刷新，LastPrimarySyncSeconds 表征不出真实失联时长。
    改法：刷新点移到副本同步建立处。

12. [P2] 双消费形态长期并存
    位置：wedb/wnode/src/resp/resp_server_session.rs:753（try_consume_messages 批次拷贝）与 :794（try_consume_pending scratch 持久游标）
    位置：wedb/wnode/src/resp/resp_session_consumer.rs:140（生产泵走 scratch）、wedb/wnode/src/net/handler.rs:435（拷贝形态服务回退与测试）
    对标：garnet/libs/server/Resp/RespServerSession.cs:474 TryConsumeMessages（单形态）
    问题：双缓冲模型并存，同一协议面两套消费入口长期维护。
    改法：长期收敛到 scratch 单形态，测试改走 scratch 入口。

13. [P2] 主端 --recover 后复制位点不回填复制域
    位置：wedb/wedb/src/main.rs:101（open_recovered_with_aof）、:152-156（rm.recover_async 仅 history + checkpoint）
    位置：wedb/wnode/src/service.rs:892-893（重放结果仅日志）；注入口已备 replication_manager.rs:217/:451
    对标：garnet/libs/cluster/Server/Replication/ReplicationManager.cs:537-563（ReplayAOF 后 replicationOffset.SetValue(ref replayedUntil)）
    问题：--recover 重启后、首批新写入前，gossip 广播的复制位点与 failover data-loss 判定基线均为初始值。
    改法：宿主装配完成后按 recover_aof 结果回填 rm。

14. [P2] 复制历史恢复生产装配下空转，构造期门控偏离 C#
    位置：wedb/wedb/src/server/cluster_provider.rs:130/:137（ReplicationManager::new() → with_options(1, None)）、wedb/wedb/src/server/replication/replication_manager.rs:93/:102（构造期无条件 recover_or_init）、:146-157（recover_replication_history/flush_config 空转）
    对标：garnet/libs/cluster/Server/Replication/ReplicationManager.cs:159-166（Recover && fileSize > 0 才 RecoverReplicationHistory，否则 Initialize）
    问题：replication.conf 持久化面未接线，主端重启即丢 replid 历史，副本被迫全量重同步；若未来接通 config_dir，无条件恢复与 C# 门控语义相反。
    改法：装配传入 checkpoint/config 目录接通持久化；构造期恢复按 --recover 门控。

15. [P2] AOF StoreRMW 重放未知命令 warn 后吞没
    位置：wedb/wnode/src/aof/aof_processor.rs:1294-1302（落空分支 warn 后 return Ok(())）
    对标：garnet/libs/server/AOF/AofProcessor.cs:StoreRMW（cmd 直通 SessionFunctions 重评估，无未知命令缺口）
    问题：恢复流程继续成功，写入端新增 RMW 编码命令而重放端漏配时静默丢恢复数据；现状写入端条件 SET 族落盘前已固化为盲写 upsert，无实际缺口，风险在演化失配。
    改法：该分支改返回 Err 显式暴露恢复失败；或以编译期穷尽匹配把写入端可入 AOF 的 RMW 命令集与重放分支绑定。

17. [P2] wconn network_loop 应答消费滞留
    位置：wedb/wconn/src/network.rs:230-231（外层先阻塞等新命令）、:272-273（应答段先 stream.read 再解析 read_buf）
    对标：garnet/libs/client/ClientSession/GarnetClientSession.cs:747 TryConsumeMessages（读事件内排空全部完整应答）
    问题：一次 socket read 读到多条应答、队列命令数少于应答数时剩余应答滞留 read_buf；下一条命令应答已在 read_buf 中泵仍先阻塞等新数据，TCP 合包（本机低延迟易发）可致命令永挂。
    改法：泵循环先排空 read_buf 中已有完整应答再等新数据；用逐帧应答的静默假端点构造合包形态补测试。

19. [P2] 迁移槽位移交先于源端删除的孤儿键投影未声明
    位置：wedb/wedb/src/server/migration/migrate_driver.rs:361-365（SETSLOTSRANGE NODE + relinquish_ownership 先行）与 :368-373（源端删除仅推进 transferred 白名单）；模块注释 :9-14 只声明 string 裁剪与 chunk 未实现
    对标：garnet/libs/cluster/Server/Migration/MigrationDriver.cs:18 TrySetSlotRangesAsync（C# 槽位移交后源端不留键）
    问题：竞态未传输的键保留在已交出槽位的源端成为不可达孤儿键；方向保守（数据不丢），与 C# 语义有偏差且无任何注释声明。
    改法：模块注释补一句声明该安全投影与 C# 语义偏差。
