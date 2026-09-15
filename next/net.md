# net 待办

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

19. [P2] 迁移槽位移交先于源端删除的孤儿键投影未声明
    位置：wedb/wedb/src/server/migration/migrate_driver.rs:361-365（SETSLOTSRANGE NODE + relinquish_ownership 先行）与 :368-373（源端删除仅推进 transferred 白名单）；模块注释 :9-14 只声明 string 裁剪与 chunk 未实现
    对标：garnet/libs/cluster/Server/Migration/MigrationDriver.cs:18 TrySetSlotRangesAsync（C# 槽位移交后源端不留键）
    问题：竞态未传输的键保留在已交出槽位的源端成为不可达孤儿键；方向保守（数据不丢），与 C# 语义有偏差且无任何注释声明。
    改法：模块注释补一句声明该安全投影与 C# 语义偏差。
