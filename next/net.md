# net 待办

3. [P1] 六个集群命令注册而无执行臂
   位置：wedb/wnode/src/resp/parser/resp_command.rs:392/:395/:435/:438/:445/:447（ATTACH_SYNC/BEGIN_REPLICA_RECOVER/SEND_CKPT_FILE_SEGMENT/SEND_CKPT_METADATA/SNAPSHOT_DATA/SYNC）
   位置：wedb/wedb/src/server/cluster_session.rs:1671（`_ =>` 统一回 unknown subcommand，全文件无这些判别臂）
   对标：garnet/libs/cluster/Session/ClusterCommands.cs:127-172
   问题：解析层与 COMMAND 目录已注册，执行层无臂，幽灵命令；即 C# 全量同步三段握手（INITIATE_REPLICA_SYNC → ATTACH_SYNC → SYNC/SEND_CKPT 检查点流）rust 只实现首段 + AOF 直推，非空库副本无法达成一致。
   改法：与检查点传输流一并立项补臂；短期先从命令目录摘除六项消除幽灵注册（或对齐 C# DisableClusterCheckpointFromFileProvider 配置形态）。

5. [P2] 异步重放模式与 ThrottlePrimary 未接线
   位置：wedb/wedb/src/server/replication/replica_replay_driver.rs:133（should_throttle_primary 仅测试引用 :172-173）、:114/:126（signal_time_advance/applied_pulse_sequence_number 无下游消费）
   位置：wedb/wedb/src/server/cluster_session.rs:593（ADVANCE_TIME 接收面 signal_time_advance）
   对标：garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:293-341（AofReplayMaxLagBytes > 0 时 InitializeBackgroundReplayTask + ThrottlePrimary 背压）
   问题：异步重放与背压整链缺失，脉冲消费无下游；默认配置不触发，属功能缺口非 bug。
   改法：接后台重放任务与节流回推。

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
