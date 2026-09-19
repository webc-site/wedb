驳回 aof-tail-witness-freq-config-wiring

票面核心断言「配置字段 aof_tail_witness_freq_ms 全仓运行期唯一读出现在测试」不成立，
系对 wedb 现状的过期扫描，漏掉了真正的消费点。核实如下。

C# 侧该旋钮的唯一消费者是 AofSyncTask 的 CLUSTER ADVANCE_TIME 脉冲节流：
garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs 的
SendAdvanceTimePulse（第 257 行）每轮直接读
clusterProvider.serverOptions.AofTailWitnessFreqMs 与
now - lastAdvanceTimePulse 比较，到期才发脉冲。声明与播种在
garnet/libs/server/Servers/GarnetServerOptions.cs 第 150 行（默认 100）与
garnet/libs/host/Configuration/Options.cs 第 245、932 行。

Rust 对位链已经完整接通，非硬编码常量：
wedb/wedb/src/server/replication/aof_sync_task.rs 的 send_advance_time_pulse
（第 343 至 355 行）经 pulse_source 持有的 Arc<RuntimeServerConfig> 调
get_milliseconds(ServerConfigType::AofTailWitnessFreq)（第 349 至 352 行）每轮实时读取，
与 C# 第 257 行同构。槽位由 wconf/src/runtime_server_config.rs 的 init（第 585 至 588 行）
自 RuntimeServerOptions::aof_tail_witness_freq_ms（wconf/src/runtime_server_options.rs
第 19 行）播种，单一 nested_text 入口经 wconf/src/node_options.rs 的
runtime_server_options 投影进 wedb/src/server/boot.rs 第 70 行构造、第 178 行以
set_runtime_config 注入 cluster_provider；TimePulseSource 在
wedb/src/server/replication/replica_sync_session.rs 第 184 至 194 行与
wedb/src/server/replication/diskless_replication/replica_sync_session.rs 第 301 行
组装时取的正是 provider.try_runtime_config() 这同一个共享 Arc，
CONFIG SET aof-tail-witness-freq 热更落槽即时生效
（runtime_server_config.rs 第 177 至 187 行 META 槽、第 420 至 422 行 NAME_LOOKUP）。
boot.rs 第 173 至 177 行与 cluster_provider.rs 第 145 至 149 行注释所述
「AofSyncTask 每轮实时读 AofTailWitnessFreqMs」与实现一致，无漂移。

票面把 cluster_provider.rs 第 520 至 593 行 ensure_replication 一节误认为
AofTailWitnessFreqMs 的节流链。该链是另一个旋钮：其 poll_frequency 取自
replication_reestablishment_timeout_secs（第 523 至 535 行），对标 C#
garnet/libs/cluster/Server/Replication/ReplicationManager.cs 的 EnsureReplication
第 184 行读 ServerConfigType.CLUSTER_REPLICATION_REESTABLISHMENT_TIMEOUT 槽，
判定到期不消费、发起处 CAS 消费（C# 第 192、252 行）也与
ensure_replication_due 与 try_consume_ensure_replication_window 逐条对位。
两条节流各读各的配置源，均无「常量冒充配置」的问题。

至于 nested_text 文件层暂无 aof_tail_witness 独立键位（播种值即 C# 默认 100，
热更走 CONFIG SET），这与其他十余个 Init 播种字段（cluster_timeout、
replica_sync_delay_ms 等）口径一致，属全表统一面，非本票所述装配漂移；若要补
文件键位应立一张覆盖全部播种字段的全局面票，不该以「旋钮未接线」为由单点开挖。

结论：链上已正确实时读该配置，票面第 4 步自判作废条款触发，予以驳回，不改代码。
