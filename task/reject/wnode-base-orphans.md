# wnode-base-orphans 拒绝记录

来源：next/design.md 条 5（wnode 基础面孤儿）与 next/net.md 条 18（QUIT 断连）甄别中被拒绝的意见点，执行结果见 task/done/wnode-base-orphans.md。

1. 意见：READONLY/READWRITE/QUIT 在 C# 集群场景有生产分派，补接线打通分派
   拒绝（改判为删重复面）：
   - C# 生产分派属实（RespServerSession.cs:861-864 ProcessBasicCommands、:673 事务态分支），但 rust 侧分派臂已存在——process_basic_commands 内联落地（wnode/src/resp/resp_server_session.rs:1036-1058）：Quit 臂置 to_dispose + 回 +OK，Readonly/Readwrite 臂置 read_only_session 并调 cluster_session 的 set_read_only_session/set_read_write_session（wnode/src/cluster_session.rs:244，消费链在 wedb/src/server/cluster_manager.rs:554 与 slot_verify.rs）。
   - basic_commands.rs 的 network_quit/network_readonly/network_readwrite 全仓零调用，且 readonly/readwrite 版比内联臂少 cluster_session 接线，属落地后未接线的重复面。补接线会形成双分派（同命令两处实现），违反「一处定义」红线，故删重复面、保留内联臂。
   - 意见的真实价值在问题 B：QUIT 置位后泵不消费哨兵（C# RespServerSession.cs:743-748 Process 尾部 if (toDispose) DisposeNetworkSender(true) 的等价物缺失），连接不断。此项成立并已修（MessageConsumerFace.take_dispose_request 通道 + 泵写出段后检查断连）。

2. 意见：并行恢复任务（RecoverLogDriver.cs / RecoverReplayTask.cs）C# 有生产调用链，补接线打通分派
   拒绝：
   - rust 侧 wnode/src/aof/recover/recover_log_driver.rs 模块头已留档转写决策：「C# 的 BulkConsumeAllAsync + 页级双闸栏并行化在 rust 侧折叠为顺序消费（跨子日志仍可由调用方并行驱动多 driver）」。
   - 生产链完整：RecoverLogDriver::new/run 由 aof_recover.rs:39/:83 消费，上层 recover_replay_driver/single_log_recover/multi_log_recover 测试与恢复链在册，无孤儿可补。
   - RecoverReplayTaskAsync/ReplayPage 已在 js/check/ignore/server.yml 登记（「Rust 已决策顺序恢复/单通道，页级双闸栏并行回放任务整面不实现」先例在册），非本任务遗漏。

3. 意见：会话/输出面孤儿按 C# 判活判活、判死删（含 set_global_latency_metrics 等 metrics 三件套）
   部分拒绝（metrics 三件套仅删其一）：
   - new_global_latency_metrics（metrics_commands.rs:137）删：等价构造已活（wmetric garnet_server_metrics.rs:56 GarnetServerMetrics::new(track_latency) 按 DEFAULT_LATENCY_TYPES 构造），重复入口。
   - to_metrics_string（role_info.rs:27）删：C# RoleInfo.cs:ToString 生产零消费（ROLE 命令逐字段手写应答 AdminCommands.cs:879-947；garnet 全仓仅 ClusterProvider/IClusterProvider/AdminCommands/ReplicationPrimaryAofSync/AofSyncDriverStore 引用 RoleInfo 类型，均逐字段读，无 ToString/插值调用）。
   - set_global_latency_metrics（resp_server_session.rs:464）保留：C# 构造期 LatencyMonitor 门装配面（RespServerSession.cs:265）生产在用；rust 装配域（监视器采样频率装配）被 next/glm.md 条 7 认领，接线落地即复活，与 task/reject/zero-ref-pubs-cleanup.md 的 stop_and_switch 先例同口径。

4. 意见：RangeIndex 复制状态面两方法若只为未实现 checkpoint 传输流服务则删
   接受但修正理由与范围：
   - pending_stream_reassembly_count：C# 生产零消费（internal 属性仅 RangeIndexStreamReplayTests 断言用），按测试观测面孤儿直接删，与「未实现传输流」无关。
   - dispose_incomplete_stream_reassembly：C# 生产链锚点是 RangeIndexManager.cs:487 Dispose()（管理器释放时丢弃未完成重组），不是 checkpoint 传输流本体；rust 删因是管理器 Dispose 生命周期域缺位（engine 为共享 Arc，树与临时文件生命周期在 wkv/wdev 域），此刻接线无装配点。已登记 ignore 并注明重建时点（ds.net.md 条 1 转写时）。

5. 意见：「全仓（src+tests）引用计数均为 0」约 33 项
   事实修正：名单可确认 25 项；甄别后实删 19 符号（含意见漏列的同族项 set_result、initialize_with_argument 与哨兵中间层 flush_if_pending/kill_requested），另 4 项保留（commit_aof_async、set_global_latency_metrics 跳过/判活，network_custom_txn/network_custom_procedure 与名单内 network_custom_raw_string_cmd 同组接线救活），2 处接线（custom 三臂、QUIT 泵哨兵）。执行以逐符号 C# 对照结论为准，不凑数。
