# wnode-base-orphans

来源：next/design.md 条 5（wnode 基础面 C# 形状孤儿）与 next/net.md 条 18（QUIT 后连接不断），主代理预清理后移交。

## 甄别结论（对照 garnet C# 生产链）

原意见「READONLY/READWRITE/QUIT 补接线」不成立：三条命令的生产分派臂已在 wnode/src/resp/resp_server_session.rs:1036-1058 的 process_basic_commands 内联落地（含 cluster_session 的 set_read_only_session/set_read_write_session 接线），basic_commands.rs 的 network_quit/network_readonly/network_readwrite 是同形重复面，按「一处定义」红线删重复面而不是补接线。

问题 B 成立：C# RespServerSession.cs:738-748 Process 尾部「Send 后 if (toDispose) DisposeNetworkSender(true)」的哨兵消费在 rust 泵缺失。QUIT 置 to_dispose、flush_if_pending 转 kill_requested，但泵（wnode/src/net/handler.rs drive_loop）只认 parse_violation 与 fatal_disconnect 两个断连哨兵，RESP 消费者（resp_session_consumer.rs）不上报 dispose 请求 → QUIT 应答后连接不关。

### 删除（零引用孤儿；C# 判死或 rust 等价覆盖/折叠）

1. wnode/src/resp/basic_commands.rs network_get_async（:449）
   C# 链 BasicCommands.cs:71 GET 分派 NetworkGETAsync（useAsync 门）；rust 已折叠进 network_get（try_read_sync 的 Ok(None) 降级承接磁盘冷读，注释自认）。删。
2. wnode/src/resp/basic_commands.rs network_get_sg（:463）
   C# 链 BasicCommands.cs:67 SG_GET 配置 + NextCommandMaybeGet 门控；rust SG 批量面折叠进单键 network_get。删。
3. wnode/src/resp/basic_commands.rs parse_get_and_key（:1717）/ next_command_maybe_get（:1729）/ set_result（:1751，意见漏列同族）
   C# SG 前视解析与 pending 输出槽管理三助手；rust 无接收缓冲前视入口与 scratch 槽机制，现体为恒 false/恒 true 的虚设。删。
4. wnode/src/resp/basic_commands.rs try_get_simple_command_info（:1733）
   C# 消费点 COMMAND GETKEYS/GETKEYSANDFLAGS（BasicCommands.cs:1352/:1389）；rust 由 prepare_command_keys_context 单源承接（basic_commands.rs:1267）。删。
5. wnode/src/resp/basic_commands.rs network_quit（:1047）/ network_readonly（:1102）/ network_readwrite（:1108）
   process_basic_commands 内联臂活链（:1036-1058）。重复面删。
6. wnode/src/resp/array_commands.rs network_array_ping（:429）
   process_basic_commands Ping 臂（count 0/1/else 三态，:1014-1025）活链。重复面删。
7. wnode/src/resp/resp_server_session.rs get_object_output（:2040）/ get_unified_output（:2045）
   C# GetObjectOutput/GetUnifiedOutput 输出视图；rust 存储 API 直写 output（get_string_output 活链保留）。零引用别名删。
8. wnode/src/resp/resp_server_session.rs abort_with_wrong_num_args_or_unknown_subcommand（:2070）
   单源已在 admin_commands.rs:638（abort_with_unknown_subcommand_or_wrong_num_args，对标 ObjectStoreUtils.cs）。重复面删。
9. wnode/src/resp/resp_server_session.rs debug_send（:1875）
   C# RespServerSession.cs:1469 DebugSend 全 garnet 零调用（C# 本身死代码）。删。
10. wnode/src/resp/resp_server_session.rs create_consistent_read_api（:2001）
    rust 等价覆盖已存在：aof/readconsistency 域（read_consistency_manager / replica_read_session_context 的 pre/post consistent read 钩子族）。影子槽实现删。
11. wnode/src/resp/resp_server_session_output.rs with_protocol_writer（:30）
    rust 自创便利面（C# 无此函数），零引用；各 write 方法已内联协议分支。删。
12. wnode/src/resp/resp_server_session_output.rs process_output（:44）
    C# ProcessOutput(SpanByteAndMemory) 是 StringOutput→响应缓冲桥；rust 存储 API 直写 output，桥无对应物。删。
13. wnode/src/resp/parser/session_parse_state.rs initialize_with_arguments（:42）与同型 initialize_with_argument（:34，意见漏列）
    零引用薄转发；底层 wresp SessionParseState::initialize_with_args 活链（wcol object_store_utils.rs:33 等消费）。薄包装删，底层保留。
14. wnode/src/role_info.rs to_metrics_string（:27）
    C# RoleInfo.cs:ToString 生产零消费（ROLE 命令逐字段手写应答 AdminCommands.cs:879-947；metrics 域不引用 RoleInfo）。删。
15. wnode/src/shutdown.rs wait_stopped（:80）
    rust 自创孤儿（无 C# 映射），零引用；防竞态 wait 活链保留。删。
16. wnode/src/resp/metrics_commands.rs new_global_latency_metrics（:137）
    等价构造已存在：wmetric garnet_server_metrics.rs:56 GarnetServerMetrics::new(track_latency) 按 DEFAULT_LATENCY_TYPES 构造。重复入口删。
17. wnode/src/resp/rangeindex/range_index_manager_replication.rs pending_stream_reassembly_count（:200）
    C# Replication.cs:57 internal 属性生产零消费（仅 RangeIndexStreamReplayTests 断言用）。rust 零引用。删。
18. wnode/src/resp/rangeindex/range_index_manager_replication.rs dispose_incomplete_stream_reassembly（:592）
    C# 锚点 RangeIndexManager.cs:487 Dispose()；rust 管理器无独立 Dispose 生命周期域（engine: Arc<Engine> 共享，树与临时文件生命周期在 wkv/wdev 域），此刻接线无装配点。删，重建时点 = ds.net.md 条 1 检查点/RI 复制流转写时随 RangeIndexManager.Dispose 域重建。
19. wnode/src/resp/resp_server_session.rs try_kill（:1790）+ kill_requested 字段（:214）+ flush_if_pending（:1857）
    C# TryKill() => networkSender.TryClose()（CLIENT KILL，ClientCommands.cs:227/:394）；rust 已结构性承接为 ConsumerEntry.kill_session 触发位 + handler.rs spawn_kill_watcher 取消令牌（consumer_registry.rs:112）。会话侧 try_kill 与 to_dispose→kill_requested 中间层是被替代机制的死面。删（kill_requested 随哨兵收敛一并消失）。

### 接线（C# 有生产调用链，补一环）

20. 自定义命令分派三臂（问题 A 的 network_custom_raw_string_cmd救活）
    C# 链 RespServerSession.cs:1099-1102 ProcessOtherCommands 内 CustomTxn/CustomRawStringCmd/CustomObjCmd/CustomProcedure 四臂（NetworkCustomTxn :1153 / NetworkCustomProcedure :1170 / NetworkCustomRawStringCmd :1194：arity 校验 → 清槽 → 执行域）。rust 分派链（process_basic_commands → process_array_commands → process_other_commands → dispatch_via_garnet_api）无三臂，函数落地后无人调用。在 process_other_commands 尾部 dispatch_via_garnet_api 之前补三臂；CustomObjCmd 已由 garnet_api 承接不动。run_custom_command 的「执行域未接线」明确报错兜底保留（执行域接线属 wcustom 域，越界只记录）。
21. QUIT 哨兵消费（问题 B 关闭，对标 RespServerSession.cs:743-748）
    - wnode/src/traits.rs：MessageConsumerFace 增 take_dispose_request（默认 false；C# toDispose 哨兵通道）。
    - wnode/src/resp/resp_session_consumer.rs：实现 take_dispose_request 转发会话哨兵。
    - wnode/src/resp/resp_server_session.rs：公开 take_dispose_request（读 to_dispose；kill_requested 中间层随 19 删除后单哨兵）。
    - wnode/src/net/handler.rs drive_loop：写出段之后、既有断连哨兵判定处补 dispose 检查，命中 break（应答已发尽后断连，process_stream 收尾 dispose 对应 C# DisposeNetworkSender(true) → 连接关闭 → 会话 Dispose）。
    - 测试：wnode/tests/net_pump_consume_tests.rs 增 QUIT 断连用例（真 socket：收尽 +OK 后读端到 EOF）。

### 跳过保留（他待办认领/判活）

22. wnode/src/resp/admin_commands.rs commit_aof_async（:144）——next/gemini.md 条 1「COMMITAOF 空壳」认领，不动。
23. wnode/src/resp/resp_server_session.rs set_global_latency_metrics（:464）——C# RespServerSession.cs:265 构造期 LatencyMonitor 门装配面，生产在用；rust 装配域（监视器采样频率装配）被 next/glm.md 条 7 认领，接线落地即复活（与 task/reject/zero-ref-pubs-cleanup.md 的 stop_and_switch 先例同口径）。保留，报告注明。

### 恢复任务域甄别结论（RecoverLogDriver.cs / RecoverReplayTask.cs）

无孤儿可删、无接线可补。rust wnode/src/aof/recover/recover_log_driver.rs 的 RecoverLogDriver::new/run 生产链完整（aof_recover.rs:39/:83 消费，service.rs recover_aof 上层在册）；C# RecoverReplayTaskAsync 的页级双闸栏并行化已被 rust 有意折叠为顺序消费（模块头留档：跨子日志仍由调用方并行驱动多 driver），属转写决策面不是孤儿。

## rust 侧改动点

1. wnode/src/resp/basic_commands.rs：删 network_get_async / network_get_sg / parse_get_and_key / next_command_maybe_get / try_get_simple_command_info / set_result / network_quit / network_readonly / network_readwrite
2. wnode/src/resp/array_commands.rs：删 network_array_ping
3. wnode/src/resp/resp_server_session.rs：删 get_object_output / get_unified_output / abort_with_wrong_num_args_or_unknown_subcommand / debug_send / create_consistent_read_api / try_kill / kill_requested 字段 / flush_if_pending；增 take_dispose_request；process_other_commands 补 custom 三臂
4. wnode/src/resp/resp_server_session_output.rs：删 with_protocol_writer / process_output
5. wnode/src/resp/parser/session_parse_state.rs：删 initialize_with_argument / initialize_with_arguments
6. wnode/src/role_info.rs：删 to_metrics_string
7. wnode/src/shutdown.rs：删 wait_stopped
8. wnode/src/resp/metrics_commands.rs：删 new_global_latency_metrics
9. wnode/src/resp/rangeindex/range_index_manager_replication.rs：删 pending_stream_reassembly_count / dispose_incomplete_stream_reassembly
10. wnode/src/traits.rs：MessageConsumerFace 增 take_dispose_request 默认方法
11. wnode/src/resp/resp_session_consumer.rs：实现 take_dispose_request
12. wnode/src/net/handler.rs：drive_loop 写出段后补 dispose 哨兵断连
13. wnode/tests/net_pump_consume_tests.rs：增 QUIT 断连端到端用例
14. js/check/ignore/ 对应 yml：登记删除符号的 C# 映射（BasicCommands.yml / ArrayCommands.yml / RespServerSession.yml / RespServerSessionOutput.yml / SessionParseState.yml / RoleInfo.yml / RangeIndexManager.Replication.yml 等，以 check.js 实际输出为准）

## 验收口径

- ./clippy.sh 零警告（禁 allow）；./test.sh 全过；bun ./js/check.js 无新增缺失
- QUIT 端到端：客户端发 QUIT 收 +OK 后连接被服务端关闭（EOF）
- READONLY/READWRITE/QUIT 行为不回归（process_basic_commands 内联臂分派不变）
- CLIENT KILL 语义不回归（ConsumerEntry.kill_session + kill_watcher 路径不受影响）

## 拒绝的意见点（详见 task/reject/wnode-base-orphans.md）

1. READONLY/READWRITE/QUIT「补接线」改判为删重复面：生产分派臂已在 process_basic_commands 内联落地（resp_server_session.rs:1036-1058，含 cluster_session 接线），basic_commands.rs 三函数是同形重复。QUIT 的真实断点在泵哨兵（问题 B 单独成立并已修）。
2. 并行恢复任务（RecoverReplayTask.cs）「补接线」拒绝：rust 已决策顺序恢复折叠（recover_log_driver.rs 模块头留档），生产链完整（aof_recover.rs 消费），RecoverReplayTask.cs 函数面已在既有 ignore 登记。

## 验证结果

- 分支：w1-wnode-orphans（已合并 dev 后回并主干，worktree 已删，分支已删）
- 实际改动：15 文件 +200/-310（净缩 110 行）
- 删除 19 符号：命令面 9（network_get_async / network_get_sg / parse_get_and_key / next_command_maybe_get / try_get_simple_command_info / set_result / network_quit / network_readonly / network_readwrite）+ network_array_ping + 会话/输出面 8（get_object_output / get_unified_output / abort_with_wrong_num_args_or_unknown_subcommand / debug_send / create_consistent_read_api / with_protocol_writer / process_output / create_consistent_read_api 计 1 项外另含 to_metrics_string / wait_stopped / new_global_latency_metrics / try_kill / kill_requested / flush_if_pending）
- 接线 2 处：
  - process_other_commands 补 custom 三臂（Customtxn/Customrawstringcmd/Customprocedure → run_custom_command，对标 C# RespServerSession.cs:1099-1102），network_custom_raw_string_cmd 孤儿救活
  - MessageConsumerFace 增 take_dispose_request（默认 false），RespSessionConsumer 转发，泵写出段后检查命中即断连（对标 C# Process 尾部 if (toDispose) DisposeNetworkSender(true)）——QUIT 后连接正常关闭，问题 B 关闭
- 保留 4 项：commit_aof_async（gemini.md 条 1 认领）、set_global_latency_metrics（glm.md 条 7 装配域认领，C# 构造期 LatencyMonitor 门装配面判活）、to_metrics_string 判死删除（C# RoleInfo.ToString 生产零消费）
- 静态检查：./clippy.sh 0 警告（禁 allow）
- 自动化测试：./test.sh 全量通过（wedb 1994 项 + regress 2 项）；wnode 339 项含新增 QUIT 断连端到端 2 用例（回退形态 + 直读形态，真 socket 验证 +OK 发尽后 EOF）
- 检查脚本：bun ./js/check.js 本任务新增缺失/重复为 0；删除映射登记于 js/check/ignore/server.yml（BasicCommands.cs 6 函数 + SessionParseState.cs InitializeWithArgument + RangeIndexManager.Replication.cs DisposeIncompleteStreamReassembly）
- 遗留（他域待办，记录不修改）：
  - check.js 现存一处重复定义（libs/storage/Tsavorite/.../TsavoriteLog.cs:Reset → wedb/waof/src/log.rs:797 WalLog::reset 与 wedb/wnode/src/aof/waof_sublog.rs:261 WaofSublog::reset_async 双映射）——dev 侧并发提交 bd8d626 既有遗留，waof 域非本任务范围
  - run_custom_command 执行域未接线（C# TryTransactionProc / TryCustomProcedure 族），arity 校验 + 明确报错兜底在册，wcustom 域立项时打通
  - dispose_incomplete_stream_reassembly 已删，重建时点 = ds.net.md 条 1 检查点/RI 复制流转写时随 RangeIndexManager.Dispose 域重建
  - RI 流重组异常终止时临时文件依赖 deserializer.dispose 清理，管理器 Dispose 域缺位（与上条同一重建时点）
