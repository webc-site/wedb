# AOF 两处门控与吞没修正（aof-gates）

来源：next/ds.net.md 条 19、14（主代理预清理后下发）。分支 w4-aof-gates。

## 甄别结论（两条全成立）

一、[P2] StoreRMW 重放未知命令 warn 后吞没：成立。

对标核实：
- C# AofProcessor.cs:StoreRMW 把 input 直通 stringContext.RMW（Tsavorite
  RMW 面），无未知命令分支缺口；
- C# MainStore/RMWMethods.cs InPlaceUpdaterWorker 的 default 分支尾部
  `throw new GarnetException("Unsupported operation on input")`——未知命令
  重放即抛异常，恢复显式失败；
- C# 重放链全链无 catch（AofReplayCoordinator.cs:ProcessTransactionGroup
  Operations 直接调用 ReplayOpDispatch 不捕获；AofProcessor.cs 无 catch），
  异常沿 Recover 上抛至服务器启动失败。

rust 偏差两处（均在 wedb/wnode/src/aof/aof_processor.rs）：
1. store_rmw 落空分支 log::warn! 后 `return Ok(())`，恢复静默继续成功；
2. process_transaction_group_operations 对组内条目 Err 只 warn 后吞并返回
   ()，偏离 C# 不捕获语义——即使 1 改 Err 也会被此吞没，必须一并打通。

写入端现状核实（可入 AOF 的 StoreRMW 命令全集，store_rmw 分支已全覆盖）：
- service.rs 事件 sink：Pexpireat/Persist（TtlWrite）、Setwithetag
  （EtagWrite）、Riset/Ridel（RangeIndexWrite）、Ricreate
  （RangeIndexCreate）、Delifexpim（TtlPurge）；
- range_index_manager_replication.rs:229/260（RI 迁移流块）；
- vector_manager_replication.rs:64（Vadd/Vrem/Vsetattr）。
主存 RMW 命令（INCR/APPEND/SETRANGE 等）写入端已固化为读 + 盲写 upsert
（StoreUpsert 条目），重放端对应分支保留（重放面等价实现）。

取舍：选「落空分支改 Err」。否决编译期穷尽绑定——需把「StoreRMW 可入
AOF 命令集」子集类型化并贯穿写入端 3 个生产点与 ReplayInput 线格式
（RespCommand 子集化），越界面大；Err 方案行为对齐 C# GarnetException，
演化失配时恢复立即失败而非静默丢数据，安全等价且改动最小。

二、[P2] handle_aof_commit_mode 缺配置门控：成立。

对标核实：C# RespCommand.cs:ParseCommand 尾部（1206-1208 行）
`if (storeWrapper.serverOptions.EnableAOF && storeWrapper.serverOptions.
WaitForCommit) HandleAofCommitMode(cmd);`；rust parse_command_with 尾部
（resp_command.rs:729）无条件调用。

配置位现状：wconf 单源化 RuntimeServerOptions 已有 enable_aof /
wait_for_commit（GarnetServerOptions.cs:EnableAOF/WaitForCommit 对应，
默认均 false）。RespServerSession 未持 serverOptions 投影；按既有先例
（connection_protection_debug = EnableDebugCommand 镜像）在
RespServerSessionOptions 增加两字段并投影为会话私有字段，调用点补门控。

消费点核实：C# Send() 中 `if (waitForAofBlocking) WaitForCommitAsync()`
依赖写入端门控免检；rust 侧 wait_for_aof_blocking 当前无生产消费点
（网络泵 Send 等价物未接 AOF 等待），仅测试断言——现状靠消费点自我豁免
的隐患在演化，门控前移对齐 C# 后语义闭环。

## 改动计划

1. wedb/wnode/src/aof/aof_processor.rs
   - store_rmw 落空分支改返回 Err（文案对齐 C# "Unsupported operation
     on input" 语义）；
   - process_transaction_group_operations 改返回 Result 上传组内条目错误，
     两处调用点（TxnAction::Commit、process_fuzzy_region_transaction_group）
     加 ? 传播。
2. wedb/wnode/src/resp/resp_server_session.rs
   - RespServerSessionOptions 增加 enable_aof / wait_for_commit（默认
     false，对齐 C# 默认）；RespServerSession 增加私有投影字段。
3. wedb/wnode/src/resp/parser/resp_command.rs
   - parse_command_with 尾部调用点补 `enable_aof && wait_for_commit` 门控。
4. wedb/wedb_standalone/src/main.rs
   - 宿主装配把 node.aof 接入 enable_aof（wait_for_commit 无 CLI 参数源，
     默认 false 即 C# 默认行为；NodeArgs 缺 wait_for_commit 启动参数为
     范围外缺口，仅记录）。
5. 测试
   - 未知 RMW 恢复报错路径（StoreRMW 条目 cmd 为非 RMW 命令 → 重放 Err）；
   - 门控开关行为（门关：解析 SET 不置位；门开：置位）。

## 验证结果

1. bun ./js/check.js：零输出（0 缺失，0 重复；分支与合并后主目录均验）。
   本次无删除带 C# 映射注释的符号，不需 check/ignore 登记。
2. ./clippy.sh（cargo clippy --workspace --all-targets 等价核验）：
   0 警告 0 错误（禁 allow，分支与合并后主目录均验）。
3. ./test.sh：分支 2030 passed / 0 failed / 1 skipped + regress 2 passed；
   合并后主目录 HEAD（07c04da）同数字全过。
4. 新增测试（wedb/wnode/tests/aof_store_rmw_replay.rs）：
   - store_rmw_replay_unknown_cmd_fails_recover：StoreRMW 条目携带命令集
     外命令（GET）→ single_log_recover 返回 Err 且文案含 unsupported cmd；
   - store_rmw_replay_unknown_cmd_in_txn_group_propagates：TxnStart →
     未知 RMW → TxnCommit 组内失败传播（验证吞没点已打通）。
   wedb/wnode/tests/resp_command_parse.rs 新增
   aof_commit_mode_gate_controls_flag_maintenance：门关解析 SET 不置位、
   门开置位。

## 工程备忘

- 范围外缺口仅记录：wconf NodeArgs CLI 未暴露 wait_for_commit 启动参数
  （RuntimeServerOptions.wait_for_commit 已有字段；单机装配按 C# 默认
  false 处理，AOF 阻塞标记不维护——与 C# 默认行为一致）。
- store_rmw 重放分支中 Incr/Append/Setrange/Expire 族保留：对应 C#
  StoreRMW 直通 RMW 面的重放语义，主存 RMW 命令写入端虽已固化盲写
  upsert，重放面等价实现不属死代码清理对象。
- 并发期验收均用独立 CARGO_TARGET_DIR=/tmp/fork/w4-aof-gates-target 复核。
