WAIT-FOR-COMMIT 两端断链：配置无 CLI 源、会话零阻塞消费

来源：第 8 轮 design 条 2（HIGH）。分支 wait-for-commit-chain 未并入 dev，按 HEAD 事实仍为断链。

现状
- 配置侧只半装：wedb/wconf/src/runtime_server_options.rs:77 字段 wait_for_commit、:128 缺省 false；
  wedb/wconf/src/runtime_server_config.rs:87 仅供 CONFIG GET 格式化。NodeArgs 无对应 CLI 项
  （wedb/wconf/src/node_options.rs:218 段 grep wait_for_commit 零命中），即「写侧无人可写」。
- 会话侧门控自认缺源：wedb/wnode/src/resp/resp_server_session.rs:193 注释「wait_for_commit 无 CLI 参数源，
  随 Default 取 false」，:498 `aof_commit_mode_gate: options.enable_aof && options.wait_for_commit` 恒假。
- 消费侧零读者：解析器维护 wedb/wnode/src/resp/parser/resp_command.rs:449 复位、:455 置位
  `wait_for_aof_blocking`（字段定义 resp_server_session.rs:290），全生产代码无读点
  （grep `wait_for_aof_blocking` 仅 tests/resp_command_parse.rs:391-431 断言）。
- 等待原语已在位但无人等：wedb/wnode/src/aof/garnet_log/commit.rs:149 wait_for_commit_async、
  wedb/waof/src/wal/flush.rs:126 wait_for_commit、
  wedb/wnode/src/database/single_database_manager.rs:235 wait_for_commit_to_aof_async——生产命令路径零调用。
- 装配期互校验缺席：C# 对 commit-frequency 与 wait-for-commit 的组合校验在 rust 无对位。

C# 参考
- garnet/libs/host/Configuration/Options.cs:253 `bool? WaitForCommit`、:934 投影；garnet/libs/host/defaults.conf:185 默认 false。
- garnet/libs/server/AOF/GarnetLog.cs:496 `WaitForCommit(untilAddress, commitNum)`。
- garnet/libs/cluster/Session/ClusterSession.cs:166-168 `appendOnlyFile != null && serverOptions.WaitForCommit` →
  `Log.WaitForCommitAsync()`（应答前等待提交的消费点形态）。
- garnet/libs/host/GarnetServer.cs:508、:519 装配期互校验（CommitFrequencyMs/WaitForCommit 组合非法即拒启）。

修法
- wconf NodeArgs 增该开关并经 ServerArgs 访问器（node_options.rs:807）投影到 RuntimeServerOptions，
  进 nested_text 导出；装配期补 C# 同位互校验（与 task/ing/aof-size-knobs-read-side-wiring.md 共用一处校验入口，
  不造第二套参数体检函数）。
- 命令收尾路径按 `aof_commit_mode_gate && wait_for_aof_blocking` 单点挂 await 等待提交，
  等待原语直取既有 wait_for_commit_async；解析器侧门控保持现形（勿改 RESP 字节）。
- 端到端用例：开 AOF + wait-for-commit，客户端应答返回时该命令记录必已刷盘（现有用例只断言解析位置位，
  tests/resp_command_parse.rs:391-431，不算链路证据）。

优先级：功能缺口（已声明的持久化语义实际不生效），高。

验收
- 开关关闭时行为逐字节不变；开启时 COMMIT 前应答不返回；grep 无「字段有、读者零」复发。

处置（2026-09-19）：已失效，实现已全部在当前 dev HEAD，拒绝归档。

核实（以当前代码为准，票据取证基线早于仓库 squash init 3c4f74a4）：
- 修法 1 在位：wedb/wconf/src/node_options.rs:293-295 CLI `--aof-commit-wait`；
  :670 投影 opts.wait_for_commit（对标 Options.cs:934）；serde derive 纳入 nested_text
  导出面（:348 注释、:741 override_explicit 含 aof_commit_wait）；
  :649-653 装配互校验 AofCommitWithoutAof（对标 GarnetServer.cs:508；:519 组合
  在 rust 无编码通路已有注释）。
- 修法 2 在位：wedb/wnode/src/net/handler/drive.rs:199-201（命令应答出网前置等待）、
  :315-317（推送帧同门），对标 RespServerSession.cs:1453；等待链
  SessionProviderFace::wait_for_commit_async（service.rs:1635）→
  wait_for_commit_to_aof_async（single_database_manager.rs:235）→
  commit.rs:149；解析器门控保持现形（resp_command.rs:449/:455）；
  aof_commit_mode_gate 有真实源（resp_server_session.rs:209/:501）。
- 修法 3 在位：wedb/wnode/tests/aof_commit_wait_e2e.rs 正反两测
  （wait_for_commit_reply_follows_flush / without_wait_for_commit_reply_precedes_flush）。
- 验收过：net_pump_consume_tests.rs:462 pump_without_aof_commit_wait_never_waits
  （门关零等待）；wait_for_aof_blocking 生产读点 drive.rs:199/:316 非零。
- C# ClusterSession.cs:166-168 消费点：rust 集群会话为 RESP 会话切面
  （resp_server_session.rs:373-375），出网走同一 drive 泵写出段，同一等待门承接，
  无需第二处。
