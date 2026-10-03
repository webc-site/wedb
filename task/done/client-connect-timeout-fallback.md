# wedb-client-connect-timeout-invented-default-cap（P2，臆造建连兜底超时删除）

## 甄别结论：通过（2026-09-28 主控现码复验，双侧锚均实测）

C# 建连限时严格由 `timeoutMilliseconds` 单值驱动，**0 即不限时**（现树实测锚）：
- garnet/libs/client/GarnetClient.cs:152 构造形参 `int timeoutMilliseconds = 0`，:184 存字段
- garnet/libs/client/GarnetClient.cs:264 `socket = await ConnectSendSocketAsync(timeoutMilliseconds, token)`
  —— 原值透传，无任何「为 0 时补一个缺省」的分支
- garnet/libs/client/GarnetClient.cs:418-438 `TryConnectSocketAsync(..., int millisecondsTimeout, ...)`：
  主体被 `if (millisecondsTimeout > 0)` 包裹，else 分支直接 `socket.ConnectAsync(endpoint)` 不限时
- garnet/libs/client/GarnetClient.cs:216/:268 超时检查任务同样仅 `> 0` 才启用

rust 侧自造兜底（唯一偏离点）：
- wedb/wedb/src/client.rs:17 `const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5000;`
- wedb/wedb/src/client.rs:155-158 `let connect_timeout_ms = if self.timeout_ms > 0 { self.timeout_ms } else { DEFAULT_CONNECT_TIMEOUT_MS };`
- wedb/wedb/src/client.rs:145-147 注释「0 = 在途超时关闭……建连保留缺省限时兜底」—— 该叙述即偏离本体，须一并改写

后果：调用方显式要求「在途超时关闭」（timeout_ms=0，对标 C# TimeoutChecker 不启用）时，
rust 仍对建连强加 5s 上限；慢端点/首包挂起场景下 C# 恒等连、rust 提前放弃并走
「建连限时放弃臂」（:167-175 的 warn + Drop 拆连），行为分叉且无对应 C# 语义。

## 任务
1. 删除 `DEFAULT_CONNECT_TIMEOUT_MS` 常量与 `if self.timeout_ms > 0 {...} else {...}` 兜底：
   `timeout_ms > 0` 时以 `Duration::from_millis(self.timeout_ms)` 包 `timeout(...)`；
   `timeout_ms == 0` 时**直接 `client.connect_async().await` 不套限时**，把两臂的
   Ok/Err 结算收敛到同一后续处理（对标 C# `TryConnectSocketAsync` 的 >0 门 +
   ConnectAsync 失败即返回假的等价形态，勿引入第三套错误码）。
2. 改写 :145-147 注释：0 即「在途超时与建连限时同时关闭」，与 C#:185/:216/:268 同态；
   不得保留「缺省限时兜底」字样。
3. 排查并修正依赖旧 5s 兜底的既有用例与调用点：
   `git grep -n "timeout_ms\|with_timeout\|DEFAULT_CONNECT_TIMEOUT" -- wedb/wedb wedb/wnode`
   逐个判定；确需限时收场的测试改为**显式传非零超时**（并在注释写明是为锁该臂而显式化），
   绝不把兜底常量请回来，也绝不留下会永久挂起的用例。
4. 补/改一处回归锁测（同族既有测试文件优先）：`timeout_ms == 0` 时对静默端点建连
   **不得**在固定 5s 处自动失败——用可控收场（端点随后 accept 或测试自身限时）断言
   「不限时」语义，且不得让 nextest 真的永久挂住。

## 边界与纪律
- 只在 worktree `/tmp/fork/client-connect-timeout-fallback` 内改动，私有 target `/tmp/_rs/client-connect-timeout-fallback`（已预热）。
- 禁触：`wedb/wconn/**`（他席在途合并冲突面，本票只调门面侧用法）、`wedb/wedb/src/server/replication/**`
  （另席 repl-sync-timeout-knob 在改，勿碰 replica_wire.rs / replica_sync_session.rs /
  replication_snapshot_iterator.rs）、`task/refactor-backlog.md`。
- 禁 `#[allow]`、禁占位实现、禁改 Cargo.toml、禁向下兼容垫片。
- 禁跑 `./test.sh` 与 `./sh/clippy.sh`（门禁归主控）。
- 自检：`cargo check -q --workspace --all-targets` 零告警 + `cargo nextest run -p wedb` 相关册定向绿。
- 净行预期减行（删常量与三元）；若为显式化超时致测试净增，申报口径即可。
- 完工：单提交，消息以 `fix(wedb): ` 起头，只 add 指派文件。

## 收口记录（2026-09-28 r6 波）
- 席提交：`f93f1b09`；合入：`ff588257`（--no-ff，双引号消息）。
- 合入面（2 文件，+111/-28）：`wedb/wedb/src/client.rs`、`wedb/wedb/tests/node_connection_gossip_delay.rs`。
- 收口形态：删臆造 `DEFAULT_CONNECT_TIMEOUT_MS = 5000` 与三元兜底，改为
  `timeout_ms > 0` 才包裹 `timeout(...)`、否则无限等待，对位 `GarnetClient.cs:264`
  原值透传与 `TryConnectSocketAsync:418-438` 的 `if (millisecondsTimeout > 0)` 臂；
  测试侧以显式小超时 + 保留端口断 `Timeout`，不靠 nextest 永挂。
- 归属澄清：席报的唯一红 `error_sink_tests::boot_tls_gate_error_routed_through_node_variant`
  经基线孤立复跑证伪与本票无关（基线 `04169915` 同样红），详见红灯册。
