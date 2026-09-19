# ACL GETUSER 回复帧改版本感知（HELLO 3 下 map/flags 头对位 C#）

来源：next/glm.data.md 第 4 条。基线：主仓 dev。

## 问题（已核实成立）

wedb/wnode/src/resp/acl_commands.rs 的 network_acl_get_user（约 :523）在 Some(user) 臂：
- :576 `write_map_len_resp2(output, 3)` 恒写 RESP2 扁平 map 头 `*6`；
- :580 `output.write_resp_array_len(1)` flags 段恒写 RESP2 数组头 `*1`。
HELLO 3 会话下 ACL GETUSER 聚合帧仍是 RESP2 形态，与 C# 分叉。
C# 对位 ACLCommands.cs:477 WriteMapLength(3)（RESP3 %3 / RESP2 *6）、:482 WriteSetLength(1)
（RESP3 ~1 / RESP2 *1）。

## 修法

network_acl_get_user 内：
- map 头 `write_map_len_resp2(output, 3)` → `cs::write_map_len(output, 3, self.resp_protocol_version)`
  （单点已存在：wresp/src/cmd_strings.rs:459）；
- flags 头 `output.write_resp_array_len(1)` → `cs::write_set_len(output, 1, self.resp_protocol_version)`
  （单点已存在：wresp/src/cmd_strings.rs:470）。
passwords 段 :585 write_resp_array_len(passwords.len()) 两侧同为数组帧（C# TryWriteArrayLength），不动。
ACL LIST / ACL USERS 的 :138/:205 计数数组、:244 categories 数组在 RESP2/RESP3 同为 `*N`，本票不动。
函数需能读到会话版本：若 network_acl_get_user 当前非 &self 方法，按仓内既有取版本口径
（self.resp_protocol_version，见 resp_server_session.rs）把版本传到位，勿另立第二版本源。

## 边界与验收

- 只动 wedb/wnode/src/resp/acl_commands.rs（必要时在调用点透传版本形参，不改 acl 逻辑/权限语义）。
- 与在途 resp3-command-layer-frame-parity（改 set_commands.rs / sorted_set_commands）不同文件，勿越界改那两处。
- RESP2 会话字节零变化（回归既有 RESP2 断言）；补 RESP3 下 ACL GETUSER 回 `%3` 与 flags `~1` 的断言。
- 子代理仅在 fork worktree 开发，仅 cargo check（私有 CARGO_TARGET_DIR），禁 test.sh/clippy/fmt，禁碰主树，禁 git add -A。
- 报告附 git log dev..HEAD、rev-parse HEAD(40)、diff --name-only、REAL_EXIT。

## 细化方案（认领后核实追加，2026-09-19）

甄别核实（行号按当前 dev 实况，票面 :576/:580/:585 实为 :572/:576/:581，内容属实）：
- network_acl_get_user 为 &self 方法，:569 已用 self.resp_protocol_version，版本直接可读，无需透传形参。
- 单点在场：wresp/src/cmd_strings.rs:457 write_map_len（RESP3 %N / RESP2 *2N）、:468 write_set_len
  （RESP3 ~N / RESP2 *N），与 C# RespServerSessionOutput.cs:164 WriteMapLength、:238 WriteSetLength
  的版本分派逐分支对位（C# else 臂均 TryWriteArrayLength，语义一致）。
- write_map_len_resp2 在 acl_commands.rs 仅 :572 一个调用点，改后同步从 import（:22）摘除。
- C# ACLCommands.cs NetworkAclGetUser：passwords 段 TryWriteArrayLength（版本无关 *N）、commands 段
  bulk（版本无关），均不动。确认与票面一致。
- 查重：reject/acl-getuser-resp3-frame-parity-dup.md 是另一张重复票被拒时命中本票；ing 内
  resp-frame-literal-single-source（版本无关基础帧型 13 位点）与 auth-ns-default-spec-drift（纯文档）
  均不同题，无冲突。

实施（worktree 分支 f36-acl-getuser）：
1. wedb/wnode/src/resp/acl_commands.rs:
   - :572 `write_map_len_resp2(output, 3)` → `cs::write_map_len(output, 3, self.resp_protocol_version)`
   - :576 `output.write_resp_array_len(1)` → `cs::write_set_len(output, 1, self.resp_protocol_version)`
   - import 摘除 write_map_len_resp2
2. wedb/wnode/tests/acl_tests.rs get_user_test：alice RESP2 断言后置 session.resp_protocol_version = 3
   重调 GETUSER，断言帧头 `%3\r\n$5\r\nflags\r\n~1\r\n$2\r\non\r\n`，passwords/commands 段不变。
3. 仅 cargo check（CARGO_TARGET_DIR=/tmp/wt-target-f36）。
