# wconn-session-dispose-face（P1，会话层拆连面对标补齐）

来源：dispose 合并（dev 779343c1）后续主控复验 + 只读复核席线索。
**开工前置**（两条，任一未清不得起手）：
1. 他席在 dev 上解 `wedb/wconn/src/client.rs`、`src/network/mod.rs`、
   `tests/client_timeout.rs` 的合并冲突（refactor-r5h）——本票要改 `network/mod.rs`
   与 `session.rs`，必须等其落地并按现树重测锚位。
2. 另票 `wconn-client-dispose-lifecycle-guards` 同改 client.rs，本票避开 client.rs；
   而 `repl-sync-timeout-knob` 票在改 `replica_wire.rs`，本票的服务端接线须在其后。

## 问题
C# 会话层有完整拆连面，rust 会话层完全没有，且注释把它说成「与 C# 同态」——是错的：

- C# `libs/client/ClientSession/GarnetClientSession.cs:413-419`：
  `public void Dispose() { if (Interlocked.Increment(ref disposed) > 1) return;
  networkSender?.ReturnResponseObject(); socket?.Dispose(); networkHandler?.Dispose();
  if (!usingManagedNetworkPool) networkPool.Dispose(); }`
- C# 同文件 `:59 Disposed => disposed > 0`、`:64 IsConnected => socket != null && socket.Connected && !Disposed`
- C# 同文件 `:397-404 ReconnectAsync`：`if (Disposed) throw new ObjectDisposedException("GarnetClientSession")` 后 socket/handler Dispose 再连
- C# 服务端持有方的退场即调它：`PrimaryOps/AofOperations/AofSyncTask.cs:146-151`
  （`Dispose()` 内 `garnetClient?.Dispose()`）、`:349`；`AofSyncDriver.cs:141`
  （`aofSyncTask.garnetClient?.Dispose()`）

rust 现码（dev 尖端）：
- `wedb/wconn/src/session.rs:80-107 connect_async`：注释声称「与 C# GarnetClientSession
  无 Dispose(bool) 拆连面同态」，实际 C# 有上述 Dispose；实现把 `let mut dispose_sink = None;`
  传入 `network::connect_and_spawn`，句柄随局部变量即刻 drop ⇒ 每次会话建连白付一对
  dup+close syscall，却拿不到任何拆连能力。
- 会话无 `disposed` 位、无 `dispose()`、无 `Drop`，`is_connected`（如有）不反映拆连。
- 生产持有方：`wedb/wedb/src/server/replication/replica_wire.rs:64 pub client: GarnetClientSession`
  （对位 C# AofSyncTask 的 garnetClient 字段），其退场链
  （`aof_sync_driver.rs:39/:187/:604`、`aof_sync_task.rs` 的 `dispose()`，
  `replica_wire.rs:512 impl Drop for TcpSessionWire`）当前只断通道/置标志，
  不会令 socket 收场——静默对端下会话级连接与池缓冲可长期悬挂。

## 任务
1. **修正错误注释**（session.rs:84-86 区）：明写 C# 会话层确有 Dispose 面（锚单点化，
   只在 Dispose 实现处留锚，connect 处叙述）。
2. 会话层补齐拆连面，与 C#:413-419 逐臂等义：
   - `disposed` 幂等位（对标 `Interlocked.Increment > 1` 的「只生效一次」判定）
   - 持有 `DisposeHandle`（既有 `wedb/wconn/src/network/stream.rs` 的 dup-fd 句柄），
     `pub fn dispose(&self)` 置位并 `shutdown_both()`；`impl Drop` 转调
   - `is_connected` 翻假口径与 C#:64 等义（`!disposed && 通道存活`）
   - 若会话有 reconnect 形：`disposed` 命中即 `Err(Error::Disposed)`（对标 :397-404）
3. `network::connect_and_spawn` 的 `dispose_slot` 形参改为
   `Option<&mut Option<DisposeHandle>>`：会话侧传真槽，其它无拆连需求调用方传 None
   免付 dup+close（消除现「凑签名出参」的白syscall）。若某调用方必须留槽，写明理由。
   注意：本票改 `network/mod.rs` 签名，**client.rs 由另一票负责**——若 client.rs 的
   调用点需要随之改形参，只做「形参适配」的最小改动（一行），不得动其 dispose 语义，
   并在回报中申报触点。
4. 服务端接线：`replica_wire.rs` / `aof_sync_task.rs` 的既有 `dispose()` 退场链内调用
   会话 `dispose()`（对标 C# AofSyncTask.cs:146-151、AofSyncDriver.cs:141），
   使「驱动退场 ⇒ 复制出站会话拆连 ⇒ 池缓冲归还」成链。
5. 测试：wconn 侧新增/并入会话拆连锁测（半开假端点：`dispose()` 后 `is_connected` 翻假、
   在途命令以断连/dispose 错误收场、dup fd 归还）；wedb 侧若既有册可断「驱动 dispose 后
   会话不再可写」则并入，不造伪断言。

## 边界与纪律
- 允许改动面：`wedb/wconn/src/session.rs`、`src/network/mod.rs`、（必要时）`src/error.rs`、
  `wedb/wconn/tests/` 会话相关册、`wedb/wedb/src/server/replication/replica_wire.rs`、
  `aof_sync_task.rs`（仅接线一行级）。
- 禁触：`wedb/wconn/src/client.rs` 的 dispose 语义（另票）、`wedb/wkv/**`、
  `wedb/wnode/src/resp/objects/rmw_helpers.rs`、`task/refactor-backlog.md`。
- 禁 `#[allow]`、禁占位实现、禁改 Cargo.toml、禁跑 `./test.sh` / `./sh/clippy.sh`。
- 自检：`cargo check -q --workspace --all-targets` 零告警 + `cargo nextest run -p wconn`
  与 `-p wedb <复制相关册>` 定向绿；`bun js/check.js` 自查锚单点化。
- 净行纪律：补齐拆连面属被授权净增，控制在 +120 内并在提交信息写口径。
- 完工：单提交，`fix(wconn): ` 起头（若含 wedb 接线则在正文列出），只 add 指派文件；不 merge 不推 dev。

## 收口记录（2026-09-28 r6 波）
- 席提交：`b7e68c7b`；沙箱并 dev：`dd631796`；合入：`57f62359`（--no-ff，双引号消息）。
- 合入面（6 文件，+336/-22）：`wconn/src/session.rs`、`src/network/mod.rs`、`src/client.rs`、
  `wconn/tests/session_dispose.rs`（新册）、`wedb/src/server/replication/replica_wire.rs`、
  `wedb/tests/replica_wire_integration.rs`。
- 冲突处置：席的「尾巴修复」与 dev 上他席 `d3ed8b38` 同区重复，合入前取 dev 侧
  `client.rs` 全文，仅保留 `connect_and_spawn` 形参适配（`dispose_slot`）一行，
  由主控逐 hunk 复核；`client.rs`/`network/mod.rs` 与
  [[project-concurrent-session-edits-shared-tree]] 的同区串行纪律一致。
- 收口形态：会话层持有 `DisposeHandle`（自持 dup 句柄 + shutdown(Both)）并暴露幂等
  `dispose()` + `Drop`；`connect_and_spawn` 以 `Option<&mut Option<DisposeHandle>>`
  槽位交付拆连面，调用方仅 `client.rs:190` / `session.rs:105` 两处（主控 grep 复核）；
  `TcpSessionWire::disconnect` 触达 `client.dispose()`，对标
  `GarnetClientSession.cs:413-419` 无条件 `socket?.Dispose()` 与
  `AofSyncTask.cs:146-151` 的 Dispose 链；两处失实注释（原称 C# 无拆连面）已改为真实契约。
