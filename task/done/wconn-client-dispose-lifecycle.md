# wconn-client-dispose-lifecycle-guards（P2，dispose 生命周期三面偏离收口）

来源：dispose 合并（dev 779343c1 / 007eea65）只读复核席三则发现，主控已复验现码。
**开工前置**：他席正在 dev 上解 `wedb/wconn/src/client.rs`、`network/mod.rs`、
`tests/client_timeout.rs` 的合并冲突（refactor-r5h，timeout_checker 时序面）。
本票必须在冲突落地后开工，并对现树逐锚复核——主案若已被别波落地，判灭失。

## 三面（均双侧锚实测）

### 面一（P1，资源泄漏）：未 dispose 直接二次 connect_async 会静默丢旧代拆连句柄
- rust：`wedb/wconn/src/client.rs:148-152` `connect_async` 直接换代 `disposed` Arc，
  随后 `*dispose_slot = Some(handle)` 覆盖旧 `DisposeHandle` —— 旧句柄被静默 drop，
  只 close dup fd、**不下发 shutdown**；旧连拆除全赖旧 tx 断→writer_done 路
  （`network/pump.rs:206-209`）。若旧写泵正挂 `write_all`（对端不读/缓冲满），
  旧读写泵、fd、池缓冲永不收场，且再无句柄可拆（Drop 兜底只触新代）。
  现仓内不可达（门面 `wedb/wedb/src/client.rs` 与 `reconnect_async` 均先 dispose），
  属 pub API 泄漏面。
- 修法（最小）：`connect_async` 首行 `self.dispose();`（全新实例置位后仍换新 Arc，零副作用）。
- C# 无需新锚：C# `ConnectAsync` 一次实例一次连，不存在换代复用面；本面是 rust 显式
  代际语义（reconnect 重建代）自带的守卫缺口。

### 面二（P2，守卫语义）：dispose 后 reconnect_async 应恒拒
- C# `libs/client/GarnetClient.cs:499` `ReconnectAsync`：`if (Disposed) throw disposeException;`
  （:501-509 随后 socket/networkWriter Dispose 再 ConnectAsync）
- rust：`wedb/wconn/src/client.rs` 的 `reconnect_async` 先 `self.dispose()` 再
  `connect_async()`，而 `connect_async` 会换新一枚 `disposed` Arc ⇒ 已 Dispose 实例
  被允许复活，与 C# 恒拒相反。
- 修法：`reconnect_async` 首判 `self.disposed.load(Acquire)` 即 `Err(Error::Disposed)`。

### 面三（P2，错误形制）：dispose 后命令面应为 Disposed，而非入通道后被动结算
- C# 发送路径在槽位填满后即时判定，六处同形：
  `libs/client/GarnetClient.cs:630`、:733、:846、:934、:1050、:1165
  `if (Disposed) { DisposeOffset(shortTaskId); ThrowException(disposeException); }`
  （异常型 `GarnetClientDisposedException`，:103 `disposeException` 单例）
- rust：`wedb/wconn/src/client.rs` 各 `execute_*` 无 disposed 判定，`channel()` 只查
  `tx.is_some()`；dispose 后 tx 仍 Some，命令照常入通道，最终以
  `ResponseChannelClosed`/`ReadPumpExited` 结算（`types.rs:195-200`）；
  且 `execute_no_response_async` 在 ≤250ms 窗内 send 成功可返回 `Ok`，而帧永不达对端。
- 修法：各 `execute_*`（含 no-response 形）入通道前先判 `disposed`，命中即
  `Err(Error::Disposed)`；顺带让 `wedb/wconn/src/error.rs:14/24` 的
  `Disposed`/`SocketDisposed` 由死码转为有构造点（`SocketDisposed` 若无自然归属点，
  按灭失处理并申报，勿造伪使用）。
- 测试：既有 dispose 册（`wedb/wconn/tests/dispose_reclaim.rs`）仅断 `expect_err`/
  `ResponseChannelClosed`，须补锁「dispose 后新命令即刻 `Error::Disposed`」与
  「no-response 形不得返回 Ok」；面二补一条 `reconnect_async` 恒拒锁测。

## 边界与纪律
- 允许改动面：`wedb/wconn/src/client.rs`、`src/error.rs`、`src/network/pump.rs`（如需）、
  `wedb/wconn/tests/dispose_reclaim.rs` 及必要的同族测试册。
- 禁触：`wedb/wconn/src/session.rs`、`src/network/mod.rs`（与会话拆连面另票同区，
  由该票负责）、`wedb/wedb/src/server/replication/**`、`wedb/wkv/**`、
  `task/refactor-backlog.md`。
- 禁 `#[allow]`、禁占位实现、禁改 Cargo.toml、禁跑 `./test.sh` / `./sh/clippy.sh`。
- 自检：`cargo check -q --workspace --all-targets` 零告警 + `cargo nextest run -p wconn`
  定向绿；写完 `bun js/check.js` 自查无新增重复定义簇（锚单点化）。
- 完工：单提交，`fix(wconn): ` 起头，只 add 指派文件；不 merge 不推 dev。

## 收口记录（2026-09-28 r6 波）
- 席提交：`c0dd9065`；合入：`c0b026e7`（--no-ff，双引号消息）。
- 合入面（`git diff --name-only c0b026e7^1 c0b026e7` 实测两文件，无越界）：
  `wedb/wconn/src/client.rs` + `wedb/wconn/tests/dispose_reclaim.rs`，2 files, +193/-14。
- 收口形态：三面并轨——`connect_async` 换代先拆旧代（dispose + dispose_handle 置空 +
  disposed 位重挂新 Arc）；`reconnect_async` 见 disposed 恒拒 `Error::Disposed`；
  命令面统一经 `channel()` 守卫即刻 `Error::Disposed`，对标
  `GarnetClient.cs:501` 的 `if (Disposed) throw disposeException` 与六处发送面守卫
  （:630/:733/:846/:934/:1050/:1165）。锁测三条新增，既有断言由 `is_err` 收紧为
  具体 `Error::Disposed`。
