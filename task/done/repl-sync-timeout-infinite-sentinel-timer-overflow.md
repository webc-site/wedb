# 复制同步超时「无限哨兵」直折 Duration 撞 compio 定时器溢出 panic（P2）
> 收口（2026-09-28 12:2x）：席 58fd651（12 文件 +146/-52，生产码净 +21，锁测 +82/-9）
> 合入 dev 154b11d（--no-ff 双引号消息，merge-tree 预检 rc=0 零冲突，fileset 与
> git diff --name-only HEAD^1 HEAD 全等）。票面 §1-§3 全数落地：值源
> ClusterProvider::replica_sync_timeout() 折 Option<Duration>（哨兵→None）、哨兵字面值提为
> wconf::node_options::INFINITE_SYNC_TIMEOUT_SECS 单源、replica_wire connect 与
> replication_snapshot_iterator 三处 timeout 统一收口 crate::server::wait_async、
> Err 文案逐臂亲验逐字保持；锁测两枚（provider 面 0/负/显式哨兵同折 None + connect 丙臂
> 静默端点 10s 窗内不 panic 不超时）。席自报 numstat 与实测全等，禁区零命中，
> 未跑门禁（合规）。§4 登记项主控已另立案并派席：
> task/ing/aof-replay-barrier-infinite-sentinel-coarsetime-overflow.md（席独立复核同结论：
> coarsetime-0.1.38 instant.rs:318-325 裸 u64 加溢出，dev 形态 panic、release 回绕假超时；
> parking_lot util.rs:36-38 与 event-listener lib.rs:866-869 两 sink 系 checked_add 偶然安全）。
> worktree/分支/私有 target 三清。
> 快照注记（2026-09-28 11:07）：本仓历史已被 squash 为单 `init` 提交，票面所引历史哈希
> （`c92de925`/`99d14ab0`/`eeae7e48`/`797b7b0b` 等）均不可 resolve；一切定位以**现树内容**为准
> （锚点行号请以 worktree 尖端复核，票内行号系 squash 前实测）。

定级：P2（`--repl-sync-timeout 0`／负值是文档化口径；命中即在主端复制链 panic 断链）

## 甄别结论：通过（2026-09-28 主控现码 + 最小复现实测）

1. C# 契约
   `garnet/libs/host/Configuration/Options.cs:995` 一带把非正值折为
   `Timeout.InfiniteTimeSpan`——语义是「不挂计时器、永等」，不是「挂一个巨大时限」。
   复制出站链的 `ConnectAsync(ReplicaSyncTimeout)` / `WaitAsync(ReplicaSyncTimeout)`
   收到 InfiniteTimeSpan 时即走无计时器臂。

2. rust 现状（r6 波 `99d14ab0`/合入 `c92de925` 落地后尖端实测）
   - 投影侧折哨兵：`wedb/wconf/src/node_options.rs:1486-1489`
     `if self.replica_sync_timeout_secs <= 0 { u64::MAX } else { … as u64 }`
     （注释自称对标 C# `<=0 ? InfiniteTimeSpan`；`runtime_server_options.rs:152` 同口径）。
   - 消费侧折 Duration：`wedb/wedb/src/server/cluster_provider/flags.rs`
     `replica_sync_timeout()` 返 `Duration::from_secs(self.replica_sync_timeout_secs.load(..))`
     ——**不判哨兵**。
   - 四个限时点把它直接送进 compio 定时器：
     `replica_wire.rs` `time::timeout(timeout, client.connect_async())`；
     `replica_sync_session.rs` `let timeout = Some(provider.replica_sync_timeout())`
     → `wait_async(Some(..))`（`server/mod.rs:29` 只认 `None` 为无限）；
     `recover_roundtrip(.., timeout, ..)` 同上；
     `replication_snapshot_iterator.rs` 的 `timeout(sync_timeout, ..)` 三处
     （扇出 CLUSTER SYNC / `reserve_vector_set_contexts_async` / `send_sync_frame`）。
   - 溢出机理实证：`compio-runtime-0.12.6/src/time/mod.rs:53`
     `sleep(d) = sleep_until(Instant::now() + d)`，std `Instant + Duration::from_secs(u64::MAX)`
     溢出即 panic——主控用 /tmp 最小件 `ovf.rs` 跑 `catch_unwind` 确证 panic（非饱和）。
   - 仓内已有正确认识与单机制：`wedb/wedb/src/server/mod.rs:25-37` 的 `wait_async`
     文档注释明写「compio 的 `timeout(d, f)` 内部 `Instant::now() + d`，`d` 取
     `Duration::MAX` 即溢出 panic，故无限时不挂计时器直接 await」。本票即补齐被绕过的那一侧。

3. 逻辑危害
   运维按文档设 `--repl-sync-timeout 0`（或不带参的负值臂）→ 副本建连限时、快照逐帧
   应答、`BEGIN_REPLICA_RECOVER` 停等、diskless 扇出任一臂触发定时器构造 → 任务 panic，
   主端复制链断；C# 同输入为「永等不超时」，语义完全反向。

## 涉及代码

rust 文件与函数：
- `wedb/wedb/src/server/cluster_provider/flags.rs:ClusterProvider::replica_sync_timeout`（值源单点）
- `wedb/wedb/src/server/cluster_provider/mod.rs:ClusterProvider`（`replica_sync_timeout_secs` 槽）
- `wedb/wedb/src/server/replication/replica_wire.rs:TcpSessionWire::connect`（形参与建连限时臂）
- `wedb/wedb/src/server/replication/replica_sync_session.rs:connect_replica_stream_wire /
  recover_roundtrip / 快照逐帧臂`（下传与 `wait_async` 口）
- `wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs:
  SnapshotIteratorManager::{new, fan_out_send} / transmit_vector_sets_to_session / send_sync_frame`
- `wedb/wconf/src/node_options.rs`（投影侧 `u64::MAX` 字面值 → 具名常量单源）
- 测试册：`wedb/wedb/tests/replica_wire_integration.rs`（既有双臂锁测同族扩形）

对应 c# 文件与函数：
- `garnet/libs/host/Configuration/Options.cs:995`（`<=0 ? Timeout.InfiniteTimeSpan`）
- `garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:121/:140/:141/:184`
  （同一旋钮的无计时器语义落点）

## 精炼执行方案

1. 值源改 `Option<Duration>` 形制：`ClusterProvider::replica_sync_timeout()` 返
   `Option<Duration>`——无限哨兵值 → `None`，正值 → `Some(Duration::from_secs(secs))`。
   哨兵字面值不得两处裸写：在 `wconf/src/node_options.rs` 提具名常量（如
   `INFINITE_SYNC_TIMEOUT_SECS: u64 = u64::MAX`）供投影臂与本口共引（禁新增 Cargo 依赖，
   禁改 Cargo.toml）。
2. 消费点统一收口既有单机制：`TcpSessionWire::connect` 形参改 `timeout: Option<Duration>`，
   建连臂由 `time::timeout(..)` 改走 `crate::server::wait_async`（`None` 即不挂计时器），
   超时到点仍落 `ErrorKind::TimedOut`；`replication_snapshot_iterator.rs` 三处
   `timeout(..)` 同改（字段/形参随之 `Option<Duration>`，`sync_timeout` 仍构造期一次折取），
   `Err` 文案与判败面逐字保持（既有测试逐字断言在案）。
3. 锁测（不加伪断言）：
   a. provider 面：`set_replica_sync_timeout_secs(0 → 经投影入槽的哨兵值)` 与显式哨兵值 →
      `replica_sync_timeout() == None`；正值 → `Some(Duration::from_secs(值))`。
   b. 运行面：静默端点上把哨兵（`u64::MAX`）送进 `TcpSessionWire::connect`，在有限观察窗
      （10s）内既不得 panic 也不得超时收场（沿用 `replica_sync_timeout_knob_drives_connect_window`
      双臂形制，扩第三臂即可，勿新建重复册）。
4. 登记不改（跨域）：`wedb/wnode/src/aof/garnet_append_only_file.rs:85`
   `read_timeout: Duration::from_secs(server_options.replica_sync_timeout_secs)` 同形制隐患，
   属 waof/wnode 段管理域，本票不越界代修；若席能单点证其为 `Some` 定时器消费则只在报告列名。

## 边界与纪律
- 只在 worktree `/tmp/fork/repl-sync-timeout-infinite-sentinel` 内改动，私有 target
  `/tmp/_rs/repl-sync-timeout-infinite-sentinel`。
- 禁触：`wedb/wconn/**`、`wedb/wnode/**`（含上述 aof 同形制点）、`wedb/wkv/**`、
  `wedb/wedb/src/client.rs`、`task/refactor-backlog.md`。
- 禁 `#[allow]`／`#[expect]`、禁占位实现、禁改 Cargo.toml、禁向下兼容垫片（不留
  「常量作缺省回落」也不留「巨大时限作无限」的折中形态）。
- 禁跑 `./test.sh` 与 `./sh/clippy.sh`（门禁归主控）。
- 自检：`cargo check -q --workspace --all-targets` 零告警 + `cargo nextest run -p wedb`
  相关册（`replica_wire_integration` / `replication_*` / `appendlog_reject_disconnect`）定向绿；
  改动文件逐个 `rustfmt`；`bun js/check.js` 自查无新增重复定义簇（锚单点化）。
- 净行：默认近零或减行；若 `Option` 形参链净增控制在 +30 内并在提交信息写明口径。
- 完工：单提交，消息以 `fix(wedb): ` 起头，只 add 指派文件；不 merge 不推 dev。
