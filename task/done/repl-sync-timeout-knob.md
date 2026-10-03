# wedb-repl-sync-timeout-knob-hardcoded（P2，对标 C# ReplicaSyncTimeout 活旋钮）

## 甄别结论：通过（2026-09-28 主控现码复验，双侧锚均实测）

C# 复制域全部超时点均读活配置 `serverOptions.ReplicaSyncTimeout`（现树实测锚）：
- garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:121（ConnectAsync 传 TotalMilliseconds）、:140、:141、:184
- garnet/libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs:143
- garnet/libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicationSyncManager.cs:38（字段 replicaSyncTimeout = opts.ReplicaSyncTimeout）
- garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:185、AofSyncTask.cs:323
- garnet/libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:68、:165
- garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:134、:142、:273、:274

rust 侧旋钮**已在位但未被复制出站链读取**（灭失判定不成立）：
- wedb/wconf/src/runtime_server_options.rs:154 `pub replica_sync_timeout_secs: u64`
- wedb/wconf/src/node_options.rs:320 `DEFAULT_REPLICA_SYNC_TIMEOUT_SECS: i32 = 5`，:602/:605 CLI/TOML 绑定，:1206 缺省投影
- 既有读取先例（同形制参照）：wedb/wnode/src/aof/garnet_append_only_file.rs:85
  `read_timeout: Duration::from_secs(server_options.replica_sync_timeout_secs)`
- wconf 已有投影锁测：wedb/wconf/tests/replica_sync_timeout_sentinel_projection.rs

硬编码常量与 7 个使用点（dev 尖端实测，`git grep -n '\bREPLICA_SYNC_TIMEOUT\b' dev`）：
- wedb/wedb/src/server/replication/replica_wire.rs:117 `pub(crate) const REPLICA_SYNC_TIMEOUT: Duration = Duration::from_secs(5);`
- 同文件 :235 `time::timeout(REPLICA_SYNC_TIMEOUT, client.connect_async())`
- wedb/wedb/src/server/replication/replica_sync_session.rs:29（导入）、:181 `Some(REPLICA_SYNC_TIMEOUT)`、:841 `let timeout = Some(REPLICA_SYNC_TIMEOUT);`（:837 注释亦称「帧级 REPLICA_SYNC_TIMEOUT(5s)」）
- wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs:89、:155、:642、:714
- 测试注释回指：wedb/wedb/tests/replication_snapshot_reader_pin.rs:206

## 任务
把复制出站/快照/AOF 同步各超时点改为读 RuntimeServerOptions.replica_sync_timeout_secs
（秒 → `Duration::from_secs`），删除 `REPLICA_SYNC_TIMEOUT` 常量与其全部引用，
不留「常量作缺省回落」的折中形态：

1. 逐点打通上下游：调用方若已持 server options / store wrapper，直接取秒数下传；
   若为静态/自由函数，改为显式 `timeout: Duration` 形参（对标 C# 构造期把
   opts.ReplicaSyncTimeout 存入字段再逐点用的形制，如 ReplicationSyncManager.cs:38），
   禁止新增全局惰性读取或再抄一个常量。
2. 建连限时臂（replica_wire.rs:235）同样改用活值；注意与「timeout==0 语义」区分：
   C# ReplicaSyncTimeout 缺省 5s 且 CLI 有下界保护，若现树 wconf 侧已有 0 的口径
   （见 sentinel projection 测试与 node_options 校验），沿用该口径并在注释写明，
   不自造兜底值。
3. 注释同步：所有回指该常量的中文注释（replica_sync_session.rs:168/:837、
   replication_snapshot_reader_pin.rs:206 等）改为叙述「取 RuntimeServerOptions
   .replica_sync_timeout_secs 活值」，锚 C# 保持单点不新增重复定义簇。
4. 补一处回归锁测（放 wedb/wedb/tests/，若已有同族册则并入既册）：改旋钮值后，
   复制出站建连/停等限时随动（可用现成假端点/慢端点夹具断言超时窗变化；
   若某些点仅内部计时不可观测，只测可观测面，不造伪断言）。

## 边界与纪律
- 只在 worktree `/tmp/fork/repl-sync-timeout-knob` 内改动，私有 target `/tmp/_rs/repl-sync-timeout-knob`（已预热）。
- 禁触：`wedb/wconn/**`（他席在途合并冲突面）、`wedb/wedb/src/client.rs`、
  `wedb/wkv/**`、`wedb/wnode/src/resp/objects/rmw_helpers.rs`、`task/refactor-backlog.md`。
- 禁 `#[allow]`、禁占位实现、禁改 Cargo.toml（依赖只走 cargo add）、禁向下兼容垫片。
- 禁跑 `./test.sh` 与 `./sh/clippy.sh`（门禁归主控，并发席互锁）。
- 自检：`cargo check -q --workspace --all-targets` 零告警 + `cargo nextest run -p wedb <复制相关册>` 定向绿。
- 净行：默认减行；若为下传形参净增，控制在 +40 内并在提交信息写明口径。
- 完工：单提交，消息以 `fix(wedb): ` 起头，只 add 指派文件。

## 收口记录（2026-09-28 r6 波）
- 席提交：`99d14ab0`（父 `ff588257`，单提交，12 文件 +182/-61）；合入：`c92de925`（--no-ff，双引号消息）。
- 合入面复核（`git diff --name-only c92de925^1 c92de925`）＝票面 6 src + 6 测试册，零越界；
  `git grep REPLICA_SYNC_TIMEOUT` 于合入后尖端归零（常量面彻底删除，仅余 wconf 缺省单源）。
- 收口形态：ClusterProvider 增 `replica_sync_timeout_secs` 承接槽（boot 装配期一次注入
  RuntimeServerOptions，缺省引 wconf `DEFAULT_REPLICA_SYNC_TIMEOUT_SECS` 不抄值）+
  `replica_sync_timeout()` 单点折 Duration；`TcpSessionWire::connect` 加显式 `timeout:
  Duration` 形参；`recover_roundtrip`/快照逐帧改调用方下传；diskless 扇出
  `SnapshotIteratorManager` 构造期存 `sync_timeout` 字段（对标 `ReplicationSyncManager.cs:38`）；
  锁测 `replica_sync_timeout_knob_drives_connect_window` 静默端点双臂（1s 必 TimedOut /
  60s 于 10s 观察窗仍挂起），证「收场窗唯活旋钮是从」。
- 主控审计遗留（另立票，见下）：`<=0` 无限哨兵在 wconf 投影折 `u64::MAX` 秒，本票把它
  直折 `Duration::from_secs` 送进 compio 定时器臂 → `Instant::now() + d` 溢出 panic；
  仓内已有 `server::wait_async` 的 None=不挂计时器单机制可承（`server/mod.rs:25-37` 注释
  即写明该溢出）。
