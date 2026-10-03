# AOF 回放对齐栅栏把无限哨兵折进 coarsetime 时限加法，溢出即 panic（release 侧静默假超时）

定级：P2（`--repl-sync-timeout 0`／负值是文档化口径；dev/test 形态命中即 panic，
release 形态 overflow-checks 关断后.wraps 成过去时刻，栅栏每次判「未会合」，
回放一致性校验假失败——C# 同输入为永等直至放行）

> 前置依赖：在途票 task/ing/repl-sync-timeout-infinite-sentinel-timer-overflow.md
> （值源单点改 `Option<Duration>` 形制 + wconf 具名无限哨兵常量）。本票以同一形制收
> wnode/aof 支链，须待该票合入 dev 后再派席，禁并行改同一旋钮的值源面。

## 甄别结论：通过（2026-09-28 主控现码 + 依赖库源码逐点亲验）

1. C# 契约
   `garnet/libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:77`（`readonly TimeSpan
   replicaSyncTimeout`）、`:81/:91-93`（构造注入，注释明写「ReplicaSyncTimeout is
   Timeout.InfiniteTimeSpan when disabled」）、`:182`（注释：the wait is bounded by
   ReplicaSyncTimeout，Timeout.InfiniteTimeSpan blocks until released）、`:188`
   （`_ = ev.Wait(replicaSyncTimeout);`）。即无限口径为「不挂时限、永等放行」。
   旋钮折无限口径的源头同票：`garnet/libs/host/Configuration/Options.cs:995`
   `<=0 ? Timeout.InfiniteTimeSpan`。

2. rust 现状（现树实测，非票面转抄）
   - 值源折哨兵：`wedb/wconf/src/node_options.rs:1486-1489`
     `replica_sync_timeout_secs = if self.replica_sync_timeout_secs <= 0 { u64::MAX } else
     { … as u64 }`；`wedb/wconf/src/runtime_server_options.rs:154` 为该 u64 字段、
     `:212` 缺省取 `DEFAULT_REPLICA_SYNC_TIMEOUT_SECS as u64`。
   - 全仓扫描 `replica_sync_timeout_secs` 消费点，除在途票已覆盖的
     `wedb/wedb/src/server/cluster_provider/**` 与 `wedb/wedb/src/server/replication/**`
     之外，**唯一残余支链**即本票：`wedb/wnode/src/aof/garnet_append_only_file.rs:85`
     `read_timeout: Duration::from_secs(server_options.replica_sync_timeout_secs)`
     （字段声明 `:49`，getter `:267`）。
   - 该 `read_timeout` 的三个 sink 已逐点定形制，危害面收敛为单点：
     (a) `wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs:105`
     `try_signal_or_wait(timeout)` → `:116/:127`
     `parking_lot::Condvar::wait_for(&mut state, timeout)`。parking_lot 0.12.4
     `src/condvar.rs:386` 走 `util::to_deadline`，`src/util.rs:36-38` 为
     `Instant::now().checked_add(timeout)`，溢出归 `None` 即永等——**形制安全但语义偶然**；
     (b) `wedb/wnode/src/aof/readconsistency/read_consistency_manager.rs:92`
     `ReplayAlignBarrier::new(virtual_sublog_count, Some(read_timeout))` →
     `wedb/wnode/src/aof/readconsistency/replay_align_barrier.rs:98-99`
     `Some(to) => { let deadline = Instant::now() + to.into(); … }`——**本票炸点**；
     另 `:358/:428` → `virtual_sublog_replay_state.rs:95/:281`
     `listener.wait_timeout(timeout)`，event-listener 5.4.2 `src/lib.rs:866-869`
     用 `std::time::Instant::now().checked_add(timeout)`——**形制安全**。
   - 炸点机理（依赖库源码确证，非推断）：`to.into()` 走
     `coarsetime-0.1.38/src/duration.rs:265-269`（`From<std::time::Duration>`）→
     `Duration::new(secs=u64::MAX, 0)`（`:15-17`）→ `helpers.rs:18-20`
     `_timespec_to_u64` 的 `tp_sec.saturating_mul(1 << 32)` 饱和为 `u64::MAX`；
     再 `instant.rs:318-325` `impl Add<Duration> for Instant { Instant(self.0 + rhs.as_u64()) }`
     为**裸 u64 加法**：dev/test 形态（仓内 `[profile.dev]` 未关 overflow-checks，
     `wedb/Cargo.toml:259-261`）溢出即 panic `attempt to add with overflow`；
     release 形态（`wedb/Cargo.toml:248-256`）回绕成小值，`deadline` 落到过去时刻，
     `:113` `if Instant::now() >= deadline { return false; }` 每轮立即判未会合。
   - 正确形制的既有先例（本票据此收口，不新造机制）：
     (i) 三态映射与 `None` 永等：`wedb/wedb/src/server/migration/migrate_driver/keys.rs:57-58`
     `wait_dur(timeout_ms) -> Option<Duration>`（`-1 → None`），`:49-51` 注释写明
     C# `WaitAsync(_timeout)` 的三态语义，`:99` deadline 仅在 `Some` 时构造；
     (ii) 无限不挂计时器：`wedb/wedb/src/server/mod.rs:25-37` `wait_async`；
     (iii) 栅栏本体已具备 `None → 永等 loop` 臂：`replay_align_barrier.rs:87-97`。

3. 逻辑危害
   运维按文档设 `--repl-sync-timeout 0`（多物理子日志 + 多回放任务形态下
   `create_or_update_key_sequence_manager`（`garnet_append_only_file.rs:340-354`）建出
   `Some(Duration::from_secs(u64::MAX))` 的栅栏）→ 每次回放对齐等待即 panic 任务炸；
   release 形态则副本读一致性校验（`verify_key_freshness`）恒判超时降级。C# 同输入为
   永等直至放行，语义完全反向。属在途票同一形制缺陷的第二条支链，非重复立案
   （在途票明确把 wnode/aof 点列为「登记不改（跨域）」）。

## 涉及代码

rust 文件与函数：
- `wedb/wconf/src/runtime_server_options.rs:RuntimeServerOptions.replica_sync_timeout_secs`
  （值源单点折叠谓词落此，返 `Option<Duration>`，无限哨兵 → `None`）
- `wedb/wnode/src/aof/garnet_append_only_file.rs:GarnetAppendOnlyFile::{new,read_timeout}`
  （:49/:85/:267 字段与 getter 随形制改 `Option<Duration>`）
- `wedb/wnode/src/aof/garnet_append_only_file.rs:create_or_update_key_sequence_manager`
  （:349 下传臂）
- `wedb/wnode/src/aof/readconsistency/read_consistency_manager.rs`（:45/:64/:91-92/:98 形参
  与 `Some(read_timeout)` 包装撤除，直传 `Option`）
- `wedb/wnode/src/aof/readconsistency/replay_align_barrier.rs:ParticipantEvent::wait`
  （:98-99 炸点本体；`None` 走 :87-97 既有永等臂，`Some` 保持现时限臂）
- `wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs:try_signal_or_wait`
  （:105/:116/:127 形参随之 `Option<Duration>`，`None` → `Condvar::wait` 永等臂，
  禁靠 parking_lot 的 checked_add 偶然饱和）
- `wedb/wnode/src/aof/readconsistency/virtual_sublog_replay_state.rs`
  （:95/:281 `listener.wait_timeout` 随之 `Option`，`None` → `listener.wait()`）
- 测试册：`wedb/wconf/tests/replica_sync_timeout_sentinel_projection.rs`（同族扩形，勿新建）

对应 c# 文件与函数：
- `garnet/libs/server/AOF/ReadConsistency/ReplayAlignBarrier.cs:77/:81/:91-93/:182/:188`
- `garnet/libs/server/AOF/GarnetAppendOnlyFile.cs`（`readTimeout` 构造投影与
  `CreateOrUpdateKeySequenceManager` 下传臂）
- `garnet/libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs`（构造期 timeout 透传）
- `garnet/libs/server/AOF/AofReplayCoordinator.cs`（栅栏会合时限的 `Wait` 消费臂）
- `garnet/libs/host/Configuration/Options.cs:995`（`<=0 ? Timeout.InfiniteTimeSpan`）

## 精炼执行方案

1. 值源单点：在 `wconf/src/runtime_server_options.rs` 加一个折叠访问器（命名与在途票的
   `Option<Duration>` 形制、`wconf/src/node_options.rs` 的具名无限哨兵常量保持一致，
   禁第二处裸写 `u64::MAX`），`<=0`/哨兵 → `None`，正值 → `Some(Duration::from_secs(值))`。
2. 支链全量改 `Option<Duration>` 透传：`garnet_append_only_file` 字段/getter、
   `create_or_update_key_sequence_manager` 下传、`read_consistency_manager` 形参、
   `replay_align_barrier::ParticipantEvent::wait`、`aof_replay_coordinator::try_signal_or_wait`、
   `virtual_sublog_replay_state` 的两处 listener。三个 sink 各自的「无限」臂改为**显式**
   永等（栅栏 :87-97 既有 loop、`Condvar::wait`、`listener.wait()`），杜绝再依赖
   checked_add 的偶然饱和。禁在 `wait` 里加「时限超阈值即视为无限」的旁路判据。
3. 锁测（不加伪断言）：
   a. 值源面：`replica_sync_timeout_secs` 取 0／负／正常值 → 折叠访问器 `None`/`Some(值)`
      逐一对位，扩进既有 `wconf/tests/replica_sync_timeout_sentinel_projection.rs`；
   b. 栅栏面：`ParticipantEvent::wait(Some(Duration::from_secs(u64::MAX)))` 形态在修复前
      必以 add-overflow panic 呈红，修复后该形态不再由任何生产路径产生（值源已折 `None`），
      故锁测钉两臂：哨兵经值源得 `None` → 永等臂起释线程可会合成功；显式小时限 → 到点
      判 `false` 且耗时上界受控；
   c. 协调器/回放状态面：`None` 臂走 `Condvar::wait`/`listener.wait()` 的会合成功对拍，
      与 `Some` 臂的到点判败并存，防形制回退。
4. 收益口径：三条 sink 的「无限」由依赖库偶然行为收为仓内单套显式机制；
   消除一条 panic/假超时支链；净行以近零或减行为目标（形参换型为主）。

## 边界与纪律
- 只在 worktree `/tmp/fork/aof-replay-barrier-infinite-sentinel` 内改动，
  私有 target `/tmp/_rs/aof-replay-barrier-infinite-sentinel`。
- 前置：在途票 task/ing/repl-sync-timeout-infinite-sentinel-timer-overflow.md 合入 dev 后
  再 fork（哨兵常量与 `Option<Duration>` 形制须复用其落地成果）。
- 禁触：`wedb/wedb/src/server/replication/**`、`wedb/wedb/src/server/cluster_provider/**`、
  `wedb/wedb/src/server/mod.rs`（在途席域）、`wedb/wconn/**`、`wedb/wtxn/**`（他席已领票在途）、
  `wedb/wkv/src/read_cache/**`（他席在途）、`wedb/wreviv/**`、`wedb/wedb/src/client.rs`、
  `wedb/wconf/src/node_options.rs` 的 TLS 旗标解析面（在途 boot-tls 席域）、
  `task/refactor-backlog.md`。
- 禁 `#[allow]`／`#[expect]`、禁占位实现、禁改 Cargo.toml（依赖只 `cargo add`）、
  禁向下兼容垫片（不留「巨大时限作无限」的折中形态，也不保留旧 `Duration` 形参的包装函数）。
- 禁跑 `./test.sh` 与 `./sh/clippy.sh`（门禁归主控）。自检：
  `cargo check -q --workspace --all-targets` 零告警 ＋ `cargo nextest run -p wnode`
  aof/readconsistency 相关册定向绿 ＋ `cargo nextest run -p wconf` 该投影册绿。
- 注释用中文；对标 C# 1:1，不自创优化；测试对标 C# 既有形态，禁编造假 mock。
- 完工：单提交，消息以 `fix(wnode): ` 起头，只 add 指派文件；不 merge 不推 dev。

## 终态注记（2026-09-28 合入）
- 合入 commit: `1bd5619`
- 修复成果：
  1. `wconf/src/runtime_server_options.rs`：新增 `replica_sync_timeout(&self) -> Option<Duration>`，将 <=0 与哨兵投影为 None。
  2. `garnet_append_only_file.rs`、`read_consistency_manager.rs`、`replay_align_barrier.rs`、`aof_replay_coordinator.rs`、`virtual_sublog_replay_state.rs` 全链路改用 `Option<Duration>`，消除 `coarsetime` overflow panic 隐患与假超时。
  3. `replica_sync_timeout_sentinel_projection.rs` 及相关单元测试补齐锁测。
  4. 消除 AOF 并行回放死等 30s 兜底超时的根因，Rank 1 慢测试恢复全绿。

