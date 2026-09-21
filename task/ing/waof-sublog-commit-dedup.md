# WaofSublog 与 WalLog 提交与位点双层冗余去除

## 判定：票面成立

逐项对照 C# 后的证据：

1. C# 的提交链只有一层驱动。`libs/server/AOF/GarnetLog.cs:477 Commit` 直接调
   `singleLog.log.Commit(spinWait, cookie)` / `shardedLog.sublog[i].Commit(...)`，
   `TsavoriteLog.cs:3302 CommitInternal` 在调用线程内同步登记 ongoingCommitRequests
   并 `allocator.ShiftReadOnlyToTail` 触发刷盘，合并发生在日志自身的提交队列里；
   C# 全仓不存在「每个物理子日志一个常驻提交任务 + 折叠通道」的形态
   （`libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs` 是
   副本推流端，与提交流水无关；`StoreWrapper.cs:TryStartCommitTask` 的周期提交在
   rust 已对位为 `PrimaryTasks` 的周期任务，直调 `GarnetLog::commit_async`）。
   故 `wedb/wnode/src/aof/waof_sublog.rs` 的 `commit_wake` + `commit_request` +
   `committer_loop` 是 rust 自造的第二套合并机制：`fetch_max` 折叠 + 容量 1 通道
   与 `GroupCommitPipeline` 的 Leader/Follower 合批完全同职能，且刷盘被钉死在单个
   常驻任务上，底层流水线永远只有一个调用者。
2. 位点与 cookie 双重维护成立。C# 把 `CommittedBeginAddress`、`RecoveredCookie`
   定义为 `TsavoriteLog.cs:115-125` 日志自身字段，写入点全在日志内部
   （`:244` 构造、`:528/:596` Initialize、`:2734` UpdateCommittedState、
   `:3095` CompleteRestoreFromCommit），门面 `GarnetLog.cs:139 CommittedBeginAddress`
   与 `:191 RecoverLatestSequenceNumber` 只是读取转发。rust 却在 `WaofSublog` 另存
   `cookie`/`committed_begin` 两个原子量，靠 `commit`/`commit_flush_async`/
   `recover_async`/`reset_async`/`safe_initialize` 五处手工同步，`WalLogInner` 内
   另有 `pending_cookie`/`recovered_cookie`/`recovered_committed_begin`。

## 方案

### waof：WalLogInner 成为位点与 cookie 单一真源

- `wal/log.rs`：`WalLogInner` 新增 `committed_begin_address: AtomicU64`
  （对位 `TsavoriteLog.cs:120 CommittedBeginAddress`），构造与 `reset` 归
  `log_begin_address`、`safe_initialize` 取入参 begin（对位 `:528/:596`）；
  新增 `WalLog::committed_begin_address()` 读取口，与既有
  `committed_until_address()` 同族。
- `wal/log.rs`：删除 `recovered_committed_begin` 字段与其读取口——恢复收敛的
  begin 快照就是当前已提交 begin，不再有「观测值」与「活值」两份。
- `wal/recover.rs`：`recover` 收敛时改写 `committed_begin_address`
  （对位 `:3095 CompleteRestoreFromCommit`），`recovered_cookie` 保持不动。
- `wal/flush.rs`：`WalCommitStep::step` 写出 commit 帧时同步把帧内 begin 落到
  `committed_begin_address`（对位 `:2734 UpdateCommittedState`），落盘位点与
  帧内容同源，外层无须再快照。

### wnode：WaofSublog 退化为纯类型适配层

- 删除 `cookie`、`committed_begin`、`commit_wake`、`commit_request`、
  `flush_event` 五个字段与 `ensure_committer`、`committer_loop`。
- `commit(&self, cookie: i64)`：签名去掉 `until_address`（C# `Commit` 亦无该参数，
  提交恒以自身尾部为目标）。写 `wal.set_pending_cookie(cookie)` 后直接在当前
  compio 运行时 `spawn` 一个 `wal.commit()` 任务并 `detach`，即 C#
  `Commit(spinWait: false)` 的「发起即返回」；并发提交的合批唯一由
  `GroupCommitPipeline` 协商承担。无运行时上下文时显式 panic 提示按
  `Runtime::new` 包裹（与 `wbase::future::blocking_wait` 既有口径一致），
  杜绝再造第二条驱动通路。
- `commit_flush_async(&self, cookie)`：改为 `set_pending_cookie` + `wal.commit().await`
  （对位 `TsavoriteLog.CommitAsync`），删去自有原子量落账与事件通知。
- `recover_async`：删去把 `recovered_*` 抄回私有字段的同步段，仅转发 `wal.recover()`。
- `reset_async`：只转发 `wal.reset()`（位点族复位已在 waof 内完成）。
- `safe_initialize(&self, begin, committed_until)`：删去 `last_commit_num` 形参——
  它此前唯一的消费者就是被删除的私有 `cookie`；序列号真源在
  `GarnetLog` 的 `SequenceNumberGenerator`（`set_starting_offset` 播种），日志层
  不再持有第二份。同步收敛 `ShardedLog::safe_initialize/initialize`、
  `GarnetLog::safe_initialize/initialize/initialize_if` 的形参。
- `committed_begin_address()`：改为读 `wal.committed_begin_address()` 的 i64 薄形变。
- `recovered_cookie()`：删除该同名遮蔽方法，读取经 `Deref` 直达
  `WalLog::recovered_cookie()`（返回 `i64` + `NO_COOKIE` 哨兵）。
- `flush_event()`：删除；无运行时事件的同步等待见下。

### wnode 门面与等待面

- `garnet_log/commit.rs:commit()`：单子日志 `commit(NO_COOKIE)`、分片以同一
  cookie 调 `commit(cookie)`，不再预算 tail。
- `garnet_log/commit.rs:wait_for_commit()`：C# `TsavoriteLog.cs:1845 WaitForCommit`
  即 `Thread.Yield` 纯自旋（注释自证「不发起提交，只等待，须由他人推进」），
  rust 保留 `Backoff::snooze` 阶梯退避自旋，删除 `flush_event` 挂起分支——
  没有常驻提交任务后该事件无人再发，留着就是空转的第二套等待机制。
  需要让位的调用方一律走 `wait_for_commit_async`（其内部 `commit_to` 本身即刷盘驱动）。
- `garnet_log/addresses.rs:recover_latest_sequence_number`：改为读
  `sublog.recovered_cookie()`，`NO_COOKIE` 即该子日志无 cookie、收敛失败
  （与 C# `cookie == null` 判据同构）。

### 测试

- `waof_sublog.rs` 的常驻回退提交测试删除，改留一条「`Runtime::new` 包裹下
  同步 `commit` 由流水线推进刷盘」的定向测试。
- `wnode/tests/garnet_log.rs`：`commit_and_bitmask` 改运行时内异步等待；
  `recover_until_converges_from_commit_cookies` 按 C# 语义改为「全子日志广播写入
  → commit → 设备面恢复 → 自 commit 帧收敛 cookie」。
- `wnode/tests/aof_domain.rs`：`commit` 去掉 tail 形参，断言改为提交完成后再取
  `committed_begin_address`。
- `garnet_append_only_file.rs` 内测试的 `commit(tail, 0)` 同步改形。

## 边界

- 不做多子日志扇出（next/sublog-fanout.md）、不做尾页截断钩子与脉冲
  （next/aof-tail-shift-truncate-and-pulse.md）。
- 不改 wait 路径的错误传播签名（task/ing/commit-wait-error-propagation.md 在途），
  本票只删 `wait_for_commit` 的事件分支与 `commit` 的形参。
- 不做旧数据迁移与兼容壳。

## 验收

1. `cargo check -p waof -p wnode -p wedb` 零 error 零 warning。
2. 定向测试：`cargo test -p wnode --test garnet_log --test aof_domain`、
   `cargo test -p waof`。
