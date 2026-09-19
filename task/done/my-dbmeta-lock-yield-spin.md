优先级：低

问题
换号元数据串行锁 lock_dbmeta 为 AtomicBool compare_exchange + yield_now().await 自旋。compio 线程钉核下，持锁方与等待方常在不同核：临界区含 DbMeta 原子批落盘（含 IO），等待核上的任务在 yield-poll 循环里持续空转烧 CPU 直到落盘完成；同核任务虽在让渡间隙可跑，但自旋任务持续占用调度槽。锁只在管理面（flush_database/flush_namespace/swap_databases），频率低，故列低。另：该锁不得改 parking_lot 同步锁（不可跨 await，误改即死锁），头注须写清这一例外依据，防后人按「锁用 parking_lot」规范误改。

取证（dev 当下代码重取）
wedb/wkv/src/store/mod.rs:198-207 lock_dbmeta（while compare_exchange 失败即 yield_now().await）；:193-197 头注自述「协作让渡自旋…绝不 park 线程」。持锁临界区见 wedb/wkv/src/session/swap.rs:47-91（锁内换格 + persist_dbmeta_batch）与 flush 换号事务同型。

C# 对标
garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase（C# lock/Monitor 阻塞挂起让出 CPU，无自旋面；rust 因 async 不可持同步锁跨 await 才自研，形态差异需异步等待原语补齐）。

修法建议
改跨 await 可等待原语：event-listener（workspace 已有依赖）构造的异步互斥或等待队列，无争用快路径保持一次 CAS；临界区落盘期间等待任务真挂起零 CPU。头注补「不用 parking_lot 的原因（跨 await）+ 替代原语」说明。来源 next/agy.my.md 条 13 与 next/muse.my.md 条 18（后者增量即此注释要求）合并处理。

判词：判成立（附 C# 侧主张订正一枚）
取证基线：主仓 dev d5c6efa（认领时 HEAD），落地时最新 dev dbef1c2。

rust 侧逐条核实为真：
- wedb/wkv/src/store/mod.rs:198-207（改前）确为 `while dbmeta_lock.compare_exchange(..).is_err() { yield_now().await }`
  自旋，:193-197 头注自述「协作让渡自旋…绝不 park 线程」；`wbase::future::YieldNow::poll` 实为首轮
  `cx.waker().wake_by_ref()` + `Poll::Pending`，即等待任务逐轮自唤自灌就绪队列、在钉核运行时持续占用该核调度槽，
  「烧 CPU」定性成立且可测。
- 持锁临界区跨 await 成立：session/swap.rs:47-91 锁内换格 + `persist_dbmeta_batch`（含设备 IO），
  故同步锁形态不可用，票面「误改即死锁」结论正确。
- 锁落点未因 vdb 拆分棒迁移：仍在 store/mod.rs（vdb-file-split 归档票的「撞面提示」预判指向 vdb/manager.rs，
  实测与本体无关）。

C# 侧订正（票面「C# lock/Monitor 阻塞挂起让出 CPU，无自旋面」不实，本棒按实测写注）：
- `garnet/libs/server/Databases/MultiDatabaseManager.cs:TrySwapDatabases` 的串行化靠
  `databasesContentLock`（`SingleWriterMultiReaderLock`），其获取口 `TryGetDatabasesContentWriteLock`
  是「`TryWriteLock` 失败即 `Thread.Yield()` 重试」循环——.NET 线程交回时间片故非热自旋，但确非队列挂起，
  即 C# 侧本身就存在票面所称「无」的自旋面；
- `DatabaseManagerBase.cs:FlushDatabase`(:301) 本体是同步无锁段（日志截断 + AOF 截断），等待全压在调用方
  的同步 yield 重试上。
此订正不反转判词：rust 一核多任务，「等而不占」只能任务态挂起，事件队列在语义上补齐 C# 效果且强于 C# 的
yield 重试（临界区整段零唤醒零 CPU），故修法仍成立，头注已把这两形态写实。

落地
- wedb/wkv/src/store/mod.rs：新类型 `DbmetaLock { busy: AtomicBool, gate: event_listener::Event }`（:210）
  + `acquire()`（:221，快路径一次 Acquire CAS；争用「先 `gate.listen()` 注册、再复核 CAS」后 `listener.await`
  真挂起——注册先于复核是丢失唤醒的唯一防线）+ `DbmetaGuard` Drop（:246，Release 清位 + `notify(1)` 精准移交队首）。
  字段 `dbmeta_lock` 类型由 `AtomicBool` 改 `DbmetaLock`（:163、:389 初始化）；`lock_dbmeta` 收为 `#[inline]`
  转发口（:275）。旧自旋 while 循环整段删除，无兼容口、无第二套机制；顶部 `wbase::future::yield_now` import 随之移除。
- 快路径代价：无争用时与改造前同形（同一次 `compare_exchange(Acquire/Relaxed)`），不多付任何原子操作或堆分配。
- 头注例外依据（:166-208 `DbmetaLock` 类型文档）：先给「规范适用前提是临界区不跨 await」的前提，再列
  1. Thread-per-Core 死锁（`wnode/src/server.rs` 每 worker 线程一个 `Runtime::new()`、任务永不跨核，
     后到者 `lock()` park 整线程 → 持锁者落盘续算无人驱动）——决定性依据；
  2. `MutexGuard: !Send` —— 诚实限定：compio `Runtime::spawn` 不要求 `Send`，故此条非硬编译墙，
     只封死未来 Send 边界（旧头注「守卫 !Send 无法编译」的过头说法一并订正）；
  3. C# 两形态（TrySwapDatabases 的 yield 重试 / FlushDatabase 的同步无锁体）逐一对照，说明为何均不可直译。
- wedb/wkv/Cargo.toml：`event-listener.workspace = true` 一枚（复用根 workspace 既有 5.4.2，与
  `wcpr/src/manager/mod.rs:246-273 lock_ckpt_gate` 同原语同形态先例）；未引 async-lock/tokio-util/
  parking_lot::deadlock 版，仓库内不新增第二套等待机制。
- 持锁调用面零改动：四处持有者（flush_database / flush_namespace / swap_databases / apply_dbmeta_record）
  仍走 `store.lock_dbmeta()`，`wkv/src/session/swap.rs` 未被本棒编辑（转发口保留了原 API 形状，无需动）；
  `lock_dbmeta` 文档现列全四持锁者（旧文只列三）。
- 撞面核查：并棒 fix-rmw-atomic-window 面为 `wkv/src/session/**` + `wkv/src/lib.rs`，与本棒两文件零交集；
  `git log dev -- wedb/wkv/src/session/swap.rs` 仍只 `31c2388 init`（dev 与死树双侧核验），未触「停手回报」条件。
  两轮回合 dev（ca1b119、dbef1c2）wkv 侧零冲突，无需双保留。

争用不烧 CPU 的行为自证（wedb/wkv/src/store/mod.rs:698 起 `#[cfg(test)] mod tests`，对位先例
store/hlog_scan.rs:289）
- `uncontended_acquire_needs_one_poll_and_no_wake`：手工 poll 一次即 Ready，等待队列登记数 0、wake 计数 0。
- `contended_waiter_sleeps_until_handoff`：等待者首轮 poll 走完「CAS 失败→注册→复核失败→挂起」，断言
  `gate.total_listeners() == 1` 且 wake 计数 0（持锁期内重复观测仍为 0），Drop 后恰醒一位，二次释放再恰醒一位。
- `contended_tasks_serialize_across_await`：真 compio `Runtime::new()` 下 8 任务 × 4 轮，临界区内
  `sleep(1ms)` + `yield_now()` 跨 await，断言任一时刻至多一位持有者（overlap 0）、32 轮全推进、末轮无残留认领位。
- 负控实测（证三枚断言非空转）：把 `acquire` 临时改回 `compare_exchange + yield_now` 自旋，
  「等待者应挂在事件队列上」（实测队列登记 0）与「挂起不得自唤醒」（实测每轮让渡自唤一次）两枚断言转红；
  测后源文件已还原并核验（工作树当时无残留 diff）。

门禁实测（最终合并态 = dev dbef1c2 + 本棒，树 /tmp/fork/dbmeta-lock，`CARGO_TARGET_DIR=/tmp/ct-dmls` 私有；
主仓 ./test.sh 与 ./sh/clippy.sh 未跑）
- `cargo check --workspace --all-targets`：exit 0，零 error（Finished 7.59s 增量）。
- `cargo nextest run -p wkv --no-fail-fast`：225 tests run: 225 passed, 0 skipped（含
  `store::swap_database::*` 换号/复建系列与 `store::tests::*` 新增三枚）。
- `cargo fmt -p wkv -- --check`：exit 0。
- `cargo clippy -q -p wkv --all-targets --all-features -- -D warnings`：exit 0。
- `bun js/check.js` 前后对跑：基线树 @dbef1c2（/tmp/gate-dmls-base，detached）与本树输出逐字节相同
  （各 4715 字节，diff 仅本棒追加的退出码标记行），锚点不增不减；两树 `git status` 均零 `js/check/ignore/**` 回写。
  本棒新增的 `MultiDatabaseManager.cs:TrySwapDatabases`、`DatabaseManagerBase.cs:FlushDatabase` 系裸文件名形态，
  按 `CS_REF_REGEX` 不登记（与既有取证口径一致），故此结果符合预期而非漏检。
- 他树在途红归因（未由本棒修）：合并前基线树 @d5c6efa 复现 wnode aof 回放 4 枚红
  （`aof_flush_replay` 的 test_flush_entry_payload_u64_domain / test_flush_ns_replica_replay /
  test_flush_db_replica_replays_entry_domains_without_local_remap（panic 于 aof_flush_replay.rs:279
  「FlushDb 条目须把载荷旧域 (3, 7) 投进本地 GC 死亡账本」）+ `aof_replay_domain` 的
  full_replay_nonzero_domain_lands_in_entry_domain），与 task/ing/r4-red-attribution-fix.md 条 1-4 逐字对位，
  起源提交 6dc1cb6，非本棒；两套件在基线树 exit=100 一致复现，本棒载荷不含 wnode/aof 面。
  合并态下该二套件曾二次复跑，因并发棒同机编译争用未取到终局读数，故红态以基线树复测 + 既立归因为准（如实记）。

落位
- 载荷 077a41e（wkv/Cargo.toml + wkv/src/store/mod.rs，+270/-30）→ 回合 3a1f2b3（并入 dev ca1b119）
  → 回合 76cf250（并入 dev dbef1c2）→ 主仓 dev 纯 FF `dbef1c2..76cf250`，FF 面两文件、与他人在途件
  （wkv/src/store/keyspace.rs 脏工作区）零交集，未动不 restore。
- 本票随此提交 `task/ing` → `task/done`。
