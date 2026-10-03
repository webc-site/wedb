锁定注记（2026-10-01 r10 波主控，基线 `7362b0b`；只读甄别席四段链穷举 + 主控逐环现码亲验（C# 侧锚与 rust 侧降级臂均现场读码复核，非转述），锚以本注记为准，台账禁钉行号）
- 病灶：**事务重放段内**命令自同步快臂降级进慢臂后，慢臂执行体在 `exec` 分派段的 RAII 锁器守卫**已退出**的状态下运行，
  会话锁器位回落到 `SessionLocking::Basic`，于是慢臂对同一批键重新走 `rmw_window*` 的**取闩臂**；
  而这些键桶的排他闩此刻正由本会话事务（wtxn `TxnKeyEntries`）在 windex 同一份锁内存上持有，
  桶闩非重入且唯一放闩者是自己（要到 EXEC 提交/复位才放）→ 让核预算环烧尽 → `WindexError::LockTimeout`
  → 客户端在 EXEC 数组里收到错误元素。
- 锁器模式位与让闩投影（现码亲验）：`wedb/wkv/src/session/rmw_window.rs::SessionLockingState`（会话级 `AtomicBool`，
  非线程局部）、`::StoreSession::push_session_locking`（返回 `SessionLockingGuard`，`Drop` 即 `set_session_locking(prev)` 还原）、
  `::BatchStoreSession::try_rmw_window` / `::try_rmw_window_sorted` / `::rmw_window_sorted` 三处入口皆以
  `session.session_locking().is_transactional()` 短路回 `held: None` / 空窗组（不取闩、复用事务已持闩）。
- 选型点与守卫退出（现码亲验）：`wedb/wnode/src/resp/garnet_api/mod.rs::GarnetApiFace::exec`
  以 `session.txn_state == TxnState::Running && session.txn_locks_cover_cmd(cmd, args)` 选型 push，
  守卫形如 `let _locking = ...`——**本分派段退出即还原 Basic**；随后 `raw::dispatch(..) == Ok(false)`
  登记 `session.pending_slow = Some(SlowWait::for_command(api, cmd, args, resp_version))`，
  `::SlowWait::for_command` 的快照字段只有 cmd / args / `resp_version`（**不含锁器模式**），
  `wedb/wnode/src/resp/garnet_api/slow.rs::exec_slow_impl` 起手 `self.session.enter_batch()`，全程无任何 `push_session_locking`。
- 降级臂未持闩即回落（现码亲验，非臆断）：`wedb/wnode/src/resp/key_admin_commands/keys.rs::rename_sync`
  多臂在任何 RMW 窗口之前即 `return Ok(false)`——登记表命中臂 `registry_alive(vector, prefix, old_key)`、
  旧键 TTL `Ok(StoreResult::RecordOnDisk)`、旧键 ETag `Ok(None)`、Meta 域命中 `Ok(TagRead::Hit(Some(_))) | Deferred`；
  即「纯降级、本命令尚未取过任何闩」，慢臂 `wedb/wnode/src/resp/key_admin_commands/slow.rs::rename_slow`
  随后对 `[old_key, new_key]` 取异步窗（`rmw_window_sorted`）——而 RENAME 的这两个键在排队期即已进事务键集并被
  `run_exec` 持排他闩（`wedb/wtxn/src/transaction_manager.rs::run_exec` 起点展开，提交/复位才 `release_held`）。
- 事务在 await 期不收口（现码亲验）：`wedb/wnode/src/resp/txn_resp_commands.rs` 的 EXEC 臂——
  `run_exec` 回 `ExecRun::Started` 即置 `TxnState::Running` 并只写数组头，**排队命令由消费循环逐条直通重放**；
  `wedb/wnode/src/net/handler/drive.rs` 的 `take_slow_wait` 臂在消费循环停机处 await `slow.resolve()`
  （`wedb/wnode/src/resp/slow_path.rs` 模块头自陈「登记 SlowWait 并停止消费本批 → 网络泵 take_slow_wait 后 await」），
  await 期事务态保持 Running、持闩保持持有；故慢臂取闩必与自己已持的桶闩相撞。
  同址第二入口：`wedb/wnode/src/resp/resp_server_session/lua.rs` 的 `take_slow_wait` 臂（脚本重入共享会话，
  判定单点 `txn_locks_cover_cmd` 在域内即让闩，同形）。
- 注意与既有机制区分：`wedb/wnode/src/resp/resp_server_session/txn.rs::park_exec_lock_wait` 承接的是
  `run_exec` **起点取锁争用**（`ExecRun::Contended`，空应答让步体 + `pending_rearm` 回退游标重驱本 EXEC），
  全程未取命令级闩、重驱复入 `exec` 选型点即重新 push——与本票「选型点已判 Transactional、
  命令体降级后在段外以 Basic 重取闩」是同一判据的**另一半**（模式须随挂起体过段界），非重复立案。
- C# 权威锚（现树逐字对过）：`libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs::TransactionalSessionLocker
  .TryLockEphemeralExclusive`——先 `Debug.Assert(store.LockTable.IsLockedExclusive(ref stackCtx.hei),
  "Attempting to use a non-XLocked key in a Transactional context (requesting XLock)")` 再**无条件 `return true`**
  （事务视图下绝不取闩、绝不失败，并以断言钉住「键必已由事务 XLocked」）；
  `::BasicSessionLocker.TryLockEphemeralExclusive` 才是单次 `LockTable.TryLockExclusive` 真取闩。
  派发面锚：`libs/server/Resp/RespServerSession.cs::ProcessMessages` 依 `txnManager.state == TxnState.Running`
  在 basicApi 与 transactionalApi 间派发——**C# 慢路径重投仍在 transactionalApi 视图内**，
  `libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs::RENAME`、
  `libs/server/Storage/Session/MainStore/MainStoreOps.cs::MSET_Conditional` 的
  `if (txnManager.state != TxnState.Running)` 门在事务内完全跳过取闩。
  故 rust 注释「降级慢路径在段外以 Basic 重取闩，与 C# 慢路径重投同址同判据」**失实**（第 5 类对标偏差：
  注释自陈与 C# 行为相反），须随本票一并订正。
- 可达性（RESP 级确定性复现，无需竞态窗）：`VADD vk 1:2` → `MULTI` → `RENAME vk vk2` → `EXEC`：
  ① `run_exec` 成功置 Running，`TxnKeyEntries` 已按桶升序持 vk/vk2 桶排他闩；② 重放遍 `exec` 内
  `txn_locks_cover_cmd` 通过、段内置 Transactional；③ `rename_sync` 在 `registry_alive(vk)` 臂 `return Ok(false)`
  （纯降级，未取闩）并登记 `pending_slow`；④ `exec` 返回，守卫 Drop 还原 Basic；⑤ 泵 `slow.resolve()`
  → `exec_slow_impl` → `rename_slow` → `rmw_window_sorted([vk, vk2])` 自撞；
  `wedb/wkv/src/session/rmw_window.rs::rmw_window_sorted` 的让核预算环内 `consume_release_notify` 永不置位
  （唯一放闩者是自己且要到提交）→ `yield_now` 支烧尽 → `Error::Index(WindexError::LockTimeout)`
  → `stor!` 折 `Err(())` → 客户端收到错误元素，同 worker 白耗整轮让步预算。
  **非向量亦同形可达**：事务内冷记录（`RecordOnDisk`）/ 翻页 `Err(page_id)` 降级的 SET / HSET / ZADD / GETDEL /
  RESTORE / MSETNX 等写族，以及 INFO 含扫描段整请求降级、脚本重入挂起臂——凡「同步臂纯降级 + 慢臂自取窗」皆命中。
  非事务遍不受影响（无自持闩，Basic 自取闩正确）。
- 危害定级依据：确定性（无竞态前提）× 客户端可见错误帧（事务语义命令整条失败）× 同 worker 无谓烧尽让步预算
  × 覆盖面为「事务内任何降级命令」的整族，非单命令；且现码注释把违背自陈为设计，后续改动会继续沿该错误口径扩散。定 P1。
- 前案边界（查重已核，四池 grep `rmw_window` / `SlowWait` / `exec_slow` / `txn_locks_cover_cmd` /
  `push_session_locking` / `LockTimeout` / `慢臂`）：
  `task/done/wtxn-exec-replay-window-generation-swap-lock-mode-degrade-atomicity-break.md` 裁的是
  **换代致选型点判不过 → 重放段命令被选成 Basic**（收口形态：`lock_prefix` 锚 + `rearm_txn_locks_for_generation_swap`
  增量补闩，严禁降级 Basic）——它修的是「选型判定输入陈旧」，本票修的是「选型判定为 Transactional 之后，
  模式作用域止于同步段、未随降级挂起体延伸」，两支不同臂；本票**不复判**换代补闩面。
  `task/done/wtxn-incremental-relock-ignores-lock-strength-shared-held-bucket-swallows-exclusive-entry.md`
  裁增量补锁强度入参，`task/done/wnode-collect-arm-rmw-writer-latch-starvation-locktimeout.md` 裁 collect 臂令
  RMW 写者饿死（他连接争用形态，非自撞），`task/done/wnode-get-slow-arm-object-key-nil-wrongtype-fork.md`
  裁慢臂类型判定分叉，均与本票「模式跨段界丢失」异面。
- 禁触域（同侪在途 + 本波自席）：`wedb/wtxn/src/txn_key_entry.rs`、`wedb/wnode/src/resp/objects/hash_commands/read.rs`、
  `wedb/wnode/src/resp/objects/sorted_set_commands/write.rs`、`wedb/wkv/src/vdb/manager.rs`（他席在途，只读其 API）、
  `wedb/wedb/src/server/replication/**`、`wedb/wnode/src/aof/**`（本波 aof 分块身份键席在途）、
  `wedb/wkv/src/store/keyspace.rs`（本波 wkv fail-closed 席在途）；
  本票只动 `wedb/wnode/src/resp/slow_path.rs`、`wedb/wnode/src/resp/garnet_api/mod.rs`（exec 选型点 + 失实注释）、
  `wedb/wnode/src/resp/garnet_api/slow.rs`（慢臂体锁器下传）、`wedb/wnode/src/net/handler/drive.rs` 与
  `wedb/wnode/src/resp/resp_server_session/lua.rs` 的挂起臂**仅在必须透传快照时**最小接线，加 `wedb/wnode/tests/**` 新册。

审核结论：通过（2026-10-01 主控逐环现码亲验立案；P1。确定性复现、无竞态前提、客户端可见错误帧、覆盖事务内降级命令
整族；C# 事务锁器恒真 + 断言「键必已由事务 XLocked」与 rust「段外 Basic 重取闩」两侧差异直接可观察。定 P1）

事务重放段降级慢臂在段外以 Basic 重取本键桶排他闩，与自家事务持闩自撞：该命令在 EXEC 数组回存储错误帧（C# 事务锁器恒真免疫）

问题分析：
1. Garnet 契约对齐：C# 事务的锁身份贯穿「取锁 → 重放 → 提交」全程，事务视图下的临时锁器
   `TransactionalSessionLocker.TryLockEphemeralExclusive` **恒回 true**（并以 `Debug.Assert` 钉住键已由事务 XLocked），
   即命令体在事务内从不二次取闩；`RespServerSession.ProcessMessages` 依 `txnManager.state == TxnState.Running`
   选择 transactionalApi 派发，慢路径重投仍在同一 transactional 视图内。rust 把「事务是否让闩」的判据投影成
   会话级模式位 + RAII 守卫，但守卫作用域只罩住**同步分派段**；命令一旦以 `Ok(false)` 纯降级交慢臂，
   执行体改由网络泵在段外 await，模式已还原 Basic——于是 rust 在同一份锁内存上向自己已持的排他闩再次入册，
   非重入桶闩必失，且唯一放闩者要等 EXEC 收口。这不是性能取舍而是**事务锁身份跨段界丢失**，
   直接违背 C# 「事务内恒不取闩」契约，也违背 `exec` 选型点自己声称的「与 C# 慢路径重投同址同判据」。
2. 工程现状：`SlowWait::for_command` 的快照字段集合（cmd / args / resp_version）**不含锁器模式**，
   `exec_slow_impl` 起手即 `enter_batch()` 且无任何模式写入——架构上就没有把调度点的模式判定送达执行体的通道。
   降级臂侧同样缺判据：同步臂多在真取窗**之前**即 `return Ok(false)`（RENAME 的登记表命中 / 冷 TTL / ETag 磁盘候选 /
   Meta 域命中四臂皆然），故「同步段已持闩」这一可能的兜底假设也不成立。两支合起来构成：
   模式跨段界丢失 → 慢臂向自家持闩入册 → 必失 → 预算环烧尽成 LockTimeout → 事务元素应答为错误帧。
3. 逻辑危害确证（最小复现，测试面**不需真实竞态**）：以 `MULTI; RENAME vk vk2; EXEC`（vk 为向量集键）
   或事务内任一「冷记录降级写」构造；断言 EXEC 该元素非错误帧、旧键消失新键在场、且慢臂取窗计数不因本命令自撞推进。
   现状必红：慢臂 `rmw_window_sorted` 自撞 → `LockTimeout` → 错误元素。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/garnet_api/mod.rs::GarnetApiFace::exec（选型点 push 与 RAII 退出即还原；降级登记 SlowWait；失实注释本体）
wedb/wnode/src/resp/slow_path.rs::SlowWait、::SlowWait::for_command（快照字段集，缺锁器模式通道）
wedb/wnode/src/resp/garnet_api/slow.rs::exec_slow_impl（起手 enter_batch，全程无模式下传）
wedb/wnode/src/net/handler/drive.rs（take_slow_wait 竞速臂——await 期事务不收口的现场）
wedb/wnode/src/resp/resp_server_session/txn.rs::park_exec_lock_wait（既有 Contended 让步重驱臂，本票区分对象）
wedb/wnode/src/resp/key_admin_commands/keys.rs::rename_sync（纯降级四臂）、::rename_slow（自取窗消费者）
wedb/wkv/src/session/rmw_window.rs::SessionLockingState、::StoreSession::push_session_locking、::BatchStoreSession::try_rmw_window、
  ::try_rmw_window_sorted、::rmw_window_sorted（让闩投影与失闩预算环；本票只读复用，不改其语义）
对应 c# 文件与函数：
libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs::TransactionalSessionLocker.TryLockEphemeralExclusive（恒 true + XLocked 断言）
libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs::BasicSessionLocker.TryLockEphemeralExclusive（真取闩，非事务态专用）
libs/server/Resp/RespServerSession.cs::ProcessMessages（Running → transactionalApi 派发）
libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs::RENAME、libs/server/Storage/Session/MainStore/MainStoreOps.cs::MSET_Conditional
  （`if (txnManager.state != TxnState.Running)` 事务内不取闩门）

精炼执行方案：
1. **模式随挂起体过段界（主案）**：在 `exec` 选型点把**已判定的** `SessionLocking` 值随降级快照下传——
   `SlowWait` 增一枚模式字段，`::for_command` 增形参接收调度点快照（与 cmd / args / `resp_version` 同渠道同口径，
   禁在慢臂内重算 `txn_locks_cover_cmd`：重算输入已随 await 窗漂移，判定必在决策时刻冻结）；
   `exec_slow_impl` 在 `enter_batch()` 外层以 `push_session_locking(snap)` 的 RAII 守卫罩住整个慢体 await 段，
   守卫退出即还原。`SessionLocking` 须可按值拷贝/比较（现系 `AtomicBool` 之上的枚举，若缺 `Copy`/`PartialEq`
   按本域既有形态最小补齐，禁新造全局态）。
2. **判定作用域严格沿用选型点结果**：慢臂模式下传只发生在「`exec` 选型点判为 Transactional」的命令上；
   非事务遍、脚本域外键臂（§139 在册：在域外即 Basic 自取闩）一律保持现形态。
   **禁**慢臂无条件 `Transactional`（无覆盖判定即让闩 = 无闩盲写丢更新，比现害更重）。
3. **落笔键集普查（必做并申报）**：逐臂申报 `exec_slow_impl` 承接的降级命令，其执行体落笔键集是否 ⊆
   调度点 `txn_locks_cover_cmd` 已判定覆盖的命令声明键集；凡体内有命令声明键之外的落笔（旁域清退、
   TTL / ETag 随键、broker 回写源键等），逐条给出该键是否走桶闩的现码结论：
   经 `put_i64_sidecar` / `delete_raw` 类旁路记录键（不取桶闩）者申报名为无害；
   真有额外取窗落笔者**该臂不得下传 Transactional**，改为保持 Basic 并在回报中说明（宁窄勿宽），禁扩事务键集（触 he席在途 `txn_key_entry.rs`）。
4. **注释与登记册订正**：`exec` 选型点「降级慢路径在段外以 Basic 重取闩，与 C# 慢路径重投同址同判据」改为本契约口径；
   `doc/zh/deviations.md` 在 §139 同族补登（现 §139 只在册「脚本域外键落 Basic」，未在册本件的「模式跨段界随挂起体延伸」），
   按既有体例写清 C# 对位与 rust 投影理由。
5. 禁做项：禁在慢臂前后释放事务持闩（违 EXEC 全程持闩契约）；禁给 `windex::HashBucket` / `wtxn` 锁登记加持有者身份做重入
   （改锁语义，且 `wtxn/src/txn_key_entry.rs` 系他席在途禁触）；禁在预算环内套第二层自旋或 `block_on`
   （compio thread-per-core 硬禁）；禁改 `rmw_window*` 的让闩投影判据与预算环续期判据（collect 饿死票已收口）；
   禁复用 `pending_rearm` 通道承载本件（那条是 EXEC 起点取锁争用重驱，语义不同）；
   禁 `#[allow]`/`#[expect]`；禁占位实现；禁把「模式过段界」与「注释/登记册订正」拆票。
6. 锁测：`wedb/wnode/tests/**` 新增一册（建议 `rmw_txn_slow_degrade_selflatch.rs`），用既有事务/向量会话夹具
   （参考 `wedb/wnode/tests/vector_vadd_watch_purge.rs` 的 `attach_transaction_components` 同族装配）跑：
   (a) `VADD vk 1:2` → `MULTI` → `RENAME vk vk2` → `EXEC`：断言该元素非错误帧、vk 消失 vk2 在场；
   (b) 事务内非向量冷记录降级写（`RecordOnDisk` / 翻页失败臂可达形态，按现码夹具择一确定性构造）同断言；
   (c) 自撞直读：复用既有取窗计数口（如 `RMW_PLAN_ACQUIRE_MISS`）**禁新造接口**，断言 EXEC 段内本命令降级慢臂
       不产生该计数增量；
   (d) 反向夹：事务外（非 MULTI）同一命令慢臂仍按 Basic 取闩——他连接同键并发形态不回归；
   (e) `transaction_tests` / `txn_queue_lockset_residual` / `watch_version_regression` / `rmw_key_concurrency` 全绿。
   revert-proof：撤第 1 步（快照不下传）后 (a)(c) 必红（(a) 出错误元素、(c) 见自撞计数）；
   若退化为「慢臂无条件 Transactional」则 (d) 必红——两向夹住，杜绝半收口与过收口。
7. 验证面：`cargo check -q -p wnode -p wkv --all-targets` 与
   `cargo nextest run -p wnode --test <新册>` + `--test transaction_tests` + `--test rmw_key_concurrency` +
   `cargo nextest run -p wtxn`；禁在主树或沙箱跑 `./test.sh`/`./sh/clippy.sh`（波次门禁由主控统一跑）。

终态注记（2026-10-01 闭环）：
1. 方案落地：`SlowWait` 扩充 `session_locking` 模式字段，`SlowWait::for_command` 接收调度点快照；`exec` 选型点按会话判定下传 `Transactional`/`Basic`；`exec_slow_impl` 外层以 `push_session_locking` 守卫罩住整个慢体 await 段，退出即还原；
2. 普查与登记：普查确认承接降级写命令的落笔键均严格在其声明键集合内，让闩完全安全；订正失实注释，并在 `doc/zh/deviations.md` §139 同族补登「事务重放段降级慢臂事务锁模式跨段界下传」；
3. 锁测覆盖：新增 `wedb/wnode/tests/rmw_txn_slow_degrade_selflatch.rs`，覆盖 (a)(b)(c)(d) 场景，自撞直读零增量且反向夹持闩有效，revert-proof 验证全通过。
