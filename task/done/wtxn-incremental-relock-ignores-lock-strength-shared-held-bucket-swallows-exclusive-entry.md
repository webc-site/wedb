锁定注记（2026-10-01 r9 波主控，基线 `55bfe32`；wtxn 锁轨只读甄别席候选 + 主控现码亲验逐条复跑全实，锚以本注记为准，台账禁钉行号）
- rust 病灶现位（两处判据同盲区）：`wedb/wtxn/src/txn_key_entry.rs::TxnKeyEntries::try_lock_incremental_entries`
  第 2 步筛选臂——`already_held` 仅 `self.held.binary_search_by(|h| h.bucket.cmp(&slot.bucket)).is_ok()`，
  **只看桶号、不看锁型强度**；而 `held` 元素就是 `LockPlanSlot { bucket, exclusive }`（`::TxnKeyEntries` 字段 `held`），
  强度信息在手却被判据丢弃。同文件 `::TxnKeyEntries::covers_user_keys` 判据同款（桶命中即算覆盖，不核 `exclusive`）。
- 归并侧口径对照（证明强度确是本仓既定维度）：`::TxnKeyEntries::lock_plan` 与增量臂第 1 步 `merged` 均按
  `last.exclusive |= slot.exclusive` 同桶取最强；增量臂第 3 步按 `slot.exclusive` 分流 `try_lock_exclusive`/`try_lock_shared`。
  即「同桶先持弱、后来要强」在全量臂与增量归并步都被当作强度问题，唯独「已持集过滤步」被当作桶问题。
- 消费点现位：`wedb/wtxn/src/transaction_manager.rs::reexpand_for_generation_swap`（EXEC 重放窗换代增量补锁单点）、
  `wedb/wnode/src/resp/resp_server_session/txn.rs::rearm_txn_locks_for_generation_swap`（失败即 `park_exec_lock_wait`）、
  `wedb/wnode/src/resp/resp_server_session/txn.rs::txn_locks_cover_cmd` → `wedb/wnode/src/resp/garnet_api/mod.rs::exec`
  的 `SessionLocking::Transactional` 选型点。
- 仓内自陈契约（判据落点）：`wedb/wkv/src/session/rmw_window.rs` 模块头「事务会话经 `TransactionalSessionLocker`
  **只断言不取闩——事务已在同一份锁内存上持该键桶的排他闩**」，执行臂为 `::StoreSession::try_rmw_window`
  （Transactional 时 `held = None`，整窗零闩）与 `::StoreSession::try_rmw_window_sorted`（Transactional 时直回空窗口组）。
  `doc/zh/deviations.md` §139 只登记「覆盖判定 + 锁集外键走 Basic ephemeral」这一**选型形态**，未登记也未曾豁免
  「判据忽略锁型强度」——故本票是既有机制的强度维度漏臂，非既有偏差的再解释。
- C# 对位：`garnet/libs/server/Transaction/TxnKeyEntry.cs::LockAllKeys`（单次全量 `Sort` 后
  `UnifiedTransactionalContext.Lock`，同桶由排序合并出最强锁型，持至 `UnlockAllKeys`）、`::AddKey`（按命令定 `LockType` 入集）、
  `garnet/libs/server/Transaction/TxnKeyManager.cs`（无代际概念）。C# 锁轨无代际种子、亦无「换代增量补锁」面，
  故增量补锁的强度正确性是 rust 双轨制（§115）自有义务。
- 前案边界（查重已核，本票为新漏维度非并案）：
  `task/done/wtxn-exec-replay-window-generation-swap-lock-mode-degrade-atomicity-break.md` 创立增量补锁机制，
  方案只裁「按 txn_keys 裸键补算哈希 + 新增桶增量取闩」；
  `task/done/wtxn-exec-replay-generation-swap-reexpand-missing-watch-key-arm.md` 补的是 WATCH 键入集**臂**；
  `task/done/wtxn-multi-queued-lock-hash-stale-across-generation.md` 裁排队键臂；
  三案皆未触「已持桶过滤须分锁型强度」这一维。§115/§139/§193/§194 亦均异面。
- 禁触域（同侪在途）：`wedb/wedb/src/server/replication/**`、`wedb/wnode/src/resp/mod.rs`、
  `wedb/wnode/src/resp/resp_server_session/mod.rs`、`wedb/wnode/src/storage/session/common/ttl_sync.rs`；
  本票只动 `wedb/wtxn/src/txn_key_entry.rs`、必要时 `wedb/wtxn/src/transaction_manager.rs` 增量臂注释面，
  以及 `wedb/wtxn/tests/**`、`wedb/wnode/tests/exec_replay_generation_swap_lock.rs`。

审核结论：通过（2026-10-01 主控亲验立案；P2。触发前提四件齐备方可达：standalone（cluster 关）+ MULTI 内存在写命令
（Exclusive 条目）+ 同事务另有仅持 Shared 的桶（WATCH 键臂或 RO 排队键）+ EXEC 重放窗内换号（队内 FLUSHDB/SWAPDB
或慢臂挂起窗并发换号），且换代后写键落桶与旧代某 Shared 持桶**同桶碰撞**（默认锁表 `DEFAULT_TXN_BUCKETS = 1024`，
单条目碰撞概率约「本事务 Shared 持桶数 / 1024」，生产面长尾但测试面可确定性构造）。后果为该写命令全程无排他保护，
且属静默破口：无断言、无日志、无观测位。与前案 mode-degrade 同族同危害量级，定 P2）

EXEC 重放窗换代增量补锁按桶去重不分锁型强度，Shared 旧持桶吞掉 Exclusive 新条目致写命令无排他执行且事务让闩窗全程裸奔

问题分析：
1. Garnet 契约对齐：C# 一笔事务的键锁集一次成型——`TxnKeyEntry.AddKey` 按命令登记 `LockType`，
   `LockAllKeys` 单次排序（同桶由比较器合并出最强锁型）后一次性 `Lock`，锁轨身份与物理代际无关，
   不存在「已持弱闩的桶后来需要强闩」的中间态；rust 采 §115 双轨制（锁轨哈希 = `scoped_key_hash(物理前缀, 裸键)`），
   换代即桶身份变化，于是「按新前缀重展开」成为 rust 自有义务，而重展开天然是**增量**过程——
   增量就必须处理「旧代已持桶」与「新代对该桶要求更强锁型」的交叠。现码增量臂把该交叠当作「已持即跳过」，
   强度维度在过滤步凭空消失，等于把 C# 一次成型的不变量在 rust 增量形态下丢了。
2. 工程现状：`reexpand_for_generation_swap` 构造的 `new_entries` = WATCH 臂（恒 `LockType::Shared`，f48873b 补齐）
   + `txn_keys` 臂（写命令按命令定 `Exclusive`），两臂归并（`merged`）后进入第 2 步过滤：
   凡桶号已存在于 `self.held`（旧代 EXEC 起点取锁时登记，含仅 Shared 的 WATCH 桶）即整条丢弃，
   既不升闩也不补闩，更不申报失败。于是当 `bucket(新代写键) == bucket(旧代某 Shared 持键)` 时：
   (a) 该桶实际只持 Shared——同桶他连接的 Shared 兼容面（读侧落槽）与本事务写窗并发，他连接事务的
   Shared 条目也能与本事务并发，写窗内「判定 → 读改写」无排他屏障；
   (b) `covers_user_keys` 同判据把该桶判为「已覆盖」，`garnet_api::exec` 据此选 `SessionLocking::Transactional`，
   `try_rmw_window` 于是 `held = None`（连自取闩都跳过），写命令在**一条闩都不持**的窗口里执行完读改写并落 AOF。
   即增量漏升闩 + 覆盖判定漏核强度，两臂叠加把「有闩但型错」放大成「完全裸奔」。
3. 逻辑危害确证：最小复现（standalone，锁表默认 1024 桶，测试面按 `find_distinct_bucket_prefixes` 同款搜索
   选定 `old_prefix`/`new_prefix` 使 `bucket(新代 k) == bucket(旧代 w)` 成立）——
   A：`WATCH w` ; `MULTI` ; `FLUSHDB`（队内换号，EXEC 展开后前缀换代） ; `SET k v` ; `EXEC`。
   EXEC 起点 A 以旧代哈希对 `bucket(旧 w)` 持 Shared；换代重展开时 `bucket(新 k)` 恰为同桶，
   Exclusive 条目被第 2 步过滤吞掉，`held` 仍只有该桶的 Shared 记录。
   并发面 B：`SET k other`（走 Basic ephemeral 自取排他闩——Shared 与排他虽不兼容，但 B 侧同桶共享兼容读窗
   与 A 的读改写交错）与他事务 `MULTI; GET k; EXEC`（Shared 条目）即时得逞；
   A 侧 `SET k v` 因 `covers_user_keys` 判 covered 而完全不取闩，写值与被覆盖旧值之间无任何原子屏障，
   `EXEC` 提交后 `k` 终态可为 B 的值（lost update），亦可在 A 自身读改写中途被 B 改写（torn write）。
   全程无断言、无 `warn`、`GcStatsSnapshot`/事务观测面无档位分叉位，运维侧不可见。定 P2。

涉及代码：
rust 文件与函数：
wedb/wtxn/src/txn_key_entry.rs::TxnKeyEntries::try_lock_incremental_entries（第 2 步过滤臂忽略锁型强度，主病灶）
wedb/wtxn/src/txn_key_entry.rs::TxnKeyEntries::covers_user_keys（同判据盲区，把「型错」放大成「无闩」）
wedb/wtxn/src/transaction_manager.rs::reexpand_for_generation_swap（增量臂唯一消费点，换代重展开单点）
wedb/wnode/src/resp/resp_server_session/txn.rs::txn_locks_cover_cmd、::rearm_txn_locks_for_generation_swap（选型与失败承接面）
wedb/wnode/src/resp/garnet_api/mod.rs::exec（`SessionLocking::Transactional` 选型点）
wedb/wkv/src/session/rmw_window.rs::StoreSession::try_rmw_window、::StoreSession::try_rmw_window_sorted（让闩执行臂，自陈契约源）

对应 c# 文件与函数：
libs/server/Transaction/TxnKeyEntry.cs::LockAllKeys（:106-124，单次全量取最强、持至终局）
libs/server/Transaction/TxnKeyEntry.cs::AddKey（:88-104，按命令定 LockType）
libs/server/Transaction/TxnKeyManager.cs（:16-21，无代际/无增量补锁面的对位证据）

精炼执行方案：
1. 增量臂过滤步改「按桶 + 按强度」双判：`held` 命中且 `slot.exclusive` 为真而该持槽 `exclusive` 为假时，
   该桶进入**升闩集**（不跳过）。升闩形态固定为「先释本会话旧 Shared → 再取 Exclusive → 任一步失败整批对称回滚」：
   释闩复用增量失败臂已有的 `unlock_shared`/`unlock_exclusive` 逐槽对称放闩口（本文件内既有形态，勿新造守卫类型）。
   **严禁原地升排他**（自家 Shared 未释即 `try_lock_exclusive`：`HashBucket` 内嵌闩非重入，自家持闩令取闩必假、
   失败臂又按「非自家新增」不回滚自家旧闩，慢臂 `park_exec_lock_wait` 重驱即活锁）；
   **严禁只回滚不回补**（回滚臂须把本次释掉的旧 Shared 按升序原样回补，回补亦失则整笔增量判失败并让
   `held`/闩态严格一致——宁可让重驱慢臂重来，绝不留「`held` 记着有闩而桶上无闩」或反之的半持态）。
   合并后第 4 步 `held` 并集需按新强度落槽（升闩成功的槽 `exclusive` 置真）。
2. `covers_user_keys` 判据并锁型入参：调用方按本命令是否需要排他（写命令）传入要求强度，
   判据改为「桶命中 **且** 该桶持槽强度 ≥ 要求强度」才算覆盖；写命令落仅 Shared 桶时回 false →
   `garnet_api::exec` 选 Basic ephemeral，窗口自取排他闩（与 §139 已登记的「锁集外键走 Basic ephemeral」同一收口形态，
   不新造第三态）。`txn_locks_cover_cmd` 侧强度取值须与排队落键段（`register_run_preamble` / `txn_queued_command_info`）
   的 `LockType` 判定**同源单点**，禁另造「写命令集合」第二份判据。
3. 顺带核清并申报（本票不预判、若非属实须在回报中写证据）：增量臂成功后 `self.plan`（缓存全量计划）是否仍为旧代哈希——
   若重驱臂 `try_lock_all_keys_once` 在换代后被再次调用，按旧计划取闩会与 `held` 现持态冲突（同桶重复取非重入闩）。
   若坐实为独立病灶，回报单列，主控另立案，勿在本票内顺手改。
4. 禁做项：不改 §115 双轨口径注释与 `scoped_key_hash` 构造口；不动 `lock_plan` 的全量归并算法；
   不改 `run_exec` 门控位与 `park_exec_lock_wait` 语义；不给锁表加新配置项或新观测旋钮；
   禁把 `covers_user_keys` 改成「一律回 false」（那会让所有事务写命令退化 Basic，属另一形态的语义破口）；
   禁 `#[allow]`/`#[expect]`；禁新增占位实现。
5. 锁测（两处各一臂，缺一不收）：
   (a) `wedb/wtxn/tests/**`（或扩既有增量取闩册）：旧代 Shared 持桶 + 新代同桶 Exclusive 条目的增量重展开，
       断言该桶 `is_latched_exclusive()` 为真、`held` 内该槽 `exclusive` 落真，终局 `commit` 后闩释放且幂等；
       并加争用臂：他连接先占该桶排他闩时升闩失败，断言旧 Shared 被原样回补（他连接 `try_lock_shared` 亦失败）
       且增量臂回 `false`。
   (b) `wedb/wnode/tests/exec_replay_generation_swap_lock.rs`：扩既有换代册，断言同桶碰撞下写命令
       `txn_locks_cover_cmd(Set, ..)` 回 **false**（选型转 Basic ephemeral，窗口自取闩），
       非碰撞桶维持 true（防把判据降档成恒假）。
   revert-proof：撤第 1 步强度判据后 (a) 排他断言必转红；撤第 2 步强度入参后 (b) 覆盖断言必转红。
6. 验证面：`cargo check -q -p wtxn -p wnode --all-targets` 与 `cargo nextest run -p wtxn` +
   `cargo nextest run -p wnode --test exec_replay_generation_swap_lock`；禁在主树跑 `./test.sh`/`./sh/clippy.sh`。

终态注记（2026-10-01 闭环）：
1. 换代增量补锁收口：`txn_key_entry.rs::try_lock_incremental_entries` 引入「按桶 + 按强度」双判，旧代仅持 Shared 且新代同桶需要 Exclusive 时进入升闩集（先释放旧 Shared → 再获取 Exclusive；失败则整批回滚并原样回补旧 Shared，不留半持态；成功后置 `exclusive = true` 并入 `held`）；
2. 覆盖判定对齐：`txn_key_entry.rs::covers_user_keys` 引入 `require_exclusive` 强度参数（要求 Exclusive 时持槽必须为排他），`wnode txn.rs::txn_locks_cover_cmd` 侧与其同源单点对齐（写命令要求 Exclusive，落仅 Shared 桶回 false 走 Basic ephemeral 自取闩）；
3. 锁测覆盖：`wtxn` 增量册三臂（升闩断排他闩在位与幂等释放、升闩争用断旧 Shared 原样回补、整批回滚断升闩桶回补）；`wnode` 换代册扩充同桶碰撞写命令判不覆盖用例，45+5 单测全绿，revert-proof 检验均转红；
4. self.plan 核查申报：`reexpand_for_generation_swap` 仅增量更新 `held`/`keys` 未清空旧代 `plan`，当前执行路径终局通过 `unlock_all_keys` 清空，不经过二次 `try_lock_all_keys_once`；若后续演进有重驱重入风险，建议在重展开成功时置 `self.plan = None`。

---

## 主控反证式审计注记（2026-10-01 r9 波，席面 e553065 / dev 落地 054b8e2）
- **交付对账**：席面申报 numstat（txn.rs +10/−1、wnode 册 +82/0、transaction_manager.rs +3/−2、txn_key_entry.rs +136/−33、
  wtxn 册 +209/0）与分支逐字节相符；禁触域四路径零接触。dev 侧 e553065..dev 的 src 差量仅 rustfmt 折行
  （`covers_user_keys` 调用链与 `binary_search_by` match 头），语义零漂移；`wtxn/tests` 差量为测试侧
  元组改具名结构体 `RelockCollision` +  fmt，判据断言原样保留——他席合并未削弱本票锁测。
- **反证两轮（主控独立复跑，沙箱内自证撤臂转红后 `git checkout --` 还原复绿）**：
  1. 撤第 1 步强度判据（`upgrade_buckets` 集置恒空）→ wtxn 册 **3 臂同时转红**，红位正是票面要求的
     `is_latched_exclusive()`（升闩在位）、`reexpand` 必回 `false`（升闩争用）、重驱后 `is_latched_exclusive()`
     （整批回滚臂）三处断言，其余 18 测仍绿（特异性达标）；
  2. 撤第 2 步强度入参（`covers_user_keys` 回「桶命中即覆盖」）→ wnode
     `test_covers_strength_write_on_shared_bucket_not_covered` 转红（`!s.txn_locks_cover_cmd(Set, ..)`），
     既有换代四测不红。
  两轮还原后 wtxn 45/45、wnode 换代册 5/5 复绿。
- **方案 3 裁定采纳**：席面给出四段证据链（重驱入口只回退至当前排队命令起点、Running 态 `network_exec` 分支直 `commit`
  走 `release_held`、`ensure_plan`/`acquire_plan` 消费点仅 Started 态与线程臂）⇒ `plan` 旧代残留**无可达消费面**，
  不另立案；本册第 4 条留档建议（重展开成功置 `plan = None`）作为防御性注记保留，不作为缺陷追踪。
- **偏差采纳**：① 票面 (a)「他连接先占升闩桶排他闩」确为物理不可构造（自家旧 Shared 挡他连接排他入册，
  drain 臂保证回退），席面改以「他连接持共享令升闩败」+「他连接先占新增桶令整批回滚」两臂覆盖同一失败语义，
  等价且更强（后者才真正测到回滚+回补对偶），准；② 票面 (b) 在升闩成功后会自然转真，故按问题分析 §2(b)
  的「未经重展开」放大场景构造直击强度入参，与 (a) 三臂双向夹住，准。
- **残项处置**：「回补亦失 → `held` 除名」臂无确定性构造（需释 Shared 与回补之间的竞态窗），
  代码路径已实现且失败即整批回 `false` 交重驱，不留半持态——**登记为待测面**，若后续引入取闩注入口钩子
  （本仓 `#[cfg(test)]` 屏障钩家族先例）再补锁测；wnode 级端到端同桶碰撞选型断言同理（换代后前缀不可自由选定）。
  两者均不阻断收口。

## 合入后重构复核（2026-10-01 r9 波，共主席 `aad4782`）
- **改动面**：`wedb/wtxn/src/txn_key_entry.rs::try_lock_incremental_entries` 由「两集（新增/升闩）归并 + `done`
  前缀跟踪」简化为「按 `merged` 桶升序单趟生成 `steps`，失败步先就地处理升闩回补、再按
  `steps[..failed_idx]` 逆序对称回滚」；并落实本册第 4 条留档建议——重展开成功（含 `steps` 空返回臂）置
  `self.plan = None`。
- **复核结论：通过**。`merger` 严格升序 ⇒ `steps` 天然升序，删除的 `debug_assert!` 与二路归并为等价冗余；
  失败步为 `Upgrade` 时「旧 Shared 已释出」的回补/除名臂前置于回滚循环之前，与原实现同语义；
  强度双判（`slot.exclusive && !self.held[held_pos].exclusive`）与 `covers_user_keys` 强度入参原样保留。
- **`plan = None` 安全性**：现拓扑下重展开成功态不会再复调 `try_lock_all_keys_once`
  （该内核消费点在 Started 态与线程臂，重驱走增量臂），故不引入「旧代 `held` 桶号按新钉定索引解释」的串锁面；
  本册方案 3 裁定（旧代残留 `plan` 无可达消费面）与此一致，此改动属防御性收敛而非缺陷修复。
- **反证复跑**（dev tip `9e8a429`，沙箱 `.forks/audit-wtxn-relock`）：摘除强度双判（条件置恒假）后
  `exec_replay_generation_swap_lock` 恰三测转红——`test_incremental_relock_upgrades_shared_bucket_to_exclusive`
  （`wtxn/tests/exec_replay_generation_swap_lock.rs:327`）、`test_incremental_relock_upgrade_step_contended_restores_shared`
  （同册 `:416`）、`test_incremental_relock_batch_rollback_restores_upgraded_and_new_slots`（同册 `:502`），
  其余三测绿；`git checkout --` 还原后 6/6 复绿。席面锁测在本次重构后仍具载负特异性。
