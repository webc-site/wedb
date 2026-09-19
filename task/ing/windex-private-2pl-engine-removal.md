优先级：高（多套架构：索引层私藏第二套 2PL 事务锁引擎）
来源：next/agy.db.md 条 2。核销 2026-09-19，取证基线 = 主仓 /Users/z/git/db/wedb 分支 dev 当下 HEAD。

结论一句话
windex HashIndex 在索引层内实现了一套完整的多键两阶段锁（桶序排序、原地去重、自旋退避、
逆序回滚），而 C# 的索引层锁件只有单桶闩，多键 2PL 归服务端事务层（本仓已由 wtxn 实现）。
该引擎全工程只有 wkv ttl.rs 两处单键调用消费，属分层倒置 + 双套机制，须删引擎、
把 ttl 的读改写窗口改为单键桶闩守卫。

现状（主仓 HEAD 实测）
1. 私有 2PL 引擎（windex/src/table.rs）：:652-671 in_place_dedup_by（原地切片去重）、
   :673-708 acquire_bucket_locks（栈上内联 16 条目 + 堆回退、桶序排序、:704 sort_unstable_by）、
   :710-714 acquire_keys_lock_exclusive（对外入口）、:716-779 acquire_unique_locked_entries
   （阶段一加锁、阶段三逆序回滚、:769-777 指数退避 + jitter 自旋、:764 YIELD_RETRY_BUDGET 超时）。
   守卫类型 MultiBucketGuard 在 windex/src/guard.rs（:76 注释与 acquire_unique_locked_entries 的
   get_unchecked 安全前提绑定）。
2. 真实消费面只有两处，且都是单键：wkv/src/ttl.rs:456（expire_at 持本键独占桶锁串行化读改写窗口）、
   :504（persist 同款）。两处形态均为 index.acquire_keys_lock_exclusive(&[user_key])，
   键数组长度恒为 1，多键排序/去重/回滚/退避全链为死复杂度。
3. 其余引用全在测试：windex/tests/index/latch_concurrency.rs:376、:397、:513、:548、:590、:596。
4. 单桶闩原语（保留件）：windex/src/bucket.rs:74 try_lock_shared 与 try_lock_exclusive /
   unlock_shared / unlock_exclusive，桶字内嵌闩，已被 wtxn 事务锁面共用
   （wtxn/src/txn_key_entry.rs:182-186 取闩、:164-169 逆序放闩，钉定索引版本在 :205/:223）。

C# 参考
1. libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs
   全文只有单桶 TryLockShared / TryLockExclusive / TryPromoteLock / UnlockShared / UnlockExclusive
   与排序比较辅助（CompareKeyHashes / SortKeyHashes），没有任何「批量取闩 + 失败回滚」驱动。
2. 多键两阶段锁的归属层是服务端事务：libs/server/Transaction/TxnKeyEntry.cs:LockAllKeys /
   TryLockAllKeys（配合 TransactionalContext.cs 的 DoTransactionalLock/DoTransactionalUnlock
   逆序回滚）——本仓已 1:1 落在 wtxn/src/txn_key_entry.rs:130-245（lock_plan / acquire_plan /
   release_held / lock_all_keys / try_lock_all_keys）。
3. 单键 ephemeral 桶锁的 C# 对位（ttl 窗口真正需要的形态）：
   libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:70
   FindOrCreateTagAndTryEphemeralXLock（取不到即返回状态，不自旋不回滚）。

修法
1. 删除 windex 私有 2PL 引擎：acquire_keys_lock_exclusive、acquire_bucket_locks、
   acquire_unique_locked_entries、in_place_dedup_by、MultiBucketGuard 及
   INLINE_LOCK_ENTRIES / SPIN_RETRY_THRESHOLD / SPIN_LIMIT_MAX_EXP / SPIN_LIMIT_JITTER_MASK /
   YIELD_RETRY_BUDGET 常量（删前逐个 grep 确认无其他消费方）。
   注意：in_place_dedup_by 是为本机 nightly slice 无 dedup_by 而写的替代件，随引擎一并删除，
   不得搬去 wbase 续命。
2. 索引层只保留单桶闩 + 一个按键取本键独占闩的最小守卫：在 windex 增加
   HashIndex::try_lock_key_exclusive(key) -> Option<KeyLatch>（RAII Drop 放闩，
   对标 FindOrCreateTagAndTryEphemeralXLock 的「一次尝试、失败返回」形态），
   由 bucket_index_for_key + HashBucket::try_lock_exclusive 组合，无自旋、无回滚。
3. wkv/src/ttl.rs:456、:504 改持该单键守卫，并在取闩失败处按 C# 口径返回重试语义
   （现有 Error::LockTimeout 分支由守卫的调用方承接，勿在索引层造超时）。
4. windex/tests/index/latch_concurrency.rs 的多键用例随之改写为单键闩用例（跨桶序、
   回滚、退避的断言全部删除——C# 无该形态，测试也不留）。

边界与同锁源约束
1. next/string-rmw-key-bucket-lock.md 与 next/rmw-atomic-read-modify-write-window.md 计划
   复用 acquire_keys_lock_exclusive 作为 RMW 读改写窗口的唯一锁源；本票删的正是该入口，
   两票落地时一律改用 HashIndex::try_lock_key_exclusive（同一把 windex 桶闩，锁源仍唯一，
   不新增第四把锁）。实施顺序：本票先落，或由同一子代理合做，勿两处在 windex 各改一次。
2. next/wtxn-lock-stripe-count-parity.md 的主路径（wtxn 改用 windex 桶闩）已按
   loader + pin 形态落地（wtxn/src/txn_lock_table.rs:86-134、txn_key_entry.rs:205/:223），
   但该票「让 wtxn 消费 acquire_keys_lock_exclusive」的表述与本票方向相反，落地时以本票为准，
   该票只剩注释订正价值。
3. wtxn/src/txn_lock_table.rs 四函数（:108/:116/:124/:132）挂 OverflowBucketLockTable.cs 锚点，
   是 check.js 的对位挂载点，本票不得顺手删除（另见 task/reject/design-txn-locktable-anchor-remount.md）。

验收判据
1. 全仓 grep（含 tests）对 HashIndex::acquire_keys_lock_exclusive、acquire_bucket_locks、
   acquire_unique_locked_entries、in_place_dedup_by、MultiBucketGuard 五符号零命中。
2. wkv::StoreSession::expire_at 与 wkv::StoreSession::persist 函数体内出现且仅出现一次
   HashIndex::try_lock_key_exclusive，锁窗覆盖其后的 ttl_of/purge_expired/put_ttl 全程。
3. windex 对外导出不含任何多键锁类型（windex/src/lib.rs 面收敛为 HashIndex + HashBucket + KeyLatch）。
4. cargo check 通过（禁在共享 target 跑 test.sh / clippy.sh，由主代理合并后统一跑）；
   js/check.js 对 OverflowBucketLockTable.cs 与 TxnKeyEntry.cs 的符号覆盖不减少。

双花登记（本票认领前必读）
并发分拣代理就 next/agy.db.md 条 2 另立了同题票 next/db-hashindex-2pl-single-orchestration.md
（若已被消费则看它在 task/ing/ 或 task/done/ 的同名件，或已随 dev 在途）。两票同指
windex/src/table.rs 与 wkv/src/ttl.rs，禁止两棒各改一次：派发时只取一棒，
取本票则按上述「删引擎 + 单键闩」实施，取那票则须补上其对 RMW 两票的锁源交接说明。
分拣补记（agy.db 条 2 增量，2026-09-19）：修法方向确认——向下单点化收敛双套 2PL 编排，禁止 wkv 依赖 wtxn。
在途核位（2026-09-19 22:05 复核）：worktree /tmp/fork/windex-2pl-removal（分支 windex-2pl-removal）已开、
diff 尚空，说明本题已被认领，本票即该棒的正文依据；对手薄票 next/db-hashindex-2pl-single-orchestration.md
当下仍在 next/，派发/合并时删之，禁第二棒。
