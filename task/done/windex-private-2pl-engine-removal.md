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

收口合并棒判词（2026-09-19 23:30 +0800，载荷落 dev bfbd1e0）

一、步骤 0 复核：部分落地、残差真实，按未落地并残差收口，不判重复。
1. 引擎删除本体确已在 dev：wip 快照 0b318df 一次带走 windex/src/guard.rs −87 与
   table.rs −155，五符号在 dev 的 .rs 面零命中；该快照同时卷入 acl-ns-compact 的认领
   mv、wval/src/meta.rs 与 wkv/session/consistent_read.rs 他域改动，属钩子扫走的
   无归属落地，不是本票正文的产出。
2. 收敛形态未落地：dev 停在中间态 HashIndex::lock_key_exclusive（索引层 1024 轮外层
   spin_loop 后报 Error::LockTimeout），与本票修法 2「无自旋、无回滚」、修法 3
   「勿在索引层造超时」直接相反（HashBucket::try_lock_exclusive 自身已含 128 轮自旋
   与 1024 轮读者排空，外层再叠一轮是第二套自旋驱动），wkv/src/ttl.rs 持的即该超时态入口。
3. 文档面未落地：wedb/README.md、wedb/readme/{en,zh}.md、wedb/windex/README.md、
   wedb/windex/readme/{en,zh}.md 仍叙述 MultiBucketGuard 与 acquire_keys_lock_exclusive。
4. 对手薄票 next/db-hashindex-2pl-single-orchestration.md 已随 0b318df 消失
   （现刻 git ls-tree dev next/ 零命中），双花解除，本棒无需再删。
5. 归档位是被卷入的：R task/ing→task/done 记在 03c21af「style: cargo fmt 收敛」，
   系 pre-commit 钩子 git add -u 扫走他棒暂存的 mv，非判词归档，故本段补正文终裁。

二、载荷与合并注记
1. 死树 /tmp/fork/windex-2pl-removal 的 5 枚载荷提交（e1285bb 删引擎、f2977b8 用例改写、
   15ed80c ttl 单键闩、7b5404c wtxn 注释、dd9d9fe README 收口）经新树 /tmp/fork/windex-2pl-2
   一次 merge 并入（合并提交 994c6c3，测试补强 5b098e0）。死树暂存区三型幽灵
   （根 README.md 软链展开、js/check/ignore/{common,server}.yml、批量 D next/*.md）一概未取。
2. dev 侧回合三次（317fb1e、844417d、a2231a5 及其后代）后以 bfbd1e0 纯 FF 落主仓 dev，
   未产生回滚；bfbd1e0 现仍为 8416358 的祖先。

三、冲突处置记录（逐文件手工，非 --theirs 一把梭）
1. windex/src/table.rs（自动合并出双套闩入口，最险处）：dev 侧 lock_key_exclusive 自旋
   驱动与分支侧 try_lock_key_exclusive 同区并存，且 dev 的返回类型 BucketExclusiveGuard
   已被分支侧换名导入，直接编译不过。处置＝删 dev 的自旋驱动，索引层键级排他入口回归一个。
2. wkv/src/ttl.rs 两处：取分支形态（let Some(..) else 早返 + 调用方承接
   IndexError::LockTimeout 为 C# RETRY_LATER），保住 dev 侧同函数已落地的
   coarse_expire_ticks 头部粗化前置（该段 dev 独有，未被覆盖）。
3. wtxn/src/txn_lock_table.rs 文件头同源锁叙述：改指 try_lock_key_exclusive；
   四个 OverflowBucketLockTable.cs 锚点函数（:106/:114/:122/:130）原样保留，
   本棒对 wtxn 的全部改动＝1 行注释。
4. windex/tests/index/latch_concurrency.rs：取分支单键闩用例（dev 侧对该文件的改动只有
   删一个未用 use gxhash::HashSet，无语义差）。
5. wedb/README.md、wedb/readme/{en,zh}.md：保 dev 已校正的 crate 地图正文（短版地图是老
   基线残象，不回收），只把 HashIndex 行的 lock_exclusive_guard 改指 try_lock_key_exclusive、
   导出行的 MultiBucketGuard 换成 KeyLatch。

四、验收判据逐条实测（现刻 dev = 8416358 复核）
1. 成立：Grep 工具（非 shell glob）对五符号 + 五常量（INLINE_LOCK_ENTRIES /
   SPIN_RETRY_THRESHOLD / SPIN_LIMIT_MAX_EXP / SPIN_LIMIT_JITTER_MASK / YIELD_RETRY_BUDGET）
   在 wedb/ 全仓（含 tests 与 md）零命中，dev 的 interim 名 lock_key_exclusive 亦归零。
2. 成立：windex/src/table.rs:601 唯一入口，wkv/src/ttl.rs:459（expire_at）与
   :511（persist）各出现且仅出现一次，守卫 _key_lock 活到函数尾，覆盖其后
   contains_key_ignore_ttl / ttl_of / purge_expired / put_ttl 全程。
3. 成立：windex/src/lib.rs 导出面含 KeyLatch（BucketExclusiveGuard 的按键寻址别名，
   同字同 Drop，无第二份守卫代码），无任何多键批量取闩类型；判据原文
   「面收敛为 HashIndex + HashBucket + KeyLatch」按字面读会削掉 wtxn 与 entry_info
   共用的 BucketSharedGuard/BucketExclusiveGuard，按本意（不含多键锁类型）执行。
4. 成立：见下门禁数字；C# 锚点集合逐项无损（见第五段）。

五、门禁数字（私有 CARGO_TARGET_DIR=/tmp/ct-widx2，未跑主仓 ./test.sh 与 ./clippy.sh）
1. cargo check --workspace --all-targets：exit 0，零 error 零 warning（终树 bfbd1e0 复跑一次）。
2. cargo nextest run -p windex -p wtxn -p wkv：294 passed / 0 failed / 0 skipped，exit 0
   （终树复跑 10.0s）。首轮跑出的 1 枚 180s TIMEOUT
   （wtxn::txn_lock_stress stress_manual_locks_across_threads_without_deadlock）归因并发
   负载饥饿：本棒对 wtxn 的全部改动是 1 行注释（git diff --stat 965e16d..HEAD -- wedb/wtxn），
   该用例隔离跑 48s 通过、全量复跑 18s 通过，非代码红。
3. cargo fmt：--all 扫到的三处非本票在途未格式化文件（wedb/tests/cluster_slot_verify_wait.rs、
   wnode/src/rangeindex/range_index_manager_replication.rs、wresp/tests/main.rs）已还原，
   未混入本棒；本票文件 cargo fmt --check 干净。
4. bun js/check.js 前后对跑（基线树 = detached 965e16d + 同套软链，本树 = 合并后）：
   两跑 exit 0，stdout 44 行逐字节仅一处差异——重复定义段
   Tsavorite.cs:ContextReadWithPrefetch 下 wedb/windex/src/table.rs 的行号 621→619
   （table.rs 净减 2 行所致，条目本身不变），stderr（B 层 129 提示与 194/1425 语料降级数）
   逐字节相同；ignore 语料零回写零删除（跑后 git status 只余本票载荷文件）。
   锚点条目集合另按 CS_REF_REGEX 复刻全库比对：965e16d 与 5b098e0 各 4282 个不同锚点、
   4654 次出现，多重集逐条相同（判据 4 的 OverflowBucketLockTable.cs 与 TxnKeyEntry.cs
   符号覆盖不减少，windex 四 passthrough 闩锚点原样在册）。

六、移交登记（他票射程，本棒未代改）
1. string-rmw-key-bucket-lock 与 rmw-atomic-read-modify-write-window 两票票面仍写
   「复用 acquire_keys_lock_exclusive 作为唯一锁源」，该入口现已不存在，本票边界条款 1
   生效：两票一律改持 HashIndex::try_lock_key_exclusive（同一把 windex 桶闩，锁源仍唯一）。
2. windex-table-file-split 票的「待先删项：table.rs 这 128 行」段已被本票清空，
   其拆分射程按现表长度重取。
3. 测试面补强另记一棒候选：wkv 键级 TTL 读改写窗口取闩失败现走 Error::Index(LockTimeout)
   上浮，wnode 命令面对该变体的重试映射（是否落 -BOUSEY/RETRY_LATER 类回包）本票射程外，
   未实测。
