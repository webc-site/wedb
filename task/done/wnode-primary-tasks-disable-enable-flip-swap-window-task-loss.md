锁定注记（2026-10-01 r8 波主控，基线 f89369e；票面 09-30 行号已漂，以本注记为准）：
- primary_tasks.rs 现位：try_start_commit_task :171（swap :182）、try_start_object_collect_task :227（swap :239）、
  aof_commit_loop :269（退出臂 store(false) :284）、object_collect_loop :324（两退出臂 :345 有频率臂 / :355 引擎释放臂）。
  五站点计数不变（swap 二 + store 三），票面「漏一即留窗」口径照旧。
- 漂移来源：本波同文件仅模块头注释 +2 行（e7efacb 悬空锚订正），零逻辑改动，判据与方案 1 单锁串行口径成立。
- 锁测门钉按票面方案 2 走 cfg(test) 门控，禁在生产路径落屏障钩子。
- 禁触域：wedb/wnode/src/resp/vector/vector_store_callbacks.rs、wedb/wedb/src/server/replication/**、
  wedb/wnode/src/storage/session/common/ttl_sync.rs（同侪在途）；本票只动 primary_tasks.rs 与新增/追加锁测册。

审核结论：通过（2026-09-30 甲轮48 审核席；P3。方案 1 已补五站点精确枚举、锁测改 cfg(test) 门控形、C# 段补 TryStart 两函数行号）

Primary 类周期任务禁用→启用快速翻转在 env 锁外交错丢任务，幂等重拉被未落位标志击穿

问题分析：
1. Garnet 契约对齐：StoreWrapper.cs ReconcilePrimaryTask（:1011-1033）在 taskLifecycleLock（:949）下先 CancelAsync 等任务终局再重启，cancel 与 restart 严格串行，零交错窗。
2. 工程现状：rust 双轨归一由「循环自退出 + try_start 幂等重拉」承接（primary_tasks.rs 模块头自陈），但两侧判定各自持锁后锁外操作 started 标志。退出臂：aof_commit_loop/object_collect_loop 在 commit_env/object_collect_env 锁内读得禁用值（AofCommitFreq<=0 / ExpiredObjectCollectionFreq<=0）→释放锁→锁外 started.store(false) 再 break（primary_tasks.rs:281-283/:339-344/:351-354）。重拉臂：try_start_commit_task/try_start_object_collect_task 同样锁内读频率、锁外 started.swap(true)（:169-185/:225-242）。交错窗：旧任务锁内读得禁用→释放锁；CONFIG SET 重启用（写正值）走 try_start，swap(true) 因旧任务尚未落 false 而失败返回；随后旧任务落 false 退出——任务消失且零日志。:340-342 注释自称「退出决定与标志翻转同点生效」已封此窗，实际仅把窗从整轮 sleep 收窄到「锁释放至 store 之间」，被 OS 抢占即无界，未闭合。
3. 逻辑危害确证：周期 AOF 提交节拍（aof_commit_loop 的 commit_async 落盘节拍）或周期对象收集+分层降阶评估轮（tiered_demote_round 唯一生产宿主，§120）静默停摆，直至下一自愈触发点：新连接首会话惰性拉起（service.rs:2036/:2084）、再一次 CONFIG SET 调停（config_owner.rs:50/:63）、升主 resume。自愈面存在使危害有界，定 P3。运行期合法域核验：expired-object-collection-freq 含 0（禁用可达）、aof-commit-freq 含 -1（禁用可达），禁用→启用翻转均可经 CONFIG SET 合法构造。

涉及代码：
rust 文件与函数：
wedb/wnode/src/primary_tasks.rs:169 try_start_commit_task、:225 try_start_object_collect_task（started swap 在 env Mutex 临界区外）
wedb/wnode/src/primary_tasks.rs:267 aof_commit_loop、:322 object_collect_loop（退出臂 store(false) 在 env Mutex 临界区外）

对应 c# 文件与函数：
libs/server/StoreWrapper.cs:949 taskLifecycleLock、:979 TryStartCommitTask、:987 TryStartObjectCollectTask、:1011 ReconcilePrimaryTask

精炼执行方案：
1. try_start_*_task 的 started swap 与两循环退出臂 store(false) 一并移入既有 commit_env/object_collect_env Mutex 临界区：退出判定、标志翻转、重拉判定三点同锁串行（即 C# taskLifecycleLock 的单锁对位，零新机制）。具体站点：swap 两处（primary_tasks.rs:180、:237）与退出臂 store(false) 三处（:282、:343、:353）移入所在 env Mutex 既有临界区并与频率读取同锁持有，spawn 与 is_replica 检查留在锁外；object_collect_loop 有频率臂与引擎释放臂两个退出臂，三处 store 须一并移入，漏一即留窗
2. 锁测：测试门钉以 cfg(test) 门控（如临界区内 test-only 屏障钩子）钉中间态构造「旧任务锁内读禁用值后挂起、CONFIG SET 重启用抢跑 swap」交错，断言终态恒为恰一任务在跑；门钉不落生产路径
3. 验证 CONFIG SET aof-commit-freq -1→正值 与 expired-object-collection-freq 0→正值 两面收敛

---

## 终态注记
- **合入哈希**：`35a14bb`（cherry-pick 自 `1ac90c6`）
- **收口形态**：
  1. 在 `wedb/wnode/src/primary_tasks.rs` 中，将 `try_start_commit_task` 和 `try_start_object_collect_task` 的 `started.swap(true)` 移入对应的 `commit_env` / `object_collect_env` Mutex 锁内，与频率读取同锁串行。
  2. 将 `aof_commit_loop` 与 `object_collect_loop` 退出臂中的 `started.store(false)` 移入对应 env 锁内，确保退出判定与标志翻转原子串行，彻底消除禁用→启用快速翻转交错丢失任务窗。
  3. 增加 `#[cfg(test)]` 门控屏障锁测及 `tests/primary_task_manager.rs` 翻转收敛集成测试。
- **门禁验证**：`cargo check -p wnode --all-targets` 通过，`primary_tasks`、`primary_task_manager` 与 `object_collect_task` 全部单测通过。

## 主控全量复核（2026-10-01）

- **五站点齐备无漏项**（票面「漏一即留窗」口径逐点复验，dev 尖端 `wnode/src/primary_tasks.rs` 现码）：
  `try_start_commit_task` 的 `commit_started.swap(true, AcqRel)` 已在 `commit_env` 锁块内、与频率读取同锁；
  `try_start_object_collect_task` 的 `object_collect_started.swap` 已在 `object_collect_env` 锁块内；
  退出臂三处——`aof_commit_loop` 频率 `<=0` 臂与引擎升级失败臂、`object_collect_loop` 频率 `<=0` 臂与
  `store.upgrade()` 释放臂（外加 `env` 缺席臂）——的 `store(false, Release)` 全部落在各自 env 锁块内，
  旧的「锁外先落标志再 break」形态（含被票面点名已失效的那段「退出决定与标志翻转同点生效」注释）已消除。
  `spawn_*` 与 `is_replica` 判定留在锁外，符合方案 1 边界。
- **无新机制、无跨 await 持锁**：单锁对位 C# `taskLifecycleLock` 成立；锁块体内全同步（`upgrade`/`get_int`），
  无 `.await`，未引入第二把锁或原子 CAS 环。
- **门钉不落生产路径**：`COMMIT_EXIT_BARRIER`/`OBJECT_COLLECT_EXIT_BARRIER` 与 `BarrierGuard` 一律 `#[cfg(test)]`，
  形态与仓内既有留钩先例同族（`wkv::session::TEST_COLD_WINDOW_HOOK` 及 range_index/replication 三处同族注释互引），
  非本票新造范式。新增 `commit_running()` 观测面在 `src` 测试册与 `tests/primary_task_manager.rs` 双册有消费，零死码。
- **同批侧改动（授权范围内，如实登记）**：标志读写的 `Ordering::Relaxed` 折为 `AcqRel`/`Release`——
  标志现由持锁方写、非持锁方（观测面）读，加强序为单锁收口的自然连带，不改判定语义；
  `is_replica` 由「锁内读频率之后判」上提为函数首行早退（原顺序亦在 swap 之前，等价）。
- **锁测自证性**（替代独立反证，成本更低且更强）：`test_*_exit_restart_interleaving_serialized` 把屏障放在
  **锁内 store 之前**，随后断言「starter 线程 50ms 内 `try_recv().is_err()`」——该断言只在
  `try_start` 与退出臂争同一把 env 锁时成立。若后人把 `store(false)` 挪回锁外（即回退本票），
  旧任务在钩内挂起时已不再持锁，starter 立刻抢跑 → 该断言必转红。故册内自带 revert-proof。
- **沙箱复跑**（`--all-features`，`.forks/audit-itembroker` @ dev 尖端，日志 `.bench_run/audit-primary-tasks.log`）：
  `wnode --lib primary_tasks` 2/2、`wnode --test primary_task_manager` 6/6，均 passed、无 skipped。
