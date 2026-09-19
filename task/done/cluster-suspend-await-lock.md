认领：cluster-suspend-await-lock 收口进 dev

现状：
- 死亡代理分支 cluster-suspend-await-lock（末提交 08:42）领先 dev 四个提交（其中两个为 Merge branch dev 同步），其待办档 next/qcode8.cluster-suspend-await-lock.md 已被分拣代理删除（未合入即未落地）。
- 该分支把 cluster_manager 的 active_merge_lock 从 parking_lot::RwLock 改为 async_lock 异步读写锁，并把 suspend_config_merge、try_merge、try_remove_worker、try_reset 及迁移汇聚调用点 await 化。
- 前置取证前提已过时：dev 自 08:42 经 suspend-lock-async-aware 分支（4edaa0fa 异步读写锁落地、84d1f988 注释口径校正）提前落了同一把异步感知锁，active_merge_lock 现为 AsyncLockRwLock，上述四方法均已 async，worker_state、gossip_manager、keys.rs 调用点已 await。因此分支的锁改写与多数 await 化在 dev 上已是既成事实，本次合并转为收口分支的剩余净贡献，而非重做锁。

落地范围（本次合并净贡献）：
- cluster_session/basic.rs：CLUSTER GOSSIP 与 CLUSTER FORGET 的同步段不再用 block_on 内联驱动挂起门，改挂慢路径执行体（cluster_gossip_slow、cluster_forget_slow），由网络泵在批尾纪元快照清零之后 await 驱动，挂起门读锁/写锁的等待以 await 让出线程。纪元让渡由慢路径天然承接，等价 C# 先 ReleaseCurrentEpoch 再取 SuspendConfigMerge/TryMerge 的死锁规避，未引入第二套挂起机制。
- cluster_session/mod.rs：配合上行，会话字段 remote_node_id、last_sent_config_version 由裸值改为 Arc，供慢路径闭包 owned 捕获。
- cluster_manager.rs：active_merge_lock 字段收口私有（对标 C# private readonly activeMergeLock），唯一入口 suspend_config_merge、try_merge；锁实现与两方法沿用 dev 的异步版本与命名，不并存两套。
- replica_failover_session.rs：修 dev 残留的一处同步读守卫跨 await。原单条 if 链里 current_config() 的 parking_lot 读守卫会存活到 try_merge().await，改为先把 is_known 落 bool known 再 await，确保同步读守卫绝不跨挂起门读锁 await 持有。
- 测试：保留 dev 现有更强的 test_meet_without_lock_succeeds_while_merge_suspended（持挂起写锁贯穿 await 窗口；后台 acquire_lock=true 任务让出，用 wait_for+!is_finished 判定不越窗取锁；收口后读侧立即放行），不取分支较弱的 sleep(50ms) 变体。

明确不取（避免双套与回退 dev）：分支对 cluster_manager.rs 锁的重复改写与别名命名、worker_state/gossip_manager/keys.rs 的等价 await 化（dev 已实现且 C# 锚点注释更详）、cluster_config_persist.rs 与 cluster_management.rs 中重复的 Runtime 声明、gossip_manager 测试的 sleep 变体。

验收：
- cargo +nightly check -p wedb --tests --all-targets --all-features 通过，退出码 0，六个改动文件零告警。
- clippy --lib --no-deps：六个改动文件零诊断；仓库残留九处 clippy 告警全部落在本次未触碰的 cluster_provider、frame_import、migrate_session_vector_set、replica_sync_session、replication_sync_manager，属 dev tip 既有 lint 债（正常由仓库 --fix 门清理），与本次合并无关。
- bun js/check.js：唯一 A 层虚构锚点 wnode/tiered_collection_ops.rs:91 在纯净 dev 上同样复现（退出码 1），合并未新增锚点失败。
- ./test.sh（workspace nextest）：结果见收口回报。

## 实现方案（f40 甄别后细化，2026-09-19）

甄别结论：票据前提中"分支领先 dev 四提交"已失效——分支 cluster-suspend-await-lock 与其 worktree 均已被删除（branch/reflog/for-each-ref/fsck 悬空提交全查无，悬空树内亦无 cluster_gossip_slow）。但"落地范围"四项在当前 dev 上逐一核实成立、不重复、不回退：

1. dev 已落 AsyncLockRwLock + suspend_config_merge/try_merge/try_remove_worker/try_reset await 化（cluster_manager.rs:130/586/637），basic.rs:191/:474 仍 block_on 内联驱动挂起门——单线程 compio 网络泵线程被锁等待卡死的窗口仍在。
2. 慢路径机制成熟：pending_slow + SlowWait::new + admin_commands.rs:324 提升会话槽 + resp_server_session.rs:1265 停消费 + drive.rs:186 批尾 await resolve（此时 try_consume_messages 批尾 release_current_epoch 已执行，纪元天然清零，等价 C# 先 ReleaseCurrentEpoch 再取锁）。cluster_reset_slow 即同款样板。
3. active_merge_lock 外部零代码访问（仅 cluster_manager.rs:587/:641 内部两处），pub(crate) → private 纯收口。
4. replica_failover_session.rs:250-258 let 链 current_config() 读守卫（parking_lot）存活跨 :255 try_merge().await，真实守卫跨挂起门。

净改动四文件：
- cluster_session/mod.rs：remote_node_id 改 Arc<RwLock<Option<u128>>>、last_sent_config_version 改 Arc<AtomicI64>（慢路径执行体 owned 捕获，其余使用点 Arc 透明）。
- cluster_session/basic.rs：新增 cluster_gossip_slow / cluster_forget_slow 慢路径执行体（体内与 C# RespClusterBasicCommands.cs:383-427 / :80-100 逐段同构，known 判定先落 bool 再 await）；network_cluster_gossip / network_cluster_forget 同步段收缩为参数校验+统计+装配检查+版本预检+挂 SlowWait；删除同步段 release/acquire_current_epoch 对（慢路径天然承接）与两处 block_on。
- cluster_manager.rs：active_merge_lock 去 pub(crate)。
- failover/replica_failover_session.rs：let 链拆段，known 先落 bool 再 try_merge().await。

测试面：不新增不改动（保留 dev 现有 test_meet_without_lock_succeeds_while_merge_suspended 强变体）。

验收：worktree 内 cargo +nightly check -p wedb --tests --all-targets --all-features（CARGO_TARGET_DIR=/tmp/wt-target-f40）退出码 0。
