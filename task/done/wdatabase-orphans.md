# wdatabase-orphans

来源：next/design.md 条 6（wdatabase 面零引用孤儿 10 项），主代理预清理后移交。

## 甄别结论（对照 garnet C# 生产链）

### checkpoint 策略族 4 项：C# 均生产在用，rust 功能内核已在，删固有转发层

功能内核全部保留（DatabaseManagerBase + IDatabaseManager trait 面），删除的是零外部调用者的固有方法转发层（与 server.yml Databases 段既有决策「彻底清理孤儿门面方法与重复转发，消除双轨门面」同轨）。

1. single_database_manager.rs:48 recover_checkpoint
   - C# 链：StoreWrapper.cs:392 RecoverAsync（--recover 非集群分支）→ databaseManager.RecoverCheckpointAsync
   - rust 等价覆盖已存在：wnode/src/service.rs:595 recover_checkpoint_store 直调 base.recover_database_checkpoint_async（rust 恢复拓扑需接管恢复出的 store 句柄，门面 Result<()> 签名装不下该产物，生产永不经此门面）
   - 处理：删固有方法，trait impl 内联 base 调用（与 Multi 侧形态对齐）
2. single_database_manager.rs:86 take_on_demand_checkpoint
   - C# 链：StoreWrapper.cs:428 ← ReplicaSyncSession.cs:292（副本磁盘同步 OnDemandCheckpoint 分支：主端 checkpoint 不覆盖 AOF 截断位点时按需补拍）
   - rust 复制面（wedb/src/server/replication/replica_sync_session.rs）仅转写 SendCheckpointAsync 起段，该分支未转写且属 server/ 并发代理域
   - 处理：删固有方法，trait impl 内联 base.take_on_demand_checkpoint_async（语义内核保留）；复制面接线记录为复制域待办
3. single_database_manager.rs:96 task_checkpoint_based_on_aof_size_limit
   - C# 链：StoreWrapper.cs:962 StartPrimaryTasks（AofSizeLimit 配置 > 0 注册 AofSizeLimitTask）→ AutoCheckpointBasedOnAofSizeLimitAsync:657 周期轮询
   - rust 配置槽 aof-size-limit 为 ConfigMeta::read_only（runtime_server_config.rs:398）且 wnode 构造恒 RuntimeServerOptions::default()（aof_size_limit = ""）→ 恒关闭；wnode/src/task.rs TaskManager 生产零注册（register_and_run 仅测试使用）
   - 接线即「读恒关配置的空转循环」= 占位假实现，红线禁止 → 删固有方法，trait impl 内联 base.checkpoint_if_aof_exceeds；任务装配域（对标 StoreWrapper.StartPrimaryTasks 全家）记录为独立待办
4. single_database_manager.rs:104 commit_to_aof
   - C# 链：StoreWrapper.cs:685 CommitTaskAsync 周期提交 + :525 CommitAOFAsync 命令面
   - rust 等价覆盖已存在：同步 group commit（wnode/config_owner.rs:24-31 留档：rust 无 CommitTaskAsync 周期任务域，提交在 AOF 日志构造期固化）；COMMITAOF 命令空壳在 wnode/resp/admin_commands.rs:160（resp/ 并发代理域，只记录不修改）
   - 处理：删固有方法，trait impl 内联 base.commit_aof

### grow_indexes_if_needed 全链删

5. single_database_manager.rs:141 grow_indexes_if_needed
   - C# 链：StoreWrapper.cs:1067 StartGenericNodeTasks（AdjustedIndexMaxCacheLines > 0）→ IndexAutoGrowTaskAsync:810 → databaseManager.GrowIndexesIfNeededAsync
   - rust 底层在线扩容缺失（next/db.md 条 1 认领：GrowIndexAsync/SplitIndex 未转写），base.grow_index_if_needed_async 是虚设内核（读 store_index_maxed_out 标志恒 true，无扩容动作）
   - 处理：全链删——trait grow_indexes_if_needed_async、single/multi impl、base.grow_index_if_needed_async、GarnetDatabase.store_index_maxed_out 字段；js/check/ignore 登记；db.md 条 1 实现时随实现重建

### multi 锁/延续门面 3 项

6. multi_database_manager.rs:100 try_get_databases_content_write_lock
   - C# MultiDatabaseManager.cs:881 public 但仅类内自用（TrySwapDatabases:686）；rust swap_impl 已直用 content_lock.write().await
   - 处理：删
7. multi_database_manager.rs:109 try_get_databases_content_read_lock
   - C# :863 public 但仅类内自用（TakeCheckpointAsync:150 / TakeOnDemandCheckpointAsync:232 / TaskCheckpointBasedOnAofSizeLimitAsync:261 / CommitToAofAsync:323）
   - 处理：删门面；C# 各方法持内容读锁的语义在 multi 四个 trait impl 补齐（async_lock 读等待等价 C# 自旋等待，四方法内部均不再获取写锁，无升级死锁）
8. multi_database_manager.rs:163 run_paused_checkpoints_and_release_locks
   - C# :997 私有延续（TakeCheckpointAsync background 路径：对暂停库逐个拍检查点后 resume 并放锁）；rust 门面语义漂移（仅 resume 全部、不拍检查点、无 paused 列表跟踪）且零调用
   - 处理：删；multi take_checkpoint_async 的 pause 互斥域（TryPauseCheckpoints 批量 + multiDbCheckpointingLock）整体缺失记录为待办

### 跳过保留（他待办认领）

9. database_manager_base.rs:355 get_database_keyspace_stats：留给 next/glm.md 条 19（INFO KEYSPACE）；且被 collect_hybrid_log_stats_for_db 内部调用，非全孤儿，不动
10. cache_size_tracker.rs:39 add_read_cache_heap_size：留给 next/glm.md 条 62（ReadCache 开关），不动

### 拒绝的意见点

- 删 wnode/src/task.rs 的 TaskType::IndexAutoGrowTask 枚举臂：拒绝。该枚举是 libs/server/TaskManager/TaskType.cs 的 1:1 转写面（TASK_PLACEMENT_MAPPING 与 get_task_types 测试在册，REPLICA 类别唯一 ALL 成员），非孤儿函数；db.md 条 1 实现扩容时随 StartGenericNodeTasks 等价装配接入

## rust 侧改动点

1. wdatabase/src/single_database_manager.rs：删固有 recover_checkpoint / take_on_demand_checkpoint / task_checkpoint_based_on_aof_size_limit / commit_to_aof / grow_indexes_if_needed，trait impl 内联 base 调用
2. wdatabase/src/i_database_manager.rs：删 trait grow_indexes_if_needed_async
3. wdatabase/src/multi_database_manager.rs：删 try_get_databases_content_write_lock / try_get_databases_content_read_lock / run_paused_checkpoints_and_release_locks / grow_indexes_if_needed_async impl；take_checkpoint_async / take_on_demand_checkpoint_async / task_checkpoint_based_on_aof_size_limit_async / commit_to_aof_async 补 content_lock.read().await 门控（对标 MultiDatabaseManager.cs:150/232/261/323）
4. wdatabase/src/database_manager_base.rs：删 grow_index_if_needed_async
5. wdatabase/src/garnet_database.rs：删 store_index_maxed_out 字段
6. js/check/ignore/server.yml：Databases 段按 check.js 实际输出登记删除映射的 C# 函数

## 验收口径

- ./clippy.sh 零警告（禁 allow）；./test.sh 全过；bun ./js/check.js 无新增缺失
- 生产调用面回归：wnode BGSAVE（garnet_api.rs checkpoint_command_slow）、恢复链（service.rs recover_checkpoint_store / recover_aof）行为不变

## 验证结果

- 分支：w1-wdb-orphans（已合并 dev 后回并主干，worktree 已删）
- 静态检查：./clippy.sh 0 警告（禁 allow）
- 自动化测试：./test.sh 全量通过（wedb 1992 项 + regress 2 项；合并 dev 前复验 1993 项，dev 侧删除 1 项后为 1992）
- 检查脚本：bun ./js/check.js 输出 0 缺失 0 重复（删除映射在 js/check/ignore/server.yml Databases 段登记：GrowIndexesIfNeededAsync / GrowIndexIfNeededAsync / TryGetDatabasesContentReadLock / TryGetDatabasesContentWriteLock / RunPausedCheckpointsAndReleaseLocksAsync）
- 实际改动：wdatabase 5 文件（single -75 行、multi -49 行净缩），生产调用面（wnode BGSAVE checkpoint_command_slow、恢复链 recover_checkpoint_store / recover_aof）行为不变
- 遗留（他域待办）：TaskManager 生产装配域整体未启用（对标 StoreWrapper.StartPrimaryTasks 全家）；复制面 OnDemandCheckpoint 分支未转写（server/ 域）；COMMITAOF 命令空壳在 wnode/resp/admin_commands.rs（resp/ 域，并发代理认领）
