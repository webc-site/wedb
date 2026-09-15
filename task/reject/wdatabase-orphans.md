# wdatabase-orphans 拒绝记录

来源：next/design.md 条 6（wdatabase 面零引用孤儿 10 项）甄别中被拒绝的意见点，执行结果见 task/done/wdatabase-orphans.md。

1. 意见：checkpoint 策略族若 C# 生产在用则补接线到 wnode/wedb 装配点
   拒绝（部分）：task_checkpoint_based_on_aof_size_limit 与 take_on_demand_checkpoint 两项未做装配接线。
   原因与证据：
   - task_checkpoint_based_on_aof_size_limit（C# 链：StoreWrapper.cs:959 StartPrimaryTasks 按 AofSizeLimit 配置注册 AofSizeLimitTask → AutoCheckpointBasedOnAofSizeLimitAsync 周期轮询）：
     rust 配置槽 aof-size-limit 在 wconf/src/runtime_server_config.rs:398 登记 ConfigMeta::read_only，且 wnode/src/service.rs:753 以 RuntimeServerOptions::default() 构造（aof_size_limit = "" 恒值）——功能处于「永远关闭」态；wnode/src/task.rs 的 TaskManager 生产零注册（register_and_run 仅 wedb_standalone/tests/task_manager_tests.rs 使用）。此刻接线 = 读恒关配置的空转任务循环 = 占位假实现，违反「禁止写死/占位」红线。与 next/db.md 条 2（AOF 回放漂移：机制在、配置门不在）完全同型，应作为独立待办（对标 StoreWrapper.StartPrimaryTasks 全家任务装配，server.yml 已有 StartPrimaryTasks/AutoCheckpointBasedOnAofSizeLimitAsync 的 ignore 先例）。
   - take_on_demand_checkpoint（C# 链：ReplicaSyncSession.cs:292 副本磁盘同步 OnDemandCheckpoint 分支，主端 checkpoint 不覆盖 AOF 截断位点时按需补拍）：rust 复制面 wedb/src/server/replication/replica_sync_session.rs 仅转写 SendCheckpointAsync 起段（AOF 流式同步），该分支未转写，且 replication 属 server/ 目录并发代理域，本任务禁入。语义内核已保留（base.take_on_demand_checkpoint_async + trait 面 1:1），复制面转写时直接调用。
   - recover_checkpoint 与 commit_to_aof 两项接受原意见精神但不走「装配接线」：二者 rust 等价覆盖已存在（service.rs:595 recover_checkpoint_store 直调 base 需接管恢复出的 store 句柄；AOF 提交由同步 group commit 等价覆盖，wnode/config_owner.rs:24-31 留档在案），处理为删零调用固有转发层。

2. 意见：连带删 wnode/src/task.rs 里 IndexAutoGrowTask 空枚举臂
   拒绝：TaskType 是 libs/server/TaskManager/TaskType.cs 的 1:1 转写面，非孤儿函数——TASK_PLACEMENT_MAPPING（wnode/src/task.rs:47）含其位次，placement_mapping_matches_csharp 等三个测试断言在册，且它是 REPLICA 放置类别唯一 ALL 成员（get_task_types(REPLICA) 断言依赖）。删它破坏 C# 对齐并需同步改测试，db.md 条 1（GrowIndexAsync/SplitIndex）实现时随 StartGenericNodeTasks 等价装配自然接入。

3. 意见：grow_indexes_if_needed 若 C# 生产在用则补接线
   拒绝接线、改为删除：C# 链（StoreWrapper.cs:1067 StartGenericNodeTasks → IndexAutoGrowTaskAsync:810）虽在用，但 rust 底层在线扩容本体缺失（next/db.md 条 1 已认领 GrowIndexAsync/SplitIndex 未转写），base.grow_index_if_needed_async 是读 store_index_maxed_out 标志恒返回 true 的虚设内核——接线即虚设链路，违反「禁写死、禁占位」红线。全链删除并在 js/check/ignore/server.yml 登记，db.md 条 1 落地时重建。
