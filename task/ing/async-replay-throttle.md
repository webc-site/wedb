# 异步重放与 ThrottlePrimary 接线（async-replay-throttle）

来源：review 待办「异步重放模式与 ThrottlePrimary 未接线」。直接输入：
task/done/replica-offset-semantics.md（enqueued 语义收敛路径 = 应用完成后经
ReplicaReplayDriver 权威面回推位点）与 task/done/repl-recover-wiring.md。

## 对标核实

C# 全链（ReplicaReplaySession.cs:26-131 + ReplicaReplayDriver.cs）：

1. syncReplay = (AofReplayMaxLagBytes == 0)，默认 -1 → 默认走异步路径。
2. 异步路径（ProcessPrimaryStream else 分支）：enqueue 后
   InitializeBackgroundReplayTask(previousAddress)（幂等，仅首帧启动：
   ScanSingle 从 startAddress 起迭代器 + BackgroundReplayTaskAsync 常驻
   BulkConsumeAllAsync，REPLICA_SYNC_DELAY 空转等待新数据）+
   ThrottlePrimary（maxLag != -1 且迭代器在场且 tail - replicationOffset >
   maxLag 时自旋让渡）。会话线程在异步路径不推进位点——位点 100% 由重放
   链应用后推进（Consume → ConsumeDirect：起点 SetSublogReplicationOffset
   (currentAddress)，ProcessAofRecordInternal 应用进存储，终点推进
   replicationOffset；检查点起始标记记 ReplicationCheckpointStartOffset）。
3. 同步路径（maxLag == 0）：会话线程 ResumeReplay 持重放权 → enqueue →
   Consume 内联应用 → Throttle（TryApplyPendingPulse）→ SuspendReplay；
   enqueue 前有位点追平校验（offset != tail 即断流）。
4. 时间脉冲（SignalTimeAdvance）：pending 原子记录；会话线程持有重放权
   （maxLag == 0 或迭代器未启动）时就地 TryApplyPendingPulse，否则由背景
   重放循环每轮 Throttle 调用消化；守卫 pending > applied 且
   GetSublogReplicationOffset == TailAddress（追平才推进时间）；
   ApplyPulse → readConsistencyManager.AdvanceVirtualSublogTime。
   主端发送面 AofSyncTask.SendAdvanceTimePulse 经
   ExecuteClusterAdvanceTime 带(CLUSTER ADVANCE_TIME)内联送达副本。

rust 现状（全部核实）：

1. replica_replay_driver.rs should_throttle_primary 仅测试引用；
   signal_time_advance 无条件 pending=applied=max（无守卫、无读一致时间
   下游）；consume_direct 空回调。
2. cluster_session.rs ADVANCE_TIME 臂 → driver.signal_time_advance 下游
   无真实消费者；aof_sync_task.send_advance_time_pulse 仅记原子不发送
   （主端发送面网络化不在本任务范围，接收面消费必须闭环）。
3. advance_virtual_sublog_time（ReadConsistencyManager）已转写，无副本
   运行期调用方。
4. 应用链唯一调用方在恢复链（recover_log_driver →
   process_aof_record_internal），副本运行期无应用。
5. 配置面：wconf aof_replay_max_lag_bytes（默认 -1）与
   cluster_provider AtomicI32 + setter + INFO 读已在；main.rs 未注入
   （恒 -1），getter 缺。
6. C# ReplicaReplayTask.cs 页级双闸栏并行重放（AofReplayTaskCount > 1）
   在 ignore 登记为「compio 异步事件流式驱动折叠」，与
   recover_log_driver 模块头「页级双闸栏并行化折叠为顺序消费」先例一致。

## 甄别结论：真实现（拒绝配置面禁用）

背景重放 + 主端节流是 enqueued 位点语义的收敛路径，也是副本数据面
（存储应用）的唯一运行期入口，禁用即永久保留位点虚假窗口。四个架构折叠
（对标差异全部登记注释）：

1. 背景重放跑专用 OS 线程 + 自有 compio runtime（先例：waof_sublog
   commit 的线程回退）。原因：wnode 线程每核模型下每个 worker 单线程
   runtime，会话消费面 MessageConsumerFace::try_consume_messages_into 是
   同步 trait——在会话线程内联驱动异步存储应用不可行，且同运行时任务
   会被同步阻塞饿死；独立线程使 ThrottlePrimary 阻塞等位期间重放照常
   推进。
2. ThrottlePrimary 的 Thread.Yield 自旋折叠为 Condvar 通知 + 超时兜底
   等待（自旋在单线程 runtime 会饿死重放任务必死锁；等待语义等价，零
   CPU 空转）。解锁条件：追平 / 处置（dispose）/ 已追平越 maxLag。
3. C# 同步形态（maxLag == 0）会话线程内联 ConsumeDirect 折叠为「背景
   重放线程 + maxLag=0 阻塞至追平」：每帧 enqueue 后等 applied 追平
   tail（先应用后应答下一帧，锁步语义等价）；C# enqueue 前位点校验被
   追平等待覆盖（顺序扫描保证前帧先应用）。
4. AofReplayTaskCount > 1 页级并行维持既有 ignore 登记的顺序消费折叠，
   Consume 分发与 ConsumeSchedulePage 不转写。

位点语义切换（本任务核心）：

1. 背景重放任务在场（资产已注入）：会话不再直推位点，由重放链应用后
   按 ConsumeDirect 语义推进（批起点 currentAddress → 应用 → 批终点），
   位点切回 C# applied。
2. 退化形态（无资产：测试装配 / 未接 store+aof）：initialize 幂等空转、
   throttle 不等、会话保持现状 enqueued 直推。理由：replica-offset-
   semantics.md 方向 A 拒绝链——无挂点强推 applied 会使位点恒停初值，
   checkpoint covered、failover 安全线、gossip 位点上告全部退化。

## 改动点

1. 新模块 wedb/wedb/src/server/replication/replica_replay_task.rs
   - ReplayAssets：aof（GarnetAppendOnlyFile，扫描/tail/读一致时间源）+
     store（WedbStore，重放应用目标）+ 长生命周期 AofProcessor（事务/
     分块/模糊区状态跨记录，对标 C# rm 构造期 recordToAof:false 的
     aofProcessor；含 RangeIndexManagerReplication 注入）。
   - 背景重放线程体：InitializeBackgroundReplayTask /
     BackgroundReplayTaskAsync / ConsumeDirect 转写——扫描 [applied,
     tail] 窗口（1MB chunk 对标 maxChunkSize）→ 批起点推进位点 → 逐
     记录 process_aof_record_internal → 检查点起始标记记
     set_sublog_checkpoint_start_offset → 批终点推进位点 + 通知节流
     等待者 → 每轮 throttle()（脉冲消化）→ 空转 REPLICA_SYNC_DELAY。
     store.new_session 独占纪元参与者 + pause_aof_listeners 常驻
     （= recordToAof:false：重放应用不镜像回写本地 wal）。
2. replica_replay_driver.rs 重写
   - 所有权面拆分：is_active（DriverLifecycle：创建→dispose）与
     replay_owner（C# activeWorkerMonitor TryEnter/Exit 语义）。
   - 字段：assets（Option<Arc<ReplayAssets>>）+ Weak<ReplicationManager>
     （杜绝 rm→store→driver→rm 强环）+ 背景任务面
     Mutex<Option<BackgroundReplay>>（stop/running/Condvar）。
   - initialize_background_replay_task：幂等，无资产空转（退化登记）。
   - throttle_primary(max_lag)：maxLag==-1 直通 / 背景未启动直通 /
     lag<=maxLag 直通，否则 Condvar 等待（dispose 可打断）。
   - signal_time_advance 对标改写：pending 单调守卫 + 会话持有权重判定
     + try_apply_pending_pulse；throttle()（C# Throttle）/
     try_apply_pending_pulse / apply_pulse（读一致时间推进，
     advance_virtual_sublog_time）补全，追平守卫对标。
   - 删 should_throttle_primary（ThrottlePrimary 映射迁至
     throttle_primary）、consume_direct（ConsumeDirect 映射迁至背景
     模块消费体）、applied_pulse_sequence_number getter（伪造映射，
     C# 无此公共成员；字段保留）。
3. replica_replay_driver_store.rs：add 带 (assets, weak rm)，容器关闭
   返回 None（修复关闭后孤儿驱动）。
4. replication_manager.rs：set_replay_assets 注入点（对标 C# rm 构造期
   aofProcessor，rust 装配期差异同 set_commit_channel 先例）；
   initialize_replica_replay_driver 传资产；set/get_sublog_replication_
   offset 注释更新（applied 回推路径落地）。
5. cluster_replication_session.rs：process_primary_stream 重放分支改写
   （初始化背景任务 + throttle + 条件位点推进）；文档注释 enqueued
   语义段改写为「背景重放在场 applied / 退化 enqueued」双形态。
6. cluster_provider.rs：aof_replay_max_lag_bytes() getter。
7. main.rs：装配链注入 aof_replay_max_lag_bytes（配置面最小接线）。
8. assembly.rs：wire_replication_data_plane 构建 ReplayAssets（try_aof +
   try_store 双在场才建，对标 C# EnableAOF 门控）注入 rm。
9. js/check/ignore/cluster.yml：ReplicaReplayDriver.cs 登记项移除已实现
   （InitializeBackgroundReplayTask、BackgroundReplayTaskAsync、Throttle、
   TryApplyPendingPulse、ApplyPulse、Dispose）；保留折叠项并更新理由
   （Consume、ConsumeSchedulePage、ValidateSublogIndex）。

## 测试

1. replica_replay_driver.rs 单测重写：所有权面、pulse 守卫（未追平不
   应用 / 追平应用 / pending 单调）、throttle_primary 门控真值。
2. 新集成测试（wedb/tests/replica_background_replay.rs，复用复制测试
   基建：SegmentedDevice wal + 单日志 aof + open_test_store）：
   - 真实 AOF 条目（RecordShape 编码）经 process_append_log 落盘 →
     背景重放应用进 store → 读回断言（存储应用闭环）。
   - 位点追平：applied 位点随应用推进至 tail（水位追平）。
   - 主端节流：max_lag 收紧后 process_append_log 返回时位点已追平
     （节流生效，滞后触发等待）。
   - 滞后触发：首帧触发背景任务启动（幂等：次帧不重复启动）。
   - ADVANCE_TIME：追平后 signal_time_advance 推进读一致时间
     （advance_virtual_sublog_time 下游生效）。
3. 既有复制链路测试全数保持通过（退化形态行为不变）。

## 验收口径

1. ./clippy.sh 零警告（禁 allow）。
2. ./test.sh 全过。
3. bun ./js/check.js 无新增缺失/重复。
4. 行为口径：资产在场时位点 = 应用进度（applied，收敛 replica-offset-
   semantics.md 登记路径）；退化形态行为与现状逐字节一致。

## 边界与冲突

- 主端 ADVANCE_TIME 发送面网络化（ExecuteClusterAdvanceTime 带内发送）
  不在本次范围（aof_sync_task 仅记原子），接收面消费闭环后主端发送面
  为独立待办。
- AofReplayTaskCount > 1 页级并行维持折叠登记。
- 并发代理可能刚改 replication_manager.rs / cluster_session.rs，冲突以
  先合并者为准。
