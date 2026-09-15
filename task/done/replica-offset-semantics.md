# 副本 AOF 位点推进语义甄别与登记（enqueued vs applied）

来源：next/ds.net.md 条 3 与 next/net.md 条 2（主代理预清理，两文件同一问题）。

## 对标核实

C# 位点推进的全部位置（grep 全仓核实）：

1. ReplicaReplayDriver.cs:ConsumeDirect（164/204/211 行）：进 consume 先
   SetSublogReplicationOffset(currentAddress) 起点标记，循环内
   ProcessAofRecordInternal 把记录应用进存储，结束后
   WaitForVectorOperationsToComplete，再 SetSublogReplicationOffset(replicationOffset)
   终点推进。异常路径也按已成功工作量推进位点后上抛。
2. ReplicaReplayDriver.cs:ConsumeSchedulePage（137/148 行）：页发布后记
   currentAddress，栅栏等待全部重放任务应用完成，再推进 nextAddress。
3. ReplicaReplaySession.cs:71：FastAofTruncate 跳过分支 SafeInitialize 快进后
   记 currentAddress。

即 C# 位点推进 100% 在重放（应用）语境，session 的 ProcessPrimaryStream 正常
流从不直接推进位点。位点的权威语义 = applied（已应用进存储）。

C# 同步/异步形态：syncReplay = (AOF_REPLAY_MAX_LAG_BYTES == 0)，默认 -1
（RuntimeServerConfig.cs:166）→ 默认走异步路径
InitializeBackgroundReplayTask + ThrottlePrimary，位点由后台 ReplicaReplayTask
应用后推进。wedb 侧 wconf aof_replay_max_lag_bytes 默认同为 -1。

Rust 现状（wedb/wedb/src/server/replication/cluster_replication_session.rs，
worktree 内路径 wedb/wedb/src/server/replication/）：

process_primary_stream 三步：
1. wal.enqueue_raw(payload) 保真落盘；
2. replay_hook 形态（测试）推进 watermark，生产形态走
   driver.consume_direct(payload, current, next, 空回调)——空回调，无任何
   记录应用；
3. 无条件 rm.set_sublog_replication_offset(idx, next_address)。

位点推进挂在落盘后，语义 = enqueued（流式落盘），非 C# 的 applied。
原注释「重放位点权威面在 replay driver，此处先记流式落盘位点」中前半句
不成立：driver.replayed_offset 是内部记账位点，无任何生产消费方。

## 甄别结论：部分成立，方向 A 拒绝，走方向 B（增强版）

### 方向 A（位点上报改挂「存储应用完成」事件）拒绝的核实链

1. consume_direct 空回调没有下游：既不经 replay coordinator，也不直推存储。
   全仓 process_aof_record_internal 唯一调用方是
   wnode/src/aof/recover/recover_log_driver.rs:82（进程启动恢复重放链）。
   副本运行期没有任何把记录应用进存储的动作。
2. C# 的两个应用形态（lag=0 同步就地应用 ConsumeDirect；默认 lag=-1 后台
   ReplicaReplayTask）在 rust 均未转写。位点回推挂点（存储应用完成事件）
   当前不存在。
3. 实现挂点 = 转写 InitializeBackgroundReplayTask / ReplicaReplayTask
   （async 应用链 + 会话泵 blocked-wait 挂起机制 + 存储面接线；
   process_aof_record_internal 为 async，session 链路为同步泵，需
   take_blocked_wait 机制桥接）。这正是另一条待办「背景重放任务与主端
   节流」的主体，任务指令明确划界不做，且现在做必然与该待办冲突返工。
4. 位点静止化（无挂点却严格执行 applied 语义 → 位点恒停初值）的下游退化：
   checkpoint covered_aof_address（cluster_provider.rs:854）、failover 数据
   安全线（cluster_provider.rs:498/549/604）、gossip 位点上告
   （gossip_manager.rs:78/233）、clusterconfiguration 输出全部退化，
   危害大于现状。

### 方向 B 核实与口径

1. data_loss_check（replication_manager.rs:502，对标
   ReplicaSyncSession.cs:DataLossCheck）消费的是副本发起同步时上报的
   wal begin/tail（assembly.rs:recover_replication 130-131 行，磁盘真实面），
   不消费 replication_offset。enqueued 语义下该口径已自洽，无需调整。
2. enqueued 语义的真实风险窗口（登记为已知风险，非本次消除）：
   - 位点承诺「wal 已落盘」（page cache），不承诺 fsync、不承诺存储应用。
     掉电丢 wal 未刷帧时位点虚假。
   - 副本 checkpoint 的 covered_aof_address 取位点；若后续按位点截断 AOF
     而 wal 未 fsync，checkpoint 存储态未含该增量 → 丢数据。
   - C# TryApplyPendingPulse 守卫（GetSublogReplicationOffset != tail 则
     return）在读一致性时间脉冲面隐式依赖 applied 语义；enqueued 下该守卫
     恒通过。
   - 最终收敛路径：背景重放待办落地后，应用完成后推进位点（经
     replica_replay_driver 权威面回推），位点切回 C# applied 语义。

### 孤儿处置（上一轮特意保留的两个符号）

1. replica_replay_driver.rs:set_replayed_offset：无调用方（死代码），且 C#
   ReplicaReplayDriver 无 ReplayedOffset 成员（全仓 grep 确认），不属于
   C# API 对标面 → 删除。replayed_offset() 的伪造映射注释
   `ReplicaReplayDriver.cs:ReplayedOffset` 一并修正；字段本身保留（内部
   重放记账 + 未来重放任务回推挂点）。
2. replication_manager.rs:get_sublog_replication_offset：C# 真实公共 API
   （ReplicationManager.cs:87，消费方 TryApplyPendingPulse 守卫 /
   syncReplay 校验均属背景重放待办的转写范围）→ 保留，注释登记现状。

## 改动点

1. cluster_replication_session.rs
   - process_primary_stream 文档注释第 4 点改写：位点语义如实登记为
     enqueued，写明与 C# applied 的差异、掉电窗口、最终挂点（背景重放
     待办）。
   - 位点推进行注释修正：删除「重放位点权威面在 replay driver」的错误
     表述。
2. replica_replay_driver.rs
   - 删除 set_replayed_offset 死代码。
   - replayed_offset() 映射注释修正。
   - consume_direct 映射注释修正：写明 C# ConsumeDirect 应用后推进位点，
     rust 直接模式当前无应用链，位点推进语义由调用方（session 落盘面）
     承担。
3. replication_manager.rs
   - set_sublog_replication_offset / get_sublog_replication_offset 注释
     登记推进源现状与 C# 权威语义路径。
4. js/check/ignore：无需新增。删除的 set_replayed_offset 无 C# 映射注释，
   基线 check.js 0 缺失保持。

## 验收口径

1. 行为零变化：本任务不动任何推进时序，只做语义登记与死代码清理；
   cluster_replication.rs / replication_pipeline.rs / replication_end_to_end.rs
   既有断言全部保持通过。
2. ./clippy.sh 零警告（禁 allow）。
3. ./test.sh 全过。
4. bun ./js/check.js 无新增缺失。

## 验证结果

1. 实现形态：行为零变化（语义登记 + 死代码清理），改动三文件
   - wedb/wedb/src/server/replication/cluster_replication_session.rs：
     process_primary_stream 文档与位点推进行注释登记 enqueued 语义、与
     C# applied 的差异、掉电窗口与背景重放待办回推路径；删除「重放位点
     权威面在 replay driver」错误表述。
   - wedb/wedb/src/server/replication/replica_replay_driver.rs：删除
     set_replayed_offset 死代码（无调用方、无 C# 映射）；replayed_offset()
     伪造映射注释 `ReplicaReplayDriver.cs:ReplayedOffset`（C# 无此成员）
     修正为内部记账位点说明；consume_direct 注释登记 enqueued 语义由
     调用方承担。
   - wedb/wedb/src/server/replication/replication_manager.rs：
     set_sublog_replication_offset 登记 C# 推进源（重放链 applied）与
     rust 现推进源（session 落盘面 enqueued）；get_sublog_replication_offset
     保留并登记（C# 真实 API，消费方 TryApplyPendingPulse 守卫与
     syncReplay 校验随背景重放任务转写落地）。
2. js/check/ignore 无需新增：删除的 set_replayed_offset 无 C# 映射注释，
   check.js 保持 0 缺失。
3. ./clippy.sh：0 警告（含修复 doc_lazy_continuation 缩进一处）。
4. ./test.sh：2 passed 0 failed（回归门禁）。
5. 复制链路测试：cluster_replication 5/5、replication_pipeline 5/5、
   replication_end_to_end 2/2 全过，既有位点断言保持通过。
6. bun ./js/check.js：exit 0，无新增缺失。
7. 合并：分支内先 merge dev（无冲突），回主目录 merge w2-replica-offset
   （干净合入，仅 3 个 replication 文件），worktree 与分支已删除。
8. 遗留登记（非本任务范围）：enqueued 位点语义的风险窗口（wal fsync 前
   掉电、checkpoint 截断联动、读一致性时间脉冲守卫恒通过）与最终收敛
   路径（背景重放任务应用后经 driver 回推位点）已写入代码注释，作为
   「背景重放任务与主端节流」待办的输入。
