# 副本重放窗口丢失修复：重连重放起点回卷至 applied 位点

裁决：接受。票面核心主张成立，但闸门子主张不成立（见甄别），
启动闸无需改动。

## 甄别结论（双侧证据）

成立部分：

1. 重放任务死亡永久，applied 冻结。rust：replica_replay_task.rs
   run_replay_loop processor Err 即 warn 后 break，任务退出，
   background 槽位仅 dispose 清（replica_replay_driver.rs dispose）。
   C# 同形：ReplicaReplayDriver.cs:295 replayIterator == null 门控，
   BackgroundReplayTaskAsync catch 记警告终止，replayIterator 不复位。
2. 重连 resync 起点不消费 applied。rust：assembly.rs recover_replication
   上报 aof_tail = wal.tail_address()（enqueued）；
   replication_manager.rs negotiate_resync 取
   replay_until = min(rep_tail, committed)，与 C#
   GarnetAppendOnlyFile.cs:ComputeAofSyncReplayAddress 逐行同形。
3. 后果差异（rust 独有丢窗）：C# attach 期重建存储——
   ReplicaDiskbasedSync.cs ReplicaSyncAttachTaskAsync 先
   storeWrapper.Reset()，TryReplicaDiskbasedRecovery 再
   storeWrapper.ReplayAOF(replayUntil)（MultiDatabaseManager.cs:456）
   把本地 AOF 补放到协商位点，[applied, replayUntil) 在恢复期重放，
   随后从该位点续流，无窗可丢。rust 引擎不随 attach 重构
   （replica_diskbased_sync.rs 模块文档登记 replayAOFMap 恒 0 的
   架构边界），新重放任务从主端授予的 sync_start（= 推流首帧
   previous_address，aof_sync_task.rs previous 初值即 start_address）
   起放，[applied, sync_start) 只存在于本地 wal、永不入存储，
   主从静默发散。且不止死亡场景：断连时重放正常滞后
   （aof-replay-max-lag-bytes 允许）同样丢窗。

不成立部分（闸门子主张，不改）：

「死亡后合法重入被 background.is_some() 闸吞」不成立。全部重连
路径先清仓库再注册新驱动：assembly.rs:149 recover_replication 开头
reset_replica_replay_driver_store；cluster_replication_session.rs
dispose 断链时 has_active_replication_stream 即 reset；
DriverRegistry::reset 逐驱动 dispose（background 槽位随之清空）。
连接存活期间无 init 帧重入，闸从不被咨询。C# 闸同形且同寿
（ResetReplicaReplayDriverStore 在每次 attach 开头）。

## 修法：选案 1，落地为重放起点回卷（不取案 2）

重连首帧启动背景重放时，起点取 min(previous_address, applied 位点)
（cluster_replication_session.rs process_primary_stream 的
initialize_background_replay_task 调用点）。applied 位点即存储真实
应用水位（replication_manager set/get_sublog_replication_offset，
重放链批终点权威回推），回卷后既有重放任务按既有扫描逻辑先补放
本地 wal 窗口 [applied, sync_start)，随后与入流帧无缝衔接
（推流帧落盘与本地扫描同一地址空间）。这正是 C# attach 期
ReplayAOF(replayUntil) 的 rust 对位：C# 用一次性恢复重放补窗，
rust 用常驻重放任务改起点补窗，终点状态一致。

选案 1 理由：

1. 单机制：复用既有重放扫描链，不新增第二套数据通路；案 2 需重放
   线程到会话的跨线程 fatal 通道 + 全量检查点传输，复杂度高且
   C# 部分重同步同样靠本地补窗而非全量。
2. 顺带修复常态滞后断连丢窗（不限死亡场景），案 2 不覆盖。
3. 不动协议、不动协商、不截断本地日志（票面案 1 的 safe_initialize
   丢弃尾段不需要——窗口本就在本地日志内，直接补放）。

min 钳制两向安全：init 帧时机 applied ≤ previous 恒成立（previous =
sync_start = 断连时日志尾，applied 为其应用水位；applied > previous
的场景在首帧 divergence 校验 tail == current_address 处已断流），
min 退化为恒等；FastAofTruncate 跳跃帧 prior 分支 previous < applied
（跳跃点即重对齐后的 offset），min 取 previous，扫描起点 clamp 至
日志 begin，行为与现状一致。

前置静默化：旧任务死后可能仍在跑最后一批（dispose 置 stop 后循环
顶才复查），若新任务采样 applied 后旧任务批末再推位点，回卷起点
过期导致窗口重复应用。故 BackgroundReplay 增持 JoinHandle，
dispose 置 stop 后 join（上界一个批次窗口），使 reset 路径
（断链 dispose / recover_replication 开头）先于新驱动注册静默旧
应用者，采样点必然读到底值。单子日志装配下 join 串行一次。

## 改动点

1. wedb/wedb/src/server/replication/cluster_replication_session.rs：
   process_primary_stream 背景重放启动处回卷起点（中文注释注明
   C# ReplayAOF 映射与两向钳制论证）。
2. wedb/wedb/src/server/replication/replica_replay_driver.rs：
   BackgroundReplay 挂 JoinHandle，dispose join 静默旧写者。
3. wedb/wedb/src/server/replication/replica_replay_task.rs：spawn
   返回 JoinHandle 供驱动持有。
4. 定向测试 wedb/wedb/tests/replica_background_replay.rs：
   单错注入 → 任务死 → 重连 → 断言 [applied, tail) 被补放。
   注入形态：信封键预置标签匹配而 blob 损坏的载荷（HashObject::
   from_blob 返回 None → object_store_rmw 上抛 corrupted envelope，
   aof_processor_object_replay.rs replay_object_channel），重放任务
   必死且槽位驻留；随后推普通记录构造 [applied, tail) 窗。重连前
   删信封键（错误条件消失，瞬时单错语义，无需字节修补——扫描
   内存环优先，设备回写不可见）；重连 init 帧 + 首记录帧
   previous = 当前尾位（主端授予 sync_start 形态）。断言：窗内
   记录应用进存储（旧实现从 sync_start 起放必丢，红相可辨）、
   applied 追平、窗后记录与重连新记录连续收敛。

## 验收

cargo check -p wedb -p wnode 零 error 零 warning；定向测试通过。
门禁：只跑 cargo check 与定向测试，不跑 test.sh / clippy.sh。
