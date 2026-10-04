终态注记（2026-09-30 执行席收口，合入 5dc732c，分支 fix-repl-pin --no-ff 合入 dev）：收口形态为泵扫描臂单点判别——pump_backlog 逐驱动扫描臂对 task_ref(0) 增设 wire 在位判别（AofSyncTask 新增 has_wire 直读 wire 槽），无 wire 钉线驱动（diskbased 臂 send_checkpoint_and_recover 预锁与 diskless 臂扇出前批量入库的 new(…, None) 形态）跳过本轮扫描保持休眠，对标 C# 钉线任务唯一消费泵 RunAofSyncTaskAsync 仅由 TryConnectToReplica 启动的休眠契约；consume 记账分支内部语义与内联单测未动，attach_stream_driver 先入库后接线次序不变（窄窗危害随判别消除），diskbased/diskless 两臂共用泵体单点收口不另设分支。回归 tests/aof_pump_wireless_pin_dormancy 双则通过：三位点休眠+置换真驱动续推、钉线窗口内 safe_truncate_aof 钳制不越 pin_start；定向单测（aof_sync_task/aof_sync_driver/aof_replication_pump 内联 + replica_sync_pin_release/aof_pump_scan_error_eviction 等既有集成）全绿，cargo check 无警告。

甄别结论:通过(P1,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-B，P1 级）。推流泵 pump_backlog 对无 wire 钉线驱动按记账形态空推积压位点至 safe_tail 事实确证，击穿 safe_truncate_aof 截断线钳制导致副本所需 WAL 段被物理删毁、主从发散。执行席遵照：pump_backlog 扫描臂增设 wire 槽在位判别，无 wire 钉线驱动跳过本轮扫描保持休眠，严禁改动 consume 记账分支内部语义与内联单测。

原票面：
推流泵对无 wire 钉线驱动按记账形态空推积压位点，主端 AOF 截断线钉制失效

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# 在快照下发前以覆盖位点入库钉线驱动防截断。diskbased 臂 ReplicaSyncSession.AcquireCheckpointEntryAsync 源注自陈 "Enqueue AOF sync task with startAofAddress to prevent future AOF truncations"，在获取检查点条目后、快照传送前 TryAddReplicationDriver(startAofAddress) 入库；diskless 臂 PrimarySync.PrepareForSyncAsync pauseAofTruncation 段以当下 Log.BeginAddress 批量入库钉线。C# 钉线驱动的 AofSyncTask 自构造即持 garnetClient 但迭代器 iter 为 null，唯一消费泵 RunAofSyncTaskAsync（BulkConsumeAllAsync）仅由 startAofSync 段 TryConnectToReplica 启动，钉线窗口内任务休眠：previousAddress 恒钉起始位点，SafeTruncateAof 以全部活跃驱动 previousAddress 取小钳制截断线，传送窗内安全截断无从越过覆盖位。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）：rust 推流泵 pump_backlog 遍历在册全部驱动逐任务 consume，无「任务是否已挂发送通道」判别；AofSyncTask::consume 在 wire 为 None 时走「未注入 wire 的记账形态」分支，previous_address、shipped_watermark_address（经 ratchet_shipped）、accepted_address 三位点照常推进且返回 Ok。而钉线驱动构造后全程不挂 wire：diskbased 臂 send_checkpoint_and_recover 以 AofSyncDriver::new(…, None) 构造 pin_driver，仅 try_add_replication_driver 入库，整个快照传送（send_store_checkpoint 段流 + BEGIN_REPLICA_RECOVER 往返，秒到分钟级）期间无 wire；diskless 臂 replication_sync_manager 扇出前 try_add_replication_drivers 批量入库的钉线驱动同样 new(…, None) 无 wire，横跨整个 run_snapshot_fanout。只要推流唤醒循环已激活（本进程任一副本曾成功 attach 后 wal 复制唤醒信号常驻不撤）或有并发 attach 调 sync_backlog，写入负载下 pump_backlog 即对钉线驱动自 pin_start 起以记账形态空推至 safe_tail_address，记录帧一条未发而三位点全部推满。
3. 逻辑危害确证（并发/数据丢失/资源实际危害）：其一，钉线失效——pin 驱动 previous_address 被空推至日志尾，safe_truncate_aof 的 min_aof_address_from_active_sync_tasks 取小钳制失效，传送窗内任一并发检查点完成（add_new_checkpoint_entry 经 safe_truncate_aof）即把 truncated_until 推进到新覆盖位并物理删段，副本尚需的 [pin_start, 新覆盖位) 段被删：data_loss_check 或 start_gate_ok 拒绝致整轮全量传送作废（attach 失败重灌，allow_data_loss=false），或带损放行下 wal.scan 起点被 from.max(begin) 静默截断、副本静默漏段主从发散（allow_data_loss=true）。其二，背压闸门反向松绑——记账分支 ratchet_shipped 把未发送数据计入落网水位，publish 链把虚高水位写入闸门，传送窗内对未送达副本放行追加，与钉线「收紧各子日志最小已发水位」设计意图相反。其三，attach_stream_driver 先 try_add 后 attach_wire 的窄窗内新真驱动同样可被空推，后续 sync_backlog 自已被抬高的 accepted_address 起扫，[sync_start, 尾) 永不补发（主端视为已发、副本停钉在恢复位点等待帧头衔接）。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/aof_replication_pump.rs:pump_backlog（逐驱动扫描臂缺 wire 在位判别）
wedb/wedb/src/server/replication/aof_sync_task.rs:AofSyncTask::consume（wire None 记账推进分支）
wedb/wedb/src/server/replication/aof_sync_driver.rs:AofSyncDriverStore::safe_truncate_aof、min_aof_address_from_active_sync_tasks（被污染的取小钳制源）
wedb/wedb/src/server/replication/replica_sync_session.rs:send_checkpoint_and_recover、attach_stream_driver
wedb/wedb/src/server/replication/diskless_replication/replication_sync_manager.rs:ReplicationSyncManager 扇出前钉线批量入库段
对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:AcquireCheckpointEntryAsync（源注 Enqueue AOF sync task with startAofAddress to prevent future AOF truncations）
garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:AofSyncTask、AofSyncTask.RunAofSyncTaskAsync（唯一消费泵，钉线窗口不启动故任务休眠）
garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:SafeTruncateAof（活跃驱动 previousAddress 取小钳制截断线）
garnet/libs/cluster/Server/Replication/PrimaryOps/PrimarySync.cs:PrepareForSyncAsync（pauseAofTruncation 钉线段）

精炼执行方案：
1. pump_backlog 逐驱动扫描臂增设推流就绪判别：驱动任务未挂发送通道（AofSyncTask wire 为 None）即跳过该驱动本轮扫描，钉线驱动保持休眠直到被带 wire 的真驱动原地置换；判别位可由 AofSyncTask 直读 wire 槽，严禁引入第二套泵、严禁改动 consume 记账分支语义（内联单测与位面观测依赖该分支）。
2. attach_stream_driver 先入库后接线次序保持不变，判别落实后窄窗危害随之消除；diskbased 与 diskless 两臂共用泵体单点收口，不另设分支。
3. 测试验证点：注册无 wire 钉线驱动并写入积压，断言泵轮转后 pin 驱动 previous_address、shipped_watermark_address、accepted_address 三位点不动且后挂真驱动照常推流；补「钉线窗口 + 并发 safe_truncate_aof」场景断言 truncated_until 不越过 pin_start；运行 ./test.sh 全量回归。
