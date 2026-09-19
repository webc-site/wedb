副本重放 CheckpointEndCommit 臂补本地拍检查点分支：副本 WAL 只增不减、无本地检查点基线

来源：next/glm.db.md 条 1 立项（该文件本波剪空删除）。取证基线：主仓 /Users/z/git/db/wedb
分支 dev，行号按当下 HEAD 的符号重取。判定：成立且待做。

结论一句话
C# 在 AOF 重放遇到检查点结束标记且为主端新版本时，副本同步拍一次本地检查点，拍完由检查点
完成回调把本地 AOF 物理截断到先前在检查点起始标记处记下的位点。rust 的重放臂只做了模糊区
收尾，拍检查点这一触发面整链缺失：上游记账面与下游截断面都在位，中间那一环没人调，副本本地
wal 从建立到断链只增不减，副本重启也没有本地检查点基线可用，只能全量重放本地 wal 再 attach。

现状（主仓 HEAD 实测）
1. 缺口臂本体：/Users/z/git/db/wedb/wedb/wnode/src/aof/aof_processor.rs:533-554
   AofEntryType::CheckpointEndCommit 分支体内只有 set_in_fuzzy_region(false)、
   process_fuzzy_region_operations、clear_fuzzy_region_buffer 三件事，无任何拍检查点调用，
   也没有 header.store_version 与 target.store_version 的比较（该文件的
   AofRecordHeader::store_version 声明在 :101，取源于 :110 store.current_version()）。
   起始标记臂 :501-532 仅置模糊区与虚拟子日志序号，与 C# 同形。
2. 上游记账面在位：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_replay_task.rs:179-190
   重放循环对每条记录调 process_aof_record_internal（as_replica 恒 true），返回
   is_checkpoint_start 即 rm_ref.set_sublog_checkpoint_start_offset。记账内核
   /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replication_manager.rs:470
   get_replication_checkpoint_start_offset 与 :480 set_sublog_checkpoint_start_offset。
3. 下游截断面在位但无人触发：/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs:1243-1256
   on_checkpoint_initiated 副本角色即取该起始位点作为 covered；:1262-1285
   add_new_checkpoint_entry 登记 CheckpointMetadata 历史后转 :1199-1214 safe_truncate_aof，
   副本角色走 aof.log().truncate_until_async。二者全仓生产调用只有
   cluster_provider.rs:1047 take_on_demand_checkpoint（:1070 调 add_new_checkpoint_entry），
   而该入口唯一消费点是副本 attach 链
   /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_sync_session.rs:303。
   on_checkpoint_initiated 本身全仓生产零调用（仅 wedb/wedb/tests 三处用例）。
4. 副本推流落盘面无截断形态：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/cluster_replication_session.rs:267-285
   fast_aof_truncate 命中跳跃时只做 wal.safe_initialize 地址空间重对齐，无物理回收。
5. 现存的拍检查点触发口全在主库侧：/Users/z/git/db/wedb/wedb/wnode/src/database/database_manager_base.rs:186
   take_database_checkpoint_async，生产调用为 SAVE/BGSAVE
   （/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:1027、:1038）与 AOF 体积超限
   周期任务（/Users/z/git/db/wedb/wedb/wnode/src/service.rs:518 spawn_aof_size_limit_task →
   /Users/z/git/db/wedb/wedb/wnode/src/database/single_database_manager.rs:216
   checkpoint_within_pause_gate，该闸 :221 副本角色直接轮空）。副本重放链没有任何入口。

C# 参考
/Users/z/git/db/wedb/garnet/libs/server/AOF/AofProcessor.cs:291-326（CheckpointEndCommit 臂：
asReplica && header.storeVersion > storeWrapper.store.CurrentVersion 时，非 sharded 直接
AsyncUtils.BlockingWait(storeWrapper.TakeCheckpointAsync)，sharded 经
ProcessSynchronizedOperation(LeaderBarrierType.CHECKPOINT) 栅栏拍；拍后才
ProcessFuzzyRegionOperations）；完成回调链
/Users/z/git/db/wedb/garnet/libs/server/Databases/DatabaseManagerBase.cs:528-535（EnableCluster &&
EnableAOF 时 AddNewCheckpointEntry）；副本截断分支
/Users/z/git/db/wedb/garnet/libs/cluster/Server/ClusterProvider.cs:170-186 SafeTruncateAOF；
主端标记写入面对照
/Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/ReplicationManager.cs:328-343
CheckpointVersionShiftStart/End（rust 对位已通：cluster_provider.rs:1684/:1697 由
database_manager_base.rs:202、:230 驱动）。

修法
在 aof_processor.rs 的 CheckpointEndCommit 臂内、process_fuzzy_region_operations 之前（与 C#
次序一致），补 as_replica && header.store_version > target.store_version 判定分支，经现有拍摄
链触发一次：wnode 侧取 SingleDatabaseManager::take_checkpoint（
/Users/z/git/db/wedb/wedb/wnode/src/database/single_database_manager.rs:204，内核即
take_database_checkpoint_async），或按重放任务的宿主注入回调（AofProcessor 现不持数据库管理器
句柄，接线形态随宿主侧最小改动选一并一处定源）。多回放拓扑下对齐 C#：经现有
LeaderBarrierType 栅栏（record_gate / aof_replay_coordinator 已承接 ProcessSynchronizedOperation
形态，同臂 FlushAll 分支即先例）让 Leader 独占拍。拍完必须走通既有回调面：本臂的
covered 由 on_checkpoint_initiated 取副本起始位点、add_new_checkpoint_entry 完成登记与截断，
不要另起第二套截断路径。同时确认 store.CurrentVersion 随新 token 推进
（database_manager_base.rs:204 已在内核内做，重放臂不重复推版本）。

边界
与 task/ing/resync-strategy-store-version.md（该票管 diskless/diskbased 两个恢复体一次性
SetVersion 收敛缺失，且 rust 侧连 MainStoreStreamingCheckpointStartCommit 臂都尚未存在）不同面：
本票管稳态重放臂的持续拍检查点与截断动作，修完那条本条问题仍在。
与 task/ing/primary-checkpoint-cluster-callback.md（同波新立，管主库检查点完成回调的集群分支）
共用 add_new_checkpoint_entry/on_checkpoint_initiated 这一对回调面，两单合起来才把主从两侧的
触发接齐，实施次序建议先主后副，避免同一回调面改两遍。
与 task/ing/aof-driver-register-pre-transfer.md（副本同步流的驱动注册时机）不重叠。

优先级
功能缺口（副本磁盘占用单调增长 + 副本重启无本地基线，集群长期运行必现），中档偏上。

盘点补记（qw13.invA replica-replay-checkpoint-end-arm）：dev e75716e 复核原样：aof_processor.rs:522 CheckpointEndCommit 臂仍只有 set_in_fuzzy_region(false) + process_fuzzy_region_operations + clear_fuzzy_region_buffer 三件事，无 store_version 比较、无本地拍检查点、无回调截断链。上游主端标记链已就位（checkpoint_version_shift_start/end 落地），本票可派性提高。
