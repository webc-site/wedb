# zcode-r10-idem 幂等性横向审计

视角:盘点全仓「重试点」,横向核对每条重放体恰一次。
范围:客户端可见重试属 redis 语义不立;内部重试(复制重连重放/迁移重试/检查点重试/gossip 重发/AOF 提交与 fsync 重试)、恢复重放(AOF/检查点)、通知副作用、升降阶在途、CONFIG SET 重入、一次性初始化重入。
r5-repl(offset 部分)、r6-del(墓碑重放)、r2-crash(AOF 重组重放)、r8-sample-b(引擎级)已立不复述;本轮为全入口横向盘点。

立两条。

1 副本背景重放任务死亡后,resync 起点取 enqueued 尾,[applied, tail) 重放体静默丢失

重试点:副本稳态 AOF 重放 → 重放链单错退出 → 断连重连 PartialResync。

重放体:
- wedb/src/server/replication/replica_replay_task.rs:run_replay_loop —— processor Err(条目损坏/存储 IO/store_rmw default 臂不支持命令,wedb/wnode/src/aof/aof_processor_store_ops.rs:store_rmw)即 warn 后 break,任务永久退出;复制位点停在最后成功批尾(applied)
- wedb/src/server/replication/replica_replay_driver.rs:initialize_background_replay_task —— background.is_some() 幂等启动闸:任务死后槽位不清(仅 dispose 清),合法重入被闸吞,死任务不能重生
- wedb/src/server/replication/assembly.rs —— INITIATE_REPLICA_SYNC 上报 aof_tail = 本地 wal 尾(enqueued)
- wedb/src/server/replication/replication_manager.rs:negotiate_resync —— sync_start = min(rep_tail, committed),全程不消费 applied 位点;磁盘链上副本 current_replication_offset 干脆以 rep_tail 回填(wedb/src/server/cluster_session/replication.rs:network_cluster_initiate_replica_sync)
- wedb/src/server/replication/cluster_replication_session.rs:process_primary_stream —— initialize_background_replay_task(首帧 previousAddress),新重放任务从 rep_tail 起扫本地日志

触发链:回放链单错(一条确定性损坏条目即足)→ 任务死 → 副本会话继续入队(本地尾前进、applied 冻结;lag 越闸后经 throttle TCP 背压停流)→ 断连重连 → PartialResync 授 rep_tail → [applied, tail) 段只存在于本地日志、永不入存储 → 主从静默发散。集合域 LPUSH/HINCRBY/ZINCRBY 等 input 重放型条目在此表现为缺效果(非双执行)。

判定:非幂等(重放体丢失,应重放的记录被跳过)+ 可触发(副本磁盘 IO 抖动/磁盘满/任一损坏条目)。

修复方向(二选一):resync 重放起点取 min(rep_tail, applied 位点) 并对本地日志 safe_initialize 丢弃未应用尾段;或背景任务 processor 出错即置 fatal_disconnect 断流,逼 FullResync。

C# 对位:libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:ValidateMetadata 与 libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:InitializeBackgroundReplayTask 同形(1:1 继承);task/done/replica-offset-semantics.md 只登记退化形态 enqueued 位点超前半边,死任务 resync 跳窗半边未登记。

2 向量集迁移上下文预留:失败 recover 无目标端补偿,重试永久泄漏预留登记

重试点:CLUSTER RESERVE VECTOR_SET_CONTEXTS(一次性登记)× 迁移失败重试。

重放体:
- wedb/src/server/sync_transport.rs:transmit_vector_set_frames —— 每次传输首步重新预留
- wedb/wnode/src/resp/vector/vector_manager_context_metadata.rs:reserve_contexts_for_migration —— 纯新分配并标 in-use,全仓无 release/unreserve 原语
- wedb/src/server/migration/migrate_driver/keys.rs:try_recover_from_failure —— 只复位本端槽位与会话状态,不向目标发任何补偿
- wedb/src/server/cluster_session/replication.rs:network_cluster_reserve —— 接收端照单全收

触发链:向量集迁移停等超时/远端拒绝 → recover 判败 → 运维重试迁移 → 目标再次预留新批;上批预留上下文永久 in-use(重试后 import_migrated_index 同名键改指新 context,旧 context 孤儿);u32::MAX 上下文编址上限可被持续重试耗尽,此后 RESERVE 恒错,向量迁移永久不可用。

判定:非幂等(登记无重入保护无补偿)+ 可触发(迁移失败 + 人工重试即现;一次成功无影响)。

修复方向:补预留回滚原语(recover 携带上批 context 清单到目标注销),或 RESERVE 按源键集指纹幂等(同键集复用旧预留)。

C# 对位:libs/server/Resp/Vector/VectorManager.ContextMetadata.cs:ReserveContextsForMigration 同为只分配无释放,同形继承。

3 已核幂等面(横向盘点结论,无增量)

- AOF 恢复重放:版本基线过滤(wedb/wnode/src/aof/record_gate.rs:should_skip_record)+ initialize_if 覆盖位点对齐(wedb/wnode/src/service.rs:open_recovered_with_config_and_aof)+ commit 帧 cookie 收敛上界(waof/src/wal/commit.rs)→ 检查点/AOF 边界恰一次(r2-crash 已立面横向确认一致)
- 恢复并行内核:record_gate.rs:can_replay 哈希归属,一条目仅一 worker;FLUSH/检查点族无 key 条目全员认领,但栅栏键 (barrier_id, sequence_number) 全局共享,R 方对齐、Leader 独占执行一次(wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs:process_synchronized_operation_async):幂等在位
- 模糊区:缓冲条目主扫跳过、CheckpointEndCommit 一次重放;CheckpointStart 撞未闭区清缓冲为丢弃非重放(wedb/wnode/src/aof/aof_processor.rs:process_aof_record_internal):幂等在位
- 副本入队去重闸:cluster_replication_session.rs 尾 != current 即 Divergent 断流,重发同帧必拒:闸在位(唯 finding 1 起点错位可绕过 applied 位点)
- 主端推流:pump 单飞闸 + accepted_address fetch_max + shipped 水位单调 ratchet;aof_sync_task.rs:consume 只拒 < prev 不拒 == prev,但位点单调使重发无二次推进:幂等在位
- 迁移/无盘导入:frame_import.rs 全值覆写 replace:true、错误复位两接收态;检查点接收段绝对位置写(receive_checkpoint_handler.rs:write_chunk)重传幂等:在位
- AOF 提交/fsync 重试:committer_loop 失败留痕 + 唤醒等待者复查,下轮覆盖重刷(fsync 幂等);commit 帧每刷一写、恢复取末帧(wedb/wnode/src/aof/waof_sublog.rs:committer_loop/commit_flush_async):幂等在位
- 通知副作用:重放路径(恢复/副本重放/迁移导入)纯 StorageSession 与对象通道,不挂 RESP metrics、pubsub、阻塞唤醒 → 无双计双投:在位;时间脉冲 pending/applied 双单调闸 + 位点追平守卫(wedb/src/server/replication/replica_replay_driver.rs:signal_time_advance/try_apply_pending_pulse):在位;watch 版本 bump/ACL 代数 bump 单调非幂等但仅良性误冲突:在位
- gossip 重发:merge_worker_info config_epoch 越限跳过、replication_offset 保留本地值(wedb/src/server/cluster_config/mod.rs:merge_worker_info);MEET 重发 connection_store 去重、失败允许立即重发:幂等在位
- CONFIG SET 重入:PrimaryTasks swap 幂等重拉 + 退出决定点先落标志(wedb/wnode/src/primary_tasks.rs);GC 句柄槽 + is_active 替换重拉(wedb/wkv/src/gc/task.rs:reconcile_gc_scan);CONFIG SET index 扩容 Rest→PrepareGrow CAS 单飞(wedb/wkv/src/store/resize.rs:grow_index);推流 throttle loop swap 闸;attach_wake 重复挂接拒绝:全在位
- 一次性初始化:spawn_bftree_reclaimer compare_exchange 挂载位(wedb/wkv/src/gc/reclaim.rs);ReplicaReplayDriverStore get_or_insert 原子 + 重复 init 帧拒绝;INITIATE_REPLICA_SYNC try_add 去重 + Drop 摘除;failover 无自动重试环:在位。注意闸的影子:背景重放启动闸同时挡死了死后重生(见 finding 1)
- 升降阶在途:tiered_replay_arm stub 缺失留痕跳过、绝不落信封通道(wedb/wnode/src/aof/aof_processor_object_replay.rs),重放/重试不物化幻影信封;迁移链 TRANSMITTING/DELETING 键门阻断升降阶交错(r6-del/r8-sample-b 已立发散面不复述):在位

无增量确认
- 闸在位清单外,未发现第三处缺失闸
- 字符串域 INCR/DECR/APPEND/SETRANGE/BITFIELD 命令端折终值(wkv/src/session/raw/write/rmw.rs:try_rmw_sync 终值写回 → StoreUpsert 镜像),重放天然幂等
- failover、检查点重试(含 ODC 两拍上限)、gossip 重发、AOF 提交重试均幂等或闸保护
- 其余 spawn/登记/挂载点均有 compare_exchange/get_or_insert/swap 类闸

视角结论:有增量
