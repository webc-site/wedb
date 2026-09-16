# db 待办

1. [P1] COMMITAOF 命令空壳实现与物理刷盘提交管线脱节
   位置：wedb/wnode/src/resp/admin_commands.rs:152（RespServerSession::network_commitaof）；wedb/wnode/src/config_owner.rs:32（apply_config_reconcile 中 ConfigReconcile::CommitTask）
   对标：garnet/libs/server/Resp/AdminCommands.cs:NetworkCOMMITAOF；garnet/libs/server/StoreWrapper.cs:CommitAOFAsync；garnet/libs/server/StoreWrapper.cs:CommitTaskAsync；garnet/libs/server/Databases/SingleDatabaseManager.cs:CommitToAofAsync；garnet/libs/server/Databases/MultiDatabaseManager.cs:CommitToAofAsync
   C# 机制：NetworkCOMMITAOF 解析可选参数 dbId 后在网络线程调用 AsyncUtils.BlockingWait(CommitAofAsync(dbId))。CommitAOFAsync 路由至 databaseManager.CommitToAofAsync：单库直接调用 AppendOnlyFile.Log.CommitAsync；多库模式持 TryGetDatabasesContentReadLock 读锁，并发构建各库 AwaitCommitAsync(db, db.AppendOnlyFile.Log.CommitAsync) 并发等待。此外 StoreWrapper.CommitTaskAsync 启动常驻后台循环，Primary 节点每隔 commitFrequencyMs 毫秒主动触发一次全库提交。
   问题：network_commitaof 仅校验参数与数据库 ID 后无条件直接写入固定文本 "+AOF file committed\r\n"，注释自认未挂载真实提交 provider；且配置调停分支中的周期提交任务（CommitTask）被当作空操作分支抛弃。在启用 AOF 的生产环境下，客户端执行 COMMITAOF 不会触发任何物理刷盘与位点推进，掉电将导致已写入数据丢失；同时无后台定时提交驱动，提交位点长期滞后。
   改法：
   第一步，在 GarnetApi 与 StoreGarnetApi 扩展 slow 慢路径接口 commit_aof_slow(db_id: i64)，network_commitaof 接入 route_slow_command 转挂慢路径 SlowWait 状态机。
   第二步，慢路径执行器调用常驻 IDatabaseManager::commit_to_aof_async(db_id)：db_id == -1 时遍历所有活跃库并发触发 aof.commit_flush_async()。底层 GarnetLog 驱动 WaofSublog 触发 WalLog::commit_pipeline 的 GroupCommitStep，等待底层块设备 sync_all 完成后，将 committed_until_address 推进至当前 safe_tail_address，全部完成后应答客户端。
   第三步，服务启动上下文根据 options.commit_frequency_ms（>0 时）使用 compio 挂载常驻循环 commit_task_loop，Primary 节点定期执行提交；CONFIG SET aof-commit-freq 变更时通过取消令牌重启该后台循环。

2. [P1] SAVE 与 BGSAVE 每次临时构造局部 DatabaseManager 破坏互斥与状态常驻
   位置：wedb/wnode/src/resp/garnet_api.rs:777（StoreGarnetApi::checkpoint_command_slow）
   对标：garnet/libs/server/StoreWrapper.cs:TakeCheckpointAsync；garnet/libs/server/Databases/SingleDatabaseManager.cs:TakeCheckpointAsync；garnet/libs/server/Databases/MultiDatabaseManager.cs:TakeCheckpointAsync；garnet/libs/server/Databases/DatabaseManagerBase.cs:TryPauseCheckpoints
   C# 机制：StoreWrapper 实例生命周期内全局单例持有 IDatabaseManager databaseManager。SAVE/BGSAVE 到达时统一委托 storeWrapper.TakeCheckpointAsync(background: true/false)。管理器内部通过 TryPauseCheckpoints 以原子 CAS 抢占检查点槽位，已有在途检查点时立即拒绝；快照完成后在 finally 块调用 ResumeCheckpoints 释放锁并推进全局 LastSaveTime。
   问题：checkpoint_command_slow 每次收到 SAVE 或 BGSAVE 命令时，现场临时执行 let db = Arc::new(GarnetDatabase::with_garnet_aof(...)); let mgr = SingleDatabaseManager::new(...);，命令执行完毕该 mgr 立即释放。每个客户端会话各自持有独立的局部管理器，mgr.try_pause_checkpoints() 锁定的仅是局部对象的内部状态，跨连接并发发起的 SAVE/BGSAVE 无法互相感知，并发互斥保护彻底失效；且快照进度与全局常驻管理器状态脱节。
   改法：
   第一步，改造 StoreGarnetApi<D>，移除临时的 checkpoint 上下文，改为直接注入并常驻持有全局唯一的 Arc<dyn IDatabaseManager<D>> 实例。
   第二步，重构 checkpoint_command_slow 分发：
   - LASTSAVE：直读 self.database_manager.try_get_database(0).unwrap().last_save_ms()。
   - SAVE：调用 self.database_manager.take_checkpoint_async(false, -1).await，已占用回 ERR checkpoint already in progress，成功回 +OK\r\n。
   - BGSAVE：先执行 self.database_manager.try_pause_checkpoints(-1)，失败立即报错；成功后在后台 spawn 异步执行快照，主流程即刻向客户端返回 +Background saving started\r\n，异步快照任务完成后在尾部调用 resume_checkpoints(-1)。

3. [P1] Checkpoint 两阶段状态机退化为局部日志打印且快照后才推进版本
   位置：wedb/wcpr/src/manager.rs:465（create_checkpoint_inner）；wedb/wdatabase/src/database_manager_base.rs:224（take_database_checkpoint_async）
   对标：garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/FullCheckpointSM.cs:NextState；garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/VersionChangeSM.cs:NextState；garnet/libs/storage/Tsavorite/cs/src/core/Index/Recovery/IndexCheckpoint.cs:GlobalBeforeOnExit
   C# 机制：Tsavorite 严格遵循 CPR（Consistent Prefix Recovery）协议，状态机状态流转为 REST -> PREPARE -> IN_PROGRESS -> WAIT_INDEX_CHECKPOINT -> WAIT_FLUSH -> PERSISTENCE_CALLBACK -> REST。在 PREPARE 阶段结束、进入 IN_PROGRESS 阶段的瞬间，全局版本号原子递增（Version = start.Version + 1）。随后进入 IN_PROGRESS 的所有新写入自然携带新版本号 v+1，快照逻辑仅持久化 Version <= v 的数据，二者在逻辑版本上天然隔离。
   问题：wcpr/src/manager.rs 中的 CprPhase 仅为单个函数内部的局部变量流转，每一步仅打印 debug 日志，无跨会话状态协调；更严重的是版本号推进被放置在 database_manager_base.rs:224（快照文件写完、AOF 截断完成之后）才调用 set_current_version。导致长达数秒的快照写盘期间，并发写入的所有新数据依然被打上旧版本号 v；系统重启恢复时，AOF 重放逻辑依据 header.store_version < checkpoint_version 判定条目，将这些新写记录误判为已包含在快照中的旧数据而直接跳过，产生严重数据静默丢失。
   改法：
   第一步，在 wkv::WedbStore 引入系统级状态机驱动，维护全局原子状态 SystemState { version: i64, phase: CprPhase }。
   第二步，调整版本号推进时机：在 PREPARE 阶段捕获 tail 地址并执行 shift_read_only_address(tail) 封印旧区域后，立即将版本号原子递增为 v+1，并进入 IN_PROGRESS 阶段。
   第三步，通过 store.epoch().bump_current_epoch_action 等待所有持有旧版本 v 的在途写操作排空；排空后启动后台 I/O 刷写 Version <= v 的内存脏页与生成索引快照，此后所有新写入天然获得 v+1 版本号，彻底保障 CPR 代际隔离与 AOF 回放正确性。

4. [P1] AOF 回放巨型单文件超长逻辑混杂与状态机拆分
   位置：wedb/wnode/src/aof/aof_processor.rs:1（全文 1862 行）；wedb/wnode/src/aof/aof_processor.rs:643（process_aof_record_internal）；wedb/wnode/src/aof/aof_processor.rs:1041（store_rmw）；wedb/wnode/src/aof/aof_processor.rs:1285（object_store_rmw）
   对标：garnet/libs/server/AOF/AofProcessor.cs；garnet/libs/server/AOF/AofProcessor.Record.cs；garnet/libs/server/AOF/AofProcessor.Replication.cs；garnet/libs/server/AOF/AofProcessor.ChunkReplay.cs；garnet/libs/server/AOF/AofProcessor.Structures.cs
   C# 机制：C# 将 AOF 回放器通过 5 个 partial class 严格拆分：AofProcessor.cs 专注主循环、拓扑与协调器状态；AofProcessor.Structures.cs 承载输入反序列化；AofProcessor.Record.cs 专职主存与统一存操作落地；AofProcessor.ChunkReplay.cs 专职分块条目重组；AofProcessor.Replication.cs 负责版本跳过判定。
   问题：aof_processor.rs 单文件膨胀至 1862 行，将会话存储抽象（RangeIndexSessionFace）、ReplayInput 编解码、模糊区操作缓冲、事务组重放、主存 RMW（混杂 TTL 毫秒换算、ETag 旁路、向量命令、RI 命令）、以及对象存 4 大类型的信封编解码全部堆砌在一起。函数体长达数百行，内部大量裸字节解析，缺乏单一职责划分。
   改法：
   将 aof_processor.rs 重构成模块文件夹 wedb/wnode/src/aof/aof_processor/：
   - types.rs：声明 AofReplayError、PreparedParameters、ReplayTarget，将 RangeIndexSessionFace 独立出 trait 定义。
   - input.rs：收拢 ReplayInput、ReplayInputSlice 的栈缓冲无分配反序列化与边界校验。
   - mod.rs：保留精简的 AofProcessor 核心，负责拓扑预处理（prepare_key）、条目版本过滤（should_skip_record）、分块累加驱动与任务分派（can_replay / skip_replay）。
   - dispatch.rs：主存与统一存操作分发器，拆解 store_upsert、store_rmw（细化出 ttl_rmw、etag_rmw、math_rmw）、store_delete。
   - object_dispatch.rs：对象存储分发器，收拢 ReplayObject trait 与 Hash/Set/List/ZSet 信封重放，实现单通道无 dyn 静态分发。
   - fuzzy_region.rs：专职管理模糊区操作缓存（process_fuzzy_region_operations）与事务组回放（process_transaction_group_operations）。

5. [P1] CacheSizeTracker 退化为离线计数器且无内存与读缓存动态扩缩容通道
   位置：wedb/wdatabase/src/cache_size_tracker.rs:10（CacheSizeTracker）；wedb/wkv/src/read_cache.rs:68（ReadCache::new）；wedb/whlog/src/hlog/mod.rs（HybridLog）
   对标：garnet/libs/server/Storage/SizeTracker/CacheSizeTracker.cs:Initialize；garnet/libs/server/Storage/SizeTracker/CacheSizeTracker.cs:TargetSize；garnet/libs/server/ServerConfig.cs:HandleMemorySizeChange；garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/LogSizeTracker.cs:UpdateTargetSize；garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/LogSizeTracker.cs:TrimL1Cache
   C# 机制：CacheSizeTracker 统一管理主日志与读缓存对应的 LogSizeTracker。每个 LogSizeTracker 维护 targetSize、highTargetSize（110%）和 lowTargetSize（98%）。当内存使用超过高水位时，触发异步 resizerTask 驱动日志 HeadAddress 前移，执行 CleanseHashChain 将旧内存页驱逐出 DRAM。执行 CONFIG SET memory 或 readcache-memory 时，动态更新 TargetSize 并立即驱动自适应修剪。
   问题：CacheSizeTracker 目前仅为两个无实质业务联动的 AtomicI64 累加器，注释自认“退化为估计器，无后台循环”。ReadCache 与 HybridLog 的 CircularPageBuffer 页面总数在启动后硬编码定容，未建立动态修剪机制，无法对 CONFIG SET memory 与 readcache-memory 做出响应，在写入尖峰下面临不可控的内存膨胀风险。
   改法：
   第一步，在 CacheSizeTracker 补齐配置面与动态参数：引入 target_size 与 read_cache_target_size 原子变量，以及 HighTargetSizeDeltaFraction（10%）与 LowTargetSizeDeltaFraction（2%）回差计算。
   第二步，在 CacheSizeTracker 增加事件通知通道 crossfire::mpsc，当累加字节超出高水位阈值时触发异步信号。
   第三步，底层 ReadCache 增加动态驱逐接口 cleanse_until_address(target_addr)，HybridLog 增加动态推动接口 shift_head_address。后台监听协程捕获高水位信号后，推动 HeadAddress 淘汰旧页，直到内存回落至低水位；在 ServerConfig 处理 CONFIG SET 时打通 update_target_size 实时调节通道。

6. [P1] RangeIndex 删空自愈缺失导致幽灵元记录与孤儿底层树文件泄漏
   位置：wedb/wkv/src/range_index.rs:548（range_index_del）；wedb/wkv/src/session/collection.rs:213（load_meta 判活逻辑）
   对标：garnet/libs/server/Resp/RangeIndex/RangeIndexManager.cs:TryDeleteRangeIndexUnderLock；garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeAndDeleteFilesDeferred；wedb/transpile/SKILL.md 严格删空生命周期与原子墓碑规范
   C# 机制：RangeIndex 删除字段时，当条目数减至 0 或键被删除时，执行 TryDeleteRangeIndexUnderLock。持条带独占锁从 liveIndexes 注册表摘除树实例，解除引用后挂入 storeEpoch.bump_current_epoch_action，待所有在途读者退出后，由后台动作执行 DisposeAndDeleteFilesDeferred 物理删除磁盘上的 .bftree 文件。
   问题：在 range_index_del 中，当删除字段导致 meta.size 减至 0 时，代码仅调用 save_bftree_meta_stub 把 size=0 写回日志，未做任何删空自愈；同时 load_meta 判活时硬编码了 || meta.collection_type == CollectionType::RangeIndex，导致空 RangeIndex 永远被判定为存活。这使得字段删空后，主存长期驻留幽灵空存根，底层树文件与文件句柄永不释放，造成严重的资源泄漏。
   改法：
   第一步，修改 wkv/src/session/collection.rs:213，移除对 RangeIndex 的特例放行，统一规则：任何集合类型 meta.size == 0 一律判定为已死亡。
   第二步，在 range_index.rs:range_index_del 增加删空自愈分支：当字段删除后 meta.size == 0 时，执行原子自愈序列：
   - 写入元记录墓碑（delete_raw），推进该键的版本号；
   - 调用 del_ttl 清除随键 TTL，避免产生孤儿 TTL；
   - 触发 self.store.range_index.delete_index(key)，持条带写锁注销 live_indexes 并经纪元排空后彻底删除底层 .bftree 磁盘文件；
   - 发送 StoreEvent::RangeIndexDelete 事件记录 AOF 删空墓碑。

7. [P1] FLUSHALL 与 FLUSHDB 逐键扫描写墓碑导致性能退化与巨额内存放大
   位置：wedb/wkv/src/store/keyspace.rs:132（flush_all_databases）；wedb/wdatabase/src/database_manager_base.rs:282（flush_database）
   对标：garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase；garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushAllDatabases；garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftBeginAddress
   C# 机制：C# 执行 FlushDatabase 时，直接调用 db.Store.Log.ShiftBeginAddress(db.Store.Log.TailAddress)。该原语直接将起始有效逻辑地址推至日志尾部，该地址之前的全部物理数据段就地废弃并异步解除磁盘映射，耗时恒定为 O(1)。随后 AOF 执行 TruncateUntil(TailAddress) 完成日志物理截断。
   问题：flush_all_databases 与 flush_database 目前未采用 shift_begin_address 物理截断，而是先调用 collect_user_keys(None) 将全库所有用户键一次性收集到一个内存 Vec 中（千万级键直接引发 OOM）；随后在循环中逐键执行异步 session.delete(&key)。每个 delete 均去查 TTL、查 ETag 并在日志尾部追加写入一条墓碑记录。耗时从 O(1) 暴跌至 O(N)，不仅带来极大的写放大，更会在磁盘空间接近满载时直接撑爆存储。
   改法：
   第一步，重构全库清空 FLUSHALL：彻底废弃 collect_user_keys 逐键写墓碑逻辑；持全局写屏障捕获当前 tail 地址，调用 store.index().clear() 直接重置哈希桶与溢出池，调用 store.shift_begin_address(tail).await 物理推进起始边界，触发底层设备 truncate_until_address 物理删除旧数据段文件，清空全部 RangeIndex 树文件，最后截断 AOF，全流程 O(1) 物理截断完成。
   第二步，重构单库清空 FLUSHDB：若为单库独立实例则直接复用 O(1) shift_begin_address；若为多库共享存储，将逐键 collect 改为基于游标的分页流式迭代（单批 1024 键），消除全库键一次性物化到内存中的 OOM 隐患。

8. [P2] RangeIndexManager 分层碎裂与四套薄壳代理对象冗余设计
   位置：wedb/wnode/src/resp/rangeindex/range_index_manager_locking.rs:20（RangeIndexManagerLocking）；wedb/wnode/src/resp/rangeindex/range_index_manager_index.rs:16（RangeIndexManagerIndex）；wedb/wnode/src/resp/rangeindex/range_index_manager_migration.rs:74（RangeIndexManagerMigration）；wedb/wnode/src/resp/rangeindex/range_index_chunked_serializer.rs:30（RangeIndexChunkedSerializer）
   对标：garnet/libs/server/Resp/RangeIndex/RangeIndexManager.cs；garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs；garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs；garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs；garnet/libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs
   C# 机制：C# RangeIndexManager 采用 partial class 拆分源码文件，所有方法直接归属于 RangeIndexManager 唯一类实体，共享同一套内部状态，没有跨 crate 薄壳代理。
   问题：AI 在转写时机械地把 C# partial class 拆成了 wbftree 底层实体与 wnode 层的 4 个无状态虚拟结构体（Locking、Index、Migration、Replication）。例如 RangeIndexManagerLocking 仅 49 行全为微型单行转发；且 wbftree 已经导出了完整的 RangeIndexChunkedSerializer / Deserializer / MigrationReader，wnode 层又重复包装了一套同名薄壳结构体（如 struct RangeIndexChunkedSerializer(pub(super) Engine)），导致调用链路碎裂、多层反复转发。
   改法：
   第一步，删除 wnode 层的 range_index_manager_locking.rs 与 range_index_manager_index.rs，将条带独占锁原语直接内聚为 wbftree::RangeIndexManager::acquire_exclusive_lock(key)，存根编解码直接收敛至 wbftree::RangeIndexStub。
   第二步，删除 wnode 层的 range_index_chunked_serializer.rs、range_index_chunked_deserializer.rs、range_index_migration_reader.rs，上层直接导入使用 wbftree 导出的序列化器，通过 thiserror 透明转换底层 I/O 错误。

9. [P2] 日志系统多层套娃包装（SingleLog/ShardedLog/Sublog/WaofSublog）与代码重复
   位置：wedb/wnode/src/aof/single_log.rs:12（SingleLog）；wedb/wnode/src/aof/sublog.rs:9（Sublog）；wedb/wnode/src/aof/garnet_log.rs:1（GarnetLog）；wedb/wnode/src/aof/garnet_log.rs:1612（InMemorySublog）
   对标：garnet/libs/server/AOF/GarnetLog.cs；garnet/libs/server/AOF/SingleLog.cs；garnet/libs/server/AOF/ShardedLog.cs
   C# 机制：GarnetLog 扁平持有 SingleLog 或 ShardedLog，二者直接承接 TsavoriteLog，无层层包裹。
   问题：Rust 侧存在 waof::WalLog -> WaofSublog -> Sublog (enum) -> SingleLog -> GarnetLog 深度达 5 层的调用链；SingleLog 仅 112 行全为机械透传；且 GarnetLog 内部在 garnet_log.rs:1612 独立手写了 200 多行 InMemorySublog 内存环形日志，与 waof crate 的测试与内存日志实现完全重复。
   改法：
   第一步，消除 SingleLog 薄壳，在 GarnetLog 内直接通过拓扑枚举管理：enum LogTopology { Single(Arc<WaofSublog>), Sharded(ShardedLog) }。
   第二步，废除 GarnetLog 内重复手写的 InMemorySublog，统一基于 waof::WalLog + 内存块设备（wdev::MemoryDevice）实现纯内存拓扑，使单机极速模式与持久化模式走同一套状态机代码路径。

10. [P2] 多物理子日志地址探测逻辑无脑硬编码第 0 号子日志
    位置：wedb/wnode/src/aof/garnet_append_only_file.rs:368（safe_flush_address）；wedb/wnode/src/aof/garnet_append_only_file.rs:375（can_truncate）；wedb/wnode/src/aof/garnet_append_only_file.rs:382（can_commit）；wedb/wnode/src/aof/garnet_append_only_file.rs:390（tail_address）
    对标：garnet/libs/server/AOF/GarnetAppendOnlyFile.cs:TailAddress；garnet/libs/server/AOF/GarnetAppendOnlyFile.cs:CanTruncate；garnet/libs/server/AOF/GarnetAppendOnlyFile.cs:SafeFlushAddress
    C# 机制：多物理日志模式下，TailAddress 返回聚合了各子日志位点的 AofAddress 向量；CanTruncate 严格要求所有子日志均满足截断水位；CanCommit 只要任一子日志存在未提交脏数据即返回 true；SafeFlushAddress 取各子日志已刷盘水位的全局最小值。
    问题：上述 4 个方法在当前代码中全部直接硬编码 self.log().get_sub_log(0)，只读取子日志 0 的位点与状态，直接忽略其余子日志的刷盘与提交进度，导致在多物理子日志配置下提交与截断判定失真，极易造成分片数据损坏或截断撕裂。
    改法：
    - can_truncate 改为全称规约：self.log().sublogs().iter().all(|s| s.tail_address() <= s.begin_address())。
    - can_commit 改为存在性规约：self.log().sublogs().iter().any(|s| s.flushed_until_address() < s.tail_address())。
    - safe_flush_address 改为取各子日志下界：self.log().sublogs().iter().map(|s| s.flushed_until_address()).min()。
    - tail_address 在分片模式下返回多维向量或全局最大位点，消除对 0 号子日志的单点硬编码。

11. [P2] 多物理日志恢复（MultiLogRecover）上界因缺少持久化元数据无法收敛
    位置：wedb/wnode/src/aof/recover/aof_recover.rs:6（AofRecover 文档注释）；wedb/waof/src/log.rs:42（WalLogInner）
    对标：garnet/libs/server/AOF/Recover/AofRecover.cs:MultiLogRecover；garnet/libs/server/AOF/Recover/AofRecover.cs:RecoverLatestSequenceNumber
    C# 机制：多物理日志拓扑中，每个提交条目携带全序 SequenceNumber。提交时将最后提交的序列号作为 commit cookie 持久化在子日志的元数据区。崩溃恢复时，MultiLogRecover 并行读取所有子日志提交 cookie，取最小公共序列号作为全局一致恢复边界，各子日志回放到该点截断，保证因果一致。
    问题：waof 目前仅实现了环形数据追加，未划分持久化的元数据扇区，commit(until_address, cookie) 的 cookie 仅保存在内存中，重启后丢失。导致 aof_recover.rs 显式承认“多物理日志恢复因无法收敛上界而被迫关闭”，使多子日志功能处于半残状态。
    改法：
    第一步，在 WalLog 管理的物理设备前部划分 4KB 专用超级块（Superblock），持久化记录 last_committed_address、last_committed_sequence_number 与 CRC32 校验码。
    第二步，提交刷盘时原子同步该元数据块；在 RecoverLogDriver 启动时，补齐 RecoverLatestSequenceNumber 读取通道，计算所有子日志的 min_sequence 作为回放截止线，对齐 C# MultiLogRecover 恢复算法。

12. [P2] RangeIndex 缺失批量写入与删除接口导致元数据高倍写放大与频繁页分裂
    位置：wedb/wkv/src/range_index.rs:490（range_index_set）；wedb/wbftree/src/service/mod.rs:50（BfTreeService）
    对标：garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs；wedb/transpile/SKILL.md 批量接口（Batching API）单次折叠机制规范
    C# 机制：底层 bftree 引擎支持局部排序条目的 Bulk Insert，单次获取锁并批量插入单页，压降页分裂。
    问题：RangeIndex 仅提供单条操作 ri_set 与 ri_del。批量操作时，外部循环调用 N 次：每次都要重新读取元数据、获取条带锁、进出纪元，无序散列写入引发 B+Tree 频繁页分裂；且单条维护一次 meta.size 回写，产生 N 次元数据写放大。
    改法：
    第一步，在 BfTreeService 与 RangeIndexManager 增加批量接口：ri_set_batch(key: &[u8], items: &[(&[u8], &[u8])]) 与 ri_del_batch(key: &[u8], fields: &[&[u8]])。
    第二步，接口内部单次获取条带写锁与进入纪元；在栈上利用 SmallVec 预先对待写入的 items 按 field 排序，集中命中目标叶子页；单次更新 meta.size 并回写存根，消除写放大。

13. [P2] 集合元数据存储代码残留旧版 Flattened 与 Chunked 分层逻辑
    位置：wedb/wkv/src/session/collection.rs:49（delete）；wedb/wkv/src/session/collection.rs:16（META_HAS_EXPIRE_MASK）
    对标：wedb/transpile/SKILL.md 集合类型信封存储规范
    C# 机制：Garnet 内存对象统一由 ObjectStore 承载，无所谓打平至存储引擎内部树文件的旧机制。
    问题：collection.rs 中大量残留对 meta.encoding().is_flattened()、打平集合快删以及 Compact 内联分块的兼容分支。除 RangeIndex 走独立 wbftree 外，Hash/Set/List/ZSet 已严格统一为 ObjectEnvelope 内存对象信封存储，遗留分支增加代码复杂度并拖慢主路径。
    改法：
    彻底清除 collection.rs 中涉及 is_flattened() 和非 RangeIndex 树算子回收的死代码，将集合删除统一收敛为：RangeIndex 树排空注销与通用 ObjectEnvelope 删空墓碑回写两条纯粹路径。

14. [P2] MultiDatabaseManager 检查点执行采用串行遍历造成多库位点窗口严重漂移
    位置：wedb/wdatabase/src/multi_database_manager.rs:263（take_checkpoint_async）
    对标：garnet/libs/server/Databases/MultiDatabaseManager.cs:TakeCheckpointAsync；garnet/libs/server/Databases/MultiDatabaseManager.cs:AwaitCommitAsync
    C# 机制：多库做检查点时，持 contentLock 读锁，并发为所有活跃库构建 TakeOneCheckpointAsync 任务数组，通过 await Task.WhenAll(tasks) 并行等待所有库快照完成，确保多库快照时间线严格对齐。
    问题：当前多库快照使用串行循环 for db in databases_snapshot 逐个等待 take_one_checkpoint 完成。若存在多个数据库，各库快照完成时间相差数秒至数分钟，导致各库在检查点集合内的 AOF 截断点与快照版本发生严重窗口漂移。
    改法：在 multi_database_manager.rs 的 take_checkpoint_async 中，使用 futures::future::join_all 并发调度所有活跃库的 take_one_checkpoint 任务，并行刷盘与落盘，最大限度压降库间代际漂移。

15. [P3] 集群槽位验证代码在 wedb 与 wnode 间三处重复定义
    位置：wedb/wedb/src/server/cluster_session.rs:229；wedb/wedb/src/server/slot_verify.rs:380；wedb/wnode/src/cluster_session.rs:196（network_iterative_slot_verify）
    对标：garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify；garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:ResetCachedSlotVerificationResult；garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:WriteCachedSlotVerificationMessage
    C# 机制：C# 的 RespClusterIterativeSlotVerify.cs 单一文件实现槽位验证与缓存重置，集群会话全局仅有一处定义。
    问题：check.js 明确告警 NetworkIterativeSlotVerify、ResetCachedSlotVerificationResult、WriteCachedSlotVerificationMessage 在 wedb/src/server/cluster_session.rs、wedb/src/server/slot_verify.rs 与 wnode/src/cluster_session.rs 中被重复编写 3~4 次，存在典型的多处代码拷贝坏味道。
    改法：将槽位迭代验证状态机收敛到单一模块，删除另外两处的复制粘贴代码，改为直接引用；统一保留标准 C# 映射注释，消除 check.js 的重定义告警。
