review-db 待办：底层引擎 aof bftree 存储

check.js 现状
重复 23 组，实现缺失 3 项。db 域相关重复 5 组，缺失 1 项。db 主体零缺失方向正确，问题集中在锁表双挂、快照扫描双挂、预取锚点透出、刷盘枚举重复扫描。
缺失三项原文：libs/common/RespWriteUtils，libs/server/Lua/NativeMethods，libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable
其中 OverflowBucketLockTable 已有 rust 承接但未挂锚点，见第 1 条。

1. OverflowBucketLockTable 缺失实为锚点未挂
问题：wtxn 锁表已按 C# 口径实现但 check.js 仍报缺失，原因是构造装配函数无锚点，只有结构体一行挂文件级锚点。
rust：wedb/wtxn/src/txn_lock_table.rs fn TxnLockTable::from_loader fn TxnLockTable::pin fn TxnLockTable::bucket_index_for_hash
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs fn OverflowBucketLockTable fn GetBucketIndex
动作：构造与取桶下标补锚点，缺失自消。锁内存零复刻口径已对，无需新建表。

2. HashBucket 四锁双挂 windex 与 wtxn
问题：四个闩函数同时挂在 windex 桶本体与 wtxn 锁表转发层，check.js 报 4 组重复。
rust：wedb/windex/src/bucket.rs fn HashBucket::try_lock_shared fn HashBucket::try_lock_exclusive fn HashBucket::unlock_shared fn HashBucket::unlock_exclusive 保留；wedb/wtxn/src/txn_lock_table.rs 四转发函数去锚点
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs fn TryAcquireSharedLatch fn TryAcquireExclusiveLatch fn ReleaseSharedLatch fn ReleaseExclusiveLatch
动作：转发层只留转发说明，锚点只留桶本体一处。

3. ContextReadWithPrefetch 双挂索引内核与会话编排
问题：同一 C# 函数挂到 windex 预取内核与 wkv 批量读编排，会话侧是调用方不是实现。
rust：wedb/windex/src/table.rs fn HashIndex::prefetch_batch_probes 保留；wedb/wkv/src/session/raw/batch.rs fn StoreSession::read_batch_with 去锚点改挂调用说明
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs fn ContextReadWithPrefetch
动作：只改注释。PREFETCH_WINDOW=12 单点在 windex/src/prefetch.rs，wkv/src/lib.rs 透出为 re-export 不算重复，保持。

4. AcquireExclusiveForDelete 双挂管理器与会话守卫
问题：RangeIndexManager::locks 与 StoreSession::acquire_tree_write 同挂一键，前者是锁容器后者是取锁调用，语义不同。
rust：wedb/wbftree/src/manager/mod.rs fn RangeIndexManager::locks 去锚点；wedb/wkv/src/range_index/stub.rs fn StoreSession::acquire_tree_write 保留
对应 C#：libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs fn AcquireExclusiveForDelete
动作：只改注释，取锁语义收敛到会话守卫一处。

5. GetDatabasesSnapshot 一挂三处
问题：WedbStore::store_snapshot 与 wnode 两层投影同挂 StoreWrapper.GetDatabasesSnapshot，引擎快照与 INFO 投影混挂。
rust：wedb/wkv/src/store/stats.rs fn WedbStore::store_snapshot 保留；wedb/wnode/src/resp/garnet_api/mod.rs fn project_aof_snapshot fn project_db_snapshot fn store_snapshots 去该锚点
对应 C#：libs/server/StoreWrapper.cs fn GetDatabasesSnapshot；投影侧对照 libs/server/Metrics/Info/GarnetInfoMetrics.cs fn GetDatabaseStoreStats fn GetDatabasePersistenceStats
动作：投影侧改挂 InfoMetrics 两键，引擎侧独占 StoreWrapper 锚点。

6. waof wal 与 whlog 双日志引擎要拆分说明
问题：WalLog 与 HybridLog 都是环形页缓冲加刷盘流水线，新人易误认为重复。实为物理 WAL 与混合日志分层，但根文档只在 waof 讲了半句。
rust：wedb/waof/src/wal/log.rs struct WalLog struct WalLogInner；wedb/whlog/src/hlog/mod.rs struct HybridLog；共用 wedb/wdev/src/device.rs fn Device::flush_range_aligned fn Device::truncate_begin_until
对应 C#：libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs；libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs
动作：两 crate 顶层文档互指分层，刷盘截断内核已收敛 wdev 不再下沉。

7. GroupCommitPipeline 三处复用已收敛无需再拆
问题：WalCommitStep 与 FlushStep 同构但非重复，是同一流水线两种 Step 注入。
rust：wedb/wbase/src/group_commit.rs struct GroupCommitPipeline；wedb/waof/src/wal/flush.rs struct WalCommitStep；wedb/wkv/src/store/flush.rs struct FlushStep
对应 C#：libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs fn CommitAsync；libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs fn AsyncFlushPagesForSnapshot
动作：不拆不合，保持 Step 注入形态，只需在两 Step 头注明共用内核。

8. wrecord 头双字要守住一处定义
问题：RecordHeader 16 字节双字已合并 RecordInfo 与 RDH，RecordRef 与 RecordMut 经 Deref 复用无冗余，现状良好。但 header.rs 687 行再膨胀需拆。
rust：wedb/wrecord/src/header.rs struct RecordHeader fn pack_rdh_word；wedb/wrecord/src/record_ref.rs struct RecordRef；wedb/wrecord/src/record_mut.rs struct RecordMut
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs；libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs
动作：不拆，新增位段一律进 header.rs，视图侧禁加代理。

9. 存根治愈内核已收敛禁再开副本
问题：patch_stub_record 与 encode_meta_stub_record 已是 wkv 唯一内核，compact/flush/cpr_host 全部转调，旧截断副本已删，现状良好。
rust：wedb/wkv/src/range_index/stub.rs fn patch_stub_record；wedb/wkv/src/range_index/mod.rs fn encode_meta_stub_record；调用方 wedb/wkv/src/compact.rs fn StoreSession::append_record，wedb/wkv/src/store/flush.rs fn WedbStore::on_flush_pages，wedb/wkv/src/store/cpr_host.rs
对应 C#：libs/server/Storage/Functions/GarnetRecordTriggers.cs fn PostCopyToTail fn OnFlush；libs/server/Resp/RangeIndex/RangeIndexManager.cs fn SnapshotTreeForFlush
动作：不改，后续存根位变更只改内核，调用方禁本地解码重写。

10. wbftree manager 与 service 要守住边界
问题：manager 与 service 分层清晰，但 chunk.rs 673 行与 stub.rs 449 行最易互相渗入。
rust：wedb/wbftree/src/chunk.rs struct RangeIndexChunkedSerializer struct RangeIndexChunkedDeserializer struct RangeIndexMigrationReader；wedb/wbftree/src/stub.rs struct RangeIndexStub；wedb/wbftree/src/manager/mod.rs struct RangeIndexManager；wedb/wbftree/src/service/mod.rs struct BfTreeService
对应 C#：libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs；libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs；libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs；libs/native/bftree-garnet/BfTreeService.cs；libs/server/Resp/RangeIndex/RangeIndexManager.cs
动作：不拆不合，文件 IO 只许在 chunk 与 manager 落笔，service 禁碰 fs。

11. on_flush 双入口可合并
问题：on_flush 与 on_flush_address 仅差 Option 地址参数，wkv 永远走带地址版，裸入口无调用。
rust：wedb/wbftree/src/manager/flush.rs fn RangeIndexManager::on_flush fn RangeIndexManager::on_flush_address fn on_flush_internal
对应 C#：libs/server/Resp/RangeIndex/RangeIndexManager.cs fn SnapshotTreeForFlush
动作：删裸入口或注明测试专用，wkv 只调带地址版。

12. 刷盘快照目录枚举三次全量扫描可合并
问题：on_truncate 与 remove_addr_flush_files 与 recover_all_trees_from_dir 各做一次 read_dir 全量枚举，同目录高频扫描。
rust：wedb/wbftree/src/manager/replication.rs fn RangeIndexManager::on_truncate fn RangeIndexManager::remove_addr_flush_files fn RangeIndexManager::recover_all_trees_from_dir fn parse_flush_file_name
对应 C#：libs/server/Resp/RangeIndex/RangeIndexManager.cs fn OnTruncateImpl fn EnumerateFlushFiles；libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/RangeIndexSnapshotReader.cs
动作：parse 已单点，枚举侧收敛为一次扫描多路分发或注明低频免合。
13. wdev segmented_device 1437 行要拆
问题：单文件承载设备元数据、句柄表、读写快慢路径、截断回收、DirectIO 探测、debug 守护，远超三辅文件体量。
rust：wedb/wdev/src/segmented_device.rs struct SegmentedDevice 全文件；辅文件 wedb/wdev/src/chunk.rs struct SegmentChunks，wedb/wdev/src/device.rs trait Device，wedb/wdev/src/sys.rs
对应 C#：libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs；libs/storage/Tsavorite/cs/src/core/Device/ManagedLocalStorageDevice.cs；libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs
动作：按 open/read/write/truncate/sync 拆子模块，Device trait 保持唯一定义。

14. 读缓存与主日志读链判定重叠待收敛
问题：read_cache 三文件与 whlog 读侧都有页驻留判定与链走查，RcVisit 三态与主链 ReadProbeResult/MemRead 各自为政。
rust：wedb/wkv/src/read_cache/mod.rs enum RcVisit；wedb/wkv/src/session/raw/mod.rs enum ReadProbeResult enum MemRead；wedb/whlog/src/scan.rs struct ScanIterator
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs fn FindInReadCache；libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs fn InternalRead
动作：读链判定收敛一处，主从只留转调，枚举不合但判据同源需注释互指。

15. wkv store 目录膨胀门面要收敛
问题：store 下 11 子模块加 cpr_host，WedbStore 定义在 mod.rs 619 行，地址转发、刷盘、GC、回收散各文件。
rust：wedb/wkv/src/store/mod.rs struct WedbStore；wedb/wkv/src/store/addr.rs fn WedbStore::shift_begin_address fn WedbStore::truncate；wedb/wkv/src/store/flush.rs fn WedbStore::on_flush_pages fn WedbStore::flush_all；wedb/wkv/src/store/reclaim.rs fn WedbStore::drain_bftree_release
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs fn ShiftBeginAddress fn Truncate；libs/server/Storage/Functions/GarnetRecordTriggers.cs fn OnTruncate fn OnFlush
动作：截断联动已收敛 after_truncate 单点保持，新增 store 方法先查有无同名转发。

16. GC 与紧缩双驱动要注明主从
问题：wkv gc.rs 841 行后台扫描加换号回收，wcompact LogCompactor 双策略，另有 store/gc.rs 调和入口，新人易误调直连。
rust：wedb/wkv/src/gc.rs struct GcManager struct GcHandle；wedb/wcompact/src/compactor/mod.rs struct LogCompactor；wedb/wkv/src/store/gc.rs fn WedbStore::reconcile_gc_scan；wedb/wkv/src/compact.rs struct WedbCompactionFunctions
对应 C#：libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs；libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs fn IsDeleted；libs/server/Storage/Functions/GarnetRecordTriggers.cs fn IsDeleted
动作：生产只走 WedbStore::compact 与 reconcile_gc_scan，LogCompactor::compact 直调注明测试专用。

17. 索引扩容状态机与分裂内核边界已正保持
问题：split.rs 纯函数与 resize.rs 状态机分工清晰，现状良好。
rust：wedb/windex/src/split.rs fn split_single_bucket fn split_chunk；wedb/wkv/src/store/resize.rs fn WedbStore::grow_index fn WedbStore::split_buckets
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs fn SplitChunk；libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexResizeSM.cs
动作：不拆不合，扩容新增逻辑进 resize.rs，分裂算法只进 split.rs。

18. 溢出池与桶链 coupled 紧但无重复
问题：OverflowPool 两级分块加 Treiber 栈，ChainWalker 在 chain.rs，桶 latch 在 bucket.rs，跨文件跳转多但无重复。
rust：wedb/windex/src/overflow_pool.rs struct OverflowPool；wedb/windex/src/chain.rs struct ChainWalker；wedb/windex/src/table.rs struct HashIndex
对应 C#：libs/storage/Tsavorite/cs/src/core/Allocator/MallocFixedPageSize.cs；libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs
动作：不合，get/get_unchecked 双入口保持，热路径只调 unchecked 并写明挂载不变量。

19. 会话 raw 目录过碎小文件可合并
问题：session/raw 下四分加 write 下五子文件，其中 append.rs 仅 40 行，modify.rs 135 行，多为单函数文件。
rust：wedb/wkv/src/session/raw/write/append.rs；wedb/wkv/src/session/raw/modify.rs；wedb/wkv/src/session/raw/write/mod.rs
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/BlockAllocate.cs；libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs
动作：小文件并入 write/mod.rs 或 raw/mod.rs，read/batch 保持独立。
