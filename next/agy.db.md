review-db 待办：底层引擎 aof bftree 存储

check.js 现状
底层引擎相关实现零缺失。存在 5 组跨模块锁与快照重复锚点待去重；代码层面存在多处单文件过大、双套 2PL 锁设计、读路径变体爆炸、微型小文件碎化与内联测试残留，需全面重构梳理。

1. TxnLockTable 逐桶操作频繁调用闭包与 Arc 增减
问题：TxnLockTable::try_lock_shared / try_lock_exclusive / unlock_shared / unlock_exclusive 内部每次操作单桶均调用 (self.loader)() 生成 Arc<HashIndex>，高频事务加解锁循环产生数十次原子引用计数加减与动态分发开销。
rust：wedb/wtxn/src/txn_lock_table.rs fn TxnLockTable::try_lock_shared fn TxnLockTable::try_lock_exclusive fn TxnLockTable::unlock_shared fn TxnLockTable::unlock_exclusive
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs fn GetBucketIndex
动作：事务加锁上下文进入时 pin 一次钉定索引版本，加锁循环直接在引用的 HashIndex 桶数组上批量操作，消除逐桶调闭包与 Arc 增减。

2. HashIndex 重复实现 2PL 事务锁引擎
问题：HashIndex::acquire_keys_lock_exclusive 在索引层内部实现了多键两阶段锁（排序、去重、自旋重试、回滚），全工程仅 wkv ttl.rs 调用两处。与 wtxn 的 TxnKeyManager / TxnLockTable 形成双套 2PL 机制，违反分层原则。
rust：wedb/windex/src/table.rs fn HashIndex::acquire_keys_lock_exclusive fn HashIndex::acquire_unique_locked_entries；wedb/wkv/src/ttl.rs
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs；libs/server/Transaction/TxnKeyManager.cs
动作：清理 HashIndex 私有 2PL 引擎与自旋退避循环，wkv ttl 改为复用 wtxn 事务锁或单点轻量多桶保护，索引层只保留底层单桶闩原语。

3. TransactionManager 侵入具体业务 API 与堆分配排队命令
问题：transaction_manager.rs 内部直接定义了 get、set、setex、delete、increment、sorted_set 等具体业务数据结构 trait（TxnProcApi），且 txn_proc.rs 中的 TxnQueuedCommandInfo 使用堆分配 String 存储命令名。
rust：wedb/wtxn/src/transaction_manager.rs trait TxnProcApi trait TxnProcReadApi struct TxnWatchApi；wedb/wtxn/src/txn_proc.rs struct TxnQueuedCommandInfo
对应 C#：libs/server/Transaction/TransactionManager.cs；libs/server/Custom/CustomTransactionProcedure.cs
动作：业务过程接口 TxnProcApi 与 TxnWatchApi 移出事务状态机核心文件，收敛到 txn_proc.rs 或 wnode 会话层；命令名由 String 改为 static str 或命令枚举，消除事务排队堆分配。

4. StoreSession 读路径 17 个变体方法组合爆炸与模板重复
问题：session/raw/read.rs 文件 876 行充斥 17 个读方法变体（with_prefix x with_size x unprotected x sync/async），包含 6 处几乎逐字复制的 session_tag_key -> fast_hash -> find_tag_by_hash 模板样板代码。
rust：wedb/wkv/src/session/raw/read.rs fn StoreSession::try_read_tag_in_memory_unprotected fn StoreSession::try_read_tag_sync_unprotected fn StoreSession::read_raw_with 等 17 个读变体
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs fn InternalRead；libs/server/Storage/Functions/MainStore/ReadMethods.cs
动作：抽取统一的读上下文 ReadContext 承接前缀、标签与尺寸标志，由单点 read_core 驱动内存与磁盘回退，消除 10 余个机械包装函数，压降 400+ 行重复代码。

5. session/raw 目录碎化与单函数微型文件
问题：session/raw 目录下存在多个仅包含 1 个函数的微型文件（append.rs 仅 40 行，modify.rs 135 行，rmw.rs 105 行），过度碎化增加了跨文件跳转和维护负担。
rust：wedb/wkv/src/session/raw/write/append.rs；wedb/wkv/src/session/raw/modify.rs；wedb/wkv/src/session/raw/write/rmw.rs；wedb/wkv/src/session/raw/write/copy_to_tail.rs
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/BlockAllocate.cs；libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs；libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs
动作：将 append、modify、rmw、copy_to_tail 整合入 write/mod.rs，写路径集中维护，保持 read 与 batch 独立。

6. wkv range_index stub.rs 职责超载与末尾内联测试
问题：wkv/src/range_index/stub.rs 长达 887 行，混杂集合升阶、存根持久化、就地治愈修补、树锁管理、排空注销 5 大职责，且 746-887 行在业务源码内违规内联 140+ 行集成测试。
rust：wedb/wkv/src/range_index/stub.rs 全文件与末尾 cfg(test)
对应 C#：libs/server/Resp/RangeIndex/RangeIndexManager.cs fn SnapshotTreeForFlush；libs/server/Storage/Functions/GarnetRecordTriggers.cs fn PostCopyToTail
动作：拆分子模块 promote.rs（升阶）、heal.rs（修补治愈）、guard.rs（树锁与排空）；将末尾单元测试迁移到 wkv/tests/store/range_index_stub.rs。

7. wbftree chunk.rs 三大独立序列化组件混聚
问题：wbftree/src/chunk.rs 673 行将分块序列化器、反序列化器、跨节点迁移流读取器三套独立状态机混在单文件。
rust：wedb/wbftree/src/chunk.rs struct RangeIndexChunkedSerializer struct RangeIndexChunkedDeserializer struct RangeIndexMigrationReader
对应 C#：libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs；libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs；libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs
动作：拆分为 chunk/serializer.rs、chunk/deserializer.rs、chunk/migration_reader.rs 三文件并在 chunk/mod.rs 导出，结构 1:1 对标 C#。

8. wdev segmented_device.rs 1437 行巨石文件
问题：单文件承担段映射、句柄池、DirectIO 扇区探测、跨段拆分读写、刷盘同步、物理截断删除、目录恢复扫描与 Device trait 实现。
rust：wedb/wdev/src/segmented_device.rs struct SegmentedDevice 全文件
对应 C#：libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs；libs/storage/Tsavorite/cs/src/core/Device/ManagedLocalStorageDevice.cs；libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs
动作：按 handle（句柄与路径）、io（对齐读写切片）、truncate（截断删除）、recover（元数据恢复扫描）拆分子模块，保持 SegmentedDevice 结构体在 mod.rs 统一对外。

9. waof WalLog::recover 338 行超长单函数
问题：WalLog::recover 单函数从 190 行写到 528 行，揉合帧同步、滑动窗口批量读、伪头与 CRC 过滤、截断点推导、Commit 元数据恢复、环形缓冲区预热，导致 log.rs 膨胀至 734 行。
rust：wedb/waof/src/wal/log.rs fn WalLog::recover
对应 C#：libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLogRecovery.cs
动作：将恢复流式驱动抽取为独立的 waof/src/wal/recover.rs，WalLog::recover 仅作为门面入口调用。

10. waof aof header.rs 5 种协议头汇集 735 行
问题：AofHeaderType、AofHeader、AofShardedHeader、AofSingleLogTransactionHeader、AofShardedLogTransactionHeader、AofChunkHeader 全部在单一文件中手写解析和打包，行数过大。
rust：wedb/waof/src/aof/header.rs 全文件
对应 C#：libs/server/AOF/AofHeader.cs
动作：按基础头、分片与事务头、分块大值头拆分子模块或精简宏编解码，保持对外一致导出。

11. wkv gc.rs 841 行多维度后台回收混杂
问题：文件集成 VDB 换号物理回收、TTL 过期键扫描、日志自适应紧缩触发、BfTree 树注销排空与后台 GcManager 驱动循环，并在文件末尾塞入测试代码。
rust：wedb/wkv/src/gc.rs struct GcManager struct GcHandle fn sweep_vdb fn sweep_expired fn try_compact
对应 C#：libs/server/Storage/Functions/MainStore/GarnetRecordTriggers.cs fn IsDeleted；libs/server/StoreWrapper.cs fn ReconcilePrimaryTask；libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs
动作：按 gc/vdb.rs、gc/ttl.rs、gc/compact.rs、gc/reclaim.rs 拆解，gc/mod.rs 保持门面，测试剥离至 tests/gc.rs。

12. whlog AddressManager 读路径缺乏寄存器级原子快照
问题：is_mutable / is_read_only / is_in_memory 等每个高频判定方法均执行多次 AtomicU64::load(Acquire)，且 snapshot() 是无锁顺序读 7 个字段，并发下可能读出跨字段不一致状态。
rust：wedb/whlog/src/address.rs struct AddressManager fn AddressManager::is_mutable fn AddressManager::is_read_only fn AddressManager::snapshot
对应 C#：libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs；libs/storage/Tsavorite/cs/src/core/Allocator/HybridLogConfig.cs
动作：在批处理或会话读循环前提供局部快照，热路径判定使用快照内寄存器数值直接计算，压降总线原子读争用。

13. wcpr index_ckpt.rs 747 行快照写出与恢复重构
问题：index_ckpt.rs 集中了 64 字节头部编解码、PageAlignedBatch 裸指针分配与 DirectIO 批量写盘、全量桶读取恢复，复杂度高。
rust：wedb/wcpr/src/index_ckpt.rs fn write_index_checkpoint fn read_index_checkpoint_truncated struct PageAlignedBatch
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexCheckpoint.cs
动作：拆分子模块 index_ckpt/header.rs、index_ckpt/write.rs、index_ckpt/read.rs，解耦内存对齐批缓冲与写出状态机。

14. windex table.rs 780 行单文件多职责拆分
问题：table.rs 集中了预取窗口内核、哈希桶探测查找、候选地址收集、CAS 槽位插入更新、单桶闩操作与两阶段锁。
rust：wedb/windex/src/table.rs struct HashIndex 全文件
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs；libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindTag.cs
动作：剥离 2PL 事务锁后，按 table/probe.rs（查找与预取）、table/modify.rs（插入、更新、CAS 死槽回收）、table/latch.rs（桶闩与守卫）拆分子模块。

15. wkv store/keyspace.rs 528 行键空间扫描分配放大
问题：keyspace.rs 在做 KEYS / SCAN / 多库遍历匹配时，逐条记录分配 Vec<u8>，面对大键空间时造成大量堆内存分配和 GC 压力。
rust：wedb/wkv/src/store/keyspace.rs fn WedbStore::scan_keys fn WedbStore::collect_matched_keys
对应 C#：libs/server/Storage/Functions/MainStore/ScanMethods.cs
动作：增加基于零拷贝切片借用的闭包迭代器 scan_keys_with，允许上层直接借用前缀和物理键，按需分配。

16. wkv read_cache 与主日志探测判据重叠
问题：read_cache 模块中的 RcVisit 状态判定与主日志 raw 会话中的 ReadProbeResult 枚举逻辑形态重叠，且 Promote 决策分支散落在 read.rs 中缺乏统一状态机。
rust：wedb/wkv/src/read_cache/mod.rs enum RcVisit；wedb/wkv/src/session/raw/mod.rs enum ReadProbeResult；wedb/wkv/src/session/raw/read.rs fn StoreSession::promote_immutable_to_read_cache
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs fn FindInReadCache
动作：统一读缓存探测与主日志读链的结果映射，收敛 Promote 决策流到单点函数。

17. wcompact 与 wkv store/gc.rs 驱动入口主从不分
问题：wcompact LogCompactor 暴露通用 compact 方法，而 wkv 生产紧缩必须遵循虚拟库 VDB 映射过滤与 CPR 纪元屏障，直接调用底层 compactor 会绕过上层保障。
rust：wedb/wcompact/src/compactor/mod.rs fn LogCompactor::compact；wedb/wkv/src/store/gc.rs fn WedbStore::compact
对应 C#：libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs
动作：wcompact 裸 compact 方法明确标注为底层/测试专用，WedbStore::compact 设为唯一生产合法调用链，杜绝绕道调用。

18. wdev chunk.rs 内存分块与 segmented_device 切片算法双副本
问题：wdev/src/chunk.rs 定义了 SegmentChunks 结构，但 segmented_device.rs 内部又单独实现了一套跨段和扇区对齐的读写切片划分逻辑。
rust：wedb/wdev/src/chunk.rs struct SegmentChunks；wedb/wdev/src/segmented_device.rs fn SegmentedDevice::get_segment_and_offset
对应 C#：libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs
动作：收敛跨段与扇区分片逻辑到 chunk 模块，消除重复的偏移与边界计算代码。

19. wbftree on_flush 两个入口冗余
问题：wbftree/src/manager/flush.rs 中暴露了 on_flush 与 on_flush_address 两个公有入口，前者仅为后者的 None 包装，wkv 生产永远传 Some(addr)，无参入口属冗余代码。
rust：wedb/wbftree/src/manager/flush.rs fn RangeIndexManager::on_flush fn RangeIndexManager::on_flush_address
对应 C#：libs/server/Resp/RangeIndex/RangeIndexManager.cs fn SnapshotTreeForFlush
动作：废弃或删除无参 on_flush，直接使用 on_flush_address 并注明入参语义。

20. wbftree 快照刷盘目录三度全量扫描
问题：on_truncate、remove_addr_flush_files、recover_all_trees_from_dir 各自执行一次 fs::read_dir 全量目录遍历与文件名匹配解析。
rust：wedb/wbftree/src/manager/replication.rs fn RangeIndexManager::on_truncate fn RangeIndexManager::remove_addr_flush_files fn RangeIndexManager::recover_all_trees_from_dir
对应 C#：libs/server/Resp/RangeIndex/RangeIndexManager.cs fn OnTruncateImpl fn EnumerateFlushFiles
动作：统一收敛为单次目录扫描多路分发，或提供共享的 flush 文件迭代器，减少高频 IO 遍历。

21. wrecord header.rs 687 行与位段宏展开清理
问题：RecordHeader 16 字节头集中了 RDH、RecordInfo、FillerWords、Tombstone、TtlValid 等多组位段编解码，内联展开导致文件迅速膨胀至 687 行。
rust：wedb/wrecord/src/header.rs struct RecordHeader 全文件
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs；libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs
动作：保持 RecordHeader 唯一定义不分化，将底层位运算常量和掩码提取为子模块 bits.rs，隔离字段打包细节。

22. wtxn watch_version_map 批量版本推进支持
问题：watch_version_map 仅提供逐 key 单点递增原子版本，多键写（如 MSET、事务提交）时逐个原子更新引发哈希表和缓存行抖动。
rust：wedb/wtxn/src/watch_version_map.rs fn WatchVersionMap::increment_version
对应 C#：libs/server/Transaction/TransactionManager.cs fn IncrementWatchVersion
动作：增加批量版本推进接口 increment_versions_batch，经局部预排或批量锁一次性推进多个键版本，提升事务提交吞吐。
