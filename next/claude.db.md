[清理] 清理 AOF ReadConsistency 无用代码
c#: garnet/libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:ReadConsistencyManager.CheckConsistencyManagerVersion(ref ReplicaReadSessionContext)
rust: wedb/waof/src/log.rs 或 缺失
现状: wedb 移除了多节点兼容的多子日志系统，相关冗余代码完全未引入，当前为干净缺失状态。
方案: 维持缺失状态，彻底摒弃 C# 中处理 `ReadConsistencyManager` 以及 `VirtualSublogReplayState` 的相关代码逻辑。如果检查代码时发现遗留，直接删除。在设计层面上，将此类微软内部相关的机制作为永久不需要实现的死代码剔除。

[优化] AOF 写入与并发处理优化
c#: garnet/libs/server/AOF/AofProcessor.cs:AofProcessor.ProcessAofRecordInternal(int, byte*, int, bool, out bool, long)
rust: wedb/waof/src/log.rs (AOF 日志处理逻辑)
现状: 依然可能存在类似 C# 中的基于自旋等待和独立线程模型的旧模式，缺乏 Rust 原生高并发合并能力。
方案: 彻底重写 AOF 处理逻辑。在 `wedb/waof/src/log.rs` 中，用 `compio` 异步运行时、`wbase::group_commit` 管道与 `crossfire` 无锁 MPMC 通道重构。使用单次折叠合并机制（Batching API），将并发写入请求收集在一个批次中，一次性获取锁和合并刷盘，移除自旋重试和多余的内存分配。

[去重] B+树多层接口去重与合并
c#: garnet/libs/server/Resp/RangeIndex/RangeIndexManager.cs:RangeIndexManager.ri_get_callback(byte[], out byte[])
rust: wedb/wbftree/src/manager.rs 和 stub.rs (RangeIndex Stub)
现状: Rust 端 B+ 树的接口由于对标 C#，可能遗留了多层泛型派发和抽象。
方案: 在 `wedb/wbftree/src/stub.rs` 中，删除由于 C# 泛型兼容所导致的虚函数/接口包装层（如 TsavoriteKV 和 RangeIndex 的抽象中间层）。直接暴露 `&[u8]` 切片而非堆分配的 `Vec<u8>`。统一 `ri_get_callback`，`ri_scan`，`ri_set` 等底层遍历路径，利用 Rust 枚举分发（enum dispatch）保证调用链路内联化与极简。

[清理] 清理陈旧的日志页分配机制
c#: garnet/libs/storage/Tsavorite/cs/src/core/Allocator/MallocFixedPageSize.cs:MallocFixedPageSize.GetPhysicalAddress(long)
rust: wedb/whlog/src/buffer.rs 或 缺失
现状: Rust 端不需要照搬复杂的向后兼容多级固定页分配和 GC 钉住机制。
方案: 利用 Rust 的借用规则和 `compio`，直接分配内存对齐的页缓冲区。剔除 `MallocFixedPageSize` 中的历史遗留状态机、指针锁和多级分页缓存区，完全交给所有权机制管理缓冲区的流转生命周期。

[合并] RecordHeader 位域整合与扁平化
c#: garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:RecordInfo
rust: wedb/wrecord/src/header.rs (RecordHeader 结构)
现状: Rust 端的 `RecordHeader` 的字段散落，导致缓存未命中率上升。
方案: 强制整合。将 Tombstone（墓碑标志）、PreviousAddress、Valid 位与 Epoch 版本合并为一个严格对齐的 64-bit 或 128-bit 位域标量。对外提供 inline 的 Getter/Setter 利用位掩码读取。单缓存行（Cache Line）对齐，彻底杜绝多字段散落造成的内存膨胀和查询时的 Cache Miss。

[拆分] 检查点状态机解耦
c#: garnet/libs/server/GarnetCheckpointManager.cs:GarnetCheckpointManager.InitiateCheckpoint(out Guid)
rust: wedb/wkv/src/checkpoint.rs (状态机逻辑)
现状: 检查点元数据序列化与底层数据页刷盘混合在一起，过于臃肿。
方案: 将检查点的元数据序列化（比如写入单文件并原子重命名）与数据页的刷盘状态机拆分开来。在 `wedb/wkv/src/checkpoint.rs` 中剔除微软 Azure 云存储的后端接口，移除旧版本兼容升级逻辑，只提供基于本地文件系统和 `compio` 的纯异步高内聚刷盘方法。

[优化] KV Storage Session 零拷贝与前缀外提
c#: garnet/libs/server/Storage/Session/StorageSession.cs:StorageSession.BasicGarnetRead(...)
rust: wedb/wkv/src/session/mod.rs (会话操作接口)
现状: 在批处理或遍历场景中，循环内部重复进行 Epoch 注册与会话前缀比对。
方案: 引入闭包视图和借用生命周期。提供带生命周期的 `*_with_prefix` 闭包只读 API，杜绝 `Vec<u8>` 拷贝。在批量操作前执行一次前缀外提（Prefix Hoisting，仅取一次 `session_prefix()`），并在操作入口单次获取并进入 Epoch（在 `wepoch/src/participant.rs`），杜绝循环中反复争用 Epoch，提高吞吐量。

[清理] 压缩回收 (Compaction) 机制精简
c#: garnet/libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs:TsavoriteLog.Compact(...)
rust: wedb/wcompact/src/compactor.rs (压缩核心)
现状: 照搬了 C# 为应对 LOH (大对象堆) GC 抖动而设计的各种复杂对象缓存池策略。
方案: 彻底摒弃内存缓存池机制。重构后台 Compaction 机制，交由 O(1) 推进版本号和写幽灵 Meta 墓碑。采用 Rust 原生迭代器过滤清理冷数据，通过 `compio` 并发线程在后台写出物理新页。确保完全去除冗余的老旧垃圾回收防抖动代码逻辑。

[优化] 日志截断与地址推进
c#: garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AllocatorBase.ShiftBeginAddress(long, bool, bool)
rust: wedb/whlog/src/address.rs (地址推进)
现状: 日志的截断和地址推进逻辑有可能阻塞前台。
方案: 优化 LogicalAddress 和 FlushAddress 的推进机制，采用 O(1) 原子变量操作更新截断地址（BeginAddress）。利用后台 `compio` 线程池异步执行无阻塞的文件系统层面截断（Truncate）操作，避免发生因为物理 I/O 导致的前台主线程等待。

[合并] 纪元管理 (Epoch) 保护范围整合
c#: garnet/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs:LightEpoch.ProtectAndDrain()
rust: wedb/wepoch/src/epoch.rs (纪元保护)
现状: B+树分裂、日志 GC、AOF 刷盘各有独立的锁保护体系，未统一进入纪元，导致写放大。
方案: 将索引分裂保护、回收站清理和日志垃圾截断、以及 AOF 的同步点，全盘纳入单一全局 Epoch 推进机制。去除散落的孤立读写锁。利用无锁环形屏障判定 `SafeToReclaim`，将各类内存和文件的安全回收全部整合在这个统一轻量级 Epoch 机制下处理。
