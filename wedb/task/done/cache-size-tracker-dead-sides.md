cache-size-tracker-dead-sides

问题：CacheSizeTracker 双侧皆死的假活态
rust 侧 wedb/wnode/src/database/cache_size_tracker.rs 把一个仅两个原子 i64
（heap_bytes / read_cache_bytes）加一个 stopped 标志的结构体命名为 C# 的
CacheSizeTracker，并作为 GarnetDatabase.size_tracker 字段存活。写侧
add_heap_size / add_read_cache_heap_size 全仓零调用（仅本文件 cfg(test) 触达），
读侧 heap_bytes / read_cache_bytes 亦零消费者，唯一「活着」的是
start_size_trackers→size_tracker.restart() 翻转停止标志，而 start_size_trackers
本身在全仓也没有生产调用方。整条链累加恒零、读取恒零，属死代码叠加虚设实现。

对照 C#：garnet/libs/server/Storage/SizeTracker/CacheSizeTracker.cs 并非字节估计器，
而是包一层 Tsavorite LogSizeTracker（mainLogTracker / readCacheTracker），职责是按
TargetSize 内存预算驱动主日志与读缓存的淘汰裁剪；AddHeapSize/AddReadCacheHeapSize
只是转调 mainLogTracker.IncrementSize，堆字节记账由 Tsavorite 内部经
store.Log.SetLogSizeTracker 完成（见 Initialize 注释）。INFO memory 侧
Log.CurrentHeapSizeBytes 取自 db.Store.Log.HeapSizeBytes（GarnetInfoMetrics.cs:315），
store_mainlog_memory_target_size 取自 db.SizeTracker.mainLogTracker.TargetSize
（:98）——即 C# 里追踪器提供的是「预算裁剪 + target/total 读数」，从不提供 rust
估计器那种 heap_bytes 读数。GarnetRecordTriggers.cs:21 的 cacheSizeTracker 字段
在该文件内实际未被任何 trigger 方法用于喂入 AddHeapSize（feed 由 Tsavorite 承接），
问题文件据此提出的「接线（在记录序列化处喂 add_heap_size，INFO heap 读
heap_bytes）」方案与 C# 实现不符，属臆造机制，予以否定。

rust 现状与对位：
- 定义：cache_size_tracker.rs:CacheSizeTracker（mod.rs:5/11 再导出）
- 消费者字段：garnet_database.rs:22 use、:48 size_tracker、:73 初始化
- 启动链：single_database_manager.rs:137-142 内联 start_size_trackers、
  :444-446 trait impl；i_database_manager.rs:98-101 trait 方法声明
- 内存压力等价件：wcol/src/lib.rs:45 should_promote / :51 should_demote 已按
  count/heap_bytes 双门限迟滞承担分层换页（doc/zh/collection.md），是 wedb 模型下
  真正的「按内存增减」机制，无需再留全局字节估计器。

判定：不移植（走实现分支落地删除 + ignore 登记，因需改动 rust 源码，非纯观点拒绝）。
理由：追踪器裁剪职责在 wedb 无 Tsavorite 对位、由 wcol 分层迟滞单点承接；已转写的
估计器两侧同死且字段与 C# 不对应，维持假活态违背「死代码直接删 / 无虚设实现 / 不
造额外机制」。

改造步骤：
1. 删除 cache_size_tracker.rs 整文件；mod.rs 去掉 pub mod 与 pub use。
2. garnet_database.rs 去掉 use、size_tracker 字段与初始化。
3. single_database_manager.rs 去掉内联 start_size_trackers 与 trait impl 覆写。
4. i_database_manager.rs 去掉 start_size_trackers trait 方法声明。
5. 保留 DbSnapshot 的 log_heap_size_bytes / readcache heap / target_size 等 INFO 字段
   不动：它们在 C# 分别取 Store.Log.HeapSizeBytes 与 SizeTracker.TargetSize，属 store
   快照投影面（当前 databases() 空返回故显缺省），非本估计器的读取端，不因删除追踪器
   而移除。
6. ignore 登记：CacheSizeTracker.cs 整类；IDatabaseManager.cs / SingleDatabaseManager.cs
   / DatabaseManagerBase.cs / StoreWrapper.cs 的 StartSizeTrackers 函数。

涉及上下游：仅 database 域内部字段与一个无生产调用方的 trait 方法，删除后
check.js 的 SizeTracker/StartSizeTrackers 缺失项由 ignore 收口，INFO 输出面不受影响。

验收：cargo check 通过、无新增警告；check.js 不再报 CacheSizeTracker.cs 与四处
StartSizeTrackers 缺失；grep add_heap_size/heap_bytes(/start_size_trackers 排除测试
与 ignore 文档后全仓零命中。
