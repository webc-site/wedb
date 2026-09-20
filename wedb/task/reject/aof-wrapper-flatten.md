# aof-wrapper-flatten 拒绝记录：主体方案「四层收敛两层」不成立

票面断言「C# 对位只有两层（GarnetAppendOnlyFile.cs 与 TsavoriteLog.cs）」
经 C# 原码核实为误判。实际 C# 拓扑同样是多级包装，rust 侧结构已与 C# 对齐。

## C# 原码证据

C# 侧包装链（garnet/libs/server/AOF/ 下四类并存）：

1. GarnetAppendOnlyFile.cs（261 行）：持有 seqNumGen、readConsistencyManager、
   backpressure、InvalidAofAddress/MaxAofAddress、GetVirtualSublogIdx、
   CreateOrUpdateKeySequenceManager、ResetSequenceNumberGenerator、
   ComputeAofSyncReplayAddress/DataLossCheck——真状态层，非空心。
2. GarnetLog.cs（1276 行）：持有 singleLog/shardedLog 双拓扑字段、
   usingSingleLog/usingSinglePhysicalLog、physicalSublogCount/replayTaskCount、
   backpressure 缓存、cookieGeneratorCallback、HASH 分片与全部
   Enqueue*/Commit*/Wait*/Scan*/TruncateUntil——真状态层，非空心。
3. SingleLog.cs（51 行）+ ShardedLog.cs（196 行，含 lockMap CAS 位图锁）：
   地址向量包装与子日志集合/位图锁——真状态层，非空心。
4. TsavoriteLog（libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs）：
   物理日志真身。

即 C# 就是 GarnetAppendOnlyFile → GarnetLog → SingleLog/ShardedLog →
TsavoriteLog 四级拓扑。rust 侧 GarnetAppendOnlyFile → GarnetLog →
SingleLog/ShardedLog → WaofSublog（对标 TsavoriteLog + StoreWrapper.
CommitTaskAsync 合体面，见 waof_sublog.rs 头注）→ waof::WalLog（TsavoriteLog
物理机制的 crate 化拆分）与之一一同形对齐，多出的仅是 WaofSublog/WalLog 拆分，
二者合起来对位 C# TsavoriteLog 一层。

## rust 四层逐字段核实（票面硬约束 1 要求实测为准）

1. GarnetAppendOnlyFile：seq_num_gen、read_consistency_manager、
   vector_manager、multi_log_enabled、尺寸/漂移旋钮——C# 同名类同位字段，
   真状态。
2. GarnetLog：single_log/sharded_log 拓扑容器、backpressure、seq_num_gen、
   auto_commit、路由计数；事务头（AofSingleLogTransactionHeader/
   AofShardedLogTransactionHeader）编码与分块原子入队（enqueue_span_chunked
   → enqueue_frames 单次连续预留）全在本层——正是硬约束所指「事务头等
   真实状态」，插花重组的写端不变量落点，不可删。
3. WaofSublog：cookie、committed_begin、commit_wake/commit_request/
   flush_event + 常驻提交协程（对标 StoreWrapper.cs:CommitTaskAsync）——
   真状态，非空心；i64 地址口径统一与扫描错误面均为真实逻辑。
4. waof::WalLog：物理日志真身（环形缓冲、成帧、GroupCommitPipeline），
   waof crate 内核，非包装层。

结论：四层各有独立业务状态，「中间两层无独立业务状态」断言不成立，
「收敛为两层」无从谈起，拒绝删层。

## 落地部分（票面甄别后存留的空心面）

GarnetAppendOnlyFile 门面上 5 个 C# 无对应的纯 1:1 转发方法删除，
调用点改 `aof.log().X` 直通（对标 C# 调用方形态 `appendOnlyFile.Log.X`）：

- truncate_until_async（→ GarnetLog::truncate_until_async）
- reset_async（→ GarnetLog::reset_async）
- commit_flush_async（→ GarnetLog::commit_async）
- recover_async（→ GarnetLog::recover_async）
- wait_for_commit_async（→ GarnetLog::wait_for_commit_all_async）

保留的非空心门面成员：tail_address（全子日志 max 聚合）、
wait_for_commit（全子日志 flushed>=committed 聚合）、backpressure（C#
appendOnlyFile.backpressure 公共字段的对位访问器）、enqueue_safe_flush_aof_
if_primary（IsPrimary 门控逻辑）。

## 其余拒绝子项

- 「GarnetAppendOnlyFile 直接持有 waof::WalLog」：将把 GarnetLog 的路由/
  事务头/分块/背压面整体塞进门面层，C# 拓扑无此形态，复杂度不降反升。
- 删 SingleLog/ShardedLog：C# 同名类对标（ShardedLog 位图锁真状态），
  删除即偏离对标。
- 未引入任何新 trait 间接层替代被删方法；wtxn::TxnAofLog 为既有跨 crate
  解耦口，不在本票射程。
