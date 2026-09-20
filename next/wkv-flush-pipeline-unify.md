# wkv 刷盘合并流水线与 whlog PendingFlushList 收敛

来源：next/zcode.db.md 问题 6

## 问题

whlog 内部已实现 PendingFlushList 的刷盘区间贪心合并与 flush_event 通知，
而 wkv/src/store/mod.rs 又外挂了 flush_pipeline（GroupCommitPipeline）。
两套合并流水线层叠，增加了锁与调度开销。

## 涉及路径

- wedb/wkv/src/store/mod.rs
- wedb/wkv/src/store/flush.rs
- wedb/whlog/src/flush.rs
- wedb/whlog/src/hlog/io.rs

## 解决建议

1. 消除 wkv 层的冗余 flush_pipeline，直接调用底层 whlog 的刷盘与合并机制。
2. 保持对已完成刷盘区间的等待与唤醒语义一致。
