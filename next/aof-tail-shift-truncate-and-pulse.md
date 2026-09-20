# FastAofTruncate 尾页主动截断与空闲脉冲唤醒保底

来源：next/zcode-r5-repl.md 问题 5 与问题 6

## 问题

1. C# 注册 LogShiftTailCallback，日志尾换页即按只读地址主动 SafeTruncateAof，控制段文件增长。Rust 当前仅在检查点与清库时截断。
2. C# 在背压停顿（frozen tail）时靠周期脉冲解锁跨子日志重放屏障，Rust 节流环空闲防抖收尾一次后深度休眠停发。

## 涉及路径

- wedb/wnode/src/database/database_manager_base.rs
- wedb/wedb/src/server/replication/aof_replication_pump.rs
- wedb/wedb/src/server/replication/aof_sync_task.rs
- libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs
- libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs

## 解决建议

1. 在 waof 换页或分段封口处增加安全截断钩子或基于水位周期触发 safe_truncate_aof。
2. 空闲态按 aof-tail-witness-freq 周期唤醒脉冲节流，维持跨子日志屏障解锁能力。
3. 结合 sublog-fanout 共同对齐。
