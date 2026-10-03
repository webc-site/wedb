归档注记：合入 957a0af6，SublogsLockGuard RAII 释锁，stored_proc/txn/broadcast 三入队臂对位 C# try/finally

甄别结论：通过（甄别席 J5，2026-09-27，定级 P3——潜伏态，boot.rs:108 强制单分片且字段无用户通路，防未来接通）。亲验：single_log_branch.rs enqueue_stored_proc 分片臂 enqueue_parts(...)? 早退致其后 unlock_sublogs 不可达、enqueue_broadcast_entry 分片臂 enqueue(&payload)? 同形，两处漏释确凿；waof_sublog.rs:314 flush_failures>0 恒抛 FlushFailed 终态在位，磁盘故障劣化静默挂起链成立。C# 三处 try/finally 锁段（GarnetLog.cs :1029-1045/:1106-1121/:1226-1244）逐段亲验，rust 漏 finally 语义。勘误：sharded_log.rs 现路径 wedb/wnode/src/aof/sharded_log.rs（:42 起 lock_sublogs）。RAII Drop 守卫收口，lock/unlock 单机制不变，可落。派沙箱席 c01f。

审核通过（2026-09-27）：逐锚点亲验属实——single_log_branch.rs:344/:357-359?/:361 与 :427/:435?/:437 两处 ? 早退漏 unlock、sharded_log.rs:42-76 慢路径 listener.wait() 无超时、waof_sublog.rs:314 FlushFailed 终态可达（flush_failures 于 committer 失败/settle_flush 写点）、boot.rs:108 强制 ==1 潜伏态（wconf 无用户写通路独立复核）、C# GarnetLog.cs 三处 try/finally 对账无误；方案可落（RAII 守卫优先，仓内 Drop 守卫先例充足）；格式纯粹。备注：票引 task/done/sublog-single-constraint.md 实体缺失（继承自 boot.rs 注释悬引用，断言已独立复核，无碍）。

分片 AOF 广播/事务入队锁段无错误路径释放，enqueue 失败即子日志位图锁永久漏锁

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# GarnetLog.cs 三处锁段均以 try { LockSublogs; 循环 Enqueue } finally { UnlockSublogs } 包裹：EnqueueStoredProc（:1029-1045）、EnqueueTxn 分片臂（:1106-1121）、EnqueueBroadcastEntry（:1226-1244），异常路径必释放子日志位图锁。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wnode/src/aof/garnet_log/single_log_branch.rs 两处：enqueue_stored_proc 分片臂（:344-361，:357-359 enqueue_parts(...)? 早退则 :361 unlock_sublogs 不可达）与 enqueue_broadcast_entry 分片臂（:427-437，:435 enqueue(...)? 早退则 :437 不可达）。循环内 ? 上抛时子日志位图锁永不释放；sharded_log.rs:42-76 慢路径 listener.wait() 无超时，此后一切需锁同位图的事务标记（TxnStart/TxnCommit/EXEC 组）、广播条目（FLUSH/检查点标记）在 lock_sublogs 上永久挂起——磁盘致命故障（enqueue_with_backpressure 返回非 BufferFull 终态错误，如 FlushFailed，waof_sublog.rs:314 flush_failures 非零后恒抛）被劣化为静默挂起。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
触发条件为 aof_physical_sublog_count > 1：生产装配 boot.rs:103-109 强制 ==1 且该字段无 CLI/CONFIG 通路（task/done/sublog-single-constraint.md 核实），当前为潜伏缺陷；但分片内核已有 N>1 集成测试（boot.rs 注释自证「未经 server 面点亮」），未来接通投影即激活，属真实对账缺口而非刻意差异。

涉及代码：
rust 文件与函数：
wedb/wnode/src/aof/garnet_log/single_log_branch.rs:enqueue_stored_proc / enqueue_broadcast_entry

对应 c# 文件与函数：
garnet/libs/server/AOF/GarnetLog.cs:EnqueueStoredProc / EnqueueTxn / EnqueueBroadcastEntry（try/finally 守卫）

精炼执行方案：
1 两处锁段以 RAII 守卫（Drop 释放）或显式 match 收口错误路径释放，对位 C# finally 语义；lock/unlock 单机制不变
2 测试验证点：注入 enqueue 终态错误后 lock_sublogs 可再次获取（不挂起）、后续事务标记正常入队
