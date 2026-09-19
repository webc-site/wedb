裁决：不成立（前提不实：compio Runtime::block_on 在等待期间持续推进本运行时其它任务与 I/O，「同核其他并发连接被暂停」与实现不符；且这是 wbase/future.rs 明文声明的全仓设计决策）
来源：next/agy.net.md 条 1。核销 2026-09-19。

一句话结论：execute_checkpoint_recv 的 blocking_wait 是 wbase 单点同步收割口（对标 C# AsyncUtils.BlockingWait），
在 reactor 线程上走 Runtime::block_on，其循环体每轮先 poll 目标 future、再 run() 推进 executor 队列、
再 poll driver 收割 I/O 事件——其它连接的任务与 I/O 在等待窗口内持续被服务，指控的「暂停」不存在。

逐条核销
1. compio 实现实测：compio-runtime-0.12.6/src/lib.rs:189-204 block_on_at 循环体
   `poll(future) → self.run()（推进 spawn 队列）→ poll_with/poll（驱动 I/O，唤醒的任务下轮 run() 再推进）`，
   非独占自旋。C# 对标的 AsyncUtils.BlockingWait 靠线程池 + 完成回调，同样不冻结其它工作。
2. 本仓设计声明：wedb/wbase/src/future.rs:47-53 文档明确「compio 运行时线程上……走 Runtime::block_on，
   挂起窗口内让位给本运行时的其他任务与 I/O driver，转发/落盘/锁等待得以在调入线程内闭环」，
   且 :3-13 声明全仓生产同步收割收敛为此一处，禁止各处自建第三形态。改挂 SlowWait 属推翻单点设计，需更强证据。
3. 同文件口径并非「一律 SlowWait」：wedb/wedb/src/server/cluster_session/replication.rs:734 注释
   network_cluster_begin_replica_recover / initiate_replica_sync 挂 pending_slow 的理由是「长操作」
   （C# 网络线程同步阻塞等价的慢路径承载）；而 snapshot_data / metadata / file_segment 三帧命令是
   单帧已收数据的处理（:670/:694/:722 三个闭包，process_* 系列逐帧写盘），属短收割，C# 侧
   RespClusterReplicationCommands.cs 同样在网络线程同步执行。短操作走 blocking_wait、长操作挂
   pending_slow 的现分工自洽，无证据表明数据帧命令构成 reactor 饿死源。
4. 指控未给出任何实测（延迟、吞吐、卡顿样例），仅以「使用了 blocking_wait」推断阻塞语义，与
   block_on 实际语义相反。
