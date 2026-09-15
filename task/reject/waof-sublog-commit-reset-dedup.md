# reject：waof-sublog-commit-reset-dedup 部分意见拒绝记录

来源：next/glm.md 条 22、23（主代理预清理后下发）。

## 一、条 23「第三处刷盘防重入状态机」——整体拒绝（无对象可删）

意见要点：waof_sublog.rs:116-202 的三态 CAS（FLUSH_IDLE/RUNNING/PENDING）是
wal.commit_to 防重入状态机的第三份拷贝，应转调 wal.commit_to(tail) 后删除。

拒绝原因：该状态机已不存在，意见基于过时代码状态。上一轮任务
（task/done/aof-sublog-commit-dedup.md）已删除 FLUSH_IDLE、FLUSH_RUNNING、
FLUSH_PENDING 三态状态机与 flush_state 字段，commit 收敛为纯转发
wal.commit / wal.commit_to。主代理预清理亦确认无残留。

证据：
1. dev@096d9da 的 wedb/wnode/src/aof/waof_sublog.rs 共 301 行，commit
   （106-129 行）内无任何三态 CAS / flush_state，直接构造任务调用
   wal.commit() / wal.commit_to(until_address)。
2. 全文件无 FLUSH_IDLE / FLUSH_RUNNING / FLUSH_PENDING 常量。
3. 防重入与 waiters 单点在 wedb/waof/src/log.rs:CommitPipelineState
   （is_committing + waiters，Leader/Follower 级联），waof 自身权威流水线
   完整保留，未被本次触碰。

结论：条 23 无需任何代码改动，仅归档说明。

## 二、条 22 手段「WalLog 暴露同步 reset 变体」——目标采纳、手段拒绝

意见要点：WaofSublog::reset 手抄四原子属实，改法为 WalLog 暴露同步 reset
变体，waof_sublog 转发。

拒绝部分（手段）：同步 reset 变体在本工程技术栈下无法干净实现——
1. Device::sync_data 为 compio async 接口（wedb/wdev/src/device.rs:203），
   同步变体内部必须 block_on；上游若在异步上下文（如未来 FLUSHDB 慢路径）
   调用，单线程 runtime 嵌套 block_on 直接 panic。
2. commit_lock 是 async_lock::Mutex（wedb/waof/src/log.rs:75），同步上下文
   只能 try_lock（失败语义难定义）或 blocking_lock（compio thread-per-core
   上与同线程异步任务互等死锁风险），与 async commit 的持锁语义形成两套。
3. 同步变体必然复制一份"四原子 + slots + notify"逻辑，恰违背一处定义红线。

采纳替代：SublogBackend::reset 整链 async 化（trait 已有 5 个 impl Future
方法先例，Sublog 为静态 enum 分发无 dyn，该链当前无生产调用方，波及面封闭），
WaofSublog::reset_async 直接 await 权威 WalLog::reset。目标（删手抄四原子、
持锁 + sync_data 串行化）完全达成。
