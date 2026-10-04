同步 wait_for_commit 缺致命失败出口，提交协程死亡后等待方永挂（C# cannedException 臂漏项）

审核结论：通过（审核席，2026-09-30，潜伏态低级维持）
1 真实性亲验：commit.rs:222 wait_for_commit 循环（:230-244）仅查 committed_until_address，Spin/Yield snooze、Sleep 档 :237 挂 flush_event listener，全函数零 flush_failures 检查；waof_sublog.rs ensure_committer Runtime::new 失败臂 :187-196（:194 fetch_add + :195 notify 一次性）、committer_loop 错误臂 :241-244（:242 置数 + :246 notify），panic 臂 supervise_task 捕获后线程退出连 notify 都无；enqueue_with_backpressure 同源绝对判定臂 :314-316（Sleep 注册后复查 :337-339）。死亡臂 notify 至多唤醒一轮，复查水位未达即重新 listener.wait()，此后再无唤醒源，永挂推演成立。
2 危害定级复核属实：同步 wait_for_commit 生产零调用，全仓消费仅测试三处（wnode/tests/garnet_log.rs:130、wedb/tests/primary_live_repl_offset.rs:89/:107）；waof_sublog.rs:679 是 WalLog async 同名异符号（flush.rs:180），生产链确走 async 面，潜伏态低级维持。
3 非重复：done 池 wnode-aof-sharded-enqueue 票判据面为分片 enqueue 臂锁泄漏（flush_failures 仅作既有终态引用）；done 池 waof-group-commit-leader-cancel 票与 deviations.md §171（doc/zh/deviations.md:433）均在 wbase GroupCommitPipeline LeaderGuard/通道轴，与本票同步等待循环缺致命出口判据面不重叠；issue/todo 池无同轴票。
4 架构合规：复用 flush_failures 计数 + Error::FlushFailed 既有单机制，无第二错误通道。
5 勘误（不影响判定）：票面 C# 行号错置——:869-870/:915-916 是 TryEnqueue 单条/批量臂的 cannedException 检查；同步 WaitForCommit 本体（:1845）循环内每轮检查在 :1852。契约实质（提交线程死亡即上抛、绝不无限等待）两处均成立。
6 执行方案补正：wait_for_commit 现签名无返回值，上抛须改 -> waof::Result<()> 并适配上述三处测试调用方；sublog.flush_failures() pub getter（waof_sublog.rs:132）在位可直接读数，Sleep 档注册后复查处（:238 水位复查旁）加判定即可覆盖「注册前失败」窗，与 enqueue_with_backpressure 同形。

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# TsavoriteLog.cs:WaitForCommit 自旋等待循环每轮检查提交线程致命异常（:869-870、:915-916 if (cannedException != null) throw cannedException），提交线程死亡即上抛可见错误，绝不无限等待。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   rust wedb/wnode/src/aof/garnet_log/commit.rs:GarnetLog::wait_for_commit（:222）while committed_until_address < target 循环按 Spin/Yield/Sleep 三档退避，Sleep 档挂 flush_event listener；常驻提交协程死亡（waof_sublog.rs:ensure_committer 失败臂、committer_loop 错误臂置 flush_failures）后 notify 只唤醒一轮，等待方复查水位未达标重新挂起，永不检查 flush_failures，无限循环。同文件同源的 enqueue_with_backpressure（waof_sublog.rs）对同一 flush_failures 有绝对判定返回 FlushFailed，本函数漏掉同面收口。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   whlog 为库级同步 API，嵌入方经此口等待提交时磁盘致命故障后线程永挂而非拿到错误；与 enqueue 臂「宁可显式错误绝不静默挂起」契约自相矛盾。当前生产等待链走 async 面（wait_for_commit_all_async → wait_for_commit_async → commit_to，等待者自驱刷盘、错误沿 await 上浮）不受影响，属库面潜伏漏项，危害定级低。

涉及代码：
rust 文件与函数：
wedb/wnode/src/aof/garnet_log/commit.rs:GarnetLog::wait_for_commit
wedb/wnode/src/aof/waof_sublog.rs:ensure_committer
wedb/wnode/src/aof/waof_sublog.rs:enqueue_with_backpressure

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:WaitForCommit

精炼执行方案：
1. wait_for_commit 循环每轮（至少 Sleep 档挂起前）检查 sublog flush_failures，置位即按 enqueue_with_backpressure 同款 FlushFailed 错误形态上抛（单机制，不另立第二错误通道）。
2. 测试验证点：伪造提交协程死亡（flush_failures 置位）后调 wait_for_commit，断言有限步内返回错误而非永挂。

终态注记：
- 合入收口形态：GarnetLog::wait_for_commit 签名返回 waof::Result<()>，在等待循环与 Sleep 挂起阶段全流程增加 sublog.flush_failures() > 0 致命失败检查并即时上抛 Err(waof::Error::FlushFailed)，消灭提交协程死亡后等待方无限死等隐患；已适配全部现有测试调用方并补充模拟刷盘失败与挂起唤醒的致命错误退出测试。
- 合入哈希：48c8dde
- 状态：已收口归档。

