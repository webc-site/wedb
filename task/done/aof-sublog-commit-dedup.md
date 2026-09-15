# AOF 子日志冗余双层 Group Commit CAS 状态机清理与并发队列注释校正归档

任务目标
清理 AOF 子日志冗余的双层 Group Commit CAS 状态机，统一收敛到底层 wal.commit / wal.commit_to 单轨机制，并校正并发队列注释。

已完成改动

1. 清理 waof_sublog 冗余状态机与收敛提交逻辑
在 wedb/wnode/src/aof/waof_sublog.rs 中：
- 彻底删除 FLUSH_IDLE、FLUSH_RUNNING、FLUSH_PENDING 常量定义。
- 在 WaofSublog 结构体中删除 flush_state: Arc<AtomicU8> 字段，清理 std::sync::atomic 中未使用的 AtomicU8 导入。
- 在 WaofSublog::new 中移除 flush_state 字段初始化。
- 重构 SublogBackend::commit(&self, until_address: i64, cookie: i64)：
  先以 Ordering::Release 记录 cookie。
  底层 wal.commit() / wal.commit_to() 内部已具备 Leader/Follower 级联 Group Commit 流水线（CommitPipelineState），自动合并并发提交并由单一 Leader 串行刷盘，外层无需多余的三态 CAS。
  构造 run_flush 任务调用 wal.commit_to(target)（若 target <= 0 则调用 wal.commit()）。
  若处于运行时上下文则使用 Runtime::try_current().spawn(run_flush(wal)).detach()，否则回落到 thread::spawn + rt.block_on。
- 确认 commit_flush_async 与其他方法逻辑保持自洽。

2. 校正 publish 方法并发队列注释
在 wedb/wpubsub/src/subscribe_broker.rs 中：
- 将 publish 方法上的注释由误导性的“零锁并发入队”校正为“低争用有锁入队（通过 EventWorkQueue 登记，由后台消费任务批量 drain 广播，无 CAS 自旋损耗）”。

3. 任务清单闭环
在 next/glm.md 中更新第 14 条，说明 waof_sublog 的三态 CAS 已彻底清理收敛，仅保留公共抽取项。

验证指标

1. bun ./js/check.js
0 缺失，0 重复，接口契约检查全部通过。

2. ./clippy.sh
0 警告，全仓库 clippy 检查通过。

3. ./test.sh
2098 passed, 0 failed，全量测试与回归门禁测试全部通过。

子代理审查
启动 Rust 代码审查子代理，对照 ./.agents/skills/rust_review/SKILL.md 规范进行深度代码审查。
审查确认 publish 注释校正准确；waof_sublog commit 收敛到底层 wal.commit / wal.commit_to 单轨流水线，消除冗余状态机，通过编译与测试。
