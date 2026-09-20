# WaofSublog 与 WalLog 提交与位点双层冗余去除

来源：next/zcode.db.md 问题 2 与问题 3

## 问题

1. waof::WalLog 内部已实现基于 GroupCommitPipeline 的提交合并流水线，但 WaofSublog 外部又封装了一层 commit_wake 通道与 committer_loop，导致底层流水线单线程空转。
2. WaofSublog 持有私有 cookie 与 committed_begin 原子量，与 WalLogInner 内部的物理状态双重维护且需手动同步。

## 涉及路径

- wedb/wnode/src/aof/waof_sublog.rs
- wedb/waof/src/wal/flush.rs
- wedb/waof/src/wal/log.rs
- wedb/wnode/src/aof/single_log.rs

## 解决建议

1. 消除 WaofSublog 的外部冗余 committer 通道，让各会话直接并发调用底层 WalLog::commit_to 复用 GroupCommitPipeline。
2. 统一以 WalLogInner 维护的 cookie 与 committed_begin 为单一真源，删除 WaofSublog 的冗余私有原子状态。
