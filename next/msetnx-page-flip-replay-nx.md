# MSETNX 降级慢路径重放时保持原子 NX 判定

来源：next/zcode.my.md 问题五

## 问题

network_msetnx 在前探全部通过后，try_upsert_batch_sync 遇到页翻转返回 Err(page_id) 降级慢路径。
此时部分键可能已写入日志，若慢路径整命令重放再次前探已写键，
易误判键已存在导致整体返回 0，破坏 NX 语义原子性。

## 涉及路径

- wedb/wnode/src/resp/array_commands.rs
- wedb/wkv/src/session/mod.rs

## 解决建议

1. 在 MSETNX 降级慢路径中，记录已完成前探验证的事务上下文或跳过对本批次内部已写入键的重探。
2. 保持原子提交或回滚语义。
3. 增加多键 MSETNX 跨页写入并发降级测试。
