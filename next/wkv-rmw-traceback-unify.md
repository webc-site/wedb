# RMW 可变区回溯与窗口管理逻辑收敛

来源：next/zcode.db.md 问题 8

## 问题

wkv 内部 RMW 逻辑存在三套实现分叉：
1. session/raw/modify.rs 的 trace_live_mutable_addr
2. session/raw/write/inplace.rs 的可变区 traceback
3. session/raw/write/rmw.rs 与 rmw_window.rs
各自重复编写了对可变区链回溯、IsClosed 检查与 Tombstone 检查。

## 涉及路径

- wedb/wkv/src/session/raw/modify.rs
- wedb/wkv/src/session/raw/write/inplace.rs
- wedb/wkv/src/session/raw/write/rmw.rs
- wedb/wkv/src/session/rmw_window.rs

## 解决建议

1. 抽取通用的可变区链回溯与存活记录判定辅助函数。
2. 将 modify.rs 与 inplace.rs 的原位读改写内核统一收敛至单一标准实现。
