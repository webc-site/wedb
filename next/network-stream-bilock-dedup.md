# 服务端与客户端 TLS 网络流封装与 BiLock 状态机收敛复用

来源：next/zcode.design.md 问题 8

## 问题

服务端 wnode（net/stream.rs）与客户端 wconn（network/stream.rs、pump.rs）分别独立实现了一套几乎相同的异步网络流抽象，
各自手写了基于 futures_util BiLock 的读写锁拆分与 poll_read/poll_write 胶水代码，重复度高。

## 涉及路径

- wedb/wnode/src/net/stream.rs
- wedb/wconn/src/network/stream.rs
- wedb/wconn/src/network/pump.rs

## 解决建议

1. 评估将通用的 TLS 双工流与 BiLock 包装抽取至公共网络模块（例如 wconn 内部或公共网络模块）。
2. 让 wnode 与 wconn 共享实现，消除重复胶水逻辑。
