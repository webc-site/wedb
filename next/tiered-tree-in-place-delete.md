# 分层树内就地删除与成员 TTL 驱逐支持

来源：next/zcode.my.md 问题一与问题九

## 问题

分层集合树内未实现 HDEL、SREM、SPOP、ZREM、LPOP、RPOP 以及 HEXPIRE 等删除与成员过期命令，
全部降级至 run_async_rmw 物化通道：先执行全树扫描全量反序列化为 wcol 内存对象，
在内存完成删除后再重新建树落盘。
大集合单次删除产生严重读写放大。
同时 HLEN、ZCARD 在越过 next_expiry 时触发全树扫描重建换树，导致计数操作阻塞。

## 涉及路径

- wedb/wnode/src/resp/objects/tiered_collection_ops/mod.rs
- wedb/wnode/src/resp/objects/tiered_collection_ops/hash.rs
- wedb/wnode/src/resp/objects/tiered_collection_ops/set.rs
- wedb/wnode/src/resp/objects/tiered_collection_ops/zset.rs
- wedb/wnode/src/resp/objects/tiered_collection_ops/list.rs
- wedb/wbftree/src/service/ops.rs

## 解决建议

1. 在 wbftree 建立就地删除/墓碑标记机制，消除 ScanIter 连续墓碑尾递归溢出隐患。
2. 为 hash、set、zset 分层执行臂实现树内物理删除与成员过期剔除，避免整树全量反序列化与重建。
3. tiered_count 在过期扫描时采用轻量级增量剔除。
