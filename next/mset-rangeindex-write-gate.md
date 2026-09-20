# MSET 与 MSETNX 批量写入接入 RangeIndex 写门

来源：next/zcode.data.md 问题 3

## 问题

在基础字符串单键写入口 network_set 中，前置了 ri_write_gate 检查。若目标键存在存活的 RangeIndex 元记录（KeyTag::Meta 且 collection_type == RangeIndex），则拒绝覆写并返回 WRONGTYPE，防止破坏 BfTree 树文件与留下孤儿元数据。
但在 network_mset（快路径）与 slow::mset（慢路径）中，直接将全量键值对送入 store.try_upsert_batch_sync 与 storage.upsert_string，完全绕过了 ri_write_gate 检查。
客户端若调用 MSET ri_key value，会直接向存储写入普通 String 记录，覆盖 RangeIndex 存根但保留磁盘索引树，造成元数据不一致和孤儿索引树文件损坏。

## 涉及路径

- wedb/wnode/src/resp/array_commands.rs
- wedb/wnode/src/resp/basic_commands/set.rs
- wedb/wnode/src/storage/session/storage_session.rs
- wedb/wnode/tests/range_index_wrongtype_gate.rs

## 解决建议

1. 在 network_mset 中，遍历键前置调用 ri_write_gate 进行检查；若遇到 Blocked 直接终止并返回，若遇到 Deferred 则降级慢路径。
2. 在 slow::mset 中，对写入的各个键异步调用 storage.ri_write_gate_async 检查，若存在 RI 键则写出 WRONGTYPE 错误帧并终止。
3. 补充针对 MSET 覆写 RangeIndex 键返回 WRONGTYPE 的测试用例。
