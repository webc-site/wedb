# StorageSession 补齐 read_user_async_with_prefix 并优化 bitop_command_slow

来源：next/zcode.my.md 问题 4

## 问题

在 wnode/storage/session 中，已为同步读提供了 read_user_sync_with_prefix 优化，允许外提会话前缀避免循环内重复解析。
但在异步读体系中，StorageSession 仅暴露了 read_user_async(key, f)，未提供带前缀的变体。
read_user_async 内部需要依次探查 String 域、ObjectEnvelope 域和 Meta 域，其内部调用的 read_tag_with 每次都会重新调用 batch.session_prefix()，通过原子读获取 ns/db 并重新执行 Varint 编解码。
在 basic_commands/slow.rs 的 bitop_command_slow 循环中，针对每个源键不仅在外部重复调用 session_prefix() 检查向量索引，还在内部通过 read_user_async 触发多达 3 次重复前缀计算。
同时，bitop_command_slow 传入了 |v| v.to_vec()，强制为每个源键分配新的堆内存。
此外，bitmap_commands.rs 中的同步命令 string_bit_operation 在遍历源键时也漏掉了外提前缀。

## 涉及路径

- wedb/wnode/src/storage/session/storage_session.rs
- wedb/wnode/src/resp/basic_commands/slow.rs
- wedb/wnode/src/resp/bitmap/bitmap_commands.rs

## 解决建议

1. 在 StorageSession 中新增 read_user_async_with_prefix(prefix: &[u8], key: &[u8], f: impl Fn(&[u8]) -> R) -> wkv::Result<UserReadAsync<R>> 方法，内部三次 read_tag_with 替换为 read_tag_with_prefix。原 read_user_async 包装调用它。
2. 在 bitop_command_slow 循环外部单次外提 session_prefix，使用 read_user_async_with_prefix。
3. 在 string_bit_operation 循环外部单次外提 session_prefix，使用 read_user_sync_with_prefix。
4. 优化 bitop_command_slow 中的堆内存分配。
