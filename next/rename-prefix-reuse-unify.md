# rename_slow 与 rename_sync 全链路复用外提前缀

来源：next/zcode.my.md 问题 5

## 问题

在 key_admin_commands/slow.rs 的 rename_slow 函数中，入口处第 77 行声明了 let prefix = storage.batch.session_prefix();，
但在旧键物理域三探中，调用的却是无前缀版本的 storage.read_tag_with，使得入参 prefix 未被使用，内部重复计算了 3 次前缀。
在 key_admin_commands/keys.rs 的 rename_sync 函数中，前缀获取散落在各处分支（第 465 行、第 513 行、第 532 行），且中间的 ttl_of_sync 与 etag_of_sync 未复用已外提的前缀。

## 涉及路径

- wedb/wnode/src/resp/key_admin_commands/slow.rs
- wedb/wnode/src/resp/key_admin_commands/keys.rs

## 解决建议

1. 在 rename_slow 中改用 storage.read_tag_with_prefix，传入已外提的 prefix_slice，消除重复计算。
2. 在 rename_sync 函数入口处单次提取 prefix，并在后续各分支（如 probe_alive_with_registry、registry_alive、rename_vector_set_sync、vm.delete_vector_set 等）统一复用该前缀切片。
