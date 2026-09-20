# wnode 深层工具函数可见性收敛至 pub(crate)

来源：next/zcode-r5-api.md 问题 3

## 问题

resp::objects::object_store_utils 中 14 个 pub fn 与 storage::session::common 的 read_*_sync 系列，
仅在 wnode 内部消费，外部 crate 零引用。
对 resp 和 storage 模块整体 pub 导致内部工具函数穿透成为跨 crate 暴露面。

## 涉及路径

- wedb/wnode/src/resp/objects/object_store_utils.rs
- wedb/wnode/src/storage/session/common/user_read.rs
- wedb/wnode/src/lib.rs

## 解决建议

1. 无外部消费和测试消费的内部工具函数降级为 pub(crate)。
2. 有 wnode/tests 消费的符号若无法下沉单元测试，添加 #[doc(hidden)] 或限定仅用于测试。
