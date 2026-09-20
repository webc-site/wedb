# 防止 RIPROMOTE 与 RIRESTORE 治愈状态被陈旧存根写回覆写

来源：next/zcode.my.md 问题二

## 问题

acquire_tree_write 执行 RIPROMOTE 清除 is_flushed 或 RIRESTORE 恢复句柄时，
仅更新了存储记录与局部变量，TieredCtx.stub 仍保持锁外读取的陈旧快照。
tiered_guard 内的 refresh_tiered_meta 仅刷新 meta 未刷新 stub。
写操作成功后 save_tiered_meta 调用 save_bftree_meta_stub 写回，
导致刚治愈的存根状态被陈旧快照静默回滚覆写。

## 涉及路径

- wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs
- wedb/wkv/src/range_index/heal.rs

## 解决建议

1. 在 refresh_tiered_meta 或 acquire_tree_write 成功后同步更新 TieredCtx.stub 的内存状态。
2. save_tiered_meta 写回时基于最新治愈状态构造或比对存根。
3. 增加治愈后紧接着写操作的集成测试，验证 is_flushed 标志不被回滚。
