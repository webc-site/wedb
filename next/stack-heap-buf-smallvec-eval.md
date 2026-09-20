# 评估 StackHeapBuf 迁移至 SmallVec

来源：next/zcode.design.md 问题 14

## 问题

wbase/src/buf.rs 手写了枚举分发的 StackHeapBuf<CAP>。
而全仓已广泛引入 SmallVec，评估将 StackHeapBuf 替代为标准 SmallVec 以减少私有代码量。

## 涉及路径

- wedb/wbase/src/buf.rs
- wedb/wval/src/tag.rs

## 解决建议

1. 评估 TaggedKeyBuf 使用 SmallVec 的性能与体积表现。
2. 若表现一致，清理 StackHeapBuf 手工轮子。
