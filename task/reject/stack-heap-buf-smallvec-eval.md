# 拒绝：评估 StackHeapBuf 迁移至 SmallVec

## 结论
拒绝迁移。应保留现有的 `StackHeapBuf` 手工轮子。

## 拒绝原因

1. **缓存行对齐与极致内存压缩**：
   `StackHeapBuf<62>` 的设计极其精妙。它利用 `u8` 作为长度字段，62字节容量 + 1字节长度 + 1字节枚举标签，总大小刚好是 64 字节，完美贴合一个 CPU 缓存行（Cache Line）。
   而如果替换为 `SmallVec<[u8; 62]>`，由于 `SmallVec` 内部使用 `usize` (8字节) 来记录长度，会导致整体结构膨胀到 72 字节，跨越两个缓存行，引发性能退化和内存浪费。

2. **内联容量优势**：
   若要强制 `SmallVec` 也保持在 64 字节内存占用，其最大泛型参数只能是 `SmallVec<[u8; 55]>`（55字节数组 + 8字节usize长度 + 1字节枚举和对齐 = 64字节）。相比 `StackHeapBuf<62>` 白白损失了 7 字节的内联空间。这意味着对于 56~62 字节的键，`SmallVec` 会被迫退化为堆分配（Heap Allocation），产生高昂的内存分配开销，而 `StackHeapBuf` 仍能在栈上处理。

3. **SIMD 优化**：
   现有的 `StackHeapBuf` 在 `PartialEq` 等比较操作中，自动集成了 `#cfg(feature = "simd")` 的硬件加速向量比对（`fast_key_eq`），这在频繁的比对场景（如 Hash 表查找）中性能远超标准库或 `SmallVec` 的默认比较。

**总结**：`StackHeapBuf` 是一个经过深思熟虑的面向性能敏感场景的定制数据结构，完美契合 Rust 的零成本抽象和极致性能要求。不应为了一味减少私有代码量而牺牲关键的性能指标。
