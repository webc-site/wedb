# try_upsert_batch_sync 局部排序使用 SmallVec 避免堆分配

来源：next/zcode.my.md 问题八

## 问题

try_upsert_batch_sync 在收集 pairs 时直接使用 Vec<(K,V)> 导致堆分配。
对于常见小批次 MSET（如 2 到 8 个键），每次调用均产生堆分配。

## 涉及路径

- wedb/wkv/src/session/mod.rs

## 解决建议

1. 借鉴 wbftree/bulk.rs 的模式，改用 SmallVec<[(K, V); 8]> 或类似栈上固定容量缓冲承接小批次排序。
2. 超过容量阈值时自然溢出至堆，兼顾常见小批次零分配与大批次正确性。
