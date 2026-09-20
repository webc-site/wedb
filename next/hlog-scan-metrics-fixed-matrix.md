# HybridLogScanMetrics 替换动态 HashMap 为固定统计矩阵

来源：next/zcode.db.md 问题 12

## 问题

wkv/src/store/hlog_scan.rs 中，RegionMetrics 为每个区域维护 String 名称，并在内部维护 HashMap<String, usize> 索引状态名称，
每次 AddScanMetric 都发生字符串匹配或克隆。
实际上扫描指标的区域只有 3 个（Mutable, ReadOnly, OnDisk），状态只有 3 个（Live, Tombstoned, RCUdUnsealed）。

## 涉及路径

- wedb/wkv/src/store/hlog_scan.rs

## 解决建议

1. 将 RegionMetrics 的 String 和 HashMap 替换为固定枚举索引数组或结构体字段。
2. 彻底消除扫描统计中的堆分配与哈希查表开销。
