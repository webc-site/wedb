# HybridLogScanMetrics 替换为固定统计矩阵

来源：next/zcode.db.md 问题 12

## 问题

wkv/src/store/hlog_scan.rs 中，RegionMetrics 包含 String 名称和 HashMap<String, usize> 状态索引。
每次 add_scan_metric 时均进行字符串匹配或克隆，产生哈希查表与堆分配开销。

## 观点甄别

对照 C# Garnet（libs/server/Databases/DatabaseManagerBase.cs:CollectHybridLogStats）与 wedb 现有实现：
1. 扫描范围为 [head, tail)，对齐 Garnet [HeadAddress, TailAddress]。
2. 区域由 addr >= read_only 二分为 Mutable 与 Immutable 两个区域。C# 使用 Immutable 而非 ReadOnly，且扫描不覆盖磁盘冷区（< head），无 OnDisk。
3. 状态按 wedb 状态机已收敛为 Live、RCUdUnsealed、Tombstoned 三个状态。
4. 原提议用固定矩阵替代 String 与 HashMap 的方向完全正确。

## 细化方案

1. 定义固定枚举
- ScanRegion：Immutable(0)、Mutable(1)
- ScanState：Live(0)、RCUdUnsealed(1)、Tombstoned(2)
- 枚举提供 as_str、ALL 迭代常量与数组下标映射。

2. 重构 HybridLogScanMetrics
- 去除 RegionMetrics 结构体及其内部的 String 和 HashMap。
- 使用固定维度数组 [[MetricEntry; 3]; 2] 存储 (count, size)。
- add_scan_metric(region: ScanRegion, state: ScanState, size: i64) 直接通过枚举下标累加计数与字节数，零堆分配、零哈希查表。
- dump_scan_metrics_info 保持输出格式不变，按地址顺序遍历，仅输出 count > 0 的区域与状态，无数据返回空字符串。

3. 重构扫描逻辑与类型定义
- ScanVerdict::Pending 中 region 字段由 &'static str 改为 ScanRegion。
- hlog_scan_metrics 内部判定直接使用 ScanRegion 与 ScanState 枚举。
- 移除不再需要的字符串常量与哈希表依赖。

4. 验证与回归
- 运行 cargo check 确保编译无误。
- 保证测试 wedb/wkv/tests/hlog_scan.rs 逻辑与格式兼容。
