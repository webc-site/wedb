终态：已合入 dev（2026-09-27）。31c0038 §165 登记:gc_* 四行恒0/峰值同源复制/非Linux -1 哨兵三形,纯台账零行为

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：纯台账登记三处；禁混接第二真值源；c 项发散行实测 8 行

审核结论：通过（三坐实点亲验属实：gc_* 四枚字面 0 且系 dotnet GC 专有四件套无 rust 对位物、paged/peak 两函数体逐字同读 VmData、非 Linux /proc 探针全落 -1；C# 真值源 GarnetInfoMetrics.cs:113-114/139-142 与 SystemMetrics.cs:77-93 亲验；deviations 与五池零覆盖，登记为主处方与 §130/slowlog 同案型。方向裁定：paged/peak 维持登记不取 fetch_max 真峰值化，避免冷路径新增状态）

INFO MEMORY 段 gc_* 四行恒 0、peak_paged 同源复制、非 Linux 宿主 proc 族恒 -1 三处值形态分叉无台账登记

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# PopulateMemoryInfo（garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:113-114、139-142）经 GC.GetGCMemoryInfo 取运行时真值填 gc_committed_bytes、gc_heap_bytes、gc_managed_memory_bytes_excluding_heap（= TotalCommittedBytes - HeapSizeBytes 差值）、gc_fragmented_bytes 四行；proc_paged_memory_size 与 proc_peak_paged_memory_size 两行分别取 SystemMetrics.cs:77-81 cproc.PagedMemorySize64 与 :89-93 cproc.PeakPagedMemorySize64 两个独立属性，峰值行有独立峰值语义。MEMORY 段属 DefaultInfo（:18-30），裸 INFO 即出。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
三处值形态与 C# 逐字段分叉且三处均未经 deviations.md 登记（全册 grep gc_committed / gc_heap / GetGCMemoryInfo / SystemMetrics / MemTotal 零命中，五池亦无在册票；在途 todo 票 wmetric-info-observable-divergence-registry-trio 射程为 system_page_size / 延迟直方图 tick 域 / TreeCache 超集行三案，不含本三处）：
a wmetric/src/info/garnet_info_metrics.rs:424-427 gc_* 四行以字面 0 恒出（m("gc_committed_bytes", 0) 等四枚），仅相邻 native_allocator_bytes 一行经 InfoProvider 接真值（:428）；
b wmetric/src/info/garnet_info_metrics.rs:947-967 MEM_SOURCE_PAIRS 第三、四对指向 wmetric/src/system_metrics.rs:36-39 get_paged_memory_size 与 :53-56 get_peak_paged_memory_size，两函数体逐字相同同读 VmData:（文件注自陈「VmPeak 的数据段近似不可得，取 VmData」），proc_peak_paged_memory_size 行恒等于 proc_paged_memory_size 行，峰值语义整行丢失；
c wmetric/src/system_metrics.rs:100-124 全部经 /proc 文件读取，/proc 不存在之宿主（darwin 开发测试机即此形态）total_system_memory 与 proc_* 族除 available_system_memory、proc_pageable_memory_size（两行 C# 非 Windows 亦恒 -1，两侧同形不计）外共九行恒落 -1 哨兵，而 C# 非 Windows 路径（GC.GetGCMemoryInfo 与 .NET Process 跨平台实现）在 macOS 出真值。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
零运行时危害，纯台账/对拍治理缺口，与 wmetric-info-observable-divergence-registry-trio 同案型：a 项四行对 C# 呈恒 0 假观测面，运维据此判读 GC 压力必误；b 项峰值行恒等当前值行，内存峰值告警场景读到的是复制品；c 项非 Linux 宿主上裸 INFO MEMORY 十一行哨兵与 C# 基线逐字段发散且无登记，后续 INFO 对拍轮撞此三形必反复疑报、无法判有意偏差（§130 超集行立案先例同款后果）。

涉及代码：
rust 文件与函数：
wedb/wmetric/src/info/garnet_info_metrics.rs:populate_memory_info（:424-427 四枚零值行）/ MEM_SOURCE_PAIRS（:947-998）
wedb/wmetric/src/system_metrics.rs:get_paged_memory_size / get_peak_paged_memory_size / meminfo_kb / status_kb

对应 c# 文件与函数：
garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateMemoryInfo（:113-114、139-142）
garnet/libs/server/Metrics/SystemMetrics.cs:GetPagedMemorySize / GetPeakPagedMemorySize / GetTotalMemory

精炼执行方案：
1 审核席裁定分流：a 项以 deviations.md 新条登记 gc_* 四行恒 0（.NET GC 无 rust 对位运行时，行保留作格式兼容位；严禁按 native_allocator_bytes 语义混接第二真值源）；
2 b 项登记为「峰值行同源复制」偏差，或裁定改接 /proc VmPeak 近似形（VmPeak 系虚拟峰值非数据段峰值，语义仍非全等，登记从简优先）；
3 c 项登记「/proc 探针 Linux 生产宿主限定，非 Linux 九行 -1 哨兵」偏差，注记 darwin 对拍跳行口径（与 §130 os 短名登记同款一句）；
4 测试验证点：登记后 deviations.md 与 garnet_info_metrics.rs / system_metrics.rs 注释双向 grep 命中收口，零行为码。
