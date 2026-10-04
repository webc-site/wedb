终态：已合入 dev（2026-09-27）。c53c5b0 §166:a page_size::get() 真值化(探针同源同版)+b tick 域 10MHz 登记严禁回改+c TreeCache 超集行登记;runtime probe 锁测

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：a 项 page_size 探针+InfoProvider 注入真值化；b/c 纯登记

审核结论：通过（2026-09-27 独立审核席）。真实性亲验：a 项 garnet_info_metrics.rs:1000-1004 page_size() 恒回 4096、:412 system_page_size 消费、C# GarnetInfoMetrics.cs:118 Environment.SystemPageSize、windex/src/ram/direct_vm.rs:220-221 page_size::get() 探针在位、InfoProvider 注入先例 native_allocator_bytes 在位（wmetric :264 trait 口 + wnode info_provider.rs:353 实现）；b 项「属在册 tick 域选择」注释实锚 :139-140（票面 :152-157 系行号偏移，内容在位），deviations.md「tick 域/频域」零命中、Stopwatch 唯一命中 §25 系 pending 零样本条目非 tick 域选择，虚指指认成立，wbase/src/time.rs:55-56「恒 10 MHz」断言在位，C# LatencyMetricsEntry.cs:10-11 TimeStamp.Seconds(100)；c 项 :581-582 TreeCache 两行无条件输出、:73-77 自陈 C# 无对位、§130（deviations.md:1781）超集行立案先例在位、C# GetDatabaseStoreStats :298 起无 TreeCache 行。方案可落：a 探针+注入先例双在位（真值注入或登记两可），b/c 纯登记零行为码，§130 同款先例。格式纯粹度合格。分流 task/todo/。

wmetric INFO/直方图三处可观测分叉无台账登记（system_page_size 硬编码 / tick 域虚指 / TreeCache 超集行）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# GarnetInfoMetrics.cs:118 system_page_size 读 Environment.SystemPageSize 运行时真值；LatencyMetricsEntry.cs:12-13 直方图上界 Stopwatch.Frequency*100（Linux 为 1e9 域）；GetDatabaseStoreStats（:298-330）恒 30 行无 TreeCache 行。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
三处：
a wmetric/src/info/garnet_info_metrics.rs:1000-1004 fn page_size() 恒回 4096（MEMORY 段首行 system_page_size，DEFAULT_INFO 裸 INFO 即出）——非 4K 页宿主（darwin arm64 16KB、64K 页 aarch64/ppc64 Linux）与 C# 逐字段分叉；仓内真值探针 windex/src/ram/direct_vm.rs:220 page_size::get() 已存在未接，分层可走 InfoProvider trait 注入（native_allocator_bytes 先例）。
b wmetric/src/latency/garnet_latency_metrics.rs:152-157 注释宣称 tick 域选择「属在册」，deviations.md grep Stopwatch/频域/tick 域零命中——注释虚指；wbase/src/time.rs:55「.NET Core Stopwatch.Frequency 恒 10 MHz」断言对 Linux/macOS 不成立（Unix 1e9）。LATENCY HISTOGRAM size 行与 C#-Linux 必然分叉（上界差 100 倍）。
c garnet_info_metrics.rs:581-582 STORE 段 TreeCache.ReservedBytes/BudgetBytes 两行 rust 自研超集（:73-77 自陈 C# 无对位）无条件输出，无 deviations 登记；§130 已为同类超集行（bg_task_health/aof_flush_failures）立案补登，本两行漏网。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
零运行时危害，纯台账/对拍治理缺口：非 4K 页平台对拍逐字段失败、后续审查轮撞 size 行与超集行必然反复疑报且无法判有意偏差。

涉及代码：
rust 文件与函数：
wedb/wmetric/src/info/garnet_info_metrics.rs:page_size / STORE 段装配
wedb/wmetric/src/latency/garnet_latency_metrics.rs:get_resp_histogram

对应 c# 文件与函数：
garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:MEMORY 段 / GetDatabaseStoreStats
garnet/libs/server/Metrics/Latency/LatencyMetricsEntry.cs:直方图上界

精炼执行方案：
1 审核席裁定分流：a 项接 page_size::get() 真值（InfoProvider 注入或 wmetric 直接消费，对齐 C# 运行时真值）或登记；b/c 项补 deviations.md 登记（b 注释改指条目号，c 增超集行条目）
2 测试验证点：a 案 darwin 上 system_page_size 回 16384；b/c 案登记后文档核对收口
