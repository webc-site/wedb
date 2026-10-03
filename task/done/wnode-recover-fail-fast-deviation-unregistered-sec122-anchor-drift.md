终态注记：合入 f024a0e，deviations.md 册尾顺编新落 §186（恢复失败恒拒启——FailOnRecoveryError 旗标 rust 零消费、恢复面设备错误沿 ? 恒上抛、无续行门，对 C# 缺省吞错带部分数据续行起库的刻意收紧，同谱 §57 wtls fail-fast 先例，并注明旧注锚 §122 悬空错指），六处恢复语境注锚（waof_recover_async_error.rs 头注、service.rs 恢复装配段、database_manager_base.rs/single_log.rs/waof_sublog.rs/garnet_log commit.rs 各 recover_async 头注）统一订正指 §186，零行为改动，定向回归 recover_async_surfaces_device_recovery_error 原样通过。

甄别结论：通过（2026-09-29 主控甄别，定级 P3——deviations.md:311 §122 现为 wext_json 深度面与恢复拒启无关，六处注锚（waof_recover_async_error.rs:4、service.rs:1565、database_manager_base.rs:242、single_log.rs:77、waof_sublog.rs:579、garnet_log/commit.rs:20）指向「恒拒启」行为偏离未登记。C# ServerOptions.cs:117 FailOnRecoveryError=false 缺省吞错续行。修复：册尾顺编新登记+六处注锚改指，零行为改动，回归闭环）

审核结论：通过（2026-09-29 甲轮34-B，P3 级）。六处 §122 引用全在恢复语境复核在位；册内 §122 实为 wext_json 深度宽向且来源票在 done 池——单号双裁冲突成立；「恢复失败恒拒启」全池零命中登记空洞实锤；C# FailOnRecoveryError=false 缺省+门控臂齐全、rust 旗标零消费恒上抛核实。方案：册尾顺编新登记+六处注锚改指，引 §57 wtls fail-fast 先例为真锚，waof_recover_async_error.rs 回归即闭环。无修正意见。

原票面：
恢复失败恒拒启系刻意偏差未入册且六处注释锚 deviations §122 悬空错指

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# 恢复链路全程受 FailOnRecoveryError 旗标门控（garnet/libs/server/Servers/ServerOptions.cs 声明 FailOnRecoveryError = false 缺省），恢复异常时 catch 记日志后带已恢复部分续行起库：AofRecover.cs Recover 的 RecoverReplayDriver catch 臂（FailOnRecoveryError 为真才 throw，否则返回全 -1 AofAddress）、SingleDatabaseManager.cs RecoverCheckpointAsync catch 段、DatabaseManagerBase.cs ReplayDatabaseAOF catch 段、MultiDatabaseManager.cs 三处同门。即 C# 生产缺省形态为「恢复失败不拒启」。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）：rust 侧该旗标零代码消费，恢复失败沿 ? 恒上抛拒启、无续行门，五处生产注释与一处测试头注自陈此系刻意收紧并统一回指「deviations.md §122」。但 2026-09-28 裁决册重建后，§122 已登记为 wext_json 深度上限宽向纯登记（来源 task/done/wext-json-slice-step-zero-error-phase-divergence.md 与 task/done/wnode-checkpoint-recovery-purge-meta-filtered-orphans-leak.md），与恢复拒启裁决无关；而「恢复失败恒拒启」这条刻意行为偏差在 doc/zh/deviations.md 全册与 task 五池票体均无登记（grep FailOnRecoveryError 与 恒拒启 于 task/ doc/ 零命中）。六处 §122 引用全部悬空错指他裁决，且使 §122 单号面临「wext_json 深度 + 恢复拒启」双裁冲突。
3. 逻辑危害确证：无运行时行为危害（拒启行为与测试断言自洽，非缺陷行为）；危害在裁决可追溯链断裂——后续审查者按注释查册将命中 wext_json 深度裁决，误判恢复拒启面「已在册勿报」，或以错误锚为据回改恢复语义复活 C# 吞错续行形；同时违反 deviations.md 册头「号位能对上者即在册」的对偶前提（声称在册而实未在册），恰落本轴面「恢复路径错误处置 fail-fast vs 静默跳过」的裁决登记空洞。

涉及代码：
rust 文件与函数：
wedb/wnode/src/database/database_manager_base.rs: DatabaseManagerBase.recover_database_aof_async（头注 FailOnRecoveryError 收紧段，回指 §122）
wedb/wnode/src/aof/garnet_log/commit.rs: GarnetLog.recover_async（头注回指 §122）
wedb/wnode/src/service.rs: StorageSessionProvider.open_recovered_with_config_and_aof（恢复装配段 aof.log().recover_async 上方注释回指 §122）
wedb/wnode/src/aof/single_log.rs: SingleLog.recover_async（头注回指 §122）
wedb/wnode/src/aof/waof_sublog.rs: WaofSublog.recover_async（头注回指 §122）
wedb/wnode/tests/waof_recover_async_error.rs: 模块头注（回指 §122）

对应 c# 文件与函数：
garnet/libs/server/AOF/Recover/AofRecover.cs: AofProcessor.Recover 与内嵌 RecoverReplayDriver（catch 臂 FailOnRecoveryError 门）
garnet/libs/server/Databases/SingleDatabaseManager.cs: SingleDatabaseManager.RecoverCheckpointAsync（catch 段）
garnet/libs/server/Databases/DatabaseManagerBase.cs: DatabaseManagerBase.ReplayDatabaseAOF（catch 段）
garnet/libs/server/Servers/ServerOptions.cs: FailOnRecoveryError 字段声明（缺省 false）

精炼执行方案：
1. doc/zh/deviations.md 册尾顺位取号新登记「恢复失败恒拒启」条：判据 = FailOnRecoveryError 旗标 rust 零消费、恢复面设备错误沿 ? 恒拒启、无续行门，系对 C# 缺省吞错带部分数据起库的刻意收紧（同谱 §57 wtls fail-fast 双向先例）；符号锚 recover_database_aof_async、GarnetLog::recover_async、WaofSublog::recover_async、waof_recover_async_error.rs；来源挂本票。
2. 六处注释与测试头注的「§122」引用改指新号，纯注释订正零行为改动，不触碰任何恢复逻辑。
3. 测试验证：grep 全仓恢复语境 §122 引用清零；wedb/wnode/tests/waof_recover_async_error.rs 断言原样通过（行为不变回归）。
