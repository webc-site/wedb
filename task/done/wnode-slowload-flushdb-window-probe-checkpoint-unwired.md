甄别结论：通过（2026-09-29 主控甄别，定级 P3——双侧亲验：slow.rs:1231 漏斗门禁与 resp_flush_swap_collect.rs:313 反向锁确认拒错系刻意设计；conn_on_store 未接 manager 现码确证夹具缺口；aof_flush_replay.rs:581 正向装配惯例在册；纯测试夹具缺口）

slowload 封窗物化用例窗内注入 FLUSHDB 走未接线连接，管理面缺装配即拒错（门禁 no-fail-fast 揭出，合入未验）

问题分析：
1. 门禁现状确证：./test.sh --no-fail-fast 红 wnode::slow_load_degrade_reload_window::write_arm_window_flushdb_answers_nil，窗内注入 FLUSHDB 期望 `+OK`（真实换号），实得 `-ERR checkpoint channel not configured`。该测试随 d1dc889（slowload）合入，子代理纪律只跑 cargo check，合入后无全量门禁到达本例（fail-fast 先红于 wkv），从未验证过。
2. 根因确证：FLUSHDB 清库唯一漏斗经常驻 SingleDatabaseManager 换号（wnode/src/resp/garnet_api/slow.rs:1231-1234），管理面未装配显式回 RESP_ERR_CHECKPOINT_UNWIRED 系刻意设计（resp_flush_swap_collect.rs:313 反向锁该形态，宁可拒错不静默退回 store 直调）。测试夹具 conn_on_store 产出的注入连接未接 database_manager，窗内注入 FLUSHDB 必然撞拒错门——夹具缺口，非引擎缺陷。同文件正向形 aof_flush_replay.rs:581 `with_database_manager(mgr)` 为既有装配惯例。
3. 危害面：纯测试夹具缺口；用例本意（封窗物化窗内 FLUSHDB 换号 → 装载落入新代空域 → sealed Ok(None) → SPOP nil 短路）因注入未真实换号而从未生效。

涉及代码：
rust 文件与函数：
wedb/wnode/tests/slow_load_degrade_reload_window.rs:write_arm_window_flushdb_answers_nil（:326-331 注入闭包 host_exec 直调 FLUSHDB）

对应 c# 文件与函数：
garnet/libs/server/Resp/BasicCommands.cs:ExecuteFlushDb → storeWrapper.FlushDatabase（清库经 databaseManager 漏斗，无直调旁路）

精炼执行方案：
1. 补漏斗面注入连接 conn_on_store_funnel：GarnetDatabase::new(0, store, device, cp_dir, None)（无 AOF 形态对标 C# !EnableAOF）+ SingleDatabaseManager::new + with_database_manager（aof_flush_replay 正向形同构），注入闭包改经 host_exec_funnel 执行 FLUSHDB
2. 验证：wnode::slow_load_degrade_reload_window 全绿 + 全量门禁 --no-fail-fast 全绿

查重：task 五池无同票。

终态注记（2026-09-29 执行收口）：
修复合入 ac22e75（并发席现场代收本席工作区）。
收口形态：新增 conn_on_store_funnel / host_exec_funnel——GarnetDatabase::new(0, store, device, cp_dir, None)（无 AOF 形态对标 C# !EnableAOF，safe_flush_aof aof=None 直跳）+ SingleDatabaseManager::new + with_database_manager 装配（aof_flush_replay.rs 正向形同构），write_arm_window_flushdb_answers_nil 注入闭包改经漏斗面执行 FLUSHDB 真实换号。
验证：slow_load_degrade_reload_window 全绿；全量门禁 5264/5264 绿。
