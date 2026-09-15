# store-reset-semantics

来源：next/gemini.md 条 4 与 clude.md 条 13 合并（主代理预清理后移交）。

问题：StoreWrapper.Reset 存储级拆除重建未落地，vector_set_cleanup_vs_reset_race.rs 测试注释自证仅闸门锤击近似，缺 Pause → Reset → Resume 三段语义。

## 甄别结论

### C# Reset 全链（已核）

1. StoreWrapper.cs:555 Reset(int dbId = 0)：门面，CheckMultiDatabaseCompatibility 校验后转 databaseManager.Reset(dbId)
2. SingleDatabaseManager.cs:293：dbId 恒等校验（ThrowIfNotEqual(dbId, 0)）→ ResetDatabase(defaultDatabase)
3. MultiDatabaseManager.cs:624：TryGetOrAddDatabase 取库 → ResetDatabase(db)
4. DatabaseManagerBase.cs:265 ResetDatabase(db)：
   - db.Store.Log.TailAddress > 64 时 db.Store.Reset()（日志地址归零、分配器拆除重建）
   - db.AppendOnlyFile?.Log.Reset()（AOF 地址归零）
   - db.LastSaveTime = epoch 0
5. StoreWrapper.cs:646 ResetRevivificationStats：rust 已落地（wdatabase single/multi 空操作，wkv 无 reviv 统计面，trait 在册），无需动

生产消费者（唯一）：副本 re-attach 非 diskless 分支

- ReplicaDisklessSync.cs:112-119：PauseCleanupAsync → storeWrapper.Reset() → ResumeCleanup
- ReplicaDiskbasedSync.cs:143-150：同型

测试消费者：VectorCleanupVsResetRaceTests.cs:DropVectorSetWhileResettingStore（Pause + Reset + Resume 三段锤击 5 秒窗口）

C# Reset 与 FlushDatabase 是不同强度的两族：FlushDatabase = ShiftBeginAddress(Tail) + AOF TruncateUntil(Tail)（地址不归零，不动 LastSaveTime）；ResetDatabase = Store.Reset() + AOF Log.Reset() + LastSave 归零。

### rust 现状

1. store 段：wkv 共享单日志多库 ns/db 前缀物理隔离，C# Store.Reset()（逐库原位拆除重建）无结构性等价；数据清空真身 = wkv flush_database(ns, db) / flush_all_databases()（逐键完整删除：随键 TTL/ETag 清理 + Meta 版本栅栏秒删 + wbftree 排空 + 信封双域删除）
2. wdatabase database_manager_base.rs:283 reset_database：C# ResetDatabase 的共享存储变体已存在（逐键 + AOF truncate_until + 保存点复位）；single/multi 的 flush_database 均走它——C# 的 FlushDatabase 与 ResetDatabase 两族在 rust 折叠为一
3. AOF 段：wnode GarnetLog.reset_async（C# GarnetLog.Reset / TsavoriteLog.Reset 路由面）已落地，但 wdatabase DatabaseAof trait 无 reset 方法，未接入数据库管理面
4. trait 面：IDatabaseManager 无 reset；Single/Multi 无 Reset impl；StoreWrapper.Reset 门面无对应
5. 复制面 re-attach 分支未转写（server/ 域待办，与 wdatabase-orphans 记录一致）→ Reset 当前无生产调用点
6. VectorManager pause_cleanup_async / resume_cleanup 已落地（cleanup gate 语义对齐 C# cleanupGate）

### 方案（等价实现 + 解折叠，禁两套机制）

共享存储约束下，Reset 与 Flush 的数据清空段同体（wkv 逐键删除）；真实语义差在 AOF 段（Reset 归零 vs Flush 截断）与保存点复位。按 C# 拓扑解折叠为两个内核，不造第二套清库机制：

1. wdatabase/src/aof.rs：DatabaseAof trait 补 reset_async（映射 libs/server/AOF/GarnetAppendOnlyFile.cs 的 Log.Reset 路由；`()` 默认空实现）
2. wnode/src/aof/garnet_append_only_file.rs：GarnetAppendOnlyFile impl reset_async → 内部 GarnetLog.reset_async()（复用已有内核，非新建）
3. wdatabase/src/database_manager_base.rs：
   - 新增 flush_database（映射 libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase）：wkv flush_database + AOF truncate_until；不动保存点（C# FlushDatabase 不触碰 LastSaveTime）
   - reset_database 改造：AOF 段 truncate_until → reset_async（对齐 C# ResetDatabase 的 Log.Reset() 口径），保存点复位保持
4. single_database_manager.rs：flush_database 改调 base.flush_database；新增 reset（映射 libs/server/Databases/SingleDatabaseManager.cs:Reset）；trait impl 接线
5. multi_database_manager.rs：flush_database / flush_all_databases 改调 base.flush_database；新增 reset（映射 libs/server/Databases/MultiDatabaseManager.cs:Reset，TryGetOrAddDatabase + reset_database）；trait impl 接线
6. i_database_manager.rs：trait 补 reset（映射 libs/server/Databases/IDatabaseManager.cs:Reset）
7. 测试：vector_set_cleanup_vs_reset_race.rs 改打真实路径——SingleDatabaseManager::reset 作为 Reset 入口（对标 C# 测试经 storeWrapper.Reset 门面），补 Pause → Reset → Resume 三段锤击；闸门锤击保留为并发段之一

### 装配形态对齐

生产接线点（复制面 re-attach）属 server/ 域未转写，Reset 不加零调用门面层；落地形态与 wdatabase-orphans checkpoint 族一致：base 内核 + trait 面 + single/multi impl，wnode 测试从管理器真实入口打。StoreWrapper.Reset 门面等复制面转写时随装配接入（记录待办）。

### 约束

- 避开 wkv 文件（并发去重批在改 wkv 内部）：只用 wkv 公共面 flush_database
- AOF 段语义差通过 DatabaseAof trait 承载，wnode GarnetLog 内核不动
- 无向下兼容包袱：flush/reset 两族边界一次到位

## rust 侧改动点

1. wdatabase/src/aof.rs：trait 补 reset_async
2. wdatabase/src/database_manager_base.rs：新增 flush_database；reset_database AOF 段改 reset_async
3. wdatabase/src/i_database_manager.rs：trait 补 reset
4. wdatabase/src/single_database_manager.rs：新增 reset；flush_database 换路由；trait impl 接线
5. wdatabase/src/multi_database_manager.rs：新增 reset；flush_database / flush_all_databases 换路由；trait impl 接线
6. wnode/src/aof/garnet_append_only_file.rs：impl reset_async
7. wnode/tests/vector_set_cleanup_vs_reset_race.rs：三段锤击（Pause → SingleDatabaseManager::reset → Resume），删除「尚未落地」自证注释
8. js/check/ignore：按 check.js 实际输出登记（若 Reset 映射被识别后无缺失则不动）

## 验收口径

- ./clippy.sh 零警告（禁 allow）；./test.sh 全过；bun ./js/check.js 无新增缺失/重复
- 三段锤击测试通过：Reset 与清理迭代/RMW 在 gate 串行化下无 panic，清理管道照常闭环
- FLUSHDB 生产链（garnet_api 直调 wkv）行为不变；wdatabase flush/reset 两族路由清晰单一

## 验证结果

- 分支：w5-reset（已合并 dev 后回并主干，worktree 已删）
- 静态检查：./clippy.sh 0 警告（禁 allow）
- 自动化测试：./test.sh 全量通过（2056 项；此前一轮出现 wkv test_session_lifecycle_rapid_churn 单点失败，单跑 3/3 稳定、全量复跑未复现，判为全量并发下偶发）
- 检查脚本：bun ./js/check.js 输出 0 缺失 0 重复（过程中消解两处映射重复：wkv keyspace.rs 括注与 GarnetLog.cs:Reset 路由层，均修正为单一真身映射，非绕过检查）
- 三段锤击：vector_set_cleanup_vs_reset_race 改打真实路径（Pause → SingleDatabaseManager::reset → Resume，竞争窗口 124 轮 Reset），清理管道闭环 + 无 panic，总时长 1.2s
- 实际改动：8 文件（+138/-38）；FLUSHDB 生产链（garnet_api 直调 wkv）行为不变；wdatabase flush/reset 两族路由单一（数据清空同体 wkv flush_database，语义差收敛于 AOF 段：截断至尾 vs 位点归零）
- 遗留（他域待办）：StoreWrapper.Reset 门面与复制面 re-attach 接线（ReplicaDisklessSync / ReplicaDiskbasedSync 非 diskless 分支）随 server/ 复制域转写时接入，与 wdatabase-orphans 记录同轨
