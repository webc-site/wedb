甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P3（登记级）
核验记录（现码复跑，非票面背书）：
1 rust 锚亲验：migrate.rs:124-127 解码失败回 ERR Invalid migration payload、:155-162 槽门非 IMPORTING 整体拒收回 ERR …is not in importing state（先 reset_receive_states）、frame_import.rs 旧 TTL 清退/记录写回/TTL 回填三败臂均 return Err(RESP_ERR_SLOW_PATH_STORAGE)——全臂显式 ERR 现状属实，缺陷未灭失。
2 C# 锚亲验：RespClusterMigrateCommands.cs:321 `while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK...))` 无条件 +OK 应答点在位（Process 局部函数四臂 migrateState 吞错形态经审核席 r20 亲读，本席复核应答点坐实）。
3 查重亲验：deviations.md 全册 grep migrateState/接收臂 零命中——未登记属实；task/{done,ing,issue,reject} 无同轴票（issue 池 wedb-migrate-* 二票系发送侧编排面，正交）；测试注释半真锚现读 cluster_migration.rs:1037-1039「对标 C# …IsImportingSlot 拒收」确未提恒 +OK，订正需求成立。
4 架构合规与可执行度：纯台账登记 + 注释级订正零行为改动，编号按「先入库者得号顺编让位」纪律自协调（现册尾已至 §151，落笔以当日实况为准）；登记防回改方向（严禁按 C# 吞错形态回改 rust ERR 面）与 §48/§143 同族先例一致。定级 P3：治理面，无运行期危害。

审核结论：通过（审核席 zcode-r20-review-migratereg，2026-09-26，登记级，双侧源码亲验）

亲验记录：
1. C# 四臂吞错与恒 +OK 属实（RespClusterMigrateCommands.cs 现码亲读）：Process 局部函数（:102）内 a) 载荷截断早退（:131-132/:164-165/:170-171/:215-216 return）；b) 槽位非 IMPORTING（:192-194 置 migrateState=1、:285-287 同并 continue）；c) SET 结果丢弃（:201/:295 `_ = basicGarnetApi.SET(in diskLogRecord)`）；d) RI 流失败（:256-261 功能未启用臂与 :264-270 ProcessRecord 回 false 臂均置 migrateState=1）——四臂消费不写入，Process 返回（含早退）后 :321 无条件 TryWriteDirect(RESP_OK)，发送端一律收 +OK；仅意外 kind（:135/:303）抛 InvalidOperationException 掐连一途可察觉（FastMigrate 恒 false 走同步形，§148 随行注记 2 在案）。TrackImportProgress（:57）纯日志计数不参与应答，属实。
2. rust 全臂显式 ERR 属实：migrate.rs:124-127 解码判败回 ERR Invalid migration payload；:155-162 头声明槽门任一非 IMPORTING 整体拒收回 ERR Slot X is not in importing state（复位双接收态）；frame_import.rs:326 旧 TTL 清退失败、:333 记录写回失败、:359 TTL 回填失败均回 RESP_ERR_SLOW_PATH_STORAGE；:145 RI 流失败回显式错误帧。发送端闭环属实：client.rs:406 仅 OK/+OK 判真，send_payload_and_wait（keys.rs:192）非 OK 即 Err → try_recover_from_failure（keys.rs:150）置远端 STABLE、本地 Fail、dispose，不删源键不交权。
3. 查重净：deviations.md 全册 grep migrateState/IsImportingSlot/接收臂/吞错零命中本面；§13（槽管理命令族八臂）、§86（MIGRATE timeout 三态）、§148（MIGRATE 退化形 -NOKEY 与 IOERR 句号，发送侧命令面）均非接收臂应答面，无重复登记；task/issue 与 task/ing 现存票零撞面；审查档 zcode-r20-cluster 第五节第 5 条即本票立案源，口径一致。
4. 方向正确：按 C# 形态回改 rust 应答面即复活「目标端吞记录回 +OK → 发送端停等收 +OK 删源键并继续交权编排 → 该批键两端皆无」的静默丢键窗口，属上游缺陷修复型分叉（§48 同族），严禁回改方向成立；登记级门槛成立（真实分叉 + 全册零登记 + 半真锚在位）。
5. 行号勘误（不碍立案）：测试半真锚注释实际位于 cluster_migration.rs:1038-1040（票面写 :1043，:1041 为 fn 行、:1058 帧锁断言属实）；C# d) 臂 :259 系 RI 未启用臂、ProcessRecord false 判错在 :264-270，同族不碍结论。执行按符号锚取位。

执行方案（整理版，供 task/fix.md 直接消费）：
1. doc/zh/deviations.md 新增登记条：现册尾实况 §150，顺编拟取 §151（先入库者得号、撞号让位顺编不覆写，§148/§149 编号注记同款）。登记 C# 接收臂 migrateState 四臂吞错（截断早退、IsImportingSlot 拒收、SET 结果丢弃、RI 流失败）恒 +OK 应答，对 rust 全臂显式 ERR 判败的修复型分叉；声明严禁按 C# 形态回改 rust 应答面（回改即复活源端删键后目标端无数据的静默丢键窗口）；双侧对拍遇「同批次 C# 回 +OK / rust 回 -ERR」直引本条免复勘。C# 锚用符号锚（Process 局部函数 + TryWriteDirect(RESP_OK) 无条件应答点），防行号漂移（§13 锚形态裁决同款）。
2. 半真锚订正（注释级零行为改动）：wedb/wedb/tests/cluster_migration.rs cluster_migrate_recv_requires_importing_slot 文档注释（:1038-1040）订正为「C# 置 migrateState 吞记录后仍回 +OK，rust 显式 ERR 判败（登记条回指）」；wedb/wedb/src/server/cluster_session/migrate.rs 槽门臂（:155-162）补一行同款回指锚。行为码与既有断言零改动，禁新增行为断言。
3. 验证闭环：因触及 .rs 注释，按 AGENTS.md 跑 ./test.sh（至少 cluster_migration.rs 帧锁 cluster_migrate_recv_requires_importing_slot 与 migrate_fail_inject.rs 失败注入族全绿）；本票登记级，无行为变更面。

CLUSTER MIGRATE 接收臂错误吞没恒 +OK（C# 原型）对 rust 显式 ERR 判败的修复型分叉未登记台账，测试注释半真锚误导后续对账

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 接收臂 Process（garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:102 局部函数）对四类错误全部走 migrateState 置位后继续消费不写入或直接早退：a) 载荷截断（:132/:165/:171/:216 GetSerializedRecordSpan 失败 return）；b) 槽位非 IMPORTING（:192-194 与 :285-287 IsImportingSlot 拒收置 migrateState=1 后仅跳过写入）；c) 记录写回结果丢弃（:201 与 :295 `_ = basicGarnetApi.SET(...)`）；d) RangeIndex 流处理失败（:259/:267 ProcessRecord 回 false 置 migrateState=1）。局部函数返回后主命令臂无条件应答（:321 TryWriteDirect(CmdStrings.RESP_OK)），即任何上述错误形态下发送端都收到 +OK。仅意外 kind 抛 InvalidOperationException 掐断连接一途可被发送端察觉。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 接收臂 cluster_migrate_slow（wedb/wedb/src/server/cluster_session/migrate.rs:114）与导入核心 import_migration_frames（wedb/wedb/src/server/migration/frame_import.rs:91）对同面全部显式 ERR 判败：载荷解码失败回 ERR Invalid migration payload（migrate.rs:127）；头声明槽位非 IMPORTING 回 ERR Slot X is not in importing state（migrate.rs:159，槽门锁测 wedb/wedb/tests/cluster_migration.rs cluster_migrate_recv_requires_importing_slot :1043）；记录写回失败与 TTL 回填失败回 RESP_ERR_SLOW_PATH_STORAGE（frame_import.rs:333/:359）；RI 流失败回显式错误文案。发送端停等（send_payload_and_wait 非 OK 判败）随即走 try_recover_from_failure 收口，绝不交权。该行为正确且方向更严，但 deviations.md 全册（含 §13/§86/§148 迁移域在册条目）无此接收臂应答面登记，migrate.rs:1043 测试注释只写「对标 C# IsImportingSlot 拒收」未提 C# 恒 +OK 应答，属半真锚。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
C# 形态存在真实数据丢失窗口：目标端槽位被并发 recover 回 STABLE（另一失败任务恢复期）后收到在途批次，目标端吞记录回 +OK，发送端停等收 +OK 即删源端键并继续交权编排，该批键两端皆无。rust 侧已结构性消除（ERR 即停等判败 recover），危害面在治理侧：后续双侧对拍或审查轮遇「同输入 C# 回 +OK、rust 回 -ERR」必重复立案撞面；更危险的是若有人按「对齐原型」名义把 rust 改回 +OK 吞错应答，即复活静默丢键窗口，属必须台账防回改的修复型分叉（与 §48 DELKEYSINSLOT 过滤、§143 会话入口摘闸同族）。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/cluster_session/migrate.rs:cluster_migrate_slow（:124-127 解码判败、:131-134 空载荷哨兵、:155-162 槽门判败 ERR）
wedb/wedb/src/server/migration/frame_import.rs:import_migration_frames（:333 写回失败、:359 TTL 回填失败显式 ERR）
wedb/wedb/tests/cluster_migration.rs:cluster_migrate_recv_requires_importing_slot（:1043 注释半真锚、:1058 帧锁断言）

对应 c# 文件与函数：
garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:NetworkClusterMigrate（Process 局部函数 :102-319，migrateState 四臂吞错，:321 无条件 RESP_OK）
garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:TrackImportProgress（:57 仅日志计数，不参与应答）

精炼执行方案：
1. doc/zh/deviations.md 新增登记条（按现册尾实况顺编取号，先入库者得号、撞号让位顺编不覆写）：登记 C# 接收臂 migrateState 吞错四臂（截断早退、IsImportingSlot 拒收、SET 结果丢弃、RI 流失败）恒 +OK 应答对 rust 全臂显式 ERR 判败的修复型分叉，声明严禁按 C# 形态回改 rust 应答面（回改即复活源端删键后目标端无数据的静默丢键窗口），双侧对拍遇「同批次一 +OK 一 -ERR」直引本条。
2. migrate.rs cluster_migrate_slow 槽门臂与 cluster_migration.rs:1043 测试注释补回指锚，订正为「C# 置 migrateState 吞错后仍回 +OK，rust 显式 ERR 判败（登记条回指）」，消除半真锚；行为码与既有断言零改动。
3. 测试验证点：既有帧锁 cluster_migrate_recv_requires_importing_slot（-ERR Slot ... is not in importing state）与 migrate_fail_inject.rs 失败注入族复跑全绿即闭环；本票为登记级，不新增行为断言，不跑长测试。

合入哈希：2743f48 收口形态：deviations.md §160 登记条（撞号零让位）＋ migrate.rs 槽门臂/cluster_migration.rs 槽门锁测注释回指锚订正半真锚，零行为改动，既有帧锁与 migrate_fail_inject 族复跑全绿
