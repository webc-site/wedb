锁定注记（2026-10-01 r9 波主控，基线本票落笔时 dev 尖；双侧现码亲验）：
- C# 判据源亲验在码：garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:275 行名取
  `RespCommandsInfo.GetRespCommandName(cmd).ToLowerInvariant()`，:276 `if (cmdName == "unknown") continue;`，
  :279 `$"cmdstat_{cmdName}"`；GetRespCommandName（RespCommandsInfo.cs:409-410）取名源为
  `commandInfo.Name`，缺席回 `UnknownCommandName`（:104 = "UNKNOWN"）。
  目录 Name 子命令形亲验：garnet/libs/resources/RespCommandsInfo.json 首项 ACL 的 SubCommands 内
  `"Command": "ACL_CAT"` 伴 `"Name": "ACL|CAT"`（竖线形全库 356 个 Name 项中批量存在）。
- rust 现状亲验：wnode/src/resp/info_provider.rs:228 `cmd.to_cs_name_lower()`（:230 `(name != "unknown")` 判）；
  wresp/src/command.rs:1202 to_cs_name_lower 直读 :430 CS_NAMES_LOWER，:715 该项字面即 "acl_cat"（下划线形），
  表头注自陈该表是「C# ToString().ToLowerInvariant() 对位」——即枚举成员名形，非目录 Name 形。
- 同名面自相冲突亲验：本仓目录侧本就持竖线形并在用——wresp/src/catalog/commands_info.rs:191 注
  「子命令为 ACL|CAT 形式」、:701 get_resp_command_name（UNKNOWN 回退同名常量 :33）与
  wresp/tests/catalog.rs:33 断言 "acl|cat"，COMMAND/COMMAND INFO 出竖线、INFO COMMANDSTATS 出下划线，
  同一命令同服务两面对外两名。
- 改点唯一消费者亲验：to_cs_name_lower / CS_NAMES_LOWER 全仓生产消费面仅 info_provider.rs:228 一处
  （另有 wresp/tests/command.rs 的自对拍锁），故本票改后二者即成零消费孤儿导出，须同票退役（含对拍锁）。
- 与甄别席回报的偏差订正（勿照抄票面转述）：其称 :230 的 unknown 判「恒真失效」为可达缺臂，主控复核为
  仅潜伏不可达——CS_NAMES_LOWER 洞位（C# MODULE 族/动态注册哨兵）在本仓无写入路径（动态模块注册层已删），
  表外判别值走 wmetric/src/command_stats.rs 有界 get_mut 静默不计数，故 calls>0 过滤后洞位空串名与
  "invalid" 名均不可达 INFO 渲染面。真缺陷仅竖线/下划线分叉一臂；unknown 臂系「改判据源后必达」的
  伴生要求，非独立现网缺陷，定级不再为其加权。
- 红线：本票仅裁 INFO COMMANDSTATS 行名取源单一化，禁动 CS_NAMES_LOWER 字形以图两全（该表另有
  to_cs_name 对拍语义），禁在 command_stats.rs 增观测口，禁碰 wnode/src/resp/objects/**（同侪在途）。
- 定级：P2（无崩溃无数据面，纯对外可观察面分叉且零在册登记；Redis 原生即 cmd|subcmd 竖线形，
  C# 与 Redis 同侧，本仓独偏）。
- 判据落生产路径确证（防后轮误读为「已裁决不移植面」）：js/check/ignore 册里 GarnetInfoMetrics.cs
  仅 `GetInfoMetrics` 一条被以「C# 生产零调用、rust 走 get_resp_info 文本单轨」理由忽略，
  该 ignore 不覆盖行名面；行名真值源 `cmdstat_{小写目录 Name}` 全 C# 仅 GarnetInfoMetrics.cs:279
  一处发射（本席 grep 实证），且同一数组 commandStatsInfo 由 :555-557 的
  `case InfoMetricsType.COMMANDSTATS → GetSectionRespInfo` 走套接字 INFO 出帧，
  即判据锚在 C# 生产渲染路径上，非仅测试面。

审核结论：待审（主控亲立案，双侧证据齐）

INFO COMMANDSTATS 行名取枚举成员小写形，与同仓目录竖线形及 C# 目录 Name 面双分叉

问题分析：
1. Garnet 契约对齐：C# INFO commandstats 行名源为命令目录 `Name` 字段小写（子命令目录 Name 即
   `ACL|CAT` 竖线形），并以「小写后 == unknown 即跳行」兜目录缺席面；对标本体
   garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetRespCommandName 消费段与
   garnet/libs/server/Resp/RespCommandsInfo.cs:GetRespCommandName。
2. 工程现状：rust 行名取 `to_cs_name_lower()`，该面是 C# 枚举成员 `ToString()` 的小写对位（下划线形
   `acl_cat`），与 C# 实际取名源（目录 Name）不同轨；本仓已有与 C# 同轨的目录取名单点
   `commands_info::get_resp_command_name`（含 UNKNOWN 回退），COMMAND 面在用而 INFO 面未接，
   构成同服务两面对外同名命令两名。
3. 逻辑危害确证：`--commandstats-monitor` 开启且任一子命令族被调用（`ACL LIST` / `CONFIG GET` /
   `CLIENT SETNAME` 等）后，`INFO COMMANDSTATS` 出 `cmdstat_acl_cat:calls=...`，C# 与 Redis 出
   `cmdstat_acl|cat:...`。外部采集器按 `cmdstat_<name>` 建标签，跨实现仪表盘/告警断链；
   同一节点 COMMAND 面与 INFO 面自相冲突，用户按 COMMAND 名单去 INFO 取数取不到。无内存与崩溃面。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/info_provider.rs:command_stats（行名取源改点）
wedb/wresp/src/catalog/commands_info.rs:get_resp_command_name 与 UNKNOWN_COMMAND_NAME（改后判据单源）
wedb/wresp/src/command.rs:to_cs_name_lower 与 CS_NAMES_LOWER（改后零消费孤儿，同票退役）
wedb/wresp/tests/command.rs:cs_name_lower_matches_to_cs_name（随退役删除）

对应 c# 文件与函数：
garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:commandStatsInfo 渲染段（行名小写与 unknown 跳行）
garnet/libs/server/Resp/RespCommandsInfo.cs:GetRespCommandName（Name 取名单点与 UnknownCommandName 回退）
garnet/libs/resources/RespCommandsInfo.json（子命令 Name 竖线形真值源）

精炼执行方案：
1. info_provider.rs command_stats 行名改走目录单源：`get_resp_command_name(cmd)` 取 &'static str 后
   ASCII 小写（C# ToLowerInvariant 对位；命令名恒 ASCII，to_ascii_lowercase 即可），并把 :230 的
   unknown 判改为对该返回值的实际判等（UNKNOWN 常量大小写不敏感比，与 C# :276 两步同序）——
   零新机制，复用既有目录查表，禁另建小写名表。
2. 同票退役 to_cs_name_lower 与 CS_NAMES_LOWER 及其对拍锁测试（改后全仓零生产消费，留之即
   pub API 孤儿）；若退役牵动 command.rs 头注或 docs 引该表，一并订正，禁以保留导出方式规避。
3. 禁改 CS_NAMES_LOWER 字形为竖线形（该表面语义是 C# 枚举成员名对位，改形即换轨并污染
   to_cs_name 对拍链），禁在 command_stats.rs 或 wmetric 新增观测口。
4. 测试：wresp/tests/catalog.rs 既有 "acl|cat" 断言维持；wnode 侧 INFO commandstats 现册补一条
   子命令调用后断言全文含 `cmdstat_acl|cat`（或所测子命令竖线名）且不含 `cmdstat_acl_cat`，
   并锁目录缺席判别值不回空名行；行名面改动后原下划线形锁测须一并翻转，禁留双形兼容断言。

---

## 终态注记
- **合入哈希**：`900e0b7`（cherry-pick 自 `1e7a9d7`）
- **收口形态**：
  1. `wedb/wnode/src/resp/info_provider.rs:command_stats` 行名统一改走 `commands_info::get_resp_command_name(cmd).to_ascii_lowercase()` 目录单源，并对 "unknown" 跳行，彻底对齐 C# `GarnetInfoMetrics.cs:275` 与 Redis 原生竖线命名。
  2. 退役 `wedb/wresp/src/command.rs` 中零消费的 `to_cs_name_lower` 与 `CS_NAMES_LOWER`，并移除对应的孤儿测试。
  3. 翻转与补充 `wnode` 测试断言：断言 INFO commandstats 全文含 `cmdstat_acl|cat`、`cmdstat_acl|setuser` 等竖线形且不含下划线形，断言目录缺席判别值跳过不输出空名行。
- **门禁验证**：`cargo check -p wresp -p wnode --all-targets` 通过，`wresp` 全套单元测试与 `wnode` commandstats 相关测试全部通过，`bun js/check.js` 零报错通过。
