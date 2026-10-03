终态：已合入 dev（2026-09-27）。dc1875a 一补两建三档案:MainStoreOps 补 SETEX、建 UnifiedStoreOps/API 两 yml;复跑 miss 38→35 零新增红,零代码改动

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：SortedSetAdd miss 归 wcol-zset 票勿动；一补两建三档案，复跑 check.js 验收零 miss

审核结论：通过（亲验+亲跑 check.js 复现三 miss yml 现场落盘：MainStoreOps.cs:419/423 SETEX 组 StringInput 转交 SET、UnifiedStoreOps.cs:23 GET 联合读包装、GarnetApiUnifiedCommands.cs:53 EXPIRETIME 一行转发；rust 承接 set.rs:361/get.rs:38/keys.rs:291 完整实装非假桩；同类 ignore 先例在册（MainStoreOps 四姊妹+AdvancedOps），ignore 树确无 UnifiedStoreOps.yml 与 API/ 目录；五池无对位票。登记级零行为票，ignore 档单点落笔可落）

js 门禁对位假红：MainStoreOps.SETEX / UnifiedStoreOps.GET / GarnetApiUnifiedCommands.EXPIRETIME 三个会话与 API 包装方法无 rust 对位物未登记，核心命令族常驻假「实现缺失」

问题分析：
1. C# 原型与门禁契约确证：check.js 缺失判定按「C# 方法名录 减去 已锚 减去 ignore 登记」得 miss 树（js/check/rustScan.js
   CS_REF_REGEX 锚捕获、js/check/miss 落盘、js/check/ignore 抑制）。本族三方法均属 C# 存储会话层 / GarnetApi 统一门面
   的 StringInput / UnifiedInput 转发包装：
   MainStoreOps.cs:SETEX（garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:419 毫秒重载、:423 TimeSpan 重载，
   仅组 StringInput 转交 RMW switch）；
   UnifiedStoreOps.cs:GET（garnet/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:23，UnifiedInput/UnifiedOutput
   联合上下文读包装）；
   GarnetApiUnifiedCommands.cs:EXPIRETIME（garnet/libs/server/API/GarnetApiUnifiedCommands.cs:53，公开 API 统一门面转发
   storageSession）。
   rust 采单存储引擎、命令层唯一实现、无 UnifiedInput/UnifiedOutput 联合 API 门面架构（见 task/review.md 板块 1「零全局
   可变状态」「全链路唯一机制」及 doc/zh 声明），故此三包装方法在 rust 无对位物属设计内缺席，其净效果由 RESP 命令层唯一
   实现承接：network_setex（set.rs:361，锚 BasicCommands.cs:NetworkSETEX）、network_get（get.rs:38，锚 BasicCommands.cs:
   NetworkGET）、network_expiretime（keys.rs:291，锚 KeyAdminCommands.cs:NetworkEXPIRETIME），三命令实装完整非假桩，
   且已在 doc/zh/deviations.md §405-416 与 js/check/ignore 姊妹条中在册。

2. 工程现状确证（rg 亲验）：同族同类设计内缺席既有先例均已走 ignore 登记收口——
   js/check/ignore/garnet/libs/server/Storage/Session/MainStore/MainStoreOps.yml 已登记 GETDEL/APPEND/SETRANGE/Increment
   四姊妹包装（理由「命令层唯一实现已覆盖，会话层再设手写包装即双口径来源」）与 SCAN 死桩；
   js/check/ignore/garnet/libs/server/Storage/Session/UnifiedStore/AdvancedOps.yml 已登记 Read_UnifiedStore/RMW_UnifiedStore
   （理由「联合视图会话层包装无接线必要」）。
   但 SETEX 被 MainStoreOps.yml 四姊妹条目漏收、UnifiedStoreOps.GET 与 GarnetApiUnifiedCommands.EXPIRETIME 各自类别
   连 ignore 档都未建（find 亲验：ignore 树无 UnifiedStoreOps.yml、无 GarnetApiUnifiedCommands.yml）。
   故本次 bun js/check.js 落盘 js/check/miss/libs/server/Storage/Session/MainStore/MainStoreOps.yml（fn: SETEX）、
   .../UnifiedStore/UnifiedStoreOps.yml（fn: GET）、.../API/GarnetApiUnifiedCommands.yml（fn: EXPIRETIME）三条常驻红。

3. 逻辑危害确证：三条均为纯登记缺口、零行为分叉、零数据面影响；危害在门禁甄别面——即 check.js:100-105 明文警惕的「假
   缺失污染甄别」：SETEX/GET/EXPIRETIME 系最高频核心命令，其包装面常驻假红会淹没同文件真缺失（一旦 MainStoreOps 出现
   新真漏项即被既有假红吞没），且 GET/SETEX/EXPIRETIME 三红口对账席按图索骥会误判「命令未实装」而回查已完整实装的命令
   层，浪费对账工时。属登记级收口，严禁以改命令层代码「对齐 C# 包装层」名义引入第二套会话手写包装（那才是真回归）。

涉及代码：
rust 命令层唯一实现（承接净效果，本票不动）：
wedb/wnode/src/resp/basic_commands/set.rs:RespServerSession::network_setex
wedb/wnode/src/resp/basic_commands/get.rs:RespServerSession::network_get
wedb/wnode/src/resp/key_admin_commands/keys.rs:RespServerSession::network_expiretime

对应 c# 无对位包装方法（本票登记对象）：
garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:SETEX
garnet/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:GET
garnet/libs/server/API/GarnetApiUnifiedCommands.cs:EXPIRETIME

门禁档：
js/check/ignore/garnet/libs/server/Storage/Session/MainStore/MainStoreOps.yml（漏收 SETEX）
js/check/ignore/garnet/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.yml（待建）
js/check/ignore/garnet/libs/server/API/GarnetApiUnifiedCommands.yml（待建）
js/check/miss/libs/server/Storage/Session/MainStore/MainStoreOps.yml
js/check/miss/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.yml
js/check/miss/libs/server/API/GarnetApiUnifiedCommands.yml

精炼执行方案：
1. MainStoreOps.yml 既有 GETDEL/APPEND/SETRANGE/Increment 四姊妹条目内补入 SETEX（同段同理由，仅追加方法名），
   并在头部注释列举里补 SETEX(:419/:423) 一句，与命令层 network_setex 锚 BasicCommands.cs:NetworkSETEX 互为对账指向。
2. 新建 UnifiedStoreOps.yml：按 AdvancedOps.yml 体例登记 GET，理由引姊妹裁决「命令层 network_get 唯一实现已覆盖，
   UnifiedInput/UnifiedOutput 联合包装层在 rust 单引擎无接线必要」。
3. 新建 GarnetApiUnifiedCommands.yml：登记 EXPIRETIME，理由「GarnetApi 公开统一门面 storageSession 转发包装，rust 无
   独立 API 门面层，命令层 network_expiretime 唯一实现已承接」。
4. 严禁改命令层三函数体、严禁新建会话包装；纯 ignore 落档。
5. 验收：bun ./js/check.js 复跑，js/check/miss/ 上述三 yml 由 missSync 自动撤除，全册零新增红，与 wcol-zset 票锚点位
   不重叠（本票为包装面无对位登记，非锚缺失）。

五池查重与相邻点位：
本席扫得同 run 报告内其余点均判设计内/残影非待立案，特此备查：
GETDEL 重复（user_read.rs:142 record_outcome 与 storage_session.rs:377 read_user_quiet）系两函数各在注释里引
MainStoreOps.cs:GETDEL 自陈零入账纪律，属叙述性锚重影非代码冗余，且 MainStoreOps.GETDEL 已 ignore 在册；
LPOS ReadListPositionInput 重复（read_list_position_params 与 read_list_position_input）为单源词元解析 + 三门校验拆两
函数（:519 调 :540，注释明书「严禁第二解析器」），非可收口冗余；
SortedSetAdd miss 归 task/todo/wcol-zset-zadd-fullpath-anchor-missing-checkjs-residue 票（勿重复）；
Bitmap/JSON/Tsavorite ClientSession/DirectoryServices 等 miss 属架构基座无对位或异域，越本票命令族范围。
命令覆盖抽查 LPOS/ZMPOP/LMPOP/SINTERCARD/SMISMEMBER/EXPIRETIME/GETDEL 七族：快臂（raw.rs:326/332/364/406/407、
keys.rs:165/291）+ 慢臂（set_commands/slow.rs、list_commands/slow.rs、sorted_set_commands、key_admin_commands/slow.rs:444）
+ 分层臂齐挂，无缺臂无假桩无应答形分叉，在册分叉见 deviations §89（LPOS 词元）/§113+§158 尾注（SINTERCARD）/§2094
注记（ZMPOP 空键形），均设计内不另报。
