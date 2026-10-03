甄别结论：通过（2026-09-29 主控甄别，定级 P2，缩围为合并既有施工严禁重新施工——dev 现码五消费点零接驳判据未灭失；/tmp/fork/fix-cmd-manifest-display 已有完整施工链 5 commits（98edc66 立案→49a8996 领票→012d6ba 真源 CustomCommandDisplay+宏同次展开→97597c2 五消费点接驳+resp_tests 金样→90c3230 终态注记）分支干净未合入。执行=fix.md 第 3 步合并：冲突处理并回 dev、票面以 worktree 版为准（多 28 行施工注记）、门禁后归档）

COMMAND 命令表面五处消费点未接驳扩展命令静态清单，默认构建服务面自洽性与 C# 模块装载对位形分叉

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）。C# 自定义命令只要注册时显式携带 RespCommandsInfo，即入
customCommandsInfo 字典（CustomCommandManager.cs:98/126/180/210 四注册口同构；:45-46 注释明裁
「customCommandsInfo only includes commands registered WITH explicit info」；:390
GetCustomCommandInfoCount 即该字典计数）。命令表面五处消费该字典：
a) COMMAND 全表：WriteCOMMANDResponse 先取 GetAllCustomCommandsInfos（:1144，字典定义
CustomCommandManager.cs:377），数组长度计入自定义条数（:1145-1152）并先于目录逐条渲染（:1156-1159）；
b) COMMAND COUNT：NetworkCOMMAND_COUNT 回 目录数加自定义数之和（BasicCommands.cs:1216）；
c) COMMAND INFO：零参等价 COMMAND 全表（:1299-1302）；带名目录失配落
customCommandManagerSession.TryGetCustomCommandInfo 回落臂（:1317-1318）；
d) COMMAND GETKEYS 与 e) COMMAND GETKEYSANDFLAGS：共用 TryGetSimpleCommandInfo
（:1352、:1389 调用；定义 :2041-2062），枚举与目录双失后查自定义 info
（:2050），命中即 PopulateSimpleCommandInfo（:2052），其后无键规格判定回
RESP_COMMAND_HAS_NO_KEY_ARGS（:1356-1357）。
本仓对位场景 modules/RoaringBitmap：RoaringBitmapModule.cs:33-40 四条 RegisterCommand（R.SETBIT、
R.GETBIT、R.BITCOUNT、R.BITPOS）全部显式携带 new RespCommandsInfo Arity=4/3/2/-3，均入
customCommandsInfo。该装载形态下 C# 可观测面：COMMAND COUNT 比纯目录多 4；COMMAND 全表数组长度多 4
（元素帧因 info 未填 Name 走 RespCommandsInfo.cs:418-421 空名回 null 形）；COMMAND GETKEYS
R.SETBIT k 0 1 回 "The command has no key arguments"（HAS_NO_KEY_ARGS，非 INVALID）。
对照组 modules/GarnetJSON/JsonModule.cs:32-33：JSON.SET/JSON.GET 注册未带 commandInfo，不入
customCommandsInfo，五处面均不显现。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）。rust 扩展命令为编译期静态清单且默认构建在场：
wnode/Cargo.toml:19 default = ["roaring", "json"]；清单单源 custom_objects.rs:24-29
CUSTOM_OBJECT_ENTRIES（:9-10 自述「对标 C# CustomCommandManager 集中分配面的静态化承接」）；名与元数
在册 wext_roaring/src/roaring_bitmap_commands.rs:65-76 COMMAND_INFOS（:127-161 四条目
arity 4/3/2/-3，与 C# 模块逐值一致）、wext_json/src/json_commands/dispatch.rs:35-37（自述全 21 条
等于 2 对位加 19 扩展）。服务面已全量接线：解析命中置 current_custom_command
（parser/resp_command.rs:587）、槽校验自定义臂消费清单键作用域
（resp_server_session_slot_verify.rs:85-88、:131-151）、ACL 按名门同读一张清单
（custom_objects.rs:79-81）。但 COMMAND 命令表面五处消费点全部目录单源、零接驳：
write_command_response 仅 try_get_resp_commands_info_ordered（basic_commands/mod.rs:202-221）；
network_command_count 仅 try_get_resp_commands_info_count（:224-234）；
write_command_info_p 目录失配直写 null（:307-320）；prepare_command_keys_context 目录失配即回
RESP_INVALID_COMMAND_SPECIFIED，无自定义 info 回落层（:325-360，回落缺位对位 C# :2049-2053）；
COMMAND DOCS 两侧自定义 docs 字典均空（C# 模块注册未带 commandDocs）故 docs 面双侧等形，不在本票。
全仓 grep 显示 COMMAND_INFOS 消费点仅在执行面与清单元测试，表面无。
3. 逻辑危害确证。默认构建服务端可执行 R.* 四命令（解析、槽校验、ACL、执行臂全通），而自省面将其隐匿：
COMMAND COUNT 少数 4，与 COMMAND 全表在 C# 侧同源共字典（:1216 与 :1145 同取
customCommandsInfo）的自洽关系在 rust 侧被单侧削减；COMMAND GETKEYS R.SETBIT 回
"Invalid command specified" 而 C# 对位形回 "The command has no key arguments"，依赖
GETKEYS 做键路由预演的客户端把可执行命令误判为非法命令名，错误文案族分叉为协议差分可见。
COMMAND INFO 带名臂当前两侧字节同为 null，但成因不同：C# 是命中自定义 info 后因 Name 缺席回 null，
rust 是清单零接驳的目录失配 null，属假桩未接线的巧合等形，一旦目录侧任何渲染改动即失配。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/basic_commands/mod.rs:write_command_response（202-221）
wedb/wnode/src/resp/basic_commands/mod.rs:network_command_count（224-234）
wedb/wnode/src/resp/basic_commands/mod.rs:network_command_info / write_command_info_p（288-320）
wedb/wnode/src/resp/basic_commands/mod.rs:prepare_command_keys_context（325-376，INVALID 臂 357-360，
NO_KEY_ARGS 臂 361-364）
wedb/wnode/src/resp/custom_objects.rs:CUSTOM_OBJECT_ENTRIES（24-29）、match_custom_object_command（63-69）
wedb/wcustom/src/object_desc.rs:CustomObjectEntry / CustomCommandMeta（清单元数据承载体）
wedb/wext_roaring/src/roaring_bitmap_commands.rs:COMMAND_INFOS（65-76、127-161）
wedb/wext_json/src/json_commands/dispatch.rs:COMMAND_INFOS（35-47、102-120）
wedb/wnode/Cargo.toml:default features（19）

对应 c# 文件与函数：
garnet/libs/server/Resp/BasicCommands.cs:WriteCOMMANDResponse（1137-1175）
garnet/libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_COUNT（1203-1222，并项 1216）
garnet/libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_INFO（1296-1337，回落 1317-1318）
garnet/libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_GETKEYS（1342-1363，无键判定 1356-1357）
garnet/libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_GETKEYSANDFLAGS（1389 同口）
garnet/libs/server/Resp/BasicCommands.cs:TryGetSimpleCommandInfo（2041-2062，自定义臂 2049-2053）
garnet/libs/server/Custom/CustomCommandManager.cs:Register 四口（84-102/113-130/158-185/196-214）、
customCommandsInfo 语义注释（42-46）、GetAllCustomCommandsInfos（377）、GetCustomCommandInfoCount（390）
garnet/libs/server/Resp/RespCommandsInfo.cs:ToRespFormat 空名回 null 门（416-424）
garnet/modules/RoaringBitmap/RoaringBitmapModule.cs:OnLoad 带 info 四注册（33-40）
garnet/modules/GarnetJSON/JsonModule.cs:OnLoad 无 info 两注册（32-33）

精炼执行方案：
1. 清单承载展示属性：wcustom 的 CustomCommandMeta 增 const 字段
command_info_registered: bool 与 command_info_has_name: bool（或单字段枚举形），值按 C# 对位注册形逐条
填：roaring 四条 registered=true、has_name=false（RespCommandsInfo 仅 Arity）；json 全部
registered=false。清单静态展开零堆分配零锁。
2. custom_objects.rs 增一条 const 展示视图访问口（按名查 registered 项 + 全量 registered 计数），
与 match_custom_object_command 同一张清单同一次判定，不建新字典。
3. 五处消费点接驳（仅 registered 项参与，JSON.* 维持 C# 等形不显现）：
a) network_command_count：目录数加 registered 项计数（对位 C# :1216）；
b) write_command_response：数组长度加 registered 计数，并在目录条目之前按清单静态序追加 registered 项
元素帧；元素渲染形按 has_name 判定：false 时逐字节回 null 帧（对位 C# ToRespFormat:418-421 的
Name 缺席回 null 现形，保持与模块装载态字节全等），true 时回 10 元素正帧；
c) network_command_info 零参臂复用 b) 同口自然并项；带名臂目录失配且名 registered 时回 null 帧
（字节与 C# 现形等，成因接线）；
d) prepare_command_keys_context：目录失配后追加清单名解析回落（对位 C# :2049-2053），命中 registered
项即取键规格空集走既有 RESP_COMMAND_HAS_NO_KEY_ARGS 臂（:361-364），未命中仍回
RESP_INVALID_COMMAND_SPECIFIED（:341 注释的 §106 宗 a 数字名拒形不动）。GETKEYSANDFLAGS 共用同口。
4. 顺序说明：C# 自定义条目在目录之前（BasicCommands.cs:1156-1166）；rust 按清单静态序落位，
ConcurrentDictionary 枚举序本无定序，测试按集合比对，禁按 C# 随机序仿制第二机制。
5. 测试验证点（纯静态起草，交执行席落地）：
a) wnode 集成锁测：COMMAND COUNT 等于目录计数加 usize::from(feature roaring) 乘 4，
RESP2/RESP3 两态同值；roaring/json 双 feature 开与关（--no-default-features）两档各测；
b) COMMAND 全表：数组长度与 COUNT 严格相等；roaring 开启时末四位为 null 帧（has_name=false 形）；
c) COMMAND INFO R.SETBIT 回 null 帧、COMMAND INFO JSON.SET 回 null 帧且成因均为「名在场但展示面
registered=false / has_name=false」，测试注释钉死对位锚；
d) COMMAND GETKEYS R.SETBIT k 0 1 与 COMMAND GETKEYSANDFLAGS 同名，错误帧文案
"The command has no key arguments"（cs::RESP_COMMAND_HAS_NO_KEY_ARGS 既有字节口），
不再是 "Invalid command specified"；未知名仍回后者；
e) 回归：COMMAND DOCS 头形金样（resp_tests.rs:2289-2308 的 *514/%257 系 docs 面）不变；
§106 宗 a 数字名拒形测试与 command_getkeys_parent_sub_lookup 锁测不变；
既有 wresp 目录侧金样零触碰。
6. 登记面：本方案为对齐修复非偏差，deviations.md 不登记；若阶段二改选「正帧带名」展示形（Redis 模块
命令可检索标准），则 COMMAND 末位元素与 INFO 带名臂两处字节分叉按落票不落册规则在新裁决注释内记由。

查重结论：
doc/zh/deviations.md 全文 grep「COMMAND」「自定义清单接驳」「扩展命令表」零命中，无在册裁决；
task/ 全树 grep GetCustomCommandInfoCount / TryGetCustomCommandInfo / customCommandsInfo 仅命中
task/done/wacl-acl-setuser-enum-only-name-custom-fallback.md（裁 ACL SETUSER 按名门回落臂，非本表面对象，
且其收口记录反证 current_custom_command 与清单面为不同接缝）；
task/done 中 zcode-r22-wcustom（slot_verify 多键 spec）、command_docs_null_arms / zcode-r123c-cmddocs1
（DOCS 空臂形）同域不同缝；已扫禁重派清单（OBJECT 族、CONFIG、PSUBSCRIBE、RESP3 push、脚本函数族、
SCAN 游标、gossip、waof、wepoch、panic 面）均不含 COMMAND 表面对象；
§106 宗 a、§149、GETKEYS parent_sub 组合查找在册偏差只裁目录名解析层，与本票「清单零接驳」不重叠。
本票非「按 C# 回改」：分叉点为 rust 缺接线（假桩未接线型缺陷），修复方向同时满足 C# 模块装载对位字节与
Redis 模块命令可检索惯例。

未尽面：
1. JSON.* 全 21 条（19 条无 C# 对位的 RedisJSON 扩展，wext_json dispatch.rs:35-37）是否按 Redis 标准
上线自省可见：双侧现形等（C# 无 info 注册不显现），属扩展面裁决，本票不扩面。
2. 若展示面接名，manifest 的 acl_categories（"bitmap"/"json" 字符串面，wext_roaring
roaring_bitmap_commands.rs:40-44）与 ACL 类别展开（C# AclCommandInfo 仅由目录构建，
RespCommandsInfo.cs:201-211，自定义不入类别字典——双侧等形）是否随之接线，另案。
3. COMMAND LIST（Redis 8 类别过滤形）两侧均无该子命令，双侧等形缺席，不起票。
4. MODULE LOADER 类机制属架构红线明令不转写的上游冗余，本票仅静态清单接驳，严禁借票复活动态装载。
---

## 独立审核结论（审核席 r438，主控复核后入卷）

裁决：**通过但改判方向**，定级维持 **P2**（缺省即达不降档；纯自省自洽性缺陷，无执行结果/数据/AOF/复制/权限面损害，不升 P1）。

### 一、病灶与论断：全部亲验成立
1. 缺省构建在场：`wnode/Cargo.toml:19 default = ["roaring", "json"]` 亲读；解析臂 `parser/resp_command.rs:580-591`（:587 置 `current_custom_command`）、执行臂 `resp_server_session/custom.rs:62-124`（arity 校验 :71、`try_custom_object_command` :88-111）、槽校验 `resp_server_session_slot_verify.rs:82-89`/`:131-151`、ACL 名门 `acl_commands.rs:794` 四臂全通。
2. 自省面零接驳：五消费点（`basic_commands/mod.rs` 的 202-221 / 224-234 / 288-305 + 307-322 / 325-376）只 import catalog 一族；`CUSTOM_OBJECT_ENTRIES`+`match_custom_object_command` 全仓消费者仅 parser、`garnet_api/slow.rs:823`、ACL、`object_store_utils.rs:997`（注释）与该文件自单元测试。
3. 「COMMAND INFO 带名臂属假桩未接线的巧合等形」论断成立（rust :318-320 目录失配直写 null；C# `BasicCommands.cs:1317-1324` 命中自定义 info 后经 `RespCommandsInfo.cs:418-421` 空名门写 null）。

### 二、改判项（执行席照此消费，票面原方案作废部分以本节为准）
1. **量级订正**：缺省下可执行而自省隐匿的是 **25 条**（roaring 4 + json 21，`wext_json/src/json_commands/dispatch.rs:104-291` 逐条点数与 `wext_roaring/.../roaring_bitmap_commands.rs:128-165` 四条），票面「少计 4」失实。
2. **不采 null 元素帧**：C# 的 null 帧系「模块注册只填 Arity 不填 Name」撞渲染门的**缺域副产物**，且同一名在 C# 兼表「命令不存在」（`BasicCommands.cs:1324`），抄形即自省语义自我抹除并把上游缺域建模进 rust。改采 **Redis 标准带名正帧**（`*10` + 名 + arity + 余空集/0），仅接名与 arity 两个已有真源字段。`COMMAND GETKEYS` 仍回 `HAS_NO_KEY_ARGS`，该处双侧同字节（`wresp/src/cmd_strings.rs:219`），真正支点为 `RespCommandInfoSimplifiedStructs.cs:203-228`（KeySpecs 仅由 `KeySpecifications` 非 null 时填）。
3. **撤票面 registered / has_name 双旗标**：per-command 展示旗标是把 C# 元数据缺域建模成 rust 数据，且 `has_name=true` 正帧臂零生产者成死臂（撞 dead_code 门禁与禁 `#[allow]` 规矩）。
4. **JSON.\* 不再维持隐匿**：`json` 系 default feature，21 条与 R.\* 同样可解析可执行，只接 4 条即裂缝留 21 条；票面「维持与 C# 等形不显现」判为修复不完整，且其中 19 条连 C# 对位都无、无字节可抄。
5. **真源承载改形**：票面「复用 `CUSTOM_OBJECT_ENTRIES` 同一次判定」口径不可落地——该表可枚举单位是「每扩展对象类型一条」（现 2 条），成员只持 `match_command` 函数指针，无命令级名与 arity。替换为宏同次展开 `COMMAND_DISPLAY` 切片（见下）。

### 三、替换后执行方案（唯一真源、禁第二套机制）
1. `wcustom/src/object_desc.rs`：新增 `pub struct CustomCommandDisplay { pub name: &'static str, pub arity: i32 }`；`CustomObjectEntry` 增 `pub command_display: &'static [CustomCommandDisplay]`。
2. 两扩展 crate 的命令宏（`define_roaring_commands!` `roaring_bitmap_commands.rs:46-126`、`define_json_commands!` `dispatch.rs:16-100`）内与既有 `COMMAND_INFOS`、`ALL` **同一次展开**追加 `pub const COMMAND_DISPLAY: &[CustomCommandDisplay]`，值只取宏参数 `$name`/`$arity` 字面量；各 crate `OBJECT_ENTRY` 挂该切片。既有 `COMMAND_INFOS` 面与其自校验单测（roaring :562-576、json tests :613-627）不动。
3. `wnode/src/resp/custom_objects.rs`（紧随 :59-81）：`const fn custom_command_display_count() -> usize`（长度求和）+ `pub(crate) fn custom_command_displays() -> impl Iterator<Item = &'static CustomCommandDisplay>`（`flat_map`）。名解析回落**一律复用** `match_custom_object_command`（:63-69），不建新字典。特性关闭时清单为空表、计数自然为 0，**禁写 cfg 特判或 feature 乘 4 的算术**。
4. 五消费点：a) `network_command_count` = 目录计数 + `custom_command_display_count()`；b) `write_command_response` 数组长度用同一表达式（守住 COUNT 与全表长度严格相等，即 C# :1216 与 :1145 同源自洽关系），目录渲染循环之后按清单静态序追加元素帧，目录前缀字节不变；c) 新增 wnode 私有渲染 fn（唯一新增展示件），逐字段对位 `RespCommandsInfo.cs:424-477` 十字段形：`write_array_length(10)`、name、arity、set(0) flags、0、0、0（firstkey/lastkey/step）、set(0) categories、set(0) tips、set(0) key specs、array(0) subcommands；set 头一律走 `RespWriter` 协议单点（`resp_memory_writer.rs:181-184` RESP2 退 `*0`、:232-235 RESP3 用 `~0`），命令层禁自拼 `~`/`*`，禁虚构 flags/@类别/键规格；d) `network_command_info` 零参臂随 b) 自然并项，`write_command_info_p` 带名臂改三段（目录命中走既有渲染；目录失配且 `match_custom_object_command` 命中 → 展示帧；双失 → 既有 `writer.write_null()` 不动）；e) `prepare_command_keys_context` 在 :357-360 INVALID abort 之前插清单回落臂，命中即 `abort_with_error_message(output, cs::RESP_COMMAND_HAS_NO_KEY_ARGS)` 回 `None`（对位 C# :2050-2053 + :1356-1357 两级），不构造 synthetic `SimpleRespKeySpec`/`SimpleRespCommandInfo`；`:336 from_cs_name`、:340-355 parent_sub、:331-335 数字名拒形注释一字不动；`GETKEYSANDFLAGS`（:397-408）共用同口无须单改。
5. **记由（落票不落册，deviations.md 不登记）**：COMMAND 全表追加的 25 个元素帧与 COMMAND INFO 带名臂两处对 C# 模块装载态的字节分叉，须在渲染点与测试注释自陈成因（C# 因 Name 未填走 `RespCommandsInfo.cs:418-421` 空名门回 null，系注册元数据缺域副产物；rust 清单原生有名有 arity，按铁律采 Redis 官方标准正帧），并保留 GETKEYS 的 `HAS_NO_KEY_ARGS` 双侧同字节锚自证未扩面。
6. 承判据核验：§106 正文在册（`deviations.md:247-251`，裁 Enum.TryParse 数字回退拒形）；清单名恒带 `R.`/`JSON.` 前缀，数字名不可能命中，拒形臂不动。**票面「§149」系误引**（实为 EVAL numkeys=0 双闸放行，:375-379），与本缝无涉。

### 四、测试点订正与新增
1. **既存必改**（票面「既有 wresp 目录侧金样零触碰」「末位元素为 null 帧」两句作废）：`wedb/wnode/tests/resp_tests.rs:2285` 的 `out1.starts_with(b"*258\r\n*10\r\n$3\r\nACL\r\n")` 正是 COMMAND 全表头，长度须改 **`*283`**（258+25），ACL 首项与 :2283 两次调用序一致断言保持；:2298 `*514`、:2307 `%257` 系 DOCS 面不变；wresp 目录侧金样与 `commands_info.rs:784-805` 一族零触碰。
2. 自洽不变量锁测（RESP2/RESP3 两态）：`network_command_count` 返回值与 `network_command_info(&[])` 数组头长度严格相等，且等于目录计数 + `custom_command_display_count()`。
3. 元素帧逐字节金样（RESP2）：`R.SETBIT` = `*10\r\n$8\r\nR.SETBIT\r\n:4\r\n*0\r\n:0\r\n:0\r\n:0\r\n*0\r\n*0\r\n*0\r\n*0\r\n`；`R.GETBIT :3`、`R.BITCOUNT :2`、`R.BITPOS :-3`（与 C# `RoaringBitmapModule.cs:34/36/38/40` 对拍）；`JSON.SET :-4`、`JSON.MGET :-3`；RESP3 同帧仅 set 头为 `~0\r\n`。
4. INFO 带名臂：`R.SETBIT`/`JSON.SET` 回正帧（RESP2 头 `*2\r\n*10\r\n`）；`NOSUCHCMD` 仍回既有 null（`$-1` 与 RESP3 `_` 两态各测），证明 :319 臂未被吞改。
5. GETKEYS 族：`R.SETBIT k 0 1` 与 GETKEYSANDFLAGS 同名回 `-The command has no key arguments\r\n`；`JSON.SET` 同测；未知名仍 `-Invalid command specified\r\n`；`GETKEYS 8 k` 数字名拒形回归；既存五测（:1974、:1986、:2135、:2163、:2186）零改动通过。
6. 清单同源自校验单测（`custom_objects.rs` tests，接 :123-135 既有形）：每条展示项名经 `match_custom_object_command` 命中且 `meta.name`/`meta.arity` 逐值相等；展示项总数等于两 crate `COMMAND_INFOS` 长度和；扩展特性关闭时计数为 0（双臂形援 `wedb/wnode/tests/acl_setuser_enum_only_fail_close.rs:129-142` 范式）。
7. 服务面回归不得改动：`resp_roaring_bitmap_tests.rs`、`custom_object_recheck.rs`、`resp3_null_parity.rs`、`object_encoding_extended_label_parity.rs` 全绿。

### 五、禁触线（票面原线之外加严）
1. 禁在 wnode 手写命令名清单、禁第二张 name→info map 或 Lazy 结构、禁借 wresp 目录侧 `RespCommandsInfo` 构造路径渲染扩展命令（那会把目录渲染器变成第二实现）。
2. 禁改 `prepare_command_keys_context` 数字名拒形臂与 parent_sub 组合查找臂及其锁测；禁改 `slot_verify` 的 `custom_command_key_spec`、ACL 名门、wresp 目录侧渲染与金样、既有 COMMAND DOCS 头形。
3. 禁把 JSON.\* 的 19 条无 C# 对位扩展的规格真值（flags/@类别/键规格）虚构入自省面；本轮只接 name 与 arity。
4. 禁在 deviations.md 登记（对齐 Redis 标准的修复非偏差）；禁 MODULE LOAD/LOADCS 类动态装载复活（架构红线）。
5. **与「扩展命令清单 FNV 索引编译期化」案（记忆 r330 C1）严禁同树并行**——同改 `wcustom/src/object_desc.rs` 与 `wnode/src/resp/custom_objects.rs:59-81`；主控裁本案先落，索引化票后落时须把展示口与解析口收进同一具名索引并重锚本案在 `custom_objects.rs` 的自校验单测。

### 六、票面失实引用（勿沿用）
所引三卷 `task/done/command_docs_null_arms`、`zcode-r22-wcustom`、`zcode-r123c-cmddocs1` 现树与 `git log` 零命中，系失实；确实存在且同域异缝者为 `task/done/wacl-acl-setuser-enum-only-name-custom-fallback.md` 及 `-registry.md`（只裁 ACL SETUSER 按名门与 `custom_allowed` 持久化，未顺带收口本表面）。

### 七、未尽面（另案，勿在本票扩面）
展示帧 flags 与 @类别真源接线；键规格真值接线（与 `custom_command_key_spec`、§149 谱系并案）；COMMAND DOCS 自定义 docs 面与 COMMAND LIST；ACL 类别展开对扩展命令的呈现；清单索引编译期化。

### 八、另订正微偏锚（落注释时按此形）
`write_command_info_p` 实为 307-322（null 臂 318-320）；`network_command_info` 288-305 与前者系两函数；roaring 宏调用块 128-165；`COMMAND_INFOS` const 67-76（:65-66 注释）；json `COMMAND_INFOS` const :38-47、条目区 :102-291（票面 102-120 只覆盖前两条）；C# 自定义循环 :1156-1159、目录循环 :1161-1166；`TryGetSimpleCommandInfo` if 在 :2050、`PopulateSimpleCommandInfo` 在 :2052。票面「COMMAND_INFOS 消费点在执行面」混写：执行面消费的是 `CUSTOM_OBJECT_ENTRIES`+`match_custom_object_command`，`COMMAND_INFOS` 只被各 crate 再导出与 `#[cfg(test)]` 自校验消费。

## 施工席终态注记（执行席，分支 fix-cmd-manifest-display，基线 dev 49a8996）

### 实际改动文件:函数
1. `wedb/wcustom/src/object_desc.rs`：新增 `CustomCommandDisplay { name, arity }`；`CustomObjectEntry` 增 `command_display: &'static [CustomCommandDisplay]` 字段。`wcustom/src/lib.rs` 导出该型。
2. `wedb/wext_roaring/src/roaring_bitmap_commands.rs`：`define_roaring_commands!` 内与 `COMMAND_INFOS`/`ALL` 同一次展开追加 `pub const COMMAND_DISPLAY`（值只取 `$name`/`$arity` 字面量）；`RoaringCommand::OBJECT_ENTRY` 挂该切片。
3. `wedb/wext_json/src/json_commands/dispatch.rs`：`define_json_commands!` 同形追加 `COMMAND_DISPLAY`；`JsonCommand::OBJECT_ENTRY` 挂切片。
4. `wedb/wnode/src/resp/custom_objects.rs`：新增 `custom_command_display_count()`（const fn 长度求和，零 cfg）与 `custom_command_displays()`（flat_map 迭代）；新增 `display_tests` 同源自校验模块（每展示项经 `match_custom_object_command` 命中且 meta.name/meta.arity 逐值相等、总数=两 crate `COMMAND_INFOS` 长度和、每清单项子表无重名、not(any(features)) 零计数臂）。
5. `wedb/wnode/src/resp/basic_commands/mod.rs`：新增私有渲染件 `write_custom_command_display_frame(name, arity, writer)`（十字段序对位 RespCommandsInfo.cs:424-477，set 头全走 RespWriter 协议单点，成因注释在渲染点）；`write_command_response` 数组长度 = `infos.len() + custom_command_display_count()` 并在目录循环后按清单静态序追加 25 帧（目录前缀字节不变）；`network_command_count` 并同一计数表达式；`write_command_info_p` 带名臂三段化（目录命中/清单回落正帧/双失 null 原臂不动）；`prepare_command_keys_context` 于 INVALID abort 前插清单回落臂回 `HAS_NO_KEY_ARGS`（`from_cs_name`、parent_sub、数字名拒形注释一字未动）；GETKEYSANDFLAGS 共用同口零单改。
6. `wedb/wnode/tests/resp_tests.rs`：既存金样 `*258`→`*283`（唯一既存金样改动，:2298 `*514`/:2307 `%257` DOCS 头形未触）；新增四测：`command_count_equals_full_table_length_both_protocols`（COUNT==全表头==283 两态锁）、`command_full_table_appends_custom_frames_byte_golden`（六条 RESP2 逐字节金样 + 全 25 帧拼接尾金样 RESP2/RESP3 两态 + 目录前缀不变）、`command_info_named_arm_custom_frame_and_unknown_null`（R.SETBIT/JSON.SET 正帧、小写命中规范名、NOSUCHCMD `$-1`/RESP3 `_` 两态负例）、`command_getkeys_custom_names_no_key_args`（R.SETBIT/JSON.SET 双 GETKEYS 口 no-key-args、未知名 INVALID、`GETKEYS 8 k` 数字名拒形回归）。

### 与方案的偏差（含成因）
1. 渲染 fn 入参取 `(name: &str, arity: i32)` 二标量而非 `&CustomCommandDisplay`：INFO 带名臂按审核结论第四节 4d 口径以 `match_custom_object_command` 为唯一名解析单点，meta.name/meta.arity 与展示项逐值相等由 `display_tests::every_display_resolves_on_parse_face` 钉死；若渲染件强要 struct 引用则需新建 name→display 二次查找（违「不建新字典」）。字节面同形，无第二机制。
2. 票面第四节 5a「usize::from(feature roaring) 乘 4」算术与 5b「末四位 null 帧」、双旗标、JSON.* 隐匿、wresp 目录侧金样零触碰（除 :2285）等原案作废项一律未实现，以审核结论为准。
3. `--no-default-features` 档本机实测说明：`cargo test -p wnode --lib --no-default-features` 经 dev-dependency `wnode_test → wnode`（default features）在同一构建图内 feature 统一，roaring/json 实际仍开启，`zero_without_extension_features` 零计数臂本机未能以零档真跑；`cargo check -p wnode --tests --no-default-features` 通过（该臂与全部生产面在零 cfg 下编译干净）。零档运行态由主控门禁档位（feature.check.sh/test.sh 矩阵）验。
4. 行号微偏：票面所引 basic_commands 各函数行号与现码全合（202-221/224-234/288-305/307-322/325-376），审核结论第八节订正锚已全部按现码形落注释。

### 跑过的测试目标与结果（全部离线逐目标）
- `cargo test -p wnode --test resp_tests`：110 passed / 0 failed（含新增四测、既有五 GETKEYS 锁测 :1974/:1986/:2135/:2163/:2186、改样后 command_and_docs_deterministic_order）。
- `cargo test -p wnode --lib`：178 passed / 0 failed（custom_objects tests+display_tests 共 8 项）。
- `cargo test -p wcustom`（2）/`-p wext_roaring`（13+集成）/`-p wext_json`（含 json_commands_test 32，COMMAND_INFOS 自校验 :562-576/:613-627 形不动全绿）。
- 服务面回归：`--test resp_roaring_bitmap_tests`（18）/`custom_object_recheck`（5）/`resp3_null_parity`（2）/`object_encoding_extended_label_parity`（3）/`acl_setuser_enum_only_fail_close`（1）全绿。
- `cargo check -p wnode --tests --no-default-features` 通过。

### 未尽事项（门禁待主控验）
- `./test.sh`（--all-features 全量，283 形于 all 档同值——wnode 仅 roaring/json 两扩展特性）、`./sh/clippy.sh`、`bun js/check.js`、feature.check.sh 各档。
- 零特性档 `zero_without_extension_features` 运行态（本机因 feature 统一未能执行，见偏差 3）。
- 展示帧 flags/@类别/键规格真值接线、COMMAND DOCS 自定义 docs 面、清单 FNV 索引编译期化（严禁同树并行，索引化票后落时须重锚 display_tests 与 :63-69 解析单点收口）。

## 归档终态注记（合并席，2026-09-29）

- 合入哈希：e4e8284（Merge branch 'fix-cmd-manifest-display' into dev，--no-ff；施工链 012d6ba/97597c2/90c3230 三 commit + worktree 侧预并 dev 的 13ee4f9）
- 合并收口形态：dev 自基线 49a8996 的新合入（coldctx/roaring-bitpos-rank/vector-filter/flush-detach/vrandmember 及 r8/r9 清理轮）与本分支 8 个改动文件零交集，merge dev 与回并 dev 两级均零冲突（ort 自动合并）。dev 侧 wresp catalog 改动经核为死代码清理与接口收敛（try_fast_get_resp_command_info 删除、try_get_commandsfor_acl_category 改名 commands_for_category、RespCommandsTables 收 pub(crate)），命令条目零增删，目录计数 258 不变，*283 金样基数有效。worktree 内 cargo check --all-targets 通过（零警告）。单套机制保持：名解析单点 match_custom_object_command、COMMAND_DISPLAY 宏同次展开唯一真源、无合并引入的死代码或重复实现。
- 主目录归档操作：票面以 worktree 90c3230 终态版为准合入（+28 行施工注记），主控甄别注记头保留于票顶，git mv task/ing → task/done。
- 遗留给主控门禁：./test.sh（--all-features）、./sh/clippy.sh、bun js/check.js、feature.check.sh 各档、零特性档 zero_without_extension_features 运行态（施工注记偏差 3 所述 feature 统一限制）。
