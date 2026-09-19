# session parse state extensions 旁路 —— 拒绝（票面主张与现码不符，修法已全量落地）

来源：next/session-parse-state-extensions-bypass.md（qw.design 第 11 轮条 1 拆出，浅核 2026-09-19）
裁决日期：2026-09-20　对照树：主仓 dev HEAD（/Users/z/git/db/wedb/wedb）

## 结论

票面 5 条主张逐条核证全部不成立：13 个 (parse_state, buf, idx) 下标形态口在
wedb/wnode/src/session_parse_state_extensions.rs 中早已不存在（该文件现 163 行、
仅 3 个 `fn try_get_*`），bitfield encoding/offset 复抄、wmetric 私有壳、
wbitmap 指回死口的「唯一映射点」注释同样均已消除，C# 各 TryGet* 锚点已按最终归属
一处登记。票面推荐的修法（删下标形态口 + 用例改调字节核 + bitfield 只留 wbitmap 一份
+ wmetric 直调 from_name + 锚点按归属登记）已完整实现并进了 HEAD，属「已落地」类
主张，无可执行剩余，删票归档，不建分支、不动代码。

## 原文（票面全文）

> 优先级：中
> 分拣注记（qw.design 第 11 轮条 1 拆出；浅核 2026-09-19：符号在 wnode/src/session_parse_state_extensions.rs 在场，13 下标形态口经限定路径 grep 抽验确为零生产消费者——wmetric/tiered_collection_ops/hash_commands 命中均为 wresp::options 或 wmetric 私有同名活函数；台账无同题票）
>
> SessionParseStateExtensions 的 C# 对位层整体旁路：13 个 (parse_state, buf, idx) 形态口
> 零生产消费者，其中 2 口是 wbitmap 既有单点的逐行复抄（同一 C# 函数双挂锚点、两文件互指唯一映射点）
> 问题：wnode/src/session_parse_state_extensions.rs 模块头自称对标 libs/server/SessionParseStateExtensions.cs，
> 但其 13 个「按参数下标取 token 再解析」的入口（try_get_info_metrics_type :85、try_get_latency_metrics_type :96、
> try_get_client_name :108、try_get_bit_field_overflow :156、try_get_bitfield_encoding :168、
> try_get_bitfield_offset :196、try_get_manager_type :212、try_get_operation_direction :246、
> try_get_sorted_set_add_option :266、try_get_expire_option :276、try_get_sorted_set_aggregate_type :286、
> try_get_expiration_option :296、try_get_timeout :308）全仓生产视图零引用，读者只有
> wnode/tests/session_parse_state_extensions.rs；生产实际解析走同文件另一套字节核
> （try_get_client_name_bytes :127、manager_type_from_token :221、operation_direction_from_token :255、
> try_get_timeout_bytes :330）与 wresp::options 自由函数，即 C# 的「会话态下标解析」单一入口在 rust
> 被旁路为「调用方自行索引 + 字节核」，锚点声明与实现面脱节。
> 其中两口不是薄转发而是复抄实现：try_get_bitfield_encoding :168 与 wbitmap/src/bitfield/parse.rs:51
> parse_bitfield_encoding 同体（len<=1 拒、i/u 前缀、strict_i64、有符号<=64/无符号<64，仅返回型
> (u8,bool)/(i64,bool) 之差），try_get_bitfield_offset :196 与 wbitmap parse.rs:73 parse_bitfield_offset
> 同体；两处各自挂着同一 C# 锚点（wnode 挂 SessionParseStateExtensions.cs:TryGetBitfieldEncoding、
> wbitmap 挂 BitmapCommands.cs:TryGetBitfieldEncoding），而 wbitmap/src/bitfield/parse.rs:34 注释还写着
> 「SessionParseState 版薄适配见 session_parse_state_extensions 模块（唯一 C# 映射点）」（:34-:35）——被指为唯一映射点的
> 两口本身零消费者，真身在产的另一套又各挂一锚。连带第三处同名：wmetric/src/info/info_command.rs:106
> 私有 fn try_get_info_metrics_type 再转发 InfoMetricsType::from_name，与 wnode 同名死口、wresp 内核三处同名。
> 修法：删 13 个下标形态口（含两口复抄）并把用例改调字节核，或将生产解析整体改走该层使锚点为真（择一）；
> bitfield encoding/offset 只留 wbitmap 一份并删 wnode 复抄，wmetric :106 私有壳改直调 from_name，
> js/check/ignore 按最终归属登记 SessionParseStateExtensions.cs 各方法。
> c#：garnet/libs/server/SessionParseStateExtensions.cs（TryGet* 全族）；生产消费对位
> garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:450、:541、garnet/libs/server/Storage/Functions/MainStore/PrivateMethods.cs:862、
> garnet/libs/server/Resp/Objects/ListCommands.cs:214、garnet/libs/server/Resp/Objects/SortedSetCommands.cs:1577、
> garnet/libs/server/Metrics/Info/InfoCommand.cs:37、garnet/libs/server/Metrics/Latency/RespLatencyCommands.cs:50、
> garnet/libs/server/Resp/PurgeBPCommand.cs:49、garnet/libs/server/Resp/BasicCommands.cs:1492、
> garnet/libs/server/Resp/ClientCommands.cs:525、garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:45

## 逐条核证

主张 1「wnode 存在 13 个下标形态口且零生产消费者」——不成立。
/Users/z/git/db/wedb/wedb/wnode/src/session_parse_state_extensions.rs 全文 163 行，
公开项只有 ClientType(:22)、ManagerType(:39)、serialize_snapshot(:56)、
try_get_client_name_bytes(:68)、try_get_client_type(:82)、manager_type_from_token(:110)、
operation_direction_from_token(:137)、try_get_timeout_bytes(:151)。
try_get_info_metrics_type / try_get_latency_metrics_type / try_get_bit_field_overflow /
try_get_bitfield_encoding / try_get_bitfield_offset / try_get_manager_type /
try_get_operation_direction / try_get_sorted_set_add_option / try_get_expire_option /
try_get_sorted_set_aggregate_type / try_get_expiration_option / try_get_timeout 全仓 grep
零命中（仅 wresp/src/options.rs 的字节核同名活函数在产，见主张 4）。
保留的唯一下标形态口 try_get_client_type 有真实生产读者：
wnode/src/resp/client_commands.rs:66、:220。其余 6 项逐条有生产读者：
serialize_snapshot → wnode/src/resp/metrics_commands.rs:116；
try_get_client_name_bytes → wnode/src/resp/client_commands.rs:325、
wnode/src/resp/basic_commands/mod.rs:512；manager_type_from_token →
wnode/src/resp/admin_commands.rs:429（配 ManagerType::gc_completed_text :442，
消费者还有 wnode/src/cluster_provider.rs:14、wedb/src/server/cluster_provider.rs:36）；
operation_direction_from_token → wnode/src/resp/objects/list_commands/write.rs:14、
…/list_commands/slow.rs:26、…/list_commands/blocking.rs:23；
try_get_timeout_bytes → list_commands/blocking.rs:118、:212、:250、:302、
sorted_set_commands/blocking.rs:235、:306。零死面。

主张 2「唯一读者是 wnode/tests/session_parse_state_extensions.rs」——不成立。
/Users/z/git/db/wedb/wedb/wnode/tests/session_parse_state_extensions.rs:10-13 只 import
上述 6 个在产符号（ClientType/ManagerType/manager_type_from_token/
operation_direction_from_token/try_get_client_name_bytes/try_get_client_type/
try_get_timeout_bytes），头注释 :3-7 明写「选项枚举解析与 BITFIELD 编码、INFO/LATENCY
段名解析的用例随各自的字节核归属：wresp::options、wbitmap::bitfield、
wresp::metrics::InfoMetricsType、wmetric::LatencyMetricsType」。测试与实现在产状态一致。

主张 3「wnode 复抄 bitfield encoding/offset、与 wbitmap 双挂同一 C# 锚点、
wbitmap parse.rs:34 注释仍指回 SessionParseState 薄适配为唯一映射点」——不成立。
/Users/z/git/db/wedb/wedb/wbitmap/src/bitfield/parse.rs 现存唯一实现
parse_bitfield_overflow_slice(:36)、parse_bitfield_encoding(:52)、parse_bitfield_offset(:74)，
锚点分别是 :34 `SessionParseStateExtensions.cs:TryGetBitFieldOverflow`、:50 `:TryGetBitfieldEncoding`、
:73 `:TryGetBitfieldOffset`（即票面要求的「按最终归属登记」，wnode 侧不再有第二挂点）；
:34-:35 现文为「（C# 的 BitmapCommands / PrivateMethods 调用点均走该解析态扩展口，全仓单点）」，
被指为死口的薄适配指引已删。全仓 `SessionParseStateExtensions.cs:TryGetBitfield*` 形态 grep
各仅 1 处命中，无双挂。

主张 4「wmetric/src/info/info_command.rs:106 私有 fn try_get_info_metrics_type 转发壳、
三处同名」——不成立。该文件无 try_get_* 私有壳（全仓零命中），INFO 段名解析直调
`InfoMetricsType::from_name`：wmetric/src/info/info_command.rs:58；内核单点在
/Users/z/git/db/wedb/wedb/wresp/src/metrics/info_metrics_type.rs:130，其 :15-16 注释
即「libs/server/SessionParseStateExtensions.cs:TryGetInfoMetricsType → rust
InfoMetricsType::from_name，单点即本文件」；其余消费者 wnode/src/resp/resp_server_session.rs:1756、
wnode/src/resp/garnet_api/slow.rs:320。LATENCY 同构：
wmetric/src/latency/latency_metrics_type.rs:65 from_name（锚点 :63），
消费者 wmetric/src/latency/resp_latency_commands.rs:106。

主张 5「锚点声明与实现面脱节、需按最终归属登记 ignore」——已落地。
C# SessionParseStateExtensions.cs 共 22 个方法，现码归属一处且互不重复：
wnode/src/session_parse_state_extensions.rs 承接 TryGetClientName/TryGetClientType/
TryGetManagerType/TryGetOperationDirection/TryGetTimeout（:65/:79/:107/:134/:147）；
wresp/src/options.rs 承接 TryGetSortedSetAddOption/TryGetExpireOption/
TryGetExpirationOption/TryGetSortedSetAggregateType（:45/:104/:191/:273，实现在产：
wcol/src/zset/sorted_set_object_impl.rs:121、wnode/src/resp/key_admin_commands/keys.rs:192、
wnode/src/resp/objects/hash_commands.rs:614、wnode/src/resp/basic_commands/set.rs:669、
wnode/src/resp/objects/sorted_set_commands/write.rs:612、tiered_collection_ops.rs:1357）；
wbitmap/src/bitfield/parse.rs 承接 bitfield 三口；wresp/wmetric 承接 INFO/LATENCY 两口；
键规格族与 TryGetExpirationOptionWithToken、TryGetGeoDistanceUnit 走 ignore 登记：
/Users/z/git/db/wedb/js/check/ignore/libs/server/SessionParseStateExtensions.yml
（TryGetGeoDistanceUnit，理由点名 wcol parse_utils 活实现）、
/Users/z/git/db/wedb/js/check/ignore/libs_server_SessionParseStateExtensions.yml
（TryGetExpirationOptionWithToken + ExtractCommandKeys/AndFlags + TryAppendKeys*，
理由点名 wresp options token 形态与 wnode key_spec extract_keys_from_slice 单点）。
wnode/src/key_spec.rs:1、wresp/src/key_spec.rs:10 亦明写「此处不设第二套提取实现」。
即票面「择一」的两个选项里推荐的（也唯一符合 transpile「一套机制、复杂度对标 C#」的）
那条已生效，rust 侧口径为「调用方按下标取 token + 字节核解析」，模块头 :1-9 已把这层
关系写清，不存在锚点与实现脱节。

## 越界不动项（转录给主代理，非本票射程）

1. GEO 族 C# 锚点措辞未登记/双挂，会让 js/check.js 语义出偏差（按 CS_REF_REGEX 只认
   `File.cs:Fn` 形态）：TryGetGeoSearchOptions 在 rust 有实现在产
   （wnode/src/resp/objects/sorted_set_geo_commands.rs:151 起的
   try_get_geo_search_options），但其上文档写的是 `SessionParseStateExtensions.TryGetGeoSearchOptions`
   （点号，:6、:151），不构成登记；TryGetGeoLonLat 则被两处同时挂 `.cs:` 锚点
   （wcol/src/parse_utils.rs:41 的 GeoLonLatError 枚举、
   wnode/src/resp/objects/sorted_set_geo_commands.rs:93 的 geo_lon_lat_checked），
   真实现 try_get_geo_lon_lat 在 wcol/src/parse_utils.rs:53 反而只写了「C# TryGetGeoLonLat」。
   另 TryGetGeoDistanceUnit 的活实现 wcol/src/parse_utils.rs:17 同样不带 `.cs:` 锚，
   仅靠 yml 忽略。建议单开一张「geo 族锚点归属」微票，勿并入本票。
2. wedb/src/server/cluster_session/slot_mgmt.rs:35 挂着
   `SessionParseStateExtensions.cs:TryGetSlotState` 锚点，而 C#
   garnet/libs/server/SessionParseStateExtensions.cs 全文无 TryGetSlotState（22 方法清单见主张 5），
   属冒领锚点，归属另一域（cluster/DEBUG），本票不改。
3. 同一 C# 文件的 ignore 语料现存两份载体（js/check/ignore/libs/server/… 与
   js/check/ignore/libs_server_…），条目不重叠故当前无冲突，但按「语料真实布局是
   js/check/ignore/libs/…」的既有结论，扁平件宜合并；交 ignore 对账票处理。
4. next/qw13.invA.md:98 声称「工作树 /tmp/fork/fix-session-parse-state-bypass 正在做本题，勿重派」
   —— 实为僵尸声称：`git worktree list` 与 /tmp/fork 只有 dev-2026-09-19（ulua 旧快照）、
   docs-readme-crate-map、fix-checkpoint-purge-signature 三树，无该树、无同名分支，
   且该行「仍 17 个 fn try_get_*」的读数是过期快照。该 invA 行也应由主代理一并核销。
