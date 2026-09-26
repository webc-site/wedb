甄别结论：通过（甄别席 zc-fix-r16-strbitmap，2026-09-26）定级 P4
核验记录（r19 入池票独立现码复跑，逐锚亲验）：
1. C# 锚逐条成立：MainStoreOps.cs SET_Conditional 无输出重载 :258（:279 notfound/:284 found/WRONGTYPE :273 零计）与输出重载 :321（:339/:344）、ReadWithUnsafeContext :44（:76/:81 每源恰一帧）、GET :30/:39、GETRANGE :229/:238；BasicCommands.cs GETSET :434 转 NetworkSET_Conditional、NetworkSETNX :592、SET_Conditional :786/:830 全收敛双载；BitmapOps.cs :437/:441/:445/:449/:453 五命令全走 RMW_MainStore（AdvancedOps.cs:71）/Read_MainStore（:87）双口零 incr，StringBitOperation :70/:124 逐源 ReadWithUnsafeContext；Metrics.cs :14/:17 单点入账。
2. rust 现状锚逐条成立：set.rs network_setnx :408（:433 probe_alive_with_registry 零计）、network_set_conditional :523（非 GET 探针臂与 GET 形 :641 read_user_sync 均传 None，全文件零 record）；slow.rs C::Setnx :485 与 slow_set_conditional :268 非 GET 零计、GET 形 read_cold :338 一帧；bitmap_commands.rs 快臂 :143/:168/:213/:300/:419/:452 全传 None（票面行号 ±1 漂，实质成立）；bitmap_slow SETBIT read_cold :863、BITFIELD :987、GETBIT/BITCOUNT/BITPOS read_and_frame :878/:895/:911（内核 :137-145 走簿记 read_user）；slow_bit_operation :1103-1104 read_user_with_prefix（storage_session.rs :403/:412 record_outcome 恰一帧）；read_cold 头注 :186-187「SETBIT/BITFIELD 对位 GET 族」失真属实（C# 该二命令实为零计 RMW/Read 口）。
3. 可观测性成立：wmetric/src/info/garnet_info_metrics.rs :1059-1073 total_found/total_notfound/garnet_hit_rate 直入 INFO STATS。快慢臂同输入异账系 review.md 4.2 多路径同构违例。
4. 非重复非灭失：deviations.md 全册无 total_found/notfound 记账域登记；四池 grep total_found 仅本票；getex_getdel_read_accounting.rs 审计清单未含条件写族/位图族；zcode-r163c-bitopdst 系单页容量门轴、zcode-r163c-setguard 异轴，域互斥不并案；缺陷现码仍在。
5. 架构合规与可执行度：方案复用既有 fold_outcome（get.rs :349 network_getex 先例）、read_cold_quiet（slow.rs :199 SETrange/APPEND 在用）、read_user_quiet 静默内核与 do_network_mget 收尾入账先例，单套机制零新增，测试落 tests/ 对标既有审计件，符合 transpile/rust_review 纪律；纯文本格式、双侧路径齐全。
定级理由：纯治理观测面（INFO STATS 对拍红、指标随路径漂移），无数据丢失/panic/资源危害，同谱先例 zcode-r147c-incrovf 案二取 P4。

审核结论：通过（审核席 zcode-r19-review-bitacct，2026-09-26，dev 分支）

审核亲验摘记（双侧源码逐点复核，全部属实）：
1. C# 计帧口径三类坐实：SET_Conditional 无输出重载 MainStoreOps.cs:258（:279 notfound / :284 found，WRONGTYPE 臂零计）与输出重载 :321（:339/:344），NetworkSETNX:592 / NetworkSET_Conditional:786（无 GET）/:830（GET）/ GETSET:434 全收敛此双载；BITOP 逐源 ReadWithUnsafeContext（MainStoreOps.cs:44，:76/:81）每源恰一帧；位图五命令 BitmapOps.cs:437-475（SETBIT→RMW、GETBIT/BITCOUNT/BITPOS/BITFIELD 全→Read_MainStore/RMW_MainStore，AdvancedOps.cs:71/:87 双口零 incr）全零帧。Metrics.cs:14/:17 入账单点属实。
2. rust 现状坐实：快臂 set.rs network_setnx:408（probe_alive_with_registry 零入账）、network_set_conditional:523（非 GET 探针臂零计，GET 形 :641 read_user_sync 传 None 零计）；慢臂 slow.rs C::Setnx:485 与 slow_set_conditional:268 非 GET 形零计（对 C# 双臂少计），GET 形 :338 read_cold 计一帧（快慢异账成立）；位图快臂 bitmap_commands.rs :142/:167/:212/:300/:418/:451 全传 None，慢臂 SETBIT/BITFIELD 走 read_cold:863/:987、GETBIT/BITCOUNT/BITPOS 走 read_and_frame:137-145（storage.read_user 簿记入口 storage_session.rs:403 record_outcome 恰一帧，对 C# 多计且快慢异账成立）；BITOP 快臂 :299-300 传 None 零帧、慢臂 slow.rs:1103-1104 read_user_with_prefix 逐源一帧（快慢异账成立）。read_cold 头注 :186-187 自陈「SETBIT/BITFIELD 对位 GET 族口径」系审计遗漏失真表述属实。
3. 可观测性坐实：wmetric/src/info/garnet_info_metrics.rs:1059-1064 total_found/total_notfound 直显 INFO STATS、:1065-1073 garnet_hit_rate 派生命中率，双侧可见。
4. 查重坐实：deviations 全册零 total_found/total_notfound 记账域登记（§17/§18 系 TTL 保留域、§87 锁面、§92 BIT 口径、§110 回显编码，均域互斥）；tests/getex_getdel_read_accounting.rs 审计清单覆盖 GETDEL/GETEX/INCR 族/SETRANGE/APPEND，条件写族与位图族未入册；todo 无同类记账票（zcode-r163c-bitopdst 系单页门、zcode-r163c-setguard 系向量探针，域互斥）。非重复提报。
5. 维度违例定性：快慢臂同输入异账系 review.md 板块 4.2「多路径行为同构」违例（Deferred 降级即切账）；计数是键存在性口径（SETNX 写入成功计 notfound 帧），修复须按存在性折叠勿按命令成败。

执行方案（整理优化，供 task/fix.md 直接消费）：
1. 条件写族补账恰一帧（沿 zcode-r143c-getexbig 案二 network_getex 收尾判定单点先例，fold_outcome 单规则）：network_setnx 与 network_set_conditional 非 GET 形快臂传 None 静默读、本地三态布尔折叠（存活 found / 缺席 notfound），函数尾单点补账恰一条；WRONGTYPE 与一切降级出口（Ok(false)/Deferred）零入账交慢臂唯一出口收口；慢臂 string_slow C::Setnx 与 slow_set_conditional 非 GET 形同口径补一帧，降级续跑臂零补防双计。GET 形快臂补账后与慢臂 read_cold 既有一帧对齐，双臂同账。
2. 位图慢臂归零（沿 zcode-r147c-incrovf 案二 read_user_quiet 对偶先例，不起第二套判型机制）：bitmap_slow 的 SETBIT/BITFIELD 写臂 read_cold 换既有 read_cold_quiet（slow.rs:199-206，SETRANGE/APPEND 在用）；GETBIT/BITCOUNT/BITPOS 为 read_and_frame 增静默对偶 read_and_frame_quiet（内核复用 storage.read_user_quiet，GETRANGE/STRLEN 原入账臂调用点不动）；同步订正 read_cold 头注 :186-187 的「SETBIT/BITFIELD 对位 GET 族口径」失真表述并回指本票。
3. BITOP 快臂逐源补账：network_string_bit_operation 循环内逐源改传 None 静默 + 本地累计 found/notfound 计数、收尾一次入账（do_network_mget 先例），与慢臂及 C# 逐源恰一帧口径闭环；WRONGTYPE 与降级出口零入账。
4. 测试验证点：仿 tests/getex_getdel_read_accounting.rs 增字符位图族记账矩阵锁测——SETNX 命中/缺席各恰一帧（注意存在性口径：缺席写入成功计 notfound）、SET k v GET 与 GETSET 快慢双臂同账、GETBIT/BITCOUNT/BITPOS/SETBIT/BITFIELD 慢臂零入账（修复前一帧红灯）、BITOP 快慢臂逐源同账、GET/GETRANGE/STRLEN 对照口径不动，另 INFO STATS total_found/total_notfound 端到端读数对拍一例。

原票面（事实链与本案一致，全文保留如下）

字符串与位图命令族 total_found/total_notfound 记账矩阵双向未闭环（SETNX/SET 条件写/GETSET/BITOP 少计，GETBIT/BITCOUNT/BITPOS/SETBIT/BITFIELD 慢臂多计，且快慢臂同输入异账）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 的 total_found/total_notfound 会话记账经 StorageSession.incr_session_found/notfound（Metrics.cs:14/:17，直入 GarnetSessionMetrics.total_found/total_notfound，INFO STATS 与派生命中率消费），字符串与位图命令族的记账口径为三类：
甲、条件写族恰一帧：NetworkSETNX（BasicCommands.cs:592 调 SET_Conditional）与 NetworkSET_Conditional 无 GET 形（:786）、GET 形（:830）、GETSET（:434 转 NetworkSET_Conditional）全部收敛到 MainStoreOps.cs 的 SET_Conditional 双重载（:258 与 :321），两重载对非 WRONGTYPE 结局恰计一帧（:279 incr_session_notfound / :284 incr_session_found；输出形 :339/:344 同），WRONGTYPE 臂零计。
乙、BITOP 逐源键恰一帧：BitmapOps.cs:StringBitOperation 逐源读走 MainStoreOps.cs:ReadWithUnsafeContext（:44），其尾段对非 WRONGTYPE 结局恰计一帧（:76 notfound / :81 found），N 个源键即 N 帧。
丙、位图其余命令零帧：GETBIT（BitmapOps.cs:441）、BITCOUNT（:445）、BITPOS（:449）走 AdvancedOps.cs:Read_MainStore（:87，全臂零 incr）；SETBIT（:437）与 BITFIELD 写子命令（:453）走 RMW_MainStore（AdvancedOps.cs:71，零 incr）；BITFIELD/BITFIELD_RO 的 GET 子命令同走 Read_MainStore 零计。对照组：GET/GETRANGE/STRLEN/GETEX 恰一帧（MainStoreOps.cs GET :30/:39、GETRANGE :229/:238、STRLEN 经 GET out 重载、GETEX 存储臂实存），INCR 族/SETRANGE/APPEND/GETDEL/MSET/MSETNX 零帧——仓内既有审计（tests/getex_getdel_read_accounting.rs 文档段）已核后一批，条件写族与位图族未入审计清单。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
甲向少计：快路径 network_setnx（wnode/src/resp/basic_commands/set.rs:408）经 probe_alive_with_registry 零入账，network_set_conditional（同文件 :523）探针臂与 GET 形 read_user_sync 均传 None；慢路径 string_slow 的 C::Setnx（slow.rs:485）与 slow_set_conditional（slow.rs:268）非 GET 形零入账。C# 每命令一帧，rust 双臂零帧。SET..GET/GETSET 慢臂经 read_cold（slow.rs:338，簿记漏斗 read_user）恰一帧与 C# 对齐，但快臂零帧——同输入快慢臂异账。BITOP 快臂 network_string_bit_operation（bitmap_commands.rs:294-304）逐源 read_user_sync_with_prefix 传 None 零帧，慢臂 slow_bit_operation（slow.rs:1103-1104）逐源 read_user_with_prefix（簿记入口 storage_session.rs:403，record_outcome 恰一帧）——快臂零帧对 C# 逐源一帧为少计，且快慢臂异账。
乙向多计：位图慢臂 GETBIT/BITCOUNT/BITPOS 经 read_and_frame（slow.rs:137-145，簿记漏斗 storage.read_user）计一帧，SETBIT/BITFIELD/BITFIELD_RO 经 read_cold（slow.rs:863/:987）计一帧，而 C# 该五命令全零、rust 快臂全零（network_string_get_bit/network_string_bit_count/network_string_bit_position 均传 None；string_bit_field_action 传 None）——慢臂对 C# 多计一帧且快慢臂异账。read_cold 头注（slow.rs:186-187）自陈「SETBIT 写臂 / BITFIELD / GETEX / GET 形态条件读共用，对位 C# GET 族计数口径」，把 C# 零计的 SETBIT/BITFIELD 错并入 GET 族口径，系审计遗漏而非有意裁决。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
纯治理面（INFO STATS total_found/total_notfound 与派生命中率，wmetric/src/info/garnet_info_metrics.rs:1059-1069 双侧可见）：一、双侧对拍在 INFO STATS 差分上该族必然红——SETNX 密集负载 C# 计满、rust 恒零；冷数据占比高的位图负载 rust 反超多计。二、rust 自身多路径行为同构破口（review.md 板块 4.2）：同一 SET k v GET 命中热路径零帧、降级冷路径一帧，指标随路径配比漂移，监控读者无法区分负载变化与路径迁移。三、后审席若按 read_cold 头注「GET 族口径」误外推，会把零计要求错配到 GET 形态或反向把位图族改账，本票钉死矩阵防双向误改。与 zcode-r143c-getexbig 案二、zcode-r147c-incrovf 案二（已修的 INCR/SETRANGE/APPEND/GETDEL 零入账族）同谱不同面，deviations 全册及 tests/getex_getdel_read_accounting.rs 审计清单均未覆盖本族，非重复提报。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/basic_commands/set.rs:RespServerSession::network_setnx
wedb/wnode/src/resp/basic_commands/set.rs:RespServerSession::network_set_conditional
wedb/wnode/src/resp/basic_commands/slow.rs:string_slow（C::Setnx / C::Getset 分支）
wedb/wnode/src/resp/basic_commands/slow.rs:slow_set_conditional
wedb/wnode/src/resp/bitmap/bitmap_commands.rs:RespServerSession::network_string_bit_operation
wedb/wnode/src/resp/basic_commands/slow.rs:slow_bit_operation
wedb/wnode/src/resp/basic_commands/slow.rs:bitmap_slow（C::Setbit / C::Bitfield 的 read_cold，C::Getbit / C::Bitcount / C::Bitpos 的 read_and_frame）
wedb/wnode/src/resp/basic_commands/slow.rs:read_cold / read_and_frame / read_cold_quiet
wedb/wnode/src/storage/session/storage_session.rs:read_user_with_prefix

对应 c# 文件与函数：
garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:StorageSession.SET_Conditional（双重载）
garnet/libs/server/Resp/BasicCommands.cs:RespServerSession.NetworkSETNX
garnet/libs/server/Resp/BasicCommands.cs:RespServerSession.NetworkSET_Conditional
garnet/libs/server/Resp/BasicCommands.cs:RespServerSession.NetworkGETSET
garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:StorageSession.ReadWithUnsafeContext
garnet/libs/server/Storage/Session/MainStore/BitmapOps.cs:StorageSession.StringBitOperation
garnet/libs/server/Storage/Session/MainStore/AdvancedOps.cs:StorageSession.Read_MainStore
garnet/libs/server/Storage/Session/MainStore/AdvancedOps.cs:StorageSession.RMW_MainStore
garnet/libs/server/Storage/Session/MainStore/BitmapOps.cs:StorageSession.StringGetBit / StringBitCount / StringBitPosition / StringSetBit / StringBitField
garnet/libs/server/Storage/Session/Metrics.cs:StorageSession.incr_session_found / incr_session_notfound

精炼执行方案：
1. 条件写族补账恰一帧（快臂按 GETEX 收尾判定单点先例，network_getex 的 fold_outcome 形态）：network_setnx 与 network_set_conditional 非 GET 形按窗内探针三态折叠（存活 found / 缺席 notfound / WRONGTYPE 与降级零入账交慢臂），GET 形按 read hit/missing 折叠；慢臂 string_slow C::Setnx 与 slow_set_conditional 非 GET 形同口径补一帧，降级续跑臂零补防双计。
2. 位图慢臂归零：SETBIT/BITFIELD 的 read_cold 换既有 read_cold_quiet；GETBIT/BITCOUNT/BITPOS 为 read_and_frame 增静默对偶（内核复用 read_user_quiet，GETRANGE/STRLEN 原入账臂不动，不起第二套判型机制）；同步订正 read_cold 头注的「SETBIT/BITFIELD 对位 GET 族口径」失真表述并回指本票。
3. BITOP 快臂逐源补账：循环内本地累计 found/notfound、收尾一次入账（do_network_mget 先例），与慢臂及 C# 逐源恰一帧口径闭环；WRONGTYPE 与降级出口零入账。
4. 测试验证点：仿 tests/getex_getdel_read_accounting.rs 增字符位图族记账矩阵锁测——SETNX 命中/缺席各恰一帧、SET k v GET 与 GETSET 快慢双臂同账、GETBIT/BITCOUNT/BITPOS/SETBIT/BITFIELD 慢臂零入账（修复前一帧红灯）、BITOP 快慢臂逐源同账、GET/GETRANGE/STRLEN 对照口径不动，另 INFO STATS total_found/total_notfound 端到端读数对拍一例。

视角结论:有增量
合入哈希：574d53a 收口形态：条件写族（SETNX/SET 条件写/GETSET）快臂 fold_outcome 函数尾单点与慢臂静默探针+record_read_outcome 终态各恰一帧、位图五命令慢臂 read_cold_quiet/read_and_frame_quiet 归零、BITOP 快臂逐源累加收口与慢臂 dest 静默门闭环快慢同账，六组矩阵锁测全绿含 INFO STATS 端到端对拍
