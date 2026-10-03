终态:闭环(2026-09-29)。甄别 adcd275 → 沙箱 eea0fdc → 并 dev 2f148e6。新常量 RESP_ERR_BITPOS_BIT_ARGUMENT="ERR The bit argument must be 1 or 0."(主控原始字节订正:带 ERR 前缀),strict_i64 两分支分类落地,可执行域不变;帧文案从 Redis 标准先例续接,deviations 不登记。终验 test.sh 5205/5205+clippy 0。
甄别结论:通过(P3 级,含一处金样订正,2026-09-29 主控席)。亲验:parse_bit_pos_args 位参检查 :622-627 单帧坍缩、常量 :110-111/:122 在位、慢臂共用单源。金样订正:本机 redis-server 8.10.1 原始字节实测 BITPOS k 2 → `-ERR The bit argument must be 1 or 0.`——带 ERR 前缀,票面「-The bit argument…」漏前缀;新常量落地为 "ERR The bit argument must be 1 or 0."(前缀+尾点)。VALUE 帧沿仓内现状带尾点(在册横切面)。

审核结论：通过（2026-09-29 独立审核席，P3 级协议文案分叉）。rust 单帧坍缩与 C# 同坍缩双侧亲验、Redis 标准
两分支文案经 unstable/7.2 源码与 networking.c wire 形亲验成立、「帧文案从 Redis 标准」先例经
done/wnode-vector-wrongtype-dotted-five-command-frame-text-divergence.md（§164、§110）确证。
审核席必改四点（执行席遵照，已按此修正方案口径）：
1. 票面 1c 与未尽面 2 事实订正：Redis string2ll 不容 '+' 号（7.2 util.c:438-471 仅特判 '-'），
   "+1"/"+0"/"01" 均 string2ll 失败回 value is not an integer——票面「+N 合法可执行」与原源不符。
2. 受文域钉死防扩容：仓内 strict_i64（wbase/src/num.rs:102）容 '+'，方案原样落地会把 BITPOS key +1
   从拒形翻成可执行，违本票自设「受文域不扩不减」。修正口径：可执行域维持现状（仅单字节 '0'/'1'）；
   拒形分类——strict_i64 失败，或解析成功 ∈{0,1} 但非规范单字节（"+0"/"+1"）→ 既有
   VALUE_IS_NOT_INTEGER 帧（与 Redis string2ll 失败档逐帧等）；解析成功 ∉{0,1}（2、-1 等）→ 新常量帧。
3. 测试点补：BITPOS key +1 → -value is not an integer or out of range.（尾点系在册横切面沿现状）。
4. 查重节删「smessage 帧词票」虚引（全 task/ 树零命中，无此凭据）；先例仅凭 vector wrongtype 票
   与 §110；「+N 受文域」轴另案仍沿 §32 台账，本票零触碰。

原票面：
BITPOS bit 参数错误帧将 Redis 两分支文案 collapsed 为单一「bit is not an integer or out of range」该文案在 Redis 系 SETBIT/GETBIT 专属帧

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）。Redis 官方标准（src/bitops.c，unstable 与 7.2 同构）：
a) bitposCommand（1731-1736）：bit 参数先按整数解析（getLongFromObjectOrReply 默认共享帧），
非整数 → "value is not an integer or out of range"；解析成功但值域 ∉{0,1} → 专属帧
"The bit argument must be 1 or 0."（7.2 bitops.c:891 同文）。
b) setbitCommand（855-870）：bit 参数两态共用 "bit is not an integer or out of range"
（7.2 同）；GETBIT/SETBIT offset 用 "bit offset is not an integer or out of range"
（getBitOffsetFromArgument 716）——这两支本仓与 C# 逐字节已等（两侧错误帧族中唯 BITPOS 位参
一支分叉）。
c) 值域形：Redis 侧 "+1"/"+0" 经 string2ll 符号容忍解析为合法 bit=1/0 可执行（文法轴整体受
§32 严格整数台账裁决辖域，本票不动受文域，只改拒形帧文案分支——见禁触线）。
C# 原型缺形：BitmapCommands.cs:NetworkStringBitPosition 位参检查（:283）对任意非 '0'/'1' 输入
统一 AbortWithErrorMessage(RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER)（CmdStrings.cs:229），与 SETBIT
（:151 同常量）不分家——两分支坍缩为一帧，且该帧在 Redis 语义中是 SETBIT 专属文案。
2. 工程现状确证。rust 单源转写同坍缩：bitmap_commands.rs:parse_bit_pos_args（613-661）位参检查
622-627：`bit_slice.len() != 1 || 非 '0'/'1'` → RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER
（wresp/src/cmd_strings.rs:122 与 C# :229 逐字节等）。快慢两臂共用该 parse（slow.rs:1042-1044），
故两径同帧。对照：parse_bit_args（665-691，SETBIT/GETBIT）位参/偏移帧与 Redis 逐字节等（见 1b），
不在本票。
3. 逻辑危害确证。错误帧文案级协议差分：依赖 Redis 标准文案匹配错误类别的客户端（按
"The bit argument must be 1 or 0." 识别「位值非法」与按 "value is not an integer..." 识别
「非整数入参」做分流重试/告警的调用方）在本仓两态均落第三族文案，错误分类面失真；本仓先例
（smessage 帧词、vector wrongtype 文案，均按 Redis 标准回改并登 done 票）确立「帧文案从 Redis
标准」口径，本分叉与先例同类。可达形：BITPOS key 2、BITPOS key abc 等任意非法位参，默认配置直触。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/bitmap/bitmap_commands.rs:parse_bit_pos_args（613-661；位参检查 622-627）
wedb/wnode/src/resp/bitmap/bitmap_commands.rs:parse_bit_args（665-691，SETBIT 位参帧等 Redis，
回归对照不动）
wedb/wresp/src/cmd_strings.rs:RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER（122）、
RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER（110-111，既有消费口，本票新消费）
wedb/wnode/src/resp/basic_commands/slow.rs:C::Bitpos 臂（1042-1086，经单源 parse 自然同改）

对应 c# 文件与函数：
garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringBitPosition（267-357；位参帧 :283）
garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringSetBit（位参帧 :151 等 Redis 不动）
garnet/libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_BIT_IS_NOT_INTEGER（229）

Redis 标准锚（非本仓文件）：src/bitops.c:bitposCommand（1731-1736 两分支帧；7.2 bitops.c:891
同文）；src/bitops.c:setbitCommand（855-870 单帧形，本仓已等）。

精炼执行方案：
1. 位参检查改两分支（仍在 parse_bit_pos_args 单源，快慢臂自动同形）：先按仓内既有 strict i64
口（§32 文法台账口径，受文域一字不动）解析 parse_state[1]：解析失败 → 既有
RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER 口出帧；解析成功 ∉{0,1} → 新增常量
cs::RESP_ERR_BITPOS_BIT_ARGUMENT（字节对齐 Redis bitops.c:1734「The bit argument must be 1 or
0.」）落 wresp/src/cmd_strings.rs 单源，禁散落第二字面量。
2. SETBIT/GETBIT 臂、BITPOS 其余臂（start/end 非整数帧、BYTE|BIT 单位非法 syntax error 帧
——后者两侧已等 Redis，见 588-591/644-647 与 C# 对位）零改动。
3. 测试验证点：garnet_bitmap.rs 新增锁：BITPOS key 2 → -The bit argument must be 1 or 0.；
BITPOS key abc、BITPOS key 01（§32 拒形）→ -value is not an integer or out of range.（尾点系
全局常量现形，在册跨切面，本票不订正，见未尽面）；回归锁：SETBIT k 0 2 仍 -bit is not an
integer or out of range（:122 常量既有金样零改动）；慢臂 Bitpos 同帧复核。
4. 禁触线：§32 严格整数文法台账（受文域不扩不减，"+1" 是否准入沿该台账另案，本票只裁拒形出帧
文案）；§110（BITFIELD 未知子命令回显）；BITCOUNT 各臂；既有 VALUE_IS_NOT_INTEGER/BITOFFSET
常量字节形；found/notfound 簿记面。本票为 Redis 标准回改，deviations.md 不登记。

查重结论：
全仓 grep「The bit argument」零命中（rust 与 garnet 双侧），无在册裁决亦无实现；deviations.md
grep「bit argument/位参」零命中；task/ 全树 BITPOS 命中件仅 manifest 票（COMMAND 自省面）与
审阅注记，无文案裁决。smessage 帧词票与 wnode-vector-wrongtype-dotted-five-command-frame-text-
divergence.md（deviations §110 来源件）确立同类「文案从 Redis 标准」先例，本票为该先例在
BITPOS 位参面的续接，非重复立案。

未尽面：
1. RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER 全局尾点（Redis 无尾点）系横切常量面，触全仓整数族
命令帧字节，单列另案（本票沿现状消费该常量，订正尾点会越出本票边界）。
2. Redis string2ll 对位参/偏移的「+N」准入与双侧 strict 文法的受文域差（SETBIT k 0 +1 在 Redis
合法、本仓拒）属 §32 文法台账辖域，另案清点，勿在本票扩面。
3. BITCOUNT/BITPOS 单位 token 大小写与 Redis strcasecmp 等形已核（parse_bitmap_offset_type
对位 TryGetBitOffsetType），无差分，未立案。