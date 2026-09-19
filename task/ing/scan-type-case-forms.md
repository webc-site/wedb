SCAN TYPE 取值改精确双形态比对（混合大小写走未知类型臂）

来源：next/scan-type-exact-case-forms.md（已认领删除）

判定：采纳。票面属实，与既有 LPOS 双形态先例同口径。

证据（对照 C#）
- garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:57-85
  DbScan 用 typeObject.SequenceEqual(CmdStrings.ZSET) || SequenceEqual(CmdStrings.zset)
  这类精确双形态比对，五对（ZSET/zset、LIST/list、SET/set、HASH/hash、STRING/stringt）。
  混合形态既不匹配任一已知类型，落到 :80-85 的 Unexpected typeObject 分支：
  storeCursor = lastScanCursor = 0; return true; 即回空键列表 + 游标 0，不触达扫描。
- 两形态字面值：garnet/libs/server/Resp/CmdStrings.cs:368-375
  （ZSET="ZSET"、zset="zset"、LIST/list、HASH/hash、STRING="STRING"、stringt="string"）。
  注意 string 常量名是 stringt 但值为 "string"，别按常量名写成 "stringt"。
- 选项名本身（MATCH/COUNT/TYPE）在 C# 用 EqualsUpperCaseSpanIgnoringCase
  （ArrayCommands.cs:279、:291、:305）大小写不敏感，rust 同口径，不改。分歧仅在 TYPE 的取值。

rust 现状缺口
- wedb/wnode/src/resp/array_commands.rs:116-127 parse_scan_filter 的五个已知类型
  用 type_arg.eq_ignore_ascii_case(b"zset"/...) 比较，任何大小写形态都被接受并映射成
  过滤类型照常扫描，与 C# 分叉（SCAN 0 TYPE ZsEt：C# 回空、rust 按 SortedSet 命中）。
- :66 函数文档注释自陈「TYPE 匹配使用 eq_ignore_ascii_case 支持混合大小写」，与 C# 相悖，须改写。
- 未知类型臂本身语义正确（:127-129 置 type_unknown；慢路径
  wedb/wnode/src/resp/garnet_api/slow.rs 的 C::Scan 臂对 type_unknown 短路回空 + 游标 0，
  对位 ArrayKeyIterationFunctions.cs:82-84），只是缺把混合大小写归入未知形态。

方案（单一定义处，不加双轨兼容）
- 把 :116-127 五个 eq_ignore_ascii_case 改为精确双形态比较：
  type_arg == b"zset" || type_arg == b"ZSET"（list/set/hash/string 同形，string 值为 b"string"/b"STRING"）。
  语法对齐既有先例 wedb/wcol/src/list/list_object_impl.rs:509-514
  （read_list_position_input 的 sb_param == b"RANK" || sb_param == b"rank"）。
- 混合形态自然落到 :127 既有 type_unknown = true，快慢两路径空结果短路零改动即生效。
- 改写 :62-69 文档注释：删去「支持混合大小写」承诺，改为「TYPE 取值为 C# 双形态精确比对
  （全大写/全小写），其余按未知类型回空」，不留自相矛盾描述。

测试同步（array_commands.rs 单测域）
- 现 :701 test_parse_scan_filter_type_case_insensitive 断言混合大小写命中，与新口径冲突，重命名并按新口径重写：
  全大写、全小写两形态各自命中对应类型；混合大小写（zSet/LiSt/HaSh/StRiNg）断言 type_unknown=true、type_filter=None；
  stream/STREAM 等未知类型仍 type_unknown=true；选项名 type/TyPe/TYPE 大小写不敏感仍成立。

验收
- SCAN 0 TYPE ZsEt / HaSh 回空列表 + 游标 0；TYPE zset / TYPE ZSET 仍按类型过滤命中。
- 快路径与慢路径（冷键降级）两形态结论一致。
- cargo check -p wnode 零告警（禁写 allow）。

范围边界：仅改 parse_scan_filter 比较式与文档注释、对应单测；不动 null 帧、不动慢路径体、
不改 OBJECT TYPE 回显侧（envelope_object_type_name 小写名，本就对标 Redis 输出）。
