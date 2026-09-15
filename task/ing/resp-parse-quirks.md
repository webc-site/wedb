# resp-parse-quirks：RESP 解析口径四条（宽度 / SCAN 两态 / RI.CREATE 吞错 / RILEN 死码）

来源：next/glm.md 条目（主代理预清理转述）。分支 w5-parse-quirks。

甄别结论：四条全部成立，逐 token 对照 C# 权威源码核实，另发现同函数内三处
错误文案偏差一并修正（属逐 token 对照授权范围）。

## 一、[P1] 集合命令 numkeys/count 解析宽度 i64 vs C# int32 — 成立

C# 全族 parseState.TryGetInt（SessionParseState.cs:391 → ParseUtils.cs:47
TryReadInt → RespReadUtils.TryReadInt32Safe，allowLeadingZeros: false，
int32 溢出即解析失败）：

ZMPOP SortedSetCommands.cs:423/:468、ZRANDMEMBER :832、ZINTERCARD
:1184/:1211、ZDIFF :921（SortedSetDifference）、ZDIFFSTORE :1016、
ZINTER/ZUNION :1061/:1356（SortedSetIntersect/Union）、BZMPOP :1644/:1687、
LMPOP ListCommands.cs:198/:228、BLMPOP :866/:903、GEO COUNT
SessionParseStateExtensions.cs:478。

rust 六处（sorted_set_commands.rs）用 try_parse_i64：中间值（如
"3000000000"，i64 内 i32 外）被接受而 C# 报错，行为分叉。

修法（strict_i32 收敛，先例 hash_commands.rs HRANDFIELD /
list_commands.rs LPOP 系列）：

1. ZMPOP :492/:524（溢出与 <1/<0 文案不变，C# 均报 NOT_INTEGER）
2. ZRANDMEMBER :780（换 strict_i32 后保留 min(i32::MAX >> 2) 钳制，
   C# :864 Math.Min 同款）
3. ZINTERCARD :909/:925（溢出 NOT_INTEGER 不变）
4. BZMPOP :1135/:1162
5. parse_diff_args :1488（ZDIFF/ZDIFFSTORE 共用；溢出 NOT_INTEGER 不变）
6. parse_combine_args :1527（ZUNION/ZINTER/ZUNIONSTORE/ZINTERSTORE
   共用；溢出 NOT_INTEGER 不变）
7. 范围外点名文件最小改动：list_commands.rs LMPOP :352/:378、
   BLMPOP :1071/:1097、sorted_set_geo_commands.rs GEO COUNT :286
   （溢出走原 else 分支文案，LMPOP/BLMPOP 文案已由 resp-text-dbids
   先例对齐，GEO COUNT 文案 NOT_INTEGER 不变）

同函数逐 token 对照发现的文案偏差（一并修）：

- BZMPOP：C# :1644 numkeys 非整数（含溢出）或 <= 0、:1687 count 非整数
  或 < 1 均报 GenericParamShouldBeGreaterThanZero（CmdStrings.cs:335
  "ERR Parameter `{0}` should be greater than 0"）。rust 现报
  NOT_INTEGER（numkeys 非整数 / count 非整数）与 SYNTAX_ERROR（numkeys
  < 1，rust :1139 把 <1 与长度不足合并）→ 改 Parameter 版两分支。
- ZINTERCARD：C# :1191 nKeys < 1 报 GenericErrAtLeastOneKey
  （"ERR at least 1 input key is needed for 'ZINTERCARD' command"），
  rust :914 误用 RESP_ERR_GENERIC_NUMKEYS（"ERR numkeys should be
  greater than 0"）→ 改 format! AtLeastOneKey 版（parse_combine_args
  :1532 同款先例）。C# :1218 limit < 0 报 GenericErrCantBeNegative
  "LIMIT"（"ERR LIMIT can't be negative"），rust :928 写死
  "-ERR limit is negative" → 改。

## 二、[P2] SCAN 参数两处口径 — 成立

对标 ArrayCommands.cs:275-313（NetworkSCAN）：

1. 未知选项：C# if / else-if 链无 else，未知参数静默跳过（仅消费参数名
   本身）。rust array_commands.rs:110 else 报 SYNTAX_ERROR → 删除报错
   分支，静默继续。
2. COUNT 负/零：C# :298 TryGetLong 仅校验整数性，负/零合法传入扫描层。
   Tsavorite AllocatorScan.cs:268 `acceptedCount >= count`：count <= 0
   时首条匹配记录后即停（单页至多 1 条 + 非零游标）。rust :83-85 负/零
   保持默认 10 → 改 `filter.count = n.max(0) as usize`（负/零 → 0，
   扫描层 scan_cursor `count.max(1)` 恰产出"至多 1 条"同语义）。
3. 扫描层 TYPE 口径复核（甄别提示"待复核"部分）：C# DbScan
   （ArrayKeyIterationFunctions.cs:53-85）typeObject 匹配为
   SequenceEqual 大小写敏感双字面量（zset/ZSET、list/LIST、set/SET、
   hash/HASH、string/STRING）；未匹配五类的非空 typeObject →
   storeCursor = 0 + 空列表返回（:82-84），不是 matchType = null 无过滤。
   rust 两处不符：eq_ignore_ascii_case 过宽；未知类型落到
   type_filter = None + type_given = true（无上限全扫）。
   修法：TYPE 匹配改精确双字面量；ScanFilter 新增 type_unknown 标记，
   慢路径消费端（garnet_api.rs exec_slow C::Scan）见标记直接
   write_output_for_scan(0, &[])（C# 空列表路径 :319-331 同字节：
   `*2\r\n$1\r\n0\r\n*0\r\n`）。

## 三、[P2] RI.CREATE 数值选项非数字吞错 — 成立

C# RespServerSessionRangeIndex.cs:NetworkRICREATE（CACHESIZE/MINRECORD/
MAXRECORD/MAXKEYLEN/PAGESIZE 五处）用 parseState.GetLong
（SessionParseState.cs:402 → ParseUtils.cs:64 ReadLong →
RespReadUtils.TryReadInt64，allowLeadingZeros 默认 true，前导零合法）。
失败两态（异常冒泡至 RespServerSession.cs:522 catch）：

- 非数字 / 尾随垃圾 / u64 溢出 → ThrowNotANumber → 应答
  "ERR Protocol Error: Unable to parse number: {arg}"，随后断连
- u64 内但超 i64 → ThrowIntegerOverflow（数字串不含符号）→
  "ERR Protocol Error: Unable to parse integer. The given number is
  larger than allowed: {digits}"，随后断连

rust resp_server_session_range_index.rs:105-133 五处
try_parse_i64().unwrap_or(0)：非数字吞成 0，validate 再报
"ERR numeric options must be greater than zero"（且 strict_i64 拒前导
零，C# GetLong 允许）→ 全链错误。

修法：五处换 wbase::num::try_parse_i64（NumUtils.TryParse 前导零允许
口径）+ 失败两态判定（去符号后纯数字且超 i64 域 → overflow 文案，否则
not-a-number 文案），写协议错误行后命令结束。
断连差异记录：C# DisposeNetworkSender 断开会话；rust RI 族走
exec_slow（GarnetApiFace trait，&self 无会话可变面，parse_violation
哨兵不可达），改签名波及全部分派面，超出本问题边界。文案字节级对齐，
连接保持，记录为已知差异。
RI.SCAN count :373 同文件同性质（C# TryGetInt int32，:361 报
"ERR invalid count"）顺带收敛 strict_i32。

## 四、[P2] network_rilen 无分派 arm — 成立

C# RespCommand.cs RI 族（RICREATE/RIDEL/RIPROMOTE/RIRESTORE/RISET/
RICONFIG/RIEXISTS/RIGET/RIMETRICS/RIRANGE/RISCAN）无 RILEN；
RespServerSessionRangeIndex.cs 仅 9 个 NetworkRI* 方法，无 NetworkRILEN。
rust network_rilen（resp_server_session_range_index.rs:509-532 自由
函数 + :580-588 impl 方法两层）无任何分派 arm（garnet_api.rs:678-699
九个 arm 与 C# 一致），映射注释 NetworkRILEN 系虚标。

修法：两层全删。wkv::range_index_len（SKILL 认可的内部 API，O(1)
MetaValue.size 直读）保留，wkv/tests/store/range_index.rs 仍消费，不
成孤儿。C# 无此符号，check.js 无需 ignore 登记。

## 测试计划

wnode/tests/resp_error_text_tests.rs 追加字节级断言（先例同款真服务器
形态）：

1. 集合族宽度与文案：BZMPOP numkeys/count 溢出与 0 值 Parameter 版；
   ZMPOP/ZUNION numkeys "3000000000" → NOT_INTEGER；ZRANDMEMBER
   count "3000000000" → NOT_INTEGER；ZINTERCARD numkeys 0 →
   AtLeastOneKey 实名版、LIMIT -1 → "ERR LIMIT can't be negative"、
   LIMIT "3000000000" → NOT_INTEGER
2. SCAN 两态：SCAN 0 FOO（未知选项）正常应答不报错；SCAN 0 COUNT 0
   有键库单页至多 1 条；SCAN 0 TYPE Zset（混合大小写）→
   `*2\r\n$1\r\n0\r\n*0\r\n`；SCAN 0 TYPE zset 正常过滤
3. RI.CREATE：自建 config（test_store_config + with_range_index_dir）
   起服务器，CACHESIZE abc → "ERR Protocol Error: Unable to parse
   number: abc"；CACHESIZE 9223372036854775808 → overflow 文案；
   CACHESIZE 007 → 合法创建（前导零允许口径）
4. RI.LEN → C# RESP 面无此命令，服务器应答未知命令错误（−ERR unknown
   command），断言 RILEN 分派面已删

验收：./clippy.sh 零警告、./test.sh 全过、bun ./js/check.js 无新增
缺失/重复。

## 范围外记录（只记录不动手）

- 协议错误断连语义（见三）
- RI.SCAN/RIRANGE 其余参数口径（start/end 键解析、FIELDS 位置参数）
  未在清单，未逐 token 复核
- RespCommand 枚举 rust 侧含 Ripromote/Rirestore（C# 有枚举，rust 分派
  面未见对应 arm）——归属 RI 域完整性问题，非解析口径
