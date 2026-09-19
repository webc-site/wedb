SCAN TYPE 值改精确两形态比对（混合大小写按 C# 走未知类型臂）

来源：next/glm.data.md 条 3（该文件已被并发分拣波消费删除，原文转抄存于
/Users/z/git/db/wedb/next/scan-type-mixed-case-match.md，本档按当下主仓代码重新取证）。
取证基线：主仓 /Users/z/git/db/wedb，
分支 dev，HEAD a7402c4（bb06827、6311510 两轮复核：本档取证文件未变、锚点未位移），全部行号按当下代码 grep 复核。

现状

rust 的 SCAN 参数解析单点 /Users/z/git/db/wedb/wedb/wnode/src/resp/array_commands.rs:70
`parse_scan_filter`，TYPE 值判定在 :113-131：五个已知类型依次用
`type_arg.eq_ignore_ascii_case(b"zset"/b"list"/b"set"/b"hash"/b"string")`（:116-127）比较，
任何大小写形态（"ZsEt"、"HaSh"、"STRING"、"sTrInG"）都被接受并映射成对应过滤类型，
照常执行扫描并返回真实命中集；只有完全不认识的字节串才置 :128 `type_unknown = true`。

未知类型臂的语义是对的：/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:233
`C::Scan` 臂在 :243-246 对 `type_unknown` 短路，回空键列表 + 游标 0，注释自陈对位
C# ArrayKeyIterationFunctions.cs:82-84。也就是说「未知类型 → 空结果」这条 C# 行为
rust 已经具备，缺的只是把混合大小写归入未知形态。单测
/Users/z/git/db/wedb/wedb/wnode/src/resp/array_commands.rs:708、:734、:738 已按现口径断言
（:734/:738 断言 type_unknown 为真），改口径时这三条断言的用例形态需同步复核，
防止把旧的宽容口径钉死。

C# 参考

/Users/z/git/db/wedb/garnet/libs/server/Resp/ArrayCommands.cs:311 把客户端给的类型串
原样取出（不做任何归一），:315-316 直传 DbScan；
/Users/z/git/db/wedb/garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:51-86
`DbScan` 用 `typeObject.SequenceEqual(CmdStrings.ZSET) || typeObject.SequenceEqual(CmdStrings.zset)`
这类精确比对（:57、:61、:65、:69、:73 五对，全大写或全小写各一），
混合形态既不匹配任一已知类型，也匹配不到 :77 的 STRING 反查条件，于是落到 :81-85 的
Unexpected typeObject 分支：`storeCursor = lastScanCursor = 0; return true;`，
即回空键列表 + 游标 0，不触达任何扫描。两形态常量的字面值见
/Users/z/git/db/wedb/garnet/libs/server/Resp/CmdStrings.cs:368-374
（ZSET/zset、LIST/list、HASH/hash、STRING/stringt="string"，SET/set 同段）。

复现：`SCAN 0 TYPE ZsEt` —— C# 回 `["0", []]`，rust 按 SortedSet 过滤返回真实命中集合。

修法

把 :116-127 的五个 `eq_ignore_ascii_case` 改成精确双形态比较：每个类型比
`type_arg == b"zset" || type_arg == b"ZSET"` 两式（list/set/hash/string 同形，
string 的 C# 常量名是 stringt 但值为 "string"，别按常量名写成 "stringt"）。
不新增第三条宽容分支、不引入大小写归一辅助函数：混合形态自然落到 :128 既有
`type_unknown = true`，快慢两路径的空结果短路（slow.rs:243-246 与同步 SCAN 段同口径）
零改动即生效。:66 的函数文档注释现明文写着「TYPE 匹配使用 eq_ignore_ascii_case
支持混合大小写」，把分叉当承诺在宣示，改口径时该注释必须同步改写为「TYPE 取值为
C# 两形态精确比对（全大写/全小写），其余按未知类型回空」，不留自相矛盾的描述。
若裁决认为该差异应保留为「刻意宽容」，则必须在本仓口径文档里写明
偏离 C# 的理由并在 :116 注释钉死，不允许现状这样无声分叉——本单按对标 C# 收紧执行。

MATCH/COUNT 的选项名比较（:91、:97）与 TYPE 选项名本身（:110）不改：C# 侧
`parameterWord.EqualsUpperCaseSpanIgnoringCase`（ArrayCommands.cs:279、:291、:305）
对选项名本就是大小写不敏感，rust 同口径，分歧只在 TYPE 的取值上。

优先级

功能缺口（参数校验口径与 C# 分叉，客户端可见：同一命令两侧返回集不同）。

交叉引用

/Users/z/git/db/wedb/task/ing/resp-null-protocol-single-source.md 与本单同在 SCAN 应答域
但不同位点（本单只动类型判定，不动 null 帧）。SCAN 慢路径臂所在
/Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs 的函数体量拆分见
task/ing/garnet-api-slow-path-command-split.md（认领前在
/Users/z/git/db/wedb/next/garnet-api-slow-path-command-split.md），本单不扩体，只改
parse_scan_filter 内的比较式，两单可并行。

勿双花：/Users/z/git/db/wedb/next/scan-type-mixed-case-match.md 是并发分拣波从
next/glm.data.md 条 3 原样转抄的六行 stub，主题与本单同一，派单以本单为载体
（本单已按当下代码复核锚点并给出改动边界与验收），认领时把该 stub 一并清掉，
不得按 stub 另开第二单。

验收

1. `SCAN 0 TYPE ZsEt`、`SCAN 0 TYPE HaSh` 回空列表 + 游标 0；`TYPE zset`、`TYPE ZSET`
   仍按类型过滤命中。
2. 快路径与慢路径（冷键降级）两形态结论一致。
3. array_commands.rs 既有 :708/:734/:738 断言按新口径复核后保持全绿，
   cargo check 零告警（禁写 allow）。

盘点补记（qw13.invA scan-type-exact-case-forms）：dev e75716e 复核原样：array_commands.rs:66 注释仍自述支持混合大小写、:116-127 五类型仍 eq_ignore_ascii_case。极轻棒不变。
