重复：task/ing/scan-type-exact-case-forms.md（关键符号 SCAN TYPE/大小写形态/过滤类型 命中）
优先级：中

SCAN TYPE 值大小写匹配过宽，混合形态被当作合法过滤类型
    rust parse_scan_filter 对 TYPE 值用 eq_ignore_ascii_case 判定五类已知类型，"ZsEt"/"HaSh" 等任意混合大小写均被映射为对应过滤类型正常扫描；C# DbScan 用 SequenceEqual 精确匹配全大写或全小写两种形态（CmdStrings.ZSET||zset 等），混合形态落入 "Unexpected typeObject" 分支直接回空键列表 + 游标 0。复现：SCAN 0 TYPE ZsEt，C# 回 ["0",[]]，rust 按 SortedSet 过滤返回真实命中集。修法：TYPE 值匹配改为 b"zset"/b"ZSET" 双形态精确比对（或声明该差异为刻意宽容）。
    rust：wedb/wnode/src/resp/array_commands.rs:110-131 parse_scan_filter（TYPE 分支 eq_ignore_ascii_case）
    C#：garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:57-86 DbScan（typeObject.SequenceEqual 精确两形态 + 未知类型 :82-84 短路回空）
