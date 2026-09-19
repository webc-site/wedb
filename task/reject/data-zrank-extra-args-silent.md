拒绝：ZRANK/ZREVRANK 多余参数静默忽略，不改为严格校验报错

来源：next/agy.data.md 条 5
结论：不成立（行为与 C# 逐字一致，改严格校验违反 transpile SKILL 1:1 对标）

引证：
- C# garnet/libs/server/Resp/Objects/SortedSetCommands.cs:696 SortedSetRank：`parseState.Count == 3` 才校验 WITHSCORE（EqualsUpperCaseSpanIgnoringCase），Count>3 时 includeWithScore 保持 false 静默忽略多余参数，无 syntax error 路径
- rust wedb/wnode/src/resp/objects/sorted_set_commands/read.rs:210 sorted_set_rank 已逐行为对标，且 :220-221 注释已写明「C# 仅 Count==3 时校验 WITHSCORE…Count>3 静默忽略多余参数」
- Redis 规范确实要求报 syntax error，但 transpile SKILL 明文「尽量 1:1 对标 c# 的代码实现，不要实现自己的优化」；ag 建议的参数个数严格校验属于主动偏离 C#，拒绝
