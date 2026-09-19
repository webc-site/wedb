拒绝：LPOS 选项大小写保持全大写/全小写双形态，不改 eq_ignore_ascii_case

来源：next/agy.data.md 条 6
结论：不成立（忠实转写 + 注释已标明；改宽容形态违反 1:1 对标）

引证：
- C# garnet/libs/server/Objects/List/ListObjectImpl.cs:464 ReadListPositionInput：`sbParam.SequenceEqual(CmdStrings.RANK) || sbParam.SequenceEqual(CmdStrings.rank)` 双常量精确比对，混合形态走 RESP_SYNTAX_ERROR
- rust wedb/wcol/src/list/list_object_impl.rs fn read_list_position_input：`sb_param == b"RANK" || sb_param == b"rank"` 同形态；其上文档注释已写明「C# SequenceEqual 双常量（RANK/rank、COUNT/count、MAXLEN/maxlen）仅认全大写或全小写，混合形态报语法错误」
- 原条目自己的动作 a（维持一致 + 注释标明）已经达成；动作 b（按 Redis 改 eq_ignore_ascii_case）与 transpile SKILL 1:1 冲突，拒绝
