优先级：低

问题：wval 的 GarnetObjectType 枚举文档注释自称「1:1 对标 C# libs/server/Objects/Types/GarnetObjectType.cs」，但 C# 该枚举只有 Null=0/SortedSet=1/List=2/Hash=3/Set=4/All=0xfb，无 RangeIndex 成员。RangeIndex=5 是本仓自定义扩展（transpile SKILL 类型枚举条款明文规定），C# 侧 RangeIndex 是独立存储形态（RangeIndexManager.RangeIndexRecordType）而非对象类型成员。注释的「1:1」措辞会误导维护者去 C# 枚举里找不存在的成员，或误以为加了 C# 没有的东西是偏差。

取证（rust）：
- wedb/wval/src/tag.rs:135 枚举文档注释「Garnet 全局统一对象与集合类型枚举 (1:1 对标 C# libs/server/Objects/Types/GarnetObjectType.cs)」
- 同文件 `RangeIndex = 5` 变体与 as_str 回 "rangeindex"（TYPE 命令回显口径与 C# 一致，无行为分叉）
- SCAN TYPE 过滤值无 rangeindex 形态（wedb/wnode/src/resp/array_commands.rs parse_scan_filter 仅 zset/list/set/hash/string 五值），与 C# DbScan 一致

C# 对标：
- garnet/libs/server/Objects/Types/GarnetObjectType.cs 枚举体无 RangeIndex 成员
- garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs HandleType 对 `srcLogRecord.RecordType == RangeIndexManager.RangeIndexRecordType` 特判回显 CmdStrings.rangeindext——行为上与 rust TYPE 回显一致，组织方式上 C# 是记录类型特判、rust 是枚举成员

修法建议：把 tag.rs 枚举头注释改写为「对标 C# GarnetObjectType（Null/SortedSet/List/Hash/Set/All 六成员）+ 本仓扩展 RangeIndex=5（transpile SKILL 类型枚举条款；C# 侧为 RangeIndexRecordType 独立存储形态，TYPE 回显口径见 ReadMethods.cs HandleType 特判）」，消除「1:1」的过度声明。仅改注释，不动枚举值与回显行为。
