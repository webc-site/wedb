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

落地（2026-09-19，docs-data-comment-batch 棒）
裁决：成立。现刻复核 C# 枚举体确实只有 Null=0/SortedSet=1/List=2/Hash=3/Set=4/All=0xfb，
无 RangeIndex 成员，旧注释的「1:1 对标」属过度声明。wval/src/tag.rs 枚举头改写为「对标 C# 六成员
+ 本仓扩展 RangeIndex=5（transpile SKILL 类型枚举条款明文规定）」，并写明 C# 侧 RangeIndex 是
RangeIndexManager 的独立存储形态、TYPE 回显由统一存储读侧对该记录类型的特判给出（回显
CmdStrings.rangeindext，口径与 rust 一致）、SCAN TYPE 过滤值仍只认五值不含 rangeindex，
末段保留原有权威定义与用途两行。枚举值、as_str/strum 回显、from_u8 零改动。
C# 复核锚点：ReadMethods.cs 的 HandleType 内对 RangeIndexRecordType 的特判在 :151（票面写
:126 附近，同函数内，不构成分叉）。
旁证：task/reject/my-rangeindex-type-fork.md 早已把本措辞问题记为「改注释属顺手打磨不立项，
可并入任一触及 wval/tag.rs 的在途票」，本票即该顺手件的载体，非第二处双花。
门禁：cargo check 零错误零警告；锚点集合与改前逐条相同（新增文本全部用不带冒号的写法，
不新登记也不弄丢既有条目）。提交：649db9d；回合 dev fast-forward 至 9c06e3b。
