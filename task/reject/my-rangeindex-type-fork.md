拒件：GarnetObjectType 自造 RangeIndex=5 导致 TYPE 命令响应分叉

来源：next/agy.my.md 条 11、next/muse.my.md 条 6（同题合并）。判定：不成立（核心取证不实 + 自定义点系 SKILL 明示）。

拒绝原因
1 核心主张不实：TYPE 命令对分层态集合键不回 "rangeindex"。wedb/wnode/src/resp/array_commands.rs:500-553 network_type 对升阶键走 KeyTag::Meta 读 MetaValue.collection_type（升阶时存原始集合类型：rmw_helpers.rs apply_rmw_post_operate 传原 tag，wkv/src/range_index/stub.rs:185 MetaValue::new(key_id, obj_type, count)），回显 zset/hash/set/list 标准名；muse 条 6「Meta 借对象枚举判树态混用」同不成立。
2 自定义点合法：RangeIndex=5 是 transpile SKILL 类型枚举条款明示设计（「Null=0, SortedSet=1, List=2, Hash=3, Set=4, RangeIndex=5, All=0xfb」）。纯 RangeIndex 键（RI.CREATE 产物）TYPE 回 "rangeindex" 属自定义扩展面（仓内亦有 vectorset 同类先例），无 C# 对标义务。
3 muse 条 6 增量（注释措辞）：wval/src/tag.rs:135 枚举头注自称「1:1 对标 C# libs/server/Objects/Types/GarnetObjectType.cs」而 C# 枚举（Null/SortedSet/List/Hash/Set/All，garnet/libs/server/Objects/Types/GarnetObjectType.cs）无 RangeIndex——一行注释措辞与 SKILL 扩展事实不符，随本拒件记录，改注释属顺手打磨不立项（可并入任一触及 wval/tag.rs 的在途票）。
