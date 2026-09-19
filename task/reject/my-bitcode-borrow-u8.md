拒件：bitcode 编码缺 &[u8] 借用视图导致集合序列化深拷贝，主张 HashWire<'a> 借用编码

来源：next/agy.my.md 条 17、next/muse.my.md 条 17（同题合并）。判定：不成立（上游能力边界已声明，选型系 SKILL 明示）。

拒绝原因
wcol/src/hash/hash_object.rs:125-128 HashWire 头注明文：「刻意差异：bitcode 0.6 的零拷贝借用编码仅支持 &str，&[u8] 无 Encode 实现，编码侧无法借引条目免 clone（owned 收集为格式约束下的最优解）」——muse 条 17 的诉求（「注明 owned 最优」）已注明在案。借用视图 HashWire<'a> 在 bitcode 0.6 的 Encode 实现面下不可实现（byte slice 无 Encode impl，实测 crates.io 0.6.9 源）；改手写流式编码违反 SKILL「编码尽量用 bitcode，而不是手写，不需要和 c# 格式兼容」的选型。SKILL 零拷贝准则射程为读路径 RESP 借用 API，不含序列化编码面。C# 对标 GarnetObjectSerializer.cs Serialize 亦为 owned 序列化（MemoryStream 写出）。

引证
wedb/wcol/src/hash/hash_object.rs:125-128、:216-231 serialize_wire；wedb/wcol/src/zset/sorted_set_object.rs、list/set 各 Wire 同型注释。bitcode-0.6.9 借用面仅 str（ coder.rs Unaligned 文档）。
