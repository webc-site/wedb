优先级：低

问题
wcol 内存对象 count 同名双口径：内建 count(&mut self) 先 delete_expired_items 自毁式清过期再 len，trait IGarnetObject::count(&self) 为原始 len 只读。两口径同名不同义：&mut 语境下方法解析优先命中内建版，调用方以为只读实则改对象（C# Count() 是纯读扣除，不物理删）；升阶链路用 raw len、命令链路触发 purge 版，误用面随调用点增长。

取证（dev 当下代码重取）
wedb/wcol/src/hash/hash_object.rs:571-574 与 wedb/wcol/src/zset/sorted_set_object.rs:571-574 内建 `pub fn count(&mut self)`（delete_expired_items 后 len）。wedb/wcol/src/types/garnet_object.rs:118/:190/:251/:317 trait `fn count(&self)` 原始 len。&mut 消费面示例：wedb/wnode/src/resp/objects/sorted_set_commands/slow.rs:865-868 ZPOPMIN 判空与 min 钳制（&mut obj 命中内建版）。

C# 对标
garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:606-618 Count()（HasExpirableItems 短路 raw Count，否则遍历过期字典只读扣除，零物理删除）；garnet/libs/server/Objects/Hash/HashObject.cs:Count 同口径。

修法建议
内建口径二选一：对齐 C# 改只读扣除（count = len - 过期数，零删改），或更名 purge_expired_len 显式自毁语义；trait count 保持只读。命令面计数已走 O(1) 快道（count_of_blob / MetaValue.size），本票只收口径与命名，不动快道。来源 next/muse.my.md 条 4。
