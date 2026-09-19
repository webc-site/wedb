拒件：TTL 后台清理与 GC 物理清退未推进 WATCH 版本栅栏

来源：next/agy.my.md 条 18、next/muse.my.md 条 11（同题合并）。判定：不成立（rust 现状即 C# 口径；补推进属 C# 外新优化）。

拒绝原因
C# IncrementVersion 全部挂在写路径完成钩子：SessionFunctionsUtils.cs:120/:140/:160（SingleWriter/PostSingleWriter 族）、ObjectStore/UpsertMethods.cs:48/:58/:68、ObjectStore/DeleteMethods.cs:21/:30、ObjectStore/RMWMethods.cs:79/:100/:125/:200、UnifiedStore 与 VectorStore 同族——C# 惰性过期（读路径判过期即返回 nil，不物理删）与紧缩丢弃（Compaction 的 IsDeleted 过滤）均不推进版本表。rust 的 purge_expired 物理删链路不 bump（wkv/src/ttl.rs:327-342 -> collection.rs delete -> delete_raw 无 bump_watch_version 调用）正是对齐该口径；紧缩 is_deleted 丢弃同理。给后台清除补推进属 Redis 官方「过期打断 WATCH」语义，C# Garnet 未实现，SKILL「尽量 1:1 对标 C#，不要实现自己的优化」禁止自造。若将来要对齐 Redis 官方语义，属规范层（SKILL 修订）决策，非缺陷票；源条目自给的备选「注释界定 WATCH 仅针对显式写命令生效」与现状注释（finish_tiered_arm 头注的双向契约论述）已基本覆盖。

引证
garnet/libs/server/Transaction/WatchVersionMap.cs:IncrementVersion 及上述调用面；wedb/wtxn/src/watch_version_map.rs 对位实现；wedb/wkv/src/ttl.rs:315-342 purge_expired（统一 DEL 路径保证 WAL 钩子一致的约定即指 AOF 镜像，非 WATCH）。
