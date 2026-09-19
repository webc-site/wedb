重复：task/ing/zset-aggregate-member-ttl-filter.md（关键符号 ZUNION/ZINTER/成员级 TTL 同名同题 命中）
优先级：中

zset 聚合族内核不过滤成员级 TTL 已过期成员，过期剔除只发生在装载时刻
    rust ZUNION/ZINTER/ZDIFF/ZINTERCARD 及 STORE 族聚合直接遍历 sorted_set_dict/sorted_set 原始容器，全链零 is_expired 判定；过期成员的剔除仅靠装载时 from_blob → deserialize_from_slice 的 expirations 过滤兜底。成员在装载之后、聚合遍历之前到期（毫秒级 TTL；慢路径 load_many_cold 逐键 await 装载磁盘冷键可把窗口拉大到毫秒~几十毫秒）时，过期成员被计入聚合结果并随 STORE 族落盘目标键。C# 聚合内核全部在聚合时刻过滤：ZUNION/ZUNIONSTORE/ZINTER/ZINTERSTORE/ZINTERCARD 经 SortedSetObject.Dictionary getter（堆顶已到期即重建过滤视图，无过期结构时才直返原字典），ZDIFF/ZDIFFSTORE 经 CopyDiff/InPlaceDiff（双侧 IsExpired 过滤，单键 ZDIFF 的 CopyDiff(first, null) 同样过滤）。另 rust ZINTERCARD 单键分支用 count()（过滤口径）而多键分支遍历原始 dict，同函数两分支口径不一。修法：diff_sets/combine_sets 与 ZINTERCARD 多键遍历对位补 is_expired 过滤（含 diff_sets 的 rest.is_empty → first.clone() 单键分支）。
    rust：wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:886 diff_sets、:907 combine_sets、sorted_set_intersect_length 多键遍历（:419-435 附近）；调用链 write.rs:278/:305/:346/:481/:715 与 slow.rs diff_sets/combine_sets/ZINTERCARD 各臂
    C#：garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:237-256 Dictionary getter、:537-567 CopyDiff、:572-584 InPlaceDiff；garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:1207/:1242 SortedSetUnion 内核、:1524/:1529 SortedSetIntersection、:1368 SortedSetIntersectLength
