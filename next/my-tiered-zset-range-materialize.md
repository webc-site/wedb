优先级：高

问题
分层态 SortedSet 范围与排名命令全缺失，穿透全量物化。树内仅按 member 字典序存 member -> 8B score，只承接 ZADD/ZSCORE/ZMSCORE/ZCARD/ZINCRBY/ZEXPIRE/ZTTL/ZPERSIST。ZRANGE/ZRANGEBYSCORE/ZREVRANGE/ZRANK/ZREVRANK/ZCOUNT/ZPOPMIN/ZPOPMAX 等在 tiered_zset_arm 末尾统一 Ok(false) 穿透：读族经 slow_load_eval 的 Degrade 臂全树物化求值（千万级集合一次 O(N) 内存物化）；写族经 run_async_rmw -> apply_rmw_post_operate，tiered 且不降阶时更触发 handle_bftree_drain_and_delete + promote_collection_to_bftree 整树销毁重灌（O(N) 物化 + O(N log N) 重建 + AOF 全量重发）。违背 SKILL「消除全量反序列化读放大」「对外 RESP 命令透明统一」承诺，大键一条 ZRANGE 即巨大延迟与内存抖动。

取证（dev 当下代码重取）
wedb/wnode/src/resp/objects/tiered_collection_ops.rs:597-607 zset_needs_write 仅列 Zadd/Zincrby/Zexpire/Zttl/Zpersist/Zcard；:1710 match 末尾 `_ => Ok(false)` 承接全部范围/排名/弹出操作；:1453 分值落树形态 `&score.to_be_bytes()` 以 member 为键（无分值序索引）。读路径 wedb/wnode/src/resp/objects/rmw_helpers.rs:468-485 ObjLoad::Degrade -> tiered_materialize_blob 一次性物化；写路径 rmw_helpers.rs:368-392 apply_rmw_post_operate 重灌臂。

C# 对标
garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetRangeByScore、SortedSetRange（C# 内存 SortedSet 有序结构，范围 O(log N)，任意规模语义恒定）。

修法建议
在 wbftree 建以 score+member 复合编码的有序双向索引（第二棵树或组合键），实现原生页级范围扫描与排名；升降阶导出（export_entries）与该索引同源。若裁决认为代价过大，须在 doc/zh/collection.md 显式声明分层 zset 范围命令的性能折损并给限流口径，不得维持「透明统一」的失实承诺。落地须与 next/tiered-ttl-tombstone-residual-source.md（TTL 重灌收敛）同定写形，避免两次重建面打架。
