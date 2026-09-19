拒件：批量单次折叠半边落地，「通用写批量缺 enter_batch 单纪元漏斗」

来源：next/muse.my.md 条 9。判定：不成立（无具体缺失面；run_sync_rmw 是单命令内核无 C# 跨命令对位）。

拒绝原因
写批量面逐个核对俱在：(1) MSET 批量折叠——wkv/src/session/mod.rs:899-931 try_upsert_batch_sync，单次进纪元 + 前缀单次外提 + 排序折叠（对标 C# MainStoreOps.cs:MSET_Conditional，C# 唯一的跨键写批量面）；(2) 树内写批量——tiered_collection_ops.rs:215-226 tree_put_batch（分层臂即全仓唯一树写面，HSET/SADD 已接）；(3) 升阶 bulk_load 栈上排序单次借用单次 size 回写（票面自认正确）；(4) 读批量 read_batch_with 前缀外提（票面自认正确）。剩指控「通用 run_sync_rmw 逐命令取锁」：run_sync_rmw 是单命令 RMW 内核（一条命令一次装载-求值-写回），C# 无跨命令聚合写批量形态，不存在可折叠面；同键原子性是另一题（next/rmw-atomic-read-modify-write-window.md），与批量漏斗无关。源条目自给的备选结论「或注释写清仅读批量与升阶享折叠」即现状注释口径。

引证
wkv/src/session/mod.rs:899-931；wnode/src/resp/objects/tiered_collection_ops.rs:197-226；garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs:MSET_Conditional。
