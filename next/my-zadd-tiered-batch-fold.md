优先级：中

问题
分层态 ZADD 树内写入未走批量折叠与局部排序，逐成员三次引擎穿透。主循环对每个成员依次 tree_member_state（读穿透）、tree.read_callback（二次读穿透）、tree_put_ok（写穿透），未如 HSET/SADD 采用 tiered_precheck + tree_put_batch 单次排序批量 upsert。批量 ZADD 大成员集时树页重复借用、页缓存抖动与页分裂放大，违背 SKILL「树内写入先做栈上局部排序，批量集中命中单页，压降页分裂」的批量折叠承诺。

取证（dev 当下代码重取）
wedb/wnode/src/resp/objects/tiered_collection_ops.rs:1417-1501 ZADD 主循环（:1430 tree_member_state、:1433 read_callback、:1453/:1475/:1492 tree_put_ok 逐成员）。对照同文件 :695-732 HSET/HMSET 批量臂与 :1140-1174 SADD 批量臂（precheck 全量前置 + tree_put_batch 单次下刷 + 返回真实新增数入账）。

C# 对标
garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd（C# 内存字典无树页概念；批量折叠系 SKILL 性能准则对分层写面的自定义要求）。

修法建议
按选项分派：无 XX/NX/GT/LT/INCR 的纯新增路径（CH 不影响写形）先全量 tiered_precheck，成员在栈上/小缓冲按字节序排序去重后走 tree_put_batch，返回新增数直接入账；带 NX/GT/LT/INCR 的路径保留逐成员（需旧分值比对与 INCR 回值语义），注释写明分派判据。中段解析错误提前出口的 commit_new_members! 记账口径保持不变。
