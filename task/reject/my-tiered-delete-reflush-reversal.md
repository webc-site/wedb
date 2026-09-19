拒件：分层集合单元素删除退化为全树物化重灌，主张恢复树内原生逐成员删除（循环化 bf-tree ScanIter::next 尾递归）

来源：next/agy.my.md 条 1。判定：不成立（已决事项重诉，与主代理裁决冲突）。

拒绝原因
该条修法（循环化重构底层 bf-tree 0.5.6 ScanIter::next 尾递归消除墓碑跳过栈溢出，恢复分层树原生页级逐成员删除命令臂）正是 next/tiered-zset-demote-bf-tree-recursion-stack-overflow.md 的裁决 A，已判拒：为约 20 行尾递归 vendor 上游 crate（src 700K/42 文件）属污染扩散，代价与转写使命不成比例。裁决 B（删除重臂 HDEL/SREM/ZREM/LPOP/RPOP/SPOP 摘出树内臂、统一走物化 + 整值重灌）已落地（分支 fix-tiered-tombstone-density，3f48bc1d + df35b160，含验收实测），O(N) 重灌代价是裁决明示接受的取舍；TTL 残余墓碑源已另立 next/tiered-ttl-tombstone-residual-source.md 承接。

引证
wedb/wnode/src/resp/objects/tiered_collection_ops.rs:7-35 模块头注「写形不变量『树内墓碑恒低』」即该裁决落地自述（含「C# 侧对分层集合的成员增删走 PostCopyUpdater 整值写回，本仓逐成员墓碑删除才是偏离」的论证）。garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:188-215 PostCopyUpdater（C# 无按成员分记录的树、无墓碑连跑）。按既定裁决不再翻案；如需重开须主代理在新证据下显式推翻 A/B 裁决，非本域分拣可改。
