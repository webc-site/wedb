拒件：升阶（首升阶 envelope -> tree）数据流广播与元记录落盘非原子，崩溃留双态残留

来源：next/agy.my.md 条 15、next/muse.my.md 条 1（同题合并）。判定：不成立（诉求已被现状实现与文档承接；改序反而更差）。

拒绝原因
逐项核对诉求与现状：
1 「删空后回落信封域幽灵复活」：已修。wkv/src/range_index/stub.rs:67-72 handle_bftree_drain_and_delete 的删键臂（keep_ttl=false）连带对信封域写幂等墓碑（delete_raw(env_k)，墓碑先于元记录落笔），排空后旧信封不再可达；头注 :51-66 完整记载该双态残留的机理与兜底。
2 「删信封禁静默吞错」：已修。stub.rs:212-216 `self.delete_raw(&env_k).await?` 硬错上抛令升阶命令失败，注释明言「禁 let _ = 静默吞错」——正是 task/ing/tiered-drain-envelope-tombstone.md（已落地）的两个修点。
3 「崩溃于 AOF 入队后、元记录落盘前」：RangeIndexStream 条目经 AOF 重放重建树与元记录（主库重启回放自身 AOF 即收敛），无 AOF 嵌入式形态本就不发流。
4 「先墓碑信封再落 Meta」的改序主张：现序（先 Meta 后删信封）配 Meta 优先路由使双态期读写均正确（信封不可达）；倒序会制造「信封已死、Meta 未落」的键整体不可见窗口，属数据丢失级恶化，不予采纳。
5 重灌臂（tree -> tree 先拆后建）的原子换入是另一个真缺口，已单独立项 next/tiered-reflush-atomic-swap.md，不与首升阶混票。

引证
wedb/wkv/src/range_index/stub.rs:45-66（排空单点信封墓碑文档）、:188-221（先发流后落 Meta、删信封 ? 上抛）。C# 对标 RMWMethods.cs InPlaceUpdaterWorker 单记录原子性系单物理域前提，rust 双域换域的兜底即上述墓碑设计。
