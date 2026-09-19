拒件：从库回放把条目物理域当逻辑域二次映射（set_context + flush_database）

来源：next/muse.my.md 条 12。判定：不成立（已修，源描述过期）。

拒绝原因
现状已按「从库完全继承主库映射体系，不二次映射」实现：
1 keyed 条目：wnode/src/aof/aof_processor.rs:1173-1214 KeyContextGuard.enter 经 NamespaceDbCodec::decode_tagged_key 解出 (vns, vdb, tag, user_key) 后 `set_virtual_context(vns, vdb)` 直设物理域，drop 复原；头注明言「零逻辑解析、零虚号分配、零 DbMeta 落盘」，对位 C# AofProcessor.cs:SwitchActiveDatabaseContext 切既有库实例不重解析域号。
2 Flush 族：aof_processor.rs:572-617 FlushDb/FlushNs 回放分别走 flush_virtual_database(vns, old_vdb) / flush_virtual_namespace(old_vns) 物理域退役原语，注释明言「条目载荷已是物理号，灌逻辑域入参的 flush_database 即从库本地二次映射」——即本条诉求（「回放改 set_virtual_context 直设物理域」）的 Flush 面等价物已在位。
该形态即 qw13.invB 盘点时确认落地的 aof-replay-virtual-domain-context 票结果。残余真缺口是「新虚号不在载荷、从库本地取号分叉」，已另立 next/my-flush-replica-virtual-id-divergence.md，不在本条射程。

引证
wnode/src/aof/aof_processor.rs:572-617/:1173-1214；wkv/src/vdb.rs:1046-1080 flush_db_virtual、:1124 起 flush_ns_virtual；garnet/libs/server/AOF/AofProcessor.cs:SwitchActiveDatabaseContext。
