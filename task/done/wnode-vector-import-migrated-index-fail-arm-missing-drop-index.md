归档注记：合入 ff84360a，写透/复制失败臂 drop_in_memory_index 精确回收，C# DropIndex 对位，位滞留交重启 reconcile

甄别结论：通过（甄别席 J2，2026-09-27，定级 P2——导入迁移建索引后写表/复制失败臂裸 Err 不回收，in_use 位滞留）。现码亲读：create_index 成功置 index_ptr=1 后，write_stored_index 失败臂与 replicate map_err 臂均裸 return Err 不回收刚建索引（vector_manager_migration.rs:163-196）；C# 对位臂亲核 VectorManager.Migration.cs:229-237 writeRes!=OK 即 Service.DropIndex(context, newlyAllocatedIndex) 后 throw；next_not_in_use 判据即 in_use 位（vector_manager_context_metadata.rs:131-146）亲读成立，滞留面与危害有界论证成立。派沙箱席 c01c。

审核结论：通过（P2 真案。①亲验坐实：import_migrated_index create_index 成功后（vector_manager_migration.rs:166-173），write_stored_index 失败臂（:175-180）与 replicate 失败臂（:183-185）均直接 return Err，刚建原生索引不回收；C# HandleMigratedIndexKey 写失败臂显式 Service.DropIndex(context, newlyAllocatedIndex) 后 throw（VectorManager.Migration.cs:229-237），即时回收对位分叉确认。②滞留面确认：失败后 context in_use 位滞留，next_not_in_use 判据即 in_use（vector_manager_context_metadata.rs:131-146）故运行期不复用；重启 reconcile 弃迁臂（vector_manager.rs:1477-1488 get_migrating 收敛）与 FLUSH sweep 为既有收敛通道——危害有界，与票自陈一致，定 P2。③票内矛盾订正：方案 2 锁测「断言 context 立即可复用」与方案 1「仅补 drop_index 对位 C# 形」相抵（drop_index 不清 in_use 位达不成复用；仓内 release_vector_set_context 复用亦不可行，其 release 只清 in_use 与 slot 不清 migrating 位，会留幽灵 migrating 位并触重启 reconcile mark_migration_complete 的 debug 断言），已按对位 C# 形订正锁测，见文末执行方案。④查重：五池无同题票（wedb-migrate-import-frame 票为 RMW 窗口锁面、wedb-migrate-source-vector-set-delete 票为源端 AOF 复制面，正交），deviations 无在册裁决覆盖此失败臂）

vector import_migrated_index 写透失败臂缺原生索引回收：刚建的 create_index 不 DropIndex，context 滞 migrating 位直至重启 reconcile（C# 同点位显式 Service.DropIndex）

问题分析：
1 Garnet 契约对齐：C# HandleMigratedIndexKey 写失败臂显式回收——garnet/libs/server/Resp/Vector/VectorManager.Migration.cs:230-237 writeRes != OK 即 Service.DropIndex(context, newlyAllocatedIndex) 后 throw，context 即时归还复用。
2 工程现状确证：wedb/wnode/src/resp/vector/vector_manager_migration.rs:163-180 create_index 成功后 index.index_ptr = 1，:174-178 write_stored_index 失败直接返回 Err，:166-173 刚建的原生索引不回收；:183-185 replicate 失败臂同形（登记已落、context 滞 migrating）。危害有界：migrating 位使 id 不复用（next_not_in_use 跳过）、重启 reconcile 弃迁臂（vector_manager.rs:1477-1488）或 FLUSH sweep 收敛——修复向缺口非危面。
3 逻辑危害确证：迁移高峰期连续失败时 context 池单调耗减直至重启；与 C# 即时归还语义分叉，运行期无自愈路径。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/vector/vector_manager_migration.rs:import_migrated_index 失败臂（:163-196）

对应 c# 文件与函数：
garnet/libs/server/Resp/Vector/VectorManager.Migration.cs:HandleMigratedIndexKey 写失败 DropIndex 臂（:229-237）

精炼执行方案：
1 write_stored_index 与 replicate 失败臂补 service.drop_index(context) 回收（对位 C# 形），保持 Err 返回不变
2 锁测：注入写透失败桩，断言失败后 context 立即可复用（next_not_in_use 命中）

审核裁定执行方案（审核席订正版，供 fix 消费）：
1 write_stored_index 失败臂（:175-180）与 replicate 失败臂（:183-185）各补原生索引回收：经既有 drop_index 单点（vector_manager.rs:1119，value 消费内嵌 context，此时 index_ptr 已置 1、value 形态可直接喂入）丢弃刚建索引，Err 返回保持不变，对位 C# Service.DropIndex 臂形；context in_use 与 migrating 位滞留维持交既有重启 reconcile 弃迁臂收敛（与 C# throw 后位滞留同形）。严禁改走 release_vector_set_context 即时归还：其 release 不清 migrating 位，in_use 清零而 migrating 残留的幽灵位会触重启 reconcile mark_migration_complete 的 debug 断言，且越 C# 对位形
2 锁测订正：注入写透失败桩，断言失败后服务侧原生索引已丢弃（drop 可观测）、返回 Err 保持、context 位滞留可经 reconcile_recovered_state 收敛；票面原「context 立即可复用（next_not_in_use 命中）」断言不采纳（drop_index 不清 in_use 位，该断言不可达）
