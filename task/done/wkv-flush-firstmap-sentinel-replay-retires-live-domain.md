归档注记：合入 524de931，FirstMap 形返回 None 旧域跳过广播与回收，哨兵条目灭绝，retire 存活命中臂仅兜并发换号窗

甄别结论：通过（甄别席 J6，2026-09-27，定级 P1——FLUSHALL/resolve TOCTOU 下哨兵回放退役存活域，取号换格数据丢失）。亲验：哨兵生产 keyspace.rs:176/:334（unwrap_or 自陈「RESP 面不可达」）、retire 存活命中臂本地取号换格 :356-367、判死登记 :369-379；回放裸解 aof_processor.rs:614/:635-641/:652/:669，红线自陈 :609-611 与实码矛盾确凿；无条件入队 single_database_manager.rs:552/:567。TOCTOU 可达：resolve 在 lock_dbmeta（:137）之前，FLUSHALL 持锁 reset 交叠成立；FLUSHALL_NS 漏斗实锚 cluster_session/replication.rs:336（票面与审核席 :361-368 均漂，勘误随票注）。查重五池+deviations 换号族各面正交。FirstMap 无旧域即无退役对象，跳过广播系灭伪条目非第二机制。派沙箱席 c01l。

审核结论：通过（P1）

定级理由：
1 真实性亲验成立：哨兵生产（keyspace.rs:178 old_vdb_opt.unwrap_or(new_vdb)、:334 old_vns_opt.unwrap_or(new_vns)）→ 条目无条件入队（single_database_manager.rs flush_database/flush_namespace 直送 safe_flush_aof）→ 回放裸解直送 retire（aof_processor.rs:614/:635、:652/:669）→ retire 存活命中臂本地取号换格（keyspace.rs:357-367）四环逐行核实；aof_processor.rs:609-611 红线自陈「副本绝不在回放面本地取号换格」与 :635 实码自相矛盾成立。
2 成败点核验：「RESP 面不可达」确被证伪，但仅路径 (a) 族成立。flush_database/flush_namespace 取锁前 resolve（keyspace.rs:134/:294）与取锁后 swap 之间的 TOCTOU 窗内，任一持同锁换号臂抹除映射即 FirstMap：FLUSHALL(ns0) 持锁跨异步 shift_begin_address 执行 vdb.reset 整表换新（keyspace.rs:450-455、manager.rs:99-125 清 ns_map/db_routing 仅留根），并发 FLUSHDB 交叠即成立（reset 后 swap_db 的 get_or_create_ns 盲分配 manager.rs:208-220 落新空表，swap_out None）。且可达面比票面更宽：非 0 租户 FLUSHALL（slow.rs:1236-1241 → flush_namespace，swap_ns 重指 ns_map）与同 ns 并发 FLUSHDB 交叠即 FirstMap，无需超管无需截断——两条普通管理命令即触。顺序面（无并发）FirstMap 确不可达：resolve 面（vdb_load.rs:124-155）取锁前必物化映射，哨兵纯属竞态窗产物，与票面主张一致。
3 票面路径 (b)（rollback_flush_ns None 臂删除 ns_map[logic_ns] → 同 ns 下一个 FLUSHNS swap_ns 返回 None）不成立为独立路径：下一 FLUSHNS 取锁前 resolve_ns_mapping（keyspace.rs:294）已重建 ns_map[ns]（vdb_load.rs:146-155 探查未命中即创建并持久化），顺序下一拍 swap_ns 得 Some 落 Swapped 而非 FirstMap；(b) 仅当其自身 resolve→锁窗内再叠一次抹除（即 (a) 族 TOCTOU）才成立，系 (a) 的派生而非第二独立证明。该瑕疵不动摇判定：(a) 族单证已足证伪。
4 危害形复核成立：形一——镜像批经存储事件先行入 AOF（service.rs:on_aof_store_event 放行 DbMeta，commit_swap 在 store.flush_database 内完成）、FlushDb 条目后入（safe_flush_aof 在其返回后），入队次序证据确凿；副本 apply_dbmeta_record DbMap 臂（keyspace.rs:211-216）先换格活，哨兵 retire 随后命中活格本地取号换格 + gc_dead 判死活域 + 向量登记域回收，读恒 miss、紧缩到期物理丢弃。形二——哨兵条目越检查点基线即恢复重放必达：内存 gc_dead 插入无盘墓碑（FirstMap 本无 0x03/0x04 记录），根域 retire 换指新号而盘上权威仍指 N、非根域 resolve_db is_dead 门拒装载另取新号（vdb_load.rs:103-108），两形皆崩溃前数据不可达且紧缩判死丢弃（manager.rs:503-519 内存账本驱动）；reclaim_registry 每轮恢复重放清活域向量登记表成立。票面「恢复后路由指 N 写读照常」机制描述欠准（实际路由必换离 N），危害结论不变。形三——实锚 wedb/wedb/src/server/cluster_session/replication.rs:361-368（票面路径 server/replication/replication.rs 有误，该目录无此文件），同 flush_namespace 漏斗同型成立。
5 方案评审：FirstMap 无旧域即无退役对象，清库语义由 DbMeta 镜像批（map + 0x05）完整承接，跳过广播条目系消灭伪条目非立第二机制，与 db.md 1.3「换号条目域载荷唯一合法语义是换号前旧域」对齐；返回携 Option<旧域> 同时让主库侧 reclaim_registry 两处调用同源门控（防 FirstMap 误收新哨兵域登记）。retire 臂 Swapped 正面职责不动。
6 查重：deviations.md §98/§99/§100/§115/§118/§131 换号族各管一面（ACL 认证器/快照装载读值域钉/双键移动写回序/WATCH 版本双轨/迁移 TTL 探针窗/SELECT watch 保全），与本面正交；issue/todo/ing/done/reject 五池零同题（wnode-flushall-destroys-acl 票属 FLUSHALL ACL 面，不并案）。
定级：P1 成立——危害重大（主从永久分叉 + 紧缩静默丢数据 + 恢复重放反复现形）但触发达须 FLUSH 族并发竞态窗，无单命令确定性触发，不足 P0。

执行方案（审核优化稿，供 task/fix.md 消费）：
1 flush_database/flush_namespace 返回值改携 Option<旧域>（(vns, Option<old_domain>)，FirstMap 形即 None），single_database_manager.rs flush_database/flush_namespace 按 None 跳过 safe_flush_aof 与两处 reclaim_registry——哨兵自生产端灭绝；FirstMap 清库语义由既有 DbMeta 镜像批完整承接，不立第二机制
2 锁测三点：FLUSHALL 竞态夹具（resolve 与取锁窗内插 reset / 同 ns swap_ns 换指，循 §99 同步核注入先例）断言 FirstMap 不产 Flush 广播条目且主库无 gc 登记；副本回放 FirstMap 镜像批 + 跟随 FlushDb 断言路由不本地换格、gc_dead 无活域判死；崩溃恢复重放同窗断言紧缩不判死活域、内存映射与盘上 DbMeta 权威一致
3 回归面：Swapped 正面（换号退役、SWAPDB 成对、FLUSHNS Some 臂）既有锁测全绿不回退；随修订正 keyspace.rs:175-177/:332-333 与 aof_processor.rs:609-611 注释为如实措辞（顺序面不可达、并发换号窗可达且已由哨兵灭绝收口）

以下为票面原文。

FLUSHDB/FLUSHNS 首映射哨兵域值使回放面退役活域：哨兵条目回放到副本/恢复面命中在册活格即本地换格判死——主从路由分叉、紧缩到期整域物理丢弃（注释自陈「RESP 面不可达」前提被 FLUSHALL 竞态与 rollback 队列两路径证伪）

问题分析：
1 Garnet 契约对齐：db.md 1.3「FLUSHDB：旧 ID 压入 GC 队列」——换号条目域载荷唯一合法语义是「换号前旧域」；db.md 1.4「从库完全继承主库的映射体系，不进行本地二次映射」；主从镜像段「GC 时序同步：主库换号即屏障」。C# 无对位（自研秒级换号架构，规范基准即 db.md）。aof_processor.rs:609-611 自陈红线「副本绝不在回放面本地取号换格——本地二次映射即主从分叉」。
2 工程现状确证：生产端哨兵 wedb/wkv/src/store/keyspace.rs:174-178（FLUSHDB）与 :332-334（FLUSHNS）——库/ns 首映射（无旧号）时以新号作非零哨兵填充条目域值，注释自陈「RESP 面不可达」；回放消费端 aof_processor.rs:614/:635 与 :652/:669 裸解域载荷直送 retire_dead_domain/retire_dead_namespace；retire 存活域命中臂（keyspace.rs:357-367）candidate find 命中在册活格即 alloc_next_virtual_id + swap_out 换指 + 判死。「RESP 面不可达」证伪——FirstMap ⇔ swap_out 返回 None ⇔ resolve 后取锁前路由格被整体抹除，两条真实路径：(a) FLUSHALL 竞态——flush_all_databases（keyspace.rs:449-457）持锁 vdb.reset() 整表换新根表，并发 FLUSHDB 的 resolve 在 reset 前完成取锁在后，get_or_create_ns 重新盲分配新 vns 建空表 → FirstMap；(b) rollback_flush_ns 的 None 臂删除 ns_map[logic_ns]（flush.rs:116-118），同 ns 排队中的下一个 FLUSHNS 取锁后 swap_ns 返回 None → FirstMap。条目入队无条件（single_database_manager.rs:552/:567 哨兵值直接 safe_flush_aof）。
3 逻辑危害确证：形一（副本实时回放，永久主从分叉）——主库 FirstMap 镜像批 [DbMap(vns, db→N), 0x05] 先入 AOF、FlushDb(vns, N) 后入；副本 apply_dbmeta_record 换格到 N 后 FlushDb 臂 retire_dead_domain(vns, N) 命中在册活格 N → 副本本地再取号换格 N′ 并判死 N → 主库写落 N 镜像到副本也落 N 但副本路由指 N′ → 副本读恒 miss；N 到期被紧缩物理丢弃。形二（主库崩溃恢复重放，紧缩永久丢数据）——崩溃于 FirstMap 批后盘上仅 DbMap(vns, db→N)，恢复重放 FlushDb(vns, N) 时 is_dead_domain 假 → 插内存死亡账本（不落盘）→ 恢复后路由指 N 写读照常而紧缩到期把 N 域全部记录判死丢弃，且 reclaim_registry_domain 每轮恢复重放把该活租户向量登记表连带清掉。形三（FLUSHALL_NS 收令主节点 replication.rs:361-368 同走漏斗）同型。

涉及代码：
rust 文件与函数：
wedb/wkv/src/store/keyspace.rs:首映射哨兵（:174-178、:332-334）、retire_dead_domain 存活命中臂（:356-368）、判死登记（:369-379）、retire_dead_namespace 同构（:389-395）
wedb/wnode/src/database/single_database_manager.rs:条目无条件入队（:552/:567）
wedb/wnode/src/aof/aof_processor.rs:回放消费与红线自陈（:602-643、:609-611、:616-623）
wedb/wedb/src/server/replication/replication.rs:FLUSHALL_NS 收令漏斗（:361-368）

对应 c# 文件与函数：
N.A.（自研秒级换号架构；db.md 1.3/1.4 与主从镜像段为规范基准）

精炼执行方案：
1 flush_database/flush_namespace 返回值携 Option<旧域>（或 first_map 标记），FirstMap 形不产 Flush 广播条目（single_database_manager.rs:552/:567 按形跳过）——首映射无旧域即无退役屏障对象，清库语义已由 DbMeta 镜像批（map + 0x05）完整承接，不立第二机制
2 锁测：FLUSHALL 竞态夹具（resolve 与取锁间插 reset）断言 FirstMap 不产哨兵条目；副本回放 FirstMap 批 + FlushDb 断言不再本地换格；崩溃恢复重放同窗断言紧缩不判死活域
