reject: 工单描述的现状在 dev 上已不成立，其修订方向 1a（槽位级单元格真 O(1) 换号）与方向 2（GC 锁面短临界解耦）均已完整落地，无剩余工作。

工单称 wedb/wkv/src/vdb.rs:791-826 的 flush_db 每次换号在 CAS 重试循环内整表克隆 DbRoutingTable（HashMap 全量拷贝 + Arc::new），并称 wedb/wkv/src/session/swap.rs:43-55 同型。对照现文件：vdb.rs:791-826 行号处是 is_cold_db、mark_route_authoritative、route_vdb_of；flush_db 现为 vdb.rs:961-987，换号仅 alloc_next_virtual_id 加 DbRoutingTable::swap_out 单格原子换指，无 CAS 重试循环。DbRoutingTable（vdb.rs:36-39）本身即工单方向 1a 的结构形态：papaya 并发字典承载 logic_db 到 Arc<ArcSwap<u64>> 单元格，结构未派生 Clone，全仓 grep 无任何路由表克隆命中；读面一律经 cells 私有字段的单点访问器 get、contains、cell_or_insert、set、swap_out、snapshot（vdb.rs:50-94），无散点直取。SWAPDB 同型问题亦已消除：swap.rs:52-61 为换号串行锁内两格各自单指令 ArcSwap 换指，零整表克隆。千库租户换号的线性放大前提不复存在，成本恒定与在册库数无关。

工单称 GcDeadLog::insert（原 :342-347）与 pop_reclaimable（原 :381-382）共用 expiry_heap Mutex、后台 sweep 持锁长遍历阻塞换号路径。现行实现 pop_reclaimable（vdb.rs:554-593）正是工单方向 2 的批量短临界：锁内只沿小根堆摘到期前缀入局部 Vec（纯堆操作，不触账本），账本双检、物理摘除与暂扣回插全在锁外；insert（vdb.rs:510-516）持锁仅一次堆 push，换号路径不再在 sweep 窗口等锁。

声明与实现对表项亦已闭合：doc/zh/db.md 相关条目口径全部为「单次 O(1) 原子替换槽位单元格」「零整表克隆、零 CAS 重试」「内存换号段耗时小于 1 微秒（DbMeta 同步落盘批不在此承诺内）」，db.md 与代码内 grep 克隆仅剩零整表克隆的否定式，无写时克隆自相矛盾残留；keyspace.rs:71-76 的 <1μs 注释明示其后的 DbMeta 原子批同步落盘不在承诺内。工单第 3 点称两次同步落盘收敛另由 dbmeta-atomic-batch 承接、本单不动，而现实现 flush_database（keyspace.rs:110-131）已是一次 session.persist_dbmeta_batch 原子批（新映射、墓碑、水位一条批落盘），无两次串行 try_upsert_tag_sync，无第二机制待统一。

C# 参考对照成立但无差距：garnet/libs/server/StoreWrapper.cs:613 FlushDatabase 转调 databaseManager.FlushDatabase，物理形态为 libs/server/Databases/DatabaseManagerBase.cs:305 的 db.Store.Log.ShiftBeginAddress(TailAddress) 单指针推进 O(1) 截断（已开文件核实）；rust 换号机制为自研，其规范源 db.md 声明与现实现逐点对表一致。

结论：工单为对旧代码状态的过期描述（其引用的全部 rust 行号相对现 dev 均已错位），所指 O(N) 克隆、锁耦合与文档口径漂移三项均已按其自身方向 (a) 落地，验收面由现实现与 wkv/tests/store/flush_database.rs、swap_database.rs、concurrent_flush.rs 承接，拒绝，不产生代码改动。
