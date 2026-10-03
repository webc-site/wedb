归档注记：合入 ca94f060，链首域钉单点+落盘前换代复核(GenerationMoved 显式失败)，链内键显式前缀构造，零半代提交零第二套判定

甄别结论：通过（甄别席 J6，2026-09-27，定级 P1——升阶链多点独立重解析跨代撕裂，数据丢失+孤儿永驻）。亲验：多点独立重解析实锚 promote.rs:173/:199/:249/:285/:306（票面 :218/:239/:261 已漂，结构不变）、virtual_domain（session/mod.rs:475-492）逐点现解、RI.CREATE 臂 ops.rs:109-154 同族；flush 仅持 lock_dbmeta（keyspace.rs:137）与升阶链零互斥；register 死亡域守卫（bftree_release.rs:77-90）只判登记域、新代活域恒不触发，「同域死亡无害」前提 :73-75 失实；keyspace.rs:117-122 承诺锚与 :127-130「重建空树静默清零」明令在场；session_tag_key_with_prefix 原语现成（vdb_load.rs:26/consistent_read.rs:99 等消费）。C# RegisterIndex（:396/:457）身份无代维度成立。派沙箱席 c01l。

审核结论：通过（P1 真案。①逐点现解析属实：promote.rs 五取点独立重解析（:173/:199/:218/:239 virtual_domain→session/mod.rs:475-492；save 经 stub.rs:137），微漂：env_k 实构于 :223；②可达性坐实：flush_database 仅持 lock_dbmeta（keyspace.rs:137），promote 全程零触该锁，唯一长 await 为 spawn_blocking 建树（秒级窗），控制面串行锁只覆 flush×flush，跨连接可入；③四态主证属实：register 死亡域守卫（bftree_release.rs:77-84）只判登记域死活，链中落新活域恒不触发，其 :73-76「同域死亡无害」前提在逐点重解析下确为假；重启 lazy_restore_tree（stub.rs:100-127）按存根新代域现算 id_key，文件在旧代→空树静默清零，reclaim 只遍历旁表→孤儿永驻；④C# 对位：DatabaseManagerBase.cs:301 整段截断自身实例、RegisterIndex:396 身份无代维度，属 rust 新路数自闭责任；⑤五池无重，§99/§118 域钉先例在位）

整理执行方案（审核席订正版，供 fix 消费）：
1 方案1 最小可落：入口钉三元组＋session_tag_key_with_prefix（keys.rs:26 现成）显式构造，禁新原语；用户会话勿套用 §99 set_virtual_context 钉形（会污染会话后续命令落域）；register/detach 走既有三参内核（bftree_release.rs:77）
2 方案2 复核臂为闭环要件非过度设计：纯钉域下 register 守卫静默销毁树且回 OK、副本流已 emit 即主从发散，故代数越钉即 Swapped 逆序回滚＋compensate_stream_drop，全现码复用
3 测试注入沿 migrate_cross_generation_ttl.rs:122-160 真原语先例

升阶/RI.CREATE 多 await 发布链逐点现解析会话物理域，链内换代把树身份域、旁表登记域、元记录落域、副本流域撕裂——路由存活而树失联、重启静默清零、孤儿树文件永驻

问题分析：
1 契约对齐（本仓自洽法源与 C# 形态）
doc/zh/db.md 1.4「换号后首次写入按代数-解析不可交换口径重解析最新映射，自愈收敛命令边界」的 tolerated 前提是该命令各写落同域即逐键最坏旧域泄漏、读写统计面不可见；keyspace.rs:117-120 对升阶树另立硬承诺「期限内 unlink 绝不发生……绝不出现路由存活而树已销毁」。C# 每库独立 Tsavorite 实例，FlushDatabase（garnet/libs/server/Databases/DatabaseManagerBase.cs:301）整段截断自身实例，树随实例消亡（garnet/libs/server/Resp/RangeIndex/RangeIndexManager.cs:RegisterIndex :396/:457 身份键无虚号代维度），结构上不存在「链内换代撕裂」形，属 rust 双层虚拟化新路数自身必须闭合的缝，非既定改良。
2 工程现状确证
promote_collection_to_bftree（wedb/wkv/src/range_index/promote.rs:79-267）为多 await 长链：建树装载 range_index_blocking（:104-166，大集合秒级窗）→ emit RangeIndexStream（域取点一 :173）→ publish_tree_from_snapshot_locked 换入（树身份 pub_key 取点二 :199 session_meta_key）→ register_bftree_key 旁表登记（取点三 :218）→ save_bftree_meta_stub 元记录落盘（取点四 :239，内部再经 session_meta_key）→ delete_raw 信封（取点五 :261）。每一取点均经 session_prefix/virtual_domain（wedb/wkv/src/session/mod.rs:475-492）现解析——代数被并发 FLUSHDB/FLUSHNS/SWAPDB 的 bump_generation 推进后，下一取点即改指新代域。换号侧 flush_database（wedb/wkv/src/store/keyspace.rs:131-179）仅持 lock_dbmeta，升阶链全程不触该锁亦不预钉域，两链无互斥。RI.CREATE 臂同形（wedb/wkv/src/range_index/ops.rs:109-154：id_key 先解、create_bftree 阻塞 await、register :133 与 save :142 后解）。换代落链中任两取点之间即撕裂：树数据文件与 live_indexes 注册落旧代身份域、旁表登记落新代域（bftree_release.rs register 死亡域守卫 :77-84 不触发——新代是活域；其自陈「创建方后续元记录写路径同域死亡，语义无害」:73-76 在链中重解析口径下前提为假）、元记录存根落新代域。重启回建按存根所在新代域现算树身份（tree_identity_key），文件不在该路径——惰性激活重建空树，集合内容静默清零（即 keyspace.rs:127-130 明令严禁的「重建空树静默清零」形）；旧代身份树文件无墓碑、无旁表登记（take_bftree_domains_of_vns 按登记域取数不覆它），启动对账 reclaim_dead_domain_bftrees（bftree_release.rs:218-240）亦只遍历旁表条目，该孤儿文件与 live_indexes 注册项永驻=泄露案源。副本侧另裂：RangeIndexStream 载荷域=emit 时刻解析（:173-187），回放经 set_virtual_context 虚直设无重解析，落旧代域随死域紧缩被清，主端键存活（残根）——主从终态发散。
3 逻辑危害确证
并发 FLUSHDB/FLUSHNS/SWAPDB × 升阶/RI.CREATE 交叠窗内：数据丢失（重启后集合静默清零）、孤儿树文件与注册项永驻（磁盘/内存双泄露，违 keyspace.rs:120「绝不路由存活而树已销毁」承诺）、主从发散、且全程零告警面。窗宽为建树装载实际耗时（65536+ 条目可达秒级），非理论窄窗。
查重：wkv-cold-bftree-observed-orphan-domain-leak（观察账销账）、wkv-bftree-release-queue-defers-engine-dispose-to-delay-window（dispose 时序）、wnode-flushall-destroys-acl-user-records-auth-lockout（ns0 截断 ACL）三票各轴正交；deviations §96/§99/§118 登记的是复制快照/迁移驱动链域钉已修形，RI 升阶/创建链零登记；§75 向量登记表不涉。

涉及代码：
rust 文件与函数：
wedb/wkv/src/range_index/promote.rs:promote_collection_to_bftree（取点 :173/:199/:218/:239/:261）
wedb/wkv/src/range_index/ops.rs:RI.CREATE 臂（:109-154 同族取点）
wedb/wkv/src/session/mod.rs:virtual_domain（:475-492 链中即解析）、register_bftree_key（:633）
wedb/wkv/src/vdb/bftree_release.rs:register_bftree_key 死亡域守卫（:77-90 前提失实自陈 :73-76）、reclaim_dead_domain_bftrees（:218-240 不覆旁表外孤儿）
wedb/wkv/src/store/keyspace.rs:flush_database（:137 仅 lock_dbmeta、:117-120 承诺锚、:170-172 take 序）
对应 c# 文件与函数：
garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase（:301 每库独立实例截断，无换代形态）
garnet/libs/server/Resp/RangeIndex/RangeIndexManager.cs:RegisterIndex（:396/:457 身份键无虚号代维度）

精炼执行方案：
1 链首域钉单点：promote/RI.CREATE 入口单次解析 (vns, vdb, generation) 三元组，链内全部物理键构造经既有 session_tag_key_with_prefix 显式前缀原语（keys.rs:26 外提内核，不造第二套）与带域参数的 register/detach 内核派生，禁链中逐点 virtual_domain 重解析；emit RangeIndexStream 载荷域即钉定域，副本虚直设臂不动
2 落域前换代复核：元记录落盘取点若发现全局代数已越过钉定快照，按既有 Swapped 分级失败臂逆序回滚（delete_index + compensate_stream_drop + 快照弃件均为现码，不新增通道），命令显式失败禁半代提交；换号方无需改动
3 测试验证点：以 migrate_cross_generation_ttl.rs:122-160 的 flush_db 真原语注入体在建树 await 中插 FLUSHDB，断言三域恒等钉定域、重启回建树文件在位非空、旁表/待释放队列零孤儿、副本终态与主等；RI.CREATE 臂同型一例；纯交改回归（无换代时零额外分配、与现路径同帧）
