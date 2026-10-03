终态注记：合入 de11703，出账点与零到期水位前移臂统一 live.len() 实存直赋（与 promote bulk_load 去重计数同源同值），换入滞后态（Swapped/崩溃恢复）下个计数窗收敛实存并落盘、下一轮到期旧差不残留，promote.rs 两处「兜底 size 滞后」自陈注释同步订正；测试 swapin_stale_meta_size_converges_by_live_len（覆写 stale meta 构造滞后磁盘态，复现力已验证：还原差量形应答虚高 :2）+ tiered_cmds_align 双态 parity，进程级注入串行门随入。

甄别结论：通过（2026-09-29 主控甄别，定级 P3——换入窗 promote 换新树（到期成员物理消失）而磁盘旧 meta next_expiry 仍越过，重扫 expired==0 触发零到期水位前移臂 common.rs:605-615 把虚高 size 原样落盘固化，「重数兜底」前提结构性失效。C# HashObject.cs:444-470 摘除同点同账无此窗。修复：live.len() 实存直赋同步覆盖零到期水位前移臂，同批在手零重算）

审核结论：通过（2026-09-29 甲轮34-B，P3 级）。出账差量仅内存态（:539 dec_size）+promote 写序「换入→落元记录」+落盘失败滞后态自陈+兜底前提在换入窗失效+水位前移臂固化（:598-615）+双路 tiered_count 皆取陈旧值全链坐实；「永久虚高」对无近 TTL 静默键成立；C# 同点同账无对应窗。执行席修正（采纳）：live.len() 实存直赋宜同步覆盖零到期水位前移臂（common.rs:605-615，同批在手 live 零重算成本），已固化键下个水位越过即自愈；推迟臂内存 size 语义同形对票 Hset 应答面零回退。

原票面：
分层到期出账重灌「换入已生效、元记录未落盘」窗内 meta.size 永久虚高——sweep 差量扣减兜底对已物理摘除成员结构性失效

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 集合对象常驻内存，到期出账与记账同点同账：DeleteExpiredItemsWorker（HashObject.cs:443-470、SortedSetObject.cs:684-712）堆序摘除时逐枚 UpdateExpirationSize(add:false) + UpdateSize(add:false)，HeapMemorySize 与实际存活成员恒一致，不存在「容器已变、账未扣」的持久窗；Count()（HashObject.cs:510、SortedSetObject.cs:606）直读容器计数减到期数，永无永久漂移态。rust 分层态把该不变量拆为「内存 dec_size → promote 原子换入 → 元记录落盘」三段链，其自愈闭环依赖「元记录未落盘 ⇒ 磁盘 size 与 next_expiry 停留旧值 ⇒ 下个计数命令重装载后水位仍越过 ⇒ 重扫重数出同一批到期成员再扣一次」。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
出账成功臂 expire_sweep_or_rebuild（common.rs:518-616）：ctx.meta.dec_size(expired) 仅内存态（common.rs:539），随后 promote_collection_to_bftree(replace=true) 先 rename 原子换入新树——到期成员自此物理消失——再 save_bftree_meta_stub_with_prefix 落盘 size=bulk_load 去重计数（promote.rs:250/:283-284）。换入生效而元记录落盘失败（promote.rs:283-296 重灌臂自陈「保持既有行为仅补偿摘除副本幻影」）或落盘前崩溃（promote.rs:68-69 自陈「新树 + 旧 meta 滞后态……计数校正兜底 size 滞后」）：磁盘元记录停留旧值（size=N 含 E 个已出账到期成员、next_expiry 仍越过），数据文件已是新树（E 成员已不在）。此后任一计数命令（tiered_count，common.rs:633-658）重装载旧元记录、水位越过、重走 sweep_expired_members（common.rs:443-471）：树内已无可数到期成员，expired==0 落「零到期水位前移臂」（common.rs:605-615）把含虚高 size 的元记录连同前移水位一并落盘固化——dec_size(E) 永无补记点，HLEN/ZCARD 恒答 N 而实存 N-E。promote.rs:69/:279 所宣称的「计数校正兜底 size 滞后」前提是「到期成员仍在树、重扫可再数出」；换入已生效窗内该前提不成立，兜底机制对最需要它的那个窗结构性失效。CacheBudgetExhausted 推迟臂（common.rs:568-581）树保持原样、成员仍在树，不属本面（该窗另一面已另案在册：task/issue/wnode-tiered-hash-deferred-sweep-hset-newfield-undercount.md）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
记账虚报面：HLEN/ZCARD 自首个出账命令起永久虚高 E（E=出账批到期成员数），且虚高值经「零到期水位前移臂」落盘固化，无任何自愈点（后续新一轮到期只叠加自身扣减，旧差永不清除）；删空自愈判据 meta.size==0 同被虚高推迟触发。无 panic、无数据丢失、无锁面危害，危害限于持久化计数标量失真，定 P3。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:expire_sweep_or_rebuild（:518 出账臂 :539 dec_size 差量扣减、:568-581 推迟臂、:605-615 零到期水位前移臂原样回写 stale size）
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:sweep_expired_members（:443 成员可数判据——树内已无到期记录即 expired==0 零扣减）
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:tiered_count（:633 水位越过出账出口应答 ctx.meta.size）
wedb/wkv/src/range_index/promote.rs:promote_collection_to_bftree（:93；:250 new_with_expiry 落盘 count；:283-296 换入后落盘失败 replace 臂；:68-69/:278-279 「计数校正兜底 size 滞后」自陈）

对应 c# 文件与函数：
garnet/libs/server/Objects/Hash/HashObject.cs:DeleteExpiredItemsWorker（:443-470 摘除与 UpdateExpirationSize/UpdateSize 同点同账）
garnet/libs/server/Objects/Hash/HashObject.cs:UpdateSize（:307）、UpdateExpirationSize（:336）、Count（:510）
garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:DeleteExpiredItemsWorker（:684-712 同构）、UpdateSize（:812）、UpdateExpirationSize（:648）

精炼执行方案：
1. 出账记账由「差量扣减」改「实存直赋」：expire_sweep_or_rebuild 出账点在树写锁与自迁移 claim 窗内已持有存活全集 live（drain_live 同一批产物），以 live.len() 直接赋值 ctx.meta.size 取代 dec_size(expired)，与 promote 落盘的 bulk_load 去重计数同源同值；重灌成功、CacheBudgetExhausted 推迟、换入后落盘失败（Swapped）、崩溃恢复后首个出账窗四态统一收敛到实存，历史漂移一并自愈，零额外扫树。
2. 测试验证点：构造分层 hash 双成员一存活一带到期 TTL → 触发 HLEN 出账至 promote 换入点注入元记录落盘失败（Swapped 臂）→ 现状断言 HLEN 虚高 1（复现）→ 修复后断言 HLEN 收敛存活数；下一轮到期再出账断言旧差不残留；两态 parity 入 wnode/tests 既有 tiered_cmds_align 面。
