终态：已合入 dev（merge fx0N 系，2026-09-27）。01b9e66 DomainAnchor/TtlLeg 域锚扩 TAIL_LEN 9→25 单源,六命令臂跨换号域比对;7 用例含 flushdb_in_degrade_window 风暴 e2e

甄别结论：通过 | 定级 P1 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：域锚扩 TAIL_LEN 单源+重放臂域比对收口，复用域钉族推广

审核结论：通过（订正已并入）（逐锚亲读：TtlResume 尾 9 字节/EtagResume 17 字节确无域段、apply 三降级源真实、slow.rs KeepTtl 携刻度直取对照 Full 臂现读 ttl_of 不对称坐实；窗可达非夸大——virtual_domain 逐 op 代数现解析、flush 仅持 lock_dbmeta、KeepTtl 置位于条件裁决后值未提交前，await 窗可入换号；C# BasicCommands.cs:772-847 单调用+FlushDatabase 整实例截断对位属实；五池唯此票，promote-ri 轴正交。订正：域钉族引注 §100 误书改 §118 两处；XX/GET 条件形需后写键限定。方案跨域退既有现读内核、禁 set_virtual_context 套用，无第二判据）

SET 族降级重放尾参只携过期刻度不携物理域锚，跨换号重放把 KEEPTTL 回填刻度盖进新域键成幽灵 TTL 致静默提前过期

问题分析：
1 契约对齐（C# 原型行为与本仓自研面法源）
C# 一手形态：SET key v KEEPTTL 的「读旧 TTL → 清 → 回填」与值写一体收敛在单记录 RMW 记录闩锁内（garnet/libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional :772-847 经 MainStore/RMWMethods.cs InPlaceUpdater/CopyUpdater 锁内完成），FLUSHDB 系整库独立实例截断（garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase :301），结构上不存在「TTL 判据取自一域、落笔落另一域」的交叠形——C# 线性化序里 KEEPTTL 要么整命令先于清库（键随库灭），要么后于清库（键缺席 → 无 TTL 可保 → 写入无 TTL）。本仓双层虚拟化（doc/zh/db.md 1.2/1.4）下「换号后首次写入按代数-解析口径重解析最新映射，自愈收敛命令边界」的 tolerated 前提是该命令各写落同域、最坏旧域泄漏读写统计面不可见；新域残留一个来自死域的过期刻度不属于任何已登记改良，与 task/todo/wkv-promote-ri-chain-mid-command-generation-tear.md 同谱（换代交叠链中判据与落域撕裂），亦即 set.rs 对象键臂自陈「杜绝 object 时代幽灵 TTL」（:667-676）同一承重判据在换号轴上失守。属 rust 自研新路数自身必须闭合的缝，非既定改良。
2 工程现状确证
快臂 network_set_conditional（wedb/wnode/src/resp/basic_commands/set.rs:network_set_conditional）在持窗内以当前域（旧域）ttl_of_sync 读得旧 TTL 刻度后置入续跑标记 TtlResume::KeepTtl(ticks)（:725-727 非 GET 臂、:812-816 GET 臂）；随后 apply_set_with_expiry 任一处降级（RI Deferred :161 / upsert 环形页翻转 Ok(Err) :166 / put_ttl 前置页翻转）即整命令转慢臂。快照尾参追加与逆解析只携「1 模式字节 + 8 字节 LE 刻度」（wedb/wnode/src/resp/resp_server_session/core.rs:TtlResume :166-213，TAIL_LEN :183；追加侧 wedb/wnode/src/resp/garnet_api/mod.rs:830-843 Set/Setexnx/Restore，同通道 EtagResume 17 字节形 :844-858），零物理域锚。慢臂 exec_slow 与快臂降级之间隔 await 调度窗，他会话并发 FLUSHDB/FLUSHNS/SWAPDB 的 bump_generation 一经落入（wedb/wkv/src/session/mod.rs:virtual_domain :475-492 逐 op 现解析，仅持 lock_dbmeta，与本链无互斥），慢臂 replay 的 session_prefix 即改指新代域：slow_set_conditional（wedb/wnode/src/resp/basic_commands/slow.rs:413-421 非 GET 臂、:453-461 GET 臂）命中 KeepTtl(ticks) 分支直取快照刻度跳过新域 ttl_of 重读，apply_set_with_expiry_async 将「值 + 旧域 TTL」一体落进换号后新域。对照同函数 Full 重放臂（resume 非 KeepTtl 时现读 storage.batch.ttl_of(key) :417）正是 C# 线性化终态——证唯续跑尾参一爿缺域锚。同谱第二消费面：Pending/ReplyEcho 臂（slow.rs:521-534、key_admin_commands/slow.rs:518-528）值已提交旧域、补投 put_ttl 落新域成孤 TTL 旁路（存在性探针刻意不吃 TTL，ttl_sync.rs:probe_alive_domain_with_prefix :661-681，故暂不可见、仅记账残秽，危害次于 KeepTtl 臂）；EtagResume::Pending（wedb/wnode/src/resp/basic_etag_commands.rs:983-997 剥参、:921-928/:1057-1061 补投）同尾参同形态。既有锁测全部同域交叠（nx_conditional_ttl_degrade_replay.rs :374-398/:555-620 线形与重放锁）零跨换号注入。
3 逻辑危害确证
受害链：SET k v KEEPTTL（主形=无条件 KEEPTTL；KEEPTTL+XX/GET 条件形须新域同键在位方显害——slow.rs:400 重裁决后携刻度，订正入案）快臂读旧 TTL 后降级 → 他会话 FLUSHDB 换号 → 慢臂全量重放落新域 → 新键被盖上一份属于死域旧键的过期刻度 = 幽灵 TTL：键在用户视角刚被清库抹零后重写、理应无 TTL，却携带旧域刻度于未来某刻静默消失（数据丢失向），且写面读面零告警。窗宽为降级快照到慢臂复窗的调度间隔，多连接并发清库下可复现（真原语注入先例现成）。次形：Pending 臂跨域补投在新域留孤 TTL 旁路、旧域已提交值永失 TTL（旧域随 GC 消亡，暂不可观测）。

查重：五池无「SET/RESTORE/ETag 续跑尾参 × 换号域锚」案（todo 仅 promote 链 generation-tear 一票，轴为正交链位；§96/§99/§118 登记面系复制快照/迁移驱动探针窗域钉已修形（域钉族实为 §96/§99/§118，§100 系双键写回序条，勿混引），命令降级续跑尾参零登记；w2-gate 无主红系 pairlatch 挂死与选项文法无涉）。deviations.md §29/§99/§100/§118 与「KEEPTTL 换号交叠」无既裁。TTL 扫描/sweep 域旧票（wkv-ttl-sweep-*）不涉选项文法与本尾参面。选项文法族余点本轮逐臂对账皆净（详见席报），本票独案。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/basic_commands/set.rs:network_set_conditional（KeepTtl 置点位 :725-727、GET 臂 :812-816）、apply_set_with_expiry（降级源 :156-194）
wedb/wnode/src/resp/resp_server_session/core.rs:TtlResume / EtagResume（定义与 tail_bytes/from_tail :166-260，TAIL_LEN 线长单源 :183）
wedb/wnode/src/resp/garnet_api/mod.rs:快照尾参追加臂（Set/Setexnx/Restore :830-843、ETag 写族 :844-858；VADD 定槽尾参同款先例 :859-868）
wedb/wnode/src/resp/basic_commands/slow.rs:string_slow Set/Setexnx 臂（split_last 剥参 :513-534、slow_set_conditional KeepTtl 消费 :413-421/:453-461）
wedb/wnode/src/resp/key_admin_commands/slow.rs:Restore 臂（:499-528 同形消费）
wedb/wnode/src/resp/basic_etag_commands.rs:etag_conditional_slow / Setwithetag 慢臂（:903-928、:1043-1061）
wedb/wkv/src/session/mod.rs:virtual_domain / session_prefix（:475-492 逐 op 现解析，换代即改指）
wedb/wnode/src/storage/session/common/ttl_sync.rs:probe_alive_domain_with_prefix（:661-681 TTL 不计存在性，孤 TTL 暂不可见之依据）
对应 c# 文件与函数：
garnet/libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional（:772-847 KEEPTTL 读旧回填与值写同锁内一体，无跨窗尾参形）
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdater/CopyUpdater SETKEEPTTL 与 EvaluateExpire* 臂（单记录闩内判据与落域同体）
garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase（:301 整库实例截断，与在飞 RMW 结构上不可交叠）

精炼执行方案：
1 尾参保真单源扩域锚：TtlResume / EtagResume 置点位（set.rs KeepTtl/Pending 两处、apply_set_with_expiry TTL 降级臂、basic_etag_commands.rs apply_etag_write 成功帧前）随刻度同点捕获当前物理域（与 session_prefix/virtual_domain 同源判据，取 (vns, vdb) 对或换号代数一枚，禁另立第二套域身份），tail_bytes/from_tail 线形按 TAIL_LEN 单源扩宽，零向下兼容（在途票 nx_conditional_ttl_degrade_replay.rs:374-398 线形锁测随改）。
2 重放臂域比对收口：string_slow Set/Setexnx、Restore 臂与 etag_slow 三命令剥参后，携域锚与慢臂当前解析域现比——同域：现语义逐字节不变（刻度回填/补投照旧，杜绝回归）；跨域：KeepTtl 弃携带刻度改现读当前域 ttl_of 回填（即 Full 臂既有 :417 现读式，不新增第二读内核，回到 C# 线性化终态新键无 TTL），Pending/ReplyEcho 与 EtagResume::Pending 跳余腿补投只按原契约出/续应答帧（值已随死域退役，禁把 TTL/etag 落进不相干新域），全程零新机制、比对即 §99/§118 域钉族向续跑尾参的推广。
3 测试验证点：注入体采 migrate_cross_generation_ttl.rs:122-160 flush_db 真原语先例，于「快臂 KeepTtl 置位降级后、慢臂复窗前」插并发 FLUSHDB，断言新域键存活且无 TTL（现读臂同域有 TTL 形对照）、应答帧形与无换号基线逐字节等；Pending 形同注入断言新域零孤 TTL 旁路（ttl_of 现读 NotFound）且 +OK/回显帧不变；无换代纯交回归（nx_conditional_ttl_degrade_replay.rs / ttl_composite_write_window.rs / etag_conditional_degrade_replay.rs 全绿，同域刻度回填路径零额外分配）。
