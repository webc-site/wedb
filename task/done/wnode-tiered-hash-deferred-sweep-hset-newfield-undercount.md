终态注记：合入 9d3f6e1，sweep_expired_members 单趟扫描交出到期键集、SweepOutcome::Swept 携回（仅 CacheBudgetExhausted 推迟臂非空），Hset 批量臂推迟窗内以本批去重字段与推迟键集交集按「到期视同缺席」叠加计入 new_fields（应答与 meta.size 增量同源同值，瞬态虚高由下个命令出账 live.len() 直赋收敛），重灌成功臂零改动；tiered_cmds_align 新增推迟窗三向全等面（HSET :1/:2 逐字节一致、HMSET +OK、HLEN 收敛、值覆盖探针），摘除补偿即红已验证。

甄别结论：通过（2026-09-29 主控甄别，定级 P3——推迟臂 Swept 分支 hash.rs:191 重装载丢弃内存 dec_size，tree_put_batch 到期在树字段按键已存在覆盖、new_fields 不计，HSET 覆写到期字段应答 0，三向基准（重灌/内存态/C# :206-212）全 1，违反 hash.rs:179-186 臂注两态一致不变量。修复：sweep 扫描产物携回到期键集交集判定，零额外扫树；HMSET 臂 +OK 无计数面断言按 HSET 单面落）

审核结论：通过（2026-09-29 甲轮34-B，P3 级）。推迟臂/Swept 分支/臂注不变量/tree_put_batch Found 覆盖不计/应答 :0 全链复核坐实；三向基准（重灌成功/内存态/C# :187→:206）全 :1 分叉成立；预算耗尽自愈路径可达。执行席微修正：HMSET 臂应答 +OK 无计数面，「sum(new_fields)」仅 HSET 成立，断言按 HSET 单面落。

原票面：
分层 HSET 批量臂在到期重灌被推迟窗内把「到期在树」字段误判为既有，新增计数应答与两态契约瞬时分叉

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# HashSet（libs/server/Objects/Hash/HashObjectImpl.cs:185-241）入口先 DeleteExpiredItems（HashObjectImpl.cs:187），随后逐对 !exists || IsExpired 臂视到期字段为缺席：覆写到期字段走插入臂 set++（HashObjectImpl.cs:206-212），HSET 应答恒把到期字段计为新增 1。内存态信封臂 wcol hash_set（delete_expired_items + purge_member_if_expired 前置）与该净语义逐字节对齐，两态全等是 collection.md 第 5 节 RESP 应答透明契约的组成部分；分层臂自身文注也钉死同一不变量：「批量 upsert 的树内 Found 两态前查才与『真缺席』同计（覆写到期字段回复 1，两态一致）」（tiered_collection_ops/hash.rs Hset 臂注）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
Hset/Hmset 批量臂以 expire_sweep_or_rebuild 为出账前置，依赖「Swept 出口树内已无到期记录」使 tree_put_batch 返回的 new_fields（真实新增键数）等价于逐字段三态前查。但该前提在重灌推迟臂失效：expire_sweep_or_rebuild 的 Error::CacheBudgetExhausted 臂（tiered_collection_ops/common.rs:568-581）按「读命令不失败」设计推迟重灌——树保持原样（到期成员仍在树）、水位不落盘、ctx.dirty 复位假，仅内存态 size 保留 dec_size 扣减值后返回 SweepOutcome::Swept。Hset 臂的 Swept 分支（tiered_collection_ops/hash.rs:190-201）随即 load_collection_stub 自盘重装载 *ctx.meta（磁盘元记录未动，size 回到含到期成员的旧值，内存扣减被丢弃），再取写锁落 tree_put_batch：到期在树字段被 upsert 内核按「键已存在」覆盖，new_fields 不计该字段，meta.size 亦不增。该窗口内 HSET k f v（f 恰为到期在树字段）应答 ：0，而内存态信封臂与重灌成功路径均应答 ：1，违反臂注自钉的「覆写到期字段回复 1，两态一致」不变量。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
契约分叉面：页缓存预算耗尽推迟重灌窗内（该臂明确设计可达的自愈路径，非不可达异常态），同一数据集上分层态 HSET 新增计时应答与内存态及 C# 基线分叉；多字段批量 HSET 应答为 sum(new_fields)，分叉按到期在树字段数放大。数据与记账面最终收敛（size 从未扣减、字段覆盖后单份计数正确、下个水位越过命令重试重灌自愈），无持久化失真、无 panic、无锁面危害，WATCH 镜像照常入账；危害限于该窗内计数应答的瞬时假值，故定 P3。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/tiered_collection_ops/hash.rs:tiered_hash_arm（Hset/Hmset 批量臂 Swept 重装载分支与 tree_put_batch new_fields 计数）
wedb/wnode/src/resp/objects/tiered_collection_ops/common.rs:expire_sweep_or_rebuild（CacheBudgetExhausted 推迟臂返回 Swept 但树内到期记录未摘除）
wedb/wcol/src/hash/hash_object_impl.rs:hash_set（内存态两态全等基准臂）

对应 c# 文件与函数：
garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashSet（:187 DeleteExpiredItems 前置、:206-212 到期字段缺席臂 set++）

精炼执行方案：
1. sweep_expired_members 扫描期已逐记录判到期，令其在收集存活全集同时一并交出到期键集，经 expire_sweep_or_rebuild 以 SweepOutcome 携回（仅 CacheBudgetExhausted 推迟臂需要携带，重灌成功臂树内已无该批记录，携带位恒空，零额外扫树）
2. Hset 批量臂在推迟标志为真时，对 entries 中命中该到期键集的字段叠加计入 new_fields（复用同一份扫描产物做集合交集判定，不引入第二套三态前查机制），new_fields 回复与 meta.size 增量同源同值；重灌成功臂走现有路径零改动
3. 测试验证点：构造分层 hash 双字段（一存活一带到期 TTL）→ 压低页缓存预算触发推迟重灌 → HSET 覆写到期字段断言应答 ：1（与内存态、与预算充足路径三向全等）→ 恢复预算后续命令断言 size 收敛；两态 parity 断言入 wnode/tests 既有 tiered_cmds_align 面
