甄别结论：通过（2026-09-29 主控甄别，定级 P2——六消费点 del_ttl 全部先于记录写（write/mod.rs:205/:426/:548-552、keys.rs:603-605、slow.rs:262-270），崩溃窗留无主 TTL 记录而值永生；C# UpsertMethods.cs:20-27 值与 Expiration 单记录一体原子。修复：TTL 腿后置使崩溃残留收敛为无主 TTL 记录良性自愈态，ttl_restore 补偿链随序内联消解禁留双机制；SET 带 EX 新键窗另案勿扩面）

审核结论：通过（2026-09-29 甲轮34-A，P2 级）。四处消费点写序全核实（SET 同步臂 :192-214/异步臂 :426→:438/strip 内核 :535-553/RENAME 快慢径）；whlog mod.rs:147 前缀截断规则+组提交水位落两 append 之间推演成立；load_meta 幽灵清理以 meta 死为前提不可达坐实无自愈触点；C# UpsertMethods.cs:21-23 记录一体结构免疫。方案：TTL 腿后置使崩溃残留收敛为「无主 TTL 记录」良性自愈态，ttl_restore 补偿链随序内联消解，RENAME 前置 del_ttl 摘除，单机制收口。执行席注记（非阻断）：顺带核对 SET 带 EX 新写腿落笔位点——「值已落、TTL 未落」新键 ADD 形既存窗修前修后同窗，注记或另案勿扩面。

原票面：
wkv 写内核「TTL 旁路墓碑先于主记录落笔」写序的中间崩溃态：后缀截断丢主记录留旧值永生残留，全链无恢复路径收敛

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# 键级 Expiration/ETag 是记录尾随可选字段，与值同记录一体、单次追加生效（garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:21-23 InitialWriter 先 TrySetValueSpanAndPrepareOptionals 再 TrySetExpiration，索引 CAS 即原子切换；删除臂 DeleteMethods.cs 记录连同尾随字段一体消亡）。崩溃恢复粒度 = 单记录：任意崩溃前缀下键要么完整旧态（含 TTL）要么新态，结构上不存在「旧值存活而 TTL 已清」中间态。仓内写内核头注自锚同一不变式（「任意一步失败键保持完整旧态（含 TTL）」，zcode-r34-writekernel 条目一收口），但该收口只覆盖进程内错误面（预读 + ttl_restore 回填补偿），崩溃面不变式被「旁路墓碑与主记录两条物理记录分离 append」击穿。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）：TTL 旁路记录（KeyTag::Ttl）系独立物理记录（既定改良），而四处消费点写序恒为「TTL 墓碑先 append、主记录后 append」：a) SET 同步臂 try_upsert_tag_sync_unprotected_with_prefix 先 try_delete_raw_sync_unprotected(ttl_k) 后 try_upsert_raw_sync_unprotected(rec_k)（wedb/wkv/src/session/raw/write/mod.rs:189-214）；b) SET 异步臂 upsert_tag 先 del_ttl 后 upsert_raw（同文件 :422-438）；c) DEL/GETDEL 先剥内核 try_strip_key_sidecars_sync_unprotected_with_prefix 按 [Ttl, Etag] 序先剥旁路再删主记录（同文件 :535-553，消费点 try_delete_sync_unprotected_with_prefix / try_take_sync_with_unprotected）；d) RENAME 尾部快慢两径均 del_ttl(old_key) 先于删旧键（wedb/wnode/src/resp/key_admin_commands/keys.rs:604-607、key_admin_commands/slow.rs:258-270，慢径注释自陈动机「避免孤儿 TTL 记录令后续读取长期走异步裁决慢路径」系进程内性能理由，不涉崩溃面）。hlog 恢复为前缀截断语义（尾帧不全即截断，whlog recover_committed 前缀恢复面同源），组提交页界/水位恰可落在两 append 之间：TTL 墓碑持久、主记录追加随尾窗丢失，恢复终态 = 旧值存活（哈希索引仍指旧记录）+ TTL 记录墓碑化，ttl_of 回 None。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）：恢复后旧值永生——is_expired(None) 恒假，TTL 扫描只收 TTL 记录（已墓碑不入选），紧缩按存活回拷，应过期的键永不到期（慢泄漏 + 语义腐坏）；该终态不对应任何串行序（SET/DEL 未生效或生效均非此态），非可串行化且客户端零感知（命令未 ACK）；全链无收敛点——remount 只重挂 ACL/向量元记录、惰性 probe 只清死元记录（load_meta 幽灵清理以 meta 死为前提，此处值记录存活）、migration-tmp 清扫只扫快照残件——「存活值 + TTL 墓碑」组合无任何机制触及，仅用户显式再写可清除。RENAME 面（站点 d）同窗把未 ACK 的 RENAME 固化为「旧键永生 + 新键已换入」双活加重形。

涉及代码：
rust 文件与函数：
wedb/wkv/src/session/raw/write/mod.rs:StoreSession::try_upsert_tag_sync_unprotected_with_prefix（TTL 腿先剥 :189-205、数据落笔 :214、ttl_restore 补偿 :215-223）
wedb/wkv/src/session/raw/write/mod.rs:StoreSession::upsert_tag（del_ttl :426 先于 upsert_raw :438、补偿 :444-446）
wedb/wkv/src/session/raw/write/mod.rs:StoreSession::try_strip_key_sidecars_sync_unprotected_with_prefix（:535-553 [Ttl, Etag] 先剥序）
wedb/wnode/src/resp/key_admin_commands/keys.rs:rename 快径尾部（:604-607 del_ttl_sync(old) 先于 :607 try_delete_sync(old)）
wedb/wnode/src/resp/key_admin_commands/slow.rs:finish_rename_move（:258-267 del_ttl 先于 :268 delete_string(old)）

对应 c# 文件与函数：
garnet/libs/server/Storage/Functions/MainStore/UpsertMethods.cs:InitialWriter（:21-23 值与 Expiration 单记录一体写入，崩溃原子）
garnet/libs/server/Storage/Functions/MainStore/DeleteMethods.cs:InitialDeleter（记录连同尾随 Expiration/ETag 一体消亡）
（C# 无旁路记录分解、无直位对；参照维度：板块 2.2 持久可靠与崩溃一致「恢复与重放幂等性」与写内核自锚不变式「任意一步失败键保持完整旧态（含 TTL）」的崩溃面）

精炼执行方案：
1. 根因单点收口在 wkv 写内核序：SET 两臂与 strip 内核把 TTL 腿移到主记录落笔/墓碑之后——TTL 孤儿自愈机制现成（SET 写内核清 TTL、load_meta 幽灵清理 wedb/wkv/src/session/collection.rs:299-306、读面对缺席记录过期裁决 no-op），崩溃残留收敛为「无主 TTL 记录」良性自愈态；ETag 腿维持先剥序不变（孤儿 ETag 危害面真实，其崩溃残留「记录在而 etag 失」良性有界）
2. ttl_restore 预读补偿随序内联消解（TTL 腿后置后数据落笔失败不再预删 TTL，补偿链自然消失，禁留双机制）；wnode RENAME 尾部前置 del_ttl(old_key) 移除或移到 try_delete_sync 之后（其内部 strip 已按新序承接）
3. 测试验证点：定向构造「TTL 墓碑已 append、主记录 append 前进程退出」的恢复用例（hlog 尾窗截断注入），断言恢复后旧值仍带原 TTL 且到期正常出账；回归既有 ttl_restore、GETDEL、RENAME TTL 迁移锁测全绿

查重：task 五池无同票（issue/wkv-ri-rename-dst-etag-bypass-residue-cas-baseline-fork 系稳态 ETag 残留面正交；zcode-r34-writekernel 票不在五池，其收口面为进程内错误序）；deviations.md §143 系 TTL gate 单源与粗化收口面，不覆盖本崩溃窗。

终态注记（2026-09-29 执行收口）：
合入 d602f60（Merge branch 'fix-ttl-sidecar-order' into dev，分支提交 f936e8f，9 文件 +500/-153）。
收口形态：六消费点 + 执行期核出的第 7 点（wnode EXPIRE 过去戳快径自带前置 del_ttl_sync，keys.rs expire_apply_sync，同窗同形随单机制一并摘除）全数改为「主记录落笔/墓碑先行、TTL 腿随后」——
1. wkv SET 同步臂（try_upsert_tag_sync_unprotected_with_prefix）：TTL 腿移至数据落笔成功后，失败沿降级臂交异步幂等重做；ttl_restore 预读回填补偿链随序内联消解（数据落笔失败时 TTL 零触碰，禁留双机制）
2. wkv SET 异步臂（upsert_tag）：TTL 腿（del_ttl）移至 upsert_raw 成功后，硬失败上抛由调用方重试闭环
3. wkv 删/取删同步内核（strip 内核拆分）：ETag 腿维持先剥（孤儿 ETag 危害面真实）；新单点 try_strip_ttl_sync_unprotected_with_prefix 承载后置 TTL 腿，DEL/GETDEL 记录已摘除后失败弃置不降级（异步闭包对已墓碑键回 false/nil，DEL 计数与 GETDEL 应答值不失真），残留为无主 TTL 记录良性自愈态
4. wkv 异步 delete/take_string（claim_gate_and_strip_sidecars → claim_gate_and_strip_etag）：普通键 TTL 腿后置于双域墓碑后（同步内核降级闭包同序收口）；复合/磁盘候选分支保留 load_meta 前 TTL 先剥——load_meta TTL 守卫的「TTL 已删」无递归与推进前提系结构依赖（守卫前置例外，非双机制）
5. wnode RENAME 快径（keys.rs）/慢径（slow.rs finish_rename_move）/EXPIRE 过去戳快径前置 del_ttl 全数移除，wkv 删内核级联按新序承接
自愈四通道现成核实：紧缩孤儿 TTL 判死丢弃（compact.rs host_exists_cooperative 三宿主探查）+ GC 过期扫描（gc/ttl_sweep collect_expired）+ SET 覆写清退 + 读面宿主缺席裁决 no-op。崩溃残留两形：SET 侧「新值 + 残留旧 TTL」（有界、到期正常出账）、DEL/GETDEL 侧「无主 TTL 记录」（良性自愈），全前缀绝无「TTL 已墓碑而值存活」永生形。SET 带 EX 新键「值已落 TTL 未落」窗票面明定另案未扩面（修前修后同窗）。
头注自锚订正：write/mod.rs 写序收口段由「ttl_restore 回填补偿还原旧态」如实改写为新序两形不变式；ttl.rs/rmw.rs/collection.rs 随序旧序描述（「先删 TTL 再删数据」类）同步订正。
测试：新增 wkv/tests/ttl_sidecar_order.rs（镜像序见证 SET 两臂/DEL/GETDEL 数据记录先于 TTL 墓碑、GETDEL 值契约 TTL 腿 Err 弃置不丢值、崩溃前缀恢复——直调数据腿原语构造「TTL 腿未执行」持久化等价形经 checkpoint+recover 断言全前缀无一形值永生）；write_kernel_failpath 条目一随新序改写（TTL 零触碰非回填、AOF 零删/零补偿）；ttl_fastpath_semantics 造窗注释随序订正（窗源改指并发 PERSIST 交错）。worktree 全 workspace cargo check --all-targets 零告警零错误；test.sh/clippy 留主代理集成门禁。
遗留（非阻断）：复合树态键异步删除分支的「load_meta 前 TTL 先剥」窗仍在（TTL 墓碑持久而元记录墓碑随尾窗丢失 = 复合对象永生窗）——收口需 load_meta TTL 守卫递归链重构，超出本票最小步骤，建议另案立项。
