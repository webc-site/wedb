甄别结论：通过（2026-09-29 主控甄别，定级 P2——set/set_batch/del 三臂（ops.rs:220/:302/:443）均无链首域钉，对照 create :61-63 已钉；换代窗内 heal.rs/stub.rs/session 现解 virtual_domain 代数落后即撕裂：meta 落新代幽灵键、emit 副本 NotFound 静默跳过主从发散、del 臂 drain 全取点脱靶旧域漏清。done/wkv-promote-ri-chain 射程仅 promote 链不覆稳态写臂。修复：session_tag_key_with_prefix 域钉对齐 create 同款+落盘前换代复核复用 GenerationMoved/Swapped 分级）

审核结论：通过（2026-09-29 甲轮34-A，P2 级）。三写臂逐点现解、零互斥（flush 仅 lock_dbmeta 不触热路径）、先例射程仅 create/promote 链、主从发散与回放注释互证全复核成立。执行席遵照：表述订正——「树变已落钉定旧域树、显式失败安全的原因是旧域树随换号延迟销毁整体湮灭」（promote.rs:255-257 tolerated 同口径），测试期望按此对齐；refresh_tiered_meta/migration_claimed 内部 meta_k 须改消费钉定前缀与「禁逐点重解析」自洽。

原票面：
RI 稳态写臂 set/set_batch/del 多 await 链逐点重解析物理域且无换代复核，链中换代使刷新取旧域、落盘与 AOF 入账取新域，产生跨域幽灵元记录与主从发散

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# 每库独立 Tsavorite 实例，FlushDatabase（garnet/libs/server/Databases/DatabaseManagerBase.cs:301）整段截断自身实例，RI 写链无虚号换代维度，结构上不存在跨域伪影形；本仓双层虚拟化换号系既定改良，但换号撕裂闭合责任归 rust 自身（同族判例 task/done/wkv-promote-ri-chain-mid-command-generation-tear 已钉「非既定改良、新路数自身必须闭合的缝」）。
2. 工程现状确证：range_index_set（wedb/wkv/src/range_index/ops.rs:211-273）单命令四个独立域取点——装载 load_range_index_stub（stub.rs:235 session_meta_key）、锁内刷新 refresh_tiered_meta（heal.rs:33 session_meta_key）、元记录回写 save_bftree_meta_stub（stub.rs:148 session_prefix）、AOF 入账（ops.rs:260 virtual_domain）；range_index_set_batch（:293-367）与 range_index_del（:434-514，删空臂 drain 全链各取点同形）一致。换号侧 flush_database 仅持 lock_dbmeta（wedb/wkv/src/store/keyspace.rs:137），与稳态写臂零互斥，跨连接可入。代数在「刷新命中旧域」之后、「回写/入账」之前被 bump_generation 推进即撕裂：复合元记录落新代物理域——换号清库承诺（清库后该键不存在）破损，幽灵键 EXISTS=1 而树身份在旧代待延迟销毁、读臂惰性恢复按新域路径开文件恒 NotFound；StoreEvent::RangeIndexWrite/Drop 事件域落新域而副本无此键（回放 NotFound 静默跳过）→ 主从发散；删空臂 drain 全部取点新域脱靶 → 旧域元记录/树/旁表全漏清。已收口票 wkv-promote-ri-chain-mid-command-generation-tear 射程仅覆盖 promote 与 RI.CREATE 发布长链（两处已落域钉 + 落盘前换代复核，ops.rs:161-165、promote.rs:260-267），稳态写臂面未覆盖。
3. 逻辑危害确证：FLUSHDB/FLUSHNS/SWAPDB 后幻影键半死残留（违背清库隔离承诺）、EXISTS 与读应答自相矛盾、主从终态发散、del 臂旧域树与旁表泄漏；窗宽为锁内刷新至落盘的数个 await（毫秒级），控制面换号低频交叠下真实可达。

涉及代码：
rust 文件与函数：
wedb/wkv/src/range_index/ops.rs:range_index_set / range_index_set_batch / range_index_del（四域取点 ：217/:227/:239/:260、:298/:309/:350、:439/:448/:501）
wedb/wkv/src/range_index/heal.rs:refresh_tiered_meta（:33 session_meta_key 逐点现解）
wedb/wkv/src/range_index/stub.rs:save_bftree_meta_stub（:148 session_prefix 逐点现解）
wedb/wkv/src/session/mod.rs:virtual_domain（:482-499 代数落后即重解析）
wedb/wkv/src/store/keyspace.rs:flush_database（:137 仅 lock_dbmeta，与写臂零互斥）
对应 c# 文件与函数（无直位对，参照维度：状态闭环/多路径一致）：
garnet/libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase（:301 实例截断，无换代形态）
garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexSet（单实例同域写，无跨域伪影面）

精炼执行方案：
1. 域钉对齐既收口先例：三写臂入口单次解析 (vns, vdb, generation) 三元组，链内全部物理键构造与 AOF 事件域一律消费钉定值（session_tag_key_with_prefix 现成原语），禁逐点重解析
2. 落盘前换代复核：代数越过钉定基线即按既有 GenerationMoved/Swapped 分级显式失败——set/set_batch 此刻零树变零落盘可直失败；del 实删已生效按 Swapped 分级（复用 promote 票同款机制，不新造第二套）
3. 测试验证点：沿 migrate_cross_generation_ttl.rs:122-160 注入先例，在 refresh 与 save 之间插 FLUSHDB，断言新代域零伪影、命令显式失败、副本终态无幻影键、旧域面随延迟回收收敛

终态注记（2026-09-29 执行席收口）：
合入 d6c3535（Merge branch 'fix-ri-steady-pingen' into dev，内容 57cbd43 + dev 同步 cd9a220 + import 对齐 23896ae）。
收口形态：
1. 三写臂（range_index_set / range_index_set_batch / range_index_del）链首单次解析 (pinned_gen, vns, vdb) 钉定三元组，装载记录读取与取锁树身份键构造在钉定后零 await 窗内天然同域；链内锁内刷新（refresh_tiered_meta_with_prefix 新门面）、元记录回写（save_bftree_meta_stub_with_prefix）、del 臂排空面（drain_and_delete_index 单点解域：env/meta/TTL/ETag/树身份/旁表注销三参直调/Drop 事件域）、AOF 事件域一律消费钉定值禁逐点重解析；del_ttl_with_prefix / del_etag_with_prefix 同族新门面。
2. 落盘前换代复核：set/set_batch 树写前复核零树变零落盘直失败 GenerationMoved；del 实删已生效按 Swapped 分级（先推进 WATCH 栅栏再上抛，复用 promote 换入后复核同款）；复核后残余窗全链恒落钉定旧域 tolerated 旧域泄漏口径（doc/zh/db.md 1.4）。
3. 测试 wkv/tests/ri_steady_write_domain_pin.rs 四场景（set/set_batch/del 换代窗注入 + 纯交改回归），注入走 vdb.flush_db 真原语，断言新代域零伪影三面、副本零半代条目、旧域 tolerated 基线、重试按新域收敛；worktree cargo check --all-targets 全绿（含 dev 同步后复验）。
4. 遗留：主目录他席 WIP（keyspace.rs/flush_database.rs/nextest.toml/test.sh/ricreate_domain_clamp.rs）全程未触碰；collection.rs 他席在途 import 整理经预演验证零丢失吸收进合并结果（stash 往返 + drop，吸收性 diff 逐字核对）。
