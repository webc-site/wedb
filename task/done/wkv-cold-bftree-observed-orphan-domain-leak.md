甄别结论：通过（甄别席 J6，2026-09-27，定级 P3——观察账单调滞留纯内存小账本，单点移除可落）。亲验：死域分支 continue 不动观察账（cold_tree.rs:80-83）、销账点仅 :88-91 与冷满臂 :110-114、全仓持锁操作单点 :78（grep 证实）；take_bftree_domain/:104-115、take_bftree_domains_of_vns/:128-145 摘域后 snapshot 永不含该域，条目不可达；虚号不重用（manager.rs:121-124 去回绕、:148-160 fetch_add+fetch_max）亲验。审核席单点裁定（reclaim_bftree_keys 入口 :158 顺带 remove）最小可落。派沙箱席 c01l。

审核结论（2026-09-27 方案审核席）：通过。真实性全链亲验吻合——wkv/src/gc/cold_tree.rs:81-83 死域 continue 不动观察账，观察账全部销账点（:88-91/:94-97/:103/:111）均要求快照含其域；take_bftree_domain（bftree_release.rs:104-111）/take_bftree_domains_of_vns（:129-141）摘域后 snapshot_bftree_domains 永不含该域，条目不可达；全仓 cold_bftree_observed 唯一持锁操作点 cold_tree.rs:78（另仅 store/mod.rs:186/:501 声明与初始化）；vdb 虚号不重用亲验（vdb/manager.rs:121 去回绕注释、:149-160 fetch_add 分配 + fetch_max 水位抬升）。方案裁定（票面授权审核席裁定）：「扫描轮懒清理单点」倾向否决——take 已摘域不在快照内，扫描轮遍历不到无法兜底，且观察账条目仅存 key_id（哈希）无域信息不可反查；正确单点为换号回收投递入口 reclaim_bftree_keys（bftree_release.rs:158 起，内部逐键重建 tree_identity_key，keyspace.rs:172/:247/:251/:328/:381/:408 六个 take 调用点全部立即投递此入口），在该处顺带 cold_bftree_observed.remove 即单点覆盖 FLUSHDB/FLUSHNS/SWAP 全族摘域路径；死域分支（扫描轮间隙残留）可另补逐键 remove 兜底（同动作无漂移）；clear_bftree_domains（flush_all_databases）同族摘路径执行时连带全清观察账。

冷树观察账对死域/被取走域条目不销账，FLUSHDB/FLUSHNS 换号下 cold_bftree_observed 单调泄漏

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# OnEvict 逐页同步驱逐无观察账形态（观察账系本仓轮询承接自建，cold_tree.rs:1-23 自陈），自洽面内的清理缺口。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wkv/src/gc/cold_tree.rs:80-83 is_dead_domain → continue 不动观察账；take_bftree_domain/take_bftree_domains_of_vns（wkv/src/vdb/bftree_release.rs:104-141）摘走域后快照不再含该域，观察账同样无人清理；全仓仅 cold_tree.rs:78 一处持锁操作该表。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
FLUSHDB/FLUSHNS 后域从 bftree_domains 消失且 vdb 虚号不重用（key_id 含域哈希永不复现），其冷观察条目（u128→i64）永久滞留；长寿命实例频繁换号下观察账单调增长（纯内存，量级小）。

涉及代码：
rust 文件与函数：
wedb/wkv/src/gc/cold_tree.rs:观察账维护臂

对应 c# 文件与函数：
无 C# 对位（轮询承接自建面，自洽依据）

精炼执行方案：
1 扫描轮对 is_dead_domain 分支补观察账条目销账（remove）；take_bftree_domain(s) 摘域路径同步清理对应条目（或统一收敛到扫描轮懒清理单点——审核席裁定，倾向扫描轮单点免双点漂移）
2 测试验证点：建冷树观察条目后 FLUSHDB，观察账归零；多轮换号下表尺寸有界

收口记录（收票席 R5 批次，2026-09-28）：合入 83dd96f8（验货 9ca21540/628a25bd+merge dev 复查 39 项定向全绿）。收口形态=审核裁定三点：①主单点 reclaim_bftree_keys 逐键身份键单趟双用（key_id 与 detach 同源）+观察账独立成段单锁摘除（不与条带锁嵌套，detach 未命中亦销账）；②死域兜底 continue 分支锁内逐键 remove 无漂移；③clear_bftree_domains（flush_all）连带全清。内省口 cold_bftree_observed_len 仅锁测消费。锁测 tests/store/flush_cold_observed_ledger.rs 双案（FLUSHDB 换号/flush_all 归零），摘三处销账实测即红。deviations 无需。席报在途红灯 promote_ri_create_domain_pin 系他席在途票辖面（wkv-promote-ri-chain），与本修无关，记门禁差分。
