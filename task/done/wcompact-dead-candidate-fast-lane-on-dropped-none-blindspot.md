甄别结论：通过（甄别席 J6，2026-09-27，定级 P1——死候选快速通道遇 on_dropped None 击穿安全垫，存活分层键树删除+向量误删+伪 AOF 事件）。亲验：阶段 3 死候选快速通道不重审（run.rs:311-325 注释自承「阶段 1 已判死」），非死候选 :341 区以新 now 对称重审，缺口确凿；on_dropped 安全垫 if let Some 穿透 None（wkv/compact.rs:423-428），ttl_of→read_i64_sidecar「记录不存在/墓碑/非法长度一律 None」（ttl.rs:278-290）；危害链各环在场：claim_gate_and_strip_sidecars 先 del_ttl（collection.rs:80-86）、RangeIndexDrop+delete_index（compact.rs:445-470）、向量钩子 :476-480、host.rs:172-181 承诺句「未过期零副作用」被 None 击穿。审核席 on_dropped 单点裁定（复用宿主存在性探查，窄路径零常态开销）合规。派沙箱席 c01l。

审核通过 2026-09-27：亲验 run.rs:311-325 快速通道注释自承「阶段 1 已判死」不重审、:341 非死候选以新 now 重审恰证死候选缺对称复查、compact.rs:423-428 安全垫 if let Some 不拦 None、ttl.rs:285 不存在/墓碑/非法长度一律 None、collection.rs:84 claim_gate_and_strip_sidecars 先 del_ttl 且新版本落 tail（阶段 2 上界 safe_ro 不可见）、host.rs:180-181 承诺句、Lookup 模式 compact_lookup:175-181 judge_dead→drop_dead 同型微窗，危害链（树销毁先于摘槽 CAS，主记录幸存而树/向量资源已毁）成立。落点裁定 on_dropped 单点：仅 None 分支经宿主存在性探查（复用 host_exists_cooperative 既有单点，窄路径零常态开销）在场即保守保留，随后摘槽 CAS 自然落败零操作；阶段 3 补复查只收 Scan 大窗且 Lookup 模式无对位插入点，弃。

紧缩阶段 3 死候选快速通道不重审，on_dropped 安全垫 TTL=None 盲区放行存活键破坏性清退

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 无对位（GarnetRecordTriggers.IsDeleted 恒 false，紧缩无业务判死）；本仓自洽依据为 wcompact/src/host.rs:176-181 trait 文档自承「并发安全垫由宿主的过期重读裁决承担……未过期零副作用」——None 形态击穿该承诺。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wcompact/src/compactor/run.rs:311-325 阶段 3 if cand.is_dead 快速通道直接 drop_dead，不重跑 judge_dead/find_latest_address（对照组非死候选 :341 起以新 now 重审，恰证死候选缺对称复查）。wkv/src/compact.rs:423-428 on_dropped 安全垫 if let Some(exp) = ttl_of(...) && !is_expired——None 直接穿透到破坏性清退。危害链：阶段 1 判死（TTL 旁读 Some(已过期)，run.rs:262-264）→ 阶段 2 扫 [actual_until, safe_ro)（I/O 无界可达整个定稿区）期间用户重建该键：写路径 claim_gate_and_strip_sidecars（wkv/src/session/collection.rs:84 del_ttl）/惰性 probe_alive→purge 均先删 TTL 记录、新版本落 tail（> safe_ro），阶段 2 看不到 → 阶段 3 快速通道 drop_dead → on_dropped 复查 ttl_of 返回 None（wkv/src/ttl.rs:277-281 无记录/墓碑一律 None）→ 安全垫不拦截 → get_tree 同树身份键命中新对象 → RangeIndexDrop AOF 事件（compact.rs:445-453）→ delete_index 销毁新树 → unregister_bftree_key → delete_miss_hook 向量删除钩子对存活键触发（:476-480）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
存活分层键数据丢失（树文件删除）、存活向量键被请求删除、AOF/副本收到伪 RangeIndexDrop 清理事件。主记录槽位 CAS 落败幸存但树/向量资源已毁。Lookup 模式同型微窗（judge_dead→on_dropped 两次旁读间隙），Scan 模式阶段 2 全量 I/O 大窗为实际暴露面。

涉及代码：
rust 文件与函数：
wedb/wcompact/src/compactor/run.rs:阶段 3 快速通道
wedb/wkv/src/compact.rs:on_dropped 安全垫

对应 c# 文件与函数：
无 C# 对位（host.rs trait 文档承诺为自洽依据）

精炼执行方案：
1 双点收口：on_dropped 垫对 None 形态经宿主存在性探查（主记录链首探针 find_latest 或 host_exists_cooperative 形态）确认键已不在才清退，在场即保守保留；阶段 3 死候选快速通道补对称复查（照非死候选 :341 先例以新 now 重估）——审核席裁定单点或双点（倾向 on_dropped 单点，Lookup 模式同窗一并收口）
2 测试验证点：Scan 紧缩阶段 2 期间重建已判死分层键，紧缩完成后新树存活、无 RangeIndexDrop 事件、向量钩子不触发
