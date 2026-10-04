甄别结论：通过（甄别席 J6，2026-09-27，定级 P3——热区删除穿透 max_batch_deletes 假旋钮，无正确性危害）。亲验：热区 ScanBudget 双 MAX（ttl_sweep.rs:155-161）、cap 仅冷区两处消费（:169/:179）、删除循环无上限（:198-208）；config.rs:159「单轮过期扫描物理删除键数上限」文义无热区豁免（:161-163 限定的是扫描面），穿透成立；gc.rs 测试 2b（:188-225）确将现状锁死须翻锁；C# StoreExpiredKeyDeletionScan 全窗即删无上限旋钮（DatabaseManagerBase.cs:583-600）。热区 max_picks: cap 单点改+无续扫游标第二机制，自洽。派沙箱席 c01l。

审核结论：通过（内部一致性真案非契约案。热区双 MAX 实形属实（ttl_sweep.rs:155-158，cap 仅 :169/:179 冷区消费，删除循环 :198-208 无上限）；config.rs:158 文义穿透、册内无登记；collect_expired:78 候选集仅受内存窗宽天然界；C# StoreExpiredKeyDeletionScan 全窗即删对位属实；泄洪链可达（sweep 先于 reclaim_physical、inflight 闸 :277 长占）。方案：热区 max_picks 改 cap 单点，picked 恒 ≤cap 自洽、全窗重扫天然续收饿死不成立、EXPDELSCAN 不受扰；风暴期冷区顺延与「热区优先」语义一致，随 :120 注释留痕勿另调）

TTL 清扫热区候选收集双 MAX 无视 max_batch_deletes，单轮物理删除数无上限，删除风暴单轮泄洪且旋钮文档承诺被穿透

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 后台扫描每 tick 全量扫 [Log.ReadOnlyAddress, TailAddress) 热窗口（garnet/libs/server/Databases/DatabaseManagerBase.cs:583-600 StoreExpiredKeyDeletionScan），ExpiredKeysBase.Reader 回调内逐条 DELIFEXPIM 即删即计 deletedCount（garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:217-251），无每轮删除上限——C# 本无 max_batch_deletes 对应旋钮。单轮 N 键删除上限是 wedb 自研旋钮 max_batch_deletes 的立法，其登记文义（wkv/src/config.rs:159）为「单轮过期扫描物理删除键数上限」。对标分叉在扫描面（热区全窗扫描保留，对位 C# 无虞），不在计数面——文义既立单轮上限，行为即须兑现。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wedb/wkv/src/gc/ttl_sweep.rs:sweep_expired 中 cap = cfg.max_batch_deletes 仅两处消费：冷区触发门 picked.len() < cap（:169）与冷区候选预算 max_picks = cap.saturating_sub(picked.len())（:179）；段 1 热区收集预算为 ScanBudget{max_records: u64::MAX, max_picks: usize::MAX}（:155-158），统一删除循环（:198-208）对全量 picked 无上限逐键 check_expired 物理删除——同批 EXPIREAT 同秒到期的巨量短 TTL 键天然落热窗，单轮可删任意 N 键。该穿透已被 wedb/wkv/tests/gc.rs 测试 2b（:188-223）锁死为现状（热区不受 max_batch_deletes 截断），但 config.rs:159 单轮上限文义与 ttl_sweep.rs:31「GC 热区全量记录」注均未随更，契约文本与行为分叉在册无登记（deviations.md 全册与 task 五池查重零命中；done 票 wkv-rmw-window 系 RMW 闩预算面、todo 票 wkv-ttl-sweep-cold-cursor 系冷游标失败推进臂，皆非本案）。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
①删除风暴摊分承诺落空：max_batch_deletes=256 名义硬顶对热区形同假旋钮，引擎 API 调用方与运维按文义做风暴摊分预期全部失真；②单轮无界资源：百万级候选时 ExpiredKeySet 每键 Box<[u8]> 无界堆扩，串行删除链每键两读一 purge 全程在轮内烧尽，轮时长无上限；run_once 的 inflight 单轮闸（gc/mod.rs:275-282）被长期占用，同轮的换号物理回收与紧缩判定（reclaim_physical 随 tick 尾段）整轮顺延；③观测失真：stats.last_scan_deleted 可远超上限值，INFO 面吞吐判读与容量规划被误导。热区无续扫游标不构成公平缺口——下轮全窗重扫天然兜底，截断让渡的键必被再收，幂等双检在位。

涉及代码：
rust 文件与函数：
wedb/wkv/src/gc/ttl_sweep.rs:sweep_expired（段 1 ScanBudget 双 MAX :155-158、cap 消费位 :169/:179、无上限删除循环 :198-208）
wedb/wkv/src/config.rs:GcConfig.max_batch_deletes（:159 文义）
wedb/wkv/tests/gc.rs:test_hot_window_full_clear 臂（测试 2b :188-223 锁现状，随案翻锁）

对应 c# 文件与函数：
garnet/libs/server/Databases/DatabaseManagerBase.cs:StoreExpiredKeyDeletionScan（:583-600 全窗扫描形态）
garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:ExpiredKeysBase.Reader（:234-251 逐条即删，无单轮计数旋钮对位）

精炼执行方案：
1. 段 1 热区收集预算单点改 max_picks: cap（max_records 维持 u64::MAX——纯内存扫描面不动，保留对标 C# 全窗取舍）；收集满 cap 即停，未入选键凭下轮热区无游标全量重扫天然再入候选（check_expired 双检幂等已证，勿新增热区续扫游标第二机制）；删除循环保留现形，picked 已被 cap 封住，单轮物理删除数即受 max_batch_deletes 真实约束
2. 文本随更收口：config.rs:159 文义钉死「两段合计单轮物理删除上限」；ttl_sweep.rs:31 ScanBudget 注与 gc/mod.rs:4-8 门面注热区措辞同步（全量指扫描记录面，候选与删除受 cap）
3. 测试验证点：新增案——热区预置 5 枚过期键、max_batch_deletes=2，断言首轮 last_scan_deleted 恰 2、续轮各删 ≤2 至全清零残留；gc.rs 测试 2b「热区全量清零单轮完成」断言翻转为 cap 截断多轮收敛；冷区预算与游标推进既有案回归全绿

收口记录（收票席 R5 批次，2026-09-28）：合入 48794d2d（验货 d0152325/1d450e55+二次 dev 前进复查 gc 18/18 全绿）。收口形态=热区 ScanBudget max_picks 双 MAX 改 cap 单点（max_records 维持全量扫描面对标 C#），picked 恒 ≤cap 删除循环天然受限无第二续扫机制；config.rs 文义钉死两段合计封顶、EXPDELSCAN 独立入口未触；风暴期冷区经段 2 触发门自然顺延留痕。测试 2b 按票翻锁（全删形→cap 截断多轮收敛形）+ 新锁测 test_hot_zone_deletes_capped_by_max_batch_deletes（回装 usize::MAX 双案即红实测取证）。deviations 无需。
