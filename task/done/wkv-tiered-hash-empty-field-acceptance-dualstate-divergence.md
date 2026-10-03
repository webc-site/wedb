甄别结论：通过（甄别席 J6，2026-09-27，定级 P3——审核席 B 案登记小票，零行为改动台账收口）。亲验：validate_bftree_record 空键门与「两上限不同源」先例引用失根（ops.rs:597-615）、HSET 折叠臂 tiered_precheck（hash.rs:174-177）、既有双态锁测 tiered_empty_member_contract.rs（「引擎硬限下结构性无他解」头注 :16）在位；C# HashSet 无条件受理（HashObjectImpl.cs:185，票写 HashAdd 系存储层 API 名，订正成立）。危害收敛为台账缺册+引用失根。派沙箱席 c01l。

审核结论 2026-09-27 审核席 zcode 独立亲验：判定通过，裁定 B 台账登记（零行为改动，降级为登记小票移入 todo）。裁量依据：A 统一受理被外置引擎受理面结构性阻断——空键门系 bf-tree 0.5.6 tree.rs:1127「Key too small, at least one byte」的精确镜像（wbftree/src/service/ops.rs:83-86 同证 C# 原生库同拒空值空键入树），统一受理需树键侧重编码，破 zset/set 成员树序语义（ZRANGEBYLEX/扫描序）与 validate_bftree_record 四族单点镜像设计，且推翻 wnode/tests/tiered_empty_member_contract.rs 既有双态锁测（其头注自陈「引擎硬限下结构性无他解」），非单机制最小改动；B 零行为改动，双态行为已双向锁死（分层态 InvalidKV 帧/内存态 :2，同测试 :148-157 与 :181-189），余患仅台账缺册与先例引用无台账根。执行项：1 doc/zh/deviations.md 增条登记本分叉（键侧空/超长成员分层态 InvalidKV、载荷侧空值双态一致受理的边界线即引擎受理面），「两上限不同源」先例族（升阶契约闸 promote.rs:111-119，锚 wnode/tests/tiered_promote_contract_gate.rs）一并补根或本条内锚注，后续对拍席直引跳过；2 wkv/src/range_index/ops.rs:608 码内先例引用改指该登记条目号。票面两处小误订正后不碍立案：C# 锚符号实为 garnet/libs/server/Objects/Hash/HashObjectImpl.cs:185 HashSet（票写 HashAdd 系存储层 HashOps.cs API 名，对象层无条件受理主张成立）；「双态逐字节全等」条实在 task/review.md 板块 5.1（:156），票面归 4.2 仅多路径同构（:139）成立。

分层态哈希空字段/越契约值受理分叉致双态应答漂移，且 deviations 台账无登记、码内先例引用失根

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 对象层无条件受理空字段与任意长成员（Dictionary<byte[],byte[]> 无空键限制、无长度契约），HSET k "" v 恒成功。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
内存态（信封）HSET k "" v 成功；键升阶分层态后同命令回 InvalidKV 错误帧：wkv/src/range_index/ops.rs:598-609 validate_bftree_record 空键/超长门拒绝，注释自称「沿『两上限不同源』同类目先例」——该先例在 doc/zh/deviations.md、doc/zh/collection.md、task/ 全域检索不到（git 史无迹），引用失根；tiered_collection_ops/hash.rs:174-178 HSET 折叠臂逐对 tiered_precheck 同门。同数据同命令、应答随内部态漂移，抵触双态逐字节全等契约（task/review.md 板块 4.2 多路径行为同构）。升阶建树契约闸 range_index/promote.rs:111-119 已阻断「携违约成员进入分层态」，分叉仅在升阶后新增违约成员时显形。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
有限行为危害（升阶后空字段新增被拒、客户端可见错误帧与内存态不一致）；主要危害为台账缺册导致后续对拍轮按双态全等契约误判为转写缺陷重复立案，以及码内先例引用失根削弱裁决可追溯性。

涉及代码：
rust 文件与函数：
wedb/wkv/src/range_index/ops.rs:validate_bftree_record
wedb/wnode/src/resp/objects/tiered_collection_ops/hash.rs:HSET 折叠臂 tiered_precheck

对应 c# 文件与函数：
garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashAdd（无条件受理）

精炼执行方案：
1 审核席裁定分流：A 统一受理（分层树放宽空字段受理，与内存态/C# 对齐）或 B 维持受理边界（补 doc/zh/deviations.md 登记条目，码内注释先例引用改指该登记，零行为改动）
2 测试验证点：A 案双态同命令同应答锁测；B 案登记后以文档核对收口

收口记录（主席直办，2026-09-28）：零行为改动台账收口。收口形态=deviations §178 登记分层态键侧空/超长成员 InvalidKV 与内存态受理:N 的双态分叉（边界线=引擎受理面；载荷侧空值双态一致受理不入分叉），先例同族（升阶建树契约闸）本条内补根锚注；wkv/src/range_index/ops.rs validate_bftree_record 头注失根引用「两上限不同源」改指 §178。行号漂移订正：票面 ops.rs:608 现码实为 :645，promote.rs:111-119 现码闸口在 :131-138 区（引注不再钉行号）。既有双态锁测 tiered_empty_member_contract.rs 在位反证敏感，无新增测试。
