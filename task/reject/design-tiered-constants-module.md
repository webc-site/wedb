裁决：不成立（常量位置是设计文档钦定单点；分属两文件各有语义归属，无重复无漂移）
来源：next/muse.design.md 条 17。核销 2026-09-19。

一句话结论：四个分层阈值常量 + SET_MEMBER_DUMMY_VALUE 在 wcol/src/lib.rs:29-46 一处定义，
其路径 doc/zh/collection.md:27 直接以「常量 wcol::TIERED_PROMOTE_THRESHOLD」引用钦定；
LIST_SEQ_BASE 在 types/garnet_object.rs:21 属类型域细节——「散两文件」是语义归属正常的
组织形态，非散落失控，并入 constants 模块反而使文档引用漂移。

逐条核销
1. 单点钦定：doc/zh/collection.md:27「升阶高水位：N_high = 65536（常量
   wcol::TIERED_PROMOTE_THRESHOLD）」、:29 迟滞死区 [32768, 65536]——设计文档按
   crate 根路径引用常量，位置即契约；搬入 constants 子模块需要同步改文档并制造历史引用断链。
2. 归属实测：wedb/wcol/src/lib.rs:29 SET_MEMBER_DUMMY_VALUE、:32-46 四阈值
   （TIERED_PROMOTE_THRESHOLD=65_536 / TIERED_DEMOTE_THRESHOLD=32_768 /
   TIERED_PROMOTE_BYTES=4MB / TIERED_DEMOTE_BYTES=2MB）全部 pub 且全仓唯一定义；
   wedb/wcol/src/types/garnet_object.rs:21 LIST_SEQ_BASE（u128 移位型序列基）与
   GarnetObject 类型定义同文件——类型域常量随类型走是 rust_review「高内聚」口径的正例。
3. 无重复、无第二定义、无拼写漂移面（每个常量全仓一处 const）；muse 自述动作的柔性选项
   「或注明分属」即承认现状可接受，且 lib.rs 各常量已有文档注释说明用途与 collection.md 对应。
4. SKILL 对标：阈值体系是 transpile SKILL.md:28-31 自定义设计（C# 无对应常量组），
   其归属以 collection.md 文档为准，无 C# 拓扑可对标重组。
