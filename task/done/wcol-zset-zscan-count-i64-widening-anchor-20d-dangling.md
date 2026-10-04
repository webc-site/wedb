终态注记：合入 fb9374d，deviations.md 册尾顺编新落 §185（扫描族 COUNT 截断比较 i64 加宽、不复刻 C# int32 unchecked COUNT=-2147483648 翻倍回绕退化形，修复型惯例家族 §18 同谱，回收旧册「第 20 条 d)」悬空判据面），五处代码注锚（sorted_set_object.rs/scan_input.rs/hash_object.rs/tiered scan.rs/scan_family_dualstate_frames.rs）统一订正指 §185，零行为改动，锁测 8/8 原样通过。

甄别结论：通过（2026-09-29 主控甄别，定级 P3——重建册 §63-66 已被「TYPE 命令空串表外值」占用且仅 a/b 分目，五处注锚（sorted_set_object.rs:501-502、scan_input.rs:253、hash_object.rs:354、scan.rs:509、scan_family_dualstate_frames.rs:1174）仍指「第 20 条 d)」悬空；scan_kernel i64 承载现码确认。修复：册尾顺编落新节+五处注锚订正+锁测零改动回归）

审核结论：通过（2026-09-29 甲轮34-B，P3 级）。悬空实锤复核：五处注锚全在位——sorted_set_object.rs:503-504（换行拆写）、scan_input.rs:253、hash_object.rs:354、tiered scan.rs:509、scan_family_dualstate_frames.rs:1174（后两处为审核席补全，主控原验仅两处）；§20 现册仅 a/b 分目、全册 grep 加宽零命中。C# SortedSetObject.cs:459/:522 int32 承载翻倍回绕前提成立。方案：册尾顺编落新节+五处注锚订正，scan_family 锁测原样回归即闭环。无修正意见。

原票面：
ZSCAN COUNT i64 加宽刻意偏差登记锚「第 20 条 d)」悬空，重建册漏登该在役裁决

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# Scan（libs/server/Objects/SortedSet/SortedSetObject.cs:Scan、HashObject.cs:Scan 同构）以 int32 承载 count，「截断比较」用 items.Count == count * 2 判定保留；COUNT=-2147483648 时 int32 unchecked 翻倍回绕为 0，退化为「首个未命中条目即停」的空页爬行（不属负值全量遍历族）。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）：wcol 扫描族以 i64 承载 count 并按 i64 加宽翻倍（count * 2 i64 恒不回绕），五处代码注释一致声明该刻意偏差「登记见 doc/zh/deviations.md 第 20 条 d)，严禁按 C# 回改」，并有扫描族双态帧锁测钉形。但 2026-09-28 重建后的 deviations.md 中 §20 已被「TYPE 命令对空串及表外值归未知回空」裁决占用且仅含 a/b 分目，无 d) 目；册尾「号位空缺清单」亦未收录「第 20 条 d)」。即该偏差在在册五池（task/done|reject|issue|todo|ing）与重建册均无裁决正文，登记锚全面悬空，违反重建册硬规则「注释引用号位能与本册对上者即在册裁决，对不上者见册尾号位空缺清单」。
3. 逻辑危害确证：非运行时行为缺陷（i64 加宽为修复型在役偏差，行为正确且被锁测钉形）；危害在裁决单源断链——后续审查轮或回改者按注释锚查册落空，既可能误判该偏差为未登记新分叉重复提报，也可能以「册无此裁决」为由按 C# int32 回绕形回改，复活空页爬行退化面。与文档分叉注释订正先例（deviations §120/§130/§162 登记谱系）同族。

涉及代码：
rust 文件与函数：
wedb/wcol/src/zset/sorted_set_object.rs:SortedSetObject::scan（锚注释 :495-505 段）
wedb/wcol/src/types/scan_input.rs:scan_kernel（:253 注释锚）
wedb/wcol/src/hash/hash_object.rs:HashObject scan 段（:354 注释锚，同族邻面）
wedb/wnode/src/resp/objects/tiered_collection_ops/scan.rs:分层扫描臂（:509 注释锚，同族邻面）
wedb/wnode/tests/scan_family_dualstate_frames.rs（:1174 注释锚，锁测钉形）

对应 c# 文件与函数：
garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Scan
garnet/libs/server/Objects/Hash/HashObject.cs:Scan（同族邻面）

精炼执行方案：
1. deviations.md 落册新节（册尾顺编取号）：判据=扫描族 count 截断比较 i64 加宽、不复刻 C# int32 unchecked COUNT=-2147483648 翻倍回绕退化形，修复型惯例家族（§18 同谱），严禁按 C# 回改；符号锚 scan_kernel、SortedSetObject::scan；一手来源即本票与五处代码注释锚。
2. 五处代码注释锚「第 20 条 d)」统一订正为新落册节号（zset/hash/分层 scan/锁测四处，严禁动行为代码）。
3. 测试验证点：./js/check.js 无新增缺失；scan_family_dualstate_frames.rs 锁测原样通过（零行为改动，仅登记与注释订正）。
