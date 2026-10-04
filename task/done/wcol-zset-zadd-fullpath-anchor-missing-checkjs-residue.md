甄别结论：通过（甄别席 J7，2026-09-27，定级 P3——纯锚补挂零行为）。sorted_set_object_impl.rs:316-317 无 doc 注、:241/:553 正形锚在、zset.rs:171 点号词形、tiered_cmds_align.rs:475 裸文件名、miss yml fn:[SortedSetAdd]，亲验全实；CS_REF_REGEX 含路径捕获组、check.js:343-344 isDocumentedAnchor 按归一路径 key，裸文件名不入键空间坐实。派沙箱席 c01o。

审核结论：通过（门禁锚类零行为。亲验全实：sorted_set_object_impl.rs:317 无 doc 注、:241/:553 持正形锚；zset.rs:171 点号散文言 CS_REF_REGEX（rustScan.js:35 须 .cs 段）不收；测试域 475 裸文件名不入库；ignore 全册零命中（server.yml:959、client.yml:212 系异文件不豁免——票面 958 勘正 :959）；亲跑 bun js/check.js 坐实「实现缺失」树唯一红口 SortedSetAdd；317 函数体全量转写非假桩。五池无同案）

整理执行方案（供 fix 消费）：
1 :317 上方照 :241 体例补 /// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd
2 zset.rs:171 词形就地改全路径；测注 475 并路径删换行括号
3 验收：rg 生产面两址命中、check.js 撤 miss、禁登 ignore

ZADD 主链缺 C# 方法全路径锚，check.js 门禁对本族常驻假红「实现缺失」

问题分析：
1. 门禁契约与 C# 原型侧确证：check.js 的缺失判定要求 Rust 注释里出现
libs/ 全路径形式的 File.cs:MethodName 锚（js/check/rustScan.js:34-35
CS_REF_REGEX 捕获组含路径段，js/check.js:343-344 isDocumentedAnchor 按
rel_path 加方法名查表，裸文件名不在键空间内），js/check.js:346-348 明文
规定此类「仅词元提及」项的处理口径是「属真缺失的按实现票处理、叙述性误
伤走改写注释」，而 js/check/ignore/libs/server/Objects/SortedSet/
SortedSetObject.yml 首注已立本项目正解范式：「已改挂全路径锚而非登记本档」。
C# 侧被锚对象为 libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:90-213
SortedSetAdd（ZADD 主循环：选项段消费 :114、XX continue 臂 :137、INCR 折叠
:150、NX/GT/LT 拒收与 INCR 回 nil :171-181、CH 计数 :191、INCR 单写点 :196），
系 ZSET 分值族最大语义面。
2. 工程现状确证（双侧 rg 亲验）：rust 生产面两个承接点均无全路径锚。物层
wedb/wcol/src/zset/sorted_set_object_impl.rs:317 SortedSetObject::sorted_set_add
上方 :316 impl 块起即无任何 doc 注释（同文件同族两处已有正形锚可对照：
:241 挂 GetOptions、:553 挂 SortedSetIncrement，均为 libs/ 全路径词形）；
分层臂 wnode/src/resp/objects/tiered_collection_ops/zset.rs:117-126、:143、
:171、:215 对本体一律用散文言「C# SortedSetAdd」「SortedSetObjectImpl.
SortedSetAdd：」点号词形，不在正则捕获面内。全仓唯一命中该名的锚在测试域
wedb/wnode/tests/tiered_cmds_align.rs:475，且为裸文件名形（路径写在随后括
号里换行断开，正则只收到 SortedSetObjectImpl.cs），按 :343 口径不构成对物
锚。ignore 树全册 rg SortedSetObjectImpl 零命中（server.yml:958 与
client.yml:212 的 SortedSetAdd 分属 SortedSetOps.cs 与 GarnetClient 层异
文件，不构成对物豁免），即该项既未锚也未登记，
故 js/check/miss/libs/server/Objects/SortedSet/SortedSetObjectImpl.yml 持
`fn: [SortedSetAdd]`，为整个 ZSET 族（含 SortedSetObject/Comparer/Geo 与会
话层 Commands 四档）唯一残留红口。deviations 全册与五池无同案（task/ 全池
grep SortedSetAdd 零命中）。
3. 逻辑危害确证：门禁侧长期假红即 check.js:100-105 明文警惕的「假缺失污染
甄别」形态——本族真缺失一旦出现会被常驻噪音淹没，缺失检测对本族失效；
对账侧按图索骥时 ZADD 双态主链无任何可定位的转写源锚，后续复核席只能靠
散文言反推，违背台账单源与逐字节对账纪律。纯锚链漏挂，无行为分叉，双侧
语义已核验为 1:1（含 §142 全或无、§151 奇数尾巴截断在册项）。

涉及代码：
rust 文件与函数：
wedb/wcol/src/zset/sorted_set_object_impl.rs:SortedSetObject::sorted_set_add
wedb/wnode/src/resp/objects/tiered_collection_ops/zset.rs:tiered_zset_arm Zadd 臂
wedb/wnode/tests/tiered_cmds_align.rs:分层态同分值位模式测注
js/check/miss/libs/server/Objects/SortedSet/SortedSetObjectImpl.yml

对应 c# 文件与函数：
garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd
garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:GetOptions（已锚先例）

精炼执行方案：
1. wcol/src/zset/sorted_set_object_impl.rs:317 上方按同文件 :241/:553 既有
体例补一行 doc 注：
/// libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd
不改函数体、不加第二套说明文，纯注释锚。
2. tiered_collection_ops/zset.rs:171 主循环注的「C# SortedSetObjectImpl.
SortedSetAdd」点号词形就地改写为上述全路径词形，使分层臂同挂该唯一锚，
避免双态对账时只有一侧可索；其余散文言保持不变。
3. 测试域 tiered_cmds_align.rs:475 的裸文件名锚订正为全路径形（路径并入
冒号前，删去冗余括号注）。
4. 验证：rg -g '*.rs' "libs/server/Objects/SortedSet/SortedSetObjectImpl\.cs:
SortedSetAdd" 生产面两址命中；bun ./js/check.js 后
js/check/miss/libs/server/.../SortedSetObjectImpl.yml 由 missSync 自动撤除，
且全册零新增红、无 ignore 条目新增（严禁以登记 ignore 掩盖本项）。

收口记录（收票席 R3 批次，2026-09-28）：合入 5895ad51（分支 wcol-zset-zadd-fullpath-anchor-missing-checkjs-residue，验货 commit c370bee3）。收口形态=纯锚三点：sorted_set_object_impl.rs:317 补 /// 全路径锚（对位 C# :90 亲验）、tiered_collection_ops/zset.rs:171 散文言改全路径词形、tiered_cmds_align.rs:475 裸文件名并路径；零行为改动，check.js SortedSetAdd 实现缺失红口清零、词元提及 41→40，其余红口与基线无劣化。
