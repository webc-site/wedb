终态：已合入 dev（2026-09-27）。5f43ca9 零行为文案收口:FillerRem 虚构句删,README 位段图四处漂移+末桶 65535 订正

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：零行为纯文案收口，含 README 位段图勘误六处外溢

审核结论：通过（锚订正已并入：set_filler_bytes 实位 header.rs:339-346 非 bits.rs；外溢面连带 wreviv/README.md:40 末桶「...65535」漂移一并收口）（亲验：FillerRem 全仓仅 8 处纯文档命中、garnet 零、git log -S 证代码从未存在；三方反驳逐验在位——bits.rs:47-52 词计数、RecordInfo 恒 11 位保留区断言、C# RecordDataHeader.cs:22-27 词粒度+MaxFillerWords=255、pool.rs 整词编译期断言、header.rs:323-346 恒词折算别无单字节实现；README 位段图错标 prev_address 字 bits 48..55 并虚构 56..58 坐实；§49 文案勘误先例同形制达立案门槛，§111c 未授权单字节填充；五池查重零撞）

wreviv 模块头自陈「FillerRem 单字节填充精度、分桶检索不对齐」与本体编译期整词断言及 wrecord/C# 位段实况相悖（连带 wrecord README 位段图虚构 bits 48..58 FillerWords/FillerRem）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 松弛填充从始至终只有词粒度一形：RecordDataHeader.cs:22-26 注 FillerWords 为 8 位计数、每词 8 字节（kRecordAlignment），显式填充上限 MaxFillerWords*8=2040 字节，越限走记录分裂、原记录保留 RecordSplitRetainFillerWords=64 词即 512 字节（:74-84），全文件无单字节余数字段；「FillerRem」在 C# 全仓零命中（grep FillerRem 仅命中本仓 rust 文档）。即词粒度整词对齐是上游唯一法定填充形态，与记录 8 字节对齐不变式（Constants.kRecordAlignment）同源。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 侧实况与 C# 同形：wrecord/src/header/bits.rs:47-52 FillerWords 为 RDH 字 bits 0..7 词计数、:68 MAX_FILLER_BYTES=2040、:339-346 set_filler_bytes 按词折算、header.rs:323-328 filler_bytes 自陈「词粒度」；RecordInfo 字 bits 48..58 系 11 位保留恒零区（bits.rs:21-22 RECORD_INFO_RESERVED_MASK 与编译期断言 1）。而 wreviv/src/lib.rs:34-35 模块头「单字节填充精度」条却自陈本实现「配合 wrecord 的 FillerWords/FillerRem 单字节精度松弛填充，分桶检索不对齐、按字节粒度匹配」——所指 FillerRem 字段全仓代码零实体（仅文档命中，git 史为单 init 快照、从未存在）；同仓 pool.rs:16-19 与 :55-57 恰以反向立场立论：末桶 65528 的存在理由正是「全仓记录 8 字节整词对齐不变式、非对齐尺寸切出 Pad 跨度破坏对齐」，且编译期断言强钉 DEFAULT_BIN_SIZES 全员 is_multiple_of(RECORD_ALIGNMENT)。两文件同 crate 互斥陈述，模块头一条为虚构事实。同款虚构外溢至 wreviv/README.md:25/:75、wreviv/readme/zh.md:13、wreviv/readme/en.md:13（重复 lib.rs 口径）与 wrecord/README.md:37/:103、wrecord/readme/zh.md:23、wrecord/readme/en.md:23（把 FillerWords 错标于 prev_address 字 bits 48..55、并虚构 bits 56..58 FillerRem 0..7B 子段——与代码保留位恒零实况直接冲突）。
3. 逻辑危害确证
零运行时危害，属文档事实性缺陷与概念真源污染（板块 1 单一真源、§49 裁决文勘误同族判例）：后续 fix 席若信 lib.rs「按字节粒度匹配、不对齐」口径，或以 wrecord README 位段图为准在 RecordInfo 字保留区实现「FillerRem」读写，将直接击穿 pool.rs 整词断言前提（末桶 65528 收口、Pad 跨度词整倍、扫描跳步算术 HEADER_SIZE+val_len 互逆性三处连锁），并把恒零保留区变成第二填充真源、破坏 48+11+5 位无缝铺满断言；对账席亦可能据「检索不对齐」误判 65528 末桶为漂移而按 65535「回改」。缺陷系纯文案与代码实况不符，纠正即收口，不改任何行为。

涉及代码：
rust 文件与函数：
wedb/wreviv/src/lib.rs 模块头「单字节填充精度」条（:34-35，虚构本体）
wedb/wreviv/src/pool.rs MAX_ALIGNED_BIN_SIZE 推导注与 DEFAULT_BIN_SIZES 编译期对齐断言（:16-19、:55-57，被矛盾的同仓真源）
wedb/wrecord/src/header/bits.rs FILLER_WORDS 位段群与 RECORD_INFO_RESERVED_MASK（:21-22、:47-52、:66-68、:70-77，位段唯一真源）
文案外溢面：wedb/wreviv/README.md:25/:75、wedb/wreviv/readme/zh.md:13、wedb/wreviv/readme/en.md:13、wedb/wrecord/README.md:37/:103、wedb/wrecord/readme/zh.md:23、wedb/wrecord/readme/en.md:23

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs（:22-26 FillerWords 词粒度契约、:74-84 MaxFillerWords=255 与 RecordSplitRetainFillerWords=64，全件无单字节余数字段）

精炼执行方案：
1. wreviv/src/lib.rs:34-35「单字节填充精度」条改写为与代码实况一致的表述：配合 wrecord FillerWords 词粒度（每词 8 字节）松弛填充，记录尺寸恒 8 字节整词对齐，末桶 65528 即该不变式的池侧收形（回指 pool.rs 编译期断言，不另立口径）。
2. wreviv 三处 README（README.md/readme/zh.md/readme/en.md）同步改写；wrecord 三处 README 位段图订正 FillerWords 实际驻 RDH 字 bits 0..7、RecordInfo 字 bits 48..58 为 11 位保留恒零区，删除 FillerRem 虚构子段。零代码、零行为改动。
3. 测试验证点：全仓 grep FillerRem、单字节填充两词零命中收口断言；cargo test -p wreviv 既有边界用例（purge_and_limits.rs boundary_parameter_defense 已钉末桶 65528 与整词口径）零漂移；wrecord bits.rs 编译期断言 1/2 原样通过。
