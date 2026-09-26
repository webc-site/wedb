归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 dcf76bd（P4），收口形态：readme/en.md CRC16 过时表述订正为整数混合器槽哈希口径，README.md 随 mdt 再生成同步。续排注：本票沙箱席与方案详情见下文。

甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P4
核验记录（现码复跑，非票面背书）：
1 失真句现读在位：readme/en.md:105 与 README.md:122 均含「16,384 CRC16 slot hashing」；readme/zh.md 全文件 grep -i crc16 零命中，英文单侧滞后属实。
2 现码真源亲验：wbase/src/hash_slot.rs 模块头明令「键级哈希（CRC16 与 {...} Hash Tag）与 CROSSSLOT 已彻底废除」「库级定槽」声明在位（现读 :1-12）；C# 基线 HashSlotUtils.cs:HashSlot 系被废面对位，勿回改口径与 db.md 4.1 一致。
3 同步管道核：README.md 由 README.mdt 携带 <+ ./readme/en.md > 展开，两文件同句在位，双点消句需求成立。
4 查重：deviations.md CRC16 命中系残槽语义锁条目与他条内文，无本失真面登记；四池无同轴票。
5 格式与可执行度：两处同步改句 + 再生成校验（diff 展开产物一致、grep 归零）闭环，纯文档不触 .rs；双侧路径齐全（C# 锚注明已废勿回改）。定级 P4：对外文档失真。

审核结论：通过（r23-review-misc，2026-09-26）

亲验摘要：
- readme/en.md:105 与 README.md:122 同句在位，均含 "16,384 CRC16 slot hashing"；
  README.mdt:7 存在 <+ ./readme/en.md > 同步管道。
- readme/zh.md 全文零 CRC16 命中，对位节（zh.md:104）无此词，英文单侧滞后属实。
- wedb/wbase/src/hash_slot.rs 模块头明令「键级哈希（CRC16 与 {...} Hash Tag）
  与 CROSSSLOT 已彻底废除」「全仓唯一定槽真值源 slot_of」；doc/zh/db.md:320/:340
  同口径；C# garnet/libs/common/HashSlotUtils.cs:77-92 HashSlot 键级 CRC16 为被废基线。
- 查重：deviations.md 仅 :2002 残槽语义锁条目涉 CRC16 字样，与本票（README 失真）
  无重叠；task/todo、task/reject 无同类票。
- 双重失真成立：CRC16 已废 + 定槽真值源在 wbase 基座而非 wedb crate。

整理执行方案（供 task/fix.md 直接消费）：
1. readme/en.md:105 括注改写为现行口径，如 "16,384-slot database-level
   (namespace, db) integer-mixer slot hashing"，措辞遵循 task/doc.md 约束
   （克制、无形容词堆砌），不必在 README 内展开 CRC16 废除史。
2. 经 README.mdt 管道重新生成根 README.md（保证 :122 同步消句）；
   readme/zh.md 对位节本无 CRC16，口径已一致，不动。
3. 验证：grep -in "crc16" readme/ README.md 零命中；纯文档改动不触任何 .rs；
   不回改 C# 对位面（键级 CRC16 废除属既定改良）。

readme/en.md 英文宣称集群机制为 16,384 CRC16 slot hashing，与现码库级混合器定槽架构失真（中文对位节无此词，英文单侧滞后）

问题分析：
1. Garnet 契约对齐：C# 基线键级定槽为 garnet/libs/common/HashSlotUtils.cs:HashSlot 的键字节 CRC16 查表；本仓已按 doc/zh/db.md 4.1 明令废除键级哈希（CRC16 与 Hash Tag），改为库级 (namespace, db) 整数混合器定槽（r15-resil 档 :72 在册口径，禁误报为缺陷）。
2. 工程现状确证：wedb/wbase/src/hash_slot.rs 模块头显式声明「键级哈希（CRC16 与 {...} Hash Tag）与 CROSSSLOT 已彻底废除」，全仓唯一定槽真值源为 wbase::hash_slot::slot_of（mix64 双路质数乘混 + whasher::mix13 雪崩，CLUSTER_SLOT_COUNT=16384 单周期位与）；doc/zh/db.md 4.1「完全移除针对单个用户 Key 的哈希计算」与 4.3「Integer XOR-Shift Mixer」同口径。而 readme/en.md:105（Independence of wedb 段）宣称 "All distributed clustering mechanisms (16,384 CRC16 slot hashing, failover state machines, ...) reside entirely inside wedb"，根 README.md:122 由 README.mdt 管道同步携带该句。该句双重失真：其一，CRC16 已废除，现行定槽是库级整数混合器；其二，定槽真值源在 wbase crate 而非 wedb crate。中文对位节（readme/zh.md 同段「wedb 集群完全独立」）无 CRC16 字样，系英文单侧陈旧。
3. 逻辑危害确证：对外 README 是协作者与对拍席的第一手架构认知源，按此句会误判本仓仍为 CRC16 键级分片（进而误判 CROSSSLOT 错误存在、误判槽位计算成本模型），与法定架构文档 db.md 4.1-4.4 直接冲突；英文与中文两版对位节口径不一致，跨语言读者获得矛盾信息。纯文档面，无运行期危害。

涉及代码：
rust 文件与函数：
wedb/wbase/src/hash_slot.rs:slot_of（唯一定槽真值源，模块头含 CRC16 废除声明）
wedb/wconf/src/node_options.rs:NodeArgs（README 快速开始 --port/--dir 旋钮存在性已核）

文档锚：
readme/en.md:105（"16,384 CRC16 slot hashing" 宣称位）
README.md:122（生成产物同步携带）
doc/zh/db.md 4.1/4.3（法定架构口径，与该宣称冲突）

对应 c# 文件与函数：
garnet/libs/common/HashSlotUtils.cs:HashSlot（键级 CRC16 定槽，本仓已废除勿回改）

精炼执行方案：
1. readme/en.md:105 该括注改写为库级定槽口径，如 "16,384-slot database-level (namespace, db) integer-mixer slot hashing (key-level CRC16 abolished, see doc/zh/db.md 4.1-4.3)"，措辞遵循 task/doc.md 约束（克制、无形容词堆砌）。
2. 按 README.mdt 管道（<+ ./readme/en.md > 展开）再生成根 README.md，保证两处同步消句；核对中文对位节与英文新句口径一致。
3. 测试验证点：grep -in "crc16" readme/ README.md 零命中（bench.md 等无关文件除外）；diff README.md 与 README.mdt 展开产物一致；不触任何 .rs 业务码。
