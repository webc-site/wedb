甄别结论：通过（甄别席 zc-fix-r16-zrstguard，2026-09-26）定级 P3（登记级）
核验记录：C# 三锚亲验全中——garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:721-725 SortedSetRangeStore 空键守卫 dstKey.Length==0||srcKey.Length==0 → WriteInt32(0) 回 OK 零触达、:1343 SortedSetMPop 空键 continue、:695-701 SortedSetAdd RESP 版无守卫直 RMW；会话层 Resp/Objects/SortedSetCommands.cs:208-248 亦亲验无守卫（守卫确在存储层）。rust 三锚全中——write.rs:176-234 无空键门（空 src 走 zset_load_or_bail Missing 臂 store_overwrite 删 dst 回 :0、空 dst 正常写入回 :N）、slow.rs:711-757 zrangestore_cold 同形、blocking.rs:68-98 zset_pop_first_nonempty 逐键装载无空键跳过。deviations.md 查重：ZRANGESTORE 仅 §138（:1863）lex 漏斗括注与 :1710 STORE 族 TTL 轴，空键守卫形全册零登记；ing/done/reject 各池无同轴票。方向核验：rust 侧贴真 Redis（缺源删 dst 系 ZRANGESTORE 正解），登记不盲从 C#、严禁补守卫门取向与册内既有「取 Redis 一致侧、严禁回改」先例族（空集合不落库、GEO 存储回收臂等）同宗，架构合规（零行为改动、纯登记＋锁测）。可执行度：resp_sorted_set.rs 现无 ZRANGESTORE 空键锁测，补锁方案具体闭环。格式纯文本、双侧路径齐全。方案两点微调授权执行席：条目对照注记 ZADD 空键形系 r19 已决非分叉面（review_history/zcode-r19-zset.md:51）不重复展开。

审核结论：通过（登记级，够立案门槛，审核席 zcode-r19-review-zset，2026-09-26）

亲验记录：C# 三点全中——SortedSetOps.cs:SortedSetRangeStore 空键守卫（dstKey.Length == 0 || srcKey.Length == 0 → WriteInt32(0) 即回 OK，源不读 dst 零触达）、SortedSetMPop 空键 continue（:1343）、SortedSetAdd（RESP 版存储层）无守卫直接 RMWObjectStoreOperation；会话层 SortedSetCommands.cs:SortedSetRangeStore（:208-248）亲验亦无守卫（守卫确在存储层）。rust 双臂全中——write.rs:sorted_set_range_store（:176-234）空 src 走 Missing 臂 store_overwrite 删 dst 回 :0、空 dst 正常写入结果回 :N；slow.rs:zrangestore_cold（:711-757）同形；blocking.rs:zset_pop_first_nonempty（:68-98）for 循环无空键跳过、逐键正常装载。deviations.md 查重：ZRANGESTORE 全册仅 §138 lex 空串漏斗括注，空键守卫形零登记。方向核验：rust 侧贴真 Redis（空键为合法物理键、缺源删 dst 系 Redis ZRANGESTORE 语义），严禁按 C# 守卫形补空键早退门（复刻即与真 Redis 反向分叉，回改才是真回归）——本票取向与项目「rust 严向对齐真 Redis 的改良侧登记不盲从 C#」完全契合。连带 ZMPOP 空键注记核验成立（C# continue 跳过对 rust 正常装载弹出，同族边缘随本条一并登记）。

精炼执行方案：
1. doc/zh/deviations.md 补登记级条目（落册顺编取号，禁预拟号）：ZRANGESTORE 空 src/dst 键 C# :0 零触达守卫（内部 API 防御泄漏到 RESP 面）不复刻，rust 正常执行（空键合法物理键、缺源删 dst、结果可落空键）为真 Redis 一致侧；连带注记 ZMPOP 空键 C# continue 跳过对 rust 正常装载弹出形；条目内明令严禁按 C# 守卫形补空键早退门
2. 现状锁：resp_sorted_set.rs 补 ZRANGESTORE 空键两形锁——空 dst 形（ZRANGESTORE "" src 0 -1）回 :N 且空键可 ZCARD 验存活、空 src 形（ZRANGESTORE dst "" 0 -1）删 dst 回 :0（dst 预置旧值与 TTL，验终态 EXISTS 0）；对照 ZADD "" 1 m 空键正常建（双侧同形非分叉面）注记
3. 测试验证点：上述两形快慢双臂（慢臂 SlowWait 直驱）各验一次，应答与 dst/空键终态逐字节/语义锁定

ZRANGESTORE 空 src/dst 键 C# :0 零触达守卫未登记，rust 正常执行（真 Redis 一致侧）双侧应答与 dst 终态发散缺台账

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# ZRANGESTORE 存储层入口首段守卫：libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRangeStore（:721-725）if (dstKey.Length == 0 || srcKey.Length == 0) { writer.WriteInt32(0); return GarnetStatus.OK; }——src 或 dst 任一为空键即回 :0，源不读、目标键零触达（dst 旧值与 TTL 原样）。该守卫系内部 API 防御（C# ObjectStore 全域 HashOps/SetOps 的形参版 API 普遍带 Length==0 早退）泄漏到 RESP 面；RESP 版其余 zset 命令（SortedSetAdd :695-701 等）均无空键守卫，空键经 Tsavorite 正常处理。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust 全链无空键门：快路径 wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:sorted_set_range_store（:176-234）对空 src 正常装载（空键为合法物理键，Missing 走源缺失臂——store_overwrite 删 dst 回 :0）、对空 dst 正常 store_overwrite 写入结果集回 :N；慢路径 slow.rs:zrangestore_cold（:711-757）同形。即 rust 行为贴真 Redis（空键合法、缺源删 dst、结果存入空键），双侧分叉两形：其一 ZRANGESTORE dst "" 0 -1（空 dst）——C# :0 且 dst 不存在 vs rust 结果写入空键回 :N、EXISTS("") 为 1；其二 ZRANGESTORE dst "" 0 -1 换空 src 形——C# :0 且 dst 旧值原样 vs rust 删 dst 回 :0。台账缺位：doc/zh/deviations.md 全册无 ZRANGESTORE 空键登记。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
rust 无常态数据面危害（空键为极端边缘输入，行为与 Redis 一致）。危害落治理面：双侧对拍遇空键用例应答与 dst（含空键）终态双发散且无登记可引，对账席必误判转写缺陷；后席若按 C# 守卫形给 rust 补「空键 → :0 零触达」门，即把内部 API 防御泄漏复刻进 RESP 面、与真 Redis 语义反向分叉（回改才是真回归）。连带边缘：C# SortedSetMPop 对空键 continue 跳过（SortedSetOps.cs:1343），rust zset_pop_first_nonempty 对空键正常装载弹出，同族边缘随本条一并注记。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:sorted_set_range_store
wedb/wnode/src/resp/objects/sorted_set_commands/slow.rs:zrangestore_cold
wedb/wnode/src/resp/objects/sorted_set_commands/blocking.rs:zset_pop_first_nonempty（连带空键注记位）

对应 c# 文件与函数：
garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRangeStore（:721-725 空键守卫）
garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetMPop（:1343 空键 continue）
garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetAdd（:695-701 RESP 版无守卫对照）

精炼执行方案：见文件顶部审核结论内整理版（以顶部为准）。

合入哈希：28db14fe3e0acf6bcaba427c08a07219dda804ea 收口形态：deviations §153 纯登记（ZRANGESTORE 空 src/dst 键 C# :0 零触达守卫不复刻、rust 真 Redis 一致侧、严禁补空键早退门，连带 ZMPOP 空键 continue 注记）＋resp_sorted_set.rs::zrangestore_empty_key_forms_both_arms 快慢双臂两形锁测（空 src 删 dst 回 :0 终态 EXISTS/TTL 零残留、空 dst 落空键回 :N 存活钉），零行为改动，让号一次 §152→§153
