裁决：不成立——三裸入口各有独立消费者与 C# 对位面，生产主从链在现刻 HEAD 已单点；
票面 remedy（删裸 compact 或降 pub(crate)）与 C# 公共无 cf 重载的 1:1 对标正面冲突，
且 pub(crate) 直接打断集成测试面；剩下的「doc 标注」是纯注释级动作而该标注已在册。
本棒零代码改动，票转 task/reject/ 归档。

核销 2026-09-19。取证基线：主仓 /Users/z/git/db/wedb 分支 dev 现刻 HEAD 22548fc，
行号一律按符号在当下代码重取（票面 :258/:265 已位移为 :266/:274）。

拒绝理由（逐条对票面主张）

1. 「绕过 wkv 编排即跳过 VDB 换号过滤、TTL 判死与 CPR 纪元屏障」在生产侧无现场。
   全仓 src（tests 除外）构造 LogCompactor 的路径唯一：wedb/wkv/src/compact.rs:266
   `pub fn compactor`；`.compactor()` 的消费点仅同文件 :280/:299/:309 三处，全仓无第四处。
   后台驱动 wedb/wkv/src/gc.rs:657 `store.compact(until, tier)` 即 WedbStore::compact
   （compact.rs:274）→ :281 `compact_with_filter(.., &WedbCompactionFunctions)`，
   换号过滤与 TTL 判死单点就在该谓词内（compact.rs:184 struct、:186 impl
   CompactionFunctions、:199 is_deleted）。除该门面外，src 全域不碰 wcompact 紧缩面：
   wcompact 在 src 侧的引用仅 wkv/src/compact.rs（门面本体）、wkv/src/gc.rs:73（取
   CompactionType 枚举）、wkv/src/error.rs:82（错误变体转发），无第二直调链。
2. 裸 compact（wcompact/src/compactor/mod.rs:278）不是第二套入口，是 C# 公共重载的
   1:1 对标件，删它才是失对标。C# 侧：
   garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/LogAccessor.cs:280
   `public long Compact<TInput, TOutput, TContext>(long untilAddress, CompactionType
   compactionType)` 就在 :281 以 `default(DefaultCompactionFunctions)` 委托带 cf 重载
   （同文件 :291 是公开的带 cf 版）；ClientSession.cs:430 同款无 cf 公共重载、:431 委托
   default(DefaultCompactionFunctions)、:441 带 cf 版。真正 cf 必载的「裸内核」是
   TsavoriteCompaction.cs:21 `internal long Compact<...>(cf, untilAddress, compactionType)`
   ——internal、不对外直发，对位件正是 rust 侧 cf 必载的
   wcompact/src/compactor/mod.rs:169 compact_with_filter（门面经它注入判死）。
   票面「业务判死只在 server 层注入，native 层裸内核不对外直发」由此完全成立，且 rust
   拓扑与之吻合；被票面点名的无 cf 重载在 C# 里同样是 Microsoft 公开的宿主便捷形态
   （garnet/libs/server/Databases/DatabaseManagerBase.cs:449 Scan 档与 :458 Lookup 档走的
   都是带 GarnetRecordTriggers 的 :291 重载，与 rust 门面走 :281 一致）。另注：C#
   DefaultCompactionFunctions 本身是 internal 件
   （libs/storage/Tsavorite/cs/src/core/Compaction/ICompactionFunctions.cs:23），
   公开面只有便捷重载，故 rust 保留 compact 重载、不外抛默认谓词的构造需求，即为对标态。
3. 「改 pub(crate)」在 rust 侧不可行：wcompact/tests/ 与 wkv/tests/ 都是集成测试（独立
   crate），pub(crate) 后不可见即编译不过。现刻裸 compact 的测试调用点 53 行——
   wcompact/tests/compact/{truncation.rs:30,73,117,208,231、lookup_mode.rs:35,98,136,150、
   fuzzy_bound.rs:109,120,212,222、concurrency.rs:36,101,171} 与
   wkv/tests/compact/{basic.rs、more_log_compaction.rs、spanbyte_compaction.rs、
   concurrency_and_collision.rs}——票面自己给的口径也是「直连仅限 wcompact 测试」，
   与该测试面对齐，非冗余消费。
4. compact_lazy（mod.rs:260）有独立语义与真实消费者：它是 wedb 自有的「单轮有界前推」
   入口（:244-259 doc 已写明与 Garnet 触发式回退语义互补，回退语义在 wkv gc.rs），
   消费方为 mod.rs:272 的内部策略分派、wcompact/tests/compact/truncation.rs:168,173、
   wkv/tests/compact/lazy_compaction.rs:61,89,136,182,192（该档是它的专档行为测试，
   含短路边界与多轮滚动）。按「读侧零消费者但写侧有区分即非死码」的口径，它不属可删面。
5. 票面唯一还能执行的动作只剩「doc 标注」，而标注已在册：mod.rs:275-276「生产入口应经
   宿主封装注入业务谓词，对标 DatabaseManagerBase.cs:449 注入 GarnetRecordTriggers」、
   compact_with_filter 的 :154-168 doc（safe_ro 入口硬校验与三通道判死逐条列明，
   :178-189 有对应硬拒实现）、门面侧 compact.rs:270-272「生产入口显式注入 wedb 业务过滤」。
   项目 spec 明令禁止以改注释过关，票面补录亦要求「勿为改口而改口」。
6. 同题前案已判：task/reject/agy.db.md 条 17（2026-09-19 核销）对同一主张给出同一结论
   （主从已成立无旁路、裸 compact 为 C# 1:1 对标件、动作纯注释级）。本票是该条与
   muse.db 条 16 的合并重立，自前案核销至今现场代码未变（该档案引 WedbStore::compact 在
   compact.rs:266、判死注入在 :273、三处 `.compactor()` 在 :272/:291/:301、后台驱动在
   gc.rs:604，现刻 HEAD 依次为 :274、:281、:280/:299/:309、gc.rs:657——compact.rs 侧
   恒 +8 位移系 acl-ns-compact 棒在本文件上方扩判死豁免所致，gc.rs 侧随同波改动位移，
   结论不受影响）。

顺带取证（本票拒绝时新查得、留给主代理立案的独立事实，本棒不动）：

- wedb/wkv/src/compact.rs:307 `WedbStore::compact_lazy` 是零消费者的门面转发件：全仓
  `.compact_lazy(` 命中仅本文件 :310 的内部转发与测试侧的 LogCompactor 直调（测试不经门面）。
  且它转的是内核默认无业务判死路径（mod.rs:272 → compact → DefaultCompactionFunctions），
  其 doc「对标 Garnet 周期紧缩任务语义」与实际语义不符（不经 WedbCompactionFunctions，
  TTL 过期与换号孤儿只按墓碑判）。这属「零消费者门面件 + doc 失真」一类，与本票射程
  （wcompact 裸入口收紧）不同面：删它要连 wkv 门面 doc 一并订正，宜并入
  task/ing/zero-consumer-dead-surfaces-batch-six.md 的批量判读或单立一薄票，勿在拒绝票里顺手做。

—— 以下为原票全文 ——

优先级：低
来源：next/agy.db.md 条 17 与 next/muse.db.md 条 16 两轮同题合并（agy 原引证
WedbStore::compact 在 store/gc.rs 有漂移，实际定义在 compact.rs）。取证基线：主仓 dev 当下代码。

问题
紧缩驱动主从入口不分：wcompact LogCompactor 的裸 compact/compact_with_filter/
compact_lazy 三入口均为 pub，绕过 wkv 编排即跳过 VDB 换号过滤、TTL 判死与 CPR 纪元
屏障；wkv 侧 WedbStore::compact 门面已注明「生产入口」但 wcompact 侧无对称防线，
新人可直连底层造成物理记录误判活。

取证
- wedb/wcompact/src/compactor/mod.rs:278 pub async fn compact（裸入口）、:169
  compact_with_filter、:260 compact_lazy。
- 生产链单点：wedb/wkv/src/compact.rs:265 pub async fn compact（doc 已注明对标
  TsavoriteKV.Compact、生产入口显式注入 WedbCompactionFunctions）、:258 compactor、
  后台驱动 wedb/wkv/src/gc.rs:657 try_compact -> store.compact。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs
  紧缩内核；garnet/libs/server/Databases/DatabaseManagerBase.cs:449 注入
  GarnetRecordTriggers（业务判死只在 server 层注入，native 层裸内核不对外直发）。

修法建议
wcompact 裸 compact 方法族收紧：compact（无过滤版）删除或改 pub(crate) + doc 注明
仅测试专用；compact_with_filter / compact_lazy 保留 pub 但 doc 标注「须由 wkv
WedbStore 门面注入业务判死后调用，直连仅限 wcompact 测试」；wcompact 内 tests
直调点同步改。收口后生产唯一链 = GcManager::try_compact -> WedbStore::compact ->
LogCompactor。与 next/db-wkv-gc-split.md（gc.rs 拆分引用同链）无文件冲突可并行。

主代理补录（14:13，agy.db 晚波条 17 反证）：称生产已走 WedbStore::compact → compact_with_filter(&WedbCompactionFunctions)（store/gc.rs:604 附近），裸 compact 对位 C# TsavoriteKV.Compact 默认形态、其余消费者为 compact_lazy/测试，「诉求实为纯注释级」。请甄别时按符号核实现刻 HEAD：若主从链已单点，判「已落地→注释补强或删票取证」，勿为改口而改口。
