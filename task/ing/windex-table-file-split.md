优先级：低（ crate 内最后一处多职责大文件；须在 2PL 剥离之后做）
来源：next/agy.db.md 条 14。核销 2026-09-19，取证基线 = 主仓 dev 当下 HEAD。

结论一句话
windex/src/table.rs 780 行把预取窗口内核、桶探测查找、候选地址收集、CAS 插入更新、
单桶闩转发与多键 2PL 引擎全塞在 HashIndex 一个文件里，而同 crate 其余部分已按一域一件
拆到 12 个平铺件；本票只做「按既有 grain 分家」，不改算法。

现状（主仓 HEAD 实测，windex/src/table.rs 共 780 行）
1. 预取内核：:630 起的多键批量预取体（:634-:639 第一级桶 cacheline 预取 + 哈希随探针带出、
   :642-:647 第二级链首 find_tag_by_hash + prefetch_record），常量与指令在
   windex/src/prefetch.rs（:4 PREFETCH_WINDOW、:13 prefetch_read_l1）。
2. 查找域：find_tag_by_hash / bucket_index_for_hash / bucket_index_for_key / bucket() 等
   桶定位与链首探测（本 crate 另有 chain.rs 116 行、candidate.rs 228 行的
   CandidateAddresses 候选收集件、entry_info.rs 106 行的 HashEntryInfo::try_cas 与
   BucketExclusiveGuard）。
3. 修改域：CAS 槽位插入/更新与死槽回收（部分原语已在 entry_info.rs:38 HashEntryInfo::try_cas）。
4. 闩域：单桶闩在 bucket.rs:74 起（try_lock_shared / try_lock_exclusive / unlock_*），
   table.rs 只做转发与守卫装配；guard.rs 持桶守卫类型。
5. 待先删项：:652-671 in_place_dedup_by、:673-708 acquire_bucket_locks、:710-714
   acquire_keys_lock_exclusive、:716-779 acquire_unique_locked_entries —— 这 128 行
   私有 2PL 引擎由 task/ing/windex-private-2pl-engine-removal.md 先行删除，
   本票不得把它搬来搬去。

C# 参考
1. libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs（表本体 + 公共 API 门面）
2. libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindRecord.cs
   （TryFindTag / TraceBackForKeyMatch 查找件；票内 cite 的 Implementation/FindTag.cs 不存在）
3. libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs
   （插入与 CAS 落槽）、Implementation/SplitIndex.cs（扩容，本仓已在 split.rs）
4. 即 C# 的 Implementation/ 目录「一操作一文件」粒度正是本 crate 已采用的 grain，
   table.rs 是唯一的偏离点。

修法
1. 前置：等 task/ing/windex-private-2pl-engine-removal.md 落地合并后再动，或两票同人合做。
2. 分家方向优先并入既有相邻件，其次新建平铺件（勿建 windex/src/table/ 目录模块——
   该 crate 全平铺，建目录会成唯一异类）：
   预取内核 → prefetch.rs；桶探测与链首查找 → 新建 find.rs（对位 FindRecord.cs 的
   TryFindTag 侧，若 chain.rs 现有内容更贴可并进去，二选一并记在实现说明里）；
   CAS 插入/更新/回收 → 新建 insert.rs；单桶闩转发 → 归 bucket.rs 现件，table.rs 不留第二份。
3. HashIndex 结构体定义、构造与对外门面（size/mask/load 等）留 table.rs；
   各域以 impl HashIndex 分部实现承载（本仓先例：wkv/src/store/*.rs 对 WedbStore 分域 impl）。
4. 跨件私有项一律 pub(crate) 以内，禁为拆分扩 pub 面；文档注释 C# 锚点随函数走。

验收判据
1. windex/src/table.rs 行数 ≤350，且 crate 内不再有第二处桶闩实现
   （判据：HashBucket::try_lock_exclusive 的 CAS 体只在 bucket.rs，
   HashIndex 侧只余转调；grep 定义点计数）。
2. 各域符号一处定义：HashIndex::find_tag_by_hash、HashIndex::prefetch_keys（预取内核实名）、
   HashIndex::bucket_index_for_hash、以及插入/CAS 件的主入口（落地时按现名逐个 grep 定位）。
3. windex 对外导出面（lib.rs pub use 集合）逐字不变，wkv/wtxn/wcpr 调用点零改动。
4. cargo check 通过（禁在共享 target 跑 test.sh / clippy.sh；windex/tests/index/* 由主代理跑）。

双花登记
并发代理就条 14 另立同题票 next/db-windex-table-split.md，两票同改 windex/src/table.rs，
只取一棒；且本票依赖 task/ing/windex-private-2pl-engine-removal.md（与
next/db-hashindex-2pl-single-orchestration.md 亦同题）先落。
