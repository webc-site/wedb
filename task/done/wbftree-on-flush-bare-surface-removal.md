优先级：高（死代码：零生产消费入口 + 其独占的裸名工件族与恢复分支）
来源：next/agy.db.md 条 19。核销 2026-09-19，取证基线 = 主仓 /Users/z/git/db/wedb 分支 dev 当下 HEAD。

结论一句话
wbftree 的 on_flush 只是 on_flush_address 的 None 包装，生产永不走该臂，但它牵出一整条
「裸名刷盘工件」族（bare_flush_path 命名、截断覆盖写、恢复期裸名优先分支与目录扫描），
C# 的 SnapshotTreeForFlush 签名恒带 logicalAddress，无裸名形态。应删无地址入口并连带清除裸名工件族。

现状（主仓 HEAD 实测）
1. 双入口：wbftree/src/manager/flush.rs:13 pub fn on_flush（转 on_flush_internal(.., None)）、
   :18 pub fn on_flush_address（转 Some(logical_address)）、:27 fn on_flush_internal 以
   Option<u64> 分流 :41-44：Some → log_flush_path，None → bare_flush_path。
2. 生产消费点唯一：wkv/src/store/flush.rs:84 调 on_flush_address(user_key, stub, record_addr)，
   全仓 src 内 on_flush 裸名入口零调用者；仅测试在用
   （wbftree/tests/manager_and_stub/manager.rs:44、:234、:265，另 :94/:131/:568/:569 走 on_flush_address）。
3. 裸名工件族（随 None 臂一起失去来源）：wbftree/src/manager/mod.rs:452 pub fn bare_flush_path；
   消费方 flush.rs:43、lifecycle.rs:119（建树时删旧裸名件）、lifecycle.rs:149（惰性恢复取裸名件）；
   恢复契约与「裸名优先于地址扫描」的论证见 lifecycle.rs:152-161。
4. 文档牵连：wbftree/src/lib.rs:43、manager/mod.rs:192/:197/:310/:451、
   manager/lifecycle.rs:154-160 均以 on_flush / 裸名件为叙述前提。

C# 参考
1. libs/server/Resp/RangeIndex/RangeIndexManager.cs:681 internal void SnapshotTreeForFlush(
   ReadOnlySpan<byte> key, Span<byte> valueSpan, long logicalAddress) —— 逻辑地址是必填参数，
   刷盘文件名恒为 {prefix}.{addr:16}.flush.bftree（同文件 :901 EnumerateFlushFiles 的
   命名校验 FlushFileNameLength / AddrStartIndex / AddrHexLength 亦按带地址格式写死）。
2. 触发方 libs/server/Storage/Functions/GarnetRecordTriggers.cs:99
   rangeIndexManager.SnapshotTreeForFlush(logRecord.Key, logRecord.ValueSpan, logicalAddress)
   —— 与 wkv/src/store/flush.rs:84 同位，永远带地址。
3. C# 全仓无裸名 flush 文件形态（grep FlushSuffix 的产生点均带地址段），故裸名臂属 rust 侧多出来的
   第二套命名约定。

修法
1. 删 flush.rs:13 on_flush，on_flush_internal 的 logical_address 形参由 Option<u64> 收敛为 u64
   （或直接把体并入 on_flush_address，只留一个 pub 入口），并在其文档注释写明
   「1:1 对标 SnapshotTreeForFlush(key, valueSpan, logicalAddress)」。
2. 删 mod.rs:452 bare_flush_path 与 lifecycle.rs:119 的裸名清理、lifecycle.rs:149/:152-161 的
   裸名优先分支：恢复期一律走带地址扫描（addr_flush_scan_pending 门控与最大地址胜出逻辑保留）。
3. 测试改判：manager.rs:44/:234/:265 三处改为 on_flush_address 并断言带地址工件；
   若某用例的立论本身就是「无地址刷盘」，该用例属 C# 无对应的自研形态，删用例（spec：清理 C# 没有的测试）。
4. 同步订正 lib.rs:43、manager/mod.rs:192/:197/:310/:451、lifecycle.rs:140-161 的注释叙述，
   禁留「两种命名互斥、裸名优先」这类已失效契约。

边界
与 task/ing/wbftree-flush-file-enumerator-single-source.md 同文件族（replication.rs / lifecycle.rs
的刷盘文件枚举）。先落本票（少一处枚举点与一种命名），再做枚举器收敛，避免同一循环改两遍。

验收判据
1. 全仓 grep 对 RangeIndexManager::on_flush（词界，不含 on_flush_address）与
   RangeIndexManager::bare_flush_path 零命中；on_flush_address 为唯一刷盘入口且
   其调用面（wkv/src/store/flush.rs）签名不变。
2. wkv::RangeIndexStub 的 IsFlushed 判定与 wkv/src/store/flush.rs 刷盘链行为不变
   （带地址工件路径与 addr_flush_scan_pending 门控仍在）。
3. cargo check 通过（禁跑 test.sh / clippy.sh）；js/check.js 对 SnapshotTreeForFlush 的
   锚点仍单点挂在 on_flush_address 上。

双花登记
并发代理的同号薄票 next/db-bftree-on-flush-single-entry.md（自称并入 next/muse.db.md 条 11）已由
主仓 commit 0f7ce71 作「双载体薄壳」删除，载体统一为本票，故本票为该题唯一正文，无对手票待删。
判词一致留档：双方均判 on_flush（无地址包装）与 bare_flush_path 属自造变体，C# 只有带地址签名。

落地（判词：成立，已删，dev FF e7d998e；改动 8 文件 +65/−95）
步骤 0 复核（票 cite 的行号随前一棒 enumerator 合入漂移，现刻 HEAD=ab62b98 实测）
- 主张 1 双入口：flush.rs:13 pub fn on_flush → on_flush_internal(.., None)、:18 on_flush_address →
  Some(..)、:27 on_flush_internal 以 Option 分流 :41-44（Some→log_flush_path，None→bare_flush_path）。成立。
- 主张 2 零生产调用：Grep 词界 `\bon_flush\b` 全仓 src 仅命中 flush.rs:13 定义处；消费面唯一
  wkv/src/store/flush.rs:84 on_flush_address(user_key, stub, record_addr)——record_addr 由
  :56 `page_start + offset` 恒可得，无「地址取不到需回落裸名」的写侧缺口（非功能缺口，判死成立）；
  裸入口读者仅 wbftree/tests/manager_and_stub/manager.rs:44/:234/:265。成立。
- 主张 3 裸名工件族：mod.rs:452 pub fn bare_flush_path，消费者 flush.rs:43、lifecycle.rs:118（建树删旧裸名件）、
  lifecycle.rs:148（惰性恢复取裸名件）+ :152-161 的「裸名优先于地址扫描」论证。成立。
- C# 侧：RangeIndexManager.cs:681 SnapshotTreeForFlush(key, valueSpan, long logicalAddress) 必填、
  :300 LogFlushPath 恒 `{hashPrefix}.{logicalAddress:x16}{FlushSuffix}`、:909 EnumerateFlushFiles 按
  FlushFileNameLength 定长校验收口（裸名件根本进不了枚举），触发方 GarnetRecordTriggers.cs:99 永远带地址。
  读侧复核：C# 全仓 LogFlushPath 消费点（:526 PreStage / :695 快照 / :872 OnTruncate / :986 复制枚举）
  无一按裸名取件，恢复期无「裸名优先」形态。

改动
1. flush.rs：删 on_flush 与 on_flush_internal 的 Option 分流，体并入 on_flush_address（唯一 pub 刷盘入口，
   形参 logical_address: u64），命名恒走 log_flush_path；notice_addr_flush_files 由 `if is_some()` 改无条件调用
   （与原 Some 臂逐路径等价，transferred / 工作文件缺失两处 early-return 位置未动，故零 notice 口径不变）。
2. mod.rs：删 bare_flush_path；log_flush_path 文档标注为刷盘件唯一命名形态；addr_flush_gen 字段文档的
   「未接线 on_flush 的常态部署」叙述改指宿主刷盘链。
3. lifecycle.rs：删 create_bftree_internal 的裸名件 unlink（带地址世代清理仍走 remove_addr_flush_files）、
   删惰性恢复裸名优先分支与其「两种命名互斥、裸名优先」契约论证；恢复期一律走
   `backend==Disk && !is_recovered && addr_flush_scan_pending()` 门控的 flush_files 带地址扫描，
   最大地址胜出 / 单次 copy / 证伪封存（settle_addr_flush_scan）逐字保留，未新增亦未复活任何裸名枚举。
4. tests/manager_and_stub/manager.rs：三处裸名夹具改判 on_flush_address 并断言带地址工件
   （:44→0x1000、冷树两用例→0x900/0xA00，断言由 `format!("{}.flush.bftree")` 改走 log_flush_path），
   两用例名随之改 test_on_flush_address_*；test_create_purges_old_generation_flush_artifacts 去掉裸名
   造件与断言、只留带地址世代。无「立论即无地址刷盘」的用例需要删除——三处立论（生命周期+检查点往返、
   冷树复制工作文件、工作文件缺失拒绝置位）在带地址口下同样成立。
5. 文档订正：wbftree/README.md:41/:87、readme/en.md:29、readme/zh.md:29 的惰性恢复条目去掉
   「裸名优先，否则取最大地址」，改述「命名仅带地址一种形态、最大地址胜出」并把 on_flush 改 on_flush_address。

验收实测（合并后 dev HEAD 复核）
- 验收 1：`git grep -E "\bon_flush\b"` 于 wedb 零命中（exit 1）、`bare_flush_path` 零命中；
  wbftree/src/manager 内 `pub fn on_flush*` 仅剩 flush.rs:18 on_flush_address；
  wkv/src/store/flush.rs 未被本棒触碰、:84 调用口签名不变（user_key, stub, record_addr）。
- 验收 2：IsFlushed 判定链未动（set_flushed 两处位置与 transferred/不变量拒绝置位口径逐字保留）；
  带地址工件路径与 addr_flush_scan_pending 门控仍在（lifecycle.rs:161-189）。
  cargo nextest run -p wbftree 135/135 通过（含改判的 test_range_index_manager_lifecycle_and_checkpoint、
  test_on_flush_address_cold_tree_copies_data_file、test_on_flush_address_missing_data_file_keeps_stub_unflushed、
  test_create_purges_old_generation_flush_artifacts、test_flush_file_name_strict_parsing、
  test_manager_truncate_reclaims_flush_files）；宿主刷盘链 cargo nextest run -p wkv --test main range_index
  18/18 通过（含 test_ri_flush_and_evict_cold_read、检查点/刷盘恢复用例）。
- 验收 3：cargo check --tests -p wbftree -p wkv 两轮（合 dev 前后各一轮）零警告零错误；
  按 js/check/rustScan.js 的 CS_REF_REGEX 复刻比对（基线 ab62b98 对 HEAD）：lifecycle.rs / mod.rs /
  replication.rs / lib.rs / tests 的 `File.cs:Fn` 锚点集合逐枚相同，唯一增量是 flush.rs 的
  SnapshotTreeForFlush 3→4（票面修法 1 要求在该入口文档写明对标签名而新增一枚，挂在 on_flush_address 上），
  删除的 on_flush 从未携锚，映射登记无减。
- 本棒门禁：CARGO_TARGET_DIR=/tmp/ct-onflush 私有 target；未跑 ./test.sh、./sh/clippy.sh；
  cargo fmt -p wbftree 归一后 rustfmt 干净。

射程与留手
- replication.rs 只改 remove_addr_flush_files 的文档分工说明（原「裸名工件由调用方 O(1) 直删」随裸名族
  一并失效），flush_files 枚举器与 parse_flush_file_name 机制零动（前一棒 1ddf655 成果保持）；
  枚举器文档里把「裸名刷盘件」列为外来文件的举例（replication.rs:37/:252）按现状保留——它描述的是
  严格解析跳过外来名的容错面，不是命名契约，且旧世代残件在盘上仍可能出现。
- 链W 第三棒 wbftree-chunk-serializer 未开棒，本棒未触 chunk.rs / 分块序列化面。
- 双花：next/db-bftree-on-flush-single-entry.md 薄壳已由主仓 0f7ce71 删除，本票为唯一正文，无对手票待删。
