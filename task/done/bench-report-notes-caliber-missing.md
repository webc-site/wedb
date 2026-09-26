甄别结论：通过（甄别席 zc-fix-r16-benchnotes，2026-09-26）定级 P2
真实性核验：zh.yml:40 notes 现文仅读侧中位数/缓存统一/fsync 口径，确无 len 语义与写侧单次计时注记，成立；rocksdb_engine.rs:206-210 iterator_opt(Start).count()、sqlite_engine.rs:202-207 SELECT COUNT(*) 为 O(N)，成立；redb_engine.rs:156-161 ReadableTableMetadata::len、wbftree_engine.rs:184-186 len_counter AtomicU64 load 为 O(1) 直读，成立；fjall_engine.rs:201-203 txn.len 经上游 fjall-3.1.10/src/readable.rs:243-252 亲验确为迭代计数，成立；harness.rs 第 1/2/3/4/9 节（:85/:105/:125/:149/:321）写侧均单次 Instant 无预热无重复采样、仅第 6/7 节读侧取中位数，成立；data/latest.json len 行在架混排 rocksdb 1783ms/fjall 1579ms/sqlite 64ms/wkv 28ms/redb 1µs/wbftree 0µs，横比失真危害属实；C# 锚 KV.benchmark/Options.cs:58-60 warmup-sec 默认 5 秒且丢弃成立，Embedded.perftest EmbeddedPerformanceTest.cs:116 DBSIZE 工况在架成立。
被推翻锚一枚：wkv entry_count 非「标量 O(1)」——wkv_engine.rs:227-229 转发 wkv/src/store/addr.rs:15 entry_count，系哈希索引逐桶逐链扫描计数（对标 Tsavorite GetEntryCount），latest.json wkv len 28ms 与 redb 1µs 差四量级互证；执行时注记文案必须改为三组归类（全表/LSM 迭代：rocksdb/sqlite/fjall；内存索引扫描：wkv；元数据计数直读：redb/wbftree），否则注记自身引入新失真。
非重复非灭失：deviations.md 为 C# 行为偏差台账，无 bench notes 口径覆盖条目；其余五张 bench-* 票（timeout/read-loop/alloc-tax/wbftree-flush/parity-gap）各不同轴，ing/done/reject 池无同案；缺陷现状仍在。
架构合规：仅补 zh.yml notes 与发布页注记文案，复用既有 notes 机制，零代码改动零新依赖，无过度设计，符合 transpile/rust_review。
可执行度与格式：改动点具体，验证锚 js/check.js 于仓根在位（yml 解析校验），路径为根相对非 bench/js；纯文本、双侧代码路径齐全。

审核结论：通过，定级 P2。
确证发布页 len 行混排 O(N) 全表扫描（sqlite/rocksdb/fjall）与 O(1) 元数据直读（redb/wkv）两种语义，且写侧单次计时无预热口径缺注记。方案在 zh.yml 与发布页补齐注记，方案正确。
复核（zcode-r18-review-benchmisc）：锚点全部亲验成立。zh.yml:40 notes 现文仅覆盖读侧 3 次中位数、缓存统一与 fsync 口径，确无 len 语义与写侧单次计时两项注记；六引擎 len 现码分类逐点亲验：rocksdb_engine.rs:206-210 iterator_opt(Start).count() 迭代全表、sqlite_engine.rs:202-207 SELECT COUNT(*)、fjall_engine.rs:201-203 txn.len 为 LSM 迭代计数，三者为 O(N)；redb_engine.rs:156-161 ReadableTableMetadata::len 元数据直读、wkv_engine.rs:227-229 entry_count 标量为 O(1)；data/latest.json 实况 len 行 1783ms（rocksdb）至 1µs（redb）混排在架，横比失真危害属实。harness.rs 第 1/2/3/4/9 节写侧均单次 Instant 计时、无预热丢弃窗、无重复采样（仅读侧第 6/7 节有 3 次中位数），口径描述属实；C# 侧 Options.cs:58-60 warmup-sec 默认 5 秒且从结果丢弃，对齐锚点真。
门槛裁定：不并入 deviations.md。deviations.md 为与 C# 的行为偏差台账，本案是自身发布测量口径注记缺失，既定落点就是 zh.yml notes（已有中位数与 fsync 注记先例），补注属既有机制维护，且 r14 第 8/9 条复查未整改有案，够独立立案门槛。
执行方案补强：len 分类须补 wbftree——其 len 为 len_counter AtomicU64 标量直读（wbftree_engine.rs:184-186），归 O(1) 元数据直读类，与 redb/wkv 同组；注记文案建议直接点名三组引擎归类，禁只写部分留下新歧义。

bench 发布页缺 len 行语义差异与写侧单次计时口径注记（r14-bench 第 8/9 条 P2 复查未整改）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 侧 DBSIZE 类计数为元数据 O(1) 口径（garnet/playground/Embedded.perftest/EmbeddedPerformanceTest.cs 的 DBSIZE 工况直读元数据）；C# KV.benchmark 各计量段均带 warmup-sec 丢弃窗口（garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/Options.cs 的 warmup/run 分窗），口径在输出侧可追溯。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   bench/js/i18n/zh.yml notes（约 :40）已声明读侧中位数与持久化口径，但缺两项：其一，len 行语义差异未标注——rocksdb len 为迭代全表计数（bench/bench/src/engines/rocksdb_engine.rs:RocksdbReadTxn::len 约 :206-210）、sqlite 为 SELECT COUNT(*) 全表扫描、fjall 同为迭代，而 redb 为元数据直读、wkv 为 entry_count 标量，同一行混排 O(N) 扫描与 O(1) 元数据两种语义，发布页按倍数宣传「最快 vs 最慢」实为语义差异非性能差异；其二，写侧各段（bulk_load/individual/batch/nosync/removals）均为单次计时、无预热、无重复采样，含首次页错误与冷缓存，该口径宽松未声明。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   对外评测页读者将 len 行 1800 倍差距误读为引擎性能差距，写侧行数字被当作可复现均值消费，发布数字可信度受损；属注记缺失，不改测量本身。

涉及代码：
rust 文件与函数：
bench/js/i18n/zh.yml:notes（口径注记行）
bench/bench/src/engines/rocksdb_engine.rs:RocksdbReadTxn::len（迭代全表）
bench/bench/src/engines/sqlite_engine.rs:SqliteReadTxn::len（COUNT 全表）

对应 c# 文件与函数：
garnet/playground/Embedded.perftest/EmbeddedPerformanceTest.cs:DBSIZE 元数据 O(1) 口径
garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/Options.cs:warmup/run 分窗计量口径

精炼执行方案：
1. zh.yml notes 与发布页 len 行补注：标明该行 rocksdb/fjall/sqlite 为全表扫描计数、redb/wkv 为元数据直读，跨语义倍数不可作性能结论（或对无 O(1) len 的引擎标 N/A）。
2. notes 补写「写侧各段为单次计时、无预热丢弃窗口」；若后续加丢弃式预热则同步更新注记。
3. 测试验证点：js/check.js 校验 i18n 键完整；人工核对发布页渲染后注记在表侧可见。

收口注记：合入哈希：87e3ffd 收口形态：zh/en yml notes 成组补注 len 三组计数口径与写侧单次计时，发布页 bench.md/bench.svg 按渲染链重生成补稳，纯注记面零计时逻辑改动
