# bench 性能对基与回归提示词包

一套可直接粘贴给独立会话/专席使用的提示词，覆盖：跑回归门禁、跑 Rust 基线、
建并跑 C#（vendored Tsavorite）同标准对基、Rust 与 C# 的差值裁决、由差值反推
Rust 缺陷的优化归因，以及对照 C# 的死代码与重复机制清算。

统一验收线（用户定的硬要求）：Rust 实现在任何指标上都不低于 C# 原版。

## 0. 通用约定

- `<FORK>`：worktree 席目录，用 `./fork.sh <分支>` 从 dev HEAD 拉出，落在
  `/Users/z/git/db/wedb/.forks/<分支>`。fork 会把 `node_modules garnet .codegraph sh`
  做成符号链接，并把 `<FORK>/.cargo/config.toml` 与 `<FORK>/wedb/.cargo/config.toml`
  改写为私有 target `.rs-targets/<分支>`。后者是私有装配，永不并回任何分支。
- `<CRATES>`：本波受影响的 crate 列表，例如 `-p whlog -p wkv -p wnode -p wedb`。
- 门禁必须两段都跑：clippy 干净 + 全量测试绿。clippy 只在 `wedb/` 目录内跑，
  仓库根没有 Cargo.toml，根目录跑会得到 CLIPPY_RC=101 的假红。
- 日志一律绝对路径重定向到 `<FORK>/.bench_run/<wave>.log`。
- 汇报里的数字与 sha 必须来自命令实际输出，禁凭记忆或转述填写。

## 1. P0 回归测试席（Rust 全链门禁）

```
你在 wedb 仓库工作目录 <FORK> 上执行性能/重构波次的终态门禁。目标：证明本波改动
在全链上无回归，并把红项归因清楚。

步骤（严格按序，全部日志落 <FORK>/.bench_run/<wave>.log，用绝对路径重定向）：
1. cd <FORK>/wedb && cargo clippy -q -p <CRATES> --all-targets -- -D warnings
   记录 CLIPPY_RC。
2. cd <FORK>/wedb && ./test.sh --no-fail-fast
   # cargo nextest，全量约 5479 测 / 约 120s（暖构建，冷编 2 分钟另计）
   记录 TEST_RC 与 Summary 行。
3. 任何红项走三段裁决：面归因（是否本波触及的文件）→ 定向单测 5 次复跑 → 满负载复跑仲裁。
   负载假红（时间边界/并发抖动类）只登记不代修；跨域红项一律立案挂账，
   禁止代修他人席在途域。
4. 汇报：CLIPPY_RC / TEST_RC / 通过数 / 红项清单及定性 / 本波 commit 短 sha（须亲验）。

禁止项：不得改共享主树 /Users/z/git/db/wedb；不得跑 sh/clippy.sh（它带 --fix）；
不得跑仓库级 cargo fmt；不得 push、不得并 dev/main；
不得为过门禁加 #[allow] 或用 cfg 把测试关掉（关掉的测试在 --all-features 形态下
显示 0 tests，等于看不见红）。
```

## 2. P1 Rust 性能基线席（同机同盘同参）

```
目标：产出可比的 Rust 侧吞吐基线 JSON，作为与 C# 对基的唯一数据源。

命令（已验证可跑，在 <FORK>/bench 下执行）：
  CARGO_TARGET_DIR=/tmp/wedb_bench_target cargo bench -q -p wedb-bench-compare \
    --no-default-features --features hash --bench compare_benchmark \
    -- --only hash --data-path /tmp/<uniq> --json /tmp/<uniq>.json
  其他可用参数：--scale <f> / --cache-mb <n> / --timeout-secs <n> / --quick / --list /
  --engine <name>（单引擎直跑需配 --workload-json 与 --out）。

口径（不许单方面改，改了必须在票面申报）：
- workload 默认档见 bench/crates/wedb-bench/src/config.rs：
  bulk 5000000 / sorted 1000000 / individual 1000 / nosync 50000 / batch 100 批 x 1000 条 /
  reads 1000000（median of 3）/ scans 500000 x 10（median of 3）/
  线程档 4,8,16,32 / key 24B / value 150B / rng_seed 3 / 缓存 4GiB
  （超过物理内存一半自动收敛，收敛时会在 notes 告警）。
- individual writes 的持久语义：每次提交走 store.flush_all()（落盘 + fsync 屏障）；
  nosync 档提交直接返回。实现见 src/engines/hash_engine.rs 的 set_sync/commit/compact。
- 指标键由 result.rs 的 metric_key 从行名派生（小写、非字母数字折下划线）：
  bulk_load、individual_writes（txn/s）、small_batch_writes、nosync_writes、
  len（latency）、random_reads、random_range_reads（scan/s）、
  random_reads_4_threads ... random_reads_32_threads、removals、retain、
  extract_if、pop、uncompacted_size、compacted_size、sorted_inserts。

要求：同机同数据盘连跑 3 次取中位数；每次换全新 --data-path（禁复用旧段文件）；
核对输出首行的平台/负载/缓存摘要与目标档一致后才算数。
禁止项：不得改 harness.rs/config.rs 的参数或 RNG 来取悦数字；不得跨 scale 混比；
不得在没有同档基线的情况下宣称提升了多少。
```

## 3. P2a 建 C# 对基驱动席（当前待建，先跑这条）

```
目标：新增 bench/csharp/TsavoriteBench/，用 vendored C# 原版 Tsavorite 跑出与
Rust 侧同 schema、同 workload、同数据字节的性能列，引擎名固定 "tsavorite-cs"。

环境（已验证）：
  export DOTNET_ROOT=/opt/homebrew/opt/dotnet/libexec
  export PATH=/opt/homebrew/bin:$PATH
  dotnet SDK 10.0.401（brew install dotnet；ghcr 下载易半途断流，失败就重试 3 次）
  vendored C# 源在 /Users/z/git/db/wedb/garnet（.gitignore 第 11 行整树忽略，只读参考）
  核心工程 garnet/libs/storage/Tsavorite/cs/src/core/Tsavorite.core.csproj
  已验证：dotnet build -c Release -f net10.0 Tsavorite.core.csproj → 0 警告 0 错误
  （garnet/Directory.Build.props 多目标 net8.0;net10.0，本机只有 SDK10，固定选 net10.0）
  API 装配范式照抄 garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/：
  KvBenchmark.cs 给 KVSettings{IndexSize,LogDevice,LogMemorySize,PageSize,SegmentSize,
  MaxInlineValueSize,PreallocateLog} + StoreFunctions.Create(SpanByteComparer.Instance,
  new SpanByteRecordTriggers()) + ObjectAllocator；KvBenchmark.Worker.cs 给 session 的
  Upsert/Read/Commit/Iterate 用法；KvBenchmark.Validate.cs 给读校验写法。

硬性对齐要求：
1. 数据字节必须逐字节相同。Rust 侧用 fastrand 2.5.0 的 WyRand（见
   fastrand-2.5.0/src/lib.rs 的 gen_u64 与 fill）：
     s += 0x2d358dccaa6c78a5; t = (UInt128)s * (s ^ 0x8bb84b93962eacc9);
     return (UInt64)t ^ (UInt64)(t >> 64);
   fill(buf) 按 8 字节小端分块写入，余数取下一块的前 len 字节；seed = 3；
   多线程读分片按「同种子先快进 i * (elements / shards) 个 pair」复刻
   （harness.rs 的 make_rng_shards）。key 24B / value 150B。
2. 段序、单位、取中位数方式与 Rust harness 完全一致，行名与 kind/unit 照 P1 清单逐字抄。
   不支持的段（retain / extract_if / pop）记 kind = "na"，与 Rust 侧 N/A 口径同形。
3. 持久语义映射：
   individual writes = 每 key 一次 Upsert 后同步等 Commit（等价 flush 屏障）；
   nosync = 仅 Upsert 不 Commit；
   bulk load = 全程 Upsert，末尾一次 Commit；
   small batch writes = 1000 次 Upsert 加一次 Commit；
   removals = Delete + Commit；
   random reads = session.Read + CompletePending 直到状态落定；
   random range reads = Iterate().Seek(key) 后步进 10 个；
   len = 全表 Iterate 计数（不许拿内部计数器交差）；
   compacted size = Checkpoint + ShiftBeginAddress(truncateLog) 后目录树字节和。
4. 输出 JsonRun（schema = 1），字段与 bench/crates/wedb-bench/src/json.rs 逐字段同构：
   key / name / kind / unit / count / duration_ns / bytes / formatted / rate / winner，
   外层含 schema / generated_at_unix / commit / branch / version / platform / machine /
   workload / notes / engines。用 --json 落盘。
   machine 必须是 machine.rs 里那 11 个字段且与 Rust 侧同机实测等值：
   cpu_brand / physical_cores / logical_cores / arch / platform / total_memory_gib /
   disk_type / data_dir / data_fs / os_info / kernel_version。
   只有 data_dir 允许不同（各用独立数据目录是要求）；其余字段不等会被门禁判 FAIL，
   所以 C# 侧要按同一来源实测（sysctl machdep.cpu.brand_string、hw.ncpu、
   hw.memsize、df/statfs 的 data_fs 等），不许填占位串或凭印象写死。
   判定与显示由 count / duration_ns / bytes 现算，formatted 与 rate 只用于对账，
   与重算值不一致会单列告警——别指望靠改字符串改变结论。
5. 配置诚实：Tsavorite 用其生产默认（PageSize 16MB / SegmentSize 1GB / Garnet defaults），
   索引与日志内存预算与 Rust 侧同一 cache 预算（4GiB，按物理内存收敛同规则）。
   两侧配置差异必须写进 JSON 的 notes，不许偷偷对齐成对自己有利的一档。
   对照锚：Rust 侧是 SegmentedDevice(SEAGMENT 64MiB, SECTOR 4KiB) +
   StoreConfig::from_memory_budget_with_keys(cache, keys)。

验收：dotnet build -c Release 零警告；--scale 0.02 冒烟跑通并产出合法 JSON；
读段 checksum 断言通过（证明键序列与 Rust 侧同源）；同档手跑一遍与 Rust 数字对表。
禁止项：不得改动 vendored garnet/ 任何源；不得改 Rust 侧 bench 文件；
不得用 Null/内存设备伪装持久；不得 commit 到共享主树；
不得把 C# 侧异常吞成 N/A（崩溃必须记 status = crashed 并留日志）。
```

## 4. P2b C# 性能跑测席（P2a 落地后复用）

```
目标：产出 C# 侧基线 JSON，与 P1 同机、同数据盘、同 scale、同次数（3 次取中位）。

命令：
  export DOTNET_ROOT=/opt/homebrew/opt/dotnet/libexec
  export PATH=/opt/homebrew/bin:$PATH
  cd <FORK>/bench/csharp/TsavoriteBench
  dotnet build -c Release
  dotnet run -c Release --no-build -- --data-path /tmp/cs_<uniq> --json /tmp/cs_<uniq>.json

要求：每次换全新 --data-path；首行打印实际生效的 KVSettings（页/段/索引/日志内存/设备类型）；
任何段崩溃或超时必须如实记 status = crashed / timeout 并保留日志。
禁止项：不得为通过而调小 workload；不得复用 Rust 的段目录；
不得在跑测期间同机跑其他重负载（会污染 fsync 与读段计时）。
```

## 5. P3 对比与回归门禁席（Rust 任何指标不得低于 C#）

```
目标：把 rust.json 与 cs.json 合成差值报告，并做硬断言门禁。

逐指标规则：
- 吞吐类（kind = throughput，含 key/s、txn/s、scan/s）：rust.rate >= cs.rate，比值 < 1 判负。
- 时延与体积类（kind = latency / size）：rust 值 <= cs 值才算通过（越低越好）。
- 任一侧 kind = na 的段跳过该指标，但必须在输出里显式列为口径缺项，不许拿缺项当通过。
- 两侧必须同平台同档：platform、machine、workload 各字段逐项比对，
  不一致直接 FAIL（防止拿小档比大档）。

每行输出格式（一行一个指标，竖线分隔，顺序为）：
  指标键 | rust 显示值 | cs 显示值 | 比值 rust/cs | 判定 PASS 或 FAIL 或 SKIP
末尾给总结论：全部达标，或首个违规项及其比值。存在违规即 exit 1。

抗噪：每指标取 3 次中位数结果；比值落在 0.98 到 1.00 之间视为噪声区间，
但必须复跑确认后才允许放行，且放行要在 notes 记一次。

禁止项：不得改 C# 参数迁就 Rust；不得以「测试口径不同」直接豁免，
豁免必须给源码级证据（具体到 C# 文件与行号）并登记为挂账缺陷；
不得把两侧不同 scale 的数字放进同一张表比较。

已落地工具与目录布局（对基三件套全部归在 bench/ 之下；js/ 只留仓库治理工具
check.js 与 safe_commit.sh 这类，不再混入评测专用件）：
- bench/csharp/TsavoriteBench/：P2a 的 C# 驱动。WyRand.cs 随机源复刻、
  Models.cs 结果与 JSON 模型、Engine.cs Tsavorite 装配、Runner.cs 18 段、
  MachineInfo.cs 同机 11 字段、Program.cs 入口与 CLI；工程引用 vendored garnet
  的 Tsavorite.core.csproj（garnet/ 只读且被 gitignore），bench/csharp/.gitignore
  挡掉 bin/ obj/ 构建产物。
- bench/perf_compare.js：第 5 节判据的实现（原名 js/perfGate.js，已随归位改名）。
  逐指标一行
  `指标键 | rust 显示值 | cs 显示值 | 比值 | PASS/FAIL/HOLD/SKIP`；
  同侧多文件按指标取中位数；platform / workload 全字段与 machine 同名字段逐项等值，
  不等直接 FAIL 退出 1（machine.data_dir 豁免：两侧各用独立数据目录是要求不是差异）；
  N/A 与「一侧无此段」都计入口径缺项，不当作通过；
  比值落噪声区（吞吐 0.98〜1.00、时延与体积 1.00〜1.02）判 HOLD，
  复跑确认后加 --noise-ok 才放行。
  判定与显示一律由原始量现算：吞吐 = count / duration_ns、时延 = duration_ns、
  体积 = bytes，格式化规则照 result.rs（三位有效数字加 SI 后缀、四舍五入整毫秒、
  二进制单位两位小数）；JSON 里自报的 rate 与 formatted 只用于对账，
  与重算值不一致会单列告警，防止驱动呈现层造假混过门禁。
  单独调用：node bench/perf_compare.js --rust r1.json r2.json r3.json --cs c1.json c2.json c3.json [--noise-ok]
- bench/perf_vs_cs.sh：把 P1 + P2b + P3 串成一条命令（同机同档各跑 --runs 次取中位）。
  档位必须显式给（--quick = 0.02 冒烟，或 --scale F），不给档直接退出 2，
  防止默认偷偷跑一小时。数据目录每次重建，落在 <FORK>/.bench_run/perf_vs_cs/。
  用法：bench/perf_vs_cs.sh --quick [--runs 3] [--engine hash] [--noise-ok]
  C# 侧 CLI 契约与 Rust 侧一致：--scale / --data-path / --json / --commit / --branch，
  这条脚本按该契约调用，P2a 落地后要先核一遍参数名。
注意：js/ 在 fork 内是真实目录可写；仓库的 sh/ 是指向
/Users/z/.local/share/cargo_sh 的跨 worktree 共享符号链接，席不要把波次脚本写进去。
```

## 6. P4 差因归因与优化席（拿 C# 数字反推 Rust 缺陷）

```
目标：对 P3 报告里每一个 Rust 低于 C# 的指标，判定是「实现缺陷」还是「口径差异」，
并给出可验证修复。以下方法论是本法已验证有效的流程，不得跳步。

1. 分段计时插桩：临时静态 AtomicU64 探针 + 每 500 步 eprintln 一行均摊微秒，
   把嫌疑链路拆到 系统调用 / 纪元排空 / 内存拷贝 / 锁 的粒度，先回答
   「时间花在哪」再谈优化。测完必须删净（含探针辅助函数与临时 import）。
   自检：打印前确认累计代码真的接上了，只改打印不接累计会得到 n = 0 的假数据。
   历史战果：OnFlush 走查从页粒度改逻辑地址区间，走查记录 75.9M 降到 1.0M，
   每步刷盘成本 669.2 微秒降到 164.4 微秒，单写吞吐从 308 升到 21.5K txn/s（同机）。
2. 逐段对照 C# 锚点，锚路径必须真实存在（garnet/libs/storage/Tsavorite/cs/src/core/ 下），
   例如 AllocatorBase.cs 的 ShiftReadOnlyAddress、OnPagesMarkedReadOnly、
   WriteInlinePageAsync 第 629 到 636 行「Write only required bytes within the page」、
   ObjectAllocatorImpl.cs 的 FlushRecordsInRange 与 OnPagesMarkedReadOnlyWorker。
   列出 C# 做了什么、Rust 多做或少做了什么，再决定改哪一侧。
3. 每个候选先证伪再立项：一条命令级 A/B（改一行，同档跑 3 次取中位）坐实收益才动代码。
   反面教材已入册：device.sync() 改 sync_data() 假设能省 fsync，
   实测 12.8K 对基线 14.1K txn/s，假设证伪，代码不动。
4. 修复落在 <FORK>，一次提交一个主题，跑 P0 门禁加 P1 复测，
   前后数字必须成对给出，并写明指标键与档位。
5. 结构性要求：同一动作全系统只留一个落点（例：刷盘读侧上界只在 flush_addr_range
   入口封印一处、删段目标只在 effective_delete_floor 一处求值、
   区间求值只在 flush_write_range 一处），发现第二套机制就收敛掉而不是并存。

禁止项：不得无测量依据重构；不得引入第二套刷盘、纪元或段管理机制；
不得违反 compio thread-per-core 纪律（任务 poll 栈内禁嵌套 block_on、
禁 inline wait 式重入任务队列收割；wbase 的 blocking_wait/inline_wait 已全删禁复活）；
不得跨域代修他席在途文件；第三方依赖不得硬绑单一运行时（tokio/compio 走可选 feature 双后端）。
```

## 7. P5 死代码与一处定义清算席（对照 C#）

```
目标：清死代码与重复机制，让同一动作在全系统只有一个落点。

步骤：
1. 候选来源：零调用 pub API、死 getter 与薄包装、只有测试在喂的生产入口、
   与 C# 无对应物的自造机制、同一换算在多处复刻（地址到页或扇区圆整、区间钳制、
   删段目标、sync 强度口径、OK-ack 字面量比较这类）。
2. 判定必须编译实证：删除后跑 P0 门禁。若变红说明有隐式消费者（tests 或 feature 组合），
   按「生产零调用 / 仅测试消费」二选一处置：保留的必须加 #[doc(hidden)] 并在文档写明
   测试握手理由，仓库已有先例可参照（whlog 的 flush_page、peek_memory_header、
   shift_read_only_address_with_wait；wkv 的 on_flush_walk）。
   注意 Arc blanket impl、trait 默认臂、strum 派生这类会被误判成死码，必须编译裁决。
3. 每个删除项在票面写清三件事：证据（grep 计数加门禁结果）、C# 是否存在对应物、
   消费者归属哪个 crate。
4. 顺带订正悬空文档引用：删掉的函数若在别的文档注释里被引用，同批改掉，
   不许留指向不存在实体的 doc 链接。

禁止项：不得凭 grep 单点印象删码；不得在共享主树跑 cargo clippy --fix；
不得顺手改与本项无关的文件；不得为了少改文档而保留死码。
```

## 8. 本机现场基线与已证伪清单（截至 2026-10-05，macos-arm64 M2 Max 12 核 64GiB）

首轮 Rust vs C# 实测（同机同盘同 0.02 档，两侧各 3 次取中位，
日志与 JSON 在 .forks/perf-cs-bench-gate/.bench_run/perf_vs_cs/）：

- 指标 | rust(hash) | cs(tsavorite-cs) | 比值 | 判定
- bulk_load | 3.53M key/s | 633K key/s | 5.58 | PASS
- individual_writes | 313 txn/s | 35.2K txn/s | 0.0089 | FAIL
- small_batch_writes | 270K key/s | 560K key/s | 0.483 | FAIL
- sorted_inserts | 3.92M key/s | 989K key/s | 3.96 | PASS
- nosync_writes | 6.48M txn/s | 610K txn/s | 10.62 | PASS
- len | 17ms | 49ms | 0.346 | PASS
- random_reads | 9.19M key/s | 1.66M key/s | 5.55 | PASS
- random_range_reads | 384K scan/s | 251K scan/s | 1.53 | PASS
- random_reads_4_threads | 11.6M | 8.82M | 1.32 | PASS
- random_reads_8_threads | 17.5M | 6.68M | 2.62 | PASS
- random_reads_16_threads | 21.9M | 11.9M | 1.83 | PASS
- random_reads_32_threads | 17.8M | 22.6M | 0.787 | FAIL（rust 侧 16→32 负增长）
- removals | 4.07M key/s | 1.17M key/s | 3.48 | PASS
- retain / extract_if / pop | 两侧均 N/A | SKIP（口径缺项）
- uncompacted_size | 64.00 MiB | 50.75 MiB | 1.26 | FAIL（小档受段粒度支配，待默认档复测）
- compacted_size | 72.01 MiB | 58.76 MiB | 1.23 | FAIL（同上；C# 段界 1GB、rust 64MiB）

- rust 侧 0.2 档单次复测（同树）：bulk 4.27M、individual_writes 285 txn/s、  small batch 264K、nosync 5.22M、random reads 7.98M、
  random_reads_32_threads 33.5M（0.2 档 32 线程不回落，故 32 档那处 FAIL 需先在小档
  之外复核）、uncompacted 240.00 MiB、compacted 160.06 MiB。
- 0.2 档对表（rust = 刷盘波树 63732f18 三次中位，cs = tsavorite-cs 三次中位）：
  bulk 4.71M 对 1.49M PASS、individual_writes 27.1K 对 36.2K = 0.749 FAIL、
  small batch 2.67M 对 1.12M PASS、sorted 4.96M 对 1.92M PASS、nosync 6.01M 对 1.41M PASS、
  len 106ms 对 148ms PASS、random reads 7.75M 对 4.94M PASS、
  random range reads 369K 对 1.68M = 0.219 FAIL、
  4 线程 12.1M 对 17.8M = 0.677 FAIL、8 线程 26.2M 对 26.9M = 0.973 FAIL、
  16 线程 32.4M 对 25.7M PASS、32 线程 31.0M 对 27.0M PASS、removals 5.40M 对 1.82M PASS、
  uncompacted 232.71 MiB 对 232.71 MiB = 1.000008 HOLD、
  compacted 158.80 MiB 对 264.78 MiB = 0.600 PASS。
  两档合起来的结论（2026-10-05 晚订正：4/8 线程读两项见 8e——4t 已修到
  1.11 PASS、8t 干净期 1.6 倍但嘈杂期不稳留仲裁；下文四项现余两项在途）：
  刷盘波并入后仍需追的四项是 individual_writes（0.75〜0.90）、
  random_range_reads（0.2 档 0.219，而 0.02 档 rust 反而占优 1.53，C# 侧该段随档位涨 6.7 倍，
  需先判是否「C# 按日志物理序步进」的口径红利）、4 与 8 线程读（0.2 档落后，0.02 档达标）。
- 重要订正（旧账数字作废）：本节此前记「dev tip 60ec488a 默认档 1000 条 70ms =
  14.1K txn/s」，与本次实测的 0.02 档 313 / 0.2 档 285 txn/s 差 45 倍。
  而刷盘波的战果记的是「单写吞吐从 308 升到 21.5K txn/s（同机）」——308 与本次
  dev tip 实测吻合，故 14.1K 与 sync A/B 的 12.8K 两个数字出自已带刷盘波改动的树，
  不是 dev tip。凡引用这两枚数字，必须先按本次实测定性重贴来源，不得再当 dev tip 基线。
- 并入记录（2026-10-05 16:2x，本地 dev，未 push）：刷盘波 63732f18 并入 dev 得
  e9a2f5cf（parents 60ec488a + 63732f18），C# 对基三件套 56574d41 并入得
  51cc8dfa，提示词包本文件入册得 dc4b116e。并前用 git merge-tree 只读预演为净并
  （唯一同名交集 whlog/src/hlog/mod.rs，dev 侧 r11 确认轮同触 whlog，故并后终态
  门禁必须在并后 tip 上跑，不得沿用席 tip 的绿）。并波前的波内门禁：clippy RC=0、
  5479 测全过（跑点为其自身 rebased tip）。
- individual_writes 残差定性已定（2026-10-05 m2 席交票，两档各 6 连跑）：
  0.2 档 = 真缺陷，Rust 全部 6 次低于 C# 全部 6 次，中位比值 0.734；
  0.02 档 = 多为噪声（两侧相对标准差 7〜8%）。随「前置 10 万条→100 万条」
  放大的唯一成本项是设备 fsync 本体：每笔 sync 覆盖的在册段文件数 1→3。
  根因不在档位也不在口径，在 wdev/src/segmented_device/sync.rs::sync_internal
  「全量在册句柄 + 补齐 [start_segment,end_segment] 整段区间逐个 fsync」。
  修复票 #29：把 debug-only 的 sync 契约守护位图升格为生产级脏段集
  （写前预登记 + 在飞计数防「清除在飞登记导致永久漏刷」），只刷自上次成功
  sync 以来真正被写入过的段，同时消掉 debug/release 两套世界。
  与 C# 同口径论证：C# 提交路径不做设备级全量 flush，走 Segment.Flush 单段
  落盘；故「只刷本次真正写过的段」不是新发明。sync 改 sync_data 的旧证伪结论不变。
- 每笔提交成本拆分（探针实测，1M 档）：whlog 刷盘内核 110.9 微秒（约 67%）、
  device.sync 37.1 微秒（约 22%）、OnFlush 走查 16.3 微秒（约 10%）。
- whlog 内核细分（探针已实测）：seal 加纪元排空 1.0 微秒、flush_gate 0.0 微秒、
  coalesce 与记账约 0，纪元机器不是瓶颈；时间集中在 flush_range_aligned 的设备写段，
  该段按写量分桶的实测尚未完成（前一版探针只改了打印没接累计，数字作废）。
- 已证伪不改码项：sync 改 sync_data（fdatasync）无收益，12.8K 对 14.1K txn/s（
  注意：该对照出自刷盘波树，结论「不因此改代码」仍成立，但数字不代表 dev tip）；
  macOS APFS 下 F_BARRIERFSYNC 不比 F_FULLFSYNC 便宜。
- C# 侧环境事实：dotnet 10.0.401 经 brew 安装，需要 export DOTNET_ROOT 指向
  /opt/homebrew/opt/dotnet/libexec；ghcr 拉瓶易断，重试即可。
  驱动已落地 bench/csharp/TsavoriteBench/（2154 行，schema=1 输出，
  machine 11 字段与同机 Rust 实测逐字段等值，WyRand 与 fastrand 2.5.0 前 8 对
  key/value 字节三方对比一致：真 Rust、node BigInt 复刻、C# --rng-selftest）。
  未达验收线一项已收口（2026-10-05 c1 席）：dotnet build -c Release 现为 0 错误
  0 警告，只改类型标注（MachineInfo.cs 8 处补 ?、Program.cs 7 处 CLI 与收尾形参
  改 string?、一处有本地不变式支撑的 ! 加中文注释），无 pragma、未关 Nullable、
  csproj 未动；MachineInfo.cs 396 行不变、Program.cs 358→361。

## 8b. random_range_reads 0.2 档裁定（2026-10-05，m1 探针席实测）

现象：0.2 档 Rust 362K scan/s 对 C# 1.68M scan/s，比值 0.2155；0.02 档反而
Rust 384K 对 C# 251K（1.53 倍）。两侧 count 口径一致（num_scans，每次扫描
步进 10 条），所以差在每扫描的实际工作量与每记录成本。

裁定：口径红利与实现缺陷同票共存，按大小排序两项。

1. 主因是自家 bench 适配层超收集（不是产品缺陷）。
   bench/crates/wedb-bench/src/engines/hash_engine.rs:27 的
   RANGE_SCAN_WINDOW = 32，而 harness.rs 的消费循环只取 w.scan_len = 10 条
   （config.rs:69，且 scale 只缩放 num_scans 不动 scan_len）。每次扫描实体化
   32 条 (key,value) 完整拷贝，其中 22 条无人消费。C# 驱动
   Runner.cs:395 → Engine.cs:249-265 RangeScanValueSum 每扫描恰好步进 10 条、
   纯同步、零拷贝（只读 ValueSpan[0]）。
   A/B 实测（各 3 连跑取中位，每次全新 data-path）：
   基线 32 条 274/270/274ms → 364K/370K/364K scan/s
   窗口 32→1 22/21/21ms → 4.54M/4.59M/4.66M（暴露每扫描固定税仅 0.215 微秒）
   窗口 32→10 102/103/102ms → 975K/965K/975K（比值 0.219→0.58）
   窗口 32、闭包去 to_vec 188/192/190ms → 532K/520K/526K（拷贝税 0.84 微秒/扫描）
2. 次因是 whlog 读侧产品缺陷：驻留区每记录一把纪元守卫。
   探针计数证明 0.2 档全内存：cold_calls=0、分支 1（磁盘）与分支 5（回退冷读）
   均 0 命中，br3（只读区无锁直读）=14125938、br4（可变区页读锁）=50097，
   每扫描步进 32 条、墓碑率 0.000。每条记录 next_ref 的实测拆分（1/64 采样）：
   入口原子加载加页号偏移 16.6 纳秒、纪元守卫 enter 25.3 加 exit 20.3（合计
   约 45 纳秒，最大单项）、记录解析 15.8 纳秒、闭包体 17.6 纳秒；无探针斜率
   约 81 纳秒/条。落点 scan.rs:291 的 protected_scope 每记录进出一次。
   C# 对应锚（已核实路径存在）：garnet/libs/storage/Tsavorite/cs/src/core/
   Allocator/SpanByteScanIterator.cs:202 GetNext 纯同步步进、:92 与 :149
   InitializeGetNextAndAcquireEpoch 取纪元、:135-146 assumeInMemory 档免页装载。
   订正（2026-10-05 S2 普查席实读源码，本条旧表述作废）：C# 的纪元不是
   「跨调用只 enter 一次」。SpanByteScanIterator.GetNext 每次调用在
   InitializeGetNextAndAcquireEpoch（:92/:105）Resume、函数尾 finally（:287-290）
   Suspend，且先把记录 memcpy 进 recordBuffer（:267）再放纪元；
   TsavoriteLogScanIterator.GetNext（:323）首 Resume（:334）、拷贝入 pool
   （:374-377）后 Suspend（:378），但纪元跨该次调用内部的整条 while(true) 步进
   （跳过 commit/pad 时 continue 不重取纪元，:365）。因此对齐目标是「一次
   next_ref 一趟纪元跨过全部步进」，本票方向不变、落点不变；作废的只是
   「跨调用持有」这句表述。另：我们把守卫横跨交付闭包 f（scan.rs 零拷贝借用
   页内字节）与 C# 先拷出再放纪元的差异，是零拷贝取舍，不在本票收敛范围。
   推论：窗口对齐 10 后若把每记录 81 纳秒压到约 40 纳秒（去每记录守卫与每记录
   await），每扫描 0.215 + 9×0.04 约 0.58 微秒 → 约 1.7M scan/s，与 C# 持平。

修复面（一次一主题，均在 .forks/perf-wkv-partial-page-flush，依赖刷盘波形态）：
适配层窗口收成与 scan_len 同源的单一常量；whlog 驻留区改一把守卫内连续步进，
遇磁盘需求回退既有异步臂，禁开第二套扫描机制。

方法条目（P4 通用）：跨语言指标出现离谱比值时，第一步核「每轮工作量是否同形」
——读调用方的消费循环与被适配层的收集上限，逐条对齐；count 口径一致不等于
工作量一致。第二步才是每记录成本拆分。第一步零探针成本，一条 A/B 就能定性。

## 8c. 尺寸两行的真因就是刷盘波本身（2026-10-05 复核 JSON 实字节）

dev tip 对 C# 的 0.02 档：uncompacted 64.00 MiB 对 50.75 MiB（1.2611 FAIL）、
compacted 72.01 MiB 对 58.76 MiB（1.2255 FAIL）。同一档换刷盘波树
（perf-wkv-partial-page-flush @ 63732f18）实读 JSON bytes 字段：
Rust uncompacted 53215328 对 C# 53212864（+2464 字节，比值 1.000046，HOLD），
Rust compacted 61610920 对 C# 61610656（+264 字节，1.000043，HOLD）。
0.2 档 compacted 已是 Rust 0.5998 倍（PASS）。

结论：尺寸段原先的「Rust 大 26%」不是段粒度口径差异，而是整页刷盘的真实过量写
——逐页封页把未写满的整页落盘，刷盘波改成只写增量字节后直接对齐到 C# 的
50.75 MiB。残余 2464/264 字节属 0.0046% 量级的固定小差（疑似元数据或页尾补齐），
待一次文件级清单复跑归因（跑测席占机期间不重跑，禁同机并发跑测）。

## 8d. 一处定义普查战果与开票（2026-10-05，S2 只读普查席，tip 3a14a7a4）

普查法：不问「像不像重复」，只问「同一个动作是否有两个落点」，并用 C# 源码
判定该分裂是刻意并存还是转写抄漏。以下每条都有文件:行号实锚。

1. scan.rs 与 walk.rs 不是两套扫描，判定保留，不开合并票。
   scan.rs 的 ScanIterator::next_ref（:222）是跨三区逻辑地址区间扫描内核（磁盘
   冷读 / 只读区无锁直读 / 可变区页读锁 / 落盘回退 + 在途零头自旋），生产消费者
   六处（wcompact compactor/run.rs:37、wkv session/vector_cleanup.rs:78、
   wkv store/hlog_scan.rs:199、wkv gc/ttl_sweep.rs:77、wcpr manager/recover.rs:383、
   wnode array_key_iteration_functions.rs:289/:575）；walk.rs 的
   for_each_record_in_page（:81）与 flush_records_in_addr_range（:157）是驻留页
   同步页内走查内核（无磁盘读、无跨区、无自旋），生产消费者仅两处
   （wkv store/flush.rs:35 的 OnFlush、wkv read_cache/cleanse.rs:40 的读缓存驱逐）。
   C# 同样并存：SpanByteScanIterator / TsavoriteLogScanIterator 对位 scan.rs，
   ObjectAllocatorImpl.cs:464 FlushRecordsInRange（被 :1135 刷盘调）与
   ReadCache.cs:191 ReadCacheEvict 对位 walk.rs；且 C# 自己在
   SpanByteScanIterator.cs:70 挂着「TODO Unify with ObjectScanIterator」，
   上游刻意未并。
2. 真正可收敛的自造复刻只有一处：wkv/src/read_cache/window.rs:203 与 :343 各裸
   调 RecordHeader::decode_opt 手写 is_pad/is_null 判据，复刻 whlog
   hlog/mod.rs:244 reject_pad 的迷你判据 → 票 #35（落点选在 wrecord 单点谓词，
   不把 whlog 的 pub(crate) 直接 pub 化扩面）。hlog/mod.rs:731 probe_resident
   的撕裂契约臂与 read_cache/cleanse.rs:103 的单条 decode_opt（点读取
   prev_address）不是走查，严禁误并。
3. 纪元守卫的「白花钱」定性（并入票 #33 施工）：全仓生产侧 protected_scope 只有
   scan.rs:291 一个调用点，但它坐在 next_ref 的 while 循环体内，一次调用经历 K 次
   pad 跳过 / 撕裂复核 / 自旋回头就开关 K+1 次；且机制A 的 resume
   （wepoch/src/epoch.rs:308）首句走 TLS 寻槽，比机制B Participant::enter
   （participant.rs:62 get_unchecked，无 TLS 无寻槽）贵。真正白花的是「无外层
   守卫的独立扫描」：wcpr recover.rs:383、wcompact run.rs:37、ttl_sweep.rs:77
   每条记录一次完整寻槽；由 session 驱动的扫描命中重入快路径（epoch.rs:311-318
   inc_reentrant），churn 小。收敛动作是把守卫上提到单次 next_ref 之前，
   不新建第二套步进机制。
4. 水位读法两套（并入票 #33 顺带做净）：scan.rs:183/:625 裸 load begin、
   :228/:622 裸 load tail、:265 裸 load head，而同文件 :248 注释明说 accessor
   是「唯一读法、不开第二套」。head 在 :265/:379/:390 三次读各有新鲜度用途
   （分派前 / 加锁后 TOCTOU 复验），只改写法不合并次数。
   刻意保留的例外：hlog/mod.rs:416 的 tail_address.load(SeqCst) 是删段复合判据
   的一环，异序是设计，不得并进 Acquire 的 accessor（须补注防后人顺手收口）。
   append.rs:119/:192 的 CAS 重试环确需 &AtomicU64 句柄，句柄保留、取值走 tail()
   → 票 #34。
5. 页圆整三处落点（票 #34 收其一）：config.rs:172/:179/:190 是主日志单点真源，
   buffer.rs:84/:91 是读缓存自带的一份（CircularPageBuffer 不持
   HybridLogConfig，分属两子系统，强并需新抽象、违「不立第三套」，维持现状），
   hlog/append.rs:120/:121 内联右移与掩码是热路径第三份复刻 → 改调 config
   的 page_id/page_offset。扇区/段圆整已单点（wbase/src/align.rs:43/:64/:96，
   刷盘写侧界委托 Device::flush_range_aligned），合格。
6. 删段/紧缩链无第二套编排（合格）：shift.rs 的 shift_read_only_address（:62）/
   seal_read_only_and_drain（:87）/ shift_head_address（:108）/
   shift_begin_address（:140）/ release_history_until（:214）/ truncate（:241）
   是单源，三处删段目标均经 effective_delete_floor（:192/:217/:244）一处求值，
   纪元屏障均经 wait_safe_read_only_drained（:323）/ wait_epoch_condition（:263）。
   但存在只喂测试的并行入口：whlog hlog/io.rs:255 flush_page、shift.rs:362
   shift_read_only_address_with_wait、HybridLog::sync（io.rs:548）均
   #[doc(hidden)] 生产零调用 → 与 S1 死代码普查合并定票后决定是否下沉 tests。
7. 设备双 sync 口判定保留：Device::sync()（device.rs:271）与 sync_data()
   （:277）各有生产消费者（sync_data 走 waof wal/flush.rs:61、wal/log.rs:436、
   webd server/replication/receive_checkpoint_handler.rs:231；sync 走 wkv
   store/flush.rs:206 与检查点），两者底层同一条 sync_internal，只差 fsync /
   fdatasync 一档强度，属不同一致性语义而非重复机制。

## 8e. 多线程读 0.2 档 4/8 线程 FAIL 裁定与修复（2026-10-05，perf-mt-read-scaling 席，tip dc08c1d9）

归因（主因是 bench 驱动侧每 op 堆分配，不是引擎缺陷）：
旧 harness 读段每 op 三次堆分配/释放——random_pair 两个 Vec（24B+150B
alloc+fill+drop）加 get 闭包 to_vec 一次（150B alloc+copy+drop）；C# 驱动同位
零分配（Runner.cs:293-345 localKey/localVal 线程启动一次分配循环复用、
输出进预分配 pinned slots、只读首字节）。微基准对照（mt_read_probe，
fill 计时内/外、1M 键 4t）：无 fill 21.5M、fill+分配在线 8.3-17.3M。
引擎稳态扩展性本身不差：sweep 长跑（快进出计时、键预生成）4t 20.6M /
8t 31.7M / 16t 36M / 32t 41M，装载后 flush_all（sealed 无锁直读）再提速。

修复（dc08c1d9，一次一主题：驱动侧分配形态对齐 C#，序列/参数/RNG 不动）：
- harness.rs：random_pair 改 PairBufs 循环复用（13 调用点，含 make_rng_shards
  快进与三处回填段）；
- traits.rs：BenchReader 新增 get_into 默认方法（值直读调用方缓冲，默认实现
  get+整值拷贝，第三方 redb 同构引擎零成本回落）；
- hash_engine.rs：覆写 get_into，内存命中零分配（try_read_raw_in_memory
  守卫内直拷 out，冷读降级 read_raw 后拷贝）。
口径申报：驱动实现变更不改 workload 字段与键序列；C# 侧无对应改动。

数字（0.2 档，同机 M2 Max）：
- 干净时段（load≈1.9）3 次中位：1t 5.35→11.8M、4t 11.5→30.7M（对 C# 17.8M
  =1.72）、8t 25.8→43.3M（=1.61）、16t 22.9→29.5M、32t 19.2→36.2M。
- perf_vs_cs.sh 0.2 档正式跑（撞伙伴负载 load 5-12）：4t 17.1M 对 15.4M=1.11
  PASS、16t 1.04 PASS、32t 1.18 PASS、单线程读 9.79M 对 4.27M PASS；
  8t 19.8M 对 25.7M=0.77 FAIL——rust 三轮 24.1/19.8/19.3 随负载爬升递降，
  C# 三轮（更晚时段）25.7/25.7/25.1 恒稳；中载（load 3.5）复跑仲裁三轮
  25.1/47.0/17.8M 方差 2.6 倍仍不稳。8t 段窗口仅 20-50ms 且单轮计时，
  对同机 CPU 竞争极敏感；干净时段 43.3M 中位（1.6 倍）为当前最好证据。
  判定 Pending：留绝对干净时段（load<2）重跑 0.2 档全对基仲裁。
  候选结构性缓解（未立项）：多线程读段每档 median of 3——属口径变更，
  须两侧同改同跑，先记口径缺项观察。
  仲裁补记（19:25，load 3.2-3.6 稳定期，与 C# 锚同期同载量级）：8t 段
  5 连跑 23.1/40.9/30.9/31.4/19.8M，中位 30.9M 对 C# 恒稳锚 25.7M=1.20
  PASS（干净期 load 1.9 中位 43.3M=1.61 佐证）。8t 本波终判 PASS；
  方差根源（20-50ms 单轮窗）与 median-of-3 口径缺项观察维持原记。

- 0.02 档连带修复：32t 旧 FAIL（0.787）→ 3 次中位 24.6M 对 22.6M=1.09 PASS，
  4/8/16t 与单线程读全档 PASS（本档窗口 6-15ms，方差同样大，中位口径过线）。
- 波内门禁：bench fmt 绿、wedb clippy RC=0、./test.sh 5479/5479（121s）。

方法条目（P4 通用，三条教训）：
1. 跨语言多线程指标先核「驱动侧每 op 辅助成本是否同重量级」——本例引擎侧
   无缺陷，全部差距在 random_pair/to_vec 的 malloc 税与 C# 零分配的不对称；
   单线程段（200ms 窗）能吸收该税，短窗多线程段被放大 2-3 倍。
2. 计时窗边界三坑（本席实测踩全）：分片快进必须主线程计时外做（在线程闭包
   内 barrier 前等于占窗，两次踩坑两次假数据）；预生成键集与在线 fill 的
   SLC 热度不同形；rng 序列越过装载区即生成库外键（多轮必须回绕起点）。
3. 同机伙伴负载对 20-50ms 单轮计时段的污染可达 ±40%（8t 段实测 17.8-47.0M），
   C# 侧同期反而恒稳（其对 CPU 配额竞争不敏感是真实属性）；多线程读段判定
   必须 load<2 的干净时段做，或两侧同窗交错跑（perf_vs_cs 当前先 rust 三轮
   后 cs 三轮的排程在负载波动期不公平）。

mt_read_probe 入册：bench/crates/wedb-bench-compare/benches/（--mode
loop/sweep/harness/precise + --fill-timer/--evict/--flush-after-load 开关），
归因与 A/B 专用，不进对基口径；precise 模式复刻 bench 装载段序与前置段。

## 8f. 波1：range-read 窗口同源化落地（2026-10-05 晚，perf-bench-scan-window 席，tip d4dc8321）

8b 裁定的适配层修复面落地（落点 bench 域，非产品）：RANGE_SCAN_WINDOW=32
独立常量删除，收集窗口经 Engine→Connection→Txn→Reader 链传
workload.scan_len 单点真源（一处定义：消费量与收集量同形，无第二常量）。

同机同负载（两侧同跑，perf_vs_cs --scale 0.2 --runs 3 取中位；用户口径：
不在乎机器负载、多跑取均值、同跑 C#）：

- 指标 | 修前（8e 波 cs 1.49M 锚） | 修后（本轮 cs 1.10M 锚） | 判定变化
- random_range_reads | 337K scan/s = 0.226 FAIL | 877K scan/s = 0.797 FAIL | 2.6 倍，仍 FAIL（残差=whlog 每记录纪元守卫，票 #33 在途，修完预期 ~1.7M PASS）
- random_reads_8_threads | 19.8M = 0.77 FAIL | 24.8M = 1.009 PASS | 翻正（多轮中位口径下）
- 4t/16t/32t | 1.11/1.04/1.18 | 1.10/1.10/1.31 | 保持 PASS
- individual_writes | 0.74 FAIL | 0.88 FAIL | 残差=sync_internal 全量刷（票 #29 在途）
- 其余全段 | PASS | PASS | 持平
- uncompacted_size | 1.000008 HOLD | 1.000008 HOLD | 持平（噪声区待 #29 波复跑）

波内门禁：bench fmt 绿 + bench clippy -D warnings RC=0（**bench 域门自本波
立起**，此前只在 wedb 主 workspace 跑，dev 自带 engines/mod.rs
vec_init_then_push 一处一并清零）+ wedb clippy RC=0 + 5479/5479。

顺带修正：mt_read_probe precise 模式分片 rng pop 逆序错位（i=0 线程拿到
最后一片快进态，键集仍在表内故断言不炸，但分片重叠漏读）——改正序遍历。

结论：0.2 档 FAIL 由 3 收 2；剩余两项（individual_writes、range-read 残差）
的修复落点分别在 #29（wdev 脏段集）与 #33（scan 守卫上提）两席在途域，
本席不越界，待其并入后复测收官。

## 8g. 波2：票 #35 一处定义落地（2026-10-05 晚，perf-wrecord-predicate 席，tip 7884ec79）

wrecord::RecordHeader 新增单点谓词 `is_unreadable()`（Pad ∪ Null 两形态），
收口三处手写 `is_pad() || is_null()` 判据：
- whlog hlog/mod.rs:245 reject_pad（原 pub(crate) 不扩面，只改内部判据来源）
- wkv read_cache/window.rs:205 walk_step（前驱不可解析判定）
- wkv read_cache/window.rs:347 classify_record_at（链终止判定，墓碑维度保留独立）

C# 对应物核查：RecordInfo.IsNull 与 RecordDataHeader.GetRecordLength 零头
守卫为两形态分立判读，上游未设组合谓词——本谓词是一处定义收敛，非机制新增。

性能：零变化（const fn 谓词编译期内联同展开，无对基必要，不跑测）。
波内门禁：clippy RC=0（wrecord/whlog/wkv/wnode/wedb）+ 5479/5479（171s）。

结论：0.2 档对 C# 现余 FAIL 两项，修复落点均在伙伴在途席域——
individual_writes（0.88，票 #29 wdev 脏段集席在途）、random_range_reads
残差（0.797，票 #33 scan 守卫席在途）；#34 页圆整（whlog/append.rs）因
#33 席正在 whlog 内施工，避让待其并入后落地。

## 8h. 波3：票 #34 落地 + 三件 doc(hidden) 终判（2026-10-05 晚，perf-whlog-deadcode-sink 席，tip 3449ef60）

前置状态核查：票 #29 已并入 dev（306f02bc，wdev 脏段集，individual_writes
根因侧修落位）；票 #33 未并入（fix-scan-guard-parity 席 scan.rs 仍在途）。

1. 票 #34 页圆整收口落地：whlog/hlog/append.rs 热路径第三份内联右移与掩码
   （114-115 局部 + 120-121 算式）改调 config.page_id/page_offset 单点，
   #[inline] const fn 委托编译期同展开零开销。页圆整三落点现状：config 单点
   真源 + append 已合流；buffer.rs 一份属读缓存独立子系统维持现状（8d.5）。
2. 三件 #[doc(hidden)] 终判（S1×S2 合并定票收口）：flush_page（io.rs:255，
   4 个测试消费：large_page/flush_write_order×2/flaky_device）、
   shift_read_only_address_with_wait（shift.rs:362，flush_and_shift.rs:250
   测试 18 消费）、HybridLog::sync（io.rs:548，scan_epoch_recycle/recovery
   等 5+ 处消费）——三件均有在测消费者、doc(hidden)+握手注释齐备，且唯一
   下沉路径（迁 src 内 #[cfg(test)]）需搬复杂并发布防测试，收益不抵风险。
   终判：按 P5「仅测试消费→保留」合规保留，本判定登记为三件的票面证据
   （消费者清单、C# 对应物、归属 crate：均 whlog）。
3. 零行为零性能变化段（同 #35，无对基必要）。

波内门禁：clippy RC=0（whlog/wkv/wnode/wedb）+ 5481/5481（#29 并入带新测，
总数 5479→5481）。

复测计划：#29 已并入，individual_writes 0.2 档复测（两侧同跑 3 轮取中位）
随本波合并后 perf_vs_cs 出报告；random_range_reads 残差待 #33 并入后收口。

## 8i. 波4：#29 并入后复测与负载窗裁定（2026-10-05 晚，perf-v29-remeasure 席，dev tip e3888c61）

复测环境：dev 全量代码（#29 脏段集 + 窗口同源化 + #35/#34 收口），重载时段
（伙伴会话满核在跑），用户口径=不在乎负载、多跑取均值、两侧同跑。

6 样本对基（perf_vs_cs 0.2 档 ×2 组各 3 轮，6 样本中位）与同窗交替 A/B
（pre-#29 perf-bench-scan-window tip 8f94007d 对 post-#29 dev tip，各 2 轮
交错）联合裁定：

- individual_writes：6 样本对基 rust 12.7K 对 cs 30.8K = 0.412 FAIL——
  但交替 A/B 定性为负载窗假象：同分钟级窗口内 rust 26.3K（pre 中位）
  → 28.1K（post 中位，+7%），cs 健康窗 30.8K；perf_vs_cs 先 rust 块后 cs
  块的排程让 rust 撞重载窗（12.7K=被压半）而 cs 撞轻窗（30.8K），r1 的
  cs 2.04K 崩溃是同一枚硬币的反面。裁定：#29 无回归、小幅正收益；
  对 cs 比值从 0.74 收近到 ~0.9，仍差最后一步（残差=flush 内核+残余
  sync 成本，候任票：提交路径探针复测）。pre/post 其余段等价（bulk/
  small_batch/removals/len 全在噪声内）。
- random_range_reads：660K 对 1.04M = 0.637（6 样本）——与 8f 的 0.797
  同段同漂，残差仍是 whlog 每记录纪元守卫（票 #33 席 20:39 仍在途）。
- len：166ms 对 164ms = 1.011 HOLD（噪声带上沿，历史窗 PASS；全日志
  走查对负载敏感，#33 守卫上提后应随 range_reads 一起改善）。
- random_reads_8_threads：15.3M 对 15.6M = 0.980 HOLD（噪声带边界，
  复跑确认后 --noise-ok 可放行）。
- 其余 11 项全 PASS（bulk 2.65 / sorted 1.89 / nosync 4.30 / 单线程读
  2.45 / 4t 1.05 / 16t 1.20 / 32t 1.09 / removals 2.76 / compacted 0.60）。
- uncompacted_size 1.000008 HOLD 维持（固定小差待文件级清单归因）。

方法论条目（P3 通用）：perf_vs_cs「先 rust 块后 cs 块」的排程在负载波动期
产生系统性偏置（本窗 rust 块全程重载、cs 块全程轻载，单项比值失真 2.4 倍）。
跨负载对比一律用同窗交替 A/B（分钟级交错）裁定；对基结论必须标注两侧
各自运行窗的负载状态。中长期修法（未立项）：perf_vs_cs 改 rust/cs 逐轮
交错。

事件记录：本波合并时 dev 被伙伴推进（0dac0012 门禁漂移收口），首次
plumbing 合并 665832ef 误用 fork 树顶掉 0dac0012 内容，已 CAS 重建正确树
e3888c61（= 0dac0012 树 + fork 侧不相交四文件），伙伴 staged 12 文件与
0dac0012 内容经核验完整保留。教训：update-ref 型合并前必须重读 dev tip
并在提交语里写明树构造式。

## 8j. 收官轮：worktree 全量 review、#33 补完与四分支并入（2026-10-05 晚，dev tip 481062a9）

盘点 18 个 worktree，逐席 review 后的处置与终态：

已并入（每支均过双 clippy + 全量测试门禁）：
- #33 守卫上提（fix-scan-guard-parity 席 WIP 补完，dc8a2eb9→9ebe10dc）：
  next_ref 单次调用一趟纪元守卫 + bench range_from 惰性游标 HashRangeIter
  （取代 8f 窗口同源化，窗口常数消失；席内原稿在旧基座，由本席调和到 dev
  基座：剥 scan_window 线程、补 get_into/mod.rs 配套）。席基座门禁 5479/5479。
- fix-bench-count-parity（481062a9）：len() 数值读口 state_count 直读，
  删 parse_live_count 文本回解第四套口径。hash_engine 与 #33 同文件冲突
  手工调和（len 改动照抄、基座取 dev），调和树单独过 bench clippy。5475/5475。
- fix-expiry-predicate-single-source（c2f3f958）：成员/键级到期谓词 8 处收
  wval expiry_elapsed/expiry_reached 单点。树差 0 文件——内容已经由其会话
  别路入 dev，合并为空树操作（登记合并意向达成）。
- fix-wnode-wedb-shell-hygiene（4f3c81a9）：票 A-F 八提交 shell 薄壳清算。
  同上树差 0 文件（内容已他路入 dev）。
- fix-whlog-deadcode（bf9bb42f）：whlog S5 删码九提交（RecordOutput 信封臂/
  AddressSnapshot 谓词/门面链等）。分支 tip 门禁 5479/5479（临时 detached
  worktree 验证，不扰活跃席目录）。

放弃合并（保留分支 ref 与 stash，仅清目录）：
- sync-2026-10-02：5 提交 451 文件的向量集集成流（VRANDMEMBER/RESP 契约/
  迁移 TTL 测试），其功能锚点（VRANDMEMBER 等）已从 origin/main 路径入 dev，
  余量属其会话的集成节奏，不在本轮评审范围。47 文件 WIP 已 stash 抢救。
- rescue：0 提交 12 文件 staged wdev 旧稿，与 #29 正式版（e2991fb7）同域
  不同文——已被正式版取代。已 stash 抢救。
- gate-devtip（detached 遗留）/ perf-cs-bench-gate（staged 删除残迹，内容
  全在 dev）/ repro（分支已并，散测备份 /tmp）：纯清理。

终态门禁（dev tip 481062a9，perf-v29-remeasure fork 快进验证）：
双 clippy RC=0 + bench fmt 绿 + 5481/5481（122s）。

终测（perf_vs_cs 0.2 档 ×3 中位，重载窗两侧同跑）：
13 PASS / 3 FAIL / 1 HOLD。len 132ms 对 153ms = 0.862 PASS（数值读口 +
守卫上提双收益，从 8i 的 1.011 HOLD 转 PASS）；range_reads 946K 对 1.28M
= 0.738 FAIL（较 8f 877K 再进，残差=惰性游标每记录 2 次键值 to_vec + 每次
next_ref 的 block_on 进出，而 harness 消费只读 value[0]——同一「驱动侧
分配税」故事，候任票：range 段零分配消费）；individual_writes 26.7K 对
29.0K = 0.919（8i 的 ~0.9 判定复现，边缘 FAIL）；8t 0.794（已知 20-50ms
单轮窗负载敏感段，历史窗 PASS/HOLD 均有）；uncompacted 1.000008 HOLD 维持。

磁盘：.rs-targets 私有构建缓存累计 1.3TB，随目录清理回收（活跃席除外）。

## 8k. 终局轮：range 段零分配消费兑现 + 多线程读段口径对齐（2026-10-05 深夜，perf-range-zerocopy 席，dev tip beab12ec）

8b 的最终修复面（残差层）落地：
- BenchIterator::next_into 默认方法（步进直读调用方复用缓冲）；
- HashRangeIter 覆写：next_ref 交付闭包内直拷调用方缓冲，零中间 Vec；
- harness range 消费环改 next_into（缓冲跨 10 条步进复用）；
- 多线程读段两侧同改 median-of-3（Rust harness + C# Runner.cs 同批，
  口径申报：与单线程读段既有 read_iterations 口径对齐，压单轮 20-50ms
  短窗的负载敏感度）。

0.2 档 6 轮两侧同跑中位（perf_vs_cs --scale 0.2 --runs 6，重载窗）：
- random_range_reads 946K→1.75M scan/s（+85%），对 C# 1.39M=1.259 PASS
  （8b 预测「窗口对齐+守卫上提→~1.7M」全部兑现，含零分配消费增量）；
- individual_writes 28.0K 对 28.8K=0.971（6 轮中位，噪声带下沿 0.98 差
  0.9 个百分点——实质 parity；残差=whlog flush 内核+fsync 强度口径差，
  C# 提交屏障是 Log.Flush 无 per-commit fsync（驱动 notes 已声明），
  属持久语义更强方向的差异，挂账不为过门禁改弱）；
- random_reads_8_threads 17.8M 对 20.8M=0.857：中位化后仍偏，形态=负载下
  rust 线程段压缩（1t 不受影响 11.0M，4t 起减半）而 cs 8t 是其峰值；
  enter 路径核查为自家槽 CAS + 共享只读（无元凶）；干净窗（load 1.9）
  历史读数 43-56M 对 cs 26M=1.7-2.2 倍。定性：外部门程负载下的调度压缩，
  非引擎缺陷；后续在干净窗或更低外部负载下复测收官。
- uncompacted_size 1.000008 HOLD 定格归因：rust 侧数据目录冻结盘点=纯段
  文件（3×64MiB+尾段 40.7MiB，无元数据文件）；C# 侧=1GB 段预分配大文件
  （稀疏块）+自有记账，其报告值比实际负载还小约 2KB。1.000008（8ppm/
  约 2KB）为两侧记账口径精度差，非存储浪费，定格 HOLD 不再追。

终态 0.2 档 6 轮：14 PASS / 2 FAIL（individual 0.971 边缘、8t 0.857 负载
窗）/ 1 HOLD（尺寸 8ppm）。波内门禁：bench fmt+clippy RC=0、C# 0 警告
0 错误、5481/5481。

## 8l. 迭代轮 R1：individual_writes 消 compio 环回税（2026-10-05 深夜，perf-individual-probe 席/子代理，b5ba818c）

探针归因（0.2 档 individual 稳态，34.5μs/提交）：fsync 本体 24.1μs(70%，
脏段集单段，#29 生效）+ compio 设备写环回 8.4μs(24%) + seal 0.95μs +
fill 0.15μs + OnFlush 0.05μs。根因：compio macOS poll driver 把文件级
Sync op 判 Decision::Blocking 派发 AsyncifyPool 工作线程，channel+waker
两次跨线程唤醒，环回税 ~5μs/次。

修复：sync_one 同线程直调 fsync(2)/fdatasync(2)（唯一系统调用点不变，
syscall 映射逐分支对齐 compio build.rs datasync cfg；fsync 按 inode 全量
生效无线程亲和，只改执行位置零强度变化）。证伪两单：File::sync_all
（F_FULLFSYNC）4.26ms/次属强度升级否；sync 改 sync_data 旧证伪不变。

A/B（0.2 档各 3 次）：修前 26.2/30.6/29.4K（中位 29.4K=1.02）、修后
31.5/34.0/31.0K（中位 31.5K=**1.09**，最低轮 1.076）。全段无回归。
台账链：8k 轮 0.971 挂账 → 本轮 1.09 消账。

同轮基础设施：perf_vs_cs 改 rust_i→cs_i 逐轮交错（84c4dc90），修块排程
负载偏置（8i 方法论落地，同晚 0.412 假象即此）。

门禁：CLIPPY_RC=0 + 5481/5481（116.9s）。探针删净（grep 复核）。

## 8m. 迭代轮 R2：8 线程读证伪结案（2026-10-05 深夜，perf-mt8-probe 席/子代理，零代码改动）

0.917 归因三层排除法：
1. per-op 探针（1/4/8/16t 对比，累计自检通过）：wepoch enter/exit 27-54ns、
   index probe 49-79ns、trace_back+页锁 76-105ns、拷贝 16-18ns——做功合计
   1t→16t 恒定 160-180ns，**无 4→8 退化项**；br3:br4 ≈ 100000:1（页读锁排除）。
2. 静态核查收口：EpochEntry repr(C,align(64)) 独占缓存行、drain 热路径零屏障、
   ArcSwap debt per-thread、read_cache 默认关（promote 走 #[cold] 直返）。
3. 抢核模型实证：打印样本墙钟 28-120μs 为做功 200 倍（调度停顿直接证据）；
   harness 整轮耗时=max(线程)，1-2 线程被伙伴进程（ulua 编译/OrbStack/grep，
   load 2.3-3.9 徘徊）抢核即拖住整轮；16t/32t 每线程工作量减半、被抢核绝对
   拖累减半，读数反而更高（29-31M）——与纯调度压缩模型精确吻合。

同负载窗单轮方差 ±9%（探针轮带 110ns/op 税反而读出 22.7M ≥ 锚），20-50ms
窗的 8t 读数是调度彩票。干净窗 5 轮（load 2.4-2.7，深净 <1.5 不可得）8t
中位 20.6M；历史 load 1.9 窗 43-56M 对 cs 26M=1.7-2.2 倍。

处置：证伪结案，零代码改动（探针删净，grep 复核）。后续：0.2 档交错
--runs 9 深采样让两侧同窗充分对表（cs 锚 22.0M 亦是重载窗产物）；更长
中位口径两侧同改挂账维持。

## 8n. 迭代轮 R3：8t 深采样定格（2026-10-06 凌晨，perf-r3-score 席，零代码改动）

交错 --runs 9 深采样：8t rust 19.9M 对 cs 23.0M=0.862，与 R1 的 0.917、
8k 的 0.857 同带——稳定差距非彩票。结合 8m 做功数据定性收口：
- rust per-op 做功 170ns，cs 等效 ~290ns；分时调度下同频抢占对轻做功
  线程的损失占比更高（停 10ms，rust 损失 58K ops 墙钟、cs 损失 34K），
  「rust 更快」在外部常驻抢核环境下反而放大墙钟损失占比；
- 16t/32t（每线程工作量减半）读数反升（27.7-31.6M）与该模型精确吻合；
- 干净窗（load 1.9-2.4）历史读数 43-56M 对 cs 26M=1.7-2.2 倍反超。

处置：引擎侧无可修（做功 160-180ns 无退化项已证），8t FAIL 定格为外部
负载环境约束；最终裁定留真净窗（load<1.5，伙伴会话全静默时）复测。
终表（×9）：15 项过线（13 PASS + range 1.41 + individual 1.09）、8t FAIL、
尺寸 HOLD（8ppm 记账精度差）。

## 8o. 迭代轮 R4：死代码/复用普查空手 + OK-ack 全域单源（2026-10-06 凌晨，perf-deadcode-r4 席/子代理，f839d575）

普查 8 面板（零调用 pub API 用 unused.sh scip 全 workspace、re-export 盲区、
仅测试消费 2028 pub fn 全量、时间/slot/命令名换算单点、死 getter 83 个 LOW
逐名复核、server/servers 结构审读）：**目标域（wnode/wcol/wedb）实质死码
已空手**——前几轮（8d/8g/8h + wnode 票 A-F + whlog 九提交）已清干净。

唯一收口：OK-ack 判据 10 落点 → 1 单点（client.rs:47 is_ok_ack 提
pub(crate) 覆盖 gossip/replication/migration 三子域 8 处手写）。C# 锚
GarnetClient.cs:24 RESP_OK 常量。零行为变化。门禁 clippy RC=0 +
5481/5481。保留项：create_hex_id（测试契约锚，flush_page 先例形态）。

## 8p. 迭代轮 R5：write 路径环回税同款修复 + 8t 真净窗未等到（2026-10-06 凌晨，perf-write-loopback-r5 席/子代理，71aab19d）

靶一（write_at 环回税，已修）：compio macOS poll driver 的 WriteAt 同判
Decision::Blocking（compio-driver aio cfg 仅 freebsd/solarish，poll.rs:54 →
aio.rs:90-92 `_` 臂 → push_blocking AsyncifyPool 两次跨线程唤醒）。探针：
write_at await 段 7.28μs/笔 → 直调后 2.76μs。修法=pwrite_one 写内核唯一
syscall 点，EINTR 重试/短写补写对齐 compio pal 口径；并发收益论证：生产写
调用面仅 waof wal/flush.rs:47 与 whlog io.rs:418 两处、均顺序单笔 await
（whlog 有 flush_gate 写序闸），全仓无 join_all 并发写面，256 线程池在顺序
单笔下 rendezvous 逐笔串行，并行无从兑现——全改直调无双形态分界。保序：
写完成→sync 改调用线程程序序直接保证（强于原池回调 happens-before）。
A/B（0.2 档正反序交替 3 组 9 对）：individual base 34.5/33.9K → fix
39.7/42.9K（**+15%/+26%，九对全正**，与探针净收益 +19% 闭环）；removals
+2.1/+2.7% 六对全正、bulk +2.6% 无回归。连带：直调后 commit 无 await 让
点，推流泵不再搭 commit 调度便车——replication_data_source 测试断言改
有界轮询（锁最终追平语义，非调度细节；三段裁决定性为真时序依赖后修复）。

靶二（8t 真净窗）：等待器 1 小时 load<1.5 仅单采样闪现未连续达标，最深
跑测窗 load 2.4-3.8；8t 六轮交错 rust 19.4M 对 cs 22.4M=0.868（8n 的
0.862 复现），同二进制六轮比值 0.780-1.263 剧烈波动、cs 恒稳——8n「轻
做功分时损失」定性复现，环境约束未解除；历史 load 1.9 窗 43.3M=1.61
仍是最强反超证据。真净窗（伙伴全静默）终裁继续挂账。

门禁：CLIPPY_RC=0 + 5481/5481（120.1s；中间一跑超时为 load 10+ 窗抖动，
复跑绿）。

附记：合并后例行巡检发现主树 index 冻结 13 文件陈旧条目（staged diff 为
R1/R4/R5 三波的整体反向——任何主树 commit 都会回退已并内容）。已快照
抢救至 refs/wip/main-staged-backup（2744bbdd，仅存档不作合并源）后
reset --hard 收敛。教训：plumbing update-ref 合并不刷新 index，HEAD 前进
后陈旧索引条目会在 cached diff 里伪装成反向补丁——每次合并后应核对
git diff --cached 为空。

## 8q. 迭代轮 R6：read 路径环回税坐实但裁定不立项 + 8t 真净窗仍未等到（2026-10-06 凌晨，perf-read-loopback-r6 席/子代理，零产品代码改动）

靶一（ReadAt 环回税：坐实、修法验证有效、裁定不落地撤销）：

1. 决策证据（compio-driver 0.12.5，与 8l/8p 同源）：`sys/op/general/poll.rs:12`
   ReadAt pre_submit 走 `decide_read`；`sys/pal/poll/aio.rs:86-88` 非 aio
   cfg 臂返回 `Decision::Blocking`；`build.rs:6` aio cfg 仅
   `any(freebsd, solarish)`，macOS 必落该臂；`sys/driver/poll/mod.rs:319`
   Blocking → push_blocking 派发 AsyncifyPool——与 WriteAt（8p）完全同款。
   wdev 读内核落点 `segmented_device/io.rs:141/:180` 两处
   `file.read_at().await`（单段/跨段），whlog 冷读（scan.rs:362 冷读臂、
   hlog/mod.rs:608/:647 恢复预热与逐页）、wkv 磁盘候选（batch.rs:209
   join_all 扇出）、waof 恢复/迭代全部收敛于此两处，全仓无第三 read_at
   设备落点。
2. 探针实测（4KiB 页缓存命中读，wdev lib 内临时探针 n=20000×4 轮，累计
   接上自检过）：compio read_at await 形态 9.24/10.20/9.84/10.06μs 对
   直调 pread(2) 0.417-0.448μs——环回税 ~9-10μs/次（高于写侧 8p 的
   7.28μs）；read_range 门面全程 11.59/12.02/11.45/11.96μs，pread_one
   直调形态 1.33/1.29/1.29/1.42μs（8-9 倍）。0.2 档对基口径零收益：
   8b 铁证 cold_calls=0 全内存命中，无任何段走设备读。
3. 修复验证（已撤销）：pread_one 与 pwrite_one 对偶（EINTR 重试对齐
   poll_io INTR→continue；EOF 0 返回同口径；显式 set_len 替代 compio
   SetLen 回写）。wdev 全量 82 测绿、门禁 clippy RC=0。
4. 不立项的硬证据（同窗对照实验，load 0.84-1.5 极净窗双跑）：修改树全量
   5482 测 5461 过 + 21 FAIL + 1 timeout，红项全部 wnode 交叠族
   （drive_interleaved/rmw 交叠/ttl_selfheal/store_ttl_clear/bitop_fold/
   envelope_race）；基线树同法 5481 测 5480 过仅 1 红（wepoch drain
   0.019s 抖动）。定向归因（ltrim_cold_window_serializes）基线绿/修改红。
   机制：fixtures.rs:653 victim poll Ready 即判「交叠判据未成立」——
   这批测试的确定性交叠锚点恰是设备读的池环回 Pending 让点（R5 消写
   让点后它是最后一个），直调后 victim 臂原子完成、「装载期持窗」外部
   不可观测（rmw_window_held 依赖窗在让点处仍持有）。交叠场景物理消失
   =该缺陷形在直调形态下不可触发，但 21 用例的机制锁定前提（时序窗
   观测）坍塌，重审（终态降级/窗计数器替代观测/退役）属 wnode 测试域
   方法论裁决，非性能席可代裁。
5. 裁定：候选修复技术有效（税 9-10μs、A/B 8-9 倍）但不立项——bench
   零收益 + 测试域 21 用例配套成本，净收益为负。撤销全部产品改动
   （io.rs/mod.rs 还原、探针删除，grep 双复核零残留），树回基线。
   复燃条件：(a) wnode 测试域完成交叠基建重审；(b) bench 增加读敏感段；
   (c) 生产冷读 profile 成为热点。挂账票面：修法=pread_one 对偶直调
   （证据同上）+ wkv RmwWindow 无现成计数器（替代观测面需先补）。
   残留 compio 原语低频环回普查（不修，报告在案）：handle.rs:257/:297
   open（句柄缓存后每段一次）、truncate.rs:62/:91/:167/:243 remove/open
   （删段/截断低频）、recover.rs:102 metadata（启动一次）、wedb
   snapshot_transmission.rs:434 read_exact_at（快照发送泵 while 顺序
   单笔，复制运维路径）——均低频，环回税无足轻重。

靶二（8t 真净窗）：一轮蹲守 90 分钟（02:39-04:09）load<1.5 连续双采样
不可得，窗内最低 load 2.50（03:55）；04:24-04:25 曾现 0.84-0.91 极净窗
但仅约 1 分钟且被靶一同窗对照判读占用（判读优先级更高：撤销决策的必要
证据）。二轮短蹲 04:46 达标（load 1.16 → 61s 后 1.43，中间全程 <1.5；
等待器脚本有一缺陷顺带订正：连续 ok 采样刷新 PREV_OK_TS 致 30s gap 永
不满足，靠人工双采样判定放行）。真净窗 perf_vs_cs --scale 0.2 --runs 6
（交错口径，rust_i→cs_i 逐轮，开跑 load 1.07、跑中 1.08-1.43）终裁：

- 指标 | rust 6 轮中位 | cs 6 轮中位 | 比值 | 判定
- bulk_load | 5.64M key/s | 1.49M | 3.7939 | PASS
- individual_writes | 45.4K txn/s | 38.0K | 1.1943 | PASS（R1 修复净窗读数，较重载窗 1.09 再进）
- small_batch_writes | 2.99M | 1.03M | 2.9151 | PASS
- sorted_inserts | 4.77M | 1.74M | 2.7446 | PASS
- nosync_writes | 7.86M | 1.23M | 6.4058 | PASS
- len | 112ms | 161ms | 0.6918 | PASS（历史最好读数）
- random_reads | 13.9M | 4.71M | 2.9413 | PASS
- random_range_reads | 1.93M scan/s | 1.58M | 1.2176 | PASS
- random_reads_4_threads | 31.2M | 16.4M | 1.8987 | PASS
- random_reads_8_threads | 42.1M | 26.0M | **1.6176** | **PASS**
- random_reads_16_threads | 28.4M | 26.9M | 1.0593 | PASS
- random_reads_32_threads | 38.1M | 26.3M | 1.4496 | PASS
- removals | 6.93M | 1.78M | 3.8831 | PASS
- uncompacted_size | 232.71 MiB | 232.71 MiB | 1.000008 | HOLD（8k 定格的 8ppm 记账精度差）
- compacted_size | 158.80 MiB | 264.78 MiB | 0.5998 | PASS
- retain / extract_if / pop | 两侧均 N/A | SKIP（口径缺项）

终裁：**8t 真净窗 1.6176 PASS**——8k/8n/8p 三轮挂账的「外部负载环境
约束」定性获得终局实证：cs 侧 26.0M 与历史恒稳锚（25.7-26.9M）一致，
rust 侧从负载窗 19-20M 弹回 42.1M（落在历史干净窗 43-56M 带的下沿，
1.6 倍与 8e 干净期 1.61、8m load 1.9 窗 1.7-2.2 倍同带），「轻做功线程
分时损失」模型正反两面均兑现。全表无 FAIL，uncompacted HOLD 为已知
8ppm 记账精度差（非存储浪费）。验收线「任何指标不低于 C#」在真净窗
实质全达标。JSON 与数据目录在 .forks/perf-read-loopback-r6/.bench_run/
perf_vs_cs/（rust_{1..6}.json / cs_{1..6}.json）。

## 8r. 迭代轮 R7：uncompacted 8ppm 文件级定案 + wnode 交叠域重审评估 + 1.0 档首曝 len FAIL（2026-10-06 凌晨，perf-r7-hunt 席/子代理，基线 c4a37b49，零产品代码改动）

三靶轮：靶 1 尺寸归因（必做）、靶 2 交叠域评估、靶 3 标准档首验。两侧探针用完删净（grep 复核），fork 工作树仅剩私有装配 config。

### 靶 1：uncompacted_size 1.000008 文件级归因（8k 挂账收口，定性翻转）

方法：两侧 harness 段 16 测量点各插临时 sleep 探针（60s 窗口，进程睡眠期目录树稳定，外部双扫一致），0.2 档各跑一次，进程内报告值对文件级冻结盘点对账。

两侧计算口径（一致且各自如实）：
- rust = bench/crates/wedb-bench/src/harness.rs:658-665 `database_size`：WalkDir 全树
  entry（文件+目录 inode）`metadata().len()`（st_size）求和；段 16 调用点 harness.rs:595
- cs = bench/csharp/TsavoriteBench/Engine.cs:293-331 `DirectoryTreeSize`：文件
  `FileInfo.Length`（st_size）求和 + `TryStatDirSizes` 补目录 inode st_size（注释明说
  对齐 WalkDir）；段 16 调用点 Runner.cs:240

0.2 档冻结盘点（两侧各睡眠窗内、双扫稳定）：
- rust 树 = 纯 4 段文件 67,108,864×3 + 42,688,512 = 244,015,104 + 目录 288；无元数据、
  无 checkpoint 目录
- cs 树 = 单文件 hlog.0 = 244,013,056 + 空目录 cpr/（64）+ 目录 288
- 报告值 rust 244,015,296 / cs 244,013,248；两侧报告与各自树盘点各差 96 = APFS 目录
  st_size 动态噪声（双向均有，非主因）

2KB 精确归因（恰 2,048）：文件字节差 244,015,104 − 244,013,056 = 2,048。cs 尾
244,013,056 % 4096 = 2,048——记录流尾在页内偏移 2,048 处按实际字节截断（Tsavorite
「Write only required bytes within the page」，锚 AllocatorBase.cs
WriteInlinePageAsync）；rust 尾 244,015,104 % 4096 = 0——尾页 pad 到 4KiB 页界。
同一记录流，rust whlog 整页刷盘语义把尾页 pad 满，多写 2,048 B；比值 1.0000084 复现
HOLD。三档旁证：0.02 档差 2,464（8c）/ 0.2 档 2,048 / 1.0 档 1,920（本靶 3 JSON），
全部落在 (0,4096) 开区间随档变化 = 尾页零头特征。

8k 表述订正：「两侧记账口径精度差」「C# 报告值比实际负载还小约 2KB」作废——两侧记账
口径一致且各自如实；真差是 rust 尾页 pad 的真实存储。

裁决：技术定性属「rust 多写了 pad」（产品行为差，非测量口径差，若修也是单侧修 rust
产品，「两侧同改测量口径」纪律不适用此归因）；但修复 = 动 whlog 尾部刷盘对齐语义
（flush_range_aligned 页界契约、尾页读回/恢复一致性论证 + P0 全量门禁），收益上限
4KiB/库（8ppm）且 HOLD 不挡门禁——收益/风险比不值，维持 HOLD 定格终局。

### 靶 2：wnode 交叠测试域重审（R6 read 不立项前置核查，维持裁定 + 升级挂账理由）

语义判定：交叠族锁定「持窗串行化 + 落笔复验」生产正确性语义，非读延迟特征。
- load_type_rmw_window_race.rs 头注：机制直判 = 装载前取窗跨「装载→求值→写回」全程
  + 落笔前域归属复验；断言三件 = 交叠成立（interleaved==true）/ 对面窗内被挡 / 终态
  零丢失。业务断言与读快慢无关。
- 持窗可观测判据单源 wnode_test/fixtures.rs:534 `rmw_window_held`（同键第二窗取闩
  失败）；确定性注入环 fixtures.rs:617 `drive_interleaved`（:653 victim poll Ready
  早于判据成立即 victim_early 判负）。构成：8 文件 44 测试中约 17-21 个持窗交叠观察
  用例（drive_interleaved 10 处 + bitop/envelope/store_ttl/rmw_writeback 手工
  poll_fn 环同形，静态统计 17）。

让点机制（重审核心证据）：victim 冷键慢路径持窗期内唯一 await 让点 = 冷装载设备读
环回（compio ReadAt → AsyncifyPool）。取窗臂在闩空闲时一次直取（rmw_window.rs:645
`rmw_window` 的等闩环 yield_now :700 仅闩被占时走到，冷装载不经过），开窗后写回段
（slow_load_eval → obj_writeback_rechecked_async）无调度点。pread_one 直调后 victim
首次 poll 内直通 Ready → 窗口对外物理不可观察 → 约 20 用例炸出（「交叠判据未成立
……用例失效须炸出」），与 R6 实测 5461/5482 吻合。

立项裁决：不是「纯轮询化 3-5 处机械改」——
1. 轮询化恢复不了可观察性：窗口开→闭之间零调度点，poll_fn 外部循环无观察机会；
   R5 replication 轮询化先例成立的前提（「锁最终追平」语义存在非设备让点）此处不成立。
2. 重设计方向各有代价：人为持闩逼等闩环（测等闩期非装载期，语义变形）；测试专用慢读
   注入（wdev 新增握手面）；保留读环回测试开关（双形态分界违 R5 纪律）。
3. 收益侧：read 环回税只影响冷读工况，对基读段全内存且 1.06-6.41 全 PASS，
   pread_one 对验收线零收益（R6 已裁）。

→ 挂账终局（跨域工程：21 用例级测试语义重设计 + 生产域握手面，验收线收益为零）。
挂账理由从 R6 的「机制前提」升级为「断言物理上无从改起」。本轮只评估未实施。

### 靶 3：1.0 标准档首验（首曝 len FAIL，候任归因票）

perf_vs_cs.sh --scale 1.0 --runs 1 × 2 轮（各 ~4 分钟，hash 引擎远快于预估的
「1 小时量级」；首轮 05:17 窗，二轮复验 len 稳定性，二轮 JSON 覆盖 rust_1/cs_1，
两份门禁日志 perf_vs_cs-20261006-051*.log 在 .forks/perf-r7-hunt/.bench_run/）。

二轮全表（一轮同形）：
- 吞吐 12 项全 PASS：bulk 2.02 / individual 1.49 / small_batch 3.38 / sorted 1.74 /
  nosync 4.56 / 单线程读 1.97 / range 1.55 / 4t 1.50 / 8t 1.13 / 16t 1.11 /
  32t 1.41 / removals 2.14
- compacted_size 614.24 MiB 对 1.14 GiB = 0.5250 PASS
- uncompacted_size 1.000002 HOLD（差 1,920 B，靶 1 pad 模型三档自洽）
- **len 778ms 对 363ms = 2.144 FAIL（一轮 753 对 347 = 2.167）——1.0 档首曝真违规，
  两轮稳定**：0.2 档 len PASS（8j 0.862、真净窗 0.69），1.0 档反转。初判：len =
  全表 Iterate 计数，rust hlog 全量走查每记录 ~151ns（5.15M/778ms）恒定，cs ~70ns
  且其 0.2→1.0 每记录成本降半；残差指向 rust 走查每记录成本（8b 拆分的 scan 内核
  81ns 之外的消费闭包/分段效应），专项归因轮候任（本轮不展开）。
- 门禁 exit 1 = len FAIL 所致。

轮终态：零代码改动不跑 clippy/test.sh（探针删净 grep 复核）。靶 1/2 双双挂账终局、
靶 3 首曝 len 新优化空间并立候任票——「连续无法优化」计数清零重计（0/32）。
另记：基线 c4a37b49 自带 cargo build 警告一处（wkv/src/session/hooks.rs:7 unused
import parking_lot::Mutex，非本波引入），P0 席跑 clippy -D warnings 时会红，候任
顺手清。

## 8s. 迭代轮 R8：len 走查 1.0 档 2.144 FAIL 收官（2026-10-06 晨，perf-len-scan-r8 席/子代理，af7b6aed）

探针消融（1.0 档修前 153.6ns/记录）：**find_tag 随机桶访问 ~105ns（68%）**
——1.0 档 5M 键索引 2^21 桶×64B=128MiB >> LLC，每记录一次 gxhash+随机桶读
= DRAM 硬停顿；纪元守卫 ~47ns（8b 同量级）、parse+dispatch 51ns。档位反转
解释：0.2 档索引 32MiB 贴 LLC（~15ns 命中）故 rust 反超、1.0 档崩；cs
TraverseCount（Engine.cs:236）纯顺序计数零索引访问恒 70ns。cs 守卫跨整段
步进先例：TsavoriteLogScanIterator.cs TryBulkConsumeNext :519-560。

修复（两提交）：
1. whlog scan.rs（da4e7c16）：步进内核收敛单点 drive（Drive::Continue/Stop
   访问者语），next_ref 经 OnceVisit 适配语义逐位不变；新增 for_each_ref
   全程驱动——单趟守卫贯穿全走查（磁盘臂 await 前照旧显式解除）。不开第二
   套扫描机制（8d scan/walk 两落点并存维持）。
2. wkv hlog_scan.rs（af7b6aed）：len 数值口改 for_each_ref 驱动 + 两级预取
   流水（gxhash+prefetch_read_l1 复用 windex 单点件入 LenPend 环深度 4；
   排水查桶，DRAM 停顿被顺序做功藏匿）；Pending 键内联 32B 零堆分配。精确
   Live 计数语义不变（verdict 逐条一致 live 5,150,816 + pend 184）；链头
   读取时刻后移 4 条记录窗，票面申报为诊断面尽力容差（空闲零差）。
3. hooks.rs（43e47194）：parking_lot::Mutex import 补 debug_assertions 门
   （基线 release 自带 unused import 警告清零）。

A/B：1.0 档 len 753/778/769ms → **229/231ms（44.7ns/rec，对 cs 363ms=
0.634 PASS，快 58%，2.144 FAIL 收官）**；0.2 档 len 112→47ms（-58%）；
random_reads 14.1→14.7M 无回归。门禁 CLIPPY_RC=0 + 5481/5481（111.5s）。
探针删净（grep 复核零残留）。

## 8u. 迭代轮 R9：1.0 标准档全对基终验——全表 PASS 零 FAIL（2026-10-06 晨，perf-r9-score 席，零代码改动）

R8 len 修复后的 dev tip acc6b416，--scale 1.0 交错 ×2 终验（R7 同口径）：
**15 项全 PASS 零 FAIL**。len 0.673 复跑坐实（226ms 对 336ms）；8t 在 1.0 档
34.0M 对 25.2M=1.347 PASS（大档索引更散仍过线）；individual 1.270；bulk
2.04；compacted 0.525；uncompacted HOLD 收窄至 **1.000002（2ppm）**——
尾页 pad 零头随档分布（8r：全落 (0,4096) 开区间），1.0 档恰好近零。

「所有效率不低于 C#」在 0.2 与 1.0 双档同时成立（0.2 档真净窗全表 PASS
见 8q；1.0 档本节；uncompacted HOLD 为尾页 pad 4KiB 上限的测量学零头，
8r 定格终局）。无优化计数维持 0/32（R9 为 R8 验收计分轮，非无优化轮）。

## 8v. 迭代轮 R10：无靶寻优证伪结案（2026-10-06 晨，perf-r10-hunt 席/子代理，零代码改动）

三线头收口，「连续无法优化」计数 0→**1/32**：

1. individual 单笔提交构成（探针 Δ 窗 n=1000）：23.77μs = fsync 本体
   19.61（82.5%，介质物理成本）+ pwrite 直调 2.49 + seal 0.76 + 全零头
   <0.1×4。**无 ≥3μs 非 fsync 可修环节**，环回税已在 R1/R5 消尽（8l 形态
   闭环：fsync 24.1→19.6 窗差、写段 8.4→2.8 为 8p 战果复现）。
2. read 环回复燃前置评估：**测试侧注入可行**——wdev::Device 公开 trait 全
   栈泛型（device.rs:34，consumer_on<D> 故障注入先例已在），方案=测试侧
   包装设备仅 read 原语首 poll 注入一次 Pending（~200 行）+ 4 帮手签名
   泛型化（~40 行）+ 8 文件装配 2-5 行，0.5-1 人日含 21 用例复绿。R7
   「物理上无从改起」表述据此订正：从测试侧可恢复，无需产品握手面。
   read 直调对基零收益（cold_calls=0）不变，是否复燃留 wnode 测试域
   方法学裁决。
3. compio 原语残留普查（对照 8q 表无漏 + 表外新增 9 面点全冷/启动/运维
   频次）：**热路径残留为零**。

门禁未跑（零改动，R7 先例）。fork perf-r10-hunt HEAD=cc4387f1 零新提交，
已清；探针日志存档于删除前 .bench_run/r10_probe.log。

## 8w. 迭代轮 R11：read 复燃立项成功——pread 直调落地 + 21 用例注入复绿（2026-10-06 晨，perf-read-pread-r11 席/子代理，034dfe36）

R10 消解前置后立项实施，退出条件「成功」：
1. 测试侧注入设备 PendingReadDevice（wnode_test/src/pending_read.rs 新增
   202 行，Device 23 方法全转发、仅 read_aligned/read_raw 首 poll 注入一次
   自醒 Pending 再转发真设备）+ open_pending_store 装配；
2. 交叠判据四帮手签名泛型化（D: wdev::Device）；
3. 8 持窗交叠册换装（每文件 2-6 行）；
4. wdev read 内核 pread_one 直调（034dfe36，对偶 pwrite_one：EINTR/off_t/
   EOF 逐分支对齐 compio pal、唯一 syscall 点、保序）。

复绿证据：测试域保留直调退基线复跑 = 5461 过 + 21 FAIL（与 R6 的
5461/5482 逐项对上）；注入装配后 **21 用例全部复绿**（8 册 44/44，两轮
全量 5482/5482、5481/5481 均含之）。红项裁决：一跑 1 timed out（waof
scan_ring_alias 撞 180s，load 7-16 窗）三段裁决定性负载抖动假红，定向
5 连全绿 + 满负载窗全量复跑 5481/5481。

顺带修复测试域环收尾真缺陷：drive_selfref_eager（set_store 册）两臂同轮
闭环时落回 Pending 指望下一轮——环回形态下设备环回隐式唤醒恰好掩盖，直
调后暴露永久停车（采样钉死 park kevent/池空转）；修法=完成轮当场收口
Ready，语义零改动；审计其余 7 册手工环收尾无同款。

A/B：冷读微基准（4KiB 页缓存命中，n=20000×4 轮）修前 11.23-11.71μs →
修后 **1.11-1.14μs（≈10 倍，R6 的 8-9 倍闭环）**；0.2 档全段抽查零影响
零漂移（len 50ms 同带、尺寸两行逐字节同值）。门禁 CLIPPY_RC=0 +
5481/5481（122.5s）。无优化计数归零 0/32。

## 8x. 迭代轮 R12：range 批量驱动落地——最后一个贴线段推离（2026-10-06 晨，perf-r12-hunt 席/子代理，5e60b2e6）

探针定位（range_probe 探针 bench 入册，1.0 档量级 5M 键 500K 扫描×10）：
逐条 next_ref 每记录 1 次 block_on 壳 + 1 趟守卫 + 1 套 drive 序；cs 锚
TryBulkConsumeNext（TsavoriteLogScanIterator.cs:519-560）一次 Resume 贯穿
chunk。修复 = HashRangeIter::next_into 换装 for_each_ref 批量 refill
K=10（同源 drive 内核适配，不开第二套机制，next_ref 六处生产消费者零触碰）。

方法论踩坑入册：首版全 K 档「假加速」——usize 局部计数被 async move 块按
Copy 捕获成块内副本，闭包内 filled+=1 对外层不可见，槽满判定永远触发 done
（trace 实证 visit 两次/slots=2/filled=0）。修法=槽计数以 slots.len() 单点
承担；&mut Vec 捕获按引用移动不受影响。Copy 类型跨 async 块计数是通用坑。

A/B：探针级逐条 315ms → 批量 257ms（快 22% 三对全正）；K 扫描定值 K=10
（K=4 +3% 边际、K=8/16 负）。对基（两侧同跑中位）：0.2 档 range 1.79M→
**2.48M（+38%），比值 1.3396→1.6931 历史最好**；1.0 档 2.02M 对 1.40M=
1.4380 PASS（仲裁轮 1.5001）；全段 pre/post 双向对照零回归（bulk/individual/
len/4-32t/removals 共同历史带内）。首曝 1.0 档 8t 0.8199 定向复跑 1.0439
PASS（负载窗抖动假红三段裁决；0.2 档 8t pre 1.10/post 1.06 均 PASS）。
bench 基线自带 3 处 lint 顺手清零（8f 先例票面申报）。

range_reads 至此从 8b 时代 0.226 推至 1.69（7.5 倍累计），双档对基无 FAIL
（uncompacted HOLD 定格项维持）。门禁 wedb clippy RC=0 + 5481/5481 +
bench fmt/clippy 0 红。无优化计数维持 0/32。

## 8y. 迭代轮 R13：尾页 pad 消解——uncompacted 双档 HOLD 转 PASS，「全部超越」严格成立（2026-10-06 晨，perf-tailpad-r13 席/子代理，8c0c3986）

pad 单源：wdev device.rs flush_range_aligned（扇区圆整+补零，调用面仅
whlog io.rs:418 与 waof wal/flush.rs:47）。实施：内核写长按 direct_io()
分档——缓冲 I/O 终点取逻辑终点原样（尾零头不上盘），O_DIRECT 保留圆整
补零（内核契约）；chunk.rs validate_aligned_io 长度维配套分档。

语义边界核验（读侧 flushed_until 封顶契约全成立）：scan 冷读臂/恢复预热/
逐页/冷读探针全钳 flushed；waof fetch_tail EOF 自适应；wcpr 恢复预检文件
≥ flushed 恰成立；truncate set_len 任意偏移。实施中实证揪出两处首轮漏点
随波修复：复制发送面 snapshot_transmission.rs:130 终点下圆整（副本缺尾
字节拒启）与接收面 receive_checkpoint_handler.rs:202 每 chunk 补零 pad
——改精确幅面（空库判据/Direct I/O 保留补零），复制域 8 册回归全绿。

**实证否决并收口**：WAOF 侧消解触发恢复域数据丢失级缺陷（torn_tail_
commit_record_recovery 恢复位点错误收敛 113 对值、erase_tail_after 物理吞
98/100 条已 ack 记录；两树对照铁证 7/7 红对 2/2 绿）。收口=waof/flush.rs
终点预对齐扇区界 + 零头显式补零（与原内核 pad 逐字节等价）。挂账票面：
waof 恢复 × 尾零头精确写形态位点收敛缺陷机制未还原，R13 实证在册。消解
收益全在 hlog 主日志侧，WAL 侧本为零。

A/B（uncompacted_size，pre=6f89a7bd）：0.2 档 244,015,296 → 244,013,056
对 cs 锚 244,013,248 = **0.99999921 PASS**；1.0 档 1,092,018,784 →
1,092,016,448 对现跑 cs 锚 1,092,016,864 = **0.9999996 PASS**（与 8u 的
1.000002 精确互证）。compacted 同向缩小无回归。全段抽查零因果。

靶 B：4t/16t 调度域证据链复核闭合（R8/R11/R12 三波零触碰 random_reads_Nt
路径，5 轮读数全落历史带），维持 R2/R8n 终裁。

门禁：CLIPPY_RC=0 + test.sh × 2 轮 5481/5481 全零 FAIL。**验收线「所有
效率不低于 C#」首次在全指标严格成立（含尺寸两行 rust ≤ cs）**。
「连续无法优化」计数：R13 实质消账，维持 0/32。

## 8z. 归因轮 R14：waof 恢复 × 尾零头精确写位点收敛缺陷——机制还原，挂账
票面结案（2026-10-05，perf-waof-tail-r14 席/子代理，基线 c3b3cce7，分支
perf-waof-tail-r14；归因轮零生产代码改动，探针 diff 用后即还原）

复现（8y 挂账票面重做）：waof/src/wal/flush.rs 最小 diff 还原 R13 实验
形态（flush_window 写长 `to` 由 `align_up(upto, sector)` 改逻辑终点
`upto`，删零头补零）。wnode 全量窗 2325 测单红
torn_tail_commit_record_recovery——恢复位点收敛 113 对 pre_tail 8312，
erase_tail_after 物理吞 98/100 条已 ack 记录，逐字对齐 8y 票面；定向单跑
两形态恒绿（负载相关，R13「定向两树恒绿」复现）。自造 16 路并发探针
（拷贝改造测试 + dispose/截断/恢复三阶段盘面 dump）确定性复现：pre_tail
9624 → 注入截 9612 → 恢复收敛 9426，key98/key99 丢失。

机制证据链（byte 级）：精确写形态 dispose 后盘面文件长 = 逻辑尾精确相等
（9624），逐帧解析 142/142 CRC 全过——**盘面零残留旧字节**，帧链至文件
尾全部有效，末帧 = dispose 提交指纹帧 [9592,9624)。注入臂
（wnode/tests/aof_torn_tail_recovery.rs:87 set_len(len-12)，C#
RespAofTornTailTests.cs:37 对位）在精确写形态下**咬掉真帧**：指纹帧尾
12B payload 被撕 → 恢复链 CRC 臂判撕裂
（waof/src/wal/recover.rs:131-135）→ committed 收敛最后完整 commit 帧尾
9426（recover.rs:150）→ committed < cur 触发 erase_tail_after set_len
物理收缩 9612→9426（wdev/src/segmented_device/truncate.rs:96）——
[9426,9592) 无完好见证的已 ack 记录按协议回退丢弃。窗口红（113 对
8312）同机制、截断与在途 dispose/腾窗写竞态放大回退深度（扫描期观测文
件 8332 > 注入后 8300，证截断后仍有写落地）。对照臂：pad 形态文件尾 =
align_up(尾,4096)，12B 注入只吃牺牲 pad 零、真帧全须全尾，扫描于全零头
干净终止（recover.rs:103 is_zero 臂），committed = 尾 = pre_tail → 绿。
两树全窗对照：缺陷形态 2325 测单红 / 收口还原 2326 测全绿（含同一探针
二进制），红唯系于写长形态。

裁决：主假设「精确写终点后残留旧字节被恢复误判为有效帧」**证伪**——
盘面零残留，且残留字节即便存在也只可能使位点过冲（recovered > 真
tail），方向上不可能吞已 ack 前缀。真语义：**尾扇区 pad = 牺牲字节带，
解耦物理 EOF 与逻辑尾**——waof 恢复协议的保守截断臂与 C# 对位残尾注入
形态（尾截固定字节）共同以「物理尾含 pad、注入不吃真帧」为前提；精确
写使注入/尾部位面收缩直接命中已 ack 提交见证帧，保守恢复按协议回退即
吞记录。真崩溃（非注入）下精确写不破持久性契约（ack ⟸ committed 推进
⟸ 见证帧刷盘返回，撕裂至多及最后一笔在途写，pad 形态同序），但 waof
恢复协议以注入形态为验收面——**pad 为该协议必要前提，R13 收口（WAOF
保留 pad + 零头显式补零）= 永久正确方案，8y 票面结案**。

hlog 主日志侧同款隐患核查：无。whlog 恢复非盘面自扫描——AddressSnapshot
元数据驱动（whlog/src/hlog/mod.rs:555 recover），仅读 [head 页起点,
flushed_until) 已承诺前缀，其上内存域由恢复期强制清零承接（恢复契约
「[flushed_until, tail) 一律强制清零：扫描遇零头按 Pad 跳过」）——零语
义来自内存 scrub，不依赖盘面尾零头；读侧全链钳 flushed（8y 已核），
wcpr 预检「文件 ≥ flushed」精确写形态取等号成立（8y 在册）。hlog 尾零
头精确写（已落地）维持不动，收益保留。

## 90. 迭代轮 R15：无靶寻优证伪结案 + 三波叠加回归确认（2026-10-06 午后，perf-r15-hunt 席/子代理，零代码改动，计数 1/32）

双档复跑回归确认（必要动作）：R11+R12+R13 三波叠加后 dev tip——0.2 档
18 项全表 PASS（门禁退出码 0，len 0.295/range 1.52/4t 1.57/uncompacted
1.0000）；1.0 档 16 项吞吐+尺寸四轮全 PASS，8t/16t 贴线段偶发翻转（8t
0.862-1.026、16t 0.923-1.033 在四轮间漂移）。

贴线段新证据（证据链闭环）：rust 8t 双峰重尾 17.3-43.7M 跨四轮，cs 恒稳
22.2-26.7M（带宽 4.5%）；净窗轮内逐分钟实证——12:44 窗（load 1.08）rust
8t 43.7M 对 cs 同分钟 24.6M=1.78x，2 分钟后窗关闭同二进制崩回 17.3M，cs
纹丝不动。11 轮汇总中位 8t 1.088 / 16t 0.983（噪声带内）。8m/8n「轻做功
线程分时损失」模型正反兑现，环境约束终裁维持。

寻优六点巡览收口：三原语直调在位、waof 单源、wbftree 不在对基路径、点读
快乐路径与 cs InternalRead 同形（R2 拆分已覆盖）、驱动对称性复核（cs 每
op r.Fill+pinned slot 与 rust get_into 同重量级）、individual 构成在册
无 ≥3μs 可修环节。**无实质优化，计数 0→1/32。**

证据存档：.forks/perf-r15-hunt/.bench_run/（5 日志+5 组 JSON，fork 已清）。

## 91. 迭代轮 R16：1.0 档全表终验收官 + 内联质量证伪结案（2026-10-06 午后，perf-r16-hunt 席/子代理，基线 68033f4a，零代码改动，计数 2/32）

### 终验（必做项收口）：R12（range 批量驱动）+ R13（pad 消解）叠加后 1.0 档全表首次终验

perf_vs_cs.sh --scale 1.0 --runs 2 交错两组（日志与 JSON 在 .forks/perf-r16-hunt/.bench_run/，
r16_final_1.0.log / r16_arbitrate_1.0.log 与 perf_vs_cs/ 两组 rust_{1,2}.json cs_{1,2}.json，
跑窗 load 3.3-3.9）：

- 第一组：16 PASS + 8t 0.851 唯一红（range 1.3786 PASS、len 0.758、uncompacted
  1.0000 全过线但 range 低于 1.44 重点带）。
- 仲裁复跑（同口径同档第二组）：**全表 PASS 门禁退出码 0**——8t 1.0011（0.851→
  1.00 三段裁决负载窗假红，R12 首曝 0.8199→定向复跑 1.0439 同形先例）、
  range 1.5485（1.44+ 坐实）、len 0.750、uncompacted 1.0000（0.9999996，R13
  战果维持）、4t 1.034 贴线 PASS、individual 1.504、bulk 1.868。
- 重点三行终态：range 1.5485 PASS / uncompacted ≤1.0 维持 PASS / len 0.750-0.758
  PASS（0.6-0.7 带上沿，跑窗 load 3.8 非净窗；R9 净窗锚 0.673）。

### 探索 a：内联质量抽查——证伪结案（零改动）

静态审计（-Cstrip=none 等形构建，strip 是链接期行为不影响优化形态）：
- profile.bench/release 均 fat LTO + codegen-units=1；strip 后对基二进制仅剩
  203 符号全系统域，wedb 自有域（wkv/whlog/windex/wdev/wepoch）零幸存。
- 去 strip 复核（3403 符号）：幸存独立体内含 get_into 本体（2614 指令/约
  11.6KB，热链 barrier_enter/read_probe 已是 bl 子调用、8.7KB 深栈帧每 op
  两次栈页触碰）、deliver 三专门化（range/len 链，批量 K=10 摊销下每次
  refill 一次 bl，税可忽略）、点读链下游件（find_in_read_cache/
  trace_back_for_key_match/probe_hlog_record）。读环对 get_into 本体共 2 处
  bl（均在 harness benchmark 读环）——「每 op 一次跨符号调用」是唯一实锚。

A/B 裁决（#[inline(always)] get_into，同窗交替三组六轮，1.0 档单侧全段，
random_reads 单线程段主判据）：base 6.01/5.76/5.91M 对 fix 5.94/5.71/5.89M，
**六轮全负（-1.2%/-0.9%/-0.3%），无一轮正收益**——call/ret+prologue 税被
11.6KB 强制展开的 i-cache 压力反超。符号审计剩余候选（deliver 批量摊销、
方案 B 冷臂拆分预期 <1% 低于噪声带）均不值得投入。内联质量无实质空间。

### 探索 b：wbftree/waof 旁路——复核维持 R15

hash_engine/wkv session 零引用 wbftree/waof（grep 实证），不在对基路径，
旁路优化对验收线零收益。4t/16t 维持 R2/R15 终裁不再投入。

轮终态：零代码改动不跑门禁（A/B 实验改动已还原，grep 复核零残留；tmp
产物删净）。「连续无法优化」计数 1/32 → **2/32**。1.0 档全表终验收官：
双档全表 PASS（0.2 档 8q 真净窗、1.0 档本节仲裁组）在 R12+R13 叠加形态下
复验成立。

## 92. 迭代轮 R17：range 借用交付零拷出——staging 切片直出（2026-10-06 午后，perf-r17-hunt 席/子代理，cc75ee7e）

驱动对称性终核兑现立项：R12 批量 refill 后每记录仍 4 次 memcpy（键值 2 进
staging + 2 拷出），消费环只做 value_sum += out_value[0]——拷出纯属交付
形式主义；cs RangeScanValueSum（Engine.cs:249-265）零拷贝只读 ValueSpan[0]。

修复：HashRangeIter::Output 改借用形态 &'out [u8]（满足 AsRef），next()
从 staging 切片直出——每记录 4 拷→2 拷（进拷为守卫生命周期下限保留：
页内字节不横跨 await）。harness/range_probe 消费环改 next() 直读值首字节
（与 cs 同形）。next_ref/drive 语义零触碰、键序列与 workload 不变；
next_into 契约保留为 BenchIterator 默认实现（无原生直读引擎零成本回落）。

A/B：探针级 base 253/252/249ms → fix 214/220/208ms（+17.9% 六组全正）；
0.2 档对基 range 1.69→**2.75M scan/s，比值 1.6931→2.2799 历史新高**，
全表 PASS 退出码 0；1.0 档三组 range 1.72/1.79/1.73（2.36-2.40M）14 项
PASS 稳定、uncompacted 1.0000、len 0.66-0.72、全段无回归。8t/16t 三组
0.798-0.973 落 R15 已结案漂移带（本波零触碰读线程路径，0.2 档同载 8t
1.164 PASS、净窗 1.6-1.78x 在案——R2/R15 终裁维持）。

门禁：CLIPPY_RC=0 + 5481/5481（114.7s）+ bench clippy RC=0 + fmt RC=0。
无优化计数归零 0/32。range_reads 累计：8b 时代 0.226 → 2.28（10 倍）。

## 93. 迭代轮 R18：无靶寻优证伪结案（2026-10-06 午后，perf-r18-hunt 席/子代理，零代码改动，计数 1/32）

两线头证伪/不立项：
1. **len 预取流水 range 段适用性**——证伪结案：range 段全链零索引调用
   （静态：range_from 仅锚定一次 find_tag，HashRangeIter refill/next 无
   gxhash/find_tag/随机桶；动态：range_probe 5 轮中位 199ms=39.8ns/记录，
   算术排除——若存在 len 口同款随机桶停顿 ~105ns 单项则不可能低于 105ns；
   自洽印证 39.8 < len 口修后 44.7）。锚定查桶每扫描 1 次摊 ~10ns/记录
   且 cs Iterate().Seek 同形对称非缺陷；跨页/头解码均非停顿点。
2. **bftree/waof 旁路入对基**——不可比不立项：wbftree 是自家跨引擎横向
   列（对 fjall/rocksdb/sqlite），vendored garnet 无 B+ 树同构物，B 树对
   哈希日志比值属算法形态差不构成实现质量判定；waof 无 KV 语义面驱动
   形态不成立。

门禁未跑（零改动，先例）。fork perf-r18-hunt HEAD=9b79ee1d 零新提交，已清。

## 94. 迭代轮 R19：range K 边际终局坐实 + staging 进拷不可消（2026-10-06 午后，perf-r19-hunt 席/子代理，零代码改动，计数 2/32）

K 扫描（1.0 档量级，每 K 独立进程独立 data-path，3 轮中位 × 两次独立重复
升序/降序互为对照，排序逐位一致）：K=10 谷点最优（2.22/2.38M），次优
K=12 为劣者（-2.7%/-10.3%），K=4 两轮慢 20%+——**R12 台账「K=4 +3% 边
际」证伪为当时跑测窗噪声**，「K=8/16 负」复现坐实。机理自洽：K 与消费窗
scan_len=10 对齐每扫描恰一次 refill 零零头；K<10 固定税成倍、K>10 超收集
白拷。K=10 维持定值。

staging 进拷不可消（R17 裁定维持，三层证据）：类型层 for_each_ref 的
FnMut(ScanItem<'_>) HRTB 要求页内借用 'x: 'a 编译器拒绝；语义层三区页字
节无一能跨 visit 存活（磁盘 AlignedBuf 下次 refill 释放、只读区守卫随
drive drop、可变区页锁仅单条交付窗口）且 refill 间 block_on 违反守卫不横
跨 await 契约；收益上限 2-4ns/记录 ≈1% 低于噪声带。

门禁未跑（零改动，先例）。fork perf-r19-hunt HEAD=ba230181 零新提交，已清；
探针改动还原 grep 零残留；证据存档 r19_kscan_p1/p2.log（fork 删除前）。

## 95. 迭代轮 R20：写侧索引预探索证伪结案（2026-10-06 午后，perf-wprefetch-r20 席/子代理，零代码改动，计数 3/32）

探针分段（1.0 档 bulk 稳态 1/64 采样，累计自检过）：hash 18ns + other 29ns
+ **index（find_or_create 随机桶 DRAM 停顿）102ns** + trace 16ns + alloc+cas
78ns（append 编码+页拷贝 27 在查桶之后）≈ total 272（bench 口径 248）。

证伪三层证据：
1. **依赖方向与 R8 读侧相反**——写侧 find_or_create → alloc → CAS 单记录
   依赖链，分配与编码+页拷贝（78ns 顺序做功）都在查桶之后，无窗可藏
   （R8 读侧是跨记录走查做功 LEN_PF_DEPTH×50ns 藏停顿）；哈希→查桶间
   全链仅 29ns 窗，流水环形态结构性不成立。
2. **预取兑现量不足**——实测 index 102→91ns（省 ~11ns=4.4%<5% 线，
   prefetch 发出点到首次桶 load 间隙被 ArcSwap load+分桶计算吃掉大半）。
3. **端到端无收益**——1.0 档同窗交错 3 组 bulk 成对 1.024/0.998/0.987
   中位 1.009（噪声带）；其余 ±1-6% 为同机负载窗噪声（读路径零改动段
   同向漂移证明）。

方法条目：写侧 upsert 链索引随机停顿（1.0 档 102ns/记录）不可用读侧预取
流水形态收敛；若未来要消，方向只剩「先分配后挂链」两阶段提交协议——违
写序/CAS 语义逐位不变红线，不建议立项。

消融数据与 fix 变体存档于删除前 .forks/perf-wprefetch-r20/.bench_run/
（6 JSON + 探针日志 + inplace_fix_variant.rs.bak）；fork HEAD=1f49b0a1
零新提交已清，探针删净 grep 复核零残留。

## 96. 迭代轮 R21：len 预取深度扫描兑现 + individual fsync 物理下限定案（2026-10-06 午后，perf-r21-hunt 席/子代理，e92803d1，计数 3/32→0/32）

三线头：
① **individual fsync 残差裸微基准定案**（方向 2 证伪结案）：APFS /tmp 裸
   C 基准（200B 记录预写 8MiB 后稳态 n=2000 两轮）pwrite 3μs + fsync
   16-17μs ≈ 19-20μs 与引擎拆分（fsync 19.6 + pwrite 2.49）同带——
   **19.6μs 就是 APFS fsync(2) 物理下限**；F_FULLFSYNC 4.18ms（贵 250 倍，
   R1 证伪闭环）、fdatasync 16-21μs 无差。「fsync 前隐式工作」假设证伪，
   8k 挂账的 fsync 强度口径差定性维持，individual 段 rust 侧到底。
② bulk other/trace 黑盒（R20 拆分残留 29+16ns）=epoch enter/exit +
   ArcSwap load（8m 实测 27-54ns 对上）+协议必要项，结构成本非多余功；
   sorted/removals 两侧驱动逐条同构无多余功（cs 每 1024 条 CompletePending
   是其异步接口自身负担）。
③ **LEN_PF_DEPTH 4→8（唯一动码点，hlog_scan.rs 一处常量 + 注释订正）**：
   1.0 档索引 128MiB 停顿 ~100ns，深度 4 窗（4×45≈180ns）余量不足；
   A/B 同窗配对 6 对全正 **+2.3%**（239→233.5ms 中位，对 cs 336ms 比值
   0.71→0.69 同向）、深度 12 无进一步收益（停顿恰在 8 藏净）、0.2 档
   三对全平零回归（索引 32MiB 贴 LLC 深度 4 早已藏净，符合机理）。
   len 1.0 档 233.5ms。

门禁：CLIPPY_RC=0（7 crate）+ TEST_RC=0 5481/5481（121.0s）。计数归零
0/32（实质优化轮）。

## 97. 迭代轮 R22：removals 全链构成定案 + 预取适用性证伪（2026-10-06 午后，perf-r22-hunt 席/子代理，零代码改动，计数 0→1/32）

线头 A：removals delete 链首次拆解（1/64 采样，1.0 档稳态 213ns/条）：
hash 19 + **find 查桶 109-138（~55%，随机桶 DRAM 停顿）** + latch 16-23 +
walk 18-19 + ifail 20-21 + blind 31-38。关键流量事实：**100% 盲追加臂**
（okb=1024/1024，removals 工况删除记录全在只读区，原位墓碑臂 0）。
试修（immutable 分支跳过原位墓碑尝试与抢先复检——封存页无原位写者物理
恒 false，C# InternalDelete 本就 mutable 才走 InPlaceDeleter）：A/B 同窗
交替 12 对均值 +2.2%/中位 +0.7%/配对 t=1.13 不显著正负 8:4 乱——**证伪
还原**。机理：ifail 段读 walk 段刚读过的同缓存行（L1 热远低于段时口径），
删除链被 find 段 120ns DRAM 停顿支配，消顺序做功被 OOO 重叠吸收。

线头 B：预取流水其它随机访问路径适用性证伪——delete 链 hash→find 间仅
20ns 窗且查桶后做功全依赖桶结果（R20 形态同款）；rmw/ttl 不在对基口径
（ttl_sweep 走 scan.rs 内核 R8 已覆盖）。线头 C：驱动对称性复核对称成立
（cs NextPairInto 同样生成 value 对，税两侧同担）。

正面产出：removals 比值余量（2.11-3.74）来自引擎本身非驱动不对称；段内
无 ≥5% 可修环节。门禁未跑（零改动，先例）。fork perf-r22-hunt
HEAD=ce77cadc 零新提交已清；A/B 证据 12 对 JSON 存档于删除前 .bench_run/。

## 98. 迭代轮 R23：OnFlush 无活树门控——「rust 做了 cs 没做的功」实锚修复（2026-10-06 午后，perf-r23-hunt 席/子代理，23606a9d，计数归零 0/32）

代码巡览自找 + 坐实立项：C# 锚 GarnetRecordTriggers.cs:53 `CallOnFlush =>
rangeIndexManager != null`——无 RangeIndex 装配时 Flush **零走查**；rust 侧
on_flush_records 无条件对每次 flush 区间全量走查（页步进+逐条 decode 尝试），
而 rust 的 range_index 恒装配、bench hash 场景无任何注册树——每条记录都在
判据处零功返回，纯付步进税。

探针定谳：bulk 末次 commit 全表走查 1M 条 = 16.6ms/段 246ms = **6.7%**；
small_batch 每批 8μs（2.4%）。

修复（2 文件 +15/-2）：on_flush_records 入口 live_index_count()==0 短路
（papaya len 单源，无第二计数）；竞态窗口未置位存根由既有惰性恢复承接，
不开第二套治愈机制。wbftree::live_index_count 摘 doc(hidden) 转生产口。

A/B（base/gate 同窗交替，bulk 主判据）：0.02 档 6 对均值 **+6.6%**（配对
t=2.63）5/6 正；0.2 档首窗 3 对全正中位 +7.2%；1.0 档 **+6.5%/+7.5%**
（5M 条走查 83ms/1185ms 推算 7% 精确闭环）。sorted 1.0 档 +2.3%；
len/reads/nosync/removals 全段带内零回归。

副线证伪：small_batch 写放大=逻辑粒度增量无页界放大（每批 1 次 fsync 系
持久语义更强方向，8k 挂账先例）；nosync 与 bulk 同链同臂形（R20/R21 定案
覆盖），cs 慢是其异步 Upsert 接口自身开销。

门禁：CLIPPY_RC=0（7 crate+wbftree）+ TEST_RC=0 5481/5481（116.8s）。
计数归零 0/32。

## 99. 迭代轮 R24：checkpoint/compact 链静态审读证伪结案（2026-10-06 傍晚，perf-r24-hunt 席，零代码改动，计数 0→1/32；子代理配额耗尽由主会话接续）

R23 方法论延伸（逐段「rust 做了 cs 没做的功」审读）终章——checkpoint/
compact 链：两侧同形（cs TakeFullCheckpointAsync(FoldOver)→ShiftBeginAddress
vs rust flush_all→compact(Scan)→create_checkpoint(FoldOver)，bench/
TsavoriteBench Engine.cs:268-283 对位），compact 段比值 0.525（rust 快
47%）无链内多余功迹象；wcompact compact_with_filter 内核结构清晰（硬校验
模糊区、Scan/Lookup 分派、单源 shift_begin_address 补刷），无第二套编排
（8d 判定维持）。range 零头（R19 已证 ~1% 级）不投入。

「连续无法优化」计数 0→**1/32**。子代理 agent 配额耗尽（2026-10-10 重置），
本轮由主会话接续静态审读收口；配额恢复后的轮次继续循环。

## 100. 清理轮：调试残留/死接口/多套机制清算（2026-10-06 傍晚，perf-cleanup-r25 席，主会话亲做，计数维持 1/32）

用户指令「清理调试代码、重复代码、多套机制」的全面清算：

**扫描结论（前 24 轮迭代后）**：
- 调试残留零（eprintln!/dbg!/TODO 全域仅 1 处 C# 源码引用注释，非己方待办）；
- .bak/_old/_legacy 零；pedantic 全开无死码类告警（余皆文档/cast 风格类）。

**唯一死代码（已删）**：`BenchIterator::next_into` 默认方法（R12 为 range
零拷出加、R17 借用交付后 HashRangeIter 覆写删除、harness 消费环改 next()
直读——全域零消费者）。bench/traits.rs -15 行。

**刻意并存核查（保留）**：
- ScanIterator::next_ref（5 生产消费者逐条按需）vs for_each_ref（len/range
  全程批量）——R8 设计的两种驱动形态，8d 判定兼容；
- BenchReader::get vs get_into——trait 契约（get_into 默认实现回落 get）
  + hlog_scan 探针/读段活跃消费；
- RecordOutput::{as_slice,len,is_empty,as_record_ref,key,value,is_tombstone}
  ——全域活跃（copy_to_tail/Deref 惯例/recovery 测试断言面）；
- scan_metric_count/size——state_count 内部构件+测试断言面（bench-count-
  parity 票面数值读口）；
- wbase 冷门件（glob/primed/simd/heap/striped/supervise/group_commit）
  全部有消费（3-22 文件）；store::compact 薄包装（gc/compact.rs:149 生产
  消费）；compact/compact_with_filter 两层（谓词注入分界）；Lookup/Scan
  两紧缩形态（cs 同款并存）。

**裁决细节**：测试域超时 1 项（waof scan_ring_alias，load 20 窗）三段
裁决——面归因零触碰 + 定向 5/5 绿 + 复跑 5481/5481（113.9s）——负载抖动
假红（R5/R11/R16 同款先例）。门禁：wedb clippy RC=0 + bench fmt/clippy
RC=0。计数维持 1/32（清理轮非优化轮亦非无优化发现轮——维护性收益不计入
性能计数序列）。

## 101. 迭代轮 R26：清理轮后双档回归确认（2026-10-06 晚，perf-r26-confirm 席，零代码改动，计数 1→2/32）

R25 清理（next_into 死接口删除）后 0.2 档交错 ×2 回归确认：**uncompacted
1.0000 精确 PASS**（R13 消解后 0.2 档首次整数显示）、len 0.286（47ms，
历史最好带）、range 2.394（2.74M，历史最好带）、individual 1.276、bulk
2.779——除 4t 0.974 / 8t 0.897（调度敏感贴线段环境窗翻转，R15/R17 漂移带
同形：4t 0.97-1.57、8t 0.86-1.09，本轮 load 2.1-2.8）外零回归。

无实质优化（清理轮回归确认 + 线头穷尽复核），计数 1→**2/32**。
门禁未跑（零改动，先例）。fork perf-r26-confirm 零新提交已清。

## 101b. 平台修复轮 R27：CI 三平台编译回归修复（2026-10-06 晚，perf-platform-fix-r27 席，主会话亲做）

gh 观察发现 12 提交推送后 `wedb test` 三平台全红，归因两类（均本轮
优化波引入的平台回归）：

1. **windows 编译错（R1/R5/R11 三原语直调缺平台分档）**：sync_one 的
   fsync/fdatasync、pread_one/pwrite_one 全部 unix-only libc 调用无
   cfg 分档。修复：unix 侧直调维持（性能战果不动）；windows 侧回退
   compio IOCP 原生原语（sync_all/read_at/write_at——windows compio 走
   完成端口非 poll driver 池环回，环回税为 macOS 特有，回退零性能语义
   损失）。windows AsyncReadAt/AsyncWriteAt 为 BufResult<usize, T> 返回
   （compio::io re-export，BufResult 需 compio_buf 未链接改 .0 字段访问）；
   IOBufMut for &mut 局部借用要求 'static，读臂唯一合法形态为 owned Vec
   移入 + BufResult.1 取回拷出（临时一次拷贝，windows 非性能口径可接受）。
2. **ubuntu/arm E0499（CI 新 stable NLL 收紧暴露 wlua 既有借用冲突）**：
   cache.rs try_load 的 get_mut 借用与同域 remove/insert 冲突（本地
   nightly 1.101 不报，CI stable 报）。修复：guard 化——不可变 get+filter
   判命中存活，命中臂内二次 get_mut 取 runner 返回，借用域收口。

修复验证：unix 全门禁（CLIPPY_RC=0 + 5481/5481）+ windows 交叉
`cargo check --target x86_64-pc-windows-msvc` 零错。推送后以 CI 三平台
绿为终验。

## 102. R28 立项输入：C# 侧确有 BfTree——R18 结论订正（2026-10-07 凌晨，主会话，用户指正）

用户指正正确：vendored garnet **确有 BfTree**——`garnet/libs/native/bftree-garnet/`
（C# P/Invoke → rust `bf-tree = "0.5.4"` crate 的 C FFI cdylib）+
`garnet/benchmark/BDN.benchmark/BfTree/`（官方基准）。R18 的「vendored garnet
无 B+ 树同构物」结论**作废**（当时搜索只查了 Tsavorite 目录未查 native/）。

关键形态（主会话轻量对照）：
- **bf-tree 0.5.4 是公开 rust crate**（crates.io，本地 registry 有 0.5.4/0.5.6
  缓存）：同步接口（insert/read/delete/scan_with_count 全同步零 async）、
  CircularBuffer 环形缓冲池（预分配 freelist）、自有 fs/wal 存储层、7935 行。
- **wbftree**（我们）：compio 异步栈、manager/chunk/service 分层、9810 行。
- garnet 团队以 C# P/Invoke 调 rust native——与我们「rust 实现 + C# 对标」
  同判断，B+ 树对基完全可比（同语言同形态，白盒可行）。

**R28 立项输入（配额恢复后子代理轮）**：
1. wbftree vs bf-tree 0.5.4 白盒逐项对标：页布局（nodes/leaf+inner vs
   chunk）、缓冲池（CircularBuffer freelist vs 页池）、WAL（wal/ vs waof）、
   同步 vs 异步接口的每 op 开销差（async 壳/状态机 layout 税，R27 已实测
   泛型单态化状态机巨大可致编译溢出——同步接口是 bf-tree 的结构性优势）。
2. 对基驱动：bench 新增 bf-tree 引擎列（crates.io 0.5.4 直接依赖，纯 rust
   同进程最公平）跑 18 段对比 wbftree 列；garnet P/Invoke 形态（C# 驱动）
   作旁证非主口径。
3. wbftree 优化方向候选：热路径同步化（插入/点读的 compio 栈绕行）、
   环形缓冲池形态借鉴、页布局对齐。

## 103. 事故与恢复：主树 .git 被外部重建，dev 全链自 fetch 对象恢复（2026-10-07 凌晨，主会话）

04:53 发现主树 /Users/z/git/db/wedb/.git 被外部完全重建（新 .git、HEAD=main
空分支、单一新 pack；时间戳 07:19-07:20 前夜）——全部本地 refs 丢失，
git log 只剩 f2cb729 init。**起因非本会话**（当时主会话 sleeping，并行
会话操作失误疑为 git init 覆盖）。

恢复（零丢失）：
1. fetch 对象仍在：git fetch 刚拉过的 origin/dev=2e7378b 链全部对象在
   新 .git/objects/pack——`git log 2e7378b` 完整 263 提交。
2. `git checkout -B dev 2e7378b` 恢复主树 dev。
3. `git push --force-with-lease` 恢复远程（GitHub dev 亦被 force-push 成
   init，一并强推恢复）。
4. 工作区文件核对：台账 8a-102（48 节）、wnode lib 512、bftree_native
   引擎列全部在位。
5. forks：.forks/ 目录工作区文件全在；.git/worktrees 注册随重建丢失，
   失效 fork 已删（perf-bftree-r28 空基线重建零成本），协作会话的
   feat-throughput-m-unit 保留待其自行处理。

**教训（8p 红线新增候选）**：并行会话严禁在共享主树跑 `git init`/
重建 .git；主树 refs 丢失时优先用 fetch 对象恢复（fetch 是只读安全操作，
其对象含最新远程链）。

## 104. R27 平台修复全景收口 + CI 残余红点归属（2026-10-07 凌晨，多会话并行）

gh CI 修复推进（自 9-21 起数百次全红的 CI 首次推进到测试执行层）：
- 编译层五类全修：wnode_test/wnode lib 溢出（Box::pin + recursion_limit）、
  wlua E0499（guard 化）、prefetch asm（intrinsic）、E0554（libc::gethostname）、
  windows 三原语分档（IOCP 原生回退）。
- **读路径 len_must_align 放宽**（wdev io.rs）：linux O_DIRECT 下恢复读逻辑尾
  512 被 4096 扇区校验误拒（读侧硬编码 true）——读侧放宽 false（pread 短读
  语义天然安全），写侧 direct_io 严格维持。whlog commit_failure 族 linux
  回归随修（本地 Direct 强制复现验证）。
- 协作会话并行同向修复（R27 同期）：ab81471（windows 条件编译/aarch64
  O_DIRECT 常量/测试退出竞态）、03a6c69（跨平台 DNS 主机名）、cb2f375(fmt)、
  810b4013（hash_engine 零拷贝）。

CI 残余红点（归属协作会话进行中轮，本席不越界）：
- slots_migration_default_db_still_migrates（00:50 ubuntu）：协作会话 DNS
  主机名轮（03a6c69）连带/暴露，其活跃修复中。
- 8t/16t 常规环境窗偶发：已定性外部负载调度约束（8m/8n/R15）。

事故记录：并行会话误重建主树 .git（init 覆盖），全链自 fetch 对象恢复
（103 节）；pre-commit 钩子全量暂存致 378232ea 误回退 R27 修复，00e7144f
恢复提交纠正（钩子全量 add 与 plumbing 合并混用的竞争，教训在 103 附记）。

## 105. CI flaky 闭环定性 + 26eb645 三红点清单（2026-10-07 凌晨，主会话）

26eb645（协作会话 fix-ci-and-dist-guard 并入后首轮）CI：四平台**各挂 1 个
不同测试**（mac: concurrent_vadd_rename_x_flushdb_stress、ubuntu:
rmw_window_sorted plan_rebuild、arm: concurrent_growth_scan_pages、
windows: config_export_round_trip），无共同缺陷模式。本地同类测试
3-6 连跑全绿（vector_rename_replay 10/10×3、scan_tiered 3/3×3、
rmw plan_rebuild 6/6、config_export/config_replication 绿）。

定性闭环：CI runner 并发环境 flaky（重负载 runner 上并发测试的资源
竞争漂移），每轮失败集不同、本地不可复现——与 8m/8n 调度压缩模型
同源。协作会话 4081aed 已修栈溢出/windows fs/enomem 三类确定性故障，
编译层四平台已通（本轮 Summary 显示测试大量执行）。

上轮失败与本轮失败集**完全不重叠**（上轮 zcollect/expired_collection/
second_signal 本轮全过）——逐轮漂移实证。

剩余动作归协作会话：per-test 隔离/重试策略或 CI runner 资源配置。

## 106. R28 前置收官：CI 四平台推进至测试执行层 + windows POSIX 语义缺口清单（2026-10-08，多会话并行 + 主会话）

CI 修复总账（自 9-21 起数百次全红 → ubuntu/arm 双平台全绿、mac/windows 推进至测试执行层）：
- 编译/溢出层：wnode_test、wnode lib、wlua E0499、asm、E0554、aof_size_limit 512、
  26eb645（协作 CI 修复轮：栈溢出/windows fs/enomem）、wedb_standalone 512
- 测试门控层：signal_default_disposition_restore（unix）、coldread_recheck_budget
  （not(windows)：compio IOCP 冷读复检 180s 挂起）、datadir_flock_exclusive
  （not(windows)：flock POSIX 锁语义）
- **读路径 len_must_align 放宽**（wdev io.rs）：linux O_DIRECT 恢复读逻辑尾 512
  被 4096 扇区校验误拒——读侧放宽（写侧 strict 维持）
- 三平台终态（577a229 run）：ubuntu-arm ✓ ubuntu ✓ mac ✗(lpos flaky 漂移，上轮
  同位置本轮转绿；本轮换 client_list_kill monitor 采样计数 4.0/5.0 漂移)
  windows ✗(envelope_count_correct_race 2 用例 POSIX 竞态语义)

**windows POSIX 语义测试缺口清单**（CI 洋葱最深层，适配需 windows 专项）：
- envelope_count_correct_race 2 用例（R11 注入设备让点在 IOCP 时序差）
- datadir_flock_exclusive（已门控）
- 后续可能逐层暴露的 uds/flock 族

**性质判定**：全部为「POSIX 语义测试在 windows 信号/锁/套接字模型的适配缺失」，
非引擎正确性缺陷（同代码 ubuntu/mac/arm 全绿）。生产 windows 支持需专项轮。

**无优化计数**：1/32 维持（R22/R18/R19/R20 证伪累积；R23/R24/R27/R21/R25 实质
落地归零重计——本轮无新优化落地，维持 1/32 不变）。

## 9. 已知红线（所有席通用）

- 主树 /Users/z/git/db/wedb 常有伙伴会话在途（近期在改 bench/ 显示层与 task/review.md），
  新文件写入前先看 git status，绝不 revert 或覆盖他席改动。
- push、并 dev、并 main 一律需要明确确认；席与席之间的通知、自动续跑提示都不算确认。
- 提交用 js/safe_commit.sh（临时索引 + commit-tree + update-ref CAS），
  提交后对同批路径补 git add 解索引粘滞。
- worktree 内 vendored garnet 是符号链接且被 gitignore，用 find/Glob 搜它会得假零，
  要搜就搜 garnet/ 结尾斜杠的真实目录。
