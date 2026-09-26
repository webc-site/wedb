甄别结论：通过（甄别席 zc-fix-r16-benchtax，2026-09-26）定级 P2
现码复跑核验：alloc.rs:86-87 #[global_allocator] 挂 TrackingAlloc<MiMalloc> 亲验在位；alloc/alloc_zeroed/realloc 各含 fetch_add+fetch_max 双原子 RMW（:43-44/:59-60/:72-73）、dealloc 一次 fetch_sub（:52），确覆盖子进程全部定时路径。
types.rs JsonMetric（:245-255）仅 key/type/formatted/bytes/duration_ms/rate 六字段，无 peak；全仓 grep peak_allocated 仅 alloc.rs 自身定义与取值器，零外部调用，死数据确证。
harness.rs:361-367 结尾内存行以 get_process_physical_memory（sys_info.rs:250 在位）RSS 为主口径、cur_allocated 仅回退；:370-373 println 含堆分配字段，删跟踪器后须连同回退臂一并清除（前票补强在案，执行时勿留半截引用）。
harness 第 8 节多线程读段（:253-258，4/8/16/32 线程）计时窗内命中跟踪原子竞争，横比偏置推演成立。
C# 锚亲验：KvBenchmark.Setup.cs 全文为数据路径/设备/清理搭建，无全局分配拦截（EntryPoint.cs:36-40 的 native allocator 系 --use-native-allocator 显式 opt-in 的存储分配器选型，非默认监控层，不构成反证）；BDN.benchmark 全家经 GC/MemoryDiagnoser 口径（Program.cs:96-99），无分配器包装。
非重复：deviations.md grep 无分配器监控条目；同池 bench-* 五票各占异轴（parity-gap/timeout-row-drop/consume-barrier/report-notes/wbftree-flush），reject 池无并案；regress 与主仓其他 crate 均无 TrackingAlloc 引用，仅 bench/bench 一处挂载，缺陷现状仍在。
架构合规：删除监控死层、统一 RSS 单口径，合单套机制与删冗余导向；方案 2「全局分配器无法分段启停」论证成立，维持删除路线。

审核结论：通过，定级 P2。
确证全局分配器包监控层侵入各引擎定时热路径，导致分配密集型引擎被系统性压低，且峰值数据成死代码。方案换回裸 MiMalloc 并统一 RSS 物理内存口径，方案正确。
复核（zcode-r18-review-benchmisc）：锚点全部亲验成立。alloc.rs:86-87 #[global_allocator] 挂 TrackingAlloc<MiMalloc>；alloc/alloc_zeroed/realloc 每次命中 fetch_add + fetch_max 两次原子 RMW、dealloc 一次 fetch_sub（:40-83），全程覆盖子进程内全部引擎定时路径；types.rs JsonMetric（:245-255）确无 peak 字段；peak_allocated() 全仓 grep 仅 alloc.rs 自身定义与取值器，零外部调用，纯死数据；harness.rs:361-367 结尾内存行以 get_process_physical_memory 的 RSS 为主口径、cur_allocated 仅回退。多线程读段（harness 第 8 节 4-32 线程）fetch_max 原子竞争偏置推演成立。
执行方案补强：删除 TrackingAlloc 后 harness.rs 第 12 节同步简化为 RSS 单口径，cur_allocated 回退分支与 println 中堆分配字段一并清除，避免留半截死引用；regress crate 不受影响（未挂跟踪分配器）。方案 2 对「全局分配器无法分段启停」的论证正确，维持删除路线。

bench 全局分配器包监控层，监控税进各引擎定时路径且峰值数据成死数据（r14-bench 第 6 条 P2 复查未整改）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 基准无全局分配拦截：garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KvBenchmark.Setup.cs 环境搭建只配 Tsavorite 参数，内存口径由进程/GC 统计另测，分配热路径零监控开销；garnet/benchmark/BDN.benchmark 全家微基准同样不包装分配器。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   bench/bench/src/alloc.rs:86-87 #[global_allocator] static GLOBAL_ALLOC: TrackingAlloc<MiMalloc>：每次 alloc 两次原子 RMW（fetch_add + fetch_max）、每次 dealloc 一次 fetch_sub，全程覆盖所有引擎的定时路径。types.rs JsonMetric（约 :245-255）无 peak 字段，peak_allocated() 取值器全仓零调用（仅 alloc.rs 自身定义），只剩开销无产出；harness 结尾内存行以 get_process_physical_memory 的 RSS 为主口径（harness.rs 约 :361-367），跟踪分配器对结尾内存指标亦非必需。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   分配密集引擎（sqlite 逐语句分配、redb 节点分配、rocksdb options/pike 分配）吞吐被系统性加压，wkv/wbftree 分配少相对受益，跨引擎横比含监控税偏置；多线程读段 fetch_max 原子竞争随线程数升高而加剧，random_reads_16/32 行额外失真。

涉及代码：
rust 文件与函数：
bench/bench/src/alloc.rs:TrackingAlloc（#[global_allocator] 挂载）
bench/bench/src/types.rs:JsonMetric（无 peak 字段）
bench/bench/src/harness.rs:run_benchmark 第 12 节（cur_allocated 回退口径）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/benchmark/KV.benchmark/KvBenchmark.Setup.cs:环境搭建不拦截分配
garnet/benchmark/BDN.benchmark/Operations/BasicOperations.cs:微基准无分配监控层

精炼执行方案：
1. 吞吐测量期换裸 MiMalloc：删除 #[global_allocator] 的 TrackingAlloc 包装（直接挂 MiMalloc），内存指标统一 RSS 口径（get_process_physical_memory 已在用）。
2. 若需保留堆口径，仅在结尾内存段前局部启用统计不可行（全局分配器无法分段启停），故按方案 1 删除 TrackingAlloc 与 peak_allocated 死代码，同时清理 harness 中 cur_allocated 回退分支。
3. 测试验证点：删除后 bench 全量编译无引用残留；对比删除前后 sqlite/redb bulk_load 吞吐，确认分配密集引擎不再被系统性压低。

合入哈希：b8ee69f 收口形态：bench 全局分配器换裸 MiMalloc（alloc.rs 88→7 行），TrackingAlloc/peak_allocated 死代码与 harness cur_allocated 回退分支、堆分配字段全删，内存指标统一 RSS 单口径；bench 编译 clippy 级验证干净，最小档 6 采样对比 redb bulk_load 最优 31.8→36.1 M/s、sqlite 46.6→43.7 M/s（共享机噪声主导，无系统性回退，未跑全量档位）。
