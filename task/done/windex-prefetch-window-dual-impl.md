优先级：中
分拣注记（qw.design 第 11 轮条 3 拆出；浅核 2026-09-19：windex/src/table.rs:48 PREFETCH_WINDOW、:623 find_tag_batch_by_hash 与 wkv/src/session/raw/batch.rs:15 BATCH_READ_PREFETCH_SIZE 双双在场；台账 grep PREFETCH/prefetch_batch/acquire_hash_locks 无在册命中，非重复）

12 项硬件预取窗口两处定义两套实现：windex 单点（对标 C# PrefetchSize=12）零生产消费者，
生产批量读在 wkv 手写第二套并跨 crate 摸 windex 私有布局
问题：windex/src/table.rs:48 pub const PREFETCH_WINDOW = 12（注释「1:1 对标 Garnet Tsavorite
PrefetchSize = 12」）与 :593 batch_pipeline（先热前 12 桶、每处理第 i 项前预取第 i+12 项）是 C#
ContextReadWithPrefetch 的 1:1 落点，但其仅有的两个外部入口 lookup_candidates_batch_by_hash :609、
find_tag_batch_by_hash :623 生产零消费者（读者仅 windex/tests/main.rs:119/:128/:133/:141），
同族多键锁 acquire_hash_locks :698 同样只被 windex/tests/index/latch_concurrency.rs:470、:498、:659 消费，
is_latched_shared bucket.rs:263、is_locked table.rs:572、as_aligned_slice / as_aligned_mut_slice
ram/direct_vm.rs:114/:125 一组白盒谓词亦仅测试面；真正的生产批量读另起一套：
wkv/src/session/raw/batch.rs:15 pub const BATCH_READ_PREFETCH_SIZE = 12（同一 C# 常量第二处定义）+
:265 prefetch_batch_probes 手写两级预取（第一级哈希桶 cacheline、第二级 [head,tail) 内记录物理地址），
并绕过 windex 封装直取 index.mask（:277）/ index.buckets.as_ptr()（:286）/ get_unchecked。同一 C# 单点在 rust
成为两份实现 + 两个 12 常量，且规范那份是死码。附带文档漂移：windex/README.md:33、:79 与
windex/readme/en.md:21、windex/readme/zh.md:22 宣称的批量入口名 find_tag_batch / lookup_candidates_batch
在源码中不存在（实名是 *_by_hash 两形态）。
修法：把预取核收口为 windex 单点并让其返回键哈希+首地址探针数组（现 KeyProbe 形态上提），
wkv batch.rs 改调该核、删本地 12 常量与手写下钻；生产多键锁走 acquire_hash_locks 或删；
白盒谓词族收 #[cfg(test)]；README 命令面按实名修正。
c#：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:518 ContextReadWithPrefetch
（:561 const int PrefetchSize = 12、:581 桶预取、:595 记录地址预取）；
garnet/libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:94 / TransactionalContext.cs:472
ReadWithPrefetch 为唯一入口

落地（分支 fix-windex-prefetch，commit 7827ba18，dev 1986e3ee）：
1. 预取内核收口 windex 单点 HashIndex::prefetch_batch_probes（windex/src/table.rs）：
   逐键单次算哈希 → on_hash 交回调用方推进协作式扩容分块 → 第一级预取主桶 cacheline →
   第二级 find_tag_by_hash 装载首地址并交 prefetch_record 回调预取记录物理内存，
   产出 PrefetchProbe{hash, first_addr} 定长窗口数组（原 wkv KeyProbe 形态上提）。
2. 窗口 12 单点定义移到 windex/src/prefetch.rs 自由常量 PREFETCH_WINDOW
   （HashIndex::PREFETCH_WINDOW 无法被 use 再导出，故改为模块级常量）；
   wkv/src/lib.rs 以 pub use windex::PREFETCH_WINDOW 原样透出，
   wnode vector_store_callbacks 改引用该路径，删 wkv 本地 BATCH_READ_PREFETCH_SIZE。
3. 删 wkv batch.rs 手写两级下钻（index.mask / buckets.as_ptr / get_unchecked）与第二套 12 常量；
   删 windex 死码 batch_pipeline / find_tag_batch_by_hash / lookup_candidates_batch_by_hash /
   acquire_hash_locks 及专属测试（tests/main.rs 批量段改测新内核、
   latch_concurrency 混合锁用例删除、压力测试混合段改走 acquire_keys_lock_exclusive）。
4. README 命令面实名修正（windex/README.md、readme/en.md、readme/zh.md），
   js/check/ignore/storage.yml 理由同步。

剩余缺口：票据附带的白盒谓词族（bucket.rs:is_latched_shared、table.rs:is_locked、
ram/direct_vm.rs:as_aligned_slice / as_aligned_mut_slice）未收 cfg —— windex 的读者是
tests/ 集成测试（独立 crate），#[cfg(test)] 不可见，收口需改单测内联或以行为断言替代，
属另一射程，留待零生产消费者普查批次处置。

并行复核补记（同票第二代理 worktree dev-prefetch，载荷 198639db 未投，以 7827ba18 为准）：
1. 语义等性定版：C# Tsavorite.cs:561 `const int PrefetchSize = 12` 一处三用——:563 stackalloc
   哈希缓冲长度、:576 外层 `while (nextBatchIx < batchCount)` 单轮条数上界、:581/:595 两级预取
   以整块为窗口，作用层是会话批量读（唯一入口 BasicContext.cs:94 / TransactionalContext.cs:472
   ReadWithPrefetch），C# 无「处理第 i 项预取第 i+12 项」滑窗。据此本代理按票面自证 (2) 的保守
   分支落地过一版（判旧 windex PREFETCH_WINDOW 为纯滑窗距离、与 wkv BATCH_READ_PREFETCH_SIZE
   不等价 → 只删零消费者那一面、留 wkv 单点）；7827ba18 把内核重写为块级两趟后，
   PREFETCH_WINDOW 之名已与 C# 三用同构，统一常量不属名不副实，两案同归单源，取先落地者。
2. 零消费者复核与本票一致且已再验：windex 批量面在生产 src 全域零命中（读者仅 tests/），
   table.rs 无 `#[cfg(...)]`、windex 无 macro_rules! 与 build.rs，lib.rs 仅 pub use table::HashIndex，
   排除宏/cfg 间接使用；dev af0eb61b 上 `cargo check --workspace --all-targets` exit 0、0 warning。
3. 边界告警（转 string-rmw-key-bucket-lock、wtxn-lock-stripe-count-parity 两票与门禁）：
   7827ba18 顺删 windex 锁源 `acquire_hash_locks` 及 tests/index/latch_concurrency.rs 混合锁用例
   约 69 行、压力测试混合段改走 acquire_keys_lock_exclusive。本票硬边界明令不碰该锁源，
   该删除非本票裁定范围，请两票代理确认锁条带/桶闩对位判据未失依。

