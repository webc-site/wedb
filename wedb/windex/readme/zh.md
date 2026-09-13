# windex : 无锁哈希索引

## 项目介绍

windex 提供 Garnet Tsavorite 风格的 64 字节缓存行对齐无锁并发哈希索引：哈希桶、紧凑条目、溢出桶池与定容索引表。

寻址不变量：容量恒为 2 的幂，`mask == buckets.len() - 1`，构造时终身绑定。

## 模块组成

- `bucket`：`HashBucket`，每桶严格占一个 64B 缓存行（`[AtomicU64; 8]`）；槽 0..6 为数据项，槽 7 低 48 位存溢出桶 1-based 索引、次高 15 位为共享锁读者计数、最高 1 位为独占锁
- `entry`：`HashBucketEntry(u64)` 紧凑条目：低 48 位地址（上限 256TB；bit47 为 read_cache 指示位，与地址最高位复用，置位时有效地址仅低 47 位）、15 位哈希指纹 tag、bit63 tentative 两阶段插入标记
- `overflow_pool`：溢出桶内存池，两级分块连续分配（每 Chunk 1024 桶），原子 1-based 序号；allocate 优先从无锁空闲栈（Treiber stack + 32 位 ABA 代数标签）复用 free 回收的桶，空闲栈为空才递增序号新分配
- `table`：`HashIndex` 定容索引表 + `HashBuckets`（DirectVirtualMemory Demand-Zero 映射，≥2MB 自动对齐并开 MADV_HUGEPAGE）+ 多键加锁与 L1 预取

## 核心 API

- `HashBucket` / `BucketSharedGuard` / `BucketExclusiveGuard`；`ENTRIES_PER_BUCKET = 8`、`DATA_ENTRIES = 7`、`OVERFLOW_INDEX = 7`
- `HashBucketEntry`：address / tag / tentative 位打包，CAS 更新
- `OverflowPool`：allocate / free / get / has_free / allocated_count（`CHUNK_SIZE = 1024`、`MAX_CHUNKS = 4096`）
- `HashIndex`：insert / find_tag / lookup，查重插入 find_tag_or_insert / find_or_create_tag(\_with_min_addr)（死槽清退），批量预取 find_tag_batch / lookup_candidates_batch（12 项滑动窗口、64 键分块），update_address / delete（RCU CAS / 原子置零），多键加锁 acquire_keys_lock_exclusive / acquire_hash_locks
- `HashBuckets` / `HashEntryInfo`（定点 CAS / try_elide）/ `CandidateAddresses` / `MultiBucketGuard` / `prefetch_read_l1`；锁升降级 `BucketSharedGuard::try_promote` / `BucketExclusiveGuard::downgrade`

## 设计要点

- 自旋读写闩内嵌桶槽 7：15 位共享计数（上限 32767 读者）+ 1 位独占；单次尝试自旋 128 轮、独占读者排空 1024 轮（前 32 轮 spin_loop、其后 yield_now），失败即返回 false，排空超时自动回退独占位。多键批量加锁另有三段退避：指数自旋（含抖动）→ yield（1024 次）→ 睡眠（100µs→1ms 封顶、16384 次）后报 `LockTimeout`
- 单次 CAS 无锁插入：空槽 0 → 完整条目原子发布，无半成品窗口；同 tag 查重由调用方按候选地址择新承担（C# FindOrCreateTag 两阶段 tentative 协议的合并等价实现，tentative 位保留用于批量查重的瞬时可见性判定）
- RCU 无锁 CAS 更新 + 原子置零删除；`MAX_CHAIN_STEPS = 1 << 20` 单指针计数防链环
- `MultiBucketGuard` 栈内联 16 条目，Drop 逆序解锁满足 2PL，按序加锁防死锁

## 测试覆盖

tests/index/ 覆盖：缓存行对齐与位打包边界、tag 掩码防御、共享 / 独占闩生命周期与读者排空、锁升级降级、多桶防死锁排序、满竞争压测、跨 Chunk 并发分配、1024+ 深溢出链与链环检测、并发 RCU 更新、find_tag 探测、混合负载插查。
