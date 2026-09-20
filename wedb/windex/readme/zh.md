# windex : 无锁哈希索引

## 项目介绍

windex 提供 Garnet Tsavorite 风格的 64 字节缓存行对齐无锁并发哈希索引：哈希桶、紧凑条目、溢出桶池、分块分裂迁移与并发索引表。

寻址不变量：容量恒为 2 的幂，`mask == buckets.len() - 1`，单表实例构造时终身绑定。

## 模块组成

- `bucket`：`HashBucket`，每桶严格占一个 64B 缓存行（`[AtomicU64; 8]`）；槽 0..6 为数据项，槽 7 低 48 位存溢出桶 1-based 索引、次高 15 位为共享锁读者计数、最高 1 位为独占锁
- `entry`：`HashBucketEntry(u64)` 紧凑条目：低 48 位地址（上限 256TB；bit47 为 read_cache 指示位，与地址最高位复用，置位时有效地址仅低 47 位）、15 位哈希指纹 tag、bit63 tentative 两阶段插入标记
- `overflow_pool`：溢出桶内存池，两级分块连续分配（每 Chunk 1024 桶），原子 1-based 序号；allocate 优先从无锁空闲栈（Treiber stack + 32 位 ABA 代数标签）复用 free 回收的桶，空闲栈为空才递增序号新分配
- `split`：`split_chunk` / `split_single_bucket` 分块与单桶分裂迁移原语，配合动态扩容状态机无锁平滑迁移
- `table`：`HashIndex` 索引表 + `HashBuckets`（DirectVirtualMemory Demand-Zero 映射，≥2MB 自动对齐并开 MADV_HUGEPAGE）+ 单键桶闩与 L1 预取

## 核心 API

- `HashBucket` / `BucketSharedGuard` / `BucketExclusiveGuard`；`ENTRIES_PER_BUCKET = 8`、`DATA_ENTRIES = 7`、`OVERFLOW_INDEX = 7`
- `HashBucketEntry`：address / tag / tentative 位打包，CAS 更新
- `OverflowPool`：allocate / free / get / has_free / allocated_count（`CHUNK_SIZE = 1024`、`MAX_CHUNKS = 4096`）
- `HashIndex`：insert / find_tag / lookup，查重插入 find_tag_or_insert / find_or_create_tag(\_with_min_addr)（死槽清退），批量读两级预取内核 prefetch_batch_probes（12 键窗口、PrefetchProbe 哈希与首地址同源），update_address / delete（RCU CAS / 原子置零），单键独占闩 try_lock_key_exclusive
- `HashBuckets` / `HashEntryInfo`（定点 CAS / try_elide）/ `CandidateAddresses` / `KeyLatch` / `prefetch_read_l1`；锁升降级 `BucketSharedGuard::try_promote` / `BucketExclusiveGuard::downgrade`

## 设计要点

- 自旋读写闩内嵌桶槽 7：15 位共享计数（上限 32767 读者）+ 1 位独占；单次尝试自旋 128 轮、独占读者排空 1024 轮（前 32 轮 spin_loop、其后 yield_now），失败即返回 false，排空超时自动回退独占位。键级读改写窗口一律经 `HashIndex::try_lock_key_exclusive` 单次尝试取闩（对标 C# `TryEphemeralXLock`：取不到即返回状态），取闩失败由调用方承接重试；索引层无多键批量编排、无逆序回滚、不自旋造超时
- 单次 CAS 无锁插入：空槽 0 → 完整条目原子发布，无半成品窗口；同 tag 查重由调用方按候选地址择新承担（C# FindOrCreateTag 两阶段 tentative 协议的合并等价实现，tentative 位保留用于批量查重的瞬时可见性判定）
- RCU 无锁 CAS 更新 + 原子置零删除；`MAX_CHAIN_STEPS = 1 << 20` 单指针计数防链环
- `KeyLatch` 是 `BucketExclusiveGuard` 的按键寻址别名（同一把桶闩、同一 Drop 放闩实现，无第二份守卫代码）；索引层不再导出任何多键批量取闩守卫，多键两阶段锁编排唯一归 `wtxn::TxnKeyEntry`

## 测试覆盖

tests/index/ 覆盖：缓存行对齐与位打包边界、tag 掩码防御、共享 / 独占闩生命周期与读者排空、锁升级降级、单键闩同键竞争互斥、满竞争压测、跨 Chunk 并发分配、1024+ 深溢出链与链环检测、并发 RCU 更新、find_tag 探测、混合负载插查。
