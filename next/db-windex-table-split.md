优先级：中
来源：next/agy.db.md 条 14 立项。取证基线：主仓 dev 当下代码。

问题
windex table.rs 780 行单文件多职责：哈希桶探测查找、预取窗口内核、CAS 槽位插入
更新、候选地址收集、单桶闩守卫与多键两阶段锁编排混聚。

取证
- wedb/windex/src/table.rs:60 pub struct HashIndex、:105 get_bucket、:112 hash_key、
  :128 find_tag_by_hash（探测查找）、:620 prefetch_batch_probes（两级预取内核）、
  :554 bucket_index_for_key、:603 lock_exclusive_guard（单桶守卫）、:680
  acquire_bucket_locks + :712 acquire_keys_lock_exclusive + :719
  acquire_unique_locked_entries（多键 2PL 编排，见
  next/db-hashindex-2pl-single-orchestration.md）；文件共 780 行。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs
  （ContextReadWithPrefetch 等驱动编排）与 garnet/libs/storage/Tsavorite/cs/src/core/
  Index/Tsavorite/Implementation/FindTag.cs（探测查找独立实现文件）——C# 探测 /
  驱动 / 锁 partial 分文件。

修法建议
在 next/db-hashindex-2pl-single-orchestration.md 落地（2PL 编排收敛）之后再做本票，
按 table/probe.rs（find_tag_by_hash / 候选地址收集）、table/prefetch.rs（预取窗口
内核，PREFETCH_WINDOW 常量随之迁移，wkv 侧 re-export 路径同步）、table/modify.rs
（插入更新 CAS 死槽回收）、table/latch.rs（单桶守卫 + 多桶编排内核）拆分，
table/mod.rs 留 HashIndex 定义与公共操作。两票先后串行认领，不得并行改同一文件。
