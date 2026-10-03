领票注记（2026-09-28 主控）：甄别结论通过，五条标准逐项现验（真实性/非重复/架构合规/可执行度/格式纯粹）；
定级 P3，派席沙箱 /tmp/fork/windex-unscoped-key-latch（分支 windex-unscoped-key-latch）；
纯 windex 内部清理与测试收敛，清理无租户前缀的裸键门面与无调用的锁升降级孤儿方法。

审核结论：通过（2026-09-28 独立方案审核席）

真实性与契约确证：
1 C# 原型确证：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs 仅定义 TryPromoteLatch，完全不存在 DowngradeLatch。注释声称对标 C# HashBucket.DowngradeLatch 系凭空虚构。
2 Rust 现状确证：wedb 采用单物理存储多租户虚拟库架构，生产锁收口为基于 scoped_hash 的 try_lock_key_hash_exclusive 以及桶下标 direct latch。windex/src/bucket.rs 遗留的裸键加锁门面（try_lock_key_exclusive 等）盲目使用 fast_hash(key) 寻桶，丢失租户前缀，导致并发锁脱节与互斥失效隐患真实存在。
3 孤儿死代码确证：锁升降级族（HashBucket::try_promote_latch、HashBucket::downgrade_latch、BucketSharedGuard::try_promote、BucketExclusiveGuard::downgrade）及 HashEntryInfo::lock_shared_guard 在全仓生产代码零调用，仅在内部测试自环，属未消费死代码。

架构与可落度审查：
1 单机制纯洁：清理裸键加锁门面与锁升降级双重机制，锁仲裁统一收敛为 scoped_hash / bucket_index 桶锁，消除并发锁脱节隐患。
2 数据面零开销：纯清理孤儿死代码，不增加任何运行时分配与控制面开销。
3 内部实现化简：HashBucket::drain_or_rollback 消除冗余参数 restore_shared（删除 try_promote_latch 后仅排空超时回退独占标记，无需补回共享读者计数）。
4 外部兼容性护栏：table.rs 中的 bucket_index_for_key 与 bucket_index_for_hash 保留给诊断及外部既有测试使用，避免扩大修改半径。

整理优化执行方案（供 task/fix.md 直接消费）：
1 清理 wedb/windex/src/bucket.rs 裸键加锁门面与死逻辑：
  删除 HashIndex 裸键门面：bucket_for_key、try_lock_shared、unlock_shared、try_lock_exclusive、unlock_exclusive、downgrade、is_locked、lock_shared_guard(&self, key)、try_lock_key_exclusive(&self, key)
  保留并扶正：try_lock_key_hash_exclusive(hash: u64) -> Option<KeyLatch<'_>>
  删除锁升降级族：HashBucket::try_promote_latch、HashBucket::downgrade_latch、BucketSharedGuard::try_promote、BucketExclusiveGuard::downgrade
  化简 HashBucket::drain_or_rollback：移除 restore_shared 参数，排空超时直接 CAS 清除 EXCLUSIVE_LATCH_MASK
2 清理 wedb/windex/src/entry_info.rs 孤儿方法：
  删除未消费的 HashEntryInfo::lock_shared_guard
  保留生产在用的 HashEntryInfo::lock_exclusive_guard（用于 ephemeral_x_latch!）
3 收敛 wedb/windex/tests 测试集：
  更新 tests/index/latch_concurrency.rs 与 tests/index/release_notify.rs，移除已删门面与升降级自环测试用例
  单键独占锁互斥测试改用 try_lock_key_hash_exclusive 验证
4 同步清理文档与注释：
  更新 wedb/windex/README.md、wedb/windex/readme/zh.md、wedb/windex/readme/en.md 以及根 README.md 中对锁升降级与裸键锁门面的陈旧描述

原工单内容：

windex 裸键寻址桶闩门面与未消费锁升降级孤儿死代码清理

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# Garnet 的 Tsavorite 存储引擎中，HashBucket.cs 仅定义了 TryPromoteLatch（供 TransactionalContext/OverflowBucketLockTable 事务锁升级使用），完全不存在 DowngradeLatch 降级原语（注释中对标 C# HashBucket.DowngradeLatch 系凭空虚构）。此外，Garnet Tsavorite 原型为单实例存储，无多租户虚拟库前缀，哈希定位恒为裸键 GetHashCode()。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wedb 架构采用单物理存储多租户虚拟库架构（doc/zh/db.md），所有物理键操作强制要求带租户/虚拟库前缀，桶寻址单点统一定义为 whasher::scoped_hash(prefix, key)（票 wtxn-wkv-keybucket-hash-scope-desync）。
生产并发控制已完全收口为：wkv/src/ttl.rs 使用 try_lock_key_hash_exclusive(scoped_hash)；wkv/src/session/rmw_window.rs 使用桶下标 direct latch；wtxn 事务使用批量 2PL 桶锁。
但在 wedb/windex/src/bucket.rs 中，遗留了一整套按裸键寻址的桶闩门面函数：
- bucket_for_key(&self, key: &[u8])
- try_lock_shared(&self, key: &[u8])
- unlock_shared(&self, key: &[u8])
- try_lock_exclusive(&self, key: &[u8])
- unlock_exclusive(&self, key: &[u8])
- downgrade(&self, key: &[u8])
- is_locked(&self, key: &[u8])
- lock_shared_guard(&self, key: &[u8])
- try_lock_key_exclusive(&self, key: &[u8])
这些函数内部全部经 bucket_for_key 盲目调用 whasher::fast_hash(key)，完全丢弃了租户/会话前缀。
同时，windex/src/bucket.rs 还实现了锁升降级族：
- HashBucket::try_promote_latch(&self)
- HashBucket::downgrade_latch(&self)
- BucketSharedGuard::try_promote(self)
- BucketExclusiveGuard::downgrade(self)
全仓生产代码零调用上述升降级方法，仅在 windex/tests 内部自环测试。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
- 锁脱节与并发安全隐患：若上层或新扩展命令误用 HashIndex::try_lock_key_exclusive(user_key) 或 try_lock_shared(user_key)，其计算得到的桶下标基于裸键 fast_hash，与生产真实持锁点（基于 scoped_hash）计算出的桶完全不同，导致互斥彻底失效，写操作与 RMW/TTL/事务并发交织破坏数据一致性。
- 违反单一机制与零死代码规范：违反 rust_review 纪律第 12 条“pub API 孤儿（零调用导出函数、导出无人消费）定期清理”，以及 task/review.md“单一机制、接口最小暴露、清理未引用的孤儿逻辑”。
- 并发脆弱性：try_promote_latch 存在多读者并发升级互斥死锁/排空超时活锁风险，且生产根本无需 S 转 X 升级机制，保留无用死代码徒增维护与误读成本。

涉及代码：
rust 文件与函数：
wedb/windex/src/bucket.rs:HashIndex::bucket_for_key
wedb/windex/src/bucket.rs:HashIndex::try_lock_shared
wedb/windex/src/bucket.rs:HashIndex::unlock_shared
wedb/windex/src/bucket.rs:HashIndex::try_lock_exclusive
wedb/windex/src/bucket.rs:HashIndex::unlock_exclusive
wedb/windex/src/bucket.rs:HashIndex::downgrade
wedb/windex/src/bucket.rs:HashIndex::is_locked
wedb/windex/src/bucket.rs:HashIndex::lock_shared_guard
wedb/windex/src/bucket.rs:HashIndex::try_lock_key_exclusive
wedb/windex/src/bucket.rs:HashBucket::try_promote_latch
wedb/windex/src/bucket.rs:HashBucket::downgrade_latch
wedb/windex/src/bucket.rs:BucketSharedGuard::try_promote
wedb/windex/src/bucket.rs:BucketExclusiveGuard::downgrade
wedb/windex/src/entry_info.rs:HashEntryInfo::lock_shared_guard
wedb/windex/src/table.rs:HashIndex::bucket_index_for_key

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:TryPromoteLatch
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:TryAcquireSharedLatch
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:TryAcquireExclusiveLatch
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:TryPromoteLock

精炼执行方案：
1. 清理 windex/src/bucket.rs 中裸键加锁门面：删除 HashIndex 上的 bucket_for_key、try_lock_shared、unlock_shared、try_lock_exclusive、unlock_exclusive、downgrade、is_locked、lock_shared_guard(&self, key)、try_lock_key_exclusive(&self, key)，保留并扶正 try_lock_key_hash_exclusive(hash)。若内部测试需要定位桶，显式使用 bucket_index_for_hash 或桶下标。
2. 清理未消费的锁升降级死逻辑：删除 HashBucket::try_promote_latch、HashBucket::downgrade_latch、BucketSharedGuard::try_promote、BucketExclusiveGuard::downgrade，以及 HashEntryInfo::lock_shared_guard。
3. 相应更新 windex/tests 单元测试夹具，移除针对已删孤儿门面的测试用例，保留对 HashBucket 基础 shared/exclusive 自旋互斥与排空特性的有效测试。

## 终态注记（2026-09-28 合入）
- 合入 commit: `9d799aa`
- 修复成果：
  1. `wedb/windex/src/bucket.rs`：彻底清理裸键加锁门面（`bucket_for_key`、`try_lock_shared`、`unlock_shared`、`try_lock_exclusive`、`unlock_exclusive`、`downgrade`、`is_locked`、`lock_shared_guard`、`try_lock_key_exclusive`），锁仲裁统一收敛为带租户前缀的 `try_lock_key_hash_exclusive(hash: u64)`；
  2. 清理全仓生产零消费的锁升降级孤儿死逻辑（`try_promote_latch`、`downgrade_latch`、`BucketSharedGuard::try_promote`、`BucketExclusiveGuard::downgrade`）；
  3. `drain_or_rollback` 简化，剔除已无调用的 `restore_shared` 参数；
  4. `entry_info.rs` 清理孤儿 `lock_shared_guard`，更新测试与文档；
  5. 净减 371 行代码，消除无租户前缀裸键加锁可能导致的锁脱节隐患。

