## 终态注记（2026-09-28 合入）
- 合入 commit: `fb45f7d`（分支改动 `be70d85`）
- 收口形态：
  1. `wedb/wtxn/src/txn_lock_table.rs`：彻底删除未钉定桶闩门面五方法（`bucket_index_for_hash`、`try_lock_shared`、`try_lock_exclusive`、`unlock_shared`、`unlock_exclusive`）及区块头警告注释（共删除 54 行危险门面代码）。
  2. 锁操作收敛为单一机制：整笔事务单次 `pin()` 钉定 `HashIndex` 实例，杜绝跨 resize 扩容版本锁脱节与他人锁被误放的并发隐患。
  3. 测试用例改造收口：
     - `wedb/wtxn/tests/txn_lock_table.rs`
     - `wedb/wtxn/tests/txn_key_entries.rs`
     - `wedb/wtxn/tests/txn_lock_stress.rs`
     - `wedb/wtxn/tests/txn_lock_table_instance.rs`
     - `wedb/wtxn/tests/cross_tenant_txn_lock_isolation.rs`
     - `wedb/wnode/tests/wtxn_exec_lock_async_retry.rs`
     全部调用点统一改造为显式 `let index = table.pin(); index.bucket_index_for_hash(...)` 与 `index.get_bucket(bucket).try_lock_... / unlock_...`。
- 自查断言：
  1. 编译自查：沙箱运行 `cargo check --manifest-path wedb/Cargo.toml --tests --all-targets` 零错误零警告通过。
  2. 零孤儿门面断言：`git grep` 验证全仓再无对 `TxnLockTable` 这 5 个未钉定门面方法的调用。

审核结论：通过（2026-09-28 独立方案审核席）
甄别结论：通过（定级：P2 并发防御与死代码清理，双侧锚确证成立）


真实性与契约确证：
1 C# 原型确证：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs 中的 TryLockShared、TryLockExclusive、UnlockShared、UnlockExclusive 恒接受 ref HashEntryInfo hei（锁定 hei.firstBucket），不存在脱离固定版本或首桶的裸桶下标加解锁门面。
2 Rust 现状确证：wedb/wtxn/src/txn_lock_table.rs 中暴露的 5 个门面方法（bucket_index_for_hash、try_lock_shared、try_lock_exclusive、unlock_shared、unlock_exclusive）每次调用均重新触发 self.pin() 现取版本。代码注释已明确警告「仅供测试装配，跨版本重 pin 隐患见区块头注，禁作生产加锁会话」。
3 生产实际确证：生产事务严格通过 TxnKeyEntries 在首轮准备 ensure_plan 时调用 lock_table.pin() 钉定整笔事务单一 HashIndex 版本（self.latch = Some(index)），后续 acquire_plan 与 release_held 均基于该单一钉定实例操作，零消费这 5 个门面方法。门面方法仅在 wtxn 与 wnode 单元/集成测试中作为夹具调用。

架构与可落度审查：
1 单机制纯洁：清理危险的 unpinned 裸桶加解锁门面，锁仲裁统一收敛为整笔事务单次 pin() 钉定版本的单一机制，彻底消除跨 resize 扩容版本加老锁放新锁导致的死锁与他人锁释放隐患。
2 接口最小暴露与零死代码：TxnLockTable 不再暴露不安全且生产零调用的孤儿门面，符合 rust_review 规范“零死代码、清理未引用的孤儿逻辑、接口最小暴露与字段直取”。
3 数据面零开销承诺：纯代码与接口清理，消除重复调用 self.pin() 的开销，不增加任何堆分配。
4 方案可落度：改动集中且收口清晰。删除 5 个方法，更新 6 个测试文件改为显式 table.pin() 钉定实例后取放桶闩，测试全绿可闭环验证。

整理优化执行方案（供 task/fix.md 直接消费）：
1 清理 wedb/wtxn/src/txn_lock_table.rs 未钉定门面五方法：
  删除 bucket_index_for_hash(&self, key_hash: i64) -> usize
  删除 try_lock_shared(&self, bucket: usize) -> bool
  删除 try_lock_exclusive(&self, bucket: usize) -> bool
  删除 unlock_shared(&self, bucket: usize)
  删除 unlock_exclusive(&self, bucket: usize)
  清理相关的区块头警告注释（第 193-203 行）
2 收敛并改造测试用例（改为显式 let index = table.pin(); index.bucket_index_for_hash(...) 与 index.get_bucket(bucket).try_lock_.../unlock_...）：
  - wedb/wtxn/tests/txn_lock_table.rs
  - wedb/wtxn/tests/txn_key_entries.rs
  - wedb/wtxn/tests/txn_lock_stress.rs
  - wedb/wtxn/tests/txn_lock_table_instance.rs
  - wedb/wtxn/tests/cross_tenant_txn_lock_isolation.rs
  - wedb/wnode/tests/wtxn_exec_lock_async_retry.rs
3 验证闭环：
  运行 ./sh/clippy.sh 确保无告警，运行 ./test.sh 确保全部测试通过。

原工单内容：

TxnLockTable 清理未钉定桶闩门面五方法彻底消除跨扩容版本锁脱节死锁隐患

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# Garnet/Tsavorite 中，OverflowBucketLockTable.cs（及上层 TransactionalContext.cs）中所有锁表操作恒要求传入 ref HashEntryInfo hei（如 TryLockShared、TryLockExclusive、UnlockShared、UnlockExclusive），其底层锁操作严格绑定于 hei.firstBucket，即整个加锁/解锁流程锚定在单次查找初始化的具体哈希桶指针上，从未提供脱离固定版本或脱离固定首桶的裸桶下标加解锁门面。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wedb/wtxn/src/txn_lock_table.rs 中暴露了 5 个未钉定（unpinned）的桶闩门面方法：
- bucket_index_for_hash
- try_lock_shared
- try_lock_exclusive
- unlock_shared
- unlock_exclusive
这 5 个方法内部每次调用均重新触发 self.pin() 现取当时的活跃 HashIndex 版本（如 self.pin().get_bucket(bucket).try_lock_shared()）。代码注释中已明文警告「仅供测试装配，跨版本重 pin 隐患见区块头注，禁作生产加锁会话」。
生产实际中，全量事务加锁严格经由 TxnKeyEntries 流程：在事务生命周期开始时通过 ensure_plan 单次调用 lock_table.pin() 钉定整笔事务专用的单一 HashIndex 版本（self.latch = Some(index)），后续的归并排序、逐桶加锁（acquire_plan）与逆序放锁（release_held）全量基于该已钉定版本操作，生产代码完全不消费这 5 个门面方法。
这 5 个方法目前仅在 wtxn 与 wnode 的测试集成用例中作为测试夹具被调用。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
- 跨扩容版本锁脱节与死锁：若外部会话或未来开发调用门面方法，在 lock 与 unlock 调用之间一旦发生哈希表 resize 扩容，self.pin() 将分别取得扩容前与扩容后的两个不同 HashIndex 实例。由此导致加锁发生在老版本桶上，而解锁作用在新版本桶（或下标对应但物理实例不同的桶）上，产生严重并发缺陷：老版本桶闩永久无法释放（死锁），新版本桶闩被错误释放（释放他人持有的锁），直接击穿事务并发互斥与数据一致性。
- 违反单一机制与最小暴露原则：生产已有完善的 TxnKeyEntries 单一钉定版本锁机制，在 TxnLockTable 暴露具有严重安全隐患的 unpinned 门面属于过度设计与危险后门，违反 rust_review 规范“零死代码、清理未引用的孤儿逻辑、接口最小暴露与字段直取”。

涉及代码：
rust 文件与函数：
wedb/wtxn/src/txn_lock_table.rs:TxnLockTable::bucket_index_for_hash
wedb/wtxn/src/txn_lock_table.rs:TxnLockTable::try_lock_shared
wedb/wtxn/src/txn_lock_table.rs:TxnLockTable::try_lock_exclusive
wedb/wtxn/src/txn_lock_table.rs:TxnLockTable::unlock_shared
wedb/wtxn/src/txn_lock_table.rs:TxnLockTable::unlock_exclusive

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:OverflowBucketLockTable::TryLockShared
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:OverflowBucketLockTable::TryLockExclusive
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:OverflowBucketLockTable::UnlockShared
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:OverflowBucketLockTable::UnlockExclusive

精炼执行方案：
1. 彻底删除 wedb/wtxn/src/txn_lock_table.rs 中 5 个 unpinned 门面方法（bucket_index_for_hash、try_lock_shared、try_lock_exclusive、unlock_shared、unlock_exclusive）及相关警示注释区块。
2. 改造消费上述门面方法的测试用例，统一改为显式通过 table.pin() 钉定 HashIndex 单一实例后调用 index.get_bucket(bucket).try_lock_.../unlock_...，确保测试夹具在单版本内安全闭环：
   - wedb/wtxn/tests/txn_lock_table.rs
   - wedb/wtxn/tests/txn_key_entries.rs
   - wedb/wtxn/tests/txn_lock_stress.rs
   - wedb/wtxn/tests/txn_lock_table_instance.rs
   - wedb/wtxn/tests/cross_tenant_txn_lock_isolation.rs
   - wedb/wnode/tests/wtxn_exec_lock_async_retry.rs
3. 运行 ./sh/clippy.sh 与 ./test.sh 验证全仓编译无告警且测试全绿通过。
