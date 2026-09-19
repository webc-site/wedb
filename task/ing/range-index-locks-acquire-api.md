RangeIndexManager 裸锁面外溢：按 C# 收回私有，只暴露取锁 API

来源：next/agy.design.md 条 15（同题另见 next/muse.design.md 条 7「锁面两套入口」，以本单为载体）。
取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev 当下工作树，行号按符号重取。

现状
- 裸锁出口：/Users/z/git/db/wedb/wedb/wbftree/src/manager/mod.rs:378
  `pub fn locks(&self) -> &StripedRwLock<(), NUM_LOCK_STRIPES>`，wbftree 内无任何
  `fn acquire_*` 取锁方法（`grep -rn "fn acquire_" wedb/wbftree/src` 零命中），
  即该 `&StripedRwLock` 是全仓唯一取锁入口。
- 跨 crate 直取锁的生产位点（7 处）：
  /Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:168（升阶快照窗口）、
  :295（读守卫循环内）、:355（写守卫循环内，属 StoreSession::acquire_tree_write :346 的实现体）；
  /Users/z/git/db/wedb/wedb/wkv/src/range_index/migration.rs:49、:84、:173；
  /Users/z/git/db/wedb/wedb/wnode/src/rangeindex/range_index_manager_migration.rs:124。
  集成测试另有 /Users/z/git/db/wedb/wedb/wbftree/tests/manager_and_stub/locks.rs:30、:49 与
  manager.rs:509、:829 直取裸锁。
- 双入口的事实：带屏障的一侧是 wkv 的异步守卫
  （/Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:346 acquire_tree_write，
  循环内先 `wait_tree_checkpoint(self.store.range_index, key)`（实现在
  /Users/z/git/db/wedb/wedb/wkv/src/range_index/mod.rs:32）再取锁、取到后复查
  `get_tree` 与 `is_flushed`，不满足即 drop 重试）；
  不带屏障的一侧是各处 `mgr.locks().write(hash)` 裸取——stub.rs:168、migration.rs 三处、
  wnode 一处共 5 个位点绕过该守卫，且传入的 hash 由调用侧自行 fast_hash，
  与键的对应关系无人核对（同键不同 hash 即互斥失效）。
- 一锁一源成立（两侧最终都落同一 StripedRwLock），故本单不是「两套锁架构」，
  而是「锁原语对外裸露 + 守卫可绕过」的封装面问题，与
  next/wtxn-lock-stripe-count-parity.md（管 wtxn 私有条带表第二套锁）不重叠。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:84
  `private readonly ReadOptimizedLock rangeIndexLocks;`——锁表字段私有，
  对外只有 :194 `internal ExclusiveRangeIndexLock AcquireExclusiveForDelete(long keyHash)`
  与 :121 起的 `ReadRangeIndex` 系 RAII 持有者（:38 ReadRangeIndexLock、ExclusiveRangeIndexLock），
  文件内 :281 的独占取锁也是同 partial class 内部调用；
  跨文件消费一律走方法面：/Users/z/git/db/wedb/garnet/libs/server/Storage/Session/MainStore/
  RangeIndexOps.cs:201、:280、:419、:475、:527、:615、:665 与
  RangeIndexManager.Migration.cs:80、:286、RangeIndexManager.Replication.cs:196、:227。
  即 C# 从不把锁对象交给调用方。

修法
1. wbftree 侧按 C# 补取锁方法面，字段与 `locks()` 收为私有/pub(crate)：
   `pub fn acquire_exclusive_for_delete(&self, key: &[u8]) -> ExclusiveRangeIndexLock`
   与 `pub fn read_range_index_lock(&self, key: &[u8]) -> SharedRangeIndexLock`
   （持有者结构体在 wbftree 内定义，Drop 释放，对标 ExclusiveRangeIndexLock/ReadRangeIndexLock），
   键→hash 的换算是方法内部职责（复用既有 key_id_of/fast_hash 口径），调用侧不再自取 hash。
2. 上述 7 个生产位点改走第 1 步方法；带检查点屏障的三处（stub.rs:295、:355 及 mod.rs:32 屏障）
   仍留在 wkv 会话层——屏障依赖 await 与 promote 重试，属会话职责，不下沉进 wbftree；
   下沉只做「取锁 + 释放」这一对，与 C# 一致。
3. 集成测试改调第 1 步公开方法（wbftree/tests/manager_and_stub/locks.rs:30、:49 与
   manager.rs:509、:829），禁为测试保留 `pub fn locks()`，禁加 cfg 扩面。
4. 若某位点确需自定义 hash 域（如迁移期跨键批量持锁 stub.rs:168 的 create_key），
   用带 key 参数的方法表达，不得回传裸锁对象。

验收判据
- `grep -rn "pub fn locks" wedb/wbftree/src` 零命中（或仅 pub(crate)）。
- 全仓 `grep -rn "\.locks()" wedb/*/src wedb/*/tests` 零命中；
  `grep -rn "acquire_exclusive_for_delete\|read_range_index_lock" wedb` 的位点覆盖第 2 步的 7 个生产点
  与第 3 步的测试点，且每处的锁源都是同一 StripedRwLock 字段。
- `grep -rn "wait_tree_checkpoint" wedb/wkv/src` 的消费点数量不减（屏障未被削弱）。
- 分层/迁移/复制既有集成测试（wbftree/tests/manager_and_stub、wkv 侧 range_index 用例）全绿，
  无新增死锁或锁窗口放宽。

优先级
污染扩散（锁原语可被任意 crate 以任意 hash 取用，异步守卫可绕过；一旦扩散难收）。
