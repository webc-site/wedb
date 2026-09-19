拒绝原因：问题已被修——建议的形态就是当前实现

来源：next/agy.db.md 条 1（TxnLockTable 逐桶操作频繁调用闭包与 Arc 增减）。

原主张：事务加解锁循环逐桶调 (self.loader)() 生成 Arc<HashIndex>，应改为进入事务时
pin 一次、循环直接操作桶数组。

取证（主仓 dev 当下代码）：
- 事务主链已经是「pin 一次批量操作」：wedb/wtxn/src/txn_key_entry.rs:203
  lock_all_keys 与 :221 try_lock_all_keys 均为 `let index = self.lock_table.pin();`
  取一次索引版本（:208/:226 存 self.latch 复用），此后 :175 acquire_plan 循环内
  直接 `index.bucket(slot.bucket)` 取桶加解锁、:159 release_held 逆序释放，
  全程零逐桶闭包调用、零逐桶 Arc clone。
- TxnLockTable 四个逐桶转发方法（wedb/wtxn/src/txn_lock_table.rs:108/:116/:124/:132）
  与 bucket_index_for_hash(:100) 全仓零生产调用（grep 仅命中定义自身），
  不构成热路径；它们是 C# OverflowBucketLockTable 公开面的锚点承接
  （挂 OverflowBucketLockTable.cs:TryLockShared 等四键，commit 02dbd6a 刚归位）。
- C# 对标：OverflowBucketLockTable.cs:38-58 TryLockShared/TryLockExclusive/
  UnlockShared/UnlockExclusive 同为单桶转发面，GetBucketIndex :29 逐次现取
  size_mask——rust 形态与 C# 一致。

结论：建议中的目标形态（pin 一次 + 桶数组直操作）已是现状，无待办。四转发方法的
死面问题属另一话题（zero-consumer 系列票管辖口径），不在本条射程。
