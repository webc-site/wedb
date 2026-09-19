优先级：中
来源：next/agy.db.md 条 2 立项。取证基线：主仓 dev 当下代码，行号为当下实测（HEAD 7a6d670 一带，仓库正被并行改动，认领时按符号重定位）。

问题
多键两阶段锁（排序、去重、逐桶取闩、失败逆序回滚、重试）存在两份同构编排：
windex 索引层一份、wtxn 事务层一份。锁内存同一份桶闩（无第二套锁内存），但编排代码
双份，行为细节已经分叉（退避策略一边自旋+jitter、一边 yield_now；重试预算一边
16384、一边无限），后续修死锁或锁升级时极易只改一处。

取证
- windex 侧：wedb/windex/src/table.rs:680 acquire_bucket_locks（栈上 16 条目内联、
  sort_unstable_by 桶序全序 + 同桶排他优先、in_place_dedup_by 去重）、:712
  acquire_keys_lock_exclusive（pub 多键独占入口）、:719 acquire_unique_locked_entries
  （自旋退避 YIELD_RETRY_BUDGET=16384 :77、SPIN_RETRY_THRESHOLD 指数退避 + jitter、
  部分失败栈上逆序回滚）。消费者全仓仅两处：wedb/wkv/src/ttl.rs:456、:504。
- wtxn 侧：wedb/wtxn/src/txn_key_entry.rs:143 lock_plan（排序计划）、:175 acquire_plan
  （逐桶 try 取闩、失败 release_held 逆序回滚）、:159 release_held、:203 lock_all_keys /
  :221 try_lock_all_keys（thread::yield_now 无限重试）。主链每笔事务 pin 一次
  （self.lock_table.pin()）后直接 index.bucket 操作，不经 windex 多键编排。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs
  （:25 GetBucketIndex、:38-58 TryLock*/Unlock* 单桶转发、:113 SortKeyHashes +
  :86 KeyHashComparer 排序去重单点在锁表层）；多键取闩循环在事务层
  garnet/libs/server/Transaction/TxnKeyManager.cs 与 TransactionalContext 的
  DoTransactionalLock/DoTransactionalUnlock。C# 的排序工具（SortKeyHashes/KeyHashComparer）
  是锁表公开面一处定义、事务层复用，不存在两份排序回滚编排。

修法建议
方向向下单点化，不动 crate 依赖方向（wkv 严禁依赖 wtxn）：把「排序去重 + 逐桶取闩 +
逆序回滚」提取为 windex 唯一多桶编排内核（现 acquire_bucket_locks / acquire_unique_
locked_entries 保持 pub(crate) 级单点），wtxn 的 lock_plan/acquire_plan 改为调 windex
内核（自备持锁登记 TxnKeyEntries 不变），退避与重试预算两套参数收敛为内核参数注入。
锁源仍是 windex HashBucket 内嵌闩，与 next/rmw-atomic-read-modify-write-window.md、
next/string-rmw-key-bucket-lock.md 的收口锁源同根：认领时先读该两票，锁内存与单桶
原语不得二次改动，只合并编排层。C# 侧 GetBucketIndex/SortKeyHashes 锚点保持挂 windex。
