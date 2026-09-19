裁决：不成立（「同挂四组锚点报重复」前提不实：两层各挂不同 C# 符号，无复挂；转发壳是规范去重形态）
来源：next/agy.design.md 条 14 + next/muse.design.md 条 10（两轮同题）。核销 2026-09-19。

一句话结论：wtxn TxnLockTable 四函数挂的是 OverflowBucketLockTable.cs:Try* 锚点，
windex HashBucket 四函数挂的是 HashBucket.cs:TryAcquire* 锚点——C# 符号不同名不同挂，
check.js（js/check.js:304 dupDefFind 按「路径.cs:符号」聚合）不会报重复；
rust 用「一处真实现 + 委托薄壳」替代 C# 两份实现恰是 SKILL.md:65 鼓励的去重，去锚点反而丢失对标。

逐条核销
1. 锚点实测（不同）：
   wedb/wtxn/src/txn_lock_table.rs:106/:114/:122/:130 挂
   libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs
   的 TryLockShared / TryLockExclusive / UnlockShared / UnlockExclusive；
   wedb/windex/src/bucket.rs:70/:96 等挂
   libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs 的
   TryAcquireSharedLatch / ReleaseSharedLatch 等。两 C# 文件均真实存在。
2. C# 原貌：HashBucket（桶字内嵌闩）与 OverflowBucketLockTable（溢出桶锁表）是两个类；
   rust 的「windex 真实现 + wtxn 按桶下标转发」正是把 C# 两份 CAS 实现收敛为一处定义，
   锚点各自保留即对标完整，无 check.js 误报可消。
3. 动作评估：「wtxn 侧去除锚点并注明委托」会令 OverflowBucketLockTable.cs 对位符号失去
   rust 挂载，js/check.js 反而报该 C# 符号缺失实现；两文件已互相注明关系
   （txn_lock_table.rs:1-14 模块头写明承接关系与 HashBucket 位引用），无信息缺口。
