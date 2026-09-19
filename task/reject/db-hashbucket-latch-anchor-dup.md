拒绝原因：问题已被修——双挂已随 commit 02dbd6a 锚点归位消解

来源：next/muse.db.md 条 2（HashBucket 四锁双挂 windex 与 wtxn）。

原主张：四个闩函数同挂 windex 桶本体与 wtxn 锁表转发层，check.js 报 4 组重复，
应去转发层锚点只留桶本体。

取证（主仓 dev 当下代码，HEAD 7a6d670 一带）：
- check.js 现场实测：重复定义清单（11 组）中无任何 HashBucket.cs:TryAcquire* /
  Release* 组，双挂已不存在。
- 锚点归位后两侧各归其主：wtxn 转发层 wedb/wtxn/src/txn_lock_table.rs:104-:133
  经 commit 02dbd6a 改挂 OverflowBucketLockTable.cs:TryLockShared /
  TryLockExclusive / UnlockShared / UnlockExclusive（C# OverflowBucketLockTable.cs
  :38-:58 确有这四个公开转发方法，转发到桶本体）；
  windex 桶本体 wedb/windex/src/bucket.rs:70/:96/:111/:240 保持挂
  HashBucket.cs:TryAcquireSharedLatch / ReleaseSharedLatch / TryAcquireExclusiveLatch /
  ReleaseExclusiveLatch——与 C# 的两层结构（锁表转发 + 桶本体实现）一一对位。

结论：原建议方向（转发层去 HashBucket 锚）已按更优形态落地（转发层改挂锁表键
而非去锚），双挂报告消失，无待办。
