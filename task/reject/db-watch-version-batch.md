拒绝原因：C# 无批量形态，自创优化与 transpile SKILL「1:1 对标，不要实现自己的优化」冲突

来源：next/agy.db.md 条 22（wtxn watch_version_map 批量版本推进支持）。

原主张：多键写（MSET、事务提交）逐键 increment_version 引发缓存行抖动，应增
increment_versions_batch 批量推进接口。

取证（主仓 dev 当下代码 + C# 源）：
- rust 现状即 C# 形态：wedb/wtxn/src/watch_version_map.rs:64 increment_version
  单键 fetch_add（挂锚 WatchVersionMap.cs:IncrementVersion），:51 read_version。
  C# 源 garnet/libs/server/Transaction/WatchVersionMap.cs 全文仅 ReadVersion /
  IncrementVersion 两方法（Interlocked.Increment 单键），无任何批量接口。
- C# 消费面同样是逐键单点调用：garnet/libs/server/Storage/Functions/
  SessionFunctionsUtils.cs:120/:140/:160 与 ObjectStore/UpsertMethods.cs:48-68、
  DeleteMethods.cs:21/:30 等写钩子每次单键 IncrementVersion——多键写在 C# 同样
  逐个原子更新，这正是 WATCH 语义的对标基线（版本只需单调，桶冲突只致假失效）。
- 批量合并会改变可见性时序：把多键版本推进合并为「预排 + 批量锁」会让部分键的
  版本推进时刻偏离其物理写入时刻，偏离 C# 语义而非优化它。

结论：不立项。若未来实测 WATCH 成为吞吐瓶颈，须先证 C# 同场景无此瓶颈再议，
当前属纯自创优化。
