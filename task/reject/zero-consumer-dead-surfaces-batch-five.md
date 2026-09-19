# 批五甄别结论：不成立项留档（原文照录 + 拒绝理由）

来源单：`task/ing/zero-consumer-dead-surfaces-batch-five.md`（本仓绝对路径
`/Users/z/git/db/wedb/task/ing/zero-consumer-dead-surfaces-batch-five.md`）。
取证 HEAD：b8dd92d（原快照 f974dd1f 之下一切行号均已再取一次）。
以下条目按当下代码判为不成立，不进入本批改动面。

---

## 条 1 「wlua 两份 script_digest 便捷包装（注释谎指接线）」——不成立：所指两口在 HEAD 不存在

> 1. wlua 两份 script_digest 便捷包装（注释谎指接线）
>    /Users/z/git/db/wedb/wedb/wlua/src/commands.rs:463-466（doc「供 resp 域接线使用」，resp 域零引用）、
>    /Users/z/git/db/wedb/wedb/wlua/src/runner/mod.rs:391-394（doc「SCRIPT/EVAL 调度路径使用」，该路径零引用）。
>    真实单点 /Users/z/git/db/wedb/wedb/wlua/src/cache.rs:290
>    `SessionScriptCache::get_script_digest`，生产消费点 commands.rs:229、commands.rs:337、
>    functions/redis.rs:111、functions/redis.rs:120。

拒绝理由：两个「壳」在 HEAD 已被别的东西占了行号，符号本体全仓零命中。

- `commands.rs:463-466` 当下是 `#[cfg(test)] pub struct NoopScriptingApi;`（哑会话，测试/benchmark 形态），
  不是 script_digest 包装。
- `runner/mod.rs:390-393` 当下是 `pub fn source(&self) -> &[u8]`（脚本源码访问器，有生产读者），
  不是 digest 包装。
- 全仓 `grep -rn "script_digest" wedb/wlua/src` 命中仅：`cache.rs:290` 定义本体 + 其测试 :348/:361/:368/:384/:420/:433、
  生产读者 `commands.rs:229`、`commands.rs:337`、`functions/redis.rs:111`、`functions/redis.rs:120`、
  `functions/redis.rs:484`（测试）。即「真实单点 + 生产消费点」已是现状终态，无第二出口可删。

结论：本项验收目标（消两份带谎指的壳）已达成，无需改动。

---

## 条 2 「wlua 导出面死常量 BLOCK_HEADER_SIZE」——不成立：该常量不存在

> 2. wlua 导出面死常量 BLOCK_HEADER_SIZE
>    /Users/z/git/db/wedb/wedb/wlua/src/limited_allocator.rs:554 + 再导出
>    /Users/z/git/db/wedb/wedb/wlua/src/lib.rs:42，全仓零引用；分配路径按 BlockRef 偏移寻址
>    （同文件 :241 一带 `self.block(block_ref)?.offset`），无 16 常量参与。

拒绝理由：

- `grep -rn "BLOCK_HEADER_SIZE" wedb` 全仓零命中——被举报的符号从来没在这个 HEAD 上。
- 所指 `limited_allocator.rs:554` 当下是私有辅助 `fn block_of(block_ref: BlockRef) -> usize`
  （引用 → 池内偏移，同文件内被调，非导出、非死码）。
- `lib.rs:42` 当下为 `pub use limited_allocator::{BlockRef, LuaLimitedManagedAllocator};`，
  不含任何常量导出项。

结论：无可删对象。

---

## 条 8 「active_deadlines 白盒口」——不成立：已收 `#[cfg(test)]`

> 8. wvector 超时登记白盒口 active_deadlines
>    /Users/z/git/db/wedb/wedb/wlua/src/timeout.rs:164（pub(crate)），读者在
>    /Users/z/git/db/wedb/wedb/wlua/src/cache.rs:405、:411 用例；C# 全域无 ActiveDeadlines。收 #[cfg(test)] 或删。

拒绝理由：`timeout.rs:161-171` 当下即为

```rust
  /// 测试面：活跃登记的截止值列表（0 = 空闲）。
  #[cfg(test)]
  pub(crate) fn active_deadlines(&self) -> Vec<i64> {
```

doc 建议的两个修法之一（收 `#[cfg(test)]`）已落地，release 面不再导出。另：条目标题写「wvector」而路径是
`wlua/src/timeout.rs`，标题与载体不符，亦提示该条正文承自旧快照未复核。

结论：无需改动。

---

## 条 13 子项「两 `is_infallible_allocation` 实现须合一（对标 C# 两文件的继承关系）」——不成立：C# 无继承关系

>    两 is_infallible_allocation 实现须合一（对标 C# 两文件的继承关系，禁两份同义实现各自演化）。

拒绝理由：C# 侧 `LuaLimitedManagedAllocator`、`LuaManagedAllocator`、`LuaTrackedAllocator` 是三个平级兄弟类，
各自 `implements ILuaAllocator`（`garnet/libs/server/Lua/LuaLimitedManagedAllocator.cs:26`、
`garnet/libs/server/Lua/LuaManagedAllocator.cs:22`、`garnet/libs/server/Lua/LuaTrackedAllocator.cs:17`），
彼此无继承；同名的 `private static bool IsInfallibleAllocation(...)` 分别定义在
`LuaLimitedManagedAllocator.cs:1171` 与 `LuaTrackedAllocator.cs:83`，判据各自不同
（前者按 BlockRef 是否落在 infallible 区段，后者按 size 阈值）。rust 两份同名件正是这个平级结构的 1:1 转写，
强行「合一」即破坏对标、并制造跨分配器耦合。

条 13 其余两点（`get_free_list` 壳与生产字段直读并存、`is_infallible_allocation` rust 侧无生产读者）
按代码事实成立，已在本批落地。

---

## 条 3 依据表述纠正（结论保留、照常落地）

>    C# 无对位符号（grep SharedCountAt / IsLockedAt 于 garnet 零命中）。收 #[cfg(test)] 或删。

表述不实：按字面名 grep 确实零命中，但按角色有对位——
`garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:182 NumLatchedShared`、
`:186 IsLatchedExclusive`、`:190 IsLatched`，且在 C# 由
`.../Implementation/OverflowBucketLockTable.cs:64/:69/:74` 生产消费（取闩前的判定）。
rust `StripedLatch` 对应的锁表并未消费这三个谓词（读者全在同文件 cfg(test) 区），
故「零生产读者 → 收门」的结论仍成立，本批按 `#[cfg(test)]` 收，仅登记依据以正口径：
收门的理由是「rust 侧无消费该观测面的锁表算法」，不是「C# 无对位」。

---

## 基线与去重备注（影响下一轮取证，不影响本批裁决）

- 本单声明的去重基线 `task/done/zero-consumer-pub-surface-census.md`（批一）、
  `task/done/zero-consumer-dead-symbols-cleanup.md` 在认领时 `task/done/` 为空目录，两份在册清单不存在，
  「五张在册清单均不含本批符号」这一句无法核对，已改由我逐符号在 `task/ing/` 现存文档里 grep 去重
  （批二/三/四在途单与本批符号交集为 0）。
- 全单行号系统性漂移（十余处 ±100 行内），已逐符号按名字重新定位后再裁决，未凭行号下结论。
- 类四各条复核与文档一致（C# 侧同名件确实零消费者，rust 1:1 同构）：
  `garnet/libs/common/RespReadUtils.cs:725 TryReadByteArrayWithLengthHeader`、
  `garnet/libs/server/Resp/Vector/AttributeExtractor.cs:99 ExtractField`、
  `garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:93 GetStartAddress`、
  `garnet/libs/server/Resp/HyperLogLog/HyperLogLog.cs:521/:537 SparseToDenseCopy/SparseToSparseCopy`
  —— 本批不动，留档防下轮重抄。
