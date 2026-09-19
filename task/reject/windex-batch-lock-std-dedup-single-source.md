拒绝：windex 批量桶锁自造 in_place_dedup_by 内核（std 无切片 dedup_by，前提不成立）

结论：全单不成立，rm ing，不改码、不合并、worktree 直接弃用。
取证工具链：rustc 1.100.0-nightly (420ed2a0c 2026-09-18)（rustup default：nightly-aarch64-apple-darwin）；即本仓 windex 实际编译所用工具链（`cargo check -p windex` 与 `rustc` 解析到同一默认链）。

== 拒绝原因 ==

1. 立论根基（std `<[T]>::dedup_by` 为切片方法、自 1.47 起返回删除数）在本工具链为假。
   - std 源码：`grep "fn dedup"` 扫 `library/core` 全域 —— 零命中；`dedup / dedup_by / dedup_by_key`
     仅定义于 `library/alloc/src/vec/mod.rs`：`dedup_by` @2697、`dedup_by_key` @2644、`dedup` @3802。
   - `pub fn dedup_by<F>(&mut self, mut same_bucket: F)` 无返回类型 = 返回 `()`；`dedup_by_key` 亦
     `self.dedup_by(|a,b| key(a)==key(b))` 返回 `()`；`dedup` 同理。根本不存在「返回被删元素个数」这一说。
   - 真机 cargo check（/Users/z/git/db/wedb/wedb/windex/src/table.rs:664，按修法 1 落 `entries.dedup_by(..)`）：
     error[E0599]: no method named `dedup_by` found for mutable reference `&mut [(usize, bool)]` in the current scope。
     独立 rustc 探针（`&mut [(usize,bool)]` 与 `[(usize,bool);3]` 数组自动解引用）复现同一 E0599，确证与借用形态无关。

2. 被本单判为「两处错」的原注释（table.rs:681-683「slice 无 dedup_by——该方法为 Vec 专属，
   栈/堆统一切片借用故自行实现」）在本工具链恰恰是事实正确的：dedup_by 确为 Vec 专属、
   切片确无此方法。删除它才是制造失真。

3. 本单列举的「9 处切片 .dedup() 全部经 Deref 走同一个切片方法」实为反证：命中的是 Vec 固有方法。
   抽样 /Users/z/git/db/wedb/wedb/wext_json/src/json_path/path.rs:139 `sorted.dedup()` —— `sorted`
   上一行 :137 `.collect()` 即 `Vec`，走的是 `Vec::dedup`，其存在不能证伪「Vec 专属」。

4. 因此不存在「与 std 同一功能的两套内核」：`in_place_dedup_by` 是整条「栈数组 / 堆 Vec 统一借出
   `&mut [(usize,bool)]`」切片借用路径下唯一的去重手段（:671-679），std 无可平替的切片方法，
   且 Vec-only 的 `dedup_by` 无法服务栈路径（栈路径无 Vec，改用即需堆分配，摧毁刻意的
   「<=16 栈上内联零分配」设计，违反 transpile SKILL 零分配准则）。属单一来源、无冗余，注释即正确立论。

5. 次生点亦不成立或自相矛盾：`T: Copy` 是让 `slice[write_idx] = slice[read_idx]`（table.rs 手写内核体）
   免 clone / ManuallyDrop 的必需上界，调用点元素为 `(usize,bool)`（Copy），非「自造多余约束」；
   `get_unchecked` 的未越界前提（b_idx 恒由 `hash & mask` 截断）与去重无关，且本单现状段
   :653-656 自述已承认此点，故「三条契约仅靠注释、喂给 get_unchecked 的去重长度涉安全」不成立。

处置：保留 /Users/z/git/db/wedb/wedb/windex/src/table.rs 的 `in_place_dedup_by` 与原注释，不动、不合并。

== 原文（逐字，认领快照 task/ing/windex-batch-lock-std-dedup-single-source.md）==

windex 批量桶锁自造 in_place_dedup_by 内核：立论注释与 std 及其自身签名矛盾

来源：qcode10.db 条 3 立项。取证基线：主仓 HEAD f974dd1f（原快照 fd17e895 行号已重取）。

现状
- 手写内核：/Users/z/git/db/wedb/wedb/windex/src/table.rs:632-651
  `fn in_place_dedup_by<T: Copy, F>(slice: &mut [T], mut same_bucket: F) -> usize`，
  20 行游标搬移，唯一调用点 :685。
- 立论注释：/Users/z/git/db/wedb/wedb/windex/src/table.rs:681-683
  「slice 无 dedup_by——该方法为 Vec 专属，栈/堆统一切片借用故自行实现」。
  该断言两处错：std 的 `<[T]>::dedup_by` 本就是切片方法（stable since 1.0；自 1.47 起返回
  被删元素个数），Vec 无另一份实现、只是经 Deref 落到同一个切片方法；且本内核自己的签名
  就是 `&mut [T]`、调用点传入的也正是切片借用（:671-679 栈数组或堆 Vec 统一借出的
  `&mut [(usize, bool)]`），立论与其所服务的代码直接冲突。
- 次生代价：手写版额外要求 `T: Copy`（std 版无此约束）；「必须先排序」（:684 sort_unstable_by）、
  「保留首个即保留最高锁级」、以及去重长度喂给下游 `get_unchecked`
  （:687 → :709-723，SAFETY 注释在 :720-722，安全前提 doc 在 :653-659）三条正确性契约
  全部只靠注释维系，无类型或断言约束。
- 「Vec 专属」在本仓即可反证：以下 9 处切片 `.dedup()` 全部经 Deref 走同一个切片方法——
  /Users/z/git/db/wedb/wedb/wcpr/src/manager/mod.rs:416、
  /Users/z/git/db/wedb/wedb/wcustom/src/txn_proc.rs:157、
  /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:2661、
  /Users/z/git/db/wedb/wedb/wlua/src/loader.rs:461、
  /Users/z/git/db/wedb/wedb/wext_json/src/json_path/path.rs:139、
  /Users/z/git/db/wedb/wedb/wedb/src/server/replication/aof_sync_driver.rs:548、
  /Users/z/git/db/wedb/wedb/wedb/src/server/migration/migrate_driver/keys.rs:635、
  /Users/z/git/db/wedb/wedb/wepoch/tests/epoch/concurrency.rs:465、
  /Users/z/git/db/wedb/wedb/wnode/tests/resp_set.rs:594。

C# 参考（本仓不存在需要预压缩内核的位置）
- 相邻去重是加锁循环自身的判等分支，不是独立件：
  /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:63-102
  DoTransactionalLock（:65-68 注释 "The key codes are sorted, but there may be duplicates;
  … so we take the first occurrence of each key code"，:74 `prevBucketIndex = -1L`、
  :80-82 `if (currBucketIndex != prevBucketIndex)` 就地跳同桶）与 :104-155
  DoTransactionalTryLock（:120/:126-128 同形）。C# 全仓没有第二份「把切片压成唯一前缀并
  返回长度」的预压缩内核。
- 排序比较器：/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:86-92
  与 :94-100（桶下标优先、同桶按 LockType 字节升序），:113-115 SortKeyHashes；
  LockType 枚举值见 /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Utilities/LockType.cs:14/:19/:24
  （None=0/Exclusive=1/Shared=2，升序即排他优先，与 rust :684 的 `b.1.cmp(&a.1)` 同效）。
- 事务侧整段排序后直接交给 Lock/TryLock，同样无预压缩步骤：
  /Users/z/git/db/wedb/garnet/libs/server/Transaction/TxnKeyEntry.cs:114 与 :134。

修法（二选一，不留双）
1. 采信 std：删 :632-651 内核与 :681-683 错误注释，:685 改为
   `let removed = entries.dedup_by(|a, b| a.0 == b.0);` 后以 `entries.len() - removed`
   作切片上界（或紧随 truncate 直传 `&entries[..]`），:653-659 与 :720-722 的安全前提
   改由「同一个 std 切片方法的既有语义」背书；顺带去掉 `T: Copy` 这一自造约束。
2. 若坚持自造：改正立论注释（不得再称 slice 无 dedup_by、不得称该方法 Vec 专属），
   并把「相邻性/保首即最高锁级」前置为 debug_assert 或以类型收口，
   勿让锁路径的未越界前提仅由注释与调用顺序维系。
按本仓「一处定义、杜绝自造内核」口径取 1。

优先级
重复/多套架构（与 std 同一功能的两套内核，且第二套以错误事实立论）。

交叉引用
- 同文件族的批量预取面（batch_pipeline）已在 qcode10.db 撤销清单中判为与 C# 同构，本单不碰。
- 注释锚点类的失真另有两单：C# 锚点归 task/ing/gate-anchor-drift-reclean.md，
  rust 文件名级失效锚归 task/ing/wkv-stale-checkpoint-read-cache-anchor.md；本单是立论
  事实错误（std 能力），与该两单射程不重叠。

验收
- `in_place_dedup_by` 全仓零命中；windex 批量锁路径的去重唯一实现为 std 切片方法。
- 「slice 无 dedup_by / Vec 专属」类表述全仓零命中。
- 批量锁既有测试（含读写混合保留排他级、栈/堆两路径）全绿，无新增 unsafe 面。
