归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 af8be5e（P1），收口形态：两笔合入——主案（摘除在役表原位置零）先行落于 6c271f7，本笔 direct_vm.rs SAFETY 注释悬锚订正。续排注：本票沙箱席与方案详情见下文。

甄别结论：通过（甄别席 zc-fix-r16-flushlatch，2026-09-26）定级 P1
核验记录（逐锚现码复跑）：
rust 侧全部成立。keyspace.rs:444-453 flush_all_databases 于 :448 在 lock_dbmeta 闸内对 shift_begin_address 后 self.index.load().clear()，:434 注释宣称对标 C# index.clear()；链路 table.rs:56→buckets.rs:52→direct_vm.rs:226 ptr::write_bytes 全表置零（含闩位与 OVERFLOW_INDEX 槽），overflow_pool.rs:253 逐块 Box 即时释放；bucket.rs:93 unlock_shared 无条件 fetch_sub(1<<48)、:236 unlock_exclusive 无条件 fetch_and 清 bit63，共享计数 15 位@48 满即拒（:74-75）、独占位拒取（:113），零字回绕 0xFFFF_0000_0000_0000 恒 LockTimeout 推算成立；read.rs:919 EpochSuspendGuard 且 :877-881 注释自陈外层批守卫在场同样解除自钉；rmw_window.rs:195-201 Drop 放闩、basic_commands/slow.rs:284 等窗口跨 await 存活；txn_key_entry.rs:170-182 release_held 逆序放共享/独占闩；shift.rs:286 wait_safe_read_only_drained 为纯纪元排空不等待挂起自钉的持闩者；slow.rs:1203/:1250→database_manager_base.rs:576 在线达本路径。
C# 侧全部成立。DatabaseManagerBase.cs:301-310 FlushDatabase 仅 ShiftBeginAddress(+AOF 截断) 无任何索引清零；MultiDatabaseManager.cs:845 逐库循环同段；HashBucket.cs:67/:164 放闩为 Interlocked.Add/CAS 由持有者配对维护；TsavoriteBase.cs:316 FindTagOrFreeInternal 有「address < BeginAddress 即 CAS 置零原位清退」臂，:108 Initialize 仅换代重建。
非重复：deviations.md 全册与 todo/ing(空)/reject/issue/done(空) 无同轴登记（wcpr-checkpoint 票明示本票非同案，coldread 票明示 flushall 票非其面）；缺陷现码仍在。
架构：方案摘除 clear 归并到既有 min_valid_addr 惰性清退单机制（find.rs:179-208 CAS 置零臂在码），可选回收走 resize.rs 换代单点，二选一不并存，订正 :434 虚构锚，测试扩 pair_bucket_order_latch.rs 夹具——单向分层、无新机制、无假桩，符合 transpile/rust_review 纪律。

审核结论：通过，定级 P1。
确证 FLUSHALL 原位 clear() 在役哈希索引表会清掉在途桶闩锁字，致释放时回绕毒化或新持有者互斥失效。C# 原型仅 ShiftBeginAddress，从不清零在役表。方案摘除 index.clear()，走查找路径惰性清退，方案正确。

FLUSHALL 全域清空对在役哈希索引表原位清零，清掉在途桶闩持有者的锁字：陈旧放闩或使桶闩字回绕永久毒化（该桶全部写路径恒 LockTimeout），或清掉新持有者独占位使桶级互斥失效

问题分析：
1 Garnet 契约对齐。C# FLUSHALL/FLUSHDB 清库对位面（libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase）只做 db.Store.Log.ShiftBeginAddress(TailAddress)（MultiDatabaseManager.cs:FlushAllDatabases 逐库循环同一执行段），全程不清空、不置零任何哈希桶：低于 begin 的死条目交由查找路径惰性清退（TsavoriteBase.cs:FindTagOrFreeInternal 的 address < BeginAddress CAS 置零臂），运行期唯一的表重建点是扩容换代（TsavoriteBase.cs:Initialize，换代槽位整体重分配且经状态机屏障）。C# 桶闩字（HashBucket.cs:ReleaseSharedLatch/ReleaseExclusiveLatch）因此在任一时刻都由持有者自身配对增减，Interlocked.Add / CAS 作用在完整未被外部清零的锁字上，无「锁字被第三方置零后放闩」形态。
2 工程现状确证。wedb/wkv/src/store/keyspace.rs:WedbStore::flush_all_databases 在 lock_dbmeta 串行闸内执行 shift_begin_address(tail) 后于 :448 调用 self.index.load().clear()，链路为 windex/src/table.rs:HashIndex::clear → windex/src/buckets.rs:HashBuckets::clear → windex/src/ram/direct_vm.rs:DirectVirtualMemory::clear（ptr::write_bytes 全量置零整表），连每个桶 OVERFLOW_INDEX 槽位的 15 位共享读者计数与 1 位独占标记一并清零；overflow_pool.clear() 同步释放全部溢出桶块。而 shift_begin_address 的排空屏障是纪元语义（whlog/src/hlog/shift.rs:wait_safe_read_only_drained）：冷读磁盘 I/O 窗口经 wkv/src/session/raw/read.rs:read_from_disk 的 EpochSuspendGuard 显式解除纪元自钉（read.rs 注释自陈「外层批守卫在场时同样解除自钉…排空屏障在整段冷读期间停摆」为设计意图），此时闩仍被持有——RmwWindow（wkv/src/session/rmw_window.rs:RmwWindow，rmw_window().await 后窗口存活期内跨 probe_alive_domain/upsert_string 等 await）与 EXEC 期事务锁集（wtxn/src/txn_key_entry.rs:release_held 释放的共享/独占桶闩）均为「持闩 + 纪元挂起」的在途形态，排空屏障不等待它们。clear() 后这些持有者的 Drop 放闩落在已被清零（或已被新持有者重新占用）的锁字上：unlock_shared 为无条件 fetch_sub(SHARED_LATCH_INC)（windex/src/bucket.rs），unlock_exclusive 为无条件 fetch_and 清 bit63，均不校验持有身份。另 :434 注释「对标 C# 物理截断 O(1)：shift_begin_address(tail) + index.clear()」中 index.clear() 半步在 C# 侧无对位物，属虚构对标锚。
3 逻辑危害确证。其一（共享闩臂，wtxn EXEC 期共享锁持有者跨冷读挂起）：清零锁字上 fetch_sub(1<<48) 回绕得 0xFFFF_0000_0000_0000——共享计数位饱和 32767 且独占位置 1，try_lock_shared 见计数满拒收、try_lock_exclusive 见独占位拒收，该桶永久不可加闩（唯一复位面是下一次 clear() 或重启），落桶全部键的 RMW 窗口、TTL 闩、事务锁恒 LockTimeout（RMW_LATCH_YIELD_BUDGET / INNER_LATCH_RETRY_BUDGET 耗尽后写命令恒错帧），读路径虽免闩可读但写面全瘫；debug 构建下 unlock_shared 的 debug_assert!（prev 共享位为 0）直接 panic。其二（独占闩臂，rmw 窗口持有者跨冷读挂起）：clear() 清零后新会话 C 合法取到同桶独占闩，在途旧持有者 A 恢复后 Drop 放闩 fetch_and 清掉 C 的独占位，C 与后续 D 形成双独占并行动窗，桶级读改写互斥失效（同键并发 INCR 丢失更新、SETNX 双成功、对象装载写回互踩），无告警静默发生。FLUSHALL 为运行期在线命令（wnode/src/resp/garnet_api/slow.rs:flush_command_slow，ns 0 即达本路径），有负载服务上冷读在途窗口常态存在，触发面真实。查重：deviations.md 全册 150 条、r15 六档、r16-wkv/r16-repl/r16-tiered 三档、task/todo 与 task/reject 全目录均无本面登记（r15-conc 只核 lock_dbmeta 持有者互不嵌套的死锁面，r16-wkv 只核 flush_database 换号臂）。

涉及代码：
rust 文件与函数：
wedb/wkv/src/store/keyspace.rs:WedbStore::flush_all_databases（:437-452，:448 原位 clear；:434 虚构对标锚）
wedb/windex/src/table.rs:HashIndex::clear
wedb/windex/src/buckets.rs:HashBuckets::clear
wedb/windex/src/ram/direct_vm.rs:DirectVirtualMemory::clear（write_bytes 全量置零；:42 Sync 注释自证此类 &self 写路径需上层按桶加锁纪律，FLUSHALL 未满足）
wedb/windex/src/overflow_pool.rs:OverflowPool::clear（整池块即时 Box 释放）
wedb/windex/src/bucket.rs:HashBucket::unlock_shared / unlock_exclusive（无条件 fetch_sub / fetch_and）
wedb/wkv/src/session/raw/read.rs:read_from_disk（EpochSuspendGuard 冷读 I/O 窗，持闩者纪元挂起的确证点）
wedb/wkv/src/session/rmw_window.rs:RmwWindow::drop（跨 await 持闩的放闩点）
wedb/wtxn/src/txn_key_entry.rs:release_held（EXEC 期共享/独占闩的放闩点）

对应 c# 文件与函数：
libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase（:301-310，仅 ShiftBeginAddress，无索引清空）
libs/server/Databases/MultiDatabaseManager.cs:FlushAllDatabases（:845）
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/HashBucket.cs:ReleaseSharedLatch / ReleaseExclusiveLatch（锁字全程由持有者配对维护，无第三方置零面）
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindTagOrFreeInternal（begin 下死条目惰性清退单机制）
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:Initialize（表仅在扩容换代重建）

精炼执行方案：
1 flush_all_databases 摘除 :448 的 index.load().clear()：死条目收敛交既有单机制——begin 已推至旧 tail，全部旧条目即死条目，查找路径 classify_slot（find_or_create_tag_by_hash_with_min_addr / find_tag_entry_by_hash_with_min_addr 的 min_valid_addr CAS 置零臂）与读路径「低于 begin 判不存在」已闭环，与 C# 同款惰性清退，零新增机制；overflow 池残留空链与 C# 同形（机会主义复用，池容量上限 2^22 桶自限）。
2 若域主确需回收表内存，改走既有 grow 式换代协议（复用 resize.rs 状态机：事务屏障排空 + 纪元排空后 ArcSwap 换指新表，旧表随最后一个窗口/事务 Pin 的 Arc 析构），严禁对在役表 write_bytes 原位置零；二选一，不并存。
3 订正 keyspace.rs:434 注释：C# FlushDatabase 无 index.clear() 对标物，锚文本改为仅 shift_begin_address。
4 测试验证点：并发回归测试——线程 A 持 RMW 窗（或 EXEC 事务共享闩）对冷键进入磁盘 I/O 挂起窗，线程 B 执行 FLUSHALL，A 恢复并放闩后断言：目标桶锁字仍为 0（num_latched_shared == 0 且非独占）、该桶键后续写命令无 LockTimeout、无双持有（可扩展 wnode/tests/pair_bucket_order_latch.rs 的 pin_bucket 探针夹具形态）。
