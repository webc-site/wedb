终态：合入 076f99a（dev 经 f1038ed 快进收口），HybridLog 刷盘内核 flush_sealed_page_range 封印排空后入单把 async-lock 异步闸（flush_gate，守卫覆盖拷贝→写入→回填/短路→记账→唤醒尾段，闸内不等纪元不等 flush_event，不进 CRUD 热路径），同页重叠写恒串行、旧拷贝不再覆盖已提交前缀；新增 flush_write_order 确定性交错测试（摘闸复跑必红已验证）。

审核结论：通过

审核核实要点：
1. 无既有互斥：whlog/src/hlog/mod.rs:HybridLog 仅有 page_turn_lock（换页专用）与 PendingFlushList 内两把 parking_lot 短锁（list/completed，只护队列操作，不跨 await），flush_sealed_page_range 全程无在途写闸。wkv/src/store/flush.rs 注释所称「页序守卫在其内部同口径覆盖」在 whlog 内核中并不存在。
2. 并发真实：WedbStore/HybridLog 为 Sync（CircularPageBuffer unsafe impl Send/Sync），经 Arc 跨 worker 线程共享；即便单线程 compio 执行器，seal_read_only_and_drain、SegmentedDevice::write_impl 内 handle_capacity / get_or_open_file / write_at 均为让渡点，两任务可交错为「A 拷贝 → B 拷贝 → B 写完记账 → A 写落盘」。io_uring 对重叠写不保证完成次序，设备层无 FIFO 写序。
3. 旧拷贝确含未定稿字节：A 只封印到 t1（seal_bound.min(tail)），[t1, 页尾) 在 A 拷贝时刻可为零或在途半截记录；B 完成后 complete_flush_range 走顺序臂推进到 s_B，A 随后 complete_flush_range(from, t1) 因 max(current_flushed) 不回退，flushed_until 停在 s_B 而设备 [t1, s_B) 被 A 回写为陈旧字节。页内容非不可变（尾部持续追加），重叠写内容不同，反证不成立。
4. C# 对照成立：AllocatorBase.cs:AsyncFlushPagesForReadOnly :2210-2214 明确要求部分页片段等待前一相邻刷盘完成（PendingFlush + AsyncFlushPageCallback 链），同页写恒串行。
5. task/done、task/reject、task/todo 无同题裁决。
6. 方案评估：单个异步互斥归 whlog 所有，单机制；仅作用于刷盘内核（后台组提交、驱逐、紧缩挪线，本身已 await 设备 I/O），不进入 CRUD 热路径；用 async-lock（工作区已有依赖）跨 await 持异步闸而非同步锁，符合规范；闸内不等纪元、不等 flush_event，无死锁环。

多驱动并发刷盘对同一页发起重叠整页写且无写序约束，旧拷贝后落盘覆盖已计入 flushed_until 的字节

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# AllocatorBase.cs:AsyncFlushPagesForReadOnly（:2199-2239）每个封印区间只刷一次，区间互不重叠（起点恒为上一次 SafeReadOnly），且对页内部分区间显式串行：注释 :2210-2214 明言「必须等待前一相邻刷盘完成，否则尾扇区未完成的旧片段会覆盖已完成的同一扇区」，实现以每页 PendingFlush[index] 入队、AsyncFlushPageCallback（:2787 起）完成回调中 RemoveNextAdjacent(FlushedUntilAddress) 链式发起下一片段。即同一页上的设备写在 C# 中恒按地址/时间次序串行，后写者必为新内容。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
whlog/src/hlog/io.rs:flush_sealed_page_range 是三类驱动共用的刷盘内核：组提交 FlushStep::step（wkv/src/store/flush.rs:180）、前台驱逐 StoreSession::evict_pages_for（wkv/src/session/raw/mod.rs:347，任意会话/任意线程并发进入）、紧缩挪线 HybridLog::shift_begin_address 补刷（whlog/src/hlog/shift.rs:168）。组提交只串行化 flush_all 自身，后两者绕开该管线直入内核，内核内部无任何在途写互斥：
a. clamp_flush_range（io.rs:444-457）把区间起点恒钳到 flushed_until 所在页起点，coalesce（flush.rs:79-91）只合并「失败回填」的待刷条目，不登记在途写——两个并发调用必然得到重叠的整页区间（同含 flushed_until 所在页）；
b. 各自 seal_read_only_and_drain 后在 fill 闭包（io.rs:378-401）按各自时刻拷贝整页，再 write_aligned 下发；设备写路径 SegmentedDevice::write_impl（wdev/src/segmented_device/io.rs:161-198）在 write_at 之前还有 handle_capacity / get_or_open_file 两处 await，且无写序锁，io_uring 对重叠写亦不保证完成次序；
c. complete_flush_range（flush.rs:101-128）只按区间连续性推进 flushed_until，不感知同页更旧拷贝仍在途。
具体交错：任务 A（封印上界 t1，t1 位于页 P 中段）先拷贝页 P，此时 [t1, 页尾) 仍可能是零字节或在途半截记录；任务 B（封印上界 s_B > t1）后拷贝页 P（[t1, s_B) 已定稿），B 的写先完成，起点不高于 flushed_until 走顺序臂，flushed_until 直接推进到 s_B；随后 A 的旧拷贝落盘，把页 P 的 [t1, s_B) 覆盖回零/半截。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
flushed_until = s_B 已承诺，但设备上 [t1, s_B) 为陈旧字节：
a. 驱逐路径随后 shift_head_address 越过页 P，槽位被换页回收，内存副本消失；此后对该区间记录的读取全部走 read_disk_record 读到零头/撕裂记录——不需要崩溃即发生在线数据丢失或解析错误；
b. FlushStep 随后 device.sync 并把 synced_until 推进到覆盖该区间，检查点 meta 以此为持久化前缀，崩溃恢复（recover 按 flushed_until 前缀可信）把撕裂区当作已提交数据装载，记录链在零头处静默截断；
c. 页内部分重刷还叠加 C# 注释所述扇区级覆盖：两次写的尾/首扇区重叠，旧扇区后到即撕裂。
触发条件为常规负载：写入压力下前台驱逐（多会话）与组提交 flush 并发、或紧缩 shift_begin_address 与二者并发，均为生产路径。

涉及代码：
rust 文件与函数：
wedb/whlog/src/hlog/io.rs:HybridLog::flush_sealed_page_range（fill/write :378-401，记账 :428-431）
wedb/whlog/src/hlog/io.rs:HybridLog::clamp_flush_range
wedb/whlog/src/flush.rs:PendingFlushList::coalesce
wedb/whlog/src/flush.rs:PendingFlushList::complete_flush_range
wedb/whlog/src/hlog/shift.rs:HybridLog::shift_begin_address（补刷驱动）
wedb/wkv/src/session/raw/mod.rs:StoreSession::evict_pages_for（驱逐驱动）
wedb/wkv/src/store/flush.rs:FlushStep::step（组提交驱动）
wedb/wdev/src/segmented_device/io.rs:SegmentedDevice::write_impl（写前 await，无写序）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncFlushPagesForReadOnly
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncFlushPageCallback
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:PrepareFlushAsyncResult
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/PendingFlushList.cs:RemoveNextAdjacent

精炼执行方案（审核优化版，供 task/fix.md 直接消费）：
1. 依赖：cd wedb && cargo add async-lock -p whlog（工作区已声明 async-lock 3.4，经 workspace 继承，不手改 Cargo.toml）。
2. 字段：wedb/whlog/src/hlog/mod.rs:HybridLog 新增私有字段 flush_gate: async_lock::Mutex<()>（注释对标 AllocatorBase.cs:AsyncFlushPagesForReadOnly :2210-2214 同页写串行契约），构造处 Mutex::new(())。不新增任何队列，PendingFlushList 维持原状。
3. 内核：wedb/whlog/src/hlog/io.rs:HybridLog::flush_sealed_page_range 中 seal_read_only_and_drain(sealed_until).await 之后、第二次 clamp_flush_range 之前插入 let _gate = self.flush_gate.lock().await;，守卫存活至函数末尾，覆盖 fill → write_aligned → 错误回填 requeue / PageNotReady 短路 → complete_flush_range → flush_event.notify 全部尾段。封印排空留在闸外（持闸不等纪元）；入闸后第二次 clamp 已存在，空区间直接返回即实现并发者已覆盖的收敛，无需另加逻辑。同步更新函数文档：增加「刷盘写序」一节说明闸的不变式（同一时刻至多一个设备写在途，后入闸者恒拷贝更新的页内容）。
4. 注释校正：wedb/wkv/src/store/flush.rs:FlushStep::step 中「页序守卫在其内部同口径覆盖」改为指向 whlog flush_gate；调用侧（FlushStep::step、StoreSession::evict_pages_for、HybridLog::shift_begin_address）零代码改动。
5. 测试验证点：wedb/whlog/tests/hlog/support.rs:FaultDevice 增一种挂起模式（首个 write_aligned 在调用内层写之前 await 一个放行事件，复用 event_listener，不另造包装设备）；新增用例于 wedb/whlog/tests/hlog/ 下：追加记录使 tail = t1 位于页 P 中段 → 启动任务 A flush_page(P)（封印上界 t1，拷贝后挂起于设备写）→ 继续追加使 tail = s_B（仍在页 P）→ 启动任务 B flush_page(P) → 放行 A → 同时 join 两任务（修复后 B 阻塞在闸上，不可先单独 await B）→ 断言 flushed_until >= s_B 且设备 read 回 [page_start(P), s_B) 字节与内存页逐字节相等。修复前该用例 [t1, s_B) 必现零字节，修复后通过。
6. 验收：./sh/clippy.sh 无告警，./test.sh 通过。
