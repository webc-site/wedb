终态：合入 8d02cbc（d8374d1 并轨），seqlock 单机制收口：写侧 enqueue_reserved 预占后 fence(Release) 再写环，读侧三处（迭代器两形态 + scan_memory_records）经 WalLogInner::ring_copy_intact 单源谓词（fence(Acquire) 重读 tail，tail_now <= record_addr + cap）拷后复核，失败整体丢弃回退设备权威数据；ring_buffer.rs Sync SAFETY 注释按真实不变式订正；门闩用例确定性复现修复前错位帧（0x80 读出别名帧 seq=1000）、修复后复核拦截稳定通过，另附并发冒烟回归护栏。

甄别结论：通过（P1，数据正确性：主从静默分叉 + 副本断链）

甄别复核记录（现码逐点复跑）：
1. C# 锚亲验：TsavoriteLogScanIterator.cs GetNext 于 epoch.Resume 内经 GetNextInternal 取 HeadAddress 快照、currentAddress >= HeadAddress 走内存页并 Buffer.MemoryCopy 后 epoch.Suspend；AllocatorBase.cs:1837 OnPagesClosed 经 epoch drain 后才回收页帧。页式分配写新页不覆写旧页，内存臂被读字节绝无别名覆写，锚成立。
2. rust 锚亲验：iterator.rs scan_step 一次性 mem_base 快照、read 判 record_addr >= mem_base - cap 走内存臂且 decode Valid 即返无拷后复核、mem_decode_failed 仅覆盖 Invalid/CrcMismatch；pipeline.rs reserve_address 仅约束 required_end - align_down(flushed) <= buffer_size（tail 可推至 align_down(flushed)+cap，ring_offset 逻辑差 cap 整数倍即物理重合）；ring_buffer.rs Sync SAFETY 注释确实漏物理别名；log.rs scan_memory_records 入口一次性 mem_base；header.rs 帧头仅 8B（entry_len+crc32）无地址字段，addr+cap 处自洽新帧 CRC 必过；aof_replication_pump.rs:315 next_frame 推流竞态可达。全部成立。
3. 查重：四池无同判据票，doc/zh/ 无 deviations.md。
4. 方案复核：seqlock 预占模式（写侧 CAS tail → fence(Release) → 环写；读侧拷贝 → fence(Acquire) → 重读 tail，谓词 tail_now <= record_addr + cap）对齐 Linux kernel seqlock 实践摆位，编译器与硬件（TSO/ARM dmb）层面正确，单机制最小收口；覆写可达蕴含 record_addr < flushed，设备回退恒合法。

审核结论：通过

核实要点：
1. 真实性成立：iterator.rs:WalScanIterator::scan_step 仅读前取一次 tail / safe_tail 快照，read 内存臂 decode_frame / decode_frame_assembled 得 Valid 即返回，拷后无复核；log.rs:WalLog::scan_memory_records 同型。
2. 覆写可达：pipeline.rs:WalLog::reserve_address 仅约束 required_end - align_down(flushed) <= buffer_size，无纪元、无读者水位，写者可把 tail 推到 align_down(flushed) + cap，覆盖读者已判在窗内且 record_addr < flushed 的物理偏移；AofBackpressure 在 wnode AOF 层，不约束环形回绕。
3. 非同线程串行：acquire_inflight_slot 按线程 ID 分槽，多核写者并发入队；aof_replication_pump.rs 第 315 行 iter.next_frame() 在推流任务内执行，跨核竞态可达（快照到拷贝之间虽无 await，同核写者不可插入，但异核写者可）。
4. 帧头无地址识别：header.rs:WalFrameHeader 仅 entry_len + crc32 共 8B，无地址或序号字段，addr + cap 处自洽新帧 CRC 必过，错位不可识别。
5. C# 对照：TsavoriteLogScanIterator.GetNext 在 epoch.Resume 内拷贝，OnPagesClosed 等读者退纪元才回收页帧，内存臂不可能读到别名数据；本实现缺此保护。
6. SAFETY 注释前提不成立：ring_buffer.rs 声称读写区不相交，忽略环上物理别名。
7. 查重：task/todo、task/reject、.forks/sync-2026-10-02 无同类条目。
8. 方案修订：原方案只给读侧 Acquire 栅栏，seqlock 写侧缺 Release 栅栏（tail CAS 的 Release 只约束其前访存），已补入步骤 1；复核谓词 tail_now <= record_addr + cap 正确且充分；设备回退恒合法（覆写可达蕴含 record_addr < flushed）。

WAL 扫描迭代器内存臂读环形缓冲无拷贝后复核，并发写入回绕覆写同一物理偏移时可把 addr+cap 处的新帧当作 addr 处记录合法返回

问题分析：
1. Garnet 契约对齐
   C# TsavoriteLogScanIterator.GetNext 先 epoch.Resume()，在纪元保护内经 GetNextInternal 取 HeadAddress 快照、判定 currentAddress >= HeadAddress 后走内存页，再 Buffer.MemoryCopy 拷出条目，最后 epoch.Suspend()。页淘汰（OnPagesClosed）须等全部持纪元读者退出才回收页帧，故内存臂拷贝期间被读字节绝不会被新写入覆写，内存读到的永远是 currentAddress 本身的记录。

2. 工程现状确证
   wedb/waof/src/wal/iterator.rs:WalScanIterator::scan_step 在读之前一次性取 mem_base（tail_address 或 safe_tail 快照），read() 仅以 record_addr >= mem_base - cap 判定走内存臂，随后 RingBuffer::decode_frame / decode_frame_assembled 拷出帧头与负载并校验 CRC，Valid 即直接返回，拷贝之后不再重读 tail 复核。
   写侧 wedb/waof/src/wal/pipeline.rs:WalLog::reserve_address 的窗口约束只有 required_end - align_down(flushed) <= buffer_size，即写入者可把 tail 推到 align_down(flushed) + cap，其环形物理偏移对应逻辑区间 [旧 tail - cap, align_down(flushed))。读者快照判定在窗口内的 record_addr 正落在这段可被覆写区间。快照之后、拷贝之前或拷贝期间只要并发写入预占越过 record_addr + cap，同一物理偏移即被 record_addr + cap 处的新数据覆盖。
   现有兜底只覆盖“读到残缺”：wedb/waof/src/wal/iterator.rs:WalScanIterator::mem_decode_failed 对 Invalid / CrcMismatch 且 record_addr < flushed 的情形回退设备读。但当 record_addr + cap 恰是新数据的帧起点（定长负载、2 的幂帧长、commit 帧 32B 对齐等场景下非稀有），环中整帧（头 + 负载 + CRC）自洽，decode 判 Valid，迭代器把未来记录当作 record_addr 处记录返回，next_addr 也按新帧长度推进，游标从此偏离真实帧边界。
   wedb/waof/src/wal/ring_buffer.rs 的 unsafe impl Sync for RingBuffer 安全注释声称“写侧只落在 tail_address 预留的新区域，读侧只在 safe_tail_address/committed_until_address 以下取样”，忽略了新预留区与读者取样区在环上物理别名重叠，该不变式不成立。
   wedb/waof/src/wal/log.rs:WalLog::scan_memory_records 同型：mem_base 仅入口取一次，整段循环（含回调执行时间）不复核。

3. 逻辑危害确证
   生产消费者 wedb/wedb/src/server/replication/aof_replication_pump.rs 的推流循环经 WalScanIterator::next_frame 读帧转发副本。慢副本 accepted_address 滞后约一个环容量（默认 16MB）时扫描贴近环最旧边界，AofBackpressure 默认未启用（enabled=false、预算 i64::MAX），写负载高时边界竞态可达。命中后：
   a. 副本收到错位的未来帧并按 record_addr 记账应用，主从数据静默分叉（重复应用非幂等命令，如 INCR / LPUSH）。
   b. 游标按错误 next_addr 前进，后续读落在真实帧中段，内存臂 Invalid 回退设备后读到非帧头字节，CRC 失败上抛，副本被出册断链，掩盖首个错误帧已发出的事实。
   c. 读写同一内存的非原子并发访问违反 RingBuffer Sync 的 SAFETY 前提。

涉及代码：
rust 文件与函数：
wedb/waof/src/wal/iterator.rs:WalScanIterator::scan_step
wedb/waof/src/wal/iterator.rs:WalScanIterator::read
wedb/waof/src/wal/iterator.rs:WalScanIterator::mem_decode_failed
wedb/waof/src/wal/ring_buffer.rs:RingBuffer::decode_frame
wedb/waof/src/wal/ring_buffer.rs:RingBuffer::decode_frame_assembled
wedb/waof/src/wal/ring_buffer.rs:unsafe impl Sync for RingBuffer
wedb/waof/src/wal/pipeline.rs:WalLog::reserve_address
wedb/waof/src/wal/log.rs:WalLog::scan_memory_records
wedb/wedb/src/server/replication/aof_replication_pump.rs:next_frame 推流循环

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLogScanIterator.cs:GetNext
garnet/libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLogScanIterator.cs:GetNextInternal
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:OnPagesClosed

精炼执行方案（审核优化版，单机制：seqlock 式“预占先行 + 拷后复核”）：
1. 写侧发布序（pipeline.rs:WalLog::enqueue_reserved）：reserve_address 成功返回后、首个 ring_buffer.write_record_parts 之前插入一次 std::sync::atomic::fence(Ordering::Release)。理由：tail CAS 的 AcqRel 只约束 CAS 之前的访存，形式模型下不保证其后的环写不先于 tail 新值被读者观测；seqlock 写侧须“序号存储 → Release 栅栏 → 数据写”，此栅栏与读侧 Acquire 栅栏配对成立 fence-fence 同步。每次入队一条栅栏，x86 为空指令，ARM 为 dmb ishst 级，无分配无锁。
2. 读侧复核谓词单源（WalLogInner 上新增 #[inline] fn ring_copy_intact(&self, record_addr: u64) -> bool）：fence(Ordering::Acquire) 后 tail_address.load(Relaxed)，返回 tail_now <= record_addr + cap。判据依据：环写只落在已预占区间，帧 [record_addr, next_addr) 的物理偏移被别名覆写的必要条件是某次预占终点 > record_addr + cap，tail 单调，故复核通过即拷贝期间无覆写。不需要按 next_addr 判，帧起点物理位一旦未被覆写，其后字节对应的别名区更晚。
3. WalScanIterator::read 内存臂两形态（decode_frame / decode_frame_assembled）得 Valid 后统一调用 ring_copy_intact(record_addr)，失败则丢弃内存结果直接 read_from_device。回退恒合法：覆写可达要求 record_addr + cap < tail <= align_down(flushed) + cap，即 record_addr < flushed，设备必有权威副本。Invalid / CrcMismatch 既有处置（mem_decode_failed）不动；scan_step 的 mem_base 预判保留，仅作免拷贝快筛。
4. WalLog::scan_memory_records 每帧 decode_frame 得 Valid 后、调用回调前套用同一 ring_copy_intact(cur)，失败即 return false（调用方 waof_sublog.rs:WaofSublog::scan_with 已有 false 告警语义，绝不把错位帧交给回调）。
5. 修正 ring_buffer.rs 中 unsafe impl Sync for RingBuffer 的 SAFETY 注释：删去“写侧只落在新区域、读侧只在水位以下取样”的不成立不变式，改写为“写侧先 CAS 预占 tail 再 Release 栅栏再写；读侧拷贝后 Acquire 栅栏重读 tail，经 ring_copy_intact 丢弃可能被环形别名覆写的拷贝（seqlock 语义，竞态读出的字节恒被丢弃不被消费）”。
6. 闭环测试点（waof/tests/wal 新增一个并发用例）：小环容量（4 个扇区）、定长且总帧长为 2 的幂的帧、负载内嵌单调序号；多写线程持续 enqueue + commit，读线程以 tail - cap 附近起点反复 scan(..).next_frame() 与 scan_memory_records；断言每个返回帧的 address 与负载内嵌序号推导的地址严格相等、next_address 链连续。修复前该断言可复现失败，修复后稳定通过；同时跑 ./test.sh 全量回归。
