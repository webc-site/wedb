轮8 随机抽样精读 B:存储引擎域 30 函数逐一对 C# 精读

方法:whlog/windex/wkv/wrecord/wbftree/waof/wcpr 七 crate 核心函数均匀抽 30 个,逐函数找 C# 对位(garnet/libs/storage/Tsavorite/cs/src/core、libs/server/AOF、libs/server/Resp/RangeIndex、libs/native/bftree-garnet)逐行对照语义。只报行为语义可差点(边界、顺序、失败路径、幂等性)。差异排前。

一、差异(行为语义可差)

1. wedb/wkv/src/session/raw/write/inplace.rs:try_upsert_raw_sync_unprotected
   C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs:InternalUpsert / CreateNewRecordUpsert + Implementation/Helpers.cs:CanElide
   判定:差异
   a) 源记录在只读区时的链首 elide 未落地。C# CreateNewRecordUpsert 的 elideSourceRecord = HasMainLogSrc && CanElide,对只读区链首(prev < BeginAddress)同样生效(SealAndInvalidate + TryTransferToFreeList);rust 的 elide_src 仅在 `addr >= read_only_addr` 的可变区探针循环内记录,只读区链首走盲插 prev=addr,旧记录永不被 elide/入复活池。读语义无损(新值在前),损失的是物理回收与复活机会,链上滞留死前驱。
   b) 密封在途记录(Closed)命中:C# TryFindRecordForUpdate 返回 RETRY_LATER 整操作重试;rust 直接 break 走盲追加新版本。两者最终值一致,rust 多产一条日志记录、少一次原地机会(代码注释已自述此刻意,列此存档)。

2. wedb/wkv/src/session/raw/write/inplace.rs:try_delete_raw_sync_unprotected
   C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalDelete.cs:InternalDelete + Implementation/Helpers.cs:HandleRecordElision
   判定:差异
   a) elision 门控不对称。C# HandleRecordElision 无论 RevivificationManager 是否启用都先 hei.TryElide 清链(区别只在 freelist 还是仅 SealAndInvalidate);rust 把整个 elision(含清链)包在 `record_elision()`(会话级,默认关)内,关闭时原位墓碑条目留在索引上。读语义同(墓碑即 NOTFOUND),索引状态与紧缩活性面不同:rust 保留墓碑条目(可被 upsert 链内复活利用),C# 摘除条目。设计边界注记已自述"刻意裁剪",但两侧默认形态相反,列为差异。
   b) 盲墓碑前驱取值:rust `prev_link = addr`(链头槽位地址);C# CreateNewRecordTombstone 的 previousAddress = recSrc.LatestLogicalAddress(回溯命中的本键记录地址)。仅碰撞/陈旧快照窗口下链拓扑不同:rust 保留碰撞键全链,rust 方向更保守;常规路径(槽位即命中)两者一致。
   c) 原位墓碑失败的幂等复检(rust 特有,复检并发抢先墓碑返回 false)为 C# 无的增强,杜绝双 notify,方向安全。

3. wedb/whlog/src/hlog/io.rs:read_disk_record
   C# 对位:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:AsyncReadBlittableRecordToMemory + IStreamBuffer.DefaultInitialIORecordSize
   判定:一致(刻意参数差异,已登记)
   冷读探针长度 rust 4096 vs C# 128;首段解析头、超出按物理尺寸精确二次读、越页界判 RecordCorrupted、Pad 拒绝,口径逐项一致。探针长度只影响 I/O 次数不影响结果。

4. wedb/waof/src/wal/log.rs:truncate
   C# 对位:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:TruncateUntil / UnsafeShiftBeginAddress
   判定:差异(刻意)
   rust 钳制 `safe_until = min(入参, committed)`(只截已获持久承诺字节)并持 commit_lock 立即物理删段 + last_commit_frame.fetch_min;C# TruncateUntil 仅 MonotonicUpdate 推进内存 beginAddress(调用方自保证位点合法,截未提交地址不拦),物理删段由 UnsafeShiftBeginAddress(truncateLog:true) 显式独立。rust 更保守:截断未提交地址被静默钳制而非报错,调用方无从感知被钳。

5. wedb/waof/src/wal/recover.rs:recover / note_recover_truncation
   C# 对位:libs/server/AOF/Recover/AofRecover.cs + libs/server/AOF/GarnetAppendOnlyFile.cs:DataLossCheck(FastAofTruncate 两态)
   判定:差异(刻意,文档已登记,补充两点边界)
   a) C# FastAofTruncate=false 对副本数据缺口显式拒绝恢复;rust 恒容忍截断,仅 recover_truncated_at/dropped_bytes 观测 + warn,"上层拒绝开关不做"。宿主若需 fail-on-recovery-error 语义须在 wnode 层自建(r6-cli 曾记复制域旋钮零通路,本条为 waof 内核侧同源确认)。
   b) 截断探测 has_nonzero_after:纯零窗口与整段清零介质损坏不可区分,按常态放行;探测读自身失败也放行(仅 warn)。C# 无此探测面(靠 commit 元数据界)。丢弃字节数 dropped_bytes_after 为段文件口径尽力值。

6. wedb/waof/src/wal/iterator.rs:WalScanIterator::next / read_record
   C# 对位:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLogScanIterator.cs:GetNext
   判定:差异(刻意,已登记)
   环形覆写残迹(CRC 失败且 addr < tail-cap)显式计 overwritten_skips 后终止;C# 页式先刷后逐出无此态。SegmentNotFound 且 begin 已越过 → 平滑返回 None(对齐 C# ScanBehindBeginAddress);未落盘窗口内 CRC 失败上抛 vs 已落盘回退磁盘权威数据,优先级判定正确。C# 扫描默认钳 CommittedUntilAddress,rust 由调用方传 end(scan_committed 同口径),未提交扫描面语义等价。

7. wedb/waof/src/wal/commit.rs:encode_payload / decode_payload / is_commit_frame
   C# 对位:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:TryEnqueueCommitRecord + CommitInfo.cs
   判定:差异(刻意,已登记)
   单写 in-log 24B 帧(MAGIC+begin+cookie)替代 C# 双写(in-log commit record + logCommitManager 元数据文件);UntilAddress 由帧尾位置承载不入帧;committed 收敛"扫至最后 commit 帧,无帧回退最后完整记录"。恢复端 frame_sync 后伪 24B 负载数据条目会被读负载判魔数后丢弃为普通帧,无误判路径(魔数首字节 0xFF 不在 AofEntryType 值域)。

8. wedb/wcpr/src/manager/create.rs:create_checkpoint_inner
   C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/HybridLogCheckpointSMTask.cs + IndexCheckpointSMTask.cs:GlobalBeforeEnteringState + libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint
   判定:差异(收敛语义等价)
   a) 模糊窗起点取点:C# PREPARE 入口取 startLogicalAddress = GetTailAddress();rust 在纪元排空屏障之后、索引扫描开跑前取 index_start = tail。rust 窗口更窄,但 index_start 之后新起记录地址恒 >= index_start,恢复重放窗口仍全覆盖,等价。
   b) C# 靠 RecordInfo.IsInNewVersion + undoNextVersion 区分模糊区新旧版本记录;rust 无版本位,以"区间内升序覆写收敛"等价替代(区间内不存在旧版本记录,论证成立)。
   c) RI 树写屏障(VersionShift)跨全程持有至 flush 完成,修复了"tail 捕获到快照之间树写混入"的旧窗口;C# 屏障仅覆盖对应阶段。rust 更强,方向安全。

9. wedb/whlog/src/scan.rs:validate_cursor
   C# 对位:libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteScanIterator.cs:SnapCursorToLogicalAddress
   判定:差异(刻意,已登记)
   落在记录中间的游标:C# 自页首步进回退对齐后续扫;rust 判 false 由调用方终结遍历回 (0,空)。漏扫方向可容忍已自述;cursor >= tail 判 false 与 C# InitializeGetNextAndAcquireEpoch 终结分支同口径。

二、刻意差异(已登记,本轮逐行复核为等价)

10. wedb/whlog/src/hlog/append.rs:append
    C# 对位:libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:TryAllocate / HandlePageOverflow
    判定:一致(刻意差异已登记)
    tail CAS(掩码地址) + 换页单点锁替代 fetch-add + overflow 线程重试;工程注记已说明不可复刻原因。无 partialSlots 分裂分配(C# 该分支仅 AOF chunk writer 可达,rust waof 不经 whlog,无调用方,非缺口)。记录不跨页、先发布 tail 后编码、换页打 Pad,口径一致。PageNotReady 错误替代 C# RETRY_LATER 阻塞等待,由调用方降级异步路径,活性等价。C# 换页时 IssueShiftAddress 按 MaxAllocatedPageCount 驱动 head 前移,rust 依赖 flush/ensure_page_ready 自动推进,驱逐驱动方式不同、终态不变式一致。

11. wedb/whlog/src/hlog/append.rs:ensure_page_ready
    C# 对位:AllocatorBase.cs:NeedToWaitForFlush / NeedToWaitForClose
    判定:一致(刻意差异)
    环形回绕复用三条件(flushed/head/safe_head)齐验,safe_head 纪元排空条件比 C#(flushed + ClosedUntil)更强;flushed 达标但 head 滞后时自动单调推进 head,替代 C# IssueShiftAddress 的 needSHA 路径。错误返回替代事件等待。

12. wedb/whlog/src/hlog/mod.rs:recover
    C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:AsyncReadPagesForRecovery
    判定:一致(刻意差异已登记)
    驻留预热限定 [head, flushed_until),[flushed_until, tail) 强制清零(恢复视图严格持久前缀);C# 全页读但未落盘区本为稀疏零,终态一致。段级批量预热失败退逐页;文件末端短读按空页;设备故障快停(Error::Device)对标 C# LogCommitFailureTests Phase 2a 默认臂。环形窗口容量守卫(num_pages)为 rust 增强校验。

13. wedb/windex/src/insert.rs:find_or_create_tag_by_hash_with_min_addr
    C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteBase.cs:FindOrCreateTag / FindTagOrFreeInternal
    判定:一致(刻意差异已登记)
    取消 Tentative 两阶段(CAS 占位 + FindOtherSlotForThisTagMaybeTentativeInternal 全链查重),合并为单次 CAS 0→完整条目,调用方按候选择新;可观测性等价(读者只见空槽或完整条目)。链尾见可复用空槽直接收口、不伪分配溢出桶,同 C# 该分支语义。截断清退、首空槽记录、逐槽分类与 C# FindTagOrFreeInternal 逐项对齐。

14. wedb/windex/src/find.rs:classify_slot
    C# 对位:TsavoriteBase.cs:FindTagOrFreeInternal 截断清退臂
    判定:一致
    C# 清退 CAS 目标 kInvalidAddress=0(LogAddress.cs:22),与 rust 置 0 同值;kTempInvalidAddress=1 豁免即 tentative 豁免(C# tentative 占位地址恒为 1),rust 用 is_tentative 位判定等价。rust 增 is_read_cache 数值豁免(RC 地址含 bit47 数值上不落入截断区,显式排除与 C# 隐式行为一致)。CAS 败者复核并发覆写命中为增强,方向安全。

15. wedb/wkv/src/store/resize.rs:split_buckets / split_single_chunk / grow_index
    C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:SplitBuckets / SplitSingleBucket + Index/Checkpointing/IndexResizeSM.cs
    判定:一致(刻意差异已登记)
    分块 CAS 抢占、环形遍历、目标分块 IN_PROGRESS 自旋等待逐项对齐。rust 增:迁移内核错误回滚 UNSTARTED 并上抛(C# 纯内存无错误路径)、相位发布与切表之间同表过渡窗防护(C# 无此窗)。traceback 对 below-head 地址回填 Some 的行为与 C# TraceBackForOtherChainStart 逐字一致(C# 同样把 below-head 地址插入对侧桶,rust 复刻)。RC 条目经 skip_read_cache 换算主日志地址再取哈希,与 C# IsReadCache 分支等价。

16. wedb/wbftree/src/service/bulk.rs:insert = bulk_load(单元素)
    C# 对位:libs/native/bftree-garnet/BfTreeService.cs:Insert
    判定:一致(刻意,批量折叠架构)
    单条 insert 经排序批量内核一元素特例,同键覆盖语义与逐条 insert 等价;空值整批 InvalidKV 前置拒绝把引擎 debug_assert 结构化(C# 原生层空值未定义)。引擎本体(bf-tree crate)与 C# FFI 同源。

17. wedb/wkv/src/ttl.rs:is_expired / check_expired / purge_expired
    C# 对位:libs/server/Storage/Functions/LogRecordUtils.cs:CheckExpiry
    判定:一致(架构差异 SKILL 已登记)
    到期边界严格小于(exp < now)与 C# 逐字一致;is_expired_or_now(exp <= now)仅限 EXPIRE 写路径即时删,与 C# 落库惰性清理终态等价,文档已论证。TTL sidecar 键架构为自定义面;purge 先删 TTL 后删数据 + claim 判点前置为 rust 自定义纪律,无 C# 单记录对位。

18. wedb/wrecord/src/codec.rs:publish_extent_header / write_record_unchecked
    C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:InitializeForNewRecord(WriteInfo)+ Allocator/RecordDataHeader.cs:Initialize
    判定:一致(刻意差异已登记)
    C# 新记录先写 Sealed+Invalid 头(扫描器 SkipOnScan 跳记录不跳页);rust 以 Pad 形态 extent 头单字发布承载同一协议,扫描器按物理尺寸精确步过在途槽,比 C# 16 字节最小长度守卫更强。写序"键值字节 → RecordInfo 字 → Release 屏障 → RDH 字"与读序 from_ptr_atomic 反向配对,同 C# 双阶段发布。

19. wedb/waof/src/wal/log.rs:reset / safe_initialize
    C# 对位:TsavoriteLog.cs:Reset / SafeInitialize
    判定:一致(刻意差异已登记)
    reset 仅回退内存位点不物理清零,复活窗口与 C# Reset 后未打检查点即崩溃语义一致,warn 留痕;safe_initialize 三位点一次写序一致。recovered_* 观测与 pending_cookie 的保留/复位取舍已在 recover.rs 注明为刻意。

三、一致

20. wedb/whlog/src/hlog/shift.rs:shift_begin_address
    C# 对位:AllocatorBase.cs:ShiftBeginAddress
    判定:一致
    ro 无条件冻结 → 补刷 [flushed, new_begin) → head(纪元延迟 safe_head)→ 纪元排空屏障 → 物理截断,与 C# "推进 begin → 补刷等待 → OnPagesClosed → TruncateUntilAddress" 时序等价;begin 后置为刻意(维持 head<=flushed 不变式),补刷后未达标显式报错为增强。截断屏障把在途磁盘读者钉在其入场纪元,封闭 C# BumpCurrentEpoch 闭包等价的 happens-after 窗。

21. wedb/whlog/src/hlog/shift.rs:shift_read_only_address / shift_read_only_address_with_wait / wait_flushed_until_address_async
    C# 对位:AllocatorBase.cs:ShiftReadOnlyAddress / ShiftReadOnlyAddressWithWait / WaitToRetryNow
    判定:一致
    Unsafe 先行发布 + Safe 经纪元动作推进同构;等待体三级退避(Sleep 阶段挂 flush 事件零轮询)替代 C# flushEvent.Wait。head 钳制 flushed_until(ShiftHeadAddress 钳制语义)一致。

22. wedb/whlog/src/hlog/io.rs:flush_sealed_page_range
    C# 对位:AllocatorBase.cs:OnPagesMarkedReadOnly + AsyncFlushPagesForReadOnly / WriteInlinePageAsync
    判定:一致
    先封后刷(SafeReadOnly 排空屏障)与 C# 纪元闭包同序;页对齐写、扇区圆整、短写不计入 flushed_until、错误区间按 flushed 钳制回填防"吸收陈旧区间→失败→放大"卡死。锁内页号双重校验封 TOCTOU。记账上界恒为封印上界,不越封印区承诺。

23. wedb/whlog/src/scan.rs:ScanIterator::next_ref
    C# 对位:libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteScanIterator.cs:GetNext + TsavoriteLogScanIterator.cs:779-790
    判定:一致(刻意差异已登记)
    Pad 精确步进=SkipOnScan"跳记录不跳页";全零头有界自旋(32768)对标 Thread.SpinWait(100)+SafeTailAddress 复查的等效内联;pad_seen 复核封"parse 与复读之间撕裂"窗口;磁盘页单页缓存、advance 页尾直达次页、闭包 false 提前终止一致。上游 d20d63993 的 BufferAndLoad frame 毒化缺陷经结构性不存在论证(无 frame 预占状态机)。

24. wedb/windex/src/chain.rs:ChainWalker::advance / advance_or_extend
    C# 对位:TsavoriteBase.cs:FindTagOrFreeInternal 链尾 Allocate→CAS 挂载→败者 Free 块 + MallocFixedPageSize.cs:Free
    判定:一致
    判尾后复读防伪分配、CAS 败者归还槽位、沿赢家桶推进,与 C# 块逐项同构;步数上限 2^22 判环为增强(C# 无界)。溢出指针挂载保留锁位状态(set_overflow_index 掩码 OR)与 C# 溢出槽复用语义一致。

25. wedb/windex/src/entry_info.rs:try_cas / try_elide / set_to_current
    C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/HashEntryInfo.cs:TryCAS / TryElide / SetToCurrent
    判定:一致
    CAS(old→new / old→0)与本地字回写一致;try_elide 的 raw==0 直接失败为增强(C# ExpectedEntry.word==0 时 CAS(0,0) 亦可成,调用方契约排除,rust 显式封堵);set_to_current 的 owns_slot(Tag 一致或空槽)判定取代 C# Tentative 占位隐含保证,论证完备。

26. wedb/wkv/src/session/raw/read.rs:try_read_mem / trace_back_for_key_match / find_in_read_cache / read_from_disk
    C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs + FindRecord.cs:TraceBackForKeyMatch + Implementation/ReadCache.cs:FindInReadCache + AllocatorBase.cs:AsyncGetFromDiskCallback
    判定:一致
    三区直读门槛(read_only 覆盖模糊区)、safe_ro 提升窗口([head, safe_ro))、密封 RETRY、墓碑即 NOTFOUND、链尽/低于 begin 即 NOTFOUND、磁盘候选降序、碰撞沿 prev、命中回填 readcache/tail 二者择一次序,逐项与 C# 对齐;RcVisit 三态把"不可判读"与"未命中"分离,杜绝假 NOTFOUND 折叠,方向安全。

27. wedb/wrecord/src/header.rs:位段 / physical_size / slack_for_val_len / try_seal
    C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs + Allocator/RecordDataHeader.cs(GetAlignedComponentSum / GetRecordLength / SetFiller)+ Allocator/LogRecord.cs:TrySetPinnedValueLength
    判定:一致(位宽放宽已登记)
    RecordInfo+RDH 合并为 16B 双原子字;record_size 对齐推导、physical_size=aligned+filler<<3、val_capacity=physical-header-key、slack 容量门(oldFillerLen 判定)口径一致。key 24bit/val 32bit 放宽(C# 10/24bit+overflow)与超限拒绝(C# overflow 分裂)已在文档登记;try_seal 单 CAS 置位、失败即 false 一致。

28. wedb/wbftree/src/service/ops.rs:read / read_into / scan_with_count_callback / scan_with_end_key_callback / drain_scan_iter
    C# 对位:libs/native/bftree-garnet/BfTreeService.cs:Read / ReadByPtrInto / ScanWithCountByPtrCallback / ScanWithEndKeyByPtrCallback / DrainScanIteratorWithCallback
    判定:一致
    结果码四态映射、值超缓冲 InvalidArguments、count=0 早退、start>end 空区间早退、回调 false 终止、8192 缓冲(栈)选路、迭代器构造失败 Err(C# throw)逐项一致;Arc 借用保活对标 C# LightEpoch 延迟释放。重入写同叶子自死锁约束两侧同。

29. wedb/wcpr/src/index_ckpt/batch.rs:BatchWriter::write_bucket / BatchReader::read_bucket_into + codec.rs:sanitize_data_slot / resolve_read_cache
    C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexCheckpoint.cs:Flush(FsWriteBuffer)+ HashBucketEntry 落盘换写(GetBucketWordForStable / ForRecovery 语义)+ ReadCache.cs:SkipReadCacheBucket
    判定:一致
    写侧:RC 条目顺链回写主日志地址(仅换 Address 字段指纹保留)、溢出槽剥锁位、仅换写指纹不污染;读侧:tentative/RC 位归零、address >= tail 钳 0(C# ForRecovery 同);断链复核(槽位重读一次)补 C# 纪元固定页的兜底差。32KB 批缓冲 + 流式 CRC 与 C# 批写结构同形。

30. wedb/wcpr/src/manager/recover.rs:run_recovery_kernel / recover_latest
    C# 对位:libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:RecoverFromPage + Tsavorite(GetClosestHybridLogCheckpointInfo)
    判定:一致
    墓碑一并重插:C# RecoverFromPage 对 !Invalid 记录不区分墓碑一律 hei.entry.Set,rust 同(升序覆写收敛最大地址,验证一致);min_valid_addr=begin 与 FindOrCreateTag(ref hei, BeginAddress) 同口径;窗口外跳过重插仅回调,索引 CAS 失败暂存统一上抛防静默丢键为增强。recover_latest 自新到旧容错跳过坏 token + warn,与上游 d20d63993 后语义一致;排序稳定性与 purge_outdated 交互论证成立。

统计
抽样 30(whlog 6:windex 6:wkv session/ttl 6:wrecord 3:wbftree 4:waof 5:wcpr 5 覆盖计数含跨域条目)
差异 9(其中刻意且已文档自述 6,真增量 3:#1a 只读区链首 elide 缺失、#2a elision 默认关与 C# 恒清链的默认形态相反、#4 truncate 钳制未提交地址静默吞)
刻意等价 10,一致 11
验证手段:append/recover/split/commit-frame/RecoverFromPage/TryElide/kInvalidAddress/TryResetModifiedAtomic 等均回读 C# 原文逐行比对,非仅凭 rust 注释锚

视角结论:有增量
