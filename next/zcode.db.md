底层存储引擎、AOF、BfTree、CPR、GC 专项审查（轮 8）

结论概要：
对照 garnet 底层设计（Tsavorite / Allocator / Storage / AOF），全面审查 rust 的 whlog、waof、wbftree、wcpr、wcompact、wkv。
主干核心（whlog 追加与换页、wcpr 状态机与屏障等待、wcompact 逐记录判活与三阶段候选去重、wkv 虚拟域管理、waof 扇出与条目编码）架构清晰，与 C# 对位准确。
本轮审查发现 5 项涉及冗余设计、不合理降级、重复删除、空心包装及锚点冲突的问题，建议进行针对性重构与清理。与在途票（waof-sublog-commit-dedup、gc-compact-store-flight-gate、bftree-cpr-unwind-cleanup、wkv-rmw-traceback-unify）无重复。

问题 1：CPR 检查点恢复期 RangeIndex 存根自愈机制（堆分配与强推追加破坏只读边界）

具体问题：
在 cpr_host.rs 的 run_recovery_pass 恢复扫描 [begin, tail) 期间，RecoveryPassVisitor 逐条将所有历史版本的 RangeIndex 桩（KeyTag::Meta）分配为堆对象 (key.to_vec(), value.to_vec()) 塞入 HashMap。
在第 3 阶段，遍历哈希索引全部桶的所有槽位，若地址命中桩记录，则调用 patch_stub_record 与 hlog.try_update_in_place(addr, key, &healed)。
在 FoldOver 检查点（Tsavorite/Garnet 默认恢复形态，全量恢复时恢复区间都在只读区 read_only = tail）或冷区记录上，addr < read_only 恒成立，try_update_in_place 必定返回 false。
此时代码降级到 self.hlog.append(key, &healed, addr, false) 并在索引中更新为新地址。
这导致一次本应完全只读、恢复到检查点一致性边界的恢复操作，在日志尾部强行追加了大量自愈记录，推进了 tail 地址并改写了哈希索引。
同时在扫描期间，对整条混合日志的历史版本做无界堆分配（HashMap<u64, (Vec<u8>, Vec<u8>)>），造成严重堆开销。

rust 侧文件与函数：
wedb/wkv/src/store/cpr_host.rs:RecoveryPassVisitor::on_record（约 :290-302）
wedb/wkv/src/store/cpr_host.rs:WedbStore::run_recovery_pass（约 :204-247）

c# 对位文件与函数：
libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:RecoverHybridLogAsync（约 :1260-1262）
libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnRecoverySnapshotRead（约 :148-167）
libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:MarkRecoveredFromCheckpoint（约 :388-394）

对照分析与建议动作：
C# 在 HybridLog 恢复遍历中，直接在恢复缓冲区页加载期就地触发 OnRecoverySnapshotRead，通过 Unsafe.As 就地修改记录字节切片（stub.TreeHandle = nint.Zero; stub.IsRecovered = true;），既不产生中间堆分配集合，也不遍历哈希索引，更绝不会在恢复期对 HybridLog 调用 Append 强推尾部记录。
建议优化：在 run_recovery_kernel 扫描或页面加载时就地完成桩内存字节更新（针对内存驻留页）或仅将 pending 状态注册到 RangeIndexManager 内存映射（由后续访问触发惰性恢复，对标 C# 的惰性恢复），移除 Step 3 的全索引哈希扫描和 hlog.append 降级追加。

问题 5：check.js 存储域锚点撞名冲突甄别与注释清理（InPlaceUpdater 与 TraceBackForKeyMatch）

具体问题：
在运行 ./js/check.js 检查时，存储引擎相关文件中存在多处锚点冲突警报：
1. libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdater 同时出现在：
   - 生产代码：wedb/wkv/src/session/raw/modify.rs:53, 94
   - 存储日志：wedb/whlog/src/hlog/inplace.rs:30, 81
   - 测试文件：wedb/whlog/tests/hlog/inplace_lifecycle.rs:243, 352, 461
   - 测试文件：wedb/wrecord/tests/record/lifecycle_and_chains.rs:449, 585
2. libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindRecord.cs:TraceBackForKeyMatch 同时出现在：
   - wedb/wkv/src/session/raw/read.rs:459（trace_back_for_key_match，真正的读回溯实现）
   - wedb/wkv/src/session/raw/modify.rs:12（trace_live_mutable_addr，RMW 专用的可变区探测辅助函数）

rust 侧文件与函数：
wedb/whlog/src/hlog/inplace.rs（约 :30, :81）
wedb/whlog/tests/hlog/inplace_lifecycle.rs（约 :243, :352, :461）
wedb/wrecord/tests/record/lifecycle_and_chains.rs（约 :449, :585）
wedb/wkv/src/session/raw/modify.rs:StoreSession::trace_live_mutable_addr（约 :12）
wedb/wkv/src/session/raw/read.rs:StoreSession::trace_back_for_key_match（约 :459）

c# 对位文件与函数：
libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdater
libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindRecord.cs:TraceBackForKeyMatch

对照分析与建议动作：
根据既定规范（task/done/checkjs-dup-anchor-families.md），必须保持“本体单点持锚、测试与旁述去形”：
1. 测试代码（inplace_lifecycle.rs、lifecycle_and_chains.rs）严格禁止挂载生产 C# 锚点语法 libs/...cs:Symbol，必须剥离为纯中文对标叙述；
2. whlog/src/hlog/inplace.rs 中的原位方法属于底层日志分配器能力，不应直接挂宿主 MainStore 的 InPlaceUpdater 锚，改用概念描述；
3. modify.rs 的 trace_live_mutable_addr 剥离 TraceBackForKeyMatch 锚形，由 read.rs:trace_back_for_key_match 独占该锚点。

已复核成立、不立项（供后续轮次免重跑）
- whlog append/换页/原位更新/扫描：append.rs 的 CAS 分配推进、io.rs 异步落盘、walk.rs 混合扫描与 inplace.rs 原位更新协议均与 C# AllocatorBase / InternalRead 对位严密，无新增设计缺陷。
- waof 扇出与持久化：SingleLog / ShardedLog 架构清晰，条目编解码与 C# AOF 二进制序列化严格对齐；针对在途票 waof-sublog-commit-dedup.md 已登记的提交流水线冗余，本轮复核无新增发散。
- wcpr 检查点状态机：create.rs 的 VersionShift、Prepare、Snapshot 阶段转换与纪元排空等待完全对照 Garnet CPR 时序，无时序协议风险。
- wcompact 紧缩流程：CompactLookup / CompactScan 双模式、候选记录死亡判定短路逻辑与 TsavoriteCompaction.cs 对齐准确；关于紧缩并发门控已由在途票 gc-compact-store-flight-gate.md 覆盖，本轮不重复立项。
