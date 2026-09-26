甄别结论：通过（甄别席 zc-fix-r16-recrecord，2026-09-26）定级 P1
核验记录（逐锚现码复跑，非票面背书）：
codec.rs:169-197 write_record_unchecked 头两字 copy_nonoverlapping 普通 memcpy、:190 fence(Release) 亲历验成立；:229-243 publish_extent_header RDH 字同为普通 copy（:237-241），写侧无原子 store 属实。
header.rs:523-536 from_ptr_atomic 双字 Acquire（:532-534）、scan.rs:63-80 is_zero_header 双字 Acquire（:72-74）读侧原子成立；header.rs:64-71、scan.rs:60-61、codec.rs:183-188 自证注释均锚定 Release 配对契约，写侧普通 store 下 synchronizes-with 虚构，指控成立。
inplace.rs:362-363/:394-395 双字 AtomicU64 Release store、record_mut.rs:148-152 publish_rdh 单字 Release 发布对照面成立，方案 1 收敛为同一协议属实。
append.rs:148-171 快路径 CAS 胜点后 :164-165 无锁 encode_at、:243 慢路径 encode_at、mod.rs:777-807 encode_at 首拍 :790 publish_extent_header 次拍 :797 encode_to_slice、walk.rs:35-55 next_record 经 decode_opt 普通解头、:141 flush_records_in_range 持 write_page 页锁而护不住无锁裸写，并发在场成立。
C# 锚亲验：RecordDataHeader.cs:46「a single atomic write to word publishes…」与 :129「All access MUST go through this word…」原文逐字对上；RecordInfo.cs WriteInfo（:90-101 含 InitializeForNewRecord 调用与「Otherwise, Scan could return partial records」注释）成立；SpanByteScanIterator.cs:170-229 allocatedSize 步进与 :229 SkipOnScan 成立。
查重：deviations.md 及 todo/ing/reject/issue 各池检索 write_record_unchecked/publish_extent_header/from_ptr_atomic/原子发布/撕裂/数据竞争，无同轴覆盖或并案；缺陷现状现码仍在。测试路径勘误核实：四套件实位于 wedb/whlog/tests/hlog/，票面注记属实。
架构与可执行度：方案为写侧改既有原子发布内核同形单点，无新抽象、无锁无分配、单向分层不破，验证闭环（四套件回归＋clippy＋test）齐备，纯文本格式合规。
合入哈希：16a775e 收口形态：codec.rs write_record_unchecked 头两字普通 memcpy 改按目标对齐分形态发布——8 字节对齐（并发日志页槽位）经对齐 AtomicU64 单点原子发布（RecordInfo 字 store + RDH 字 Release store，删中缝 fence 与 to_bytes 中间物），与 revivify_record_at/publish_rdh 收敛为并发侧唯一一套原子发布内核，非对齐序列化缓冲按读侧 is_zero_header 同款降级普通整头拷贝守住纯格式层非对齐安全契约；publish_extent_header RDH 字 copy 改单条 Release store；walk.rs next_record 头解码改 from_ptr_atomic 使 OnFlush 走查与扫描共用同一原子读序；header.rs/scan.rs/codec.rs/mod.rs 四处虚构 synchronizes-with「Release 屏障」叙述订正为「RDH 字原子 Release store」，行为零变化仅内存模型合法化（四套件 append_scan/scan_epoch_recycle/inflight_extent_scan/flush_records 65 例全绿）。

审核结论：通过（审核席 zcode-r16-review-hdrace，2026-09-26）

双侧亲验记录：
读侧原子确凿：wedb/wrecord/src/header.rs:532-534 from_ptr_atomic 先 RDH 后 RecordInfo 双字 Acquire 载入；wedb/whlog/src/scan.rs:72-74 is_zero_header 双字 Acquire 载入。
写侧普通确凿：wedb/wrecord/src/codec.rs:189-195 write_record_unchecked 头两字均 copy_nonoverlapping 普通 memcpy，中缝 fence(Ordering::Release)；:237-241 publish_extent_header extent RDH 字同为普通 copy。fence(Release) 在 Rust 内存模型仅对原子操作定序，普通 store 不参与 synchronizes-with，普通写与原子读并发同位置即数据竞争 UB；码内自证注释（header.rs:64-71 RDH_WORD_OFFSET 文档、scan.rs:60-61、codec.rs:183-188）均锚定该虚构契约，票面指控成立。
复活内核可收敛：inplace.rs revivify_record_at 双字 AtomicU64 Release store（:362/:394）与 record_mut.rs publish_rdh 单字 Release 发布内核现成，追加路径收敛为同一协议不引入第二机制。
并发真实在场：append.rs:159-165 tail CAS 胜点后无锁 encode_at；mod.rs:777-807 encode_at 第一拍 publish_extent_header、次拍 encode_to_slice；walk.rs:141 flush_records_in_range 虽持页写锁但 append 快路径无锁裸写，「页写锁护不住」属实。
C# 契约核实：RecordDataHeader.cs:129 原文一字不差；RecordInfo.cs WriteInfo 内 InitializeForNewRecord 注释「Otherwise, Scan could return partial records」属实；SpanByteScanIterator.cs:170-229 allocatedSize 步进与 SkipOnScan 属实。
查重：doc/zh/deviations.md 检索原子发布、头字、撕裂、数据竞争、from_ptr_atomic 等关键词无在册条目，非重复立项。
勘误一处（不翻案）：票面测试路径 tests/hlog/ 实为 wedb/whlog/tests/hlog/（whlog crate 内测试目录）。

整理优化执行方案（供 task/fix.md 直接消费）：
1 wedb/wrecord/src/codec.rs write_record_unchecked：头两字拷贝改原子单点发布——键值落笔后 (&*(ptr as *mut AtomicU64)).store(header.prev_address, Ordering::Relaxed)，收尾 (&*(ptr.add(RDH_WORD_OFFSET) as *mut AtomicU64)).store(header.rdh_word, Ordering::Release)；删除 fence(Ordering::Release)，hdr_bytes/to_bytes 中间物随字段直取消解（零分配）；键值拷贝次序与返回语义不变。内存序论证：RDH Release store 保证 sequenced-before 的全部写（键值加 RecordInfo 字）先于 RDH 可见，读侧先 Acquire 载 RDH 即 happens-before 成立，RecordInfo 字 Relaxed 足够（x86 与 Release 同码零开销，求与 revivify_record_at 完全同形可统一 Release，二者等价）。
2 wedb/wrecord/src/codec.rs publish_extent_header：extent RDH 字 copy 改 (&*(ptr.add(RDH_WORD_OFFSET) as *mut AtomicU64)).store(RecordHeader::pad(rec_size).rdh_word, Ordering::Release)，to_bytes 中间物同样消解。
3 wedb/whlog/src/walk.rs next_record：decode_opt 普通解头改 unsafe { RecordHeader::from_ptr_atomic(page.as_ptr().add(offset)) }；保留「字节不足一头返回 None」前置检查（from_ptr_atomic 无 Option 语义，须先验 offset 加 HEADER_SIZE 不越页界）；对齐安全契约由 RECORD_ALIGNMENT=8 页内记录起点恒对齐加步进量恒对齐保证。
4 注释同步收敛：header.rs:64-71、scan.rs:60-61、codec.rs:183-188、append.rs:70-75 的「Release 屏障配对」表述统一改「RDH 字原子 Release store」，清除虚构 synchronizes-with 叙述残部。
5 验证点：wedb/whlog/tests/hlog/ 四套件（inflight_extent_scan.rs、scan_epoch_recycle.rs、append_scan.rs、flush_records.rs）全绿回归，行为零变化仅内存模型合法化；EncodeStall/VersionReadStall 注入门用例继续锁定在途窗口语义；./sh/clippy.sh 零警告加 ./test.sh 全绿。数据面零开销复核：x86-64 上 Release store 编译为单条 mov 且较现状少一条 fence 指令，AArch64 上为 stlr 与复活路径同款，全程无锁无分配无新抽象。

记录头双字发布协议写侧为普通 memcpy，与原子读者构成跨线程数据竞争（码内自证的 synchronizes-with 契约在写侧为虚构）

问题分析：
1. Garnet 契约对齐：C# RecordDataHeader 将全部布局位段收进单 8 字节 word，RecordDataHeader.cs:129 注释明令 "The 8-byte word backing all fields. All access MUST go through this word to ensure atomic reads/writes"、:46 "a single atomic write to word publishes a fully-consistent new record layout"；RecordInfo.cs:WriteInfo（:90-101）对状态字同走单字写。C#（ECMA-335 I.12.6.6）保证对齐本机字长的读写原子，故「先 RecordInfo.WriteInfo、后 RecordDataHeader.Initialize 单字收尾」的双阶段发布在 C# 内存模型下真实成立，并发扫描者（SpanByteScanIterator.GetNext 按 allocatedSize 步进、SkipOnScan 跳记录不跳页）据此绝无半字混合态。
2. 工程现状确证：rust 读侧已按原子协议落地（wrecord/src/header.rs:RecordHeader::from_ptr_atomic :523-536，RDH 字 Acquire 载入在前；whlog/src/scan.rs:is_zero_header :63-80，双字 AtomicU64 Acquire 载入），且复活路径写侧同样原子（whlog/src/hlog/inplace.rs:revivify_record_at :362-363/:394-395 双字均 AtomicU64 Release store；wrecord/src/record_mut.rs:publish_rdh 原子单字发布）。唯独追加编码路径写侧是普通拷贝：wrecord/src/codec.rs:write_record_unchecked（:169-197，键值 copy_nonoverlapping 到位后，RecordInfo 字 copy_nonoverlapping、fence(Ordering::Release)、RDH 字 copy_nonoverlapping）与同文件 publish_extent_header（:229-243，在途 extent 头 RDH 字普通 copy）。fence(Release) 在 Rust 内存模型中只对原子操作定序，对普通 store 不建立任何跨线程 happens-before；from_ptr_atomic 文档声称的「该 Release store 与本次 Acquire 载入构成 synchronizes-with 边」在写侧不存在对应的原子 store，契约为虚构。读侧普通读同样暴露：whlog/src/walk.rs:next_record（:35-55 经 decode_opt 普通解头，flush_records_in_range 的页写锁护不住 encode_at 的无锁裸写）。
3. 逻辑危害确证：并发对真实在场且被刻意构造——append 的 encode_at 在 tail CAS 胜点后无锁编码（whlog/src/hlog/append.rs 快路径 :148-171、跨页慢路径 :243），扫描器按设计要在途读该槽位（ZERO_HEADER_SPIN_BUDGET 自旋 + extent Pad 按 physical_size 步进，tests/hlog/inflight_extent_scan.rs 即以 EncodeStall 把生产者钉死在「extent 头已发布、键值未落笔」窗口验证该协议）。普通写×原子读（walk.rs 为普通写×普通读）在 Rust 抽象机下属数据竞争即未定义行为：编译器对竞争内存的优化不受约束（可拆分头字拷贝、可重排普通 store 与原子 load 的可见次序），弱序架构上读者可先见新 RDH 而键值字节仍旧（Release-Acquire 配对缺位），16 字节头布局与键值内容撕裂；当前 x86/AArch64 上靠「对齐 8 字节拷贝恰好单指令 + fence 恰好落为 dmb ish」的指令级巧合成立，任何编译器版本、目标平台或内联策略变化都可能静默打破，违背码内自证的全部撕裂安全论证（probe_resident 文档第 2/3 条、from_ptr_atomic 读序论证、encode_at 两阶段发布协议）。

涉及代码：
rust 文件与函数：
wedb/wrecord/src/codec.rs:write_record_unchecked
wedb/wrecord/src/codec.rs:publish_extent_header
wedb/wrecord/src/header.rs:RecordHeader::from_ptr_atomic
wedb/whlog/src/scan.rs:is_zero_header
wedb/whlog/src/walk.rs:next_record
wedb/whlog/src/hlog/mod.rs:HybridLog::encode_at（编排点）
wedb/whlog/src/hlog/inplace.rs:revivify_record_at（已正确的原子发布对照面）

对应 c# 文件与函数：
libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:word 字段契约（:46/:129）与 Initialize（单字发布点）
libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:WriteInfo（:90-101，状态字先行发布）
libs/storage/Tsavorite/cs/src/core/Allocator/SpanByteScanIterator.cs:GetNext（并发扫描按 allocatedSize 步进的消费面）

精炼执行方案：
1. write_record_unchecked 两处头字拷贝改为经对齐 AtomicU64 发布：RecordInfo 字 store(Relaxed)、RDH 字 store(Release)，删除中缝 fence(Ordering::Release)（Release store 自带先序），键值拷贝次序与返回语义不变；与 revivify_record_at / publish_rdh 的既有原子发布内核收敛为同一协议。
2. publish_extent_header 的 extent RDH 字改 store(Release)；walk.rs:next_record 的头解码改走 from_ptr_atomic 口径（页内 offset 8 字节对齐不变式已满足其安全契约），使 OnFlush 走查与扫描器共用同一原子读序。
3. 测试验证点：tests/hlog/inflight_extent_scan.rs、scan_epoch_recycle.rs、append_scan.rs、flush_records.rs 全绿回归（行为零变化，仅内存模型合法化）；EncodeStall/VersionReadStall 注入门用例继续锁定在途窗口语义。
