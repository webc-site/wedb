fixloop 拒绝档案：next/agy.db.md（底层引擎 aof / bftree / 存储）
核销 2026-09-19（dev 分支当下 HEAD，行号按符号现取）。本档只记本路源文件被裁为「观点不成立」的条目；
判「已落地」的两条（条 1 TxnLockTable 逐桶调闭包、条 18 wdev 切片双副本）不入本档，证据见分拣回报。

格式说明：每条先转录原文，再写拒绝理由与代码事实。原文行号是票内所写，不代表当下位置。

----------------------------------------------------------------------

条 4 TransactionManager 侵入具体业务 API 与堆分配排队命令（部分拒绝）
原文观点：transaction_manager.rs 内部直接定义了 get、set、setex、delete、increment、sorted_set 等具体
业务数据结构 trait（TxnProcApi），且 txn_proc.rs 的 TxnQueuedCommandInfo 使用堆分配 String 存储命令名。
rust：wedb/wtxn/src/transaction_manager.rs trait TxnProcApi trait TxnProcReadApi struct TxnWatchApi；
wedb/wtxn/src/txn_proc.rs struct TxnQueuedCommandInfo
对应 C#：libs/server/Transaction/TransactionManager.cs；libs/server/Custom/CustomTransactionProcedure.cs
动作：业务过程接口 TxnProcApi 与 TxnWatchApi 移出事务状态机核心文件，收敛到 txn_proc.rs 或 wnode 会话层。

拒绝理由（只拒「搬迁 trait」这半条；本条的堆分配半条成立，已单独立项
task/ing/wtxn-queued-command-name-static-str.md）
1. 搬迁无收益且伤内聚：TxnProcApi（transaction_manager.rs:125）/ TxnProcReadApi（:144）/ TxnWatchApi
   （:155）三者的唯一消费方是同文件的 TxnProcedure 三段式（:173，prepare 收只读视图、main/finalize 收
   读写视图）与 TransactionManager::run_transaction_proc（:632）。视图与消费者同文件是本仓惯例
   （对标 C# CustomTransactionProcedure.cs 同时声明过程基类与其 GarnetApi 形参界）。
2. 票述失实：TxnProcApi 不是「业务数据结构 trait 定义处」，而是 8 方法的纯派发界（get/set/setex/
   delete/increment/sorted_set_add/sorted_set_remove），真实落点在 wnode
   storage/session/txn_proc_view.rs，文件头 :120-124 已注明该映射口径与 js/check/ignore/server.yml 的
   甄别结论。搬到 txn_proc.rs 只是换文件名，check.js 的 C#→rust 锚点不因文件位置改变而消解。
3. 违背「简洁优先、不做无收益重构」的取向：spec 要求对照 C# 让拓扑更吻合，而 C# 侧
   IGarnetApi 的对本域投影本就是「事务过程的最小 API 面」，与事务状态机同处一文件不构成层级倒置
   （wtxn 未反向依赖 wkv/wnode）。

----------------------------------------------------------------------

条 5 session/raw 目录碎化与单函数微型文件
原文观点：session/raw 下存在多个仅含 1 个函数的微型文件（append.rs 40 行、modify.rs 135 行、rmw.rs 105
行），过度碎化增加跨文件跳转负担。
动作：将 append、modify、rmw、copy_to_tail 整合入 write/mod.rs，写路径集中维护。

拒绝理由（方向与「拓扑和 C# 更吻合」相反）
1. 这四个文件各对应 C# Tsavorite 的独立实现件：
   libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/BlockAllocate.cs（append）、
   InternalUpsert.cs（modify/upsert）、InternalRMW.cs（rmw）、TryCopyToTail.cs（copy_to_tail）。
   C# 侧本就是「一个操作一个实现文件」的 Implementation/ 目录拓扑（同目录 24 个文件，
   InternalDelete.cs / ContainsKeyInMemory.cs / ReadCache.cs / SplitIndex.cs 等皆单职责小文件）。
   合并进 write/mod.rs 等于把 4 个 C# 文件塞回 1 个 rust 文件，对标度下降。
2. 无重复代码、无死代码可清：合并只是文件数变化，check.js 的 File.cs:Fn 注释锚点按符号登记，
   合并不会消解任何重复定义，也不会减少一行代码。
3. 行数本身不构成缺陷：本仓同类单操作文件普遍偏小（wkv/src/session/raw/write/append.rs 40 行），
   且其文档注释承载 C# 映射，是规范要求（/// 在 garnet 中的相对路径:函数名）而非凑数。
4. 与同文件条 4 自相矛盾（条 4 嫌读文件变体多、条 5 嫌写目录碎），说明该组主张是风格偏好而非取证结论。

----------------------------------------------------------------------

条 12 whlog AddressManager 读路径缺乏寄存器级原子快照
原文观点：is_mutable / is_read_only / is_in_memory 等判定方法均执行多次 AtomicU64::load(Acquire)，
且 snapshot() 无锁顺序读 7 个字段，并发下可能读出跨字段不一致状态。
rust：wedb/whlog/src/address.rs struct AddressManager fn AddressManager::is_mutable is_read_only snapshot
对应 C#：libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs；
libs/storage/Tsavorite/cs/src/core/Allocator/HybridLogConfig.cs（后者在主仓不存在）
动作：批处理或会话读循环前提供局部快照，热路径判定用快照内寄存器数值直接计算。

拒绝理由
1. 区域判定体已单点化，票述「每方法多次原子读」的规模失实：address.rs:48-88 五个谓词各自只做 2-3 次
   load，随后一律委托 AddressSnapshot::region_mutable / region_read_only / region_in_memory /
   region_on_disk / region_valid（:51/:59/:67/:75/:83），判定算式一处定义；snapshot()（:187-197）是
   构造/迁移面（AddressSnapshot 为 Copy 视图，with_snapshot :168 反建），不在读热路径上。
2. C# 原貌即逐字段易变读：AllocatorBase / HybridLog 的 HeadAddress / ReadOnlyAddress / TailAddress 在
   C# 侧是各自独立的 Volatile 读，单次判定同样跨字段取两次，不存在「跨字段一致快照」这一层；
   本仓 C# 树内无 LogicalAddresses 快照件（全仓 grep `LogicalAddresses`、`IsInMemory(` 零命中）。
3. 快照语义反而破坏正确性：热路径判定的目的就是取「此刻」的区界，把 7 个地址钉成一份局部快照再判，
   等价于把可变的 hlog 区界当不可变用，与 C# 的逐次现取口径相反，属引入 C# 没有的复杂度
   （spec：尽量 1:1 对标 C#，不要实现自己的优化）。
4. 若日后确有批量读循环复用区界的需求，落点是调用方（wkv 会话）而非 whlog 的原子地址组，
   本票的「压降总线原子读争用」没有实测依据。

----------------------------------------------------------------------

条 15 wkv store/keyspace.rs 528 行键空间扫描分配放大
原文观点：keyspace.rs 做 KEYS / SCAN / 多库遍历匹配时逐条记录分配 Vec<u8>，大键空间下堆分配与 GC 压力大。
rust：wedb/wkv/src/store/keyspace.rs fn WedbStore::scan_keys fn WedbStore::collect_matched_keys
对应 C#：libs/server/Storage/Functions/MainStore/ScanMethods.cs
动作：增加基于零拷贝切片借用的闭包迭代器 scan_keys_with。

拒绝理由
1. 票内 rust 符号全部不存在：wkv/src/store/keyspace.rs（528 行）实际内容是过期键删除扫描与
   FLUSHDB/FLUSHALL 换号（:63 expired_key_deletion_scan、:134 flush_database、:194
   flush_virtual_database、:245 flush_namespace、:292 flush_virtual_namespace、:368
   flush_all_databases、:421 keyspace_stats），既无 scan_keys 也无 collect_matched_keys。
   全仓 grep `scan_keys_with|fn scan_keys|collect_matched_keys` 零命中（rg 复核，非 zsh glob 假阴性）。
2. 票内 C# 文件不存在：libs/server/Storage/Functions/MainStore/ScanMethods.cs 无此文件；
   KEYS/SCAN 的真实 C# 落点是 libs/server/Resp/ArrayCommands.cs:221 NetworkKEYS、:253 NetworkSCAN。
3. C# 原版就是物化键列表，不做零拷贝：GarnetApi.cs:345 `public List<byte[]> GetDbKeys(PinnedSpanByte
   pattern)` 逐键分配 byte[]，RESP 层先用 keys.Count 写数组长度再 foreach 写 bulk string
   （ArrayCommands.cs:231-241）；rust 消费面
   wnode/src/storage/session/common/array_key_iteration_functions.rs:129 scan_cursor、:381 db_keys
   与之同形。加一套闭包式 scan_keys_with 属自研优化且要重排 C# 的「先报数后写体」协议形态，
   与「尽量 1:1 对标 C#，不要实现自己的优化」冲突。
4. spec 的零拷贝条款针对点查读路径（`*_with` / `*_callback` 消除点查 Vec<u8>），该面已在位
   （read.rs:791 read_raw_with、:813 read_tag_with、batch.rs:28 read_batch_with 等），与键空间列举无关。

----------------------------------------------------------------------

条 16 wkv read_cache 与主日志探测判据重叠
原文观点：read_cache 的 RcVisit 状态判定与 raw 会话的 ReadProbeResult 枚举逻辑形态重叠，
Promote 决策分支散落在 read.rs 中缺乏统一状态机。
rust：wedb/wkv/src/read_cache/mod.rs enum RcVisit；wedb/wkv/src/session/raw/mod.rs enum ReadProbeResult；
wedb/wkv/src/session/raw/read.rs fn StoreSession::promote_immutable_to_read_cache
对应 C#：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs fn FindInReadCache
动作：统一读缓存探测与主日志读链的结果映射，收敛 Promote 决策流到单点函数。

拒绝理由
1. C# 本就是两套互不相干的分类：ReadCache 环内访问（Implementation/ReadCache.cs +
   TryCopyToReadCache.cs）与主日志哈希链单遍分类（Implementation/InternalRead.cs + FindRecord.cs）。
   rust 的 RcVisit（read_cache/mod.rs:42-52，Found/Next/Gone 三态，Next 携前驱地址、Gone 表环形窗口
   滑出）对前者，ReadProbeResult（session/raw/mod.rs:29-37，Miss/Tombstone/Retry/Found 四态）对后者，
   两者成员集合与语义均不同，不是同一状态机的两份实现。
2. Promote 决策已单点：promote_immutable_to_read_cache 定义一处
   （read.rs:638），全仓生产调用仅同文件 :463、:604 两处，且两处都在同一条内存回退链
   （try_read_mem_fallback :508 起）的相邻分支上；不存在散落多处的决策分支需要「收敛」。
3. 动作要求「统一结果映射」必然新增一个跨两套语义的复合枚举/上下文对象，是加架构而非去重
   （spec：实现复杂度要对标 C#，而不是添加额外的复杂度）。

----------------------------------------------------------------------

条 17 wcompact 与 wkv store/gc.rs 驱动入口主从不分
原文观点：wcompact LogCompactor 暴露通用 compact 方法，wkv 生产紧缩必须遵循 VDB 映射过滤与 CPR 纪元
屏障，直接调用底层 compactor 会绕过上层保障。
动作：wcompact 裸 compact 方法明确标注为底层/测试专用，WedbStore::compact 设为唯一生产合法调用链。

拒绝理由
1. 动作本身是「加注释」，而 spec 明令禁止靠改注释过关（禁止简单的通过修改注释绕过检查）。
2. 事实层面主从已成立、无绕道：生产链一律经 wkv/src/compact.rs:266 WedbStore::compact →
   :273 compact_with_filter(&WedbCompactionFunctions)（VDB 过滤 + 过期判死单点），
   后台驱动 wkv/src/gc.rs:604 try_compact 亦走 WedbStore::compact（该文件 :12-13 注记同源）；
   `compactor()` 全仓 src 内仅 compact.rs:272/:291/:301 三处消费，无旁路。
3. LogCompactor::compact（wcompact/src/compactor/mod.rs:278）是 C# `TsavoriteKV.Compact` 默认无过滤
   形态的 1:1 对标件（mod.rs:5-6 已注锚点），消费方为其同文件 compact_lazy（:272）与
   wcompact/wkv 紧缩测试；C# 同样保留该无 CompactionFunctions 的重载入
   （libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs），删它或降级其可见性
   反而失去对标并打断测试面。

----------------------------------------------------------------------

条 21 wrecord header.rs 687 行与位段宏展开清理
原文观点：RecordHeader 16 字节头集中 RDH、RecordInfo、FillerWords、Tombstone、TtlValid 多组位段编解码，
内联展开致文件膨胀至 687 行。
动作：保持 RecordHeader 唯一定义不分化，将底层位运算常量和掩码提取为子模块 bits.rs。

拒绝理由
1. 动作前半句「保持唯一定义」当下已成立：wrecord/src/header.rs 内 RecordHeader 唯一定义，
   全仓无第二处 RecordHeader 分化（本票自检类主张，无需立项）。
2. 后半句「提取 bits.rs」无 C# 拓扑依据：C# 侧位段常量与掩码即写在结构体文件本体
   （libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs 364 行，含全部
   ValidBit/TombstoneBit 等常量与位运算；RecordDataHeader.cs 同型），把掩码搬去子模块是
   把 C# 的一个文件拆成 rust 的两个，属纯风格搬移，不减一行代码、不解任何重复。
3. 该文件承载的是记录头的持久化位布局（跨 wcol/wbftree/wkv 共用的单一事实源），掩码与字段同处
   一个 impl 更利于对照 RecordInfo.cs 校验位序；拆散后每次位序修订需跨文件比对，反增出错面。

----------------------------------------------------------------------

条 22 wtxn watch_version_map 批量版本推进支持
原文观点：watch_version_map 仅提供逐 key 单点递增原子版本，多键写（MSET、事务提交）时逐个原子更新引发
哈希表和缓存行抖动。
rust：wedb/wtxn/src/watch_version_map.rs fn WatchVersionMap::increment_version
对应 C#：libs/server/Transaction/TransactionManager.cs fn IncrementWatchVersion
动作：增加批量版本推进接口 increment_versions_batch，经局部预排或批量锁一次性推进多个键版本。

拒绝理由
1. C# 原版没有这一层：Garnet 全仓 WATCH 版本推进只有单键 `watchVersionMap.IncrementVersion(keyHash)`
   一种形态，且由各记录的写回钩子逐条调用（libs/server/Storage/Functions/ObjectStore/
   UpsertMethods.cs:48/:58/:68、RMWMethods.cs:79/:100/:125/:200、DeleteMethods.cs:21/:30、
   VectorStore/VectorSessionFunctions.cs:475 等），不存在批量入口，也没有「先预排再一次推进」的
   提交期批处理。票内锚点 libs/server/Transaction/TransactionManager.cs:IncrementWatchVersion 亦为
   逐键转调。
2. spec 明文钉死了单点逐键口径：「O(1) 墓碑逻辑秒删…WATCH 版本栅栏由 wtxn watch_version_map 单点推进，
   对标 C# watchVersionMap.IncrementVersion」。加 increment_versions_batch 即引入第二套版本推进路径，
   与「杜绝多套机制」取向冲突。
3. 抖动收益无取证：批量推进要先把多键哈希排序去重（又一套桶序），与 spec 的 1:1 对标取向相悖；
   若确有 MSET 提交放大问题，正解是在 MSET 慢路径复用既有单点推进而非新造 API 面。
