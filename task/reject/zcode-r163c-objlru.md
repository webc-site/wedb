拒绝结论：判净（核对 C# Garnet UnifiedSessionFunctions.HandleObjectIdleTime/HandleObjectFreq，Garnet 基于 Tsavorite 混合日志模型无 per-key LRU/LFU 时钟，IDLETIME 固回 0，FREQ 固回不支持错误帧，REFCOUNT 固回 1，wedb 读写路径与协议帧与 C# 逐字节全等，无缺陷无分叉）

OBJECT FREQ与IDLETIME写侧LRU采样面审查报告

一、审查视角与背景说明
审查视角：OBJECT FREQ/IDLETIME 写侧 LRU 采样面 (划界 objenc/objfam 判净立案)。
核查目标：
1. 核查 wedb 中 OBJECT FREQ、OBJECT IDLETIME、OBJECT ENCODING、OBJECT REFCOUNT、OBJECT HELP 命令实现。
2. 核查读写路径上是否存在 LRU/LFU 时钟更新、访问戳采样或淘汰策略，对标 Garnet 原型行为。
3. 确证 Garnet 原型是否支持 LRU/LFU 时钟，wedb 现有实现如何响应（返回 0、错误或伪造时钟），排查协议帧、内存泄漏与架构偏差。
4. 划界既有已立案与在册议题（objenc 编码与入账族、objfam 异构判死漏斗族），核验 doc/zh/deviations.md，严禁将原型既定行为与架构设计误判为缺陷。

二、原型行为与对标核查（C# Garnet 事实确证）

1. OBJECT 各子命令原型契约
c# 对应文件与函数：
garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:UnifiedSessionFunctions.Reader
garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:UnifiedSessionFunctions.HandleObjectEncoding
garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:UnifiedSessionFunctions.HandleObjectRefCount
garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:UnifiedSessionFunctions.HandleObjectIdleTime
garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:UnifiedSessionFunctions.HandleObjectFreq
garnet/libs/server/Resp/BasicCommands.cs:BasicCommands.NetworkOBJECT
garnet/libs/server/Resp/BasicCommands.cs:BasicCommands.NetworkOBJECTHELP
garnet/libs/server/Resp/CmdStrings.cs:CmdStrings.RESP_ERR_OBJECT_FREQ_UNSUPPORTED
garnet/libs/server/Resp/RespServerSession.cs:RespServerSession.ProcessMessageInternal

核查确证事实：
1) OBJECT IDLETIME <key>：
在 HandleObjectIdleTime 中硬编码写入整型 0（writer.WriteInt64(0)）。
官方注释明文确认：Garnet does not track per-key LRU idle time。
2) OBJECT FREQ <key>：
在 HandleObjectFreq 中硬编码返回错误帧（writer.WriteError(CmdStrings.RESP_ERR_OBJECT_FREQ_UNSUPPORTED)）。
错误文案逐字固定为：ERR OBJECT FREQ is not supported: Garnet does not track access frequency (no LFU maxmemory policy)。
官方注释明文确认：Garnet does not implement an LFU maxmemory policy, so access frequency is not tracked。
3) OBJECT REFCOUNT <key>：
在 HandleObjectRefCount 中硬编码写入整型 1（writer.WriteInt64(1)）。
官方注释明文确认：Garnet does not share value objects, so the reference count is always 1。
4) OBJECT ENCODING <key>：
在 HandleObjectEncoding 中判定，ValueIsObject 为 true 时依对象实际类型返回 skiplist（SortedSet）、quicklist（List）、hashtable（Set/Hash 及其他扩展对象）；ValueIsObject 为 false 时统一返回 raw。
5) 缺失键响应：
在 NetworkOBJECT 中，storageApi.OBJECT 返回状态非 OK（如键缺失或已过期淘汰）时，统一调用 WriteNull() 返回 nil 帧（RESP2 对应 $-1\r\n，RESP3 对应 _\r\n），该行为在全部子命令下完全一致。
6) OBJECT HELP：
在 NetworkOBJECTHELP 中固定返回 11 行简要说明文本，明载 FREQ 与 IDLETIME 的非追踪特性。

2. 原型读写路径时钟更新与采样
c# 对应文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:RecordInfo
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/RecordDataHeader.cs:RecordDataHeader
garnet/libs/server/Storage/Session/UnifiedStore/AdvancedOps.cs:StorageSession.Read_UnifiedStore
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:MainSessionFunctions.ConcurrentWriter
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:MainSessionFunctions.InPlaceUpdater

核查确证事实：
Garnet 基于 Tsavorite HybridLog 混合日志存储架构，内存管理采用基于逻辑地址分代、纪元保护（Epoch）与页环推进机制，并不存在 Redis 原生的 maxmemory-policy（如 volatile-lru、allkeys-lru、volatile-lfu、allkeys-lfu）。
日志记录头（RecordInfo 与 RecordDataHeader）中仅包含前驱版本地址 PreviousAddress、Tombstone 墓碑位、Checkpoint 标志及键值长度位段，全仓零 LRU 时钟位段、零 LFU 计数器位段。
写路径（Upsert / RMW / ConcurrentWriter）与读路径（Read / SingleReader / ConcurrentReader）均未维护或采样任何时钟或访问频次信息。

三、工程现状确证（Rust wedb 实现核查）

1. 快路径与慢路径实现
rust 文件与函数：
wedb/wnode/src/resp/basic_commands/mod.rs:RespServerSession::network_object
wedb/wnode/src/resp/basic_commands/mod.rs:RespServerSession::write_object_subcmd_reply
wedb/wnode/src/resp/basic_commands/mod.rs:RespServerSession::network_objecthelp
wedb/wnode/src/resp/basic_commands/slow.rs:object_slow
wedb/wnode/src/resp/basic_commands/slow.rs:object_frame
wedb/wnode/src/resp/basic_commands/slow.rs:encoding_of_object_type
wedb/wnode/src/resp/basic_commands/slow.rs:encoding_of_envelope_payload
wedb/wnode/src/resp/garnet_api/raw.rs:exec
wedb/wnode/src/resp/garnet_api/slow.rs:string_slow
wedb/wresp/src/cmd_strings.rs:RESP_ERR_OBJECT_FREQ_UNSUPPORTED

核查确证事实：
1) 参数元数检查：
network_object 经 unpack_args 严格校验 parse_state 参数个数恰为 1，报错文案对标 C# 的 subCommandName（object|encoding, object|freq, object|idletime, object|refcount）。
2) 向量键特判：
遇到 VectorManager 存储登记的向量键，快慢两路统一映射为 raw 编码，回显形态与普通键严格同构。
3) 同步读与静默入账：
快路径经 read_user_sync 探查主存，传入 metrics=None；慢路径经 read_user_quiet 与 read_tag_quiet 续探对象信封与 Meta 升阶元数据。两路均彻底避免读指标虚标，与 C# Read_UnifiedStore 零入账完全一致。
4) 编码与应答分发：
write_object_subcmd_reply 与 slow 侧 object_frame 实现完全镜像：
- Encoding 回 bulk 编码串（String 与 RangeIndex 回 raw，SortedSet 回 skiplist，List 回 quicklist，Hash/Set 回 hashtable）。
- Refcount 恒回 :1\r\n。
- Idletime 恒回 :0\r\n。
- Freq 恒回 -ERR OBJECT FREQ is not supported: Garnet does not track access frequency (no LFU maxmemory policy).\r\n。
- 缺失键或过期键统一由 write_resp_null_ver 输出 nil 帧。
5) 降级与异步闭环：
若同步探针检测到记录冷态（Deferred），在非到期状态下平滑降级到 SlowWait 慢路径 object_slow，慢路径回读落盘数据后执行相同判定，快慢双路应答逐字节全等。

2. 底层存储与写路径确证
rust 文件与函数：
wedb/wrecord/src/header.rs:RecordHeader
wedb/wkv/src/session/raw/write/mod.rs:BatchStoreSession::put_sync
wedb/wkv/src/session/raw/write/rmw.rs:BatchStoreSession::try_rmw_sync

核查确证事实：
wedb 底层记录头 RecordHeader 严格对齐 Tsavorite 规范，由 8 字节 prev_address 与 8 字节 rdh_word 构成，不包含任何 LRU/LFU 时钟字段。
写路径（put_sync, try_rmw_sync, append）未注入任何伪造时钟或私有时钟更新逻辑，忠实遵循原型 HybridLog 纯物理日志模型。

四、划界对比与排查结论

1. 与 objenc 议题划界
既有工单 zcode-r114-objenc1 与 zcode-r157c-objenc 聚焦于：
- 编码映射全面性（扩展标签 hashtable 兜底、RI 显式 raw）。
- 慢路径降级指标泄漏（消除 read_tag_with 虚假 found/notfound 记账）。
本次核查确认：写侧与读侧不存在遗留的 LRU 采样面，objenc 的改动未对 LRU/LFU 行为产生任何副作用，四子命令在指标与数据面上均已完全闭环。

2. 与 objfam 议题划界
既有工单 zcode-r131c-objfam（驳回）与 deviations.md 第 150 条聚焦于：
- 集合算术族装载漏斗中异构判死记录（string 影子记录、RI 记录）与过期门顺序。
本次核查确认：OBJECT 命令本身走的是统一元数据读漏斗（String 域 -> Envelope 域 -> Meta 域），过期键统一在漏斗内经 TTL 门裁决为缺失并返回 nil，不涉及集合算术与成员级装载，与 objfam 关注的 Reader 判型/判死分叉场景完全正交，互不干扰。

3. 架构纯洁度与合规性评估
- 无虚假时钟：wedb 未尝试自研不兼容的内存态 LRU 伪时钟，避免了破坏与 Garnet 1:1 对齐契约。
- 无协议畸变：错误文案、整型值及空值帧均严格对标 C# Garnet 官方测试用例（RespObjectCommandTests.cs）。
- 无内存与资源泄漏：入参提取均基于切片借用，应答写出复用现有缓冲，无多余堆分配。
- 偏离在册确证：doc/zh/deviations.md 无需增补本项，因其系与 C# 原型 1:1 逐字全等行为，无刻意分歧。

五、结论总结
OBJECT FREQ 与 OBJECT IDLETIME 的写侧与读侧实现已全面核验，wedb 严格对标 Garnet 原型行为（IDLETIME 恒回 0，FREQ 恒报错），写侧与读侧均无未登记的 LRU 采样逻辑，快慢双臂同构全等，指标记账精准闭环，与 objenc/objfam 边界清晰，无可立案缺陷。

视角结论:已穷尽
