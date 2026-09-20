object-heap-overhead-constants

问题
集合对象 heap_memory_size 记账存在三处一致性问题：一是 hash/set/list/zset 四对象文件
（含 hash_object_impl / sorted_set_object 分片）散落十余处裸魔数 16，零具名常量；二是
hash_object.rs 与 sorted_set_object.rs 的 initialize/cleanup 位点行内注释写「C#:
DictionaryOverhead + PriorityQueueOverhead」却取值 16（C# 实为 80+80=160），属假注释；
三是模块头「刻意差异」声明仅在 hash 与 zset 各存一份（两份并立即重复机制），set 指向
hash 头、list 完全缺失，覆盖不一致。该字段直接驱动客户端可见的 MEMORY USAGE 与自适应
分层体积维（wcol::TIERED_PROMOTE_BYTES / TIERED_DEMOTE_BYTES 的 should_promote/
should_demote）。

判定：成立，取处置口径 A（rust 自定口径收口）。C# 证据核对：
MemoryUtils.cs:14/17/20/23/26/29/32/35/38/41/44 常量表真实存在（ByteArrayOverhead=24、
DictionaryOverhead=80、DictionaryEntryOverhead=64、HashSetOverhead=64、
ListOverhead=40、ListEntryOverhead=48、SortedSetOverhead=48、SortedSetEntryOverhead=48、
PriorityQueueOverhead=80、PriorityQueueEntryOverhead=48），非臆造；HashObject.cs:98 与
SetObject.cs:56 / ListObject.cs:70 / SortedSetObject.cs:170 构造确有 base(*Overhead)
常驻基线；HashObject.cs:317 / :348、SetObject.cs UpdateSize、ListObject.cs:203、
SortedSetObject.cs:660 均有 Debug.Assert(HeapMemorySize >= *Overhead) 不变式。rust 侧
既有「不照搬 .NET GC 开销」的刻意差异声明与 SKILL「无向下兼容、对标口径可自定」一致，
故不逐字镜像 C# 数值（口径 B 会让 MEMORY USAGE 谎报 .NET 数字），而是收口为 rust 自定
具名单元并补齐常驻基线 + debug 不变式（口径 A）。

C# 对位
libs/storage/Tsavorite/cs/src/core/Utilities/MemoryUtils.cs（常量表）
libs/server/Objects/Hash/HashObject.cs:UpdateSize / InitializeExpirationStructures /
  UpdateExpirationSize / CleanupExpirationStructuresIfEmpty / SetExpiration / Persist
libs/server/Objects/Set/SetObject.cs:UpdateSize
libs/server/Objects/List/ListObject.cs:UpdateSize
libs/server/Objects/SortedSet/SortedSetObject.cs:UpdateSize / Initialize / Update / Cleanup

rust 现状与改造
新增 wbase/src/heap.rs（无条件 pub mod，纯常量零依赖，无需 Cargo.toml 改动）作为全仓
heap_memory_size 记账唯一具名单点：
  PTR_SIZE=8（RoundUp 量化，对位 IntPtr.Size）、SLOT=16（每结构槽/句柄相对开销，
  塌缩 C# 各 *EntryOverhead 与 IntPtr.Size+sizeof(long) 为同一单元）、
  CONTAINER_BASE=SLOT*2（单容器常驻基线）、EXPIRY_STRUCT_BASE=CONTAINER_BASE*2（惰性
  过期字典+堆两容器常驻）、round_up_ptr(len)。模块文档为「刻意差异」唯一权威声明。
四对象文件：删除并立的模块头声明，改为指向 wbase::heap 的一行引用（set/list 也补齐）；
所有 heap_memory_size 加减（含 hash_object_impl 的 replace_value 与 HSET 覆写清过期、
zset initialize/cleanup/delete_expired/set_expiration）全部经具名常量，假注释
「C#: DictionaryOverhead + PriorityQueueOverhead」删除；数值与原裸魔数逐位相等（SLOT*2
=16+16、SLOT*3=16+16+16），MEMORY USAGE 相对次序与分层阈值行为不变。
常驻基线：四对象由 derive(Default) 改手写 Default，构造即带
CONTAINER_BASE（hash/set/list 单容器）、CONTAINER_BASE*2（zset 有序视图+散列双容器），
对位 C# 构造 base(*Overhead)，空集合 heap_memory_size 非零。
debug 不变式：四对象 update_size / update_expiration_size / cleanup 的回收臂补
debug_assert!(heap_memory_size >= 对应基线)，对位 C# 三处 Debug.Assert。
分层阈值：4MB/2MB 与基线（32/64 字节）量级悬殊，判定不变，在 wcol::lib.rs 常量注释
写明按 rust 记账口径（wbase::heap）标定。

上下游链路
wcol::GarnetObject::heap_memory_size / should_promote / should_demote、
object_payload.rs heap_estimate、wnode MEMORY USAGE（basic_commands/mod.rs 与
object_store_utils envelope_heap_estimate，其文档已声明 C# GC 口径不适用）均读该字段，
口径单点收敛后行为不回退。

涉及冲突域与串行
与 next/hexpire-denied-arm-declaration.md 同改 hash/zset：那条只补 set_expiration 拒绝臂
注释与测试，本条先把常量与声明落地为唯一口径，后续那条直接引用，不再并立。
与 next/tiered-background-demote.md 共用 should_demote：本条数值不变，其预筛命中集不受影响。

验收
wcol 四对象（含分片）heap_memory_size 加减零裸魔数、假注释零命中、刻意差异声明全仓一份
（wbase::heap）；cargo check --workspace 零新增警告；空集合 heap_memory_size 非零基线，
debug 回收不变式成立。

来源清理
删除独立文件 next/object-heap-overhead-constants.md（其「来源：next/qcode.data-2.md 条 2」
指向的聚合文件已不在仓内，无需再删条）。
