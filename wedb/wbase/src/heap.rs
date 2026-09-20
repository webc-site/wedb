//! 集合对象堆内存记账常量（对照 garnet `libs/server/Objects/*` 的 `HeapMemorySize`）
//!
//! 全仓 `heap_memory_size` 加减运算的唯一具名口径（杜绝跨四对象文件散落的裸魔数 16）。
//!
//! C# 侧 [`MemoryUtils`] 为 .NET 托管堆逐对象开销记账：`ByteArrayOverhead=24`、
//! `ListOverhead=40`、`ListEntryOverhead=48`、`SortedSetOverhead=48`、
//! `SortedSetEntryOverhead=48`、`DictionaryOverhead=80`、`DictionaryEntryOverhead=64`、
//! `HashSetOverhead=64`、`HashSetEntryOverhead=40`、`PriorityQueueOverhead=80`、
//! `PriorityQueueEntryOverhead=48`，另以 `IntPtr.Size + sizeof(long)` 计过期项。这些是
//! CLR 对象头 + 散列桶数组的平均开销，**不适用 Rust**：`Vec<u8>` / `HashMap` / `BTreeSet`
//! / `VecDeque` 的占用由标准库布局决定，无逐对象 GC 头。
//!
//! 故 Rust 侧不逐字镜像上述值，改用「容器常驻基线 + 每条目相对槽位」的自定口径：条目数据
//! 按 [`round_up_ptr`] 实计，任何「结构槽 / 句柄」一律按 [`SLOT`]（16 字节 ≈ 一指针 + 一
//! `long`）计。C# 区分 ByteArray/Dictionary/Set/List/PQ 各 `EntryOverhead` 只因 .NET GC
//! 开销各异，Rust 口径下它们塌缩为同一具名单元，故此处**一处定义**、按需取整数倍，而非逐
//! 类型造出值相同的名。`heap_memory_size` 绝对值与 C# 不等是刻意为之；客户端 `MEMORY USAGE`
//! 与自适应分层体积维（`wcol::TIERED_PROMOTE_BYTES` / `wcol::TIERED_DEMOTE_BYTES`）均按本口径标定。
//!
//! C# 对位为常量表 `MemoryUtils`（Tsavorite core Utilities 下）与 `libs/server/Objects` 各对象
//! 的条目记账方法（UpdateSize 一类）；rust 侧这些方法的规范映射注释写在各对象实现文件
//! （wcol hash/set/list/zset），本模块仅收口其共享常量口径，不重复登记函数映射。

/// 指针宽度（字节）：条目数据长度按此向上取整实计，对位 C# `Utility.RoundUp(len, IntPtr.Size)`。
pub const PTR_SIZE: usize = 8;

/// 单个结构槽 / 句柄的相对记账单位（≈ 一指针 + 一 `long` = 16 字节）。全仓条目/过期项的
/// 非数据开销均为 `SLOT` 的整数倍；对位 C# 各 `*EntryOverhead` 与 `IntPtr.Size + sizeof(long)`
/// 项的「每条目槽位」语义，但取值不等（.NET 逐对象 GC 头不适用 Rust，见模块文档）。
pub const SLOT: i64 = 16;

/// 单个容器（`HashMap` / `HashSet` / `BTreeSet` / `VecDeque`）的常驻基线（两槽：桶数组指针 +
/// 计数/容量句柄）。空集合亦计入，对位 C# 各对象构造 `base(*Overhead)`
/// （`DictionaryOverhead` / `HashSetOverhead` / `ListOverhead` / `SortedSetOverhead`），
/// 值不等（见模块文档）。
pub const CONTAINER_BASE: i64 = SLOT * 2;

/// 成员级过期结构整体常驻基线：随惰性初始化计一次、整体回收退一次。
/// 惰性创建时同时建「过期字典 + 最小堆」两个容器（对位 C#
/// `InitializeExpirationStructures` 的 `DictionaryOverhead + PriorityQueueOverhead`），
/// 故取两容器基线之和，值不等（见模块文档）。
pub const EXPIRY_STRUCT_BASE: i64 = CONTAINER_BASE * 2;

/// `RoundUp(len, PTR_SIZE)`：条目数据按指针宽度向上取整实计。
#[inline(always)]
pub const fn round_up_ptr(len: usize) -> usize {
  len.div_ceil(PTR_SIZE) * PTR_SIZE
}
