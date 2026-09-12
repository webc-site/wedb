[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wrecord : Record and Physical Key Encoding

- [Introduction](#introduction)
- [Module Layout](#module-layout)
- [Record Header Bit Layout (little-endian 16B)](#record-header-bit-layout-little-endian-16b)
- [Physical Key Encoding](#physical-key-encoding)
- [Compact Collection Encoding](#compact-collection-encoding)
- [Performance Design](#performance-design)
- [Test Coverage](#test-coverage)

## Introduction

wrecord is the record and physical-key encoding foundation: 16B HybridLog record headers with zero-copy record views, unified physical key encoding (namespace + tag + payload), compact inline encodings for hash / set / zset, and BfTree ordered-collection sub-keys with order-preserving f64 scores.

## Module Layout

- `header`: 16B `RecordHeader` with identical memory / disk layout (`repr(C)`, compile-time asserts pinning the 16B size and 8B alignment)
- `codec`: record-level encoding (`record_size` / `checked_record_size` / `encode_to_slice` / `try_encode_to_vec`)
- `record_ref` / `record_mut`: read-only zero-copy view and mutable in-place view (in-place updates, prev-pointer / tombstone rewrites)
- `tag`: 1-byte physical `KeyTag` (String=0x00 / Meta=0x01 / Hash=0x02 / Set=0x03 / ZSetChunk=0x04 / ZSetM2s=0x05 / ListChunk=0x06 / HashChunk=0x07 / SetChunk=0x08 / Ttl=0x09, with 0x02..=0x08 as flattened sub-keys) and `CollectionType` (Hash=1 / Set=2 / ZSet=3 / List=4 / RangeIndex=5)
- `bftag`: 1-byte BfTree block-level prefix `BfTag`; business ordered-data range 0..=31 (ZMember=0, ZScore=1) and system metadata range 32..=63 (NextNamespace=32, AclUser=33, AclMeta=34, ClusterMeta=35, ReplMeta=36)
- `ns_codec`: unified physical key codec `NamespaceDbCodec` and OPPV varints
- `meta`: 32B `MetaValue` collection metadata, 16B `CompactMetaValue`, the 17B flattened sub-key header, `StorageEncoding`
- `compact_hash` / `compact_set` / `compact_zset`: stateless codecs, owned containers, and zero-copy iterators for compact collections
- `zset`: BfTree zset sub-key encoding and order-preserving f64
- `chunk`: `ChunkCodec` (4B big-endian length prefix + payload)
- `glob` / `simd` / `sample` / `buf`: wildcard matching, SIMD key comparison, distinct random sampling, stack/heap dual-state buffer macro
- `error`: `Error` / `Result` (12 variants including BufferTooShort, AddressOverflow, NonCanonicalEncoding)

## Record Header Bit Layout (little-endian 16B)

`[0..8) prev_address`, `[8..12) key_len`, `[12..16) val_len`. prev_address compound fields: low 48 bits previous-version logical address (up to 256TB), bits 48..55 FillerWords (8B each), bits 56..58 FillerRem (0..7B), bit59 MODIFIED, bit60 SEALED, bit61 IN_NEW_VERSION, bit62 READ_CACHE, bit63 TOMBSTONE; 48+11+5 bits tile the 64-bit word exactly.

## Physical Key Encoding

- Layout: `[NsVarint] + [DbVarint] + [KeyTag 1B] + [Payload]`, big-endian lexicographic order-preserving; session prefix 2B minimum / 18B maximum, `STACK_KEY_CAP = 62` (one 64B cacheline); typical short keys never heap-allocate
- OPPV varints are self-delimiting with fixed-length first bytes (zero backtracking): 1B encodes 0..=127, 2B encodes 128..=16511, 3B encodes 16512..=2113663, 4B encodes 2113664..=270549119; values ≥ 270549120 use the 9B form (0xFF marker + 8B big-endian raw bits); a 9B encoding below the 4B bound is rejected as non-canonical. `MAX_VARINT_LEN = 9`
- Flattened sub-key: `[NsVarint][DbVarint][tag 1B][key_id 8B][version 8B][field]`, 19B minimum; in-collection sub-keys use a fixed 17B header; zset member / score sub-key headers are 17B / 25B (with 8B order-preserving score)

## Compact Collection Encoding

All begin with a 2B big-endian count prefix:

- hash: per entry `[field_len 2B][field][val_len 2B][value][expire_flag 1B]` (non-zero followed by an 8B big-endian absolute expiry timestamp)
- set: `[member_len 2B][member]`, lexicographically ordered in memory
- zset: per entry `[8B order-preserving score][member_len 2B][member]`, ordered by "score first, member lexicographic second" (same-score tie-break)

MetaValue 32B big-endian layout: `[key_id u64 0..8][type 1B 8][reserved 7B 9..16 (reserved[0] = StorageEncoding)][version u64 16..24][size u64 24..32]`; CompactMetaValue 16B: `[type 1B 0][encoding 1B 1][reserved 2B 2..4][size u32 4..8][expire_at_ms u64 8..16 (0 = never expires)]`.

Order-preserving f64: IEEE 754 negatives flip all 64 bits, positives flip only the sign bit; masks are generated with arithmetic right shift, branch-free; big-endian 8-byte comparison equals numeric order. BfTree zset key/value contract: member key `[0x00][key_id 8B][version 8B][member]` with Val = 8B big-endian f64 score; score key `[0x01][key_id 8B][version 8B][8B order-preserving score][member]` with an empty Val.

## Performance Design

- SIMD key comparison `fast_key_eq`: length pre-filter, then aarch64 NEON / x86_64 SSE2 vector compare; the tail reuses the last 16 bytes in overlap to eliminate remainder loops
- glob matching: non-recursive greedy state machine; the target pointer only moves forward (single pass over the target) while mismatch backtracking rescans only the pattern segment — O(N+M) typical, O(N·M) worst case; O(1) stack, zero heap allocation, no recursion overflow or ReDoS; semantics aligned with Garnet GlobUtils.Match
- Zero copy: `RecordRef` / `RecordMut` are compact wrappers over borrowed slices; session-prefix stripping is one SIMD prefix compare plus one tag byte, no varint decoding
- Sampling: N≤64 in a single u64 mask, N≤512 in an on-stack 8-word mask, N>512 with K≤64 via a stack array, Floyd's algorithm for K>64; output is strictly ascending and duplicate-free

## Test Coverage

tests/ covers: record roundtrip and tombstone bit, zero-copy Ref / in-place Mut, filler dynamic slack, header bit layout and lifecycle chains; OPPV roundtrip / boundaries / monotonicity / non-canonical defense; MetaValue layout and state transitions, SubKey encoding; order-preserving f64 with extreme floats, zset sub-key lexicographic contract; compact hash / set / zset CRUD, ordering and rank, streaming range iteration; unaligned SIMD compare, tag roundtrips, sampling properties.


---

<a name="zh"></a>

# wrecord : 记录与物理键编码

- [项目介绍](#项目介绍)
- [模块组成](#模块组成)
- [记录头位布局（小端 16B）](#记录头位布局小端-16b)
- [物理键编码](#物理键编码)
- [紧凑集合编码](#紧凑集合编码)
- [性能设计](#性能设计)
- [测试覆盖](#测试覆盖)

## 项目介绍

wrecord 是记录与物理键编码基础库：16B HybridLog 记录头与零拷贝记录视图、统一物理键编解码（命名空间 + 标签 + 载荷）、hash / set / zset 紧凑内联编码，以及 BfTree 有序集合子键与保序 f64 分值编码。

## 模块组成

- `header`：16B `RecordHeader`，内存 / 磁盘布局严格一致（`repr(C)`，16B 尺寸与 8B 对齐由编译期断言强制）
- `codec`：记录级编解码（`record_size` / `checked_record_size` / `encode_to_slice` / `try_encode_to_vec`）
- `record_ref` / `record_mut`：记录只读零拷贝视图与可变原位视图（可变区原位更新、前驱指针 / 墓碑原位改写）
- `tag`：物理键 1 字节 `KeyTag`（String=0x00 / Meta=0x01 / Hash=0x02 / Set=0x03 / ZSetChunk=0x04 / ZSetM2s=0x05 / ListChunk=0x06 / HashChunk=0x07 / SetChunk=0x08 / Ttl=0x09，0x02..=0x08 为打平子键）与 `CollectionType`（Hash=1 / Set=2 / ZSet=3 / List=4 / RangeIndex=5）
- `bftag`：BfTree 磁盘块级 1 字节前缀 `BfTag`，业务有序数据区 0..=31（ZMember=0、ZScore=1）与系统元数据区 32..=63（NextNamespace=32、AclUser=33、AclMeta=34、ClusterMeta=35、ReplMeta=36）
- `ns_codec`：统一物理键编解码器 `NamespaceDbCodec` 与 OPPV 变长整型
- `meta`：32B `MetaValue` 集合元数据、16B `CompactMetaValue`、17B 打平子键定长头、`StorageEncoding`
- `compact_hash` / `compact_set` / `compact_zset`：紧凑内联集合的无状态 Codec、所有权容器与零拷贝迭代器
- `zset`：BfTree zset 子键编解码与保序 f64
- `chunk`：`ChunkCodec`（4B 大端长度前缀 + 载荷）
- `glob` / `simd` / `sample` / `buf`：通配符匹配、SIMD 键比较、无重复随机抽样、栈堆双态缓冲宏
- `error`：`Error` / `Result`（BufferTooShort、AddressOverflow、NonCanonicalEncoding 等 12 类错误）

## 记录头位布局（小端 16B）

`[0..8) prev_address`、`[8..12) key_len`、`[12..16) val_len`。prev_address 复合位域：低 48 位前驱版本逻辑地址（上限 256TB）、bits 48..55 FillerWords（每词 8B）、bits 56..58 FillerRem（0..7B）、bit59 MODIFIED、bit60 SEALED、bit61 IN_NEW_VERSION、bit62 READ_CACHE、bit63 TOMBSTONE；48+11+5 位无缝铺满 64 位。

## 物理键编码

- 布局：`[NsVarint] + [DbVarint] + [KeyTag 1B] + [Payload]`，大端字典序保序；会话前缀最简 2B、最大 18B，`STACK_KEY_CAP = 62`（64B 单缓存行），典型短键全程零堆分配
- OPPV 变长整型自定界、首字节定长零回溯：1B 编码 0..=127、2B 编码 128..=16511、3B 编码 16512..=2113663、4B 编码 2113664..=270549119，≥270549120 用 9B（0xFF + 8B 大端原文）；9B 形式编码小于 4B 上界的值判为非规范。`MAX_VARINT_LEN = 9`
- 打平子键：`[NsVarint][DbVarint][tag 1B][key_id 8B][version 8B][field]`，最简 19B；集合内子键 17B 定长头；zset 成员 / 分值子键头分别 17B / 25B（含 8B 保序分值）

## 紧凑集合编码

均以 2B 大端 count 前缀开头：

- hash：每条目 `[field_len 2B][field][val_len 2B][value][expire_flag 1B]`（非 0 跟 8B 大端绝对过期时间戳）
- set：`[member_len 2B][member]`，内存字典序排列
- zset：每条目 `[8B 保序分值][member_len 2B][member]`，按"分值优先、成员字典序次之"排列（同分 tie-break）

MetaValue 32B 大端布局：`[key_id u64 0..8][type 1B 8][reserved 7B 9..16（reserved[0]=StorageEncoding）][version u64 16..24][size u64 24..32]`；CompactMetaValue 16B：`[type 1B 0][encoding 1B 1][reserved 2B 2..4][size u32 4..8][expire_at_ms u64 8..16（0=永不过期）]`。

保序 f64：IEEE 754 负数 64 位全取反、正数仅翻转符号位，算术右移生成掩码、无分支；大端 8 字节比较即数值序。BfTree zset 键值契约：成员键 `[0x00][key_id 8B][version 8B][member]`，Val=8B 大端 f64 分值；分值键 `[0x01][key_id 8B][version 8B][8B 保序分值][member]`，Val=空。

## 性能设计

- SIMD 键比较 `fast_key_eq`：长度预筛后 aarch64 NEON / x86_64 SSE2 向量比对，尾部借末 16B 重叠消除余数循环
- glob 匹配：非递归贪心状态机，目标指针单向推进（目标串单遍扫描），失配回溯仅重扫模式段，典型 O(N+M)、最坏 O(N·M)；O(1) 栈空间、零堆分配，无递归栈溢出与 ReDoS；语义对齐 Garnet GlobUtils.Match
- 零拷贝：`RecordRef` / `RecordMut` 紧凑包装切片借用；会话前缀剥离只做单次 SIMD 前缀比对 + 单字节标签解析，无需 varint 解码
- 抽样：N≤64 单 u64 位掩码、N≤512 栈上 8 字位掩码、N>512 且 K≤64 栈数组去重、K>64 Floyd 算法；输出严格升序且无重复

## 测试覆盖

tests/ 覆盖：记录 roundtrip 与墓碑位、零拷贝 Ref / 原位 Mut、filler 动态松弛、头位布局与生命周期链；OPPV roundtrip / 边界 / 单调性 / 非规范防御；MetaValue 布局与状态迁移、SubKey 编解码；保序 f64 极端浮点、zset 子键字典序契约；紧凑 hash / set / zset 增删改查、排序与 rank、范围流式迭代；SIMD 非对齐比对、标签 roundtrip、抽样性质。

