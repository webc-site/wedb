[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wcol : Collection Object Layer

- [Core Positioning](#core-positioning)
- [Module Layout](#module-layout)
- [Adaptive Tiering Thresholds](#adaptive-tiering-thresholds)
- [Core API](#core-api)

## Core Positioning

`wcol` is the collection object layer of WeDB: it hosts the object implementations of the five Redis collections (hash / list / set / zset / geo), their binary payload codecs, and the RESP input/output layer.

Collections live in a dual envelope + tiered form:

- Small collections stay in in-memory envelopes — the physical key carries `KeyTag::ObjectEnvelope` (an out-of-band type channel, mirroring the C# `LogRecord.DataHeader.ValueIsObject` bit), and the value is `[1B GarnetObjectType tag][count header][bitcode payload]`;
- Once a collection grows past the promote threshold it moves to a dedicated BfTree tiered tree, leaving only a 35B fixed-size stub in the main log;
- Below the low watermark it demotes back to the in-memory envelope. The promote / demote criteria have a single source in this crate (`should_promote` / `should_demote`); upper layers (wnode write path, wkv promotion orchestration) must not invent a second set of thresholds.

## Module Layout

Mirrors the `pub mod` list in `src/lib.rs` (10 modules):

| Module | Responsibility |
| :--- | :--- |
| `types` | The `IGarnetObject` trait and collection commons (member-level expiry queue `expiration_queue`, member TTL, normalization, scan input) |
| `hash` | `HashObject` / `HashOperation` hash object |
| `list` | `ListObject` / `ListOperation` list object |
| `set` | `SetObject` / `SetOperation` set object |
| `zset` | `SortedSetObject` / `SortedSetOperation` sorted-set object |
| `geo` | Geospatial indexing and computation (GEOADD / GEOHASH / GEODIST options and distance units) |
| `object_payload` | Binary payload codec helpers for collection objects (shared by AOF replay and sessions) |
| `resp` | Collection RESP protocol input/output layer (`ObjectOutput` / `RespInputFlags`) |
| `parse_utils` | Shared argument parsing utilities for the collection layer |
| `itembroker` | Item arbitration for blocking commands (BLPOP / BRPOP / BLMOVE / BLMPOP / BZPOPMIN / BZPOPMAX / BZMPOP): sessions register observers; on collection updates the available item is assigned to the head observer |

## Adaptive Tiering Thresholds

Two dimensions (entry count, heap memory) with high and low watermarks; constants live in `src/lib.rs`:

| Constant | Value | Meaning |
| :--- | :--- | :--- |
| `TIERED_PROMOTE_THRESHOLD` | 65,536 entries | Promote high watermark (entry-count dimension) |
| `TIERED_PROMOTE_BYTES` | 4 MB | Promote high watermark (memory dimension, via `IGarnetObject::heap_memory_size`) |
| `TIERED_DEMOTE_THRESHOLD` | 32,768 entries | Demote low watermark (entry-count dimension) |
| `TIERED_DEMOTE_BYTES` | 2 MB | Demote low watermark (memory dimension) |

- Promote `should_promote(count, heap_bytes)`: **OR** across both dimensions — fires when count ≥ 65,536 **or** memory ≥ 4MB;
- Demote `should_demote(count, heap_bytes)`: **AND** across both dimensions — falls back only when count ≤ 32,768 **and** memory ≤ 2MB;
- Between the watermarks (32,768–65,536 entries / 2–4MB) lies a hysteresis dead-band that keeps boundary collections from flapping between tiers;
- The memory dimension is calibrated against the rust-native accounting basis (single source in `wbase::heap`), deliberately not the .NET GC absolute value.

## Core API

- Object surface: `HashObject` / `ListObject` / `SetObject` / `SortedSetObject`, operation enums `HashOperation` / `ListOperation` / `SetOperation` / `SortedSetOperation`, the `IGarnetObject` trait (`count` / `heap_memory_size` / `should_promote` / `should_demote`), and the `ObjLoad` payload loader.
- RESP surface: `ObjectOutput` / `ObjectOutputFlags` / `RespInputFlags`.
- Tiering surface: `should_promote` / `should_demote` and the four `TIERED_*` constants above.
- Helper surface: `SET_MEMBER_DUMMY_VALUE` (the uniform dummy value for tree-stored set members).

---

<a name="zh"></a>

# wcol : 集合对象层

- [核心定位](#核心定位)
- [模块组成](#模块组成)
- [自适应分层门限](#自适应分层门限)
- [核心 API](#核心-api)

## 核心定位

`wcol` 是 WeDB 的集合对象层：承载 Redis 五类集合（hash / list / set / zset / geo）的对象实现、二进制载荷编解码与 RESP 输入输出。

集合存储采用「信封 + 分层」双形态：

- 小集合驻内存信封——物理键挂 `KeyTag::ObjectEnvelope`（带外类型通道，对标 C# `LogRecord.DataHeader.ValueIsObject` 位），值为 `[1B GarnetObjectType 标签][计数头][bitcode 载荷]`；
- 集合涨水越过升阶门限后转 BfTree 独立分层树，主日志仅存 35B 定长存根；
- 回落到低水位以下再降阶回内存信封。升 / 降阶判据单源在本 crate（`should_promote` / `should_demote`），上层（wnode 写路径、wkv 升阶编排）不得另设第二套门限。

## 模块组成

对照 `src/lib.rs` 的 `pub mod`（10 个）：

| 模块 | 职责 |
| :--- | :--- |
| `types` | `IGarnetObject` 公共 trait 与集合公共件（成员级过期队列 `expiration_queue`、成员 TTL、规范化、扫描输入） |
| `hash` | `HashObject` / `HashOperation` 哈希对象 |
| `list` | `ListObject` / `ListOperation` 列表对象 |
| `set` | `SetObject` / `SetOperation` 集合对象 |
| `zset` | `SortedSetObject` / `SortedSetOperation` 有序集合对象 |
| `geo` | 地理空间索引与计算（GEOADD / GEOHASH / GEODIST 等选项与距离单位） |
| `object_payload` | 集合对象二进制载荷编解码辅助（AOF 回放与会话共用） |
| `resp` | 集合 RESP 协议输入输出层（`ObjectOutput` / `RespInputFlags`） |
| `parse_utils` | 集合层共享参数解析工具 |
| `itembroker` | 阻塞命令（BLPOP / BRPOP / BLMOVE / BLMPOP / BZPOPMIN / BZPOPMAX / BZMPOP）的取件仲裁：会话注册观察者、集合更新时按队首指派可用项 |

## 自适应分层门限

双维度（条目数、堆内存）高低水位，常量定义于 `src/lib.rs`：

| 常量 | 值 | 含义 |
| :--- | :--- | :--- |
| `TIERED_PROMOTE_THRESHOLD` | 65,536 条 | 升阶高水位（条目数维） |
| `TIERED_PROMOTE_BYTES` | 4 MB | 升阶高水位（内存维，吃 `IGarnetObject::heap_memory_size`） |
| `TIERED_DEMOTE_THRESHOLD` | 32,768 条 | 降阶低水位（条目数维） |
| `TIERED_DEMOTE_BYTES` | 2 MB | 降阶低水位（内存维） |

- 升阶 `should_promote(count, heap_bytes)`：双维 **OR**——条目数 ≥ 65,536 **或** 内存 ≥ 4MB 即触发；
- 降阶 `should_demote(count, heap_bytes)`：双维 **AND**——条目数 ≤ 32,768 **且** 内存 ≤ 2MB 才回落；
- 高低水位之间（32,768–65,536 条 / 2–4MB）为迟滞死区，防止边界集合反复升降阶抖动；
- 内存维按 rust 自定记账口径标定（口径单点 `wbase::heap`），非 .NET GC 绝对值，刻意差异。

## 核心 API

- 对象面：`HashObject` / `ListObject` / `SetObject` / `SortedSetObject`、操作枚举 `HashOperation` / `ListOperation` / `SetOperation` / `SortedSetOperation`、公共 trait `IGarnetObject`（`count` / `heap_memory_size` / `should_promote` / `should_demote`）、`ObjLoad` 载荷装载。
- RESP 面：`ObjectOutput` / `ObjectOutputFlags` / `RespInputFlags`。
- 分层面：`should_promote` / `should_demote` 与上表 4 个 `TIERED_*` 常量。
- 辅助面：`SET_MEMBER_DUMMY_VALUE`（Set 成员树化存储的统一哑值）。
