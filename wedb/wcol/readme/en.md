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
