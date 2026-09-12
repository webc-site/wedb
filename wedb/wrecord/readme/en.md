# wrecord : Record and Physical Key Encoding

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
