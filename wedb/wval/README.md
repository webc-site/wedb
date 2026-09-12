# wval : Redis 值层编解码

wval 是 [WeDB](https://github.com/webc-site/wedb) 存储栈的值编码层，位于记录格式层 `wrecord` 之上，为混合日志提供带标签键方案与集合值编解码。依赖方向严格单向：`wval -> wrecord`；记录格式层绝不感知值层类型，对齐 Garnet 中 Tsavorite core 与 server 值对象层的分层约束。

## 能力

- 带标签键——`KeyTag`、`CollectionType`（`Hash` / `Set` / `ZSet` / `RangeIndex` 等）与 `StorageEncoding` 构成键标签体系；`NamespaceDbCodec` 基于 OPPV 变长整型编码多租户命名空间、数据库、键标签与子键，`SessionPrefixBuf` / `TaggedKeyBuf` 走零分配栈缓冲（`STACK_KEY_CAP`、`MAX_SESSION_PREFIX_LEN`）。
- 集合元数据与紧凑容器——`MetaValue` / `CompactMetaValue` 描述集合元数据；`CompactHash` / `CompactSet` / `CompactZSet` 编解码器与迭代器承载小规模集合的整体编码，支持字段级过期清除。
- zset 子键——`ZScoreKeyRef` / `ZMemberKeyRef` 配合保序 f64 编解码（`encode_order_preserving_f64` / `decode_order_preserving_f64`），打平子键可按字节序执行范围扫描。
- 工具面——`glob_match(_nocase)(_opt)` 提供 Redis 风格 glob 匹配；`TtlCodec` 编码字段级 TTL 值；`sample_distinct_indices` 无重复抽样；`RecordValueExt` / `RecordValueMutExt` 把记录视图桥接回值层解析。

## 用法

```rust
use wval::{glob_match, CompactHash, CompactHashCodec, NamespaceDbCodec};

// 编码命名空间 0、数据库 0 下的字符串键
let key = NamespaceDbCodec::encode_string_key(0, 0, b"user:1001");

// Redis 风格 glob 匹配
assert!(glob_match(b"h?llo", b"hello"));
assert!(glob_match(b"user:*", b"user:1001"));

// 紧凑 hash 编码与查读
let raw = CompactHashCodec::encode([(b"field" as &[u8], b"value" as &[u8], None)])?;
let hash = CompactHash::from_vec(raw)?;
assert_eq!(hash.find_field(b"field"), Some(&b"value"[..]));
```

## 集成

在 `wkv` 中承担会话前缀与子键编码，在 `wcompact` 中判定集合历史子键的紧缩去留。启用 `wbase` 的 `varint` / `glob` / `float` / `simd` / `buf` 特性获得底层编解码支撑。
