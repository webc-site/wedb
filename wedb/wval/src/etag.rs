//! key 级 ETag 记录语义常量
//!
//! 对标 C# Tsavorite LogRecord 的可选 ETag 字段语义
//! （libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs:ETagSize = 8、
//! NoETag = 0）：无 ETag 与 ETag 为 0 同义，条件比较一律以 0 为缺省基线。
//! 记录值 8 字节大端 i64 编解码单点见 [`crate::codec::I64Codec`]。

/// 无 ETag 哨兵值（对标 LogRecord.cs:NoETag：缺省 etag 视同 0）
pub const NO_ETAG: i64 = 0;
