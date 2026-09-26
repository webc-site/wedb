//! 旁路记录值定长编解码单点（8 字节大端 i64）
//!
//! key 级 TTL 记录与 key 级 ETag 记录的记录值同为定长 8 字节大端 i64，
//! 编解码收敛本模块单一实现：
//! - TTL 记录值语义为绝对过期 .NET Ticks（100ns 单位，0001-01-01 纪元的
//!   `DateTimeOffset.UtcNow.UtcTicks`），对标 Garnet `RecordDataHeader` 的
//!   expiration 内联可选字段，与 C# 侧格式同域；Unix 秒/毫秒 ↔ ticks 的
//!   边界转换见 `wbase::convert`（garnet/libs/common/ConvertUtils.cs 镜像）；
//! - ETag 记录值语义为任意 i64 etag，对标 C# Tsavorite LogRecord 的可选
//!   ETag 字段（libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs:
//!   ETagSize = 8、NoETag = 0），无 ETag 与 ETag 为 0 同义（[`crate::NO_ETAG`]）。
//!
//! 不引入 bitcode：记录值须支持原位定长改写（EXPIRE / SETWITHETAG 反复推进
//! 的主路径），定长大端格式与 C# 记录内联字段布局同域。

/// 旁路记录值定长字节数（8 字节大端 i64）
pub const I64_VAL_LEN: usize = 8;

/// 旁路记录值快速编解码器（TTL ticks 与 ETag 共用单一实现）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct I64Codec;

impl I64Codec {
  /// 将 i64 编码为 8 字节大端字节数组 (const fn, 零堆分配)
  #[inline(always)]
  pub const fn encode(v: i64) -> [u8; I64_VAL_LEN] {
    v.to_be_bytes()
  }

  /// 从字节切片中解码 i64 (const fn)
  ///
  /// 若切片长度不足 8 字节返回 None
  #[inline(always)]
  pub const fn decode(bytes: &[u8]) -> Option<i64> {
    if let Some((arr, _)) = bytes.split_first_chunk::<I64_VAL_LEN>() {
      Some(i64::from_be_bytes(*arr))
    } else {
      None
    }
  }
}
