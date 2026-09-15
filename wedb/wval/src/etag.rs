//! ETag 记录定长数值编解码（8 字节大端 i64）
//!
//! 对标 C# Tsavorite LogRecord 的可选 ETag 字段语义
//! （libs/storage/Tsavorite/cs/src/core/Allocator/LogRecord.cs:ETagSize = 8、
//! NoETag = 0）：无 ETag 与 ETag 为 0 同义，条件比较一律以 0 为缺省基线。

/// ETag 载荷定长字节数（8 字节大端 i64，对标 LogRecord.cs:ETagSize）
pub const ETAG_VAL_LEN: usize = 8;

/// 无 ETag 哨兵值（对标 LogRecord.cs:NoETag：缺省 etag 视同 0）
pub const NO_ETAG: i64 = 0;

/// ETag 记录载荷快速编解码器
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EtagCodec;

impl EtagCodec {
  /// 将 etag 编码为 8 字节大端字节数组 (const fn, 零堆分配)
  #[inline(always)]
  pub const fn encode(etag: i64) -> [u8; ETAG_VAL_LEN] {
    etag.to_be_bytes()
  }

  /// 从字节切片中解码 etag (const fn)
  ///
  /// 若切片长度不足 8 字节返回 None
  #[inline(always)]
  pub const fn decode(bytes: &[u8]) -> Option<i64> {
    if let Some((arr, _)) = bytes.split_first_chunk::<ETAG_VAL_LEN>() {
      Some(i64::from_be_bytes(*arr))
    } else {
      None
    }
  }
}
