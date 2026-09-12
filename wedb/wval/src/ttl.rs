//! TTL 记录定长时间戳编解码（8 字节大端 i64 .NET Ticks）
//!
//! 对标 Garnet `RecordDataHeader` 的 expiration 语义：绝对过期时间一律以
//! .NET Ticks（100ns 单位，0001-01-01 纪元的 `DateTimeOffset.UtcNow.UtcTicks`）存储，
//! 与 C# 侧格式同域；Unix 秒/毫秒 ↔ ticks 的边界转换见 `wbase::convert`
//! （garnet/libs/common/ConvertUtils.cs 镜像）。

/// TTL 载荷定长字节数（8 字节大端 i64 .NET Ticks，无需额外容器包装）
pub const TTL_VAL_LEN: usize = 8;

/// TTL 记录载荷快速编解码器
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TtlCodec;

impl TtlCodec {
  /// 将绝对过期 .NET Ticks 编码为 8 字节大端字节数组 (const fn, 零堆分配)
  #[inline(always)]
  pub const fn encode(expire_at_ticks: i64) -> [u8; TTL_VAL_LEN] {
    expire_at_ticks.to_be_bytes()
  }

  /// 从字节切片中解码绝对过期 .NET Ticks (const fn)
  ///
  /// 若切片长度不足 8 字节返回 None
  #[inline(always)]
  pub const fn decode(bytes: &[u8]) -> Option<i64> {
    if let Some((arr, _)) = bytes.split_first_chunk::<TTL_VAL_LEN>() {
      Some(i64::from_be_bytes(*arr))
    } else {
      None
    }
  }
}
