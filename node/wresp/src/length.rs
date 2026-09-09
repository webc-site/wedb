//! RESP 长度前缀编解码 (对标 libs/common/RespLengthEncodingUtils.cs)
//!
//! DUMP/RESTORE 载荷使用的变长长度编码：
//! - 6 位：`00` 前缀 + 6 位长度 (≤ 63)，1 字节
//! - 14 位：`01` 前缀 + 14 位大端长度 (≤ 16 383)，2 字节
//! - 32 位：`10` 前缀 + 4 字节大端长度 (≤ 0xFFFFFF)，5 字节

/// 可编码的最大长度 (对标 RespLengthEncodingUtils.cs:MaxLength)
pub const MAX_LENGTH: u32 = 0xFF_FF_FF;

/// 尝试读取 RESP 编码长度 (对标 libs/common/RespLengthEncodingUtils.cs:TryReadLength)
///
/// 返回 `(长度, 消耗字节数)`；输入不足或前缀非法返回 `None`。
///
/// 与 C# 的差异：C# case 2 直接对含信号字节的 `input` 做
/// `TryReadInt32BigEndian`，读入位置 0..4 (信号字节混入长度值，且未保证
/// 输入 ≥ 5 字节)；此处改为跳过信号字节读 1..5，写读两端自洽
/// (与 [`try_write_length`] 往返一致)，输入不足即失败而非越界。
#[inline]
pub fn try_read_length(input: &[u8]) -> Option<(u32, usize)> {
  let first = *input.first()?;
  match first >> 6 {
    // 6 位：单字节即长度
    0 => Some((u32::from(first & 0x3F), 1)),
    // 14 位：信号字节低 6 位为长度高位，次字节为低位
    1 if input.len() > 1 => Some(((u32::from(first & 0x3F) << 8) | u32::from(input[1]), 2)),
    // 32 位：跳过信号字节，4 字节大端
    2 if input.len() >= 5 => {
      let len = u32::from_be_bytes(input[1..5].try_into().ok()?);
      Some((len, 5))
    }
    _ => None,
  }
}

/// 尝试写入 RESP 编码长度 (对标 libs/common/RespLengthEncodingUtils.cs:TryWriteLength)
///
/// 返回写入字节数；长度超 [`MAX_LENGTH`] 或输出缓冲不足返回 `None`。
#[inline]
pub fn try_write_length(length: u32, output: &mut [u8]) -> Option<usize> {
  if length > MAX_LENGTH {
    return None;
  }

  // 6 位编码 (length ≤ 63)
  if length < 1 << 6 {
    let byte = output.first_mut()?;
    *byte = length as u8 & 0x3F;
    return Some(1);
  }

  // 14 位编码 (64 ≤ length ≤ 16 383)
  if length < 1 << 14 {
    if output.len() < 2 {
      return None;
    }
    output[0] = (((length >> 8) & 0x3F) as u8) | (1 << 6);
    output[1] = length as u8;
    return Some(2);
  }

  // 32 位编码 (length ≤ 0xFFFFFF)
  if output.len() < 5 {
    return None;
  }
  output[0] = 2 << 6;
  output[1..5].copy_from_slice(&length.to_be_bytes());
  Some(5)
}

#[cfg(test)]
mod tests {
  use super::{MAX_LENGTH, try_read_length, try_write_length};

  #[test]
  fn roundtrip_all_buckets() {
    // 三个编码桶边界：0、63、64、16 383、16 384、MAX_LENGTH
    for len in [0u32, 63, 64, 0x3F_FF, 0x40_00, MAX_LENGTH] {
      let mut buf = [0u8; 5];
      let written = try_write_length(len, &mut buf).unwrap();
      let (decoded, read) = try_read_length(&buf[..written]).unwrap();
      assert_eq!((decoded, read), (len, written), "len={len}");
    }
  }

  #[test]
  fn encode_rejects_overflow_and_short_output() {
    let mut buf = [0u8; 5];
    // 超 MAX_LENGTH
    assert_eq!(try_write_length(MAX_LENGTH + 1, &mut buf), None);
    // 缓冲不足：14 位需要 2 字节
    assert_eq!(try_write_length(64, &mut buf[..1]), None);
    // 缓冲不足：32 位需要 5 字节
    assert_eq!(try_write_length(16_384, &mut buf[..4]), None);
  }

  #[test]
  fn decode_rejects_truncated_and_bad_prefix() {
    // 空输入
    assert_eq!(try_read_length(&[]), None);
    // 14 位缺次字节
    assert_eq!(try_read_length(&[1 << 6]), None);
    // 32 位缺长度字节
    assert_eq!(try_read_length(&[2 << 6, 0, 0]), None);
    // 11 前缀非法
    assert_eq!(try_read_length(&[0xC0]), None);
  }
}
