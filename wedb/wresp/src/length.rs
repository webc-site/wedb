//! RESP 长度前缀编解码 (对标 libs/common/RespLengthEncodingUtils.cs)
//!
//! DUMP/RESTORE 载荷使用的变长长度编码：
//! - 6 位：`00` 前缀 + 6 位长度 (≤ 63)，1 字节
//! - 14 位：`01` 前缀 + 14 位大端长度 (≤ 16 383)，2 字节
//! - 32 位：`10` 前缀 + 4 字节大端长度 (≤ 0xFFFFFF)，5 字节
//!
//! 在 garnet 中的相对路径: libs/common/RespLengthEncodingUtils.cs + test/standalone/Garnet.test/Resp/RespReadUtilsTests.cs（RESP 长度编解码）

/// 可编码的最大长度 (对标 RespLengthEncodingUtils.cs:MaxLength)
pub const MAX_LENGTH: u32 = 0xFF_FF_FF;

/// 尝试读取 RESP 编码长度 (对标 libs/common/RespLengthEncodingUtils.cs:TryReadLength)
///
/// 返回 `(长度, 消耗字节数)`；输入不足或前缀非法返回 `None`。
///
/// 与 C# 的差异：C# case 2 直接对含信号字节的 `input` 做
/// `TryReadInt32BigEndian`，读入位置 0..4 (信号字节混入长度值，且未保证
/// 输入 ≥ 5 字节)；此处改为跳过信号字节读 1..5，写读两端自洽
/// (与 [`try_write_length`] 往返一致)，输入不足即失败而非越界。登记见
/// doc/zh/deviations.md §114（宗 b）。
#[inline]
pub const fn try_read_length(input: &[u8]) -> Option<(u32, usize)> {
  if input.is_empty() {
    return None;
  }
  let first = input[0];
  match first >> 6 {
    // 6 位：单字节即长度
    0 => Some(((first & 0x3F) as u32, 1)),
    // 14 位：信号字节低 6 位为长度高位，次字节为低位
    1 if input.len() > 1 => Some(((((first & 0x3F) as u32) << 8) | (input[1] as u32), 2)),
    // 32 位：跳过信号字节，4 字节大端
    2 if input.len() >= 5 => {
      let len = u32::from_be_bytes([input[1], input[2], input[3], input[4]]);
      Some((len, 5))
    }
    _ => None,
  }
}

/// 尝试写入 RESP 编码长度 (对标 libs/common/RespLengthEncodingUtils.cs:TryWriteLength)
///
/// 返回写入字节数；长度超 [`MAX_LENGTH`] 或输出缓冲不足返回 `None`。
#[inline]
pub const fn try_write_length(length: u32, output: &mut [u8]) -> Option<usize> {
  if length > MAX_LENGTH {
    return None;
  }

  // 6 位编码 (length ≤ 63)
  if length < 1 << 6 {
    if output.is_empty() {
      return None;
    }
    output[0] = (length as u8) & 0x3F;
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
  let bytes = length.to_be_bytes();
  output[1] = bytes[0];
  output[2] = bytes[1];
  output[3] = bytes[2];
  output[4] = bytes[3];
  Some(5)
}
