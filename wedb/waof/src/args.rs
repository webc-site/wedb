//! 参数序列区编解码单点（AOF 重放输入与存储过程条目共用布局）
//!
//! 布局 `[count u32][逐参 (4B len + bytes)]`，对标 C#
//! libs/server/Resp/Parser/SessionParseState.cs:SessionParseState 的
//! SerializeTo / DeserializeFrom（StringInput 与 CustomProcedureInput
//! 参数区共用该单点）；wresp::SessionParseState 持 unsafe 指针版，
//! 此处为安全切片版单点，AOF 重放链统一经此编解码。

/// 序列化参数序列到缓冲前部，返回写入字节数
///
/// 调用方保证 `out` 容量 ≥ `4 + Σ(4 + len)`（可用 [`arg_sequence_len`] 预算）
#[inline]
pub fn encode_arg_sequence<T: AsRef<[u8]>>(args: &[T], out: &mut [u8]) -> usize {
  let count = args.len() as u32;
  out[..4].copy_from_slice(&count.to_le_bytes());
  let mut cursor = 4;
  for arg in args {
    let slice = arg.as_ref();
    out[cursor..cursor + 4].copy_from_slice(&(slice.len() as u32).to_le_bytes());
    cursor += 4;
    out[cursor..cursor + slice.len()].copy_from_slice(slice);
    cursor += slice.len();
  }
  cursor
}

/// 参数序列区序列化字节数（`4 + Σ(4 + len)`）
#[inline]
pub fn arg_sequence_len(args: &[impl AsRef<[u8]>]) -> usize {
  4 + args.iter().map(|a| 4 + a.as_ref().len()).sum::<usize>()
}

/// 从参数序列区解码参数序列；越界 / 截断 / 计数超上限即 `None`（条目损坏）
///
/// 计数上限防御：count 超过 `剩余字节 / 4` 必为损坏条目，拒绝分配
pub fn decode_arg_sequence(mut bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
  let count = u32::from_le_bytes(*bytes.first_chunk::<4>()?) as usize;
  bytes = &bytes[4..];
  let mut args = Vec::with_capacity(count.min(bytes.len() / 4));
  for _ in 0..count {
    let len = u32::from_le_bytes(*bytes.first_chunk::<4>()?) as usize;
    bytes = bytes.get(4..)?;
    args.push(bytes.get(..len)?.to_vec());
    bytes = &bytes[len..];
  }
  Some(args)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn arg_sequence_roundtrip() {
    let args = vec![b"k".to_vec(), b"v".to_vec(), vec![]];
    let mut buf = vec![0u8; arg_sequence_len(&args)];
    let written = encode_arg_sequence(&args, &mut buf);
    assert_eq!(written, buf.len());
    assert_eq!(decode_arg_sequence(&buf).unwrap(), args);

    // 截断即 None
    assert!(decode_arg_sequence(&buf[..buf.len() - 1]).is_none());
    assert!(decode_arg_sequence(&[]).is_none());
  }
}
