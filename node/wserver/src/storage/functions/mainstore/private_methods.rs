//! 主存函数私有辅助（对标 libs/server/Storage/Functions/MainStore/PrivateMethods.cs）
//!
//! C# 侧为 Tsavorite 回调内私有方法（值字节拷贝 / 过期就地裁决 / 数字原位
//! 更新 / etag 输出组装）；wkv 模型下以纯字节函数实现，供存储层复用。

use std::str;

use crate::storage::session::mainstore::bitmap_ops::normalize_range as clamp_range;

/// 值字节拷贝（追加到输出缓冲）
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:CopyTo
pub fn copy_to(dst: &mut Vec<u8>, value: &[u8]) {
  dst.extend_from_slice(value);
}

/// 值以 RESP 批量字符串编码追加
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:CopyRespTo
pub fn copy_resp_to(dst: &mut Vec<u8>, value: &[u8]) {
  dst.push(b'$');
  dst.extend_from_slice(itoa::Buffer::new().format(value.len()).as_bytes());
  dst.extend_from_slice(b"\r\n");
  copy_to(dst, value);
  dst.extend_from_slice(b"\r\n");
}

/// 带 input 上下文的 RESP 拷贝（input 非空时以 input 为输出体）
///
/// C# 侧用于 RMW 输出覆写；Rust 侧 input 直通输出。
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:CopyRespToWithInput
pub fn copy_resp_to_with_input(dst: &mut Vec<u8>, value: &[u8], input: Option<&[u8]>) {
  match input {
    Some(i) if !i.is_empty() => copy_resp_to(dst, i),
    _ => copy_resp_to(dst, value),
  }
}

/// 就地过期裁决（复用会话函数工具的三态语义）
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:EvaluateExpireInPlace
pub fn evaluate_expire_in_place(
  expiry_ms: Option<u64>,
  now_ms: u64,
) -> super::super::session_functions_utils::ExpireEval {
  super::super::session_functions_utils::SessionFunctionsUtils::evaluate_expire(expiry_ms, now_ms)
}

/// 拷贝更新路径的过期裁决（到期须拷贝更新以落墓碑）
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:EvaluateExpireCopyUpdate
pub fn evaluate_expire_copy_update(expiry_ms: Option<u64>, now_ms: u64) -> bool {
  evaluate_expire_in_place(expiry_ms, now_ms)
    == super::super::session_functions_utils::ExpireEval::Expired
}

/// Redis 区间参数归一化（负数自尾计数，空区间哨兵 (1, 0)）
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:NormalizeRange
pub fn normalize_range(start: i64, end: i64, total: i64) -> (i64, i64) {
  clamp_range(start, end, total)
}

/// 尝试原位数字更新：解析现值并按 delta 增减，返回新值文本
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:TryInPlaceUpdateNumber
pub fn try_in_place_update_number(current: &[u8], delta: i64) -> Option<String> {
  let v = str::from_utf8(current).ok()?.trim().parse::<i64>().ok()?;
  v.checked_add(delta).map(|n| n.to_string())
}

/// 尝试拷贝式数字更新（产出全新值字节）
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:TryCopyUpdateNumber
pub fn try_copy_update_number(current: Option<&[u8]>, delta: i64) -> Option<String> {
  match current {
    None => Some(delta.to_string()),
    Some(c) => try_in_place_update_number(c, delta),
  }
}

/// 是否为合法 i64 数字文本
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:IsValidNumber
pub fn is_valid_number(bytes: &[u8]) -> bool {
  str::from_utf8(bytes)
    .ok()
    .and_then(|s| s.trim().parse::<i64>().ok())
    .is_some()
}

/// 是否为合法有限双精度文本
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:IsValidDouble
pub fn is_valid_double(bytes: &[u8]) -> bool {
  str::from_utf8(bytes)
    .ok()
    .and_then(|s| s.trim().parse::<f64>().ok())
    .is_some_and(f64::is_finite)
}

/// 输出值长度（整数编码追加）
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:TryCopyValueLengthToOutput
pub fn try_copy_value_length_to_output(dst: &mut Vec<u8>, value: &[u8]) {
  dst.push(b':');
  dst.extend_from_slice(itoa::Buffer::new().format(value.len()).as_bytes());
  dst.extend_from_slice(b"\r\n");
}

/// 携带 etag 的 RESP 输出（值批量串 + 整数 etag 行）
///
/// 缺口说明：wkv 记录无独立 etag 元数据通道，etag 以值整数文本约定承载。
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:CopyRespWithEtagData
pub fn copy_resp_with_etag_data(dst: &mut Vec<u8>, value: &[u8], etag: u64) {
  copy_resp_to(dst, value);
  dst.push(b':');
  dst.extend_from_slice(itoa::Buffer::new().format(etag).as_bytes());
  dst.extend_from_slice(b"\r\n");
}

/// 值与 etag 二进制打包（8 字节长度 + 值 + 8 字节 etag 大端）
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:WriteValAndEtagToDst
pub fn write_val_and_etag_to_dst(dst: &mut Vec<u8>, value: &[u8], etag: u64) {
  dst.extend_from_slice(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
  dst.extend_from_slice(value);
  dst.extend_from_slice(&etag.to_be_bytes());
}

/// 解析 BITFIELD 类型/偏移参数（如 `u8`、`i16#3`、`u32 10`）
///
/// 返回 (有符号, 位宽, 偏移)。
///
/// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:GetBitFieldArguments
pub fn get_bit_field_arguments(spec: &[u8]) -> Option<(bool, u8, u64)> {
  let text = str::from_utf8(spec).ok()?;
  let (type_part, rest) = text.split_once(['#', ' '])?;
  let bits = type_part
    .strip_prefix(['u', 'i'])
    .and_then(|b| b.parse::<u8>().ok())?;
  let is_signed = type_part.starts_with('i');
  let offset = match rest.strip_prefix('#') {
    Some(idx) => u64::from(bits).checked_mul(idx.parse::<u64>().ok()?)?,
    None => rest.parse::<u64>().ok()?,
  };
  Some((is_signed, bits, offset))
}
