use crate::resp::RespValue;

/// IEEE 754 双精度浮点数最大连续安全整数 (2^53 = 9_007_199_254_740_992)
pub const MAX_SAFE_INTEGER: f64 = (1_i64 << f64::MANTISSA_DIGITS) as f64;
pub const MIN_SAFE_INTEGER: f64 = -MAX_SAFE_INTEGER;

/// 将 64 位浮点数高性能格式化为字节切片（零堆分配，支持 inf/-inf/nan，符合 Redis Float2String 规范）
#[inline]
pub fn format_float_bytes(val: f64, buf: &mut zmij::Buffer) -> &[u8] {
  if val.is_infinite() {
    if val.is_sign_positive() {
      b"inf"
    } else {
      b"-inf"
    }
  } else if val.is_nan() {
    b"nan"
  } else if val == 0.0 {
    b"0"
  } else {
    let s = buf.format_finite(val);
    if let Some((int_part, frac_part)) = s.split_once('.') {
      if !frac_part.is_empty() && !frac_part.contains('e') && !frac_part.contains('E') {
        let trimmed = frac_part.trim_end_matches('0');
        if trimmed.is_empty() {
          int_part.as_bytes()
        } else {
          let total_len = int_part.len() + 1 + trimmed.len();
          &s.as_bytes()[..total_len]
        }
      } else {
        s.as_bytes()
      }
    } else {
      s.as_bytes()
    }
  }
}

/// 将 Option<Vec<u8>> 转为 RespValue::Blob 或 RespValue::Null
#[inline]
pub fn blob_or_null(val: Option<Vec<u8>>) -> RespValue {
  match val {
    Some(v) => RespValue::Blob(v),
    None => RespValue::Null,
  }
}

/// 将 Option<String> 转为 RespValue::Blob 或 RespValue::Null
#[inline]
pub fn blob_str_or_null(val: Option<impl AsRef<str>>) -> RespValue {
  match val {
    Some(s) => RespValue::Blob(s.as_ref().as_bytes().to_vec()),
    None => RespValue::Null,
  }
}

/// 将 bool 转为 RespValue::Int(1) 或 RespValue::Int(0)
#[inline]
pub fn bool_to_int(val: bool) -> RespValue {
  RespValue::Int(if val { 1 } else { 0 })
}

/// 基于 Redis 规范高性能浮点格式化生成 RespValue::Blob（零堆分配）
#[inline]
pub fn float_to_blob(f: f64) -> RespValue {
  if f.is_infinite() {
    if f.is_sign_positive() {
      RespValue::Blob(b"inf".to_vec())
    } else {
      RespValue::Blob(b"-inf".to_vec())
    }
  } else if f.is_nan() {
    RespValue::Blob(b"nan".to_vec())
  } else if f == 0.0 {
    RespValue::Blob(b"0".to_vec())
  } else if f.trunc() == f && (MIN_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&f) {
    let mut buf = itoa::Buffer::new();
    RespValue::Blob(buf.format(f as i64).as_bytes().to_vec())
  } else {
    let mut buf = zmij::Buffer::new();
    RespValue::Blob(format_float_bytes(f, &mut buf).to_vec())
  }
}

/// 将 Score 格式化为 RespValue::Blob
#[inline]
pub fn score_to_blob(score: f64) -> RespValue {
  float_to_blob(score)
}

/// 将 Option<f64> 转为 RespValue::Blob 或 RespValue::Null
#[inline]
pub fn float_or_null(val: Option<f64>) -> RespValue {
  match val {
    Some(f) => float_to_blob(f),
    None => RespValue::Null,
  }
}

/// 将 Option<f64> 转为浮点数 Blob 或 nan/inf/Null
#[inline]
pub fn float_or_nan(val: Option<f64>) -> RespValue {
  match val {
    Some(f) => float_to_blob(f),
    None => RespValue::Blob(b"nan".to_vec()),
  }
}

/// 基于 itoa 将整数高性能格式化为 RespValue::Blob（零堆分配）
#[inline]
pub fn int_to_blob(val: impl itoa::Integer) -> RespValue {
  let mut buf = itoa::Buffer::new();
  RespValue::Blob(buf.format(val).as_bytes().to_vec())
}

/// 批量将 Vec<u8> 迭代器转为 RespValue::Arr
#[inline]
pub fn blobs_to_arr<I, T>(iter: I) -> RespValue
where
  I: IntoIterator<Item = T>,
  T: Into<Vec<u8>>,
{
  let iter = iter.into_iter();
  let (lower, _) = iter.size_hint();
  let mut arr = Vec::with_capacity(lower);
  for item in iter {
    arr.push(RespValue::Blob(item.into()));
  }
  RespValue::Arr(arr)
}

/// 批量将 Option<Vec<u8>> 迭代器转为 RespValue::Arr (含 Null)
#[inline]
pub fn blobs_opt_to_arr<I, T>(iter: I) -> RespValue
where
  I: IntoIterator<Item = Option<T>>,
  T: Into<Vec<u8>>,
{
  let iter = iter.into_iter();
  let (lower, _) = iter.size_hint();
  let mut arr = Vec::with_capacity(lower);
  for item in iter {
    arr.push(match item {
      Some(v) => RespValue::Blob(v.into()),
      None => RespValue::Null,
    });
  }
  RespValue::Arr(arr)
}

/// 批量将 bool 迭代器转为 RespValue::Arr (含 0/1)
#[inline]
pub fn bools_to_arr<I>(iter: I) -> RespValue
where
  I: IntoIterator<Item = bool>,
{
  let iter = iter.into_iter();
  let (lower, _) = iter.size_hint();
  let mut arr = Vec::with_capacity(lower);
  for item in iter {
    arr.push(bool_to_int(item));
  }
  RespValue::Arr(arr)
}

/// 将有序集合 (成员, 分数) 序列化为 RESP 数组
#[inline]
pub fn member_scores_to_arr(items: Vec<(Vec<u8>, f64)>, with_scores: bool) -> RespValue {
  let mut res = Vec::with_capacity(if with_scores {
    items.len() * 2
  } else {
    items.len()
  });
  let mut zmij_buf = zmij::Buffer::new();
  let mut itoa_buf = itoa::Buffer::new();
  for (m, s) in items {
    res.push(RespValue::Blob(m));
    if with_scores {
      if s.is_infinite() {
        res.push(RespValue::Blob(if s.is_sign_positive() {
          b"inf".to_vec()
        } else {
          b"-inf".to_vec()
        }));
      } else if s.is_nan() {
        res.push(RespValue::Blob(b"nan".to_vec()));
      } else if s == 0.0 {
        res.push(RespValue::Blob(b"0".to_vec()));
      } else if s.trunc() == s && (MIN_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&s) {
        res.push(RespValue::Blob(
          itoa_buf.format(s as i64).as_bytes().to_vec(),
        ));
      } else {
        res.push(RespValue::Blob(
          format_float_bytes(s, &mut zmij_buf).to_vec(),
        ));
      }
    }
  }
  RespValue::Arr(res)
}

/// 将键值对 (Key, Val) 平铺为 RESP 数组
#[inline]
pub fn pair_blobs_to_arr(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> RespValue {
  let mut res = Vec::with_capacity(pairs.len() * 2);
  for (k, v) in pairs {
    res.push(RespValue::Blob(k));
    res.push(RespValue::Blob(v));
  }
  RespValue::Arr(res)
}
