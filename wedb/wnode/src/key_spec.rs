//! RESP 键规格模型（对标 libs/server/Resp/RespCommandInfoSimplifiedStructs.cs 与 RespCommandKeySpecification.cs）

pub(crate) use wresp::key_spec::KeySpecificationFlags;

/// 简化版 begin_search 规格（对标 C# SimpleRespKeySpecBeginSearch）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimpleRespKeySpecBeginSearch {
  /// 键前置关键字（index 型为空）
  pub keyword: Vec<u8>,
  /// 键下标或关键字检索起点
  pub index: i32,
  /// true = index 型，否则 keyword 型
  pub is_index_type: bool,
}

/// 简化版 find_keys 规格（对标 C# SimpleRespKeySpecFindKeys）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimpleRespKeySpecFindKeys {
  /// 键数量参数下标（keynum 型）
  pub key_num_index: i32,
  /// 首键下标（keynum 型）
  pub first_key: i32,
  /// 末键下标或 limit（range 型）
  pub last_key_or_limit: i32,
  /// 找到一键后跳过的参数个数
  pub key_step: i32,
  /// true = range 型，否则 keynum 型
  pub is_range_type: bool,
  /// true = range 且按 limit 截断
  pub is_range_limit_type: bool,
}

/// 简化版单条键规格（对标 C# SimpleRespKeySpec）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SimpleRespKeySpec {
  /// begin_search 规格
  pub begin_search: SimpleRespKeySpecBeginSearch,
  /// find_keys 规格
  pub find_keys: SimpleRespKeySpecFindKeys,
  /// 键规格标记位图
  pub flags: KeySpecificationFlags,
}

#[inline]
fn parse_ascii_i64(bytes: &[u8]) -> Option<i64> {
  if bytes.is_empty() {
    return None;
  }
  let (negative, digits) = match bytes[0] {
    b'-' => (true, &bytes[1..]),
    b'+' => (false, &bytes[1..]),
    _ => (false, bytes),
  };
  if digits.is_empty() {
    return None;
  }
  let mut val: i64 = 0;
  for &d in digits {
    if !d.is_ascii_digit() {
      return None;
    }
    val = val.checked_mul(10)?.checked_add((d - b'0') as i64)?;
  }
  if negative {
    val.checked_neg()
  } else {
    Some(val)
  }
}

impl SimpleRespKeySpec {
  /// 依键规格从参数流中计算 (first_idx, last_idx, step)；返回 None 表示越界或关键字未命中
  ///
  /// 对标 libs/server/SessionParseStateExtensions.cs:TryGetKeySearchArgsFromSimpleKeySpec
  pub fn try_get_key_search_args<B, F>(
    &self,
    arg_count: usize,
    mut get_arg: F,
    is_sub_command: bool,
  ) -> Option<(usize, usize, usize)>
  where
    B: AsRef<[u8]>,
    F: FnMut(usize) -> Option<B>,
  {
    let count = arg_count as isize;
    if count <= 0 {
      return None;
    }

    let begin_search_idx = if self.begin_search.index < 0 {
      count + self.begin_search.index as isize
    } else {
      self.begin_search.index as isize - if is_sub_command { 2 } else { 1 }
    };
    if begin_search_idx < 0 || begin_search_idx >= count {
      return None;
    }

    let mut first_key_idx: isize = -1;
    if self.begin_search.is_index_type {
      first_key_idx = begin_search_idx;
    } else {
      let step: isize = if self.begin_search.index < 0 { -1 } else { 1 };
      let mut i = begin_search_idx;
      while i >= 0 && i < count {
        if let Some(bytes) = get_arg(i as usize)
          && bytes
            .as_ref()
            .eq_ignore_ascii_case(&self.begin_search.keyword)
        {
          first_key_idx = i + 1;
          break;
        }
        i += step;
      }
    }
    if first_key_idx < 0 {
      return None;
    }

    let key_step = self.find_keys.key_step as isize;
    if key_step <= 0 {
      return None;
    }

    let last_key_idx: isize;
    if self.find_keys.is_range_type {
      if self.find_keys.is_range_limit_type {
        let limit = self.find_keys.last_key_or_limit as isize;
        let key_num = 1 + (count - 1 - first_key_idx) / key_step;
        last_key_idx = if limit <= 1 {
          first_key_idx + (key_num - 1) * key_step
        } else {
          first_key_idx + ((key_num / limit) - 1) * key_step
        };
      } else {
        let raw = self.find_keys.last_key_or_limit as isize;
        last_key_idx = if raw < 0 {
          raw + count
        } else {
          first_key_idx + raw
        };
      }
    } else {
      let key_num_idx = begin_search_idx + self.find_keys.key_num_index as isize;
      if key_num_idx < 0 || key_num_idx >= count {
        return None;
      }
      let key_num_bytes = get_arg(key_num_idx as usize)?;
      let key_num = parse_ascii_i64(key_num_bytes.as_ref())?;
      if key_num <= 0 {
        return None;
      }
      first_key_idx += self.find_keys.first_key as isize;
      last_key_idx = first_key_idx + ((key_num as isize - 1) * key_step);
    }

    if first_key_idx < 0 || last_key_idx >= count || first_key_idx > last_key_idx {
      return None;
    }

    Some((
      first_key_idx as usize,
      last_key_idx as usize,
      key_step as usize,
    ))
  }

  /// 针对 &[&[u8]] 切片的快捷计算（集群槽位提取高频路径，零分配）
  #[inline]
  pub fn get_key_search_args_slice(
    &self,
    args: &[&[u8]],
    is_sub_command: bool,
  ) -> Option<(usize, usize, usize)> {
    self.try_get_key_search_args(args.len(), |i| args.get(i).copied(), is_sub_command)
  }
}

/// 从参数切片中提取键切片（按下标升序，零拷贝）
pub fn extract_keys_from_slice<'a>(
  args: &[&'a [u8]],
  key_specs: &[SimpleRespKeySpec],
  is_sub_command: bool,
) -> Vec<&'a [u8]> {
  let mut keys: Vec<(&'a [u8], usize)> = Vec::new();
  for spec in key_specs {
    if let Some((first_idx, last_idx, step)) = spec.get_key_search_args_slice(args, is_sub_command)
    {
      let mut i = first_idx;
      while i <= last_idx {
        if let Some(&bytes) = args.get(i)
          && !bytes.is_empty()
        {
          keys.push((bytes, i));
        }
        i += step;
      }
    }
  }
  if key_specs.len() > 1 {
    keys.sort_unstable_by_key(|(_, i)| *i);
  }
  keys.into_iter().map(|(k, _)| k).collect()
}

/// 从参数切片中提取键切片与标记（按下标升序，零拷贝）
pub fn extract_keys_and_flags_from_slice<'a>(
  args: &[&'a [u8]],
  key_specs: &[SimpleRespKeySpec],
  is_sub_command: bool,
) -> Vec<(&'a [u8], u8)> {
  let mut keys_flags: Vec<(&'a [u8], u8, usize)> = Vec::new();
  for spec in key_specs {
    if let Some((first_idx, last_idx, step)) = spec.get_key_search_args_slice(args, is_sub_command)
    {
      let mut i = first_idx;
      while i <= last_idx {
        if let Some(&bytes) = args.get(i)
          && !bytes.is_empty()
        {
          keys_flags.push((bytes, spec.flags.0 as u8, i));
        }
        i += step;
      }
    }
  }
  if key_specs.len() > 1 {
    keys_flags.sort_unstable_by_key(|(_, _, i)| *i);
  }
  keys_flags.into_iter().map(|(k, f, _)| (k, f)).collect()
}

#[cfg(test)]
mod tests {
  use std::slice::from_ref;

  use super::*;

  #[test]
  fn flags_bitwise_and_descriptions() {
    let flags = KeySpecificationFlags::RW | KeySpecificationFlags::ACCESS;
    assert!(flags.contains(KeySpecificationFlags::RW));
    assert!(flags.contains(KeySpecificationFlags::ACCESS));
    assert!(!flags.contains(KeySpecificationFlags::RO));
    assert_eq!(flags.count(), 2);
    let desc = flags.descriptions();
    assert_eq!(desc, vec!["RW", "access"]);
  }

  #[test]
  fn flags_from_wire_names() {
    let flags = KeySpecificationFlags::from_wire_names("RW,access,OW").unwrap();
    assert!(flags.contains(KeySpecificationFlags::RW));
    assert!(flags.contains(KeySpecificationFlags::ACCESS));
    assert!(flags.contains(KeySpecificationFlags::OW));
    assert!(KeySpecificationFlags::from_wire_names("INVALID").is_none());
  }

  #[test]
  fn extract_keys_range_slice() {
    let args: &[&[u8]] = &[b"k1", b"v1", b"k2", b"v2"];
    let spec = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: 1,
        is_index_type: true,
        keyword: Vec::new(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_step: 2,
        is_range_type: true,
        last_key_or_limit: -1,
        ..Default::default()
      },
      flags: KeySpecificationFlags::RW,
    };
    let keys = extract_keys_from_slice(args, from_ref(&spec), false);
    assert_eq!(keys, vec![&b"k1"[..], &b"k2"[..]]);

    let pairs = extract_keys_and_flags_from_slice(args, &[spec], false);
    assert_eq!(
      pairs,
      vec![
        (&b"k1"[..], KeySpecificationFlags::RW.0 as u8),
        (&b"k2"[..], KeySpecificationFlags::RW.0 as u8)
      ]
    );
  }

  #[test]
  fn extract_keys_keynum_slice() {
    let args: &[&[u8]] = &[b"3", b"k1", b"k2", b"k3"];
    let spec = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: 1,
        is_index_type: true,
        keyword: Vec::new(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_num_index: 0,
        first_key: 1,
        key_step: 1,
        is_range_type: false,
        ..Default::default()
      },
      flags: KeySpecificationFlags::RO,
    };
    let keys = extract_keys_from_slice(args, &[spec], false);
    assert_eq!(keys, vec![&b"k1"[..], &b"k2"[..], &b"k3"[..]]);
  }
}
