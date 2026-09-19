//! 会话侧键提取辅助（对标 libs/server/SessionParseStateExtensions.cs）
//!
//! SimpleRespKeySpec 全家单点定义于 wresp::catalog::simplified（对标
//! RespCommandInfoSimplifiedStructs.cs），此处保留参数切片域的键提取函数。

use wresp::catalog::SimpleRespKeySpec;

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
          keys_flags.push((bytes, spec.flags.bits() as u8, i));
        }
        i += step;
      }
    }
  }
  if keys_flags.len() > 1 {
    keys_flags.sort_unstable_by_key(|(_, _, i)| *i);
  }
  keys_flags.into_iter().map(|(k, f, _)| (k, f)).collect()
}

#[cfg(test)]
mod tests {
  use std::slice::from_ref;

  use wresp::{
    catalog::{SimpleRespKeySpec, SimpleRespKeySpecBeginSearch, SimpleRespKeySpecFindKeys},
    key_spec::KeySpecificationFlags,
  };

  use super::*;

  #[test]
  fn flags_bitwise_and_descriptions() {
    let flags = KeySpecificationFlags::RW | KeySpecificationFlags::ACCESS;
    assert!(flags.contains(KeySpecificationFlags::RW));
    assert!(flags.contains(KeySpecificationFlags::ACCESS));
    assert!(!flags.contains(KeySpecificationFlags::RO));
    assert_eq!(flags.iter().count(), 2);
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
        (&b"k1"[..], KeySpecificationFlags::RW.bits() as u8),
        (&b"k2"[..], KeySpecificationFlags::RW.bits() as u8),
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

  #[test]
  fn extract_keys_keyword_slice() {
    let args: &[&[u8]] = &[b"KEY", b"k1", b"k2"];
    let spec = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: 1,
        is_index_type: false,
        keyword: b"KEY".to_vec(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_step: 1,
        is_range_type: true,
        last_key_or_limit: 1,
        ..Default::default()
      },
      flags: KeySpecificationFlags::RW,
    };
    let pairs = extract_keys_and_flags_from_slice(args, from_ref(&spec), false);
    assert_eq!(
      pairs,
      vec![
        (&b"k1"[..], KeySpecificationFlags::RW.bits() as u8),
        (&b"k2"[..], KeySpecificationFlags::RW.bits() as u8),
      ]
    );
  }

  #[test]
  fn extract_keys_reverse_keyword_scan_terminates() {
    // 负下标 = 逆序关键字扫描；关键字未命中时安全终止（下界护栏）
    let args: &[&[u8]] = &[b"KEY", b"k1", b"k2"];
    let miss = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: -1,
        is_index_type: false,
        keyword: b"MISSING".to_vec(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_step: 1,
        is_range_type: true,
        last_key_or_limit: 1,
        ..Default::default()
      },
      flags: KeySpecificationFlags::empty(),
    };
    assert!(extract_keys_from_slice(args, &[miss], false).is_empty());

    // 逆序扫描能命中参数区内的关键字（C# 语义：自尾部向前找）
    let hit = SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: -2,
        is_index_type: false,
        keyword: b"KEY".to_vec(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_step: 1,
        is_range_type: true,
        last_key_or_limit: 1,
        ..Default::default()
      },
      flags: KeySpecificationFlags::empty(),
    };
    let keys = extract_keys_from_slice(args, &[hit], false);
    assert_eq!(keys, vec![&b"k1"[..], &b"k2"[..]]);
  }
}
