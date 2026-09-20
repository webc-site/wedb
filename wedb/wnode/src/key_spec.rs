//! 会话侧键提取辅助（对标 libs/server/SessionParseStateExtensions.cs）
//!
//! SimpleRespKeySpec 与切片键提取纯函数已下沉至 [`wresp::catalog`]（对标
//! RespCommandInfoSimplifiedStructs.cs），此处保留向后兼容导出与桥接。

pub use wresp::catalog::{
  INLINE_KEYS, SimpleRespKeySpec, extract_keys_and_flags_from_slice, extract_keys_from_slice,
};

/// 扫描单条键规格的键参数区，按键下标升序回调 sink（向后兼容桥接）
#[inline]
pub fn scan_spec_keys<'a>(
  args: &[&'a [u8]],
  spec: &SimpleRespKeySpec,
  is_sub_command: bool,
  sink: impl FnMut(&'a [u8], usize),
) {
  spec.scan_keys(args, is_sub_command, sink);
}

#[cfg(test)]
mod tests {
  use std::slice::from_ref;

  use wresp::{
    catalog::{SimpleRespKeySpec, SimpleRespKeySpecBeginSearch, SimpleRespKeySpecFindKeys},
    key_spec::KeySpecificationFlags,
  };

  use super::*;

  /// keynum 型规格（numkeys 位于下标 0，首键下标 1）
  fn keynum_spec(key_step: i32) -> SimpleRespKeySpec {
    SimpleRespKeySpec {
      begin_search: SimpleRespKeySpecBeginSearch {
        index: 1,
        is_index_type: true,
        keyword: Vec::new(),
      },
      find_keys: SimpleRespKeySpecFindKeys {
        key_num_index: 0,
        first_key: 1,
        key_step,
        is_range_type: false,
        ..Default::default()
      },
      flags: KeySpecificationFlags::RO,
    }
  }

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
    assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..]]);

    let pairs = extract_keys_and_flags_from_slice(args, &[spec], false);
    let rw = KeySpecificationFlags::RW.bits() as u8;
    assert_eq!(pairs.as_slice(), &[(&b"k1"[..], rw), (&b"k2"[..], rw)]);
  }

  #[test]
  fn extract_keys_keynum_slice() {
    let args: &[&[u8]] = &[b"3", b"k1", b"k2", b"k3"];
    let keys = extract_keys_from_slice(args, from_ref(&keynum_spec(1)), false);
    assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..], &b"k3"[..]]);
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
    let rw = KeySpecificationFlags::RW.bits() as u8;
    assert_eq!(pairs.as_slice(), &[(&b"k1"[..], rw), (&b"k2"[..], rw)]);
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
    assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..]]);
  }

  /// D16 回归：恶意 numkeys（i32::MAX）不 panic 不越界，末键钳制到参数表界
  ///（对标 C# PR #2112：lastKeyIdx 出界钳制 Count - 1）
  #[test]
  fn extract_keys_malicious_numkeys_clamped() {
    let args: &[&[u8]] = &[b"2147483647", b"k1", b"k2"];

    // (numkeys - 1) * key_step 在 C# int 域溢出回绕；rust 侧 isize 饱和后钳制
    let keys = extract_keys_from_slice(args, from_ref(&keynum_spec(1)), false);
    assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..]]);

    // key_step=2 乘法放大形态：钳制后隔键提取
    let keys = extract_keys_from_slice(args, from_ref(&keynum_spec(2)), false);
    assert_eq!(keys.as_slice(), &[&b"k1"[..]]);

    // 标记变体同界不 panic，标记位完整带回
    let ro = KeySpecificationFlags::RO.bits() as u8;
    let pairs = extract_keys_and_flags_from_slice(args, from_ref(&keynum_spec(1)), false);
    assert_eq!(pairs.as_slice(), &[(&b"k1"[..], ro), (&b"k2"[..], ro)]);
  }

  /// D16 回归：超 i32 值域 / 负数 / 零 / 非数字 numkeys 一律解析拒绝，空结果
  #[test]
  fn extract_keys_malicious_numkeys_rejected() {
    for raw in ["2147483648", "-2147483648", "0", "abc", "3.5", ""] {
      let args: &[&[u8]] = &[raw.as_bytes(), b"k1"];
      assert!(
        extract_keys_from_slice(args, from_ref(&keynum_spec(1)), false).is_empty(),
        "numkeys={raw}",
      );
    }
  }
}
