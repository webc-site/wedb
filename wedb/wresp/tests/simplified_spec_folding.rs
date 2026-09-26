//! 简化键规格折叠与提键扫描的纯逻辑锁测（自 src/catalog/simplified.rs 内联 tests 迁出）
//!
//! 与 tests/getkeys_keynum_bs2_saturating_clamp.rs 的 bs=2 饱和形锁对偶：
//! 本文件掌 bs=1 截断形与 numkeys 恶意值双闸面；§149 空键区早拒锁测照搬勿删。

use std::slice::from_ref;

use wbase::store_type::StoreType;
use wresp::{
  catalog::{
    RespAclCategories, RespCommandFlags, RespCommandsInfo,
    simplified::{
      SimpleRespCommandInfo, SimpleRespKeySpec, SimpleRespKeySpecBeginSearch,
      SimpleRespKeySpecFindKeys, extract_keys_and_flags_from_slice, extract_keys_from_slice,
      populate_simple_command_info, try_get_simple_key_spec,
    },
    try_get_simple_resp_command_info,
  },
  command::RespCommand,
  key_spec::{
    BeginSearchMethod, FindKeysMethod, KeySpecificationFlags, RespCommandKeySpecification,
  },
};

/// 完整信息折叠（GET 形态：fast + readonly，index 键规格）
#[test]
fn populate_get_shape() {
  let info = RespCommandsInfo {
    command: RespCommand::Get,
    name: "GET",
    is_internal: false,
    arity: 2,
    flags: RespCommandFlags::from_member_names("Fast, ReadOnly").unwrap(),
    first_key: 1,
    last_key: 1,
    step: 1,
    acl_categories: RespAclCategories::READ | RespAclCategories::FAST,
    tips: Vec::new(),
    key_specifications: vec![RespCommandKeySpecification {
      begin_search: Some(BeginSearchMethod::Index(1)),
      find_keys: Some(FindKeysMethod::Range {
        last_key: 0,
        key_step: 1,
        limit: 0,
      }),
      notes: None,
      flags: KeySpecificationFlags::from_wire_names("RO,access").unwrap(),
    }],
    store_type: StoreType::Main,
    sub_commands: Vec::new(),
    is_sub_command: false,
    parent_is_internal: false,
  };

  let mut simple = SimpleRespCommandInfo::default();
  populate_simple_command_info(&info, &mut simple);
  assert_eq!(simple.arity, 2);
  assert!(simple.allowed_in_txn);
  assert!(!simple.is_parent);
  assert!(!simple.is_sub_command);
  assert_eq!(simple.store_type, StoreType::Main);

  let ks = &simple.key_specs[0];
  assert!(ks.begin_search.is_index_type);
  assert_eq!(ks.begin_search.index, 1);
  assert!(ks.find_keys.is_range_type);
  assert_eq!(ks.find_keys.last_key_or_limit, 0);

  // 集群槽位提取路径（参数表不含命令名）
  let args = [b"key".as_slice(), b"v1"];
  assert_eq!(ks.get_key_search_args_slice(&args, false), Some((0, 0, 1)));
}

/// keyword 型 begin_search 折叠（XAUTOCLAIM 式）
#[test]
fn simple_key_spec_keyword_form() {
  let spec = RespCommandKeySpecification {
    begin_search: Some(BeginSearchMethod::Keyword {
      keyword: "FROM".to_string(),
      start_from: 2,
    }),
    find_keys: Some(FindKeysMethod::KeyNum {
      key_num_idx: 1,
      first_key: 0,
      key_step: 1,
    }),
    notes: None,
    flags: KeySpecificationFlags::empty(),
  };
  let ks = try_get_simple_key_spec(&spec).unwrap();
  assert!(!ks.begin_search.is_index_type);
  assert_eq!(ks.begin_search.keyword, b"FROM".to_vec());
  assert_eq!(ks.begin_search.index, 2);
  assert!(!ks.find_keys.is_range_type);
  assert_eq!(ks.find_keys.key_num_index, 1);

  // Unknown 方法不可折叠
  let unknown = RespCommandKeySpecification {
    begin_search: Some(BeginSearchMethod::Unknown),
    find_keys: None,
    notes: None,
    flags: KeySpecificationFlags::empty(),
  };
  assert!(try_get_simple_key_spec(&unknown).is_none());
}

/// COMMAND GETKEYS ZUNION 2147483647 zset1 模式：大 keynum 截断至 count - 1
#[test]
fn simple_key_spec_numkeys_clamped() {
  let spec = RespCommandKeySpecification {
    begin_search: Some(BeginSearchMethod::Index(1)),
    find_keys: Some(FindKeysMethod::KeyNum {
      key_num_idx: 0,
      first_key: 1,
      key_step: 1,
    }),
    notes: None,
    flags: KeySpecificationFlags::empty(),
  };
  let ks = try_get_simple_key_spec(&spec).unwrap();
  // 模拟参数列表：["2147483647", "zset1"]（不含命令名时，begin_search_idx 0 指向 "2147483647"）
  let args = [b"2147483647".as_slice(), b"zset1"];
  let (first, last, step) = ks.get_key_search_args_slice(&args, false).unwrap();
  assert_eq!(first, 1);
  assert_eq!(last, 1);
  assert_eq!(step, 1);
}

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
  assert_eq!(
    pairs.as_slice(),
    &[
      (&b"k1"[..], KeySpecificationFlags::RW),
      (&b"k2"[..], KeySpecificationFlags::RW)
    ]
  );
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
  assert_eq!(
    pairs.as_slice(),
    &[
      (&b"k1"[..], KeySpecificationFlags::RW),
      (&b"k2"[..], KeySpecificationFlags::RW)
    ]
  );
}

#[test]
fn extract_keys_reverse_keyword_scan_terminates() {
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

#[test]
fn extract_keys_malicious_numkeys_clamped() {
  let args: &[&[u8]] = &[b"2147483647", b"k1", b"k2"];
  let keys = extract_keys_from_slice(args, from_ref(&keynum_spec(1)), false);
  assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..]]);

  let keys = extract_keys_from_slice(args, from_ref(&keynum_spec(2)), false);
  assert_eq!(keys.as_slice(), &[&b"k1"[..]]);

  let pairs = extract_keys_and_flags_from_slice(args, from_ref(&keynum_spec(1)), false);
  assert_eq!(
    pairs.as_slice(),
    &[
      (&b"k1"[..], KeySpecificationFlags::RO),
      (&b"k2"[..], KeySpecificationFlags::RO)
    ]
  );
}

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

/// numkeys=0 空键区双闸锁测（deviations §149，目录真源规格，严禁回改）：
/// EVAL 形（bs Index=2）["s","0"]／["s","0","v1"] 与 ZUNION 形（bs Index=1）
/// ["0"] 三例——参数表不含命令名（live parseState 形），numkeys=0 一律
/// 早拒回 None、提键回空（C# 槽校验核在该形无条件取 firstIdx 读同会话
/// 残留槽定槽，rust 双闸短路判无键放行）；numkeys=1 正形对照排除空门
#[test]
fn numkeys_zero_keynum_spec_yields_no_keys() {
  let eval = try_get_simple_resp_command_info(RespCommand::Eval).unwrap();
  let zunion = try_get_simple_resp_command_info(RespCommand::Zunion).unwrap();
  let eval_spec = eval.key_specs.first().unwrap();
  let zunion_spec = zunion.key_specs.first().unwrap();

  // EVAL 形两例：numkeys token 后有无残参皆不改早拒裁决（残参即 C# 首键直取值）
  let args: &[&[u8]] = &[b"s", b"0"];
  assert_eq!(eval_spec.get_key_search_args_slice(args, false), None);
  assert!(eval_spec.extract_keys(args, false).is_empty());
  let args: &[&[u8]] = &[b"s", b"0", b"v1"];
  assert_eq!(eval_spec.get_key_search_args_slice(args, false), None);
  assert!(eval_spec.extract_keys(args, false).is_empty());
  let args: &[&[u8]] = &[b"0"];
  assert_eq!(zunion_spec.get_key_search_args_slice(args, false), None);
  assert!(zunion_spec.extract_keys(args, false).is_empty());

  // 正形对照：numkeys=1 两规格键区照常命中（None 源于空键区双闸而非规格死）
  let args: &[&[u8]] = &[b"s", b"1", b"k1"];
  assert_eq!(
    eval_spec.get_key_search_args_slice(args, false),
    Some((2, 2, 1))
  );
  assert_eq!(
    eval_spec.extract_keys(args, false).as_slice(),
    &[&b"k1"[..]]
  );
  let args: &[&[u8]] = &[b"1", b"k1"];
  assert_eq!(
    zunion_spec.get_key_search_args_slice(args, false),
    Some((1, 1, 1))
  );
  assert_eq!(
    zunion_spec.extract_keys(args, false).as_slice(),
    &[&b"k1"[..]]
  );
}

#[test]
fn extract_keys_empty_specs() {
  let args: &[&[u8]] = &[b"k1", b"k2"];
  assert!(extract_keys_from_slice(args, &[], false).is_empty());
  assert!(extract_keys_and_flags_from_slice(args, &[], false).is_empty());
}

#[test]
fn extract_keys_multi_specs_sorted() {
  let args: &[&[u8]] = &[b"k1", b"v1", b"k2", b"v2"];
  let spec1 = SimpleRespKeySpec {
    begin_search: SimpleRespKeySpecBeginSearch {
      index: 3,
      is_index_type: true,
      keyword: Vec::new(),
    },
    find_keys: SimpleRespKeySpecFindKeys {
      key_step: 1,
      is_range_type: true,
      last_key_or_limit: 0,
      ..Default::default()
    },
    flags: KeySpecificationFlags::RO,
  };
  let spec2 = SimpleRespKeySpec {
    begin_search: SimpleRespKeySpecBeginSearch {
      index: 1,
      is_index_type: true,
      keyword: Vec::new(),
    },
    find_keys: SimpleRespKeySpecFindKeys {
      key_step: 1,
      is_range_type: true,
      last_key_or_limit: 0,
      ..Default::default()
    },
    flags: KeySpecificationFlags::RW,
  };
  let keys = extract_keys_from_slice(args, &[spec1.clone(), spec2.clone()], false);
  assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..]]);

  let pairs = extract_keys_and_flags_from_slice(args, &[spec1, spec2], false);
  assert_eq!(
    pairs.as_slice(),
    &[
      (&b"k1"[..], KeySpecificationFlags::RW),
      (&b"k2"[..], KeySpecificationFlags::RO)
    ]
  );
}
