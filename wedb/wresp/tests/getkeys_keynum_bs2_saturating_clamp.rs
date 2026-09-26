//! COMMAND GETKEYS bs=2 keynum 大值饱和钳制语义锁（doc/zh/deviations.md §106 宗 b）
//!
//! 与 tests/simplified_spec_folding.rs 的 bs=1 截断形锁对偶：EVAL 形键规格
//! （BeginSearchIndex=2、KeyNumIdx=0、FirstKey=1、KeyStep=1，两仓
//! RespCommandsInfo.json 全等），GETKEYS 切片态参数为
//! `COMMAND GETKEYS EVAL s 2147483647 k` 剥名后的 ["s", "2147483647", "k"]。
//! firstKeyIdx=(2-1)+1=2，numkeys=i32::MAX 时 C# 对位加法
//! 2+2147483646 以 unchecked int 回绕 -2147483648、钳制不命中恒回 *0 空数组
//! （garnet/libs/server/SessionParseStateExtensions.cs:1003）；rust 以 isize
//! 饱和算式得 2147483648 再钳 count-1=2，回真实键 "k"（*1+k）。
//! 本锁钉死 rust 定义性行为，严禁按 C# 回绕形态回改。

use std::slice::from_ref;

use wresp::{
  catalog::simplified::{SimpleRespKeySpec, extract_keys_from_slice, try_get_simple_key_spec},
  key_spec::{
    BeginSearchMethod, FindKeysMethod, KeySpecificationFlags, RespCommandKeySpecification,
  },
};

/// EVAL 形键规格（bs Index=2，JSON 亲验值原样复刻）
fn eval_shape_spec() -> SimpleRespKeySpec {
  let spec = RespCommandKeySpecification {
    begin_search: Some(BeginSearchMethod::Index(2)),
    find_keys: Some(FindKeysMethod::KeyNum {
      key_num_idx: 0,
      first_key: 1,
      key_step: 1,
    }),
    notes: None,
    flags: KeySpecificationFlags::empty(),
  };
  try_get_simple_key_spec(&spec).expect("index/keynum 规格必可折叠")
}

#[test]
fn getkeys_bs2_numkeys_int_max_saturates_and_clamps() {
  let ks = eval_shape_spec();
  // 切片态（Count=3）：["s", "2147483647", "k"]
  let args: &[&[u8]] = &[b"s", b"2147483647", b"k"];
  // C# 回绕形：2 + 2147483646 → -2147483648，零迭代回 *0；
  // rust 饱和形：saturating 得 2147483648，钳制 count-1=2
  let (first, last, step) = ks.get_key_search_args_slice(args, false).unwrap();
  assert_eq!((first, last, step), (2, 2, 1));
  // 提键面钉 *1+k（GETKEYS/GETKEYSANDFLAGS 共用提取路径）
  let keys = extract_keys_from_slice(args, from_ref(&ks), false);
  assert_eq!(keys.as_slice(), &[&b"k"[..]]);
}

#[test]
fn getkeys_bs2_numkeys_in_range_not_clamped() {
  // 界内对照组：numkeys=2 且参数齐备时不触钳制臂，两键直取
  let ks = eval_shape_spec();
  let args: &[&[u8]] = &[b"s", b"2", b"k1", b"k2"];
  let (first, last, step) = ks.get_key_search_args_slice(args, false).unwrap();
  assert_eq!((first, last, step), (2, 3, 1));
  let keys = extract_keys_from_slice(args, from_ref(&ks), false);
  assert_eq!(keys.as_slice(), &[&b"k1"[..], &b"k2"[..]]);
}
