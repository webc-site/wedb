//! wcol SortedSet 反序列化重复成员判重回归测试
//!（票 task/ing/wcol-sorted-set-deserialize-duplicate-member-desync）
//!
//! 对标 C# `libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetObject(BinaryReader)`：
//! 装载臂经 `sortedSetDict.Add(item, score)` 进入字典——.NET `Dictionary.Add` 遇重复键
//! 直接抛 ArgumentException，由 `GarnetObjectSerializer.Deserialize` 捕获阻断畸形载荷，
//! 绝不产出双索引（sortedSet / sortedSetDict）长度失步的对象。Rust 侧 HashMap::insert
//! 静默覆盖、BTreeSet 依 (score, member) 判异双插，故装载入口必须如 `from_entries`
//! 一样以 `!contains_key` 判重跳过，保持首条有效，杜绝失步与记账虚标。
//!
//! 修复前必红口径：`deserialize_from_slice` 无判重守卫时——
//! 1. 同名异分双插使 `sorted_set.len() > sorted_set_dict.len()`，ZCARD 触发
//!    `debug_assert_eq!(..., "SortedSet object is not in sync.")` panic（debug 下必红）；
//! 2. 即便不 panic，长度相等断言与单份记账断言亦必红；
//! 3. ZREM 仅摘字典现值一条，旧分同名条目以幽灵残留 BTreeSet，清空断言必红。
//!
//! 自研回归锁: zset 反序列化重复成员防御（C# 反序列化无此臂，本仓序化格式防御面）

use wbase::heap::{CONTAINER_BASE, SLOT, round_up_ptr};
use wcol::{ObjectOutput, SortedSetObject, SortedSetOperation, zset::SortedSetWire};

/// 装载含重复 member 的畸形 wire 并反序列化
fn deserialize_with_entries(entries: Vec<(&'static [u8], f64)>) -> SortedSetObject {
  let wire = SortedSetWire {
    entries: entries.into_iter().map(|(m, s)| (m.to_vec(), s)).collect(),
    expirations: None,
  };
  SortedSetObject::deserialize_from_slice(&bitcode::encode(&wire))
    .expect("重复成员载荷应正常装载（判重跳过后非畸形）")
}

/// 单条目记账构成（与 `account_entry` 落地口径一致：取整实计 + 四槽）
fn per_entry(member_len: usize) -> i64 {
  round_up_ptr(member_len) as i64 + SLOT * 4
}

/// ZCARD 经 operate 入口（debug 构建下即双索引同步断言的触发点）
fn zcard(obj: &mut SortedSetObject) -> i64 {
  let mut sink = Vec::new();
  let mut output = ObjectOutput::mount(&mut sink);
  obj.operate(SortedSetOperation::Zcard as u8, &[], 0, 0, &mut output, 2);
  output.result1
}

#[test]
fn duplicate_member_different_score_keeps_dual_indexes_in_sync() {
  // ("k1",10) 与 ("k1",20) 同名异分：散列 insert 覆盖单存、BTreeSet 判异双插，
  // 无守卫则长度发散
  let mut obj = deserialize_with_entries(vec![
    (b"k1", 10.0),
    (b"k1", 20.0),
    (b"k2", 5.0),
    (b"k3", 7.5),
  ]);
  assert_eq!(
    obj.sorted_set.len(),
    obj.sorted_set_dict.len(),
    "反序列化后双索引长度失步（sorted_set 混入同名重复条目）"
  );
  assert_eq!(obj.sorted_set.len(), 3);
  // 对标 from_entries 判重跳过语义：保持首条有效
  assert_eq!(obj.sorted_set_dict.get("k1".as_bytes()), Some(&10.0));
  // ZCARD 不触发 "SortedSet object is not in sync." 断言 panic
  assert_eq!(zcard(&mut obj), 3);
}

#[test]
fn duplicate_member_accounting_counted_once() {
  // 同名同分重复：update_size 不得重复累加，heap_memory_size 无虚标；
  // 同名异分重复：跳过的条目一并不记账（首条有效）
  let obj = deserialize_with_entries(vec![(b"member", 1.0), (b"member", 1.0)]);
  assert_eq!(obj.sorted_set_dict.len(), 1);
  assert_eq!(obj.sorted_set.len(), 1);
  assert_eq!(
    obj.heap_memory_size,
    CONTAINER_BASE * 2 + per_entry("member".len()),
    "重复条目导致 heap_memory_size 永久虚标"
  );

  let obj = deserialize_with_entries(vec![(b"k1", 10.0), (b"k1", 20.0), (b"k2", 5.0)]);
  assert_eq!(
    obj.heap_memory_size,
    CONTAINER_BASE * 2 + per_entry(2) * 2,
    "被跳过的重复条目不应参与记账"
  );
}

#[test]
fn zrem_after_duplicate_deserialize_leaves_no_ghost() {
  // 修复前：ZREM 只能按字典现值摘除一条同名条目，另一旧分条目幽灵残留
  // BTreeSet 且记账泄漏
  let mut obj = deserialize_with_entries(vec![(b"k1", 10.0), (b"k1", 20.0), (b"k2", 5.0)]);
  let base = CONTAINER_BASE * 2;

  assert_eq!(obj.rem(b"k1"), Some(10.0));
  assert_eq!(obj.rem(b"k2"), Some(5.0));
  assert!(obj.sorted_set_dict.is_empty());
  assert!(
    obj.sorted_set.is_empty(),
    "ZREM 后 BTreeSet 残留幽灵条目：{:?}",
    obj.sorted_set.iter().map(|e| e.score).collect::<Vec<_>>()
  );
  assert_eq!(obj.heap_memory_size, base, "幽灵残留伴随记账泄漏");
  // 清空后 ZCARD 仍须通过同步断言
  assert_eq!(zcard(&mut obj), 0);
}
