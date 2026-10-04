//! hash 信封反序列化判重守卫回归（zcode-r27-wcolstruct 发现三 + zcode-r46-wcolser 发现一）
//!
//! 对标 C# `libs/server/Objects/Hash/HashObject.cs:HashObject(BinaryReader)`：
//! 装载臂经 `hash.Add(item, value)` 进入字典——.NET `Dictionary.Add` 遇重复键
//! 直接抛 ArgumentException，由上层阻断畸形载荷，绝不产出静默失真对象。Rust
//! 侧 HashMap::insert 为覆盖语义，entries 向量无守卫时 update_size 逐条重复
//! 累加（记账永久虚标）且值静默末条覆盖；expirations 向量无守卫时经
//! ExpiryLedger::insert 二次满额入账（+SLOT*4）而字典项仅覆盖，净虚标
//! SLOT*2/条且堆内滞留陈旧项。两向量守卫与 zset 侧 entries 既有守卫单套形态：
//! contains_key / ledger.get_time 判重跳过，保持首条有效。
//!
//! 自研回归锁: hash/zset 信封装载判重防御（C# 反序列化无此臂，本仓序化格式防御面）

use wbase::{
  heap::{CONTAINER_BASE, EXPIRY_STRUCT_BASE, SLOT, round_up_ptr},
  time::now_ticks,
};
use wcol::{
  HashObject, HashOperation, ObjectOutput,
  hash::HashWire,
  zset::{SortedSetObject, SortedSetWire},
};

/// 远未来刻度（免真实等待造存活挂账）
const LIVE_SPAN: i64 = 1_000_000_000;

/// hash 条目记账构成（与 `account_entry` 落地口径一致：取整实计 + 三槽）
fn hash_entry(key_len: usize, value_len: usize) -> i64 {
  (round_up_ptr(key_len) + round_up_ptr(value_len)) as i64 + SLOT * 3
}

/// zset 条目记账构成（与 zset 侧 `account_entry` 一致：取整实计 + 四槽）
fn zset_entry(member_len: usize) -> i64 {
  round_up_ptr(member_len) as i64 + SLOT * 4
}

fn load_hash(wire: &HashWire) -> HashObject {
  HashObject::deserialize_from_slice(&bitcode::encode(wire))
    .expect("重复条目载荷应正常装载（判重跳过后非畸形）")
}

fn load_zset(wire: &SortedSetWire) -> SortedSetObject {
  SortedSetObject::deserialize_from_slice(&bitcode::encode(wire))
    .expect("重复条目载荷应正常装载（判重跳过后非畸形）")
}

/// 重复 field：保持首条有效，记账只计一次（修复前 update_size 重复累加 + 值末条覆盖）
#[test]
fn hash_duplicate_field_keeps_first_entry_and_accounting() {
  let obj = load_hash(&HashWire {
    entries: vec![
      (b"k1".to_vec(), b"first".to_vec()),
      (b"k1".to_vec(), b"second".to_vec()),
      (b"k2".to_vec(), b"v".to_vec()),
    ],
    expirations: None,
  });

  assert_eq!(obj.hash.len(), 2, "重复 field 应判重跳过保持单条");
  assert_eq!(
    obj.hash.get("k1".as_bytes()).map(Vec::as_slice),
    Some(b"first".as_slice()),
    "重复 field 值静默末条覆盖"
  );
  assert_eq!(
    obj.heap_memory_size,
    CONTAINER_BASE + hash_entry(2, 5) + hash_entry(2, 1),
    "重复条目致 heap_memory_size 永久虚标"
  );
}

/// 重复 expirations（同值/异值两态）：满额入账仅一次，首条刻度生效
#[test]
fn hash_duplicate_expirations_counted_once() {
  let now = now_ticks();
  for expirations in [
    // 同值两态
    vec![
      (b"m".to_vec(), now + LIVE_SPAN),
      (b"m".to_vec(), now + LIVE_SPAN),
    ],
    // 异值两态
    vec![
      (b"m".to_vec(), now + LIVE_SPAN),
      (b"m".to_vec(), now + LIVE_SPAN * 2),
    ],
  ] {
    let obj = load_hash(&HashWire {
      entries: vec![(b"m".to_vec(), b"v".to_vec())],
      expirations: Some(expirations),
    });

    assert_eq!(
      obj.heap_memory_size,
      CONTAINER_BASE + hash_entry(1, 1) + EXPIRY_STRUCT_BASE + SLOT * 4,
      "重复 expirations 经 ExpiryLedger 净虚标两槽/条"
    );
    assert_eq!(
      obj.get_expiration(b"m"),
      now + LIVE_SPAN,
      "重复条目应保持首条刻度"
    );
  }
}

/// 重复 expirations 装载后字段移除：记账归零无虚标残留（修复前堆项槽位永久滞留）
#[test]
fn hash_remove_after_duplicate_expirations_returns_to_baseline() {
  let now = now_ticks();
  let mut obj = load_hash(&HashWire {
    entries: vec![(b"m".to_vec(), b"v".to_vec())],
    expirations: Some(vec![
      (b"m".to_vec(), now + LIVE_SPAN),
      (b"m".to_vec(), now + LIVE_SPAN),
    ]),
  });

  assert_eq!(obj.remove(b"m"), Some(b"v".to_vec()));
  assert_eq!(
    obj.heap_memory_size, CONTAINER_BASE,
    "移除唯一字段后记账应完全归位容器基线"
  );
}

/// zset 重复 expirations：与 hash 侧同守卫同记账口径
#[test]
fn zset_duplicate_expirations_counted_once() {
  let now = now_ticks();
  for expirations in [
    vec![
      (b"m".to_vec(), now + LIVE_SPAN),
      (b"m".to_vec(), now + LIVE_SPAN),
    ],
    vec![
      (b"m".to_vec(), now + LIVE_SPAN),
      (b"m".to_vec(), now + LIVE_SPAN * 2),
    ],
  ] {
    let mut obj = load_zset(&SortedSetWire {
      entries: vec![(b"m".to_vec(), 1.0)],
      expirations: Some(expirations),
    });

    assert_eq!(
      obj.heap_memory_size,
      CONTAINER_BASE * 2 + zset_entry(1) + EXPIRY_STRUCT_BASE + SLOT * 4,
      "重复 expirations 经 ExpiryLedger 净虚标两槽/条"
    );
    assert_eq!(obj.get_expiration(b"m"), now + LIVE_SPAN, "应保持首条刻度");

    // 移除挂账唯一成员：双索引 + 账本全归位（zset_deserialize_dup_member
    // 同款幽灵残留判据在 expirations 面的镜像）
    assert_eq!(obj.rem(b"m"), Some(1.0));
    assert_eq!(
      obj.heap_memory_size,
      CONTAINER_BASE * 2,
      "记账应归位双容器基线"
    );
  }
}

/// HGETALL 经 operate 入口（确认判重后对象可正常服务读命令）
#[test]
fn hash_dup_load_serves_reads() {
  let mut obj = load_hash(&HashWire {
    entries: vec![
      (b"k1".to_vec(), b"v1".to_vec()),
      (b"k1".to_vec(), b"v2".to_vec()),
    ],
    expirations: None,
  });

  let mut sink = Vec::new();
  let mut output = ObjectOutput::mount(&mut sink);
  obj.operate(HashOperation::Hgetall as u8, &[], 0, 0, &mut output, 2);

  assert_eq!(output.result1, 1, "重复 field 判重后仅一条存活");
}
