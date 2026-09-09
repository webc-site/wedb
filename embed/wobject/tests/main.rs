use std::io::Cursor;

use aok::{OK, Void};
use log::info;
use wobject::hash::hash_object::HashObject;
use wobject::list::list_object::{ListObject, ListOperation};
use wobject::set::set_object::{SetObject, SetOperation};
use wobject::sorted_set::sorted_set_object::{SortedSetObject, SortedSetOperation};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

#[test]
fn test() -> Void {
  info!("> test {}", 123456);
  OK
}

/// Hash 对象：operate 基本读写 + bitcode 序列化往返
#[test]
fn hash_operate_and_roundtrip() -> Void {
  let obj = HashObject::new();
  assert_eq!(obj.operate(0 /* HSET */, b"k1", b"v1"), None);
  assert_eq!(obj.operate(2 /* HGET */, b"k1", b""), Some(b"v1".to_vec()));
  assert_eq!(obj.operate(6 /* HLEN */, b"", b""), Some(b"1".to_vec()));
  assert_eq!(
    obj.operate(7 /* HEXISTS */, b"k1", b""),
    Some(b"1".to_vec())
  );

  let mut buf = Vec::new();
  obj.serialize(&mut buf)?;
  let restored = HashObject::deserialize(&mut Cursor::new(&buf))?;
  assert_eq!(restored.operate(2 /* HGET */, b"k1", b""), Some(b"v1".to_vec()));
  assert_eq!(restored.operate(6 /* HLEN */, b"", b""), Some(b"1".to_vec()));
  OK
}

/// Set 对象：SPOP/SRANDMEMBER 随机语义（多样本下不应恒返回同一元素），序列化往返
#[test]
fn set_random_semantics_and_roundtrip() -> Void {
  let obj = SetObject::new();
  for i in 0..64u32 {
    assert!(obj.operate(SetOperation::Sadd, &i.to_be_bytes()));
  }
  assert_eq!(obj.count(), 64);

  // 16 个成员各采 1 次，随机下标下命中面应多于 1 种（恒取首元素时只会是 1 种）
  let mut distinct = std::collections::HashSet::new();
  for _ in 0..16 {
    if let Some(m) = obj.random_member() {
      distinct.insert(m);
    }
  }
  assert!(distinct.len() > 1, "SRANDMEMBER 不应恒返回同一元素");

  let mut buf = Vec::new();
  obj.serialize(&mut buf)?;
  let restored = SetObject::deserialize(&mut Cursor::new(&buf))?;
  assert_eq!(restored.count(), 64);
  assert!(restored.operate(SetOperation::Sismember, &7u32.to_be_bytes()));
  OK
}

/// List 对象：推入弹出次序 + range/trim/index 边界
#[test]
fn list_ops() -> Void {
  let obj = ListObject::new();
  obj.operate(ListOperation::Rpush, b"a");
  obj.operate(ListOperation::Rpush, b"b");
  obj.operate(ListOperation::Rpush, b"c");
  assert_eq!(obj.operate(ListOperation::Lpop, b""), Some(b"a".to_vec()));
  assert_eq!(obj.count(), 2);
  assert_eq!(obj.index(-1), Some(b"c".to_vec()));
  assert_eq!(obj.index(5), None);
  let ranged = obj.range(0, -1);
  assert_eq!(ranged, vec![b"b".to_vec(), b"c".to_vec()]);
  obj.trim(0, 0);
  assert_eq!(obj.count(), 1);
  OK
}

/// SortedSet 对象：zadd/zscore/popmin/popmax 与树-字典一致性
#[test]
fn sorted_set_ops() -> Void {
  let obj = SortedSetObject::new();
  assert_eq!(obj.operate(SortedSetOperation::Zadd, b"m1", 1.5), None);
  assert_eq!(obj.operate(SortedSetOperation::Zadd, b"m2", -0.5), None);
  assert_eq!(obj.operate(SortedSetOperation::Zadd, b"m3", 3.0), None);
  assert_eq!(obj.count(), 3);
  // 同 member 改分：树与字典同步迁移
  assert_eq!(
    obj.operate(SortedSetOperation::Zadd, b"m1", 2.5),
    None
  );
  assert_eq!(obj.operate(SortedSetOperation::Zscore, b"m1", 0.0), Some(2.5));

  assert_eq!(
    obj.pop_min(),
    Some((b"m2".to_vec(), -0.5))
  );
  assert_eq!(obj.pop_max(), Some((b"m3".to_vec(), 3.0)));
  assert_eq!(obj.count(), 1);
  OK
}
