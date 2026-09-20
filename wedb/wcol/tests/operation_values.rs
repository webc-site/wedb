//! 对象子操作枚举落盘判别值黄金快照。
//!
//! 四族子操作号即 `wnode/src/aof/replay_input.rs` 32 字节头里的 `[sub_id u8]`，与
//! `[cmd u16][obj_type u8]` 同面落 AOF、落副本流、落 RangeIndex 复制，属持久值：改一个数
//! 即改一条已写记录的子操作语义。判别值从 0 起连续占位、只允许在块尾追加。
//!
//! 对位 C# 守卫测试 test/standalone/Garnet.test/PersistedEnumStabilityTests.cs 的
//! ObjectSubOpValuesAreStable 与 ObjectSubOpsFitInByte（黄金值硬编于此，不读 garnet 目录：
//! 该目录被 gitignore 且 CI 工作树中不存在）。C# 断言判别值 ≤255 是因为其枚举底层是 int32；
//! rust 侧四族皆 `#[repr(u8)]`，单字节容纳由类型本身保证，故等价断言取「`u8` 往返同值」。

use std::fmt;

use wcol::{HashOperation, ListOperation, SetOperation, SortedSetOperation};

/// HashOperation 黄金表（对位 C# ExpectedHashOps 逐值）。
const HASH_OPS_GOLDEN: &[(HashOperation, u8)] = &[
  (HashOperation::Hcollect, 0),
  (HashOperation::Hexpire, 1),
  (HashOperation::Httl, 2),
  (HashOperation::Hpersist, 3),
  (HashOperation::Hget, 4),
  (HashOperation::Hmget, 5),
  (HashOperation::Hset, 6),
  (HashOperation::Hmset, 7),
  (HashOperation::Hsetnx, 8),
  (HashOperation::Hlen, 9),
  (HashOperation::Hdel, 10),
  (HashOperation::Hexists, 11),
  (HashOperation::Hgetall, 12),
  (HashOperation::Hkeys, 13),
  (HashOperation::Hvals, 14),
  (HashOperation::Hincrby, 15),
  (HashOperation::Hincrbyfloat, 16),
  (HashOperation::Hrandfield, 17),
  (HashOperation::Hscan, 18),
  (HashOperation::Hstrlen, 19),
];

/// SortedSetOperation 黄金表（对位 C# ExpectedSortedSetOps 逐值）。
const SORTED_SET_OPS_GOLDEN: &[(SortedSetOperation, u8)] = &[
  (SortedSetOperation::Zadd, 0),
  (SortedSetOperation::Zcard, 1),
  (SortedSetOperation::Zpopmax, 2),
  (SortedSetOperation::Zscore, 3),
  (SortedSetOperation::Zrem, 4),
  (SortedSetOperation::Zcount, 5),
  (SortedSetOperation::Zincrby, 6),
  (SortedSetOperation::Zrank, 7),
  (SortedSetOperation::Zrange, 8),
  (SortedSetOperation::Geoadd, 9),
  (SortedSetOperation::Geohash, 10),
  (SortedSetOperation::Geodist, 11),
  (SortedSetOperation::Geopos, 12),
  (SortedSetOperation::Geosearch, 13),
  (SortedSetOperation::Zrevrank, 14),
  (SortedSetOperation::Zremrangebylex, 15),
  (SortedSetOperation::Zremrangebyrank, 16),
  (SortedSetOperation::Zremrangebyscore, 17),
  (SortedSetOperation::Zlexcount, 18),
  (SortedSetOperation::Zpopmin, 19),
  (SortedSetOperation::Zrandmember, 20),
  (SortedSetOperation::Zdiff, 21),
  (SortedSetOperation::Zscan, 22),
  (SortedSetOperation::Zmscore, 23),
  (SortedSetOperation::Zexpire, 24),
  (SortedSetOperation::Zttl, 25),
  (SortedSetOperation::Zpersist, 26),
  (SortedSetOperation::Zcollect, 27),
];

/// ListOperation 黄金表（对位 C# ExpectedListOps 逐值）。
const LIST_OPS_GOLDEN: &[(ListOperation, u8)] = &[
  (ListOperation::Lpop, 0),
  (ListOperation::Lpush, 1),
  (ListOperation::Lpushx, 2),
  (ListOperation::Rpop, 3),
  (ListOperation::Rpush, 4),
  (ListOperation::Rpushx, 5),
  (ListOperation::Llen, 6),
  (ListOperation::Ltrim, 7),
  (ListOperation::Lrange, 8),
  (ListOperation::Lindex, 9),
  (ListOperation::Linsert, 10),
  (ListOperation::Lrem, 11),
  (ListOperation::Rpoplpush, 12),
  (ListOperation::Lmove, 13),
  (ListOperation::Lset, 14),
  (ListOperation::Brpop, 15),
  (ListOperation::Blpop, 16),
  (ListOperation::Lpos, 17),
];

/// SetOperation 黄金表（对位 C# ExpectedSetOps 逐值）。
const SET_OPS_GOLDEN: &[(SetOperation, u8)] = &[
  (SetOperation::Sadd, 0),
  (SetOperation::Srem, 1),
  (SetOperation::Spop, 2),
  (SetOperation::Smembers, 3),
  (SetOperation::Scard, 4),
  (SetOperation::Sscan, 5),
  (SetOperation::Smove, 6),
  (SetOperation::Srandmember, 7),
  (SetOperation::Sismember, 8),
  (SetOperation::Smismember, 9),
  (SetOperation::Sunion, 10),
  (SetOperation::Sunionstore, 11),
  (SetOperation::Sdiff, 12),
  (SetOperation::Sdiffstore, 13),
  (SetOperation::Sinter, 14),
  (SetOperation::Sinterstore, 15),
];

/// 逐值锁定：黄金表值 == 判别值，且该判别值反查回同一成员。
fn assert_values_stable<T>(golden: &[(T, u8)], at: impl Fn(u8) -> Option<T>)
where
  T: Copy + PartialEq + fmt::Debug + Into<u8>,
{
  for (op, expected) in golden {
    assert_eq!(
      (*op).into(),
      *expected,
      "子操作落盘判别值漂移：{op:?} 应为 {expected}"
    );
    assert_eq!(
      at(*expected),
      Some(*op),
      "判别值 {expected} 反查不到 {op:?}"
    );
  }
}

/// 块内稠密且上界封顶：0..=max 每位都有成员、max 之后不得长出成员（删中间成员留洞、插队
/// 重编号都会在此红）。四族皆 `#[repr(u8)]`，单字节承载由类型保证，故 max 上界即字节位宽护栏。
fn assert_dense_and_bounded(label: &str, max: u8, occupied: impl Fn(u8) -> bool) {
  for value in 0..=u8::MAX {
    if value <= max {
      assert!(
        occupied(value),
        "{label} 位 {value} 出现空洞（登记上界 {max} 之内）"
      );
    } else {
      assert!(!occupied(value), "{label} 位 {value} 越出登记上界 {max}");
    }
  }
}

#[test]
fn object_sub_op_values_are_stable() {
  assert_values_stable(HASH_OPS_GOLDEN, |v| HashOperation::try_from(v).ok());
  assert_values_stable(SORTED_SET_OPS_GOLDEN, |v| {
    SortedSetOperation::try_from(v).ok()
  });
  assert_values_stable(LIST_OPS_GOLDEN, |v| ListOperation::try_from(v).ok());
  assert_values_stable(SET_OPS_GOLDEN, |v| SetOperation::try_from(v).ok());

  assert_eq!(HASH_OPS_GOLDEN.len(), 20, "HashOperation 黄金计数漂移");
  assert_eq!(
    SORTED_SET_OPS_GOLDEN.len(),
    28,
    "SortedSetOperation 黄金计数漂移"
  );
  assert_eq!(LIST_OPS_GOLDEN.len(), 18, "ListOperation 黄金计数漂移");
  assert_eq!(SET_OPS_GOLDEN.len(), 16, "SetOperation 黄金计数漂移");
}

#[test]
fn object_sub_op_bands_are_dense_and_bounded() {
  assert_dense_and_bounded("HashOperation", 19, |v| HashOperation::try_from(v).is_ok());
  assert_dense_and_bounded("SortedSetOperation", 27, |v| {
    SortedSetOperation::try_from(v).is_ok()
  });
  assert_dense_and_bounded("ListOperation", 17, |v| ListOperation::try_from(v).is_ok());
  assert_dense_and_bounded("SetOperation", 15, |v| SetOperation::try_from(v).is_ok());
}
