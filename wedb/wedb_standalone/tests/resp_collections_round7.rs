//! 审计 round7 集成测试：补齐 C# Garnet.test.collections / Garnet.test 覆盖
//! 而 Rust 侧缺失的高价值场景，并固化 round7 错误口径修复（句点差异）。
//!
//! 场景来源逐条标注 C# 测试文件:方法名。

mod support;

macro_rules! a {
  ($($x:expr),* $(,)?) => {
    &[$($x as &[u8]),*]
  };
}

use support::with_batch;
use wnode::resp::{
  key_admin_commands::ExpireCmd,
  objects::{sorted_set_commands::RemoveRangeKind, sorted_set_geo_commands::GeoSearchCommandKind},
};

/// 批量 ZADD 便捷封装（entries 为 (score, member) 序）
fn zadd<'a, D: wdev::Device>(
  s: &mut RespServerSession,
  batch: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  entries: &[(&[u8], &[u8])],
) -> Vec<u8> {
  let mut args: Vec<&[u8]> = vec![key];
  for &(score, member) in entries {
    args.push(score);
    args.push(member);
  }
  let mut out = Vec::new();
  s.sorted_set_add(&args, batch, &mut out).unwrap();
  out
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanGetRankAndRevRank
#[test]
fn zrank_and_zrevrank_with_score() {
  with_batch(|s, batch| {
    let key = b"zr7";
    let entries = &[(b"10".as_slice(), &b"a"[..]), (b"20", b"b"), (b"30", b"c")];
    assert_eq!(zadd(s, batch, key, entries), b":3\r\n");

    let mut out = Vec::new();
    s.sorted_set_rank(a![key, b"b"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.sorted_set_rank(a![key, b"missing"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    out.clear();
    s.sorted_set_rank(a![key, b"b", b"WITHSCORE"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n$2\r\n20\r\n");

    out.clear();
    s.sorted_set_rank(a![key, b"b"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanRemoveRangeByRankOrScore
#[test]
fn zremrangebyrank_and_byscore() {
  with_batch(|s, batch| {
    let key = b"zrem7";
    zadd(
      s,
      batch,
      key,
      &[
        (b"1", b"m1"),
        (b"2", b"m2"),
        (b"3", b"m3"),
        (b"4", b"m4"),
        (b"5", b"m5"),
      ],
    );

    let mut out = Vec::new();
    s.sorted_set_remove_range(a![key, b"0", b"1"], batch, &mut out, RemoveRangeKind::Rank)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.sorted_set_length(a![key], batch, &mut out).unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.sorted_set_remove_range(
      a![key, b"4", b"+inf"],
      batch,
      &mut out,
      RemoveRangeKind::Score,
    )
    .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.sorted_set_remove_range(
      a![key, b"-inf", b"+inf"],
      batch,
      &mut out,
      RemoveRangeKind::Score,
    )
    .unwrap();
    assert_eq!(out, b":1\r\n");

    // 删空自愈：元记录随空集合消亡
    out.clear();
    s.sorted_set_length(a![key], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// 非法 min/max → RESP_ERR_MIN_MAX_NOT_VALID_STRING（无句点，逐字节）
#[test]
fn zlexcount_and_invalid_lex_bounds() {
  with_batch(|s, batch| {
    let key = b"zlex7";
    zadd(
      s,
      batch,
      key,
      &[(b"0", b"alpha"), (b"0", b"beta"), (b"0", b"gamma")],
    );

    let mut out = Vec::new();
    s.sorted_set_length_by_value(a![key, b"[alpha", b"[beta"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.sorted_set_length_by_value(a![key, b"alpha", b"beta"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR min or max not valid string range item\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanPopMinWithCount
#[test]
fn zpopmin_with_count() {
  with_batch(|s, batch| {
    let key = b"zpop7";
    zadd(s, batch, key, &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

    let mut out = Vec::new();
    s.sorted_set_pop(a![key, b"2"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");

    out.clear();
    s.sorted_set_length(a![key], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.sorted_set_pop(a![b"missing7", b"2"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanGetRandomMember
#[test]
fn zrandmember_count_and_withscores() {
  with_batch(|s, batch| {
    let key = b"zrand7";
    zadd(
      s,
      batch,
      key,
      &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c"), (b"4", b"d")],
    );

    let mut out = Vec::new();
    s.sorted_set_random_member(a![key], batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"$1\r\n"));

    out.clear();
    s.sorted_set_random_member(a![key, b"10"], batch, &mut out)
      .unwrap();
    // count 超过基数 → 全量 member 数组（随机序，仅验长度）
    assert_eq!(out.len(), 32);
    assert!(out.starts_with(b"*4\r\n"));

    out.clear();
    s.sorted_set_random_member(a![key, b"2", b"WITHSCORES"], batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"*4\r\n"));
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZRangeStore（REV + LIMIT 组合）
#[test]
fn zrangestore_byscore_rev_limit() {
  with_batch(|s, batch| {
    zadd(
      s,
      batch,
      b"zsrc7",
      &[
        (b"1", b"a"),
        (b"2", b"b"),
        (b"3", b"c"),
        (b"4", b"d"),
        (b"5", b"e"),
      ],
    );

    let mut out = Vec::new();
    s.sorted_set_range_store(
      a![
        b"zdst7", b"zsrc7", b"(1", b"(5", b"BYSCORE", b"LIMIT", b"1", b"2",
      ],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.sorted_set_range(
      a![b"zdst7", b"0", b"-1", b"WITHSCORES"],
      batch,
      &mut out,
      SortedSetRangeOpts::NONE,
    )
    .unwrap();
    assert_eq!(out, b"*4\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$1\r\n4\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoBzPopMin（立即可取 + 缺键 null + 非法 timeout）
#[test]
fn bzpopmin_immediate_paths() {
  with_batch(|s, batch| {
    zadd(s, batch, b"bz7", &[(b"1.5", b"m")]);

    let mut out = Vec::new();
    s.sorted_set_blocking_pop(a![b"bz7", b"0"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*3\r\n$3\r\nbz7\r\n$1\r\nm\r\n$3\r\n1.5\r\n");

    out.clear();
    s.sorted_set_blocking_pop(a![b"nobz7", b"0"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    out.clear();
    s.sorted_set_blocking_pop(a![b"k", b"abc"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"-ERR timeout is not a float or out of range\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZUnionStoreWithWeightsAndAggregate
#[test]
fn zunionstore_weights_aggregate() {
  with_batch(|s, batch| {
    zadd(s, batch, b"zu1", &[(b"1", b"a"), (b"2", b"b")]);
    zadd(s, batch, b"zu2", &[(b"10", b"a"), (b"3", b"c")]);

    let mut out = Vec::new();
    s.sorted_set_union_store(
      a![
        b"zuout",
        b"2",
        b"zu1",
        b"zu2",
        b"WEIGHTS",
        b"2",
        b"1",
        b"AGGREGATE",
        b"SUM",
      ],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.sorted_set_range(
      a![b"zuout", b"0", b"-1", b"WITHSCORES"],
      batch,
      &mut out,
      SortedSetRangeOpts::NONE,
    )
    .unwrap();
    // a = 2*1 + 10 = 12；b = 4；c = 3
    assert_eq!(
      out,
      b"*6\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\na\r\n$2\r\n12\r\n"
    );
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZIncrBy 非浮点 → RESP_ERR_NOT_VALID_FLOAT
#[test]
fn zincrby_invalid_float() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.sorted_set_increment(a![b"zi7", b"abc", b"m"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not a valid float\r\n");
  });
}

/// round7 口径回归：ZMPOP numkeys 语义对齐 C#
/// （SortedSetCommands.cs ZMPOP —— numkeys<1 → NOT_INTEGER 含句点；
/// 参数不足容纳 numkeys+MIN/MAX → SYNTAX_ERROR）
#[test]
fn zmpop_numkeys_error_wording() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.sorted_set_m_pop(a![b"0", b"k1", b"MIN"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    out.clear();
    s.sorted_set_m_pop(a![b"2", b"k1", b"MIN"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR syntax error\r\n");
  });
}

/// round7 口径回归：GEOSEARCH COUNT 非整数 → 含句点文案
/// （SessionParseStateExtensions.cs:TryGetGeoSearchOptions COUNT 分支）
#[test]
fn geosearch_count_error_wording() {
  with_batch(|s, batch| {
    zadd(s, batch, b"geo7", &[(b"13.361389", b"Paris")]);

    let mut out = Vec::new();
    s.geo_search_commands(
      a![
        b"geo7",
        b"FROMLONLAT",
        b"13.36",
        b"38.11",
        b"BYRADIUS",
        b"100",
        b"km",
        b"COUNT",
        b"xyz",
      ],
      batch,
      &mut out,
      GeoSearchCommandKind::GeoSearch,
    )
    .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTests.cs:CanMoveItemBetweenSets
#[test]
fn smove_paths() {
  with_batch(|s, batch| {
    s.set_add(a![b"ss1", b"a"], batch, &mut Vec::new()).unwrap();
    s.set_add(a![b"ss1", b"b"], batch, &mut Vec::new()).unwrap();

    let mut out = Vec::new();
    s.set_move(a![b"ss1", b"ss2", b"a"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.set_is_member(a![b"ss2", b"a"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    s.set_is_member(a![b"ss1", b"a"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    // 源缺失 → :0
    out.clear();
    s.set_move(a![b"ssmiss", b"ss2", b"a"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTests.cs:CanDoSMisMember
#[test]
fn smismember_mixed_membership() {
  with_batch(|s, batch| {
    s.set_add(a![b"smi7", b"a"], batch, &mut Vec::new())
      .unwrap();
    s.set_add(a![b"smi7", b"b"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.set_multi_is_member(a![b"smi7", b"a", b"x", b"b"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*3\r\n:1\r\n:0\r\n:1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTests.cs:CanDoSRandMemberWithCount（负数去重全展开）
#[test]
fn srandmember_negative_count() {
  with_batch(|s, batch| {
    for m in [&b"m1"[..], b"m2", b"m3"] {
      s.set_add(a![b"sr7", m], batch, &mut Vec::new()).unwrap();
    }

    let mut out = Vec::new();
    s.set_random_member(a![b"sr7", b"-2"], batch, &mut out)
      .unwrap();
    // *2 数组头 + 2 × ("$2\r\n" + 成员 + \r\n 共 8B)
    assert_eq!(out.len(), 20);
    assert!(out.starts_with(b"*2\r\n"));

    // C# PickKRandomIndexes 负 count 不去重：|count|>基数时允许重复
    out.clear();
    s.set_random_member(a![b"sr7", b"-10"], batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"*10\r\n"));
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:CanDoLpushxRpushx（键缺失不物化空列表）
#[test]
fn lpushx_missing_key_no_materialize() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.list_push_x(a![b"lx7", b"v"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.list_length(a![b"lx7"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.list_push(a![b"lx7", b"v1"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    s.list_push_x(a![b"lx7", b"v2"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":2\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:CanDoLpopCount（截断 / 超量 / 弹空）
#[test]
fn lpop_with_count() {
  with_batch(|s, batch| {
    s.list_push(
      a![b"lp7", b"v1", b"v2", b"v3"],
      batch,
      &mut Vec::new(),
      true,
    )
    .unwrap();
    // LPUSH 头插后存储序 v3 v2 v1

    let mut out = Vec::new();
    s.list_pop(a![b"lp7", b"2"], batch, &mut out, true).unwrap();
    assert_eq!(out, b"*2\r\n$2\r\nv3\r\n$2\r\nv2\r\n");

    // C# ListPop 仅在 count>1 时写数组头，单元素仍为裸 bulk string
    out.clear();
    s.list_pop(a![b"lp7", b"99"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"$2\r\nv1\r\n");

    out.clear();
    s.list_pop(a![b"lp7", b"2"], batch, &mut out, true).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:CanDoLset（ERR index out of range）
#[test]
fn lset_out_of_range() {
  with_batch(|s, batch| {
    s.list_push(a![b"ls7", b"a", b"b"], batch, &mut Vec::new(), true)
      .unwrap();

    let mut out = Vec::new();
    s.list_set(a![b"ls7", b"0", b"x"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    s.list_set(a![b"ls7", b"9", b"y"], batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR index out of range\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:CanDoLpos（round5 rank 语义回归）
#[test]
fn lpos_count_zero_all_matches() {
  with_batch(|s, batch| {
    // LPUSH a b a c → 存储 c a b a
    s.list_push(
      a![b"lpos7", b"a", b"b", b"a", b"c"],
      batch,
      &mut Vec::new(),
      true,
    )
    .unwrap();

    let mut out = Vec::new();
    s.list_position(a![b"lpos7", b"a", b"COUNT", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n:3\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:CanDoLtrim
#[test]
fn ltrim_negative_bounds() {
  with_batch(|s, batch| {
    s.list_push(
      a![b"lt7", b"a", b"b", b"c", b"d"],
      batch,
      &mut Vec::new(),
      true,
    )
    .unwrap();
    // 存储 d c b a

    let mut out = Vec::new();
    s.list_trim(a![b"lt7", b"-2", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    s.list_range(a![b"lt7", b"0", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nb\r\n$1\r\na\r\n");
  });
}

/// test/standalone/Garnet.test/KeyAdminTests.cs:CanDoExpireWithOptions
#[test]
fn expire_options_nx_gt_lt() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(a![b"ex7", b"v"], batch, &mut out).unwrap();

    // NX：无 TTL 时成功，已有 TTL 时拒绝
    out.clear();
    s.network_expire(
      ExpireCmd::Expire,
      a![b"ex7", b"100", b"NX"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.network_expire(
      ExpireCmd::Expire,
      a![b"ex7", b"100", b"NX"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":0\r\n");

    // GT：新 TTL 不大于现有 → :0
    out.clear();
    s.network_expire(ExpireCmd::Expire, a![b"ex7", b"50", b"GT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // LT：新 TTL 小于现有 → :1
    out.clear();
    s.network_expire(ExpireCmd::Expire, a![b"ex7", b"10", b"LT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // NX+XX 不兼容
    out.clear();
    s.network_expire(
      ExpireCmd::Expire,
      a![b"ex7", b"10", b"NX", b"XX"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(
      out,
      b"-ERR NX and XX, GT or LT options at the same time are not compatible\r\n"
    );

    // 负过期 → INVALID_EXPIRE_TIME
    out.clear();
    s.network_expire(ExpireCmd::Expire, a![b"ex7", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR invalid expire time, must be >= 0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespHashTests.cs:CanDoHashIncrement（ERR hash value is not an integer.）
#[test]
fn hincrby_non_integer_field() {
  with_batch(|s, batch| {
    s.hash_set(a![b"h7", b"f", b"str"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    s.hash_increment(a![b"h7", b"f", b"5"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b"-ERR hash value is not an integer.\r\n");
  });
}

use wnode::{
  objects::sortedset::sorted_set_object::SortedSetRangeOpts,
  resp::resp_server_session::RespServerSession,
};
