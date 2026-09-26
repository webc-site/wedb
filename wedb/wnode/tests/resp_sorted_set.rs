use core::str;
use std::{mem::take, sync::Arc};

use compio::runtime::Runtime;
use wcol::{
  types::member_ttl::encode_member,
  zset::sorted_set_object::{SortedSetOperation, SortedSetRangeOpts},
};
use wnode::{
  RespSessionConsumer,
  resp::{
    RespServerSession,
    garnet_api::{GarnetApi, StoreGarnetApi},
    objects::{
      sorted_set_commands::RemoveRangeKind, sorted_set_geo_commands::GeoSearchCommandKind,
    },
    resp_server_session::RespServerSessionOptions,
    slow_path::SlowWait,
  },
};
use wnode_test::{err_frame, roundtrip, with_batch};
use wresp::{
  cmd_strings::{
    RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_MIN_MAX_NOT_VALID_FLOAT, RESP_ERR_WRONG_TYPE,
  },
  command::RespCommand,
};
use wtest_base::{a, open_test_store};
use wval::GarnetObjectType;

fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
  let mut items = Vec::new();
  let mut pos = match frame.iter().position(|&b| b == b'\n') {
    Some(p) => p + 1,
    None => return items,
  };
  while pos < frame.len() {
    if frame[pos] != b'$' {
      break;
    }
    let len_end = frame[pos..].iter().position(|&b| b == b'\n').unwrap() + pos;
    let len: usize = str::from_utf8(&frame[pos + 1..len_end - 1])
      .unwrap()
      .parse()
      .unwrap();
    let start = len_end + 1;
    items.push(frame[start..start + len].to_vec());
    pos = start + len + 2;
  }
  items
}

const ENTRIES: &[(&[u8], &[u8])] = &[
  (b"a", b"1"),
  (b"b", b"2"),
  (b"c", b"3"),
  (b"d", b"4"),
  (b"e", b"5"),
  (b"f", b"6"),
  (b"g", b"7"),
  (b"h", b"8"),
  (b"i", b"9"),
  (b"j", b"10"),
];

const LEADERBOARD: &[(&[u8], &[u8])] = &[
  (b"Dave", b"340"),
  (b"Kendra", b"400"),
  (b"Tom", b"560"),
  (b"Barbara", b"650"),
  (b"Jennifer", b"690"),
  (b"Peter", b"690"),
  (b"Frank", b"740"),
  (b"Lester", b"790"),
  (b"Alice", b"850"),
  (b"Mary", b"980"),
];

/// lex 空串/非法词形错误帧逐字节钉（deviations §138，对 CmdStrings.cs:273 无句点尾）
const ERR_LEX_BOUNDS: &[u8] = b"-ERR min or max not valid string range item\r\n";

/// 慢臂直驱 RESP2 帧（与快臂同版）
const RESP_V2: u8 = 2;

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:AddAndLength
#[test]
fn add_and_length() {
  with_batch(|s, batch| {
    let key = b"SortedSet_Add";
    let mut args: Vec<&[u8]> = vec![key];
    for &(member, score) in ENTRIES {
      args.push(score);
      args.push(member);
    }
    let mut out = Vec::new();
    s.sorted_set_add(&args, batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    out.clear();
    s.sorted_set_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    out.clear();
    s.sorted_set_add(&[key, b"11", b"k"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.sorted_set_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":11\r\n");

    out.clear();
    s.sorted_set_add(&[key, b"12", b"a"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.network_del(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.network_exists(&[key], batch, None, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:AddWithOptions
#[test]
fn add_with_options() {
  with_batch(|s, batch| {
    let key = b"SortedSet_Add";
    let mut args: Vec<&[u8]> = vec![key];
    for &(member, score) in ENTRIES {
      args.push(score);
      args.push(member);
    }
    let mut out = Vec::new();
    s.sorted_set_add(&args, batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    // XX - Only update elements that already exist. Don't add new elements.
    out.clear();
    s.sorted_set_add(
      &[key, b"XX", b"3", b"a", b"4", b"b", b"11", b"k", b"12", b"l"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.sorted_set_score(&[key, b"a"], batch, &mut out).unwrap();
    assert_eq!(out, b"$1\r\n3\r\n");

    out.clear();
    s.sorted_set_score(&[key, b"b"], batch, &mut out).unwrap();
    assert_eq!(out, b"$1\r\n4\r\n");

    out.clear();
    s.sorted_set_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CandDoZIncrby
#[test]
fn cand_do_z_incrby() {
  with_batch(|s, batch| {
    let key = b"LeaderBoard";
    let mut args: Vec<&[u8]> = vec![key];
    for &(member, score) in LEADERBOARD {
      args.push(score);
      args.push(member);
    }
    let mut out = Vec::new();
    s.sorted_set_add(&args, batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    out.clear();
    s.sorted_set_increment(&[key, b"90", b"Tom"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$3\r\n650\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanManageNotExistingKeySE
#[test]
fn can_manage_not_existing_key_se() {
  with_batch(|s, batch| {
    let mut out = Vec::new();

    // ZCOUNT
    s.sorted_set_count(&[b"nokey", b"1", b"3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // ZLEXCOUNT
    out.clear();
    s.sorted_set_length_by_value(&[b"nokey", b"-", b"+"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // ZCARD
    out.clear();
    s.sorted_set_length(&[b"nokey"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CheckEmptySortedSetKeyRemoved
#[test]
fn check_empty_sorted_set_key_removed() {
  with_batch(|s, batch| {
    let key = b"user1:sortedset";
    let mut args: Vec<&[u8]> = vec![key];
    for &(member, score) in ENTRIES {
      args.push(score);
      args.push(member);
    }
    let mut out = Vec::new();
    s.sorted_set_add(&args, batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    out.clear();
    s.sorted_set_pop(&[key, b"10"], batch, &mut out, false)
      .unwrap();
    assert!(out.starts_with(b"*20\r\n"));

    out.clear();
    s.network_exists(&[key], batch, None, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CheckSortedSetOperationsOnWrongTypeObjectSE
#[test]
fn check_sorted_set_operations_on_wrong_type_object_se() {
  with_batch(|s, batch| {
    let wrongtype = err_frame(RESP_ERR_WRONG_TYPE);
    let mut out = Vec::new();

    // Set up Set object
    s.set_add(&[b"user1:obj1", b"Hello", b"World"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // ZADD
    out.clear();
    s.sorted_set_add(&[b"user1:obj1", b"1.1", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, wrongtype);

    // ZCARD
    out.clear();
    s.sorted_set_length(&[b"user1:obj1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, wrongtype);

    // ZSCORE
    out.clear();
    s.sorted_set_score(&[b"user1:obj1", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, wrongtype);

    // ZREM
    out.clear();
    s.sorted_set_remove(&[b"user1:obj1", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, wrongtype);

    // ZCOUNT
    out.clear();
    s.sorted_set_count(&[b"user1:obj1", b"1", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, wrongtype);

    // ZINCRBY
    out.clear();
    s.sorted_set_increment(&[b"user1:obj1", b"2.2", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, wrongtype);

    // ZRANK
    out.clear();
    s.sorted_set_rank(&[b"user1:obj1", b"Hello"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, wrongtype);
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZDiff
#[test]
fn can_do_z_diff() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.sorted_set_add(
      &[
        b"key1", b"2", b"A", b"3", b"B", b"3", b"C", b"5", b"D", b"8", b"!",
      ],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":5\r\n");

    out.clear();
    s.sorted_set_add(
      &[b"key2", b"5", b"B", b"1", b"D", b"7", b"M"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.sorted_set_difference(&[b"2", b"key1", b"key2"], batch, &mut out)
      .unwrap();
    let diff = parse_bulk_array(&out);
    assert_eq!(diff, vec![b"A".to_vec(), b"C".to_vec(), b"!".to_vec()]);

    out.clear();
    s.sorted_set_difference(&[b"2", b"key1", b"key2", b"WITHSCORES"], batch, &mut out)
      .unwrap();
    let diff_scores = parse_bulk_array(&out);
    assert_eq!(
      diff_scores,
      vec![
        b"A".to_vec(),
        b"2".to_vec(),
        b"C".to_vec(),
        b"3".to_vec(),
        b"!".to_vec(),
        b"8".to_vec()
      ]
    );

    // With only one key
    out.clear();
    s.sorted_set_difference(&[b"1", b"key1"], batch, &mut out)
      .unwrap();
    let single = parse_bulk_array(&out);
    assert_eq!(
      single,
      vec![
        b"A".to_vec(),
        b"B".to_vec(),
        b"C".to_vec(),
        b"D".to_vec(),
        b"!".to_vec()
      ]
    );
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZInterWithSE
#[test]
fn can_do_z_inter_with_se() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.sorted_set_add(
      &[b"zset1", b"1", b"one", b"2", b"two", b"3", b"three"],
      batch,
      &mut out,
    )
    .unwrap();
    s.sorted_set_add(
      &[b"zset2", b"1", b"one", b"2", b"two", b"4", b"four"],
      batch,
      &mut out,
    )
    .unwrap();
    s.sorted_set_add(
      &[b"zset3", b"1", b"one", b"3", b"three", b"5", b"five"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.sorted_set_intersect(&[b"2", b"zset1", b"zset2"], batch, &mut out)
      .unwrap();
    let inter2 = parse_bulk_array(&out);
    assert_eq!(inter2, vec![b"one".to_vec(), b"two".to_vec()]);

    out.clear();
    s.sorted_set_intersect(&[b"3", b"zset1", b"zset2", b"zset3"], batch, &mut out)
      .unwrap();
    let inter3 = parse_bulk_array(&out);
    assert_eq!(inter3, vec![b"one".to_vec()]);

    out.clear();
    s.sorted_set_intersect(&[b"2", b"zset1", b"zset2", b"WITHSCORES"], batch, &mut out)
      .unwrap();
    let inter_scores = parse_bulk_array(&out);
    assert_eq!(
      inter_scores,
      vec![
        b"one".to_vec(),
        b"2".to_vec(),
        b"two".to_vec(),
        b"4".to_vec()
      ]
    );
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZInterCardWithSE
#[test]
fn can_do_z_inter_card_with_se() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.sorted_set_add(
      &[b"zset1", b"1", b"one", b"2", b"two", b"3", b"three"],
      batch,
      &mut out,
    )
    .unwrap();
    s.sorted_set_add(
      &[b"zset2", b"1", b"one", b"2", b"two", b"4", b"four"],
      batch,
      &mut out,
    )
    .unwrap();
    s.sorted_set_add(
      &[b"zset3", b"1", b"one", b"3", b"three", b"5", b"five"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.sorted_set_intersect_length(&[b"2", b"zset1", b"zset2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.sorted_set_intersect_length(&[b"3", b"zset1", b"zset2", b"zset3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.sorted_set_intersect_length(&[b"2", b"zset1", b"zset2", b"LIMIT", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZInterStoreWithSE
#[test]
fn can_do_z_inter_store_with_se() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.sorted_set_add(
      &[b"zset1", b"1", b"one", b"2", b"two", b"3", b"three"],
      batch,
      &mut out,
    )
    .unwrap();
    s.sorted_set_add(
      &[b"zset2", b"1", b"one", b"2", b"two", b"4", b"four"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.sorted_set_intersect_store(&[b"out", b"2", b"zset1", b"zset2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.sorted_set_length(&[b"out"], batch, &mut out).unwrap();
    assert_eq!(out, b":2\r\n");
  });
}

/// 集合运算 NaN 分值恒归 +0（doc/zh/deviations.md §2 拒 NaN 入集 +
/// libs/server/Storage/Session/ObjectStore/SortedSetOps.cs 的 SortedSetIntersection
/// :1572-1577 的 IsNaN→0，Redis bug-compatible；归五点位对位工单枚举的乘积与聚合产点）。
/// inf 词形与结果排序口径对位 test/standalone/Garnet.test.collections/RespSortedSetTests.cs:
/// AddWithInfinity / CanDoZInterWithSE
#[test]
fn combine_nan_score_always_truncated_to_zero() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    // zP: x=+inf, y=1 / zN: x=-inf, z=2
    s.sorted_set_add(&[b"zP", b"+inf", b"x", b"1", b"y"], batch, &mut out)
      .unwrap();
    out.clear();
    s.sorted_set_add(&[b"zN", b"-inf", b"x", b"2", b"z"], batch, &mut out)
      .unwrap();
    out.clear();

    // 交集 SUM 相反符号 ±inf 相加 → NaN 逐步截 0（直对 C# :1574-1577）：交集仅 x，分值 0
    s.sorted_set_intersect(&[b"2", b"zP", b"zN", b"WITHSCORES"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nx\r\n$1\r\n0\r\n");

    out.clear();
    // 并集同口径：x 的 ±inf 求和 NaN 截 0，y/z 正常分值与集序不回归
    s.sorted_set_union(&[b"2", b"zP", b"zN", b"WITHSCORES"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"*6\r\n$1\r\nx\r\n$1\r\n0\r\n$1\r\ny\r\n$1\r\n1\r\n$1\r\nz\r\n$1\r\n2\r\n"
    );

    out.clear();
    // 交集：0×±inf 的加权 NaN 在 seed 与逐步聚合处均归 0
    s.sorted_set_intersect(
      &[b"2", b"zP", b"zN", b"WEIGHTS", b"0", b"0", b"WITHSCORES"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nx\r\n$1\r\n0\r\n");

    out.clear();
    // 并集：0×±inf 的加权 NaN 与 0×有限分一同落 0
    s.sorted_set_union(
      &[b"2", b"zP", b"zN", b"WEIGHTS", b"0", b"0", b"WITHSCORES"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(
      out,
      b"*6\r\n$1\r\nx\r\n$1\r\n0\r\n$1\r\ny\r\n$1\r\n0\r\n$1\r\nz\r\n$1\r\n0\r\n"
    );

    out.clear();
    // 单键交集：加权 seed 的 NaN 亦归零（C# 在 keys.Length == 1 处 :1536-1539 早返不截，
    // 本仓按 §2 恒拒 NaN 入集，属已登记刻意偏差）
    s.sorted_set_intersect(
      &[b"1", b"zP", b"WEIGHTS", b"0", b"WITHSCORES"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*4\r\n$1\r\nx\r\n$1\r\n0\r\n$1\r\ny\r\n$1\r\n0\r\n");

    out.clear();
    // MIN/MAX：seed=+inf×0 归 0 后与 -inf×1 取真实极值，结果无 NaN
    for (agg, want) in [
      (b"MIN".as_slice(), &b"*2\r\n$1\r\nx\r\n$4\r\n-inf\r\n"[..]),
      (b"MAX".as_slice(), &b"*2\r\n$1\r\nx\r\n$1\r\n0\r\n"[..]),
    ] {
      s.sorted_set_intersect(
        &[
          b"2",
          b"zP",
          b"zN",
          b"WEIGHTS",
          b"0",
          b"1",
          b"AGGREGATE",
          agg,
          b"WITHSCORES",
        ],
        batch,
        &mut out,
      )
      .unwrap();
      assert_eq!(out, want, "{:?} 归一口径不符", agg);
      out.clear();
    }

    // 非 NaN 输入下 MIN/MAX 取真实极值，不属截断面
    s.sorted_set_intersect(
      &[b"2", b"zP", b"zN", b"AGGREGATE", b"MIN", b"WITHSCORES"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nx\r\n$4\r\n-inf\r\n");

    out.clear();
    s.sorted_set_intersect(
      &[b"2", b"zP", b"zN", b"AGGREGATE", b"MAX", b"WITHSCORES"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nx\r\n$3\r\ninf\r\n");

    out.clear();
    // STORE 落盘闭环：ZINTERSTORE 截断后的 0 分值入树，ZSCORE/ZCOUNT 读取正常
    s.sorted_set_intersect_store(&[b"dstI", b"2", b"zP", b"zN"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.sorted_set_score(&[b"dstI", b"x"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\n0\r\n");

    out.clear();
    s.sorted_set_count(&[b"dstI", b"-inf", b"+inf"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    // 并集 STORE 落盘闭环：加权 0×±inf 的 NaN 与 0×有限分全部归 0 入树，
    // 同分按成员字典序输出
    s.sorted_set_union_store(
      &[b"dstU", b"2", b"zP", b"zN", b"WEIGHTS", b"0", b"0"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.sorted_set_range(
      &[b"dstU", b"0", b"-1", b"WITHSCORES"],
      batch,
      &mut out,
      SortedSetRangeOpts::NONE,
    )
    .unwrap();
    assert_eq!(
      parse_bulk_array(&out),
      vec![
        b"x".to_vec(),
        b"0".to_vec(),
        b"y".to_vec(),
        b"0".to_vec(),
        b"z".to_vec(),
        b"0".to_vec()
      ]
    );
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZInterStoreWithBadNumKeysLC
#[test]
fn can_do_zinterstore_with_bad_num_keys_lc() {
  with_batch(|s, batch| {
    let mut out = Vec::new();

    // numkeys = -1
    s.sorted_set_intersect_store(&[b"dest", b"-1", b"zset1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR at least 1 input key is needed for 'ZINTERSTORE' command\r\n"
    );

    // numkeys = 0
    out.clear();
    s.sorted_set_intersect_store(&[b"dest", b"0", b"zset1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR at least 1 input key is needed for 'ZINTERSTORE' command\r\n"
    );

    // numkeys 超过实际提供 key 数量：提供 1 个 key，但 numkeys = 2
    out.clear();
    s.sorted_set_intersect_store(&[b"dest", b"2", b"zset1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // numkeys 超过实际提供 key 数量：提供 2 个 key，但 numkeys = 3
    out.clear();
    s.sorted_set_intersect_store(&[b"dest", b"3", b"zset1", b"zset2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // 大数值 numkeys，防止溢出检查失效
    out.clear();
    s.sorted_set_intersect_store(&[b"dest", b"2147483647", b"zset1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    out.clear();
    s.sorted_set_intersect_store(&[b"dest", b"2147483646", b"zset1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // ZUNIONSTORE 对齐验证
    out.clear();
    s.sorted_set_union_store(&[b"dest", b"-1", b"zset1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR at least 1 input key is needed for 'ZUNIONSTORE' command\r\n"
    );

    out.clear();
    s.sorted_set_union_store(&[b"dest", b"0", b"zset1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR at least 1 input key is needed for 'ZUNIONSTORE' command\r\n"
    );

    out.clear();
    s.sorted_set_union_store(&[b"dest", b"2", b"zset1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));
  });
}

/// STORE 族语法门禁：ZDIFFSTORE/ZINTERSTORE/ZUNIONSTORE 协议契约恒为整数基数应答，
/// 绝无 WITHSCORES 选项；携带即 -ERR syntax error 且目标键零写入。
/// 对标 libs/server/Resp/Objects/SortedSetCommands.cs 的 SortedSetDifferenceStore :1023
/// （Count - 2 != nKeys 恒报语法错误）与 IntersectStore :1282-1322 /
/// UnionStore :1506-1545（选项循环仅识别 WEIGHTS/AGGREGATE，其他词元恒报语法错误）
#[test]
fn combine_store_rejects_withscores() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.sorted_set_add(&[b"k", b"1", b"a", b"2", b"b"], batch, &mut out)
      .unwrap();

    // ZDIFFSTORE dst 1 k WITHSCORES → 语法错误
    out.clear();
    s.sorted_set_difference_store(&[b"dst", b"1", b"k", b"WITHSCORES"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // ZINTERSTORE dst 1 k WITHSCORES → 语法错误
    out.clear();
    s.sorted_set_intersect_store(&[b"dst", b"1", b"k", b"WITHSCORES"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // ZUNIONSTORE dst 1 k WITHSCORES → 语法错误
    out.clear();
    s.sorted_set_union_store(&[b"dst", b"1", b"k", b"WITHSCORES"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // 目标键 dst 均未被静默写入
    out.clear();
    s.network_exists(&[b"dst"], batch, None, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    // 既有目标键不被畸形命令覆盖：dst 先入集，错误后基数不变
    out.clear();
    s.sorted_set_add(&[b"dst2", b"9", b"old"], batch, &mut out)
      .unwrap();
    out.clear();
    s.sorted_set_union_store(
      &[
        b"dst2",
        b"1",
        b"k",
        b"WEIGHTS",
        b"1",
        b"AGGREGATE",
        b"SUM",
        b"WITHSCORES",
      ],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));
    out.clear();
    s.sorted_set_length(&[b"dst2"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    // 对照组：非 STORE 的 ZDIFF/ZINTER/ZUNION WITHSCORES 合法路径不回归
    out.clear();
    s.sorted_set_difference(&[b"1", b"k", b"WITHSCORES"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoSortedSetCommandsWithBadNumKeysLC
#[test]
fn can_do_sorted_set_commands_with_bad_num_keys_lc() {
  with_batch(|s, batch| {
    for num_keys in [b"2147483647", b"2147483646"] {
      let mut out = Vec::new();
      s.sorted_set_m_pop(&[num_keys.as_slice(), b"zset1", b"MIN"], batch, &mut out)
        .unwrap();
      assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

      out.clear();
      s.sorted_set_union(&[num_keys.as_slice(), b"zset1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

      out.clear();
      s.sorted_set_union_store(&[b"dest", num_keys.as_slice(), b"zset1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

      out.clear();
      s.sorted_set_intersect(&[num_keys.as_slice(), b"zset1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

      out.clear();
      s.sorted_set_intersect_length(&[num_keys.as_slice(), b"zset1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));
    }

    let mut out = Vec::new();
    let ok = s
      .network_command_getkeys(&[b"ZUNION", b"2147483647", b"zset1"], batch, &mut out)
      .unwrap();
    assert!(ok);
    assert_eq!(out, b"*1\r\n$5\r\nzset1\r\n");

    // 回归：ZDIFF numkeys = 0 边界安全校验（严防 1..=0 切片越界 panic）
    out.clear();
    s.sorted_set_difference(&[b"0"], batch, &mut out).unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ZDIFF' command\r\n"
    );

    out.clear();
    s.sorted_set_difference(&[b"0", b"WITHSCORES"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZMScore
#[test]
fn can_do_z_m_score() {
  with_batch(|s, batch| {
    let key = b"SortedSet_GetMemberScores";
    let mut args: Vec<&[u8]> = vec![key];
    for &(member, score) in ENTRIES {
      args.push(score);
      args.push(member);
    }
    let mut out = Vec::new();
    s.sorted_set_add(&args, batch, &mut out).unwrap();

    out.clear();
    s.sorted_set_scores(&[key, b"a", b"b", b"z", b"i"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*4\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n$1\r\n9\r\n");

    out.clear();
    s.sorted_set_scores(&[b"nokey", b"a", b"b", b"c"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*3\r\n$-1\r\n$-1\r\n$-1\r\n");

    out.clear();
    s.sorted_set_scores(&[b"nokey"], batch, &mut out).unwrap();
    assert!(out.starts_with(b"-ERR wrong number of arguments for 'ZMSCORE'"));
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:AddRemove
#[test]
fn add_remove() {
  with_batch(|s, batch| {
    let key = b"SortedSet_AddRemove";
    let mut args: Vec<&[u8]> = vec![key];
    for &(member, score) in ENTRIES {
      args.push(score);
      args.push(member);
    }
    let mut out = Vec::new();
    s.sorted_set_add(&args, batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    out.clear();
    s.sorted_set_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    // Remove all 10 entries
    let mut rem_args: Vec<&[u8]> = vec![key];
    for &(member, _) in ENTRIES {
      rem_args.push(member);
    }
    out.clear();
    s.sorted_set_remove(&rem_args, batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    out.clear();
    s.sorted_set_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.network_exists(&[key], batch, None, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:AddPopDesc
#[test]
fn add_pop_desc() {
  with_batch(|s, batch| {
    let key = b"SortedSet_AddPop";
    let mut args: Vec<&[u8]> = vec![key];
    for &(member, score) in ENTRIES {
      args.push(score);
      args.push(member);
    }
    let mut out = Vec::new();
    s.sorted_set_add(&args, batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    // ZPOPMAX single
    out.clear();
    s.sorted_set_pop(&[key], batch, &mut out, false).unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nj\r\n$2\r\n10\r\n");

    out.clear();
    s.sorted_set_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":9\r\n");

    // ZPOPMAX count 2
    out.clear();
    s.sorted_set_pop(&[key, b"2"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b"*4\r\n$1\r\ni\r\n$1\r\n9\r\n$1\r\nh\r\n$1\r\n8\r\n");

    out.clear();
    s.sorted_set_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":7\r\n");

    // ZPOPMAX count 999
    out.clear();
    s.sorted_set_pop(&[key, b"999"], batch, &mut out, false)
      .unwrap();
    assert!(out.starts_with(b"*14\r\n"));

    out.clear();
    s.sorted_set_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.network_exists(&[key], batch, None, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanCreateLeaderBoard
#[test]
fn can_create_leader_board() {
  with_batch(|s, batch| {
    let key = b"LeaderBoard";
    let mut args: Vec<&[u8]> = vec![key];
    for &(member, score) in LEADERBOARD {
      args.push(score);
      args.push(member);
    }
    let mut out = Vec::new();
    s.sorted_set_add(&args, batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    out.clear();
    s.sorted_set_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanGetScoresZCount
#[test]
fn can_get_scores_z_count() {
  with_batch(|s, batch| {
    let key = b"LeaderBoard";
    let mut args: Vec<&[u8]> = vec![key];
    for &(member, score) in LEADERBOARD {
      args.push(score);
      args.push(member);
    }
    let mut out = Vec::new();
    s.sorted_set_add(&args, batch, &mut out).unwrap();
    assert_eq!(out, b":10\r\n");

    out.clear();
    s.sorted_set_count(&[key, b"500", b"700"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":4\r\n");

    out.clear();
    s.sorted_set_count(&[key, b"-inf", b"+inf"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":10\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoSortedSetExpireAndRemove
#[test]
fn can_do_sorted_set_expire_and_remove() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.sorted_set_add(
      &[b"mysortedset", b"1.1", b"a1", b"1.2", b"a2", b"1.3", b"a3"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.sorted_set_expire(
      "ZEXPIRE",
      &[b"mysortedset", b"60", b"MEMBERS", b"1", b"a1"],
      batch,
      &mut out,
      false,
      false,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:1\r\n");

    out.clear();
    s.sorted_set_remove_range(
      &[b"mysortedset", b"[a", b"(b"],
      batch,
      &mut out,
      RemoveRangeKind::Lex,
    )
    .unwrap();
    assert_eq!(out, b":3\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetGeoTests.cs:CanUseGeoAdd
#[test]
fn can_use_geo_add() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.geo_add(
      &[
        b"cities",
        b"-122.4194",
        b"37.7749",
        b"sf",
        b"2.3522",
        b"48.8566",
        b"paris",
      ],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":2\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetGeoTests.cs:CanUseGeoPos
#[test]
fn can_use_geo_pos() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.geo_add(
      &[b"Sicily", b"13.361389", b"38.115556", b"Palermo"],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.geo_commands(
      &[b"Sicily", b"Palermo", b"Unknown"],
      batch,
      &mut out,
      SortedSetOperation::Geopos,
    )
    .unwrap();
    let payload = String::from_utf8_lossy(&out);
    assert!(payload.contains("13.36138"), "{payload}");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetGeoTests.cs:CanUseGeoHash
#[test]
fn can_use_geo_hash() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.geo_add(
      &[
        b"Sicily",
        b"13.361389",
        b"38.115556",
        b"Palermo",
        b"15.087269",
        b"37.502669",
        b"Catania",
      ],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.geo_commands(
      &[b"Sicily", b"Palermo", b"Catania", b"Unknown"],
      batch,
      &mut out,
      SortedSetOperation::Geohash,
    )
    .unwrap();
    assert_eq!(
      out,
      b"*3\r\n$11\r\nsqc8b49rny0\r\n$11\r\nsqdtr74hyu0\r\n$-1\r\n"
    );
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetGeoTests.cs:CanUseGeoDist
#[test]
fn can_use_geo_dist() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.geo_add(
      &[
        b"Sicily",
        b"13.361389",
        b"38.115556",
        b"Palermo",
        b"15.087269",
        b"37.502669",
        b"Catania",
      ],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.geo_commands(
      &[b"Sicily", b"Palermo", b"Catania", b"km"],
      batch,
      &mut out,
      SortedSetOperation::Geodist,
    )
    .unwrap();
    let dist: f64 = String::from_utf8_lossy(&out)
      .lines()
      .nth(1)
      .unwrap()
      .parse()
      .unwrap();
    assert!((dist - 166.27).abs() < 1.0);
  });
}

/// GEODIST 单位校验对位：对标 C# GeoCommands 为 Count>3 即校验第 4 参单位、
/// 失败报 RESP_ERR_NOT_VALID_GEO_DISTANCE_UNIT，len>=5 不跳过；少于三成员
/// 报 wrong number of arguments（paramsRequiredInCommand=3）
#[test]
fn geo_dist_unit_validation() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.geo_add(
      &[
        b"Sicily",
        b"13.361389",
        b"38.115556",
        b"Palermo",
        b"15.087269",
        b"37.502669",
        b"Catania",
      ],
      batch,
      &mut out,
    )
    .unwrap();

    // len==4：单位非法
    out.clear();
    s.geo_commands(
      &[b"Sicily", b"Palermo", b"Catania", b"sm"],
      batch,
      &mut out,
      SortedSetOperation::Geodist,
    )
    .unwrap();
    assert_eq!(
      out,
      b"-ERR unsupported unit provided. please use M, KM, FT, MI\r\n"
    );

    // len==5：多余参数不跳过单位校验（修复前 len>4 静默按米）
    out.clear();
    s.geo_commands(
      &[b"Sicily", b"Palermo", b"Catania", b"sm", b"extra"],
      batch,
      &mut out,
      SortedSetOperation::Geodist,
    )
    .unwrap();
    assert_eq!(
      out,
      b"-ERR unsupported unit provided. please use M, KM, FT, MI\r\n"
    );

    // 少于三成员：wrong number of arguments（修复前穿透对象层参数越界）。
    // 命令名逐字为 "command"：C# Resp/Objects/SortedSetGeoCommands.cs:119 `var cmd =
    // nameof(command)` 取的是形参标识符（:136 AbortWithWrongNumberOfArguments(cmd)
    // → Resp/CmdStrings.cs:325 GenericErrWrongNumArgs 回填），GEODIST/GEOHASH/
    // GEOPOS 三命令同文案
    out.clear();
    s.geo_commands(
      &[b"Sicily", b"Palermo"],
      batch,
      &mut out,
      SortedSetOperation::Geodist,
    )
    .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'command' command\r\n"
    );
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetGeoTests.cs:CanUseGeoSearch
#[test]
fn can_use_geo_search() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.geo_add(
      &[
        b"pts",
        b"-122.4194",
        b"37.7749",
        b"sf",
        b"-118.2437",
        b"34.0522",
        b"la",
      ],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.geo_search_commands(
      &[
        b"pts",
        b"FROMLONLAT",
        b"-122.4194",
        b"37.7749",
        b"BYRADIUS",
        b"100",
        b"km",
        b"WITHDIST",
      ],
      batch,
      &mut out,
      GeoSearchCommandKind::GeoSearch,
    )
    .unwrap();
    let payload = String::from_utf8_lossy(&out);
    assert!(payload.contains("sf"), "{payload}");
    assert!(!payload.contains("la"), "{payload}");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetGeoTests.cs:CanUseGeoSearchStore
#[test]
fn can_use_geo_search_store() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.geo_add(
      &[
        b"pts",
        b"-122.4194",
        b"37.7749",
        b"sf",
        b"2.3522",
        b"48.8566",
        b"paris",
      ],
      batch,
      &mut out,
    )
    .unwrap();

    out.clear();
    s.geo_search_commands(
      &[
        b"store",
        b"pts",
        b"FROMLONLAT",
        b"-122.4194",
        b"37.7749",
        b"BYRADIUS",
        b"100",
        b"km",
      ],
      batch,
      &mut out,
      GeoSearchCommandKind::GeoSearchStore,
    )
    .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetGeoTests.cs:CanUseGeoSearchStoreWithDeleteKeyWhenSourceNotFound
#[test]
fn can_use_geo_search_store_with_delete_key_when_source_not_found() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.sorted_set_add(&[b"newCities", b"10", b"OldValue"], batch, &mut out)
      .unwrap();

    out.clear();
    s.geo_search_commands(
      &[
        b"newCities",
        b"missing_source",
        b"FROMLONLAT",
        b"0",
        b"0",
        b"BYRADIUS",
        b"100",
        b"km",
      ],
      batch,
      &mut out,
      GeoSearchCommandKind::GeoSearchStore,
    )
    .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.network_exists(&[b"newCities"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetGeoTests.cs:CheckGeoSortedSetOperationsOnWrongTypeObjectSE
#[test]
fn check_geo_sorted_set_operations_on_wrong_type_object_se() {
  with_batch(|s, batch| {
    let wrongtype = err_frame(RESP_ERR_WRONG_TYPE);
    let mut out = Vec::new();

    s.set_add(&[b"user1:obj1", b"Tel Aviv", b"Haifa"], batch, &mut out)
      .unwrap();

    out.clear();
    s.geo_add(
      &[b"user1:obj1", b"2.0853", b"34.7818", b"Tel Aviv"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, wrongtype);

    out.clear();
    s.geo_commands(
      &[b"user1:obj1", b"Tel Aviv"],
      batch,
      &mut out,
      SortedSetOperation::Geopos,
    )
    .unwrap();
    assert_eq!(out, wrongtype);

    out.clear();
    s.geo_commands(
      &[b"user1:obj1", b"Tel Aviv", b"Haifa"],
      batch,
      &mut out,
      SortedSetOperation::Geodist,
    )
    .unwrap();
    assert_eq!(out, wrongtype);
  });
}

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

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZRankLC
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
    s.sorted_set_rank(a![key, b"b", b"withscore"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n$2\r\n20\r\n");

    out.clear();
    s.sorted_set_rank(a![key, b"b", b"WithScore"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n$2\r\n20\r\n");

    // 第 3 参数非 WITHSCORE 返回语法错误
    out.clear();
    s.sorted_set_rank(a![key, b"b", b"INVALID"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    out.clear();
    s.sorted_set_rank(a![key, b"b", b"INVALID"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // 参数不足（< 2）
    out.clear();
    s.sorted_set_rank(a![key], batch, &mut out, true).unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ZRANK' command\r\n"
    );

    // 参数过多（> 3）：C# SortedSetRank 仅 Count==3 校验 WITHSCORE，
    // 多余参数静默忽略（includeWithScore 保持 false，回普通 rank）
    out.clear();
    s.sorted_set_rank(a![key, b"b", b"WITHSCORE", b"EXTRA"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.sorted_set_rank(
      a![key, b"b", b"WITHSCORE", b"EXTRA"],
      batch,
      &mut out,
      false,
    )
    .unwrap();
    assert_eq!(out, b":1\r\n");

    // Count==4 且第 3 参非 WITHSCORE：同样不校验，静默回普通 rank
    out.clear();
    s.sorted_set_rank(a![key, b"b", b"INVALID", b"EXTRA"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanDoZRemRangeByRank
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

/// 非法 min/max → RESP_ERR_MIN_MAX_NOT_VALID_STRING（无句点，逐字节）；
/// 空串边界三形（deviations §138：C# `TryParseLexParameter :1174` 无守卫裸读
/// `val[0]` 越界掐连、rust first() 守卫折 None 恒回本帧，严禁回改复刻）
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
    assert_eq!(out, ERR_LEX_BOUNDS);

    // ---- 空串三形 × ZLEXCOUNT：min="" / max="" / 双空 ----
    out.clear();
    s.sorted_set_length_by_value(a![key, b"", b"[beta"], batch, &mut out)
      .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);
    out.clear();
    s.sorted_set_length_by_value(a![key, b"[alpha", b""], batch, &mut out)
      .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);
    out.clear();
    s.sorted_set_length_by_value(a![key, b"", b""], batch, &mut out)
      .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);

    // ---- 空串三形 × ZREMRANGEBYLEX：拒帧且零删除 ----
    out.clear();
    s.sorted_set_remove_range(
      a![key, b"", b"[beta"],
      batch,
      &mut out,
      RemoveRangeKind::Lex,
    )
    .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);
    out.clear();
    s.sorted_set_remove_range(
      a![key, b"[alpha", b""],
      batch,
      &mut out,
      RemoveRangeKind::Lex,
    )
    .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);
    out.clear();
    s.sorted_set_remove_range(a![key, b"", b""], batch, &mut out, RemoveRangeKind::Lex)
      .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);
    // 存活自证：三成员全在簿（C# 可达前提同形：键既存，此形于删除动作前即掐连）
    out.clear();
    s.sorted_set_length(a![key], batch, &mut out).unwrap();
    assert_eq!(out, b":3\r\n");

    // ---- 空串三形 × ZRANGEBYLEX（BY_LEX 选项位） ----
    out.clear();
    s.sorted_set_range(
      a![key, b"", b"[beta"],
      batch,
      &mut out,
      SortedSetRangeOpts::BY_LEX,
    )
    .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);
    out.clear();
    s.sorted_set_range(
      a![key, b"[alpha", b""],
      batch,
      &mut out,
      SortedSetRangeOpts::BY_LEX,
    )
    .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);
    out.clear();
    s.sorted_set_range(
      a![key, b"", b""],
      batch,
      &mut out,
      SortedSetRangeOpts::BY_LEX,
    )
    .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);

    // ---- 空串三形 × ZREVRANGEBYLEX（BY_LEX|REVERSE） ----
    let rev_lex = SortedSetRangeOpts::BY_LEX.union(SortedSetRangeOpts::REVERSE);
    out.clear();
    s.sorted_set_range(a![key, b"", b"[beta"], batch, &mut out, rev_lex)
      .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);
    out.clear();
    s.sorted_set_range(a![key, b"[alpha", b""], batch, &mut out, rev_lex)
      .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);
    out.clear();
    s.sorted_set_range(a![key, b"", b""], batch, &mut out, rev_lex)
      .unwrap();
    assert_eq!(out, ERR_LEX_BOUNDS);

    // 会话存活：错误形全拒后正常界读仍正确（对标 garnet
    // "-ERR min or max not valid string range item\r\n+PONG\r\n" 存活帧形）
    out.clear();
    s.sorted_set_length_by_value(a![key, b"[alpha", b"[beta"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");
  });
}

/// 慢臂直驱（与降级快照投递同径，不经会话快路径）
fn slow_direct(rt: &Runtime, api: &GarnetApi, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  let snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
  rt.block_on(async {
    SlowWait::for_command(api, cmd, snapshot, RESP_V2)
      .resolve()
      .await
  })
}

/// 分层派发同步求值并回帧字节（与 tiered_scan_err_propagate 同款泵）
fn auto_exec(
  rt: &Runtime,
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// deviations §138 三态空串形锁：内存信封（快臂线帧）、慢臂直驱、升阶树内
/// 对 ZLEXCOUNT/ZREMRANGEBYLEX/ZRANGEBYLEX/ZREVRANGEBYLEX 空串三形逐字节全等
/// （C# 此形硬索引越界掐连、无应答帧，对照注释备查，严禁回改复刻）
#[test]
fn zlex_empty_string_bounds_three_state_byte_equal() {
  let (_dir, store) = open_test_store("zlex-empty-bounds.db").unwrap();
  let rt = Runtime::new().unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone());

  // 键预 ZADD 存活态验（可达前提是键既存：缺失走 NOTFOUND 短路两侧同形不入本形）
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[
        b"ZADD", b"zmem", b"0", b"alpha", b"0", b"beta", b"0", b"gamma"
      ]
    ),
    b":3\r\n"
  );

  // 空串三形：空头 / 尾头 / 双空
  let shapes: &[(&[u8], &[u8])] = &[(b"", b"[beta"), (b"[alpha", b""), (b"", b"")];
  // 内存信封（快臂）逐字节钉 + 慢臂逐字节全等
  let wire_cmds: &[(&[u8], RespCommand)] = &[
    (b"ZLEXCOUNT", RespCommand::Zlexcount),
    (b"ZREMRANGEBYLEX", RespCommand::Zremrangebylex),
    (b"ZRANGEBYLEX", RespCommand::Zrangebylex),
    (b"ZREVRANGEBYLEX", RespCommand::Zrevrangebylex),
  ];
  for &(name, cmd) in wire_cmds {
    let name = str::from_utf8(name).unwrap();
    for &(min, max) in shapes {
      let fast = roundtrip(&rt, &mut c, &[name.as_bytes(), b"zmem", min, max]);
      assert_eq!(fast, ERR_LEX_BOUNDS, "内存信封 {name} 空串形");
      let slow = slow_direct(&rt, &api, cmd, &[b"zmem", min, max]);
      assert_eq!(slow, fast, "慢臂与快臂 {name} 逐字节全等");
    }
  }

  // 升阶树内（Zlexcount 与 Zrange BYLEX 两枚树内臂承接 ZLEXCOUNT/ZRANGEBYLEX/
  // ZREVRANGEBYLEX；ZREMRANGEBYLEX 无树内臂、经物化漏斗已由上方慢臂覆盖）
  let sess = store.new_session().unwrap();
  rt.block_on(sess.promote_collection_to_bftree(
    b"ztier",
    GarnetObjectType::SortedSet,
    vec![
      (b"alpha".to_vec(), encode_member(&0f64.to_be_bytes(), None)),
      (b"beta".to_vec(), encode_member(&0f64.to_be_bytes(), None)),
      (b"gamma".to_vec(), encode_member(&0f64.to_be_bytes(), None)),
    ],
    i64::MAX,
    false,
  ))
  .unwrap();
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(b"ztier"))
      .unwrap()
      .is_some(),
    "键应处于 wbftree 分层态"
  );
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  for cmd in [
    RespCommand::Zlexcount,
    RespCommand::Zrangebylex,
    RespCommand::Zrevrangebylex,
  ] {
    for &(min, max) in shapes {
      let got = auto_exec(&rt, &api, &mut s, cmd, &[b"ztier", min, max]);
      assert_eq!(got, ERR_LEX_BOUNDS, "树内 {cmd} 空串形逐字节全等");
    }
  }

  // 错误形后会话存活（garnet "-ERR ...range item\r\n+PONG\r\n" 帧形）且零删除键存活
  assert_eq!(roundtrip(&rt, &mut c, &[b"PING"]), b"+PONG\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b"zmem"]), b":3\r\n");
}

/// ZCOUNT 缺失键判定序三态锁（票 wnode-zcount-missing-key-param-order）：C#
/// 键缺席经 ReadObjectStoreOperation（SortedSetOps.cs:984 → Common.cs:56）Read
/// 回调不执行，对象层 SortedSetCount 的 min/max 解析不运行，NOTFOUND 直写 :0
/// ——缺失键 + 非法 min/max 应 :0 而非 -ERR。内存信封（快臂线帧）、慢臂直驱
///（装载段 on_missing 短路）、升阶树内三态逐字节全等；存在键 + 非法参数错误帧
/// 回归不变（对标 RespSortedSetTests.cs:CanValidateInvalidParamentersZCountLC），
/// -inf/+inf 有效界零回归
#[test]
fn zcount_missing_key_param_order_three_state() {
  let (_dir, store) = open_test_store("zcount-param-order.db").unwrap();
  let rt = Runtime::new().unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone());

  // 快路径线帧：缺失键 + 非法 min/max → :0（C# NOTFOUND 短路，参数不校验）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZCOUNT", b"nokey", b"abc", b"5"]),
    b":0\r\n"
  );
  // 缺失键零副作用：短路面不建键（对齐错误臂不落库判据）
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"nokey"]), b":0\r\n");

  // 慢路径 SlowWait 直驱：缺失键 + 非法 min/max → :0（装载段 on_missing 同帧）
  let slow = slow_direct(&rt, &api, RespCommand::Zcount, &[b"nokey", b"abc", b"5"]);
  assert_eq!(slow, b":0\r\n");

  // 存在键 + 非法参数：错误帧回归不变（C# CanValidateInvalidParamentersZCountLC）
  assert_eq!(
    roundtrip(
      &rt,
      &mut c,
      &[b"ZADD", b"board", b"400", b"Kendra", b"560", b"Tom"]
    ),
    b":2\r\n"
  );
  let err_float = err_frame(RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZCOUNT", b"board", b"5", b"b"]),
    err_float
  );
  // -inf/+inf 有效界既有语义零回归
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZCOUNT", b"board", b"-inf", b"+inf"]),
    b":2\r\n"
  );

  // 升阶树内：分层存活键 ZCOUNT 走树内臂——非法界同帧（存在键参数门）、
  // 有效界计数正确；缺失键经同一分层派发仍 :0 短路（不触树）
  let sess = store.new_session().unwrap();
  rt.block_on(sess.promote_collection_to_bftree(
    b"ztier",
    GarnetObjectType::SortedSet,
    vec![
      (
        b"Kendra".to_vec(),
        encode_member(&400f64.to_be_bytes(), None),
      ),
      (b"Tom".to_vec(), encode_member(&560f64.to_be_bytes(), None)),
    ],
    i64::MAX,
    false,
  ))
  .unwrap();
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(b"ztier"))
      .unwrap()
      .is_some(),
    "键应处于 wbftree 分层态"
  );
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  assert_eq!(
    auto_exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Zcount,
      &[b"ztier", b"abc", b"5"]
    ),
    err_float
  );
  assert_eq!(
    auto_exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Zcount,
      &[b"ztier", b"-inf", b"+inf"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(
      &rt,
      &api,
      &mut s,
      RespCommand::Zcount,
      &[b"nokey", b"abc", b"5"]
    ),
    b":0\r\n"
  );

  // 会话存活
  assert_eq!(roundtrip(&rt, &mut c, &[b"PING"]), b"+PONG\r\n");
}

/// deviations §151 数据段奇数尾巴锁：首 token 即分值形「ZADD k 1 m 5」内存信封
/// （真协议往返）防御截断——应答 :1、集合恰 {m:1}、会话存活、错误帧零输出
/// （C# 此形对象层主循环 GetArgSliceByRef 越界读：debug 断言掐连 / release 垃圾
/// 成员写入；真 Redis 该形报 syntax error，rust 截断系防御容忍非 Redis 对齐形，
/// 严禁按 C# 形回改为越界读或 panic）；「ZADD k 1 m」单对完整 :1 对照
#[test]
fn zadd_odd_tail_token_truncated_defensive() {
  let (_dir, store) = open_test_store("zadd-odd-tail.db").unwrap();
  let rt = Runtime::new().unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone());

  // 奇数尾巴：会话层 Count=4 过前置门（C# 同门放行），对象层 Count=3 末轮取
  // 成员越界位——应答恰 :1 一帧（错误帧零输出），尾巴分值 5 丢弃、垃圾成员零落
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"zodd", b"1", b"m", b"5"]),
    b":1\r\n"
  );

  // 集合恰 {m:1}：基数 1 + 单成员 + 分值原样 1
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b"zodd"]), b":1\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZSCORE", b"zodd", b"m"]),
    b"$1\r\n1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGE", b"zodd", b"0", b"-1"]),
    b"*1\r\n$1\r\nm\r\n"
  );

  // 对照：单对完整（会话 Count=3 过门后对象层恰一对）正常 :1
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"zpair", b"1", b"m"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b"zpair"]), b":1\r\n");

  // 截断后会话存活
  assert_eq!(roundtrip(&rt, &mut c, &[b"PING"]), b"+PONG\r\n");
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanUseZPopMin
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
    s.sorted_set_pop(a![key, b"10"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nc\r\n$1\r\n3\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanUseZRandMember
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

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CheckSortedSetRangeStoreByScoreSE（REV + LIMIT 组合）
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

/// rev 范围族内存信封镜像形（对标 C# GetElementsInRangeByLex :975-1070 /
/// GetElementsInRangeByScore :1079 起 skip/take 链独立核算，与分层态
/// tiered_cmds_align.rs ck! 同源对拍）：ZREVRANGEBYLEX [c [a 窗口倒序（镜像
/// C# CanDoZRangeByLexReverse `ZRANGE board [c - BYLEX REV` → c,b,a；REVERSE
/// 于对象层先交换界内 min/max 再倒序输出，见 GetElementsInRangeByLex :994-1002）、
/// ZREVRANGEBYSCORE 3 1 LIMIT 1 3 交换先于 LIMIT 切片（升序窗 [a,b,c] 倒序
/// [c,b,a] 后 skip 1 take 3 → [b,a]）
#[test]
fn zrevrange_lex_score_mirror_forms() {
  with_batch(|s, batch| {
    zadd(
      s,
      batch,
      b"zrev7",
      &[
        (b"1", b"a"),
        (b"2", b"b"),
        (b"3", b"c"),
        (b"4", b"d"),
        (b"5", b"e"),
      ],
    );

    let mut out = Vec::new();
    s.sorted_set_range(
      a![b"zrev7", b"[c", b"[a"],
      batch,
      &mut out,
      SortedSetRangeOpts::BY_LEX | SortedSetRangeOpts::REVERSE,
    )
    .unwrap();
    assert_eq!(out, b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n");

    out.clear();
    s.sorted_set_range(
      a![b"zrev7", b"3", b"1", b"LIMIT", b"1", b"3"],
      batch,
      &mut out,
      SortedSetRangeOpts::BY_SCORE | SortedSetRangeOpts::REVERSE,
    )
    .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nb\r\n$1\r\na\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespBlockingCollectionTests.cs:BasicBzpopMinMaxTest（立即可取 + 缺键 null + 非法 timeout）
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

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CanUseZUnionStoreWithWeights
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

/// ZINCRBY 非浮点增量 → RESP_ERR_NOT_VALID_FLOAT（C# CandDoZIncrby 只覆盖
/// 正常增量臂且锚在 cand_do_z_incrby；C# 语料的非法浮点测试
/// CanDoZaddWithInvalidInput 是 ZADD 臂，与本命令不同，不据此改挂），
/// 本测试为 rust 侧补充，不挂 C# 测试锚
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
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));
  });
}

/// round7 口径回归：GEOSEARCH COUNT 非整数 → 含句点文案
/// （libs/server/SessionParseStateExtensions.cs:TryGetGeoSearchOptions COUNT 分支）
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

/// 解析 *SCAN 应答（*2 头 + 游标 bulk + 项数组）→ (游标, 项列表)
fn parse_scan_reply(frame: &[u8]) -> (i64, Vec<Vec<u8>>) {
  let header_end = frame.iter().position(|&b| b == b'\n').expect("缺数组头");
  let rest = &frame[header_end + 1..];
  // 游标 bulk：$len\r\n<body>\r\n
  let len_end = rest.iter().position(|&b| b == b'\n').expect("缺游标头");
  let len: usize = str::from_utf8(&rest[1..len_end - 1])
    .unwrap()
    .parse()
    .unwrap();
  let cursor: i64 = str::from_utf8(&rest[len_end + 1..len_end + 1 + len])
    .unwrap()
    .parse()
    .unwrap();
  let items = parse_bulk_array(&rest[len_end + 1 + len + 2..]);
  (cursor, items)
}

/// 成员-分值平铺项按序配对
fn pair_up(items: Vec<Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
  assert!(
    items.len().is_multiple_of(2),
    "ZSCAN 项应为成员-分值对: {items:?}"
  );
  items
    .chunks(2)
    .map(|c| (c[0].clone(), c[1].clone()))
    .collect()
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:ZScanWithExpiredItems
/// 主干（无过期形态）：单轮遍历游标归零，成员与分值成对平铺返回；
/// 分值整型文本无小数点（ObjectOutput::format_double）
#[test]
fn zscan_returns_members_with_scores() {
  with_batch(|s, batch| {
    zadd(
      s,
      batch,
      b"key1",
      &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")],
    );

    let mut out = Vec::new();
    s.network_zscan(&[b"key1", b"0"], batch, &mut out).unwrap();
    let (cursor, items) = parse_scan_reply(&out);
    assert_eq!(cursor, 0);

    let mut pairs = pair_up(items);
    pairs.sort();
    assert_eq!(
      pairs,
      vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"c".to_vec(), b"3".to_vec()),
      ]
    );
  });
}

/// C# SortedSetScan 缺键形态（NOTFOUND → 游标 0 + 空数组）
#[test]
fn zscan_missing_key_returns_zero_cursor() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_zscan(&["keyZ".as_bytes(), b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n$1\r\n0\r\n*0\r\n");
  });
}

/// 游标分页：COUNT 截断 → 续扫 → 游标归零，成员-分值对不重不漏
/// （对标 RespSortedSetTests.cs:ZScanWithExpiredItems 的全量遍历口径）
#[test]
fn zscan_cursor_pagination_covers_all_entries() {
  with_batch(|s, batch| {
    const N: usize = 9;
    let members: Vec<Vec<u8>> = (0..N).map(|i| format!("m{i}").into_bytes()).collect();
    let scores: Vec<Vec<u8>> = (0..N).map(|i| format!("{}", i + 1).into_bytes()).collect();
    let entries: Vec<(&[u8], &[u8])> = scores
      .iter()
      .zip(&members)
      .map(|(score, member)| (score.as_slice(), member.as_slice()))
      .collect();
    zadd(s, batch, b"key1", &entries);

    let mut cursor = 0_i64;
    let mut seen = Vec::new();
    loop {
      let start = cursor.to_string();
      let mut out = Vec::new();
      s.network_zscan(
        &[b"key1", start.as_bytes(), b"COUNT", b"2"],
        batch,
        &mut out,
      )
      .unwrap();
      let (next, items) = parse_scan_reply(&out);
      // COUNT 2：每成员占 2 项（成员+分值），除末轮外恰好 4 项
      assert!(
        items.len() == 4 || next == 0,
        "轮次应被 COUNT 截断：cursor={next} items={items:?}"
      );
      seen.extend(pair_up(items));
      cursor = next;
      if cursor == 0 {
        break;
      }
    }

    // 不重不漏
    seen.sort();
    let all: Vec<(Vec<u8>, Vec<u8>)> = (0..N)
      .map(|i| {
        (
          format!("m{i}").into_bytes(),
          format!("{}", i + 1).into_bytes(),
        )
      })
      .collect();
    assert_eq!(seen, all);
  });
}

/// 票 zcode-r53-waitmatrix 语义锁：
/// BZPOPMIN 与 ZMPOP 在遇到 WRONGTYPE 键时，必须且只能输出单帧错误，严禁 continue
/// 导致后续追加 null 帧形成双帧协议流错位。
#[test]
fn bzpopmin_and_zmpop_wrongtype_single_frame_lock() {
  with_batch(|s, batch| {
    // 写入一个 String 键作为异型键
    let _ = batch.try_upsert_sync(b"str_key", b"hello").unwrap();

    // 1. BZPOPMIN 遇到异型键：必须单帧 -WRONGTYPE，不得追加 null 帧
    let mut out = Vec::new();
    s.sorted_set_blocking_pop(a![b"str_key", b"0"], batch, &mut out, true)
      .unwrap();
    assert_eq!(
      out,
      err_frame(RESP_ERR_WRONG_TYPE),
      "BZPOPMIN 遇 WRONGTYPE 必须单帧终结"
    );

    // 2. BZPOPMAX 遇异型键同测
    out.clear();
    s.sorted_set_blocking_pop(a![b"str_key", b"0"], batch, &mut out, false)
      .unwrap();
    assert_eq!(
      out,
      err_frame(RESP_ERR_WRONG_TYPE),
      "BZPOPMAX 遇 WRONGTYPE 必须单帧终结"
    );
  });
}

/// deviations §153 空 src/dst 键两形锁（票 wnode-zrangestore-empty-key-guard）：
/// C# 存储层 :0 零触达守卫（SortedSetOps.cs:721-725）不复刻，rust 正常装载执行
/// 为真 Redis 一致侧——空 src 形（空键未建态）缺源删 dst 回 :0（dst 预置旧值与
/// TTL，终态 EXISTS :0／TTL -2 零残留）；空 dst 形结果落空名键回 :N、空键经
/// ZCARD/ZRANGE 验存活且内容逐字节钉。快臂线帧＋慢臂 SlowWait 直驱两形各一拍，
/// 双臂同帧；对照 ZADD "" 单对正常建（r19 已决非分叉面，单点现状钉）
#[test]
fn zrangestore_empty_key_forms_both_arms() {
  let (_dir, store) = open_test_store("zrangestore-empty-key.db").unwrap();
  let rt = Runtime::new().unwrap();
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone());

  // 源集合预置 zesrc = {a:1, b:2}
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"zesrc", b"1", b"a", b"2", b"b"]),
    b":2\r\n"
  );

  // ---- 空 src 形先拍（此时空键 "" 尚未建立，缺源臂可达）----
  // dst 预置旧值与 TTL：zedst = {old:9} + EXPIRE 100
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"zedst", b"9", b"old"]),
    b":1\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"EXPIRE", b"zedst", b"100"]),
    b":1\r\n"
  );
  // 快臂：ZRANGESTORE zedst "" 0 -1 → 缺源删 dst 回 :0（C# 守卫形此拍亦 :0，
  // 但 C# 零触达保旧值旧 TTL，rust 删键终态发散即本条登记面）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGESTORE", b"zedst", b"", b"0", b"-1"]),
    b":0\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"zedst"]), b":0\r\n");
  assert_eq!(roundtrip(&rt, &mut c, &[b"TTL", b"zedst"]), b":-2\r\n");
  // 慢臂直驱同帧删臂复验（zrangestore_cold 回收臂与快臂单源同形）
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"zedst", b"9", b"old"]),
    b":1\r\n"
  );
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Zrangestore,
      &[b"zedst", b"", b"0", b"-1"]
    ),
    b":0\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"EXISTS", b"zedst"]), b":0\r\n");

  // ---- 空 dst 形：结果集落空名键回 :N（C# 守卫形此拍 :0 且零触达，rust 建键）----
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGESTORE", b"", b"zesrc", b"0", b"-1"]),
    b":2\r\n"
  );
  // 空键存活：ZCARD 直读计数 + ZRANGE 内容逐字节钉
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b""]), b":2\r\n");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZRANGE", b"", b"0", b"-1"]),
    b"*2\r\n$1\r\na\r\n$1\r\nb\r\n"
  );
  // 慢臂直驱同帧（覆写幂等，仍回 :2）
  assert_eq!(
    slow_direct(
      &rt,
      &api,
      RespCommand::Zrangestore,
      &[b"", b"zesrc", b"0", b"-1"]
    ),
    b":2\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b""]), b":2\r\n");

  // ---- 对照钉：ZADD "" 空键正常建系双侧同形非分叉面（r19 已决，§153 划界注）----
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"ZADD", b"", b"3", b"c"]),
    b":1\r\n"
  );
  assert_eq!(roundtrip(&rt, &mut c, &[b"ZCARD", b""]), b":3\r\n");

  // 会话存活
  assert_eq!(roundtrip(&rt, &mut c, &[b"PING"]), b"+PONG\r\n");
}
