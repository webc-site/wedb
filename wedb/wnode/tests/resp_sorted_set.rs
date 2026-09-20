use core::str;

use wnode_test::with_batch;
use wtest_base::a;

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
use wcol::zset::sorted_set_object::{SortedSetOperation, SortedSetRangeOpts};
use wnode::resp::{
  RespServerSession,
  objects::{sorted_set_commands::RemoveRangeKind, sorted_set_geo_commands::GeoSearchCommandKind},
};
use wnode_test::err_frame;
use wresp::cmd_strings::{RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_WRONG_TYPE};

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

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CandDoZIncrby 非浮点 → RESP_ERR_NOT_VALID_FLOAT
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
