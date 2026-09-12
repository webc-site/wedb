mod support;

use core::str;

use support::with_batch;

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
use wnode::{
  objects::sortedset::sorted_set_object::SortedSetOperation,
  resp::objects::{
    sorted_set_commands::RemoveRangeKind, sorted_set_geo_commands::GeoSearchCommandKind,
  },
};

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
    s.network_exists(&[key], batch, &mut out).unwrap();
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
    s.network_exists(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetTests.cs:CheckSortedSetOperationsOnWrongTypeObjectSE
#[test]
fn check_sorted_set_operations_on_wrong_type_object_se() {
  with_batch(|s, batch| {
    let wrongtype = b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";
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
    assert_eq!(out, b"-ERR syntax error\r\n");

    // numkeys 超过实际提供 key 数量：提供 2 个 key，但 numkeys = 3
    out.clear();
    s.sorted_set_intersect_store(&[b"dest", b"3", b"zset1", b"zset2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR syntax error\r\n");

    // 大数值 numkeys，防止溢出检查失效
    out.clear();
    s.sorted_set_intersect_store(&[b"dest", b"2147483647", b"zset1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR syntax error\r\n");

    out.clear();
    s.sorted_set_intersect_store(&[b"dest", b"2147483646", b"zset1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR syntax error\r\n");

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
    assert_eq!(out, b"-ERR syntax error\r\n");
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
    s.network_exists(&[key], batch, &mut out).unwrap();
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
    s.network_exists(&[key], batch, &mut out).unwrap();
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
      &[b"mysortedset", b"60", b"a1"],
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
    s.network_exists(&[b"newCities"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSortedSetGeoTests.cs:CheckGeoSortedSetOperationsOnWrongTypeObjectSE
#[test]
fn check_geo_sorted_set_operations_on_wrong_type_object_se() {
  with_batch(|s, batch| {
    let wrongtype = b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";
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
