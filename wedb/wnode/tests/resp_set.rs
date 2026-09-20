use core::str;

use wnode_test::{err_frame, with_batch};
use wresp::cmd_strings::{RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_WRONG_TYPE};
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

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CandDoSaddBasic
#[test]
fn cand_do_sadd_basic() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"myset", b"Hello"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.set_add(&[b"myset", b"Hello"], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanAddAndListMembers
#[test]
fn can_add_and_list_members() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"myset", b"Hello", b"World"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.set_members(&[b"myset"], batch, &mut out).unwrap();
    let mut items = parse_bulk_array(&out);
    items.sort();
    assert_eq!(items, vec![b"Hello".to_vec(), b"World".to_vec()]);
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanCheckIfMemberExistsInSet
#[test]
fn can_check_if_member_exists_in_set() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"myset", b"Hello"], batch, &mut out).unwrap();

    out.clear();
    s.set_is_member(&[b"myset", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.set_is_member(&[b"myset", b"World"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanRemoveField
#[test]
fn can_remove_field() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"myset", b"Hello", b"World"], batch, &mut out)
      .unwrap();

    out.clear();
    s.set_remove(&[b"myset", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.set_remove(&[b"myset", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CheckEmptySetKeyRemoved
#[test]
fn check_empty_set_key_removed() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"myset", b"Hello"], batch, &mut out).unwrap();

    out.clear();
    s.set_remove(&[b"myset", b"Hello"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.network_exists(&[b"myset"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanReturnEmptySet
#[test]
fn can_return_empty_set() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_members(&[b"nokey"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSetUnion
#[test]
fn can_do_set_union() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"key1", b"a", b"b"], batch, &mut out).unwrap();
    s.set_add(&[b"key2", b"b", b"c"], batch, &mut out).unwrap();

    out.clear();
    s.set_union(&[b"key1", b"key2"], batch, &mut out).unwrap();
    let mut items = parse_bulk_array(&out);
    items.sort();
    assert_eq!(items, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSetUnionStore
#[test]
fn can_do_set_union_store() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"key1", b"a", b"b"], batch, &mut out).unwrap();
    s.set_add(&[b"key2", b"b", b"c"], batch, &mut out).unwrap();

    out.clear();
    s.set_union_store(&[b"dest", b"key1", b"key2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":3\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSetInter
#[test]
fn can_do_set_inter() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"key1", b"a", b"b"], batch, &mut out).unwrap();
    s.set_add(&[b"key2", b"b", b"c"], batch, &mut out).unwrap();

    out.clear();
    s.set_intersect(&[b"key1", b"key2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n$1\r\nb\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSetInterStore
#[test]
fn can_do_set_inter_store() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"key1", b"a", b"b"], batch, &mut out).unwrap();
    s.set_add(&[b"key2", b"b", b"c"], batch, &mut out).unwrap();

    out.clear();
    s.set_intersect_store(&[b"dest", b"key1", b"key2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSdiff
#[test]
fn can_do_sdiff() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"key1", b"a", b"b"], batch, &mut out).unwrap();
    s.set_add(&[b"key2", b"b", b"c"], batch, &mut out).unwrap();

    out.clear();
    s.set_diff(&[b"key1", b"key2"], batch, &mut out).unwrap();
    assert_eq!(out, b"*1\r\n$1\r\na\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSdiffStoreOverwrittenKey
#[test]
fn can_do_sdiff_store_overwritten_key() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"key1", b"a", b"b"], batch, &mut out).unwrap();
    s.set_add(&[b"key2", b"b", b"c"], batch, &mut out).unwrap();

    out.clear();
    s.set_diff_store(&[b"dest", b"key1", b"key2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSinterCard
#[test]
fn can_do_sinter_card() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"key1", b"a", b"b"], batch, &mut out).unwrap();
    s.set_add(&[b"key2", b"b", b"c"], batch, &mut out).unwrap();

    out.clear();
    s.set_intersect_length(&[b"2", b"key1", b"key2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSPOPCommandLC
#[test]
fn can_do_spop_command_lc() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"myset", b"one"], batch, &mut out).unwrap();

    out.clear();
    s.set_pop(&[b"myset"], batch, &mut out).unwrap();
    assert_eq!(out, b"$3\r\none\r\n");

    out.clear();
    s.set_pop(&[b"myset"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSPOPWithCountCommandLC
#[test]
fn can_do_spop_with_count_command_lc() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"myset", b"one", b"two"], batch, &mut out)
      .unwrap();

    out.clear();
    s.set_pop(&[b"myset", b"2"], batch, &mut out).unwrap();
    assert!(out.starts_with(b"*2\r\n"));

    out.clear();
    s.network_exists(&[b"myset"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSPOPWithCountCommandWhenKeyDoesNotExistLC
#[test]
fn can_do_spop_with_count_command_when_key_does_not_exist_lc() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    // 缺键带 count：应返回空数组 *0\r\n 而非 nil
    s.set_pop(&[b"fooset", b"3"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n");

    // count 为 0：应直接返回空数组 *0\r\n
    out.clear();
    s.set_pop(&[b"fooset", b"0"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n");

    // count 为负数或非整数：应报错
    out.clear();
    s.set_pop(&[b"fooset", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    // 缺键不带 count：应返回 nil $-1\r\n
    out.clear();
    s.set_pop(&[b"fooset"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSRANDMEMBERWithCountCommandLC
#[test]
fn can_do_srandmember_with_count_command_lc() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(&[b"myset", b"one", b"two", b"three"], batch, &mut out)
      .unwrap();

    out.clear();
    s.set_random_member(&[b"myset", b"2"], batch, &mut out)
      .unwrap();
    assert!(out.starts_with(b"*2\r\n"));

    out.clear();
    s.set_length(&[b"myset"], batch, &mut out).unwrap();
    assert_eq!(out, b":3\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CheckSetOperationsOnWrongTypeObjectSE
#[test]
fn check_set_operations_on_wrong_type_object_se() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"str", b"plain"], batch, &mut out).unwrap();

    out.clear();
    s.set_add(&[b"str", b"elem"], batch, &mut out).unwrap();
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSmoveBasic
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

/// libs/server/Storage/Session/ObjectStore/SetOps.cs:SetMove
/// （源集合不含该成员 → :0；目标已含成员：不重复添加但仍从源删除并回 :1）
#[test]
fn smove_missing_member_and_existing_destination_member() {
  with_batch(|s, batch| {
    s.set_add(a![b"sm1", b"a", b"b"], batch, &mut Vec::new())
      .unwrap();
    s.set_add(a![b"sm2", b"a", b"c"], batch, &mut Vec::new())
      .unwrap();

    let mut out = Vec::new();
    // 源集合不含成员 → :0（且不产生任何写）
    s.set_move(a![b"sm1", b"sm2", b"z"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // 目标已含成员 a：不重复添加，但仍从源删除，返回 :1
    out.clear();
    s.set_move(a![b"sm1", b"sm2", b"a"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.set_length(a![b"sm2"], batch, &mut out).unwrap();
    assert_eq!(out, b":2\r\n");
    out.clear();
    s.set_length(a![b"sm1"], batch, &mut out).unwrap();
    // 源剩 {b}
    assert_eq!(out, b":1\r\n");
  });
}

/// LIMIT i32 上界/负数/语法矩阵、截断语义、缺键按空集回 :0（SetIntersectLength 语义见 resp/objects/set_commands.rs）
#[test]
fn sintercard_limit_align() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(a![b"ic1", b"a", b"b", b"c"], batch, &mut out)
      .unwrap();
    s.set_add(a![b"ic2", b"b", b"c", b"d"], batch, &mut out)
      .unwrap();

    // 缺键按空集求交 → :0
    out.clear();
    s.set_intersect_length(a![b"2", b"nokey1", b"nokey2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // 交集 {b,c} 基数 2：LIMIT 1 → 截断为 :1
    out.clear();
    s.set_intersect_length(a![b"2", b"ic1", b"ic2", b"LIMIT", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // LIMIT 0：不截断（C# limit > 0 才取 min）
    out.clear();
    s.set_intersect_length(a![b"2", b"ic1", b"ic2", b"LIMIT", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // LIMIT 负数 → 拒绝
    out.clear();
    s.set_intersect_length(a![b"2", b"ic1", b"ic2", b"LIMIT", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR LIMIT can't be negative\r\n");

    // LIMIT 负溢出超 i32 下界：C# TryReadInt32Safe overflow → 非整数错误
    out.clear();
    s.set_intersect_length(
      a![b"2", b"ic1", b"ic2", b"LIMIT", b"-3000000000"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    // LIMIT 超 i32 上界：C# TryReadInt32Safe 失败 → 非整数错误
    out.clear();
    s.set_intersect_length(
      a![b"2", b"ic1", b"ic2", b"LIMIT", b"99999999999"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    // numkeys 超上界 / 负溢出：同上
    out.clear();
    s.set_intersect_length(a![b"99999999999", b"ic1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
    out.clear();
    s.set_intersect_length(a![b"-3000000000", b"ic1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    // numkeys 为域内 0/负数：nKeys<1 → greater than 0（非 out-of-range）
    out.clear();
    s.set_intersect_length(a![b"0", b"ic1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR numkeys should be greater than 0\r\n");
    out.clear();
    s.set_intersect_length(a![b"-5", b"ic1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR numkeys should be greater than 0\r\n");

    // LIMIT 尾随多余参数 → syntax error
    out.clear();
    s.set_intersect_length(
      a![b"2", b"ic1", b"ic2", b"LIMIT", b"1", b"x"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));
  });
}

/// count 大于集合基数：弹出全量元素并删空整键（SetPop 语义见 wcol/src/set/set_object_impl.rs）
#[test]
fn spop_count_exceeds_cardinality_pops_all() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(a![b"spop9", b"one", b"two"], batch, &mut out)
      .unwrap();

    out.clear();
    s.set_pop(a![b"spop9", b"5"], batch, &mut out).unwrap();
    assert!(out.starts_with(b"*2\r\n"));
    let mut items = parse_bulk_array(&out);
    items.sort();
    assert_eq!(items, vec![b"one".to_vec(), b"two".to_vec()]);

    // 弹空 → 整键回收
    out.clear();
    s.network_exists(&[b"spop9"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CheckIfMemberExistsInSetLC
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

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSRANDMEMBERWithCountCommandSE（负数去重全展开）
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

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanUseSScanNoParameters
/// （不存在键分支：C# NOTFOUND → 游标 0 + 空数组）
#[test]
fn sscan_non_existing_key_returns_zero_cursor() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_sscan(&[b"foo", b"0"], batch, &mut out).unwrap();
    assert_eq!(out, b"*2\r\n$1\r\n0\r\n*0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanUseSScanWithMatch
/// MATCH 通配全量命中：aa 命中 *aa，aaf 不命中
#[test]
fn sscan_with_match() {
  with_batch(|s, batch| {
    for m in [&b"aa"[..], b"bb", b"cc", b"dd", b"ee", b"aaf"] {
      s.set_add(&[b"myset", m], batch, &mut Vec::new()).unwrap();
    }

    let mut out = Vec::new();
    s.network_sscan(&[b"myset", b"0", b"MATCH", b"*aa"], batch, &mut out)
      .unwrap();
    // 单轮走完整集合：游标归零 + 唯一命中 aa
    assert_eq!(out, b"*2\r\n$1\r\n0\r\n*1\r\n$2\r\naa\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespSetTest.cs:CanDoSScanWithCursor /
/// CanUseSScanWithCollection：COUNT 截断 → 续扫 → 游标归零，全量成员不重不漏
#[test]
fn sscan_cursor_pagination_covers_all_members() {
  with_batch(|s, batch| {
    const N: usize = 10;
    for i in 0..N {
      let member = format!("member:{i}");
      s.set_add(&[b"myset", member.as_bytes()], batch, &mut Vec::new())
        .unwrap();
    }

    let mut cursor = 0_i64;
    let mut seen = Vec::new();
    loop {
      let start = cursor.to_string();
      let mut out = Vec::new();
      s.network_sscan(
        &[b"myset", start.as_bytes(), b"COUNT", b"3"],
        batch,
        &mut out,
      )
      .unwrap();
      let (next, items) = parse_scan_reply(&out);
      // COUNT 3：除末轮外每轮恰好 3 个成员
      assert!(
        items.len() == 3 || next == 0,
        "轮次应被 COUNT 截断：cursor={next} items={items:?}"
      );
      seen.extend(items);
      cursor = next;
      if cursor == 0 {
        break;
      }
    }

    // 不重不漏
    seen.sort();
    seen.dedup();
    let all: Vec<Vec<u8>> = (0..N).map(|i| format!("member:{i}").into_bytes()).collect();
    assert_eq!(seen, all);
  });
}

/// SPOP count 值域帧（参数推导单源：非整数与负数同报 NOT_INTEGER、arity 门；
/// 慢侧同帧断言见 resp_slow_path.rs 的 object_slow_parse_frames_match_fast）
#[test]
fn spop_count_frames() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(a![b"spk", b"a"], batch, &mut out).unwrap();

    // count 负数 → 非整数帧（C# TryGetInt || count < 0 同报）
    out.clear();
    s.set_pop(a![b"spk", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    // count 超 i32 上界 → 同帧
    out.clear();
    s.set_pop(a![b"spk", b"3000000000"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    // arity > 2 → wrong number of arguments
    out.clear();
    s.set_pop(a![b"spk", b"1", b"x"], batch, &mut out).unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'SPOP' command\r\n"
    );

    // count 为 0 → 空集合（不触达后端）
    out.clear();
    s.set_pop(a![b"spk", b"0"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}
