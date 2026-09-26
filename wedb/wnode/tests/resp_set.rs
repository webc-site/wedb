use core::str;
use std::{mem::take, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    key_admin_commands::{ExpireCmd, TtlCmd},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wnode_test::{err_frame, with_batch};
use wresp::{
  cmd_strings::{RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_WRONG_TYPE},
  command::RespCommand,
};
use wtest_base::{a, test_store_config};

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
    s.network_set(&[b"str", b"plain"], batch, None, &mut out)
      .unwrap();

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
    s.set_move(a![b"ss1", b"ss2", b"a"], batch, None, &mut out)
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
    s.set_move(a![b"ssmiss", b"ss2", b"a"], batch, None, &mut out)
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
    s.set_move(a![b"sm1", b"sm2", b"z"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // 目标已含成员 a：不重复添加，但仍从源删除，返回 :1
    out.clear();
    s.set_move(a![b"sm1", b"sm2", b"a"], batch, None, &mut out)
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

// ——— §113 集合算术族「首键缺席/交空早退免检后续键」短路三形行为锁 ———
// （票 doc-deviations-set-arith-absence-shortcut-divergence；裁决=维持 rust
// 全集判型在先、独占 -WRONGTYPE、STORE 错误臂目标键零触达，严禁按 C# 回改）
//
// C# 对照形态备查（原型短路缺陷族，一手锚 SetOps.cs 私有 SetIntersect
// :452-454 首键 NOTFOUND 直返 OK 空集／:476-481 循环臂 GET 前判
// output.Count==0 交空早退／:496-500 后续键 NOTFOUND 清空早退，私有
// SetDiff :889-891 首键同形早退；四臂均免检后续键类型）：
// a) SINTER/SDIFF「首键缺失＋任一后续键 string」→ C# 回空集应答
//    （SetCommands.cs:84-101 WriteSetLength(0)／:722-725 WriteEmptySet），
//    SINTERCARD 经私有交体 SetOps.cs:964-967 回 :0——而非 -WRONGTYPE；
// b) SINTER/SINTERCARD/SINTERSTORE「中途交空＋后续 string」同 a 免检；
// c) SINTERSTORE/SDIFFSTORE 上述形 → C# 对空结果执行
//    EXPIRE(dst, TimeSpan.Zero)（SetOps.cs:426/:864），dst 既存值（含
//    string 键与其 TTL）被真实删除并回 :0。SUNION/SUNIONSTORE 免检族外
//    （私有 SetUnion :612-637 逐键判型与 rust 同构，对照组）。
// rust 三形应答：独占 -WRONGTYPE（文案逐字节），STORE 臂目标键零触达。

/// §113 形 a 同步臂锁：SINTER/SDIFF/SINTERCARD「首键缺失＋尾键 string」
/// rust 全集判型、独占 -WRONGTYPE（C# 该形短路免检回空集/:0，见上备查注，
/// 严禁按 C# 回改）
#[test]
fn set_arith_first_absent_key_with_string_tail_wrongtype() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(a![b"str", b"plain"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    for (name, reply) in [
      ("SINTER", {
        out.clear();
        s.set_intersect(a![b"noseed", b"str"], batch, &mut out)
          .unwrap();
        out.clone()
      }),
      ("SDIFF", {
        out.clear();
        s.set_diff(a![b"noseed", b"str"], batch, &mut out).unwrap();
        out.clone()
      }),
      ("SINTERCARD", {
        out.clear();
        s.set_intersect_length(a![b"2", b"noseed", b"str"], batch, &mut out)
          .unwrap();
        out.clone()
      }),
    ] {
      assert_eq!(reply, err_frame(RESP_ERR_WRONG_TYPE), "{name} 应独占错误帧");
      // 独占面：应答仅此一条错误行，无任何空集/:0 帧渗漏
      assert_eq!(reply.len(), err_frame(RESP_ERR_WRONG_TYPE).len());
    }
  });
}

/// §113 形 b 同步臂锁：SINTER/SINTERCARD「中途交空＋尾键 string」——
/// C# 循环臂 :476-481 交空早退免检尾键回空集/:0；rust 仍逐键装载全集判型
#[test]
fn set_arith_mid_empty_intersection_with_string_tail_wrongtype() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(a![b"k1", b"a"], batch, &mut out).unwrap();
    s.set_add(a![b"k2", b"b"], batch, &mut out).unwrap();
    s.network_set(a![b"str", b"plain"], batch, None, &mut out)
      .unwrap();

    out.clear();
    s.set_intersect(a![b"k1", b"k2", b"str"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));

    out.clear();
    s.set_intersect_length(a![b"3", b"k1", b"k2", b"str"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));
  });
}

/// §113 形 c STORE 锁：SINTERSTORE 两形与 SDIFFSTORE 首键缺席形（其无
/// 中途空臂，危险形仅首键缺席一类）——rust 错误臂不落笔，dst 预置值与
/// TTL 原样保留零触达（C# 该形 EXPIRE(dst, TimeSpan.Zero) 真实删除 dst
/// 既存 string 值与其 TTL 并回 :0，见上备查注，回改即复活误删数据面）
#[test]
fn set_store_arith_shortcut_error_arm_keeps_dst_value_and_ttl() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(a![b"dst", b"v"], batch, None, &mut out)
      .unwrap();
    out.clear();
    s.network_expire(ExpireCmd::Pexpire, a![b"dst", b"600000"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    s.set_add(a![b"k1", b"a"], batch, &mut out).unwrap();
    s.set_add(a![b"k2", b"b"], batch, &mut out).unwrap();
    s.network_set(a![b"str", b"plain"], batch, None, &mut out)
      .unwrap();

    // 形 a：SINTERSTORE dst 首键缺失＋尾键 string
    out.clear();
    s.set_intersect_store(a![b"dst", b"noseed", b"str"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));
    out.clear();
    s.set_intersect_store(a![b"dst", b"k1", b"k2", b"str"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));
    out.clear();
    s.set_diff_store(a![b"dst", b"noseed", b"str"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));

    // dst 零触达：既存值与 TTL 原样保留（C# 该形 EXPIRE TimeSpan.Zero 已删）
    out.clear();
    s.network_get(a![b"dst"], batch, &mut out).unwrap();
    assert_eq!(out, b"$1\r\nv\r\n");
    out.clear();
    s.network_ttl(TtlCmd::Pttl, a![b"dst"], batch, &mut out)
      .unwrap();
    let pttl: i64 = str::from_utf8(&out[1..out.len() - 2])
      .unwrap()
      .parse()
      .unwrap();
    assert!(pttl > 0, "STORE 错误臂不得触碰 dst TTL，实际 PTTL={pttl}");
  });
}

/// 冷化执行域（object_cold_degrade.rs 同款小容量单文件装配，GC 关闭）
fn cold_env() -> (
  Runtime,
  GarnetApi,
  Arc<WedbStore<SegmentedDevice>>,
  tempfile::TempDir,
) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("set-arith.db")).unwrap());
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

/// 挂接分派器的会话（降级链路须经 session.garnet_api 挂起 SlowWait）
fn session_with(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 热键同步闭环执行（磁盘无候选，必同步应答）
fn sync_exec(
  api: &GarnetApi,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  let out = take(&mut s.output);
  assert!(!out.is_empty(), "{cmd} 热键形态必须同步闭环而非挂起");
  out
}

/// 冷键降级执行：快路径空应答挂起 SlowWait，驱动挂起体闭环回慢臂应答
fn cold_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  assert!(
    s.output.is_empty(),
    "冷键 {cmd} 应降级挂起而非同步应答：{:?}",
    String::from_utf8_lossy(&s.output)
  );
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("冷键 {cmd} 降级未挂起 SlowWait"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// §113 慢臂同形锁：三形夹具冷键强制降级至 slow.rs load_many_async 漏斗，
/// 同步/冷双臂应答逐字节全等（本族全为独占错误帧；成功路径成员序非契约，
/// 见 §136 登记口径）；STORE 冷臂错误中止后 dst 值与 TTL 零触达
#[test]
fn set_arith_shortcut_slow_arm_matches_sync_arm() {
  let (rt, api, store, _dir) = cold_env();
  let mut s = session_with(&api);

  // 建键（热闭环）：str=string、k1={a}、k2={b}（k1∩k2=∅）、dst=string＋TTL
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, a![b"str", b"plain"]),
    b"+OK\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Sadd, a![b"k1", b"a"]),
    b":1\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Sadd, a![b"k2", b"b"]),
    b":1\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, a![b"dst", b"v"]),
    b"+OK\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Pexpire, a![b"dst", b"600000"]),
    b":1\r\n"
  );

  // 同步臂（热）基线——全部独占 -WRONGTYPE
  let err = err_frame(RESP_ERR_WRONG_TYPE);
  let hot_cases: Vec<(RespCommand, Vec<&[u8]>)> = vec![
    (RespCommand::Sinter, vec![b"noseed", b"str"]),
    (RespCommand::Sdiff, vec![b"noseed", b"str"]),
    (RespCommand::Sintercard, vec![b"2", b"noseed", b"str"]),
    (RespCommand::Sinter, vec![b"k1", b"k2", b"str"]),
    (RespCommand::Sintercard, vec![b"3", b"k1", b"k2", b"str"]),
    (RespCommand::Sinterstore, vec![b"dst", b"noseed", b"str"]),
    (RespCommand::Sinterstore, vec![b"dst", b"k1", b"k2", b"str"]),
    (RespCommand::Sdiffstore, vec![b"dst", b"noseed", b"str"]),
  ];
  for (cmd, args) in &hot_cases {
    assert_eq!(
      sync_exec(&api, &mut s, *cmd, args),
      err,
      "同步臂 {cmd} 基线"
    );
  }

  // 逐形冷化：快路径降级挂 SlowWait，慢臂应答与同步臂逐字节全等
  for (cmd, args) in &hot_cases {
    rt.block_on(store.flush_and_evict_all()).unwrap();
    assert_eq!(
      cold_exec(&api, &rt, &mut s, *cmd, args),
      err,
      "慢臂 {cmd} 应与同步臂应答逐字节全等"
    );
  }

  // 冷臂错误中止后 dst 既存值与 TTL 原样保留（重新冷化后直读存储态）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Get, a![b"dst"]),
    b"$1\r\nv\r\n"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let pttl = cold_exec(&api, &rt, &mut s, RespCommand::Pttl, a![b"dst"]);
  let ms: i64 = str::from_utf8(&pttl[1..pttl.len() - 2])
    .unwrap()
    .parse()
    .unwrap();
  assert!(ms > 0, "STORE 冷臂错误中止不得触碰 dst TTL，实际 PTTL={ms}");
}

// ——— §150 集合算术族对象装载漏斗「判死异构键按空集吸收」现状锁 ———
// （票 zcode-r159c-sdiff 案一；裁决=维持 rust 侧——obj_load_custom_sync 三步
// 探针每步域读恒先过 wkv 域内 TTL 单点门（TtlGate::Due→NotFound），到期未
// 清退的 string 影子键位恒折叠 Missing 按空集静默吸收，对齐真 Redis
// 「lookupKeyRead 过期即缺席」，与自家 EXISTS/TTL 观测面自洽）
//
// C# 对照形态备查（ObjectStore/ReadMethods.cs:Reader :19-23 !ValueIsObject→
// WrongType 判型先行、:25-30 到期门后至——与自家 MainStore Reader :31 判型先
// vs UnifiedStore Reader :22 CheckExpiry 先原型内部反序，§133 在册）：
// SetOps.cs:SetDiff 首键 :892-893、尾键循环 :914-915 对判死 string 影子键回
// -WRONGTYPE（SetCommands.cs:737-740），SetDiffStore 错误臂先于 SET :860/
// EXPIRE :864（§125 锚区）dst 零触达；至主动扫描/写路径清退后两侧同收敛。
// 该窗内分叉直引 deviations.md §150 判有意偏差跳过，严禁按 C# Reader 门序
// 回改（回改穿域 TTL 单点门全栈口径并复现 §133 在册过期幽灵形）。
//
// 「物理在位、TTL 已过期未清退」残留态经 put_ttl_sync 裸写过去刻度确定性
// 构造（RESP 面 EXPIRE/PEXPIRE 过去刻度系物理删除非残留，无法自然构造；
// 先例 ttl_rmw_semantics.rs::seed_expired_ttl 与
// rename_nx_expired_residual_parity.rs::expire_residual）。

/// 判死 string 键位构造器：SET 存活值后裸写过去刻度 TTL（域读恒判死、物理
/// 残留，模拟清退窗内 string 影子）
fn seed_dead_string(batch: &wkv::BatchStoreSession<'_, wdev::SegmentedDevice>, key: &[u8]) {
  put_ttl_sync(batch, key, now_ticks() - TICKS_PER_SECOND).unwrap();
}

/// §150 同步臂锁：判死尾 string 按缺失吸收回活首集成员、判死首 string 回空
/// 数组（C# 该形 ObjectStore Reader 判型先行回 -WRONGTYPE，见上备查注）
#[test]
fn set_diff_dead_hetero_string_absorbed_as_missing() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(a![b"s1", b"a", b"b"], batch, &mut out).unwrap();
    assert_eq!(out, b":2\r\n");
    out.clear();
    s.network_set(a![b"st", b"v"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");
    seed_dead_string(batch, b"st");
    // 判死前置确证：观测面与装载漏斗同穿域内 TTL 门（§150 裁语自洽面）
    out.clear();
    s.network_exists(&[b"st"], batch, None, &mut out).unwrap();
    assert_eq!(out, b":0\r\n", "残留前提：st 须已判死");
    out.clear();
    s.network_ttl(TtlCmd::Pttl, a![b"st"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-2\r\n", "判死键 PTTL 恒 -2");

    // 判死尾键：s1 − {st 吸收为空} = s1 成员（计数帧 *2 逐字节，成员序非
    // 契约按 §136 口径排序比对）
    out.clear();
    s.set_diff(a![b"s1", b"st"], batch, &mut out).unwrap();
    assert!(
      out.starts_with(b"*2\r\n"),
      "判死尾 string 应吸收为缺失回 s1 成员: {:?}",
      String::from_utf8_lossy(&out)
    );
    let mut members = parse_bulk_array(&out);
    members.sort();
    assert_eq!(members, vec![b"a".to_vec(), b"b".to_vec()]);

    // 判死首键：Missing→首集空折叠，SDIFF st 与 SDIFF st s1 皆回空数组
    out.clear();
    s.set_diff(a![b"st"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n", "判死首 string 应回空数组");
    out.clear();
    s.set_diff(a![b"st", b"s1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n", "判死首键×活尾键亦回空数组");
  });
}

/// §150 STORE dst 终态两形锁（同步窗内面，C# 错误臂皆零触达）：
/// 尾键判死形——rust 成功臂覆写 dst 旧值并随写清 TTL 回基数；
/// 首键判死形——rust 空结果回收删 dst 回 :0
///
/// dst 预置须同域（信封 Set）方克同步闭环——异质活 string dst 于本装配
/// （with_batch 无慢臂驱动）恒沿 `SyncStoreWindow::begin` 单域不变量拒写
/// 降级（`combine_store` 回 `Ok(false)`），其覆写/回收终态由
/// `set_diff_dead_hetero_slow_arm_matches_sync_arm` 慢臂对拍锁覆盖
#[test]
fn set_diff_store_dead_hetero_dst_terminal_forms() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(a![b"s1", b"a", b"b"], batch, &mut out).unwrap();
    s.network_set(a![b"st", b"v"], batch, None, &mut out)
      .unwrap();
    seed_dead_string(batch, b"st");

    // 尾键判死形：dst 预置活 set＋TTL → 成功臂覆写集合值、TTL 随写清退
    s.set_add(a![b"dst", b"z"], batch, &mut out).unwrap();
    out.clear();
    s.network_expire(ExpireCmd::Pexpire, a![b"dst", b"600000"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    s.set_diff_store(a![b"dst", b"s1", b"st"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n", "判死尾键吸收后成功臂应回基数 2");
    out.clear();
    s.set_members(a![b"dst"], batch, &mut out).unwrap();
    let mut members = parse_bulk_array(&out);
    members.sort();
    assert_eq!(
      members,
      vec![b"a".to_vec(), b"b".to_vec()],
      "dst 须为新集合值"
    );
    out.clear();
    s.network_ttl(TtlCmd::Pttl, a![b"dst"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-1\r\n", "随写清 TTL：dst 存活无 TTL");

    // 首键判死形：空结果回收删 dst（含其 TTL）回 :0
    s.set_add(a![b"dst2", b"w"], batch, &mut out).unwrap();
    out.clear();
    s.network_expire(ExpireCmd::Pexpire, a![b"dst2", b"600000"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");
    out.clear();
    s.set_diff_store(a![b"dst2", b"st", b"s1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n", "判死首键空结果应回 :0");
    out.clear();
    s.network_exists(&[b"dst2"], batch, None, &mut out).unwrap();
    assert_eq!(out, b":0\r\n", "空结果回收须删 dst");
  });
}

/// §150 快慢双臂对拍锁：判死 string 残留冷键强制降级至 slow.rs
/// load_many_async 漏斗，与同步臂同形同答（成功路径成员序非契约按 §136
/// 口径校基数＋排序成员；空数组/:N/错误帧逐字节全等）
#[test]
fn set_diff_dead_hetero_slow_arm_matches_sync_arm() {
  let (rt, api, store, _dir) = cold_env();
  let mut s = session_with(&api);

  // 建键（热闭环）：s1={a,b}、st=活 string、dst2 预置活 string＋TTL
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Sadd, a![b"s1", b"a", b"b"]),
    b":2\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, a![b"st", b"v"]),
    b"+OK\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Set, a![b"dst2", b"old2"]),
    b"+OK\r\n"
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Pexpire, a![b"dst2", b"600000"]),
    b":1\r\n"
  );
  // 判死残留：独立会话批裸写过去刻度（rename_nx_expired_residual_parity.rs 同款）
  {
    let sess = store.new_session().unwrap();
    let batch = sess.enter_batch();
    seed_dead_string(&batch, b"st");
  }

  // 同步臂（热）基线
  let hot = sync_exec(&api, &mut s, RespCommand::Sdiff, a![b"s1", b"st"]);
  assert!(
    hot.starts_with(b"*2\r\n"),
    "同步臂判死尾键吸收基线: {:?}",
    String::from_utf8_lossy(&hot)
  );
  assert_eq!(
    sync_exec(&api, &mut s, RespCommand::Sdiff, a![b"st"]),
    b"*0\r\n"
  );

  // 逐形冷化：慢臂应答与同步臂同形（数组按 §136 口径，帧头与整数逐字节）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let cold = cold_exec(&api, &rt, &mut s, RespCommand::Sdiff, a![b"s1", b"st"]);
  assert!(
    cold.starts_with(b"*2\r\n"),
    "慢臂判死尾键吸收应同形 *2: {:?}",
    String::from_utf8_lossy(&cold)
  );
  let (mut hm, mut cm) = (parse_bulk_array(&hot), parse_bulk_array(&cold));
  hm.sort();
  cm.sort();
  assert_eq!(hm, cm, "双臂成员集须全等");
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Sdiff, a![b"st"]),
    b"*0\r\n",
    "判死首键空数组双臂逐字节"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Sdiffstore,
      a![b"dst", b"s1", b"st"]
    ),
    b":2\r\n",
    "STORE 慢臂成功臂基数逐字节"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Pttl, a![b"dst"]),
    b":-1\r\n",
    "慢臂随写清 TTL 与同步臂同终态"
  );
  // 首键判死空结果回收删异质 dst2：dst2 为活 string，同步窗按单域不变量
  // 拒写降级，回收终态恒由慢臂漏斗（store_dest_cold_common 旧域清退）闭环
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Sdiffstore,
      a![b"dst2", b"st", b"s1"]
    ),
    b":0\r\n",
    "慢臂空结果回收删异质 dst2 回基数 0"
  );
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    cold_exec(&api, &rt, &mut s, RespCommand::Exists, a![b"dst2"]),
    b":0\r\n",
    "回收删后 dst2 冷键存在性亦归零"
  );
}

/// 窄二幂等锁（判净面钉形，与 sintercard 覆盖面一同款缺桩补钉）：输入清单
/// 同名重复集天然幂等——SDIFF k k 恒空、SDIFF s1 s2 s2 与 SDIFF s1 s2 全等
/// （rust load_many 逐位装载不去重＋retain 幂等，对位 C# SetDiff :911-926
/// 逐 occurrence ExceptWith 同幂等，双侧等价面无分叉）
#[test]
fn set_diff_duplicate_key_names_idempotent() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.set_add(a![b"kdup", b"a", b"b"], batch, &mut out).unwrap();
    s.set_add(a![b"s1d", b"a", b"c"], batch, &mut out).unwrap();
    s.set_add(a![b"s2d", b"b"], batch, &mut out).unwrap();

    // SDIFF k k → 空数组（自指全减）
    out.clear();
    s.set_diff(a![b"kdup", b"kdup"], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n", "SDIFF k k 应恒空");

    // SDIFF s1 s2 s2 ≡ SDIFF s1 s2（同进程同装载布局，逐字节全等）
    out.clear();
    s.set_diff(a![b"s1d", b"s2d"], batch, &mut out).unwrap();
    let once = take(&mut out);
    out.clear();
    s.set_diff(a![b"s1d", b"s2d", b"s2d"], batch, &mut out)
      .unwrap();
    assert_eq!(out, once, "重复尾键应逐项幂等");

    // STORE 形重复 occurrence 基数等价、零幽灵写面
    out.clear();
    s.set_diff_store(a![b"dstdup", b"s1d", b"s2d", b"s2d"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n", "重复 occurrence 基数不变");
  });
}
