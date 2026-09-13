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
    s.network_exists(&[b"myset"], batch, &mut out).unwrap();
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
    s.network_exists(&[b"myset"], batch, &mut out).unwrap();
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
    assert_eq!(
      out,
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
    );
  });
}
