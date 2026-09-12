mod support;

use support::with_batch;

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicLPUSHAndLPOP
#[test]
fn basic_lpush_and_lpop() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let val = b"Value-0";

    let mut out = Vec::new();
    s.list_push(&[key, val], batch, &mut out, true).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_pop(&[key], batch, &mut out, true).unwrap();
    assert_eq!(out, b"$7\r\nValue-0\r\n");

    out.clear();
    s.network_exists(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicRPUSHAndRPOP
#[test]
fn basic_rpush_and_rpop() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"Value-0"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_pop(&[key], batch, &mut out, false).unwrap();
    assert_eq!(out, b"$7\r\nValue-0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicLPUSHAndLRANGE
#[test]
fn basic_lpush_and_lrange() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"val_0", b"val_1", b"val_2"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_range(&[key, b"0", b"0"], batch, &mut out).unwrap();
    assert_eq!(out, b"*1\r\n$5\r\nval_2\r\n");

    out.clear();
    s.list_range(&[key, b"-3", b"2"], batch, &mut out).unwrap();
    assert_eq!(out, b"*3\r\n$5\r\nval_2\r\n$5\r\nval_1\r\n$5\r\nval_0\r\n");

    out.clear();
    s.list_range(&[key, b"-100", b"100"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*3\r\n$5\r\nval_2\r\n$5\r\nval_1\r\n$5\r\nval_0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicRPUSHAndLINDEX
#[test]
fn basic_rpush_and_lindex() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"val_0", b"val_1", b"val_2"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_index(&[key, b"0"], batch, &mut out).unwrap();
    assert_eq!(out, b"$5\r\nval_0\r\n");

    out.clear();
    s.list_index(&[key, b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"$5\r\nval_2\r\n");

    out.clear();
    s.list_index(&[key, b"99"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicRPUSHAndLINSERT
#[test]
fn basic_rpush_and_linsert() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"val_0", b"val_1"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.list_insert(&[key, b"BEFORE", b"val_1", b"val_mid"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_range(&[key, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(
      out,
      b"*3\r\n$5\r\nval_0\r\n$7\r\nval_mid\r\n$5\r\nval_1\r\n"
    );
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicRPUSHAndLREM
#[test]
fn basic_rpush_and_lrem() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"val_0", b"val_1", b"val_0"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_remove(&[key, b"1", b"val_0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_range(&[key, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*2\r\n$5\r\nval_1\r\n$5\r\nval_0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicLPUSHAndLTRIM
#[test]
fn basic_lpush_and_ltrim() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"val_0", b"val_1", b"val_2"], batch, &mut out, false)
      .unwrap();

    out.clear();
    s.list_trim(&[key, b"1", b"2"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    s.list_range(&[key, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*2\r\n$5\r\nval_1\r\n$5\r\nval_2\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:CanDoRPopLPush
#[test]
fn can_do_rpop_lpush() {
  with_batch(|s, batch| {
    let key = b"mylist";
    let mut out = Vec::new();
    s.list_push(
      &[key, b"Value-one", b"Value-two", b"Value-three"],
      batch,
      &mut out,
      false,
    )
    .unwrap();

    out.clear();
    s.list_right_pop_left_push(&[b"mylist", b"myotherlist"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$11\r\nValue-three\r\n");

    out.clear();
    s.list_range(&[b"myotherlist", b"0", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n$11\r\nValue-three\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:LMoveSameKeySingletonReturnsCorrectValue
#[test]
fn lmove_same_key_singleton_returns_correct_value() {
  with_batch(|s, batch| {
    let key = b"singleton_list";
    let mut out = Vec::new();
    s.list_push(&[key, b"only_elem"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // LMOVE 同键单元素旋转（例如 LEFT RIGHT 或 RIGHT LEFT）
    out.clear();
    s.list_move(&[key, key, b"LEFT", b"RIGHT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$9\r\nonly_elem\r\n");

    // 验证元素仍完整存在
    out.clear();
    s.list_range(&[key, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*1\r\n$9\r\nonly_elem\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:LMoveDestinationWrongTypeDoesNotCorruptSource
#[test]
fn lmove_destination_wrong_type_does_not_corrupt_source() {
  with_batch(|s, batch| {
    let src = b"lmove_src";
    let dst = b"lmove_dst_str";
    let mut out = Vec::new();

    // 目标键存字符串
    s.network_set(&[dst, b"str_val"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    // 源键推入元素
    out.clear();
    s.list_push(&[src, b"elem1", b"elem2"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // LMOVE 目标键类型错误，应返回 WRONGTYPE 且不损坏源键
    out.clear();
    s.list_move(&[src, dst, b"LEFT", b"RIGHT"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
    );

    // 源键元素完整保留
    out.clear();
    s.list_range(&[src, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*2\r\n$5\r\nelem1\r\n$5\r\nelem2\r\n");
  });
}
