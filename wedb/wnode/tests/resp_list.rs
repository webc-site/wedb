use wnode_test::{err_frame, with_batch};
use wresp::cmd_strings::{RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_WRONG_TYPE};

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
    s.network_exists(&[key], batch, None, &mut out).unwrap();
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
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));

    // 源键元素完整保留
    out.clear();
    s.list_range(&[src, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*2\r\n$5\r\nelem1\r\n$5\r\nelem2\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:LPOSWithoutOptions
#[test]
fn lpos_without_options() {
  with_batch(|s, batch| {
    let key = b"KeyA";
    let mut out = Vec::new();
    s.list_push(&[key, b"a", b"c", b"b", b"c", b"d"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":5\r\n");

    // 命中：首现下标
    out.clear();
    s.list_position(&[key, b"c"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_position(&[key, b"d"], batch, &mut out).unwrap();
    assert_eq!(out, b":4\r\n");

    // 未命中（缺省 COUNT）：RESP2 null
    out.clear();
    s.list_position(&[key, b"nx"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:LPOSWithInvalidKey
#[test]
fn lpos_with_invalid_key() {
  with_batch(|s, batch| {
    let key = b"KeyA";
    let mut out = Vec::new();

    // 键缺失：无 COUNT → null；含 COUNT → 空数组（命令层 NOTFOUND 分支）
    s.list_position(&[key, b"nx"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");

    out.clear();
    s.list_position(&[key, b"nx", b"COUNT", b"3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");

    // 键存在但元素不匹配：同样 null / 空数组
    out.clear();
    s.list_push(&[key, b"e"], batch, &mut out, true).unwrap();
    out.clear();
    s.list_position(&[key, b"nx"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");

    out.clear();
    s.list_position(&[key, b"nx", b"COUNT", b"3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");

    out.clear();
    s.list_position(&[key, b"nx", b"cOuNt", b"3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");

    // RESP3：缺省形态无匹配回 `_`（C# RespMemoryWriter.WriteNull 按版本）
    s.resp_protocol_version = 3;
    out.clear();
    s.list_position(&[key, b"nx"], batch, &mut out).unwrap();
    assert_eq!(out, b"_\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:LPOSWithOptions
///（rank/count/maxlen 组合；列表 [a,c,b,c,d]）
#[test]
fn lpos_with_options() {
  with_batch(|s, batch| {
    let key = b"KeyA";
    let mut out = Vec::new();
    s.list_push(&[key, b"a", b"c", b"b", b"c", b"d"], batch, &mut out, false)
      .unwrap();

    // 词元双形态：全大写 / 全小写（C# SequenceEqual RANK|rank）
    for rank_token in [b"RANK".as_slice(), b"rank"] {
      out.clear();
      s.list_position(&[key, b"c", rank_token, b"2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":3\r\n");

      out.clear();
      s.list_position(&[key, b"c", rank_token, b"-2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");

      out.clear();
      s.list_position(&[key, b"c", rank_token, b"3"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$-1\r\n");
    }

    // COUNT 形态：数组回复；count 0 = 全量
    for count_token in [b"COUNT".as_slice(), b"count"] {
      out.clear();
      s.list_position(&[key, b"c", count_token, b"2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*2\r\n:1\r\n:3\r\n");

      out.clear();
      s.list_position(&[key, b"c", count_token, b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*2\r\n:1\r\n:3\r\n");

      out.clear();
      s.list_position(&[key, b"c", count_token, b"9"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*2\r\n:1\r\n:3\r\n");
    }

    // MAXLEN 截断扫描
    out.clear();
    s.list_position(&[key, b"c", b"MAXLEN", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_position(&[key, b"c", b"maxlen", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // rank -1 + COUNT 0：自尾全量（顺序 3,1）
    out.clear();
    s.list_position(
      &[key, b"c", b"rank", b"-1", b"count", b"0"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*2\r\n:3\r\n:1\r\n");

    // 混合大小写词元匹配（eq_ignore_ascii_case）
    out.clear();
    s.list_position(&[key, b"c", b"Rank", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_position(&[key, b"c", b"rAnK", b"-2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_position(&[key, b"c", b"cOuNt", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n:3\r\n");

    out.clear();
    s.list_position(&[key, b"c", b"MaxLen", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_position(
      &[key, b"c", b"Rank", b"-1", b"cOuNt", b"0", b"MaxLen", b"2"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:3\r\n");

    // 非法选项仍报语法错误
    for opts in [
      [b"UNKNOWN".as_slice(), b"1".as_slice()],
      [b"Rankk", b"1"],
      [b"cOuNtX", b"2"],
      [b"Max_Len", b"3"],
    ] {
      out.clear();
      s.list_position(&[key, b"c", opts[0], opts[1]], batch, &mut out)
        .unwrap();
      assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));
    }
  });
}
