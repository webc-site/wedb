use std::iter::repeat_n;

use wbitmap::bit_op::BitmapOperation;
use wnode_test::{err_frame, with_batch};
use wresp::cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR;

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapSetBitResponseTest
#[test]
fn bitmap_set_bit_response_test() {
  with_batch(|s, batch| {
    let key = b"setResponseTest";
    let offsets: &[&[u8]] = &[b"7", b"14", b"37", b"144", b"777", b"1444", b"9999"];

    let mut out = Vec::new();
    for &off in offsets {
      out.clear();
      s.network_string_set_bit(&[key, off, b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");
    }

    for &off in offsets {
      out.clear();
      s.network_string_set_bit(&[key, off, b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");
    }

    out.clear();
    s.network_string_get_bit(&[key, b"7"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.network_string_get_bit(&[key, b"8"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.network_string_get_bit(&[key, b"14"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.network_string_get_bit(&[key, b"15"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.network_string_get_bit(&[key, b"37"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.network_string_get_bit(&[key, b"42"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapSetBitBoundaryValidationNegativeTest
#[test]
fn bitmap_set_bit_boundary_validation_negative_test() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_string_set_bit(
      &[b"setbit_boundary_neg", b"4294967296", b"1"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(
      out,
      b"-ERR bit offset is not an integer or out of range\r\n"
    );
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapBitOpNotRejectsMultipleSourceKeys
#[test]
fn bitmap_bit_op_not_rejects_multiple_source_keys() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"bitop_not_a", b"ab"], batch, &mut out)
      .unwrap();
    out.clear();
    s.network_set(&[b"bitop_not_b", b"abcdefghij"], batch, &mut out)
      .unwrap();

    out.clear();
    s.network_string_bit_operation(
      BitmapOperation::Not,
      &[b"bitop_not_dst", b"bitop_not_a"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.network_string_bit_operation(
      BitmapOperation::Not,
      &[b"bitop_not_dst", b"bitop_not_a", b"bitop_not_b"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(
      out,
      b"-ERR BITOP NOT must be called with a single source key.\r\n"
    );
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapGetBitResponseTest
#[test]
fn bitmap_get_bit_response_test() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    for i in 0..32 {
      out.clear();
      let off = i.to_string();
      s.network_string_get_bit(&[b"getResponseTest", off.as_bytes()], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");
    }

    out.clear();
    s.network_string_get_bit(&[b"getResponseTest", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR bit offset is not an integer or out of range\r\n"
    );
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapSetGetBitResponseTest
#[test]
fn bitmap_set_get_bit_response_test() {
  with_batch(|s, batch| {
    let key = b"setGetResponseTest";
    let mut out = Vec::new();

    for i in (0..32).step_by(2) {
      out.clear();
      let off = i.to_string();
      s.network_string_set_bit(&[key, off.as_bytes(), b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");
    }

    for i in (0..32).step_by(2) {
      out.clear();
      let off = i.to_string();
      s.network_string_get_bit(&[key, off.as_bytes()], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");
    }
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapBitCountSimpleTest
#[test]
fn bitmap_bit_count_simple_test() {
  with_batch(|s, batch| {
    let key = b"mykey";
    let mut out = Vec::new();
    s.network_set(&[key, b"foobar"], batch, &mut out).unwrap();

    out.clear();
    s.network_string_bit_count(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":26\r\n");

    out.clear();
    s.network_string_bit_count(&[key, b"0", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":4\r\n");

    out.clear();
    s.network_string_bit_count(&[key, b"1", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":6\r\n");

    out.clear();
    s.network_string_bit_count(&[key, b"1", b"1", b"BYTE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":6\r\n");

    out.clear();
    s.network_string_bit_count(&[key, b"5", b"30", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":17\r\n");

    out.clear();
    s.network_string_bit_count(&[key, b"16", b"22", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":5\r\n");

    out.clear();
    s.network_string_bit_count(&[key, b"-30", b"-5", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":14\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapBitPosFixedTests
#[test]
fn bitmap_bit_pos_fixed_tests() {
  with_batch(|s, batch| {
    let key = b"mykey";
    let val = [0x00, 0xff, 0xf0];
    let mut out = Vec::new();
    s.network_set(&[key, &val], batch, &mut out).unwrap();

    out.clear();
    s.network_string_bit_position(&[key, b"1", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":8\r\n");

    out.clear();
    s.network_string_bit_position(&[key, b"1", b"2", b"-1", b"BYTE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":16\r\n");

    out.clear();
    s.network_string_bit_position(&[key, b"1", b"0", b"0", b"BYTE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-1\r\n");

    out.clear();
    s.network_string_bit_position(&[key, b"0", b"0", b"0", b"BYTE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    out.clear();
    s.network_string_bit_position(&[key, b"1", b"7", b"15", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":8\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitOp_Unary_BitwiseNot
#[test]
fn bit_op_unary_bitwise_not() {
  with_batch(|s, batch| {
    let src = b"src";
    let dst = b"dst";
    let val = [0b1010_1010u8, 0b0101_0101];

    let mut out = Vec::new();
    s.network_set(&[src, &val], batch, &mut out).unwrap();

    out.clear();
    s.network_string_bit_operation(BitmapOperation::Not, &[dst, src], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.network_get(&[dst], batch, &mut out).unwrap();
    assert_eq!(out, b"$2\r\n\x55\xaa\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapBitfieldGetTest
#[test]
fn bitmap_bitfield_get_test() {
  with_batch(|s, batch| {
    let key = b"BitmapBitFieldGetTest";
    let mut out = Vec::new();
    s.string_bit_field(&[key, b"SET", b"u4", b"1", b"15"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n");

    out.clear();
    s.string_bit_field(&[key, b"GET", b"u4", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:15\r\n");

    out.clear();
    s.string_bit_field(&[key, b"GET", b"i4", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:-1\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapBitfieldSetTest
#[test]
fn bitmap_bitfield_set_test() {
  with_batch(|s, batch| {
    let key = b"BitmapBitFieldSetTest";
    let mut out = Vec::new();
    s.string_bit_field(&[key, b"SET", b"u8", b"0", b"200"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n");

    out.clear();
    s.string_bit_field(&[key, b"SET", b"u8", b"0", b"250"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:200\r\n");

    out.clear();
    s.string_bit_field(&[key, b"GET", b"u8", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:250\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapOperationNonExistentSourceKeys
#[test]
fn bitmap_operation_non_existent_source_keys() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_string_bit_operation(
      BitmapOperation::And,
      &[b"dstKey", b"a", b"b", b"c"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapOperationInvalidOption
#[test]
fn bitmap_operation_invalid_option() {
  with_batch(|s, batch| {
    // 缺少源键或操作类型非法
    let mut out = Vec::new();
    s.network_string_bit_operation(BitmapOperation::And, &[b"dst"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR wrong number of arguments for command\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitFieldWithoutSubcommandsReturnsEmptyArray
#[test]
fn bitfield_without_subcommands_returns_empty_array() {
  with_batch(|s, batch| {
    let key = b"bitfield_no_sub";
    let mut out = Vec::new();

    // BITFIELD key
    s.string_bit_field(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b"*0\r\n");

    // BITFIELD_RO key
    out.clear();
    s.string_bit_field_read_only(&[key], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");

    // BITFIELD key OVERFLOW SAT
    out.clear();
    s.string_bit_field(&[key, b"OVERFLOW", b"SAT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitFieldMaxOffsetGetAsync
#[test]
fn bitfield_max_offset_get() {
  with_batch(|s, batch| {
    let key = b"bitfield_max_offset";
    let mut out = Vec::new();

    // 缺失键边界 GET
    s.string_bit_field(
      &[b"bitfield_max_offset_missing", b"GET", b"u1", b"4294967295"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n");

    // 设置低位比特 (bit 7)
    out.clear();
    s.network_string_set_bit(&[key, b"7", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // 边界比特 GET (bit 4294967295) -> 0
    out.clear();
    s.string_bit_field(&[key, b"GET", b"u1", b"4294967295"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n");

    // 回读已置位比特 (bit 7) -> 1
    out.clear();
    s.string_bit_field(&[key, b"GET", b"u1", b"7"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:1\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapBitCountNegativeOffsetClampingTest
#[test]
fn bitmap_bit_count_negative_offset_clamping_test() {
  with_batch(|s, batch| {
    let key = b"BitmapBitCountNegativeOffsetClampingTest";
    let val = [0x80, 0x01];
    let mut out = Vec::new();
    s.network_set(&[key, &val], batch, &mut out).unwrap();

    // BITCOUNT key -2 -2 BYTE => 1
    out.clear();
    s.network_string_bit_count(&[key, b"-2", b"-2", b"BYTE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // BITCOUNT key -3 -1 BYTE => 2
    out.clear();
    s.network_string_bit_count(&[key, b"-3", b"-1", b"BYTE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // BITCOUNT key -1 -2 BYTE => 0
    out.clear();
    s.network_string_bit_count(&[key, b"-1", b"-2", b"BYTE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // BITCOUNT key -16 -16 BIT => 1
    out.clear();
    s.network_string_bit_count(&[key, b"-16", b"-16", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // BITCOUNT key -17 -1 BIT => 2
    out.clear();
    s.network_string_bit_count(&[key, b"-17", b"-1", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // BITCOUNT key -1 -2 BIT => 0
    out.clear();
    s.network_string_bit_count(&[key, b"-1", b"-2", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.complexstring/GarnetBitmapTests.cs:BitmapBitPosNegativeOffsetClampingTest
#[test]
fn bitmap_bit_pos_negative_offset_clamping_test() {
  with_batch(|s, batch| {
    let key = b"BitmapBitPosNegativeOffsetClampingTest";
    let val = [0x80, 0x01];
    let mut out = Vec::new();
    s.network_set(&[key, &val], batch, &mut out).unwrap();

    // BITPOS key 1 -2 -2 BYTE => 0
    out.clear();
    s.network_string_bit_position(&[key, b"1", b"-2", b"-2", b"BYTE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // BITPOS key 1 -3 -2 BYTE => 0
    out.clear();
    s.network_string_bit_position(&[key, b"1", b"-3", b"-2", b"BYTE"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // BITPOS key 1 -16 -16 BIT => 0
    out.clear();
    s.network_string_bit_position(&[key, b"1", b"-16", b"-16", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // BITPOS key 1 -17 -16 BIT => 0
    out.clear();
    s.network_string_bit_position(&[key, b"1", b"-17", b"-16", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// 对应 libs/server/Resp/Bitmap/BitmapCommands.cs 的 NetworkStringBitCount
///（行为测试，函数级映射声明归实现 resp/bitmap/bitmap_commands.rs）
/// BITCOUNT 可选区间与 BYTE/BIT 模式的负向分支：非法标志回语法错误、
/// 非整数区间回值错误、参数形态仅允许 1/3/4
#[test]
fn bitmap_bit_count_range_flags_and_arity() {
  with_batch(|s, batch| {
    let key = b"bitcount_flags";
    let mut out = Vec::new();
    s.network_set(&[key, b"foobar"], batch, &mut out).unwrap();

    // 非法标志（大小写不敏感匹配仅认 BIT/BYTE）→ ERR syntax error
    out.clear();
    s.network_string_bit_count(&[key, b"0", b"-1", b"FOO"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // BIT/BYTE 大小写不敏感：BIT 口径 [0,7] 与 BYTE 口径 [0,0] 同为第 0 字节
    out.clear();
    s.network_string_bit_count(&[key, b"0", b"7", b"bit"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":4\r\n");

    out.clear();
    s.network_string_bit_count(&[key, b"0", b"0", b"Byte"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":4\r\n");

    // BIT 口径单点位 [0,0]：'f' = 0b0110_0110 的 bit0 = 0
    out.clear();
    s.network_string_bit_count(&[key, b"0", b"0", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // start/end 非整数
    out.clear();
    s.network_string_bit_count(&[key, b"a", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

    // 参数形态仅 1/3/4：2 参与 5 参均回参数个数错误
    out.clear();
    s.network_string_bit_count(&[key, b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'BITCOUNT' command\r\n"
    );

    out.clear();
    s.network_string_bit_count(&[key, b"0", b"-1", b"BIT", b"X"], batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'BITCOUNT' command\r\n"
    );
  });
}

/// 对应 libs/server/Resp/Bitmap/BitmapCommands.cs 的 NetworkStringBitPosition
///（行为测试，函数级映射声明归实现 resp/bitmap/bitmap_commands.rs）
/// BITPOS 合法位偏移区间之外的 start/end 直接回 -1（不触存储），
/// 非法标志回语法错误，bit 参数须单字符
#[test]
fn bitmap_bit_pos_out_of_range_and_flags() {
  with_batch(|s, batch| {
    let key = b"bitpos_flags";
    let mut out = Vec::new();
    s.network_set(&[key, b"foobar"], batch, &mut out).unwrap();

    // start 超出字节口径上界（MaxBitmapPayloadBytes-1）→ 越界即 -1
    out.clear();
    s.network_string_bit_position(&[key, b"1", b"536870912", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-1\r\n");

    // start 负向越界（-MaxBitmapPayloadBytes-1）→ -1
    out.clear();
    s.network_string_bit_position(&[key, b"1", b"-536870913", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-1\r\n");

    // BIT 口径界为位上限（MaxOffsetForBitmapLength+1），界内不触发越界短路
    out.clear();
    s.network_string_bit_position(&[key, b"1", b"536870911", b"-1", b"BIT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":-1\r\n");

    // 非法标志 → ERR syntax error
    out.clear();
    s.network_string_bit_position(&[key, b"1", b"0", b"-1", b"FOO"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // bit 参数非单字符 0/1
    out.clear();
    s.network_string_bit_position(&[key, b"2", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR bit is not an integer or out of range\r\n");

    // start 非整数
    out.clear();
    s.network_string_bit_position(&[key, b"1", b"x", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
  });
}

/// 对应 libs/server/Resp/Bitmap/BitmapCommands.cs 的 NetworkStringBitOperation
///（行为测试，函数级映射声明归实现 resp/bitmap/bitmap_commands.rs）
/// BITOP DIFF：源键不足两个报错、语义为首源对其余源按位清除、
/// 源键含 destkey 超出 64 个报错
#[test]
fn bitmap_bit_op_diff_and_key_limit() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    s.network_set(&[b"bitop_diff_a", b"\xf0\xaa"], batch, &mut out)
      .unwrap();
    s.network_set(&[b"bitop_diff_b", b"\x0f\x55"], batch, &mut out)
      .unwrap();

    // DIFF 仅一个源键 → 专有错误
    out.clear();
    s.network_string_bit_operation(
      BitmapOperation::Diff,
      &[b"bitop_diff_dst", b"bitop_diff_a"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(
      out,
      b"-ERR BITOP DIFF must be called with at least two source keys.\r\n"
    );

    // DIFF 双源：dst = a &~ b
    out.clear();
    s.network_string_bit_operation(
      BitmapOperation::Diff,
      &[b"bitop_diff_dst", b"bitop_diff_a", b"bitop_diff_b"],
      batch,
      None,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.network_get(&[b"bitop_diff_dst"], batch, &mut out)
      .unwrap();
    // 0xF0 &~ 0x0F = 0xF0，0xAA &~ 0x55 = 0xAA
    assert_eq!(out, b"$2\r\n\xf0\xaa\r\n");

    // destkey + 65 源 = 66 参数 > 64 → 键上限错误
    out.clear();
    let mut args = vec![b"bitop_limit_dst" as &[u8]];
    args.extend(repeat_n(b"k" as &[u8], 65));
    s.network_string_bit_operation(BitmapOperation::Or, &args, batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR Bitop source key limit (64) exceeded\r\n");

    // destkey + 63 源 = 64 参数恰在上限内：全缺失源回 0
    out.clear();
    let mut args = vec![b"bitop_limit_dst" as &[u8]];
    args.extend(repeat_n(b"missing_key" as &[u8], 63));
    s.network_string_bit_operation(BitmapOperation::Or, &args, batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// 对应 libs/server/Resp/Bitmap/BitmapCommands.cs 的 StringBitFieldReadOnly
///（行为测试，函数级映射声明归实现 resp/bitmap/bitmap_commands.rs）
/// BITFIELD_RO 仅允许 GET：SET/INCRBY/OVERFLOW 一律回语法错误；
/// BITFIELD 的 OVERFLOW 三策略与 INCRBY 组合对标
#[test]
fn bitfield_ro_read_only_and_overflow_policies() {
  with_batch(|s, batch| {
    let key = b"bitfield_ro_policies";
    let mut out = Vec::new();

    // BITFIELD_RO 拒绝 SET / OVERFLOW
    out.clear();
    s.string_bit_field_read_only(&[key, b"SET", b"u8", b"0", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    out.clear();
    s.string_bit_field_read_only(&[key, b"OVERFLOW", b"SAT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));

    // BITFIELD_RO 正常 GET（缺失键回 0）
    out.clear();
    s.string_bit_field_read_only(&[key, b"GET", b"u8", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n");

    // OVERFLOW SAT：INCRBY u8 0 250 → 饱和至 255
    out.clear();
    s.string_bit_field(
      &[key, b"INCRBY", b"u8", b"0", b"250", b"OVERFLOW", b"SAT"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:250\r\n");

    out.clear();
    s.string_bit_field(
      &[key, b"INCRBY", b"u8", b"0", b"250", b"OVERFLOW", b"SAT"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:255\r\n");

    // OVERFLOW FAIL：i8 127 自增 1 溢出 → 应答 nil（RESP2 $-1）；
    // C# IncrementBitfield 溢出时仍把 newValue=0 写入（仅应答替换为 nil）
    let fail_key = b"bitfield_fail_policy";
    out.clear();
    s.string_bit_field(&[fail_key, b"SET", b"i8", b"0", b"127"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n");

    out.clear();
    s.string_bit_field(
      &[fail_key, b"INCRBY", b"i8", b"0", b"1", b"OVERFLOW", b"FAIL"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n$-1\r\n");

    // FAIL 溢出后域内值已被写为 0（对标 C# 写 0 语义）
    out.clear();
    s.string_bit_field_read_only(&[fail_key, b"GET", b"i8", b"0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n");

    // OVERFLOW WRAP：重置 127 后 +1 回绕至 -128
    out.clear();
    s.string_bit_field(&[fail_key, b"SET", b"i8", b"0", b"127"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n:0\r\n");

    out.clear();
    s.string_bit_field(
      &[fail_key, b"INCRBY", b"i8", b"0", b"1", b"OVERFLOW", b"WRAP"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:-128\r\n");
  });
}
