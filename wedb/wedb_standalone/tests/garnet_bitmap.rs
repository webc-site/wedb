mod support;

use support::with_batch;
use wnode::resp::bitmap::bitmap_manager_bit_op::BitmapOperation;

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
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.network_string_bit_operation(
      BitmapOperation::Not,
      &[b"bitop_not_dst", b"bitop_not_a", b"bitop_not_b"],
      batch,
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
    s.network_string_bit_operation(BitmapOperation::Not, &[dst, src], batch, &mut out)
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
    s.network_string_bit_operation(BitmapOperation::And, &[b"dst"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"-ERR wrong number of arguments\r\n");
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
