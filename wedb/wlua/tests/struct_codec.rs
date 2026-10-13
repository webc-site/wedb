#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! struct.pack / unpack / size 编解码层集成测试。

use wlua::functions_struct::{
  Endianness, StructValue, get_to_align, struct_pack, struct_size, struct_unpack,
  try_decode_double, try_encode_double,
};

#[test]
fn pack_unpack_roundtrip() {
  // 小端 i32 + i8。
  let packed = struct_pack(
    b"<ib",
    &[
      StructValue::Number(305_419_896.0),
      StructValue::Number(-1.0),
    ],
  )
  .unwrap();
  assert_eq!(packed.len(), 5);
  assert_eq!(packed[..4], 0x1234_5678i32.to_le_bytes());
  assert_eq!(packed[4], 0xff);

  let out = struct_unpack(b"<ib", &packed).unwrap();
  assert_eq!(
    out.values,
    vec![
      StructValue::Number(305_419_896.0),
      StructValue::Number(-1.0)
    ]
  );
  assert_eq!(out.consumed, 5);
}

#[test]
fn sized_integer_suffix_is_byte_width() {
  // C# TryGetNum：`i8` 后缀为字节尺寸（非元素个数）。
  assert_eq!(struct_size(b"<i8"), Some(8));
  assert_eq!(struct_size(b"i"), Some(4));
  assert_eq!(struct_size(b"i33"), None, "超过 MAXINTSIZE=32 报错");

  let packed = struct_pack(b">I2", &[StructValue::Number(4660.0)]).unwrap();
  assert_eq!(packed, vec![0x12, 0x34]);
  let out = struct_unpack(b">I2", &packed).unwrap();
  assert_eq!(out.values, vec![StructValue::Number(4660.0)]);
}

#[test]
fn sign_extension_by_token_case() {
  // 小写符号扩展 / 大写零扩展。
  let out = struct_unpack(b"B", &[0xff]).unwrap();
  assert_eq!(out.values, vec![StructValue::Number(255.0)]);
  let out = struct_unpack(b"h", &[0xff, 0xff]).unwrap();
  assert_eq!(out.values, vec![StructValue::Number(-1.0)]);
  let out = struct_unpack(b"H", &[0xff, 0xff]).unwrap();
  assert_eq!(out.values, vec![StructValue::Number(65535.0)]);
}

#[test]
fn double_preserves_fraction() {
  let packed = struct_pack(b"d", &[StructValue::Number(1.5)]).unwrap();
  let out = struct_unpack(b"d", &packed).unwrap();
  assert_eq!(out.values, vec![StructValue::Number(1.5)]);
}

#[test]
fn string_family() {
  // c 定长：写入原样、不足报错。
  let packed = struct_pack(b"c4", &[StructValue::Bytes(b"abcd".to_vec())]).unwrap();
  assert_eq!(packed, b"abcd");
  assert!(struct_pack(b"c4", &[StructValue::Bytes(b"ab".to_vec())]).is_none());
  // c0 解包：尺寸取自上一个已解码数值（数值本身被消费，C# TryDecodeCharacter 的 Remove 形态）。
  let out = struct_unpack(b"Ic0", &[4, 0, 0, 0, b'a', b'b', b'c', b'd']).unwrap();
  assert_eq!(out.values, vec![StructValue::Bytes(b"abcd".to_vec())]);
  assert_eq!(out.consumed, 8);
  // s 解包：NUL 终结，消费量含 NUL。
  let out = struct_unpack(b"s", b"abc\0rest").unwrap();
  assert_eq!(out.values, vec![StructValue::Bytes(b"abc".to_vec())]);
  assert_eq!(out.consumed, 4);
  // s 打包：整串 + 1 字节填充。
  let packed = struct_pack(b"s", &[StructValue::Bytes(b"ab".to_vec())]).unwrap();
  assert_eq!(packed, b"ab\0");
  // struct.size 不支持 s / c0。
  assert_eq!(struct_size(b"s"), None);
  assert_eq!(struct_size(b"c0"), None);
}

#[test]
fn struct_size_and_align() {
  assert_eq!(struct_size(b"<ibB"), Some(6));
  assert_eq!(struct_size(b"x"), Some(1));
  assert_eq!(struct_size(b"c10"), Some(10));
  // `!` 对齐：b(1B) 后 i 补 3 字节至 4 边界。
  assert_eq!(struct_size(b"!4 bi"), Some(8));
  // 端序控制符。
  assert_eq!(struct_size(b">"), Some(0));
  // 未知字母（C# StructSize 对 size 0 的字母静默跳过）。
  assert_eq!(struct_size(b"z"), Some(0));
}

#[test]
fn unpack_reports_short_data() {
  assert!(struct_unpack(b"i", &[1, 2, 3]).is_none());
  assert!(struct_unpack(b"s", b"no-null").is_none());
  assert!(struct_unpack(b"q", &[0]).is_none(), "未知字母报错");
}

#[test]
fn big_endian_doubles() {
  let mut out = Vec::new();
  try_encode_double(&mut out, 1.5, Endianness::Big);
  assert_eq!(out, 1.5f64.to_be_bytes());
  assert_eq!(try_decode_double(&out, 0, Endianness::Big), Some(1.5));
}

#[test]
fn alignment_padding_positions() {
  // `!4`：i(4B) + c(2B) 后 d 对齐至 8 偏移（6 → 8 填 2 字节）。
  let format = b"!4 icd";
  assert_eq!(struct_size(format), Some(16));
  let packed = struct_pack(
    format,
    &[
      StructValue::Number(1.0),
      StructValue::Bytes(b"ab".to_vec()),
      StructValue::Number(1.5),
    ],
  )
  .unwrap();
  assert_eq!(packed.len(), 16);
  let out = struct_unpack(format, &packed).unwrap();
  assert_eq!(out.consumed, 16);
  assert_eq!(out.values[2], StructValue::Number(1.5));
}

#[test]
fn get_to_align_matches_csharp_bit_form() {
  // C# GetToAlign：(size - (len & (size-1))) & (size-1)。
  assert_eq!(get_to_align(0, 4, b'i', 4), 0);
  assert_eq!(get_to_align(5, 4, b'i', 4), 3);
  assert_eq!(get_to_align(5, 4, b'c', 3), 0, "c 不对齐");
  assert_eq!(get_to_align(5, 1, b'i', 4), 0, "默认不对齐");
}

#[test]
fn array_decimal_reads() {
  // i4 = 4 字节整数（非 4 个 i）。
  let packed = struct_pack(b"i4", &[StructValue::Number(-2.0)]).unwrap();
  assert_eq!(packed, (-2i32).to_le_bytes());
  let out = struct_unpack(b"i4", &packed).unwrap();
  assert_eq!(out.values, vec![StructValue::Number(-2.0)]);
  assert_eq!(out.consumed, 4);
}
