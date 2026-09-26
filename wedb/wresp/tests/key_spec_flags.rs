//! 键规格标记 16 位精度回归（对标 test/standalone/Garnet.test/RespCommandTests.cs:CommandGetKeysAndFlagsTest）
//!
//! 简化规格提取面曾把 [`KeySpecificationFlags`] 强转 `u8`，bit 8 以上的
//! NOT_KEY / INCOMPLETE / VARIABLE_FLAGS 被静默抹零。本用例直接吃生产目录
//! （MIGRATE 第二规格 Flags = "RW, Access, Delete, Incomplete"），钉住高位保全。
//!
//! 在 garnet 中的相对路径: libs/server/Resp/CommandKeys? 对标 C# KeyArgumentsSpec 键位标志

use wresp::{
  catalog::{extract_keys_and_flags_from_slice, try_get_simple_resp_command_info},
  command::RespCommand,
  key_spec::KeySpecificationFlags,
};

/// C# TestCase("MIGRATE", ["127.0.0.1","6379","","0","5000","KEYS","key1","key2"])
/// 的切片参数（已剔除命令名，等价 parseState.Slice(1)）
const MIGRATE_ARGS: &[&[u8]] = &[
  b"127.0.0.1".as_slice(),
  b"6379",
  b"", // 单键位为空串：规格命中但零长参数被跳过（C# key.Length == 0 continue）
  b"0",
  b"5000",
  b"KEYS",
  b"key1",
  b"key2",
];

/// MIGRATE 高位 INCOMPLETE 标记必须在提取后原样在场
#[test]
fn extract_keys_and_flags_preserves_high_bits() {
  let info = try_get_simple_resp_command_info(RespCommand::Migrate).unwrap();
  let expected =
    KeySpecificationFlags::RW | KeySpecificationFlags::ACCESS | KeySpecificationFlags::DELETE;

  let pairs = extract_keys_and_flags_from_slice(MIGRATE_ARGS, &info.key_specs, info.is_sub_command);
  assert_eq!(pairs.len(), 2, "MIGRATE 单键位为空串，仅 KEYS 段两键");
  for &(key, flags) in pairs.as_slice() {
    assert!(
      flags.contains(KeySpecificationFlags::INCOMPLETE),
      "键 {} 丢失 INCOMPLETE（1 << 9）",
      String::from_utf8_lossy(key),
    );
    assert_eq!(flags, expected | KeySpecificationFlags::INCOMPLETE);
  }
}
