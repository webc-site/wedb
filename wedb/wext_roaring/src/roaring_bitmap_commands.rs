//! R.* 四条命令执行体（对标 modules/RoaringBitmap/RoaringBitmapCommands.cs）
//!
//! C# 每条命令为独立 `CustomObjectFunctions` 子类；Rust 以
//! [`wcustom::CustomObjectFns`] 函数指针集承接同一四接口形态：
//! - RSetBit：NeedInitialUpdate（防空墓碑校验）+ Updater（读改写）
//! - RGetBit / RBitCount / RBitPos：Reader（命中只读）+ NotFound（读不建键）
//!
//! 载荷域为对象信封载荷字节：空载荷 = 键缺失新建初值
//!（C# 工厂 `RoaringBitmapFactory.Create` 的零载荷承接）。

use core::str;

use wcustom::{CommandType, CustomCommandInfo, CustomObjectFns};
use wresp::{RespVecExt, cmd_strings::write_error_raw};

use crate::roaring_bitmap_object::RoaringBitmapObject;

/// 校验失败文案（逐字节对标 C# 各命令类私有 ReadOnlySpan 错误串）
const ERR_OFFSET: &str = "ERR bit offset is not an unsigned 32-bit integer";
const ERR_VALUE: &str = "ERR bit value must be 0 or 1";
const ERR_BIT: &str = "ERR bit must be 0 or 1";
const ERR_FROM: &str = "ERR from offset is not an unsigned 32-bit integer";
/// 载荷解码失败文案（C# Deserialize 异常经框架折算的通用错误承接）
const ERR_DECODE: &str = "ERR RoaringBitmap object decode failed";
const ERR_WRITE_ONLY: &str = "ERR command is write-only";
const ERR_READ_ONLY: &str = "ERR command is read-only";

/// 共享参数解析辅助（C# RoaringBitmapArgs 静态类）+ 四命令执行体
pub struct RoaringBitmapCommands;

impl RoaringBitmapCommands {
  /// 对象类型名（C# RoaringBitmapFactory 的 RegisterType 承接形态）
  pub const TYPE_NAME: &'static str = "roaringbitmap";

  /// 静态注册 RoaringBitmap 类型与 R.* 四条命令到 CustomCommandManager
  pub fn register(manager: &mut wcustom::CustomCommandManager) -> wcustom::Result<()> {
    const COMMANDS: [(&str, CommandType, i32, CustomObjectFns); 4] = [
      (
        "R.SETBIT",
        CommandType::ReadModifyWrite,
        4,
        RoaringBitmapCommands::R_SET_BIT,
      ),
      (
        "R.GETBIT",
        CommandType::Read,
        3,
        RoaringBitmapCommands::R_GET_BIT,
      ),
      (
        "R.BITCOUNT",
        CommandType::Read,
        2,
        RoaringBitmapCommands::R_BIT_COUNT,
      ),
      (
        "R.BITPOS",
        CommandType::Read,
        -3,
        RoaringBitmapCommands::R_BIT_POS,
      ),
    ];
    for (name, command_type, arity, functions) in COMMANDS {
      manager.register_object_command(
        Self::TYPE_NAME,
        name,
        command_type,
        Some(functions),
        Some(CustomCommandInfo {
          name: name.to_string(),
          arity,
          acl_categories: vec!["bitmap".to_string()],
        }),
        None,
      )?;
    }
    Ok(())
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseUInt32
  pub fn try_parse_uint32(raw: &[u8]) -> Option<u32> {
    if raw.is_empty() || raw.len() > 10 {
      return None;
    }
    let mut val: u64 = 0;
    for &b in raw {
      if !b.is_ascii_digit() {
        return None;
      }
      val = val * 10 + u64::from(b - b'0');
    }
    u32::try_from(val).ok()
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseBit
  pub fn try_parse_bit(raw: &[u8]) -> Option<bool> {
    match raw {
      b"0" => Some(false),
      b"1" => Some(true),
      _ => None,
    }
  }

  /// R.SETBIT key offset value（ReadModifyWrite，arity 4）
  ///
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RSetBit
  pub const R_SET_BIT: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::set_bit_need_initial_update,
    updater: Self::set_bit_updater,
    reader: Self::reject_write_only,
    not_found: Self::not_found_null,
    is_empty: Self::payload_is_empty,
  };

  /// R.GETBIT key offset（Read，arity 3）
  ///
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RGetBit
  pub const R_GET_BIT: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::get_bit_reader,
    not_found: Self::get_bit_not_found,
    is_empty: Self::payload_is_empty,
  };

  /// R.BITCOUNT key（Read，arity 2）
  ///
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RBitCount
  pub const R_BIT_COUNT: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::bit_count_reader,
    not_found: Self::bit_count_not_found,
    is_empty: Self::payload_is_empty,
  };

  /// R.BITPOS key bit [from]（Read，arity -3）
  ///
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RBitPos
  pub const R_BIT_POS: CustomObjectFns = CustomObjectFns {
    need_initial_update: Self::reject_read_only_initial,
    updater: Self::reject_read_only_update,
    reader: Self::bit_pos_reader,
    not_found: Self::bit_pos_not_found,
    is_empty: Self::payload_is_empty,
  };

  // ---- 载荷编解码（C# 工厂 Create/SerializeObject/Deserialize 分层承接）----

  /// 载荷解码：空载荷 = 新建初值；非空解码失败返回 None（信封由本模块
  /// 单一写入方产出，损坏即上层落库事故，以错误帧上抛而非 panic）
  fn decode(payload: &[u8]) -> Option<RoaringBitmapObject> {
    if payload.is_empty() {
      return Some(RoaringBitmapObject::create());
    }
    RoaringBitmapObject::deserialize(&mut &payload[..]).ok()
  }

  /// 载荷重编码进缓冲（Vec 写入端无 I/O 失败面，恒 Some）
  fn encode(obj: &RoaringBitmapObject, payload: &mut Vec<u8>) -> Option<()> {
    payload.clear();
    obj.serialize_object(payload).ok()
  }

  /// 空对象判定（wedb 严格删空公理：空载荷整键回收，不落空对象信封）
  fn payload_is_empty(payload: &[u8]) -> bool {
    payload.is_empty() || Self::decode(payload).is_some_and(|obj| obj.bit_count() == 0)
  }

  /// 不可达执行槽兜底（对标 C# 基类虚方法 NotImplementedException；
  /// 路由层按 CommandType 分流后不可达，以明确错误帧替代 panic）
  fn reject_write_only(_payload: &[u8], _args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    write_error_raw(output, ERR_WRITE_ONLY);
    false
  }

  fn reject_read_only_initial(_args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    write_error_raw(output, ERR_READ_ONLY);
    false
  }

  fn reject_read_only_update(
    _payload: &mut Vec<u8>,
    _args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> bool {
    write_error_raw(output, ERR_READ_ONLY);
    false
  }

  /// RMW 侧 NotFound 不可达兜底（C# 基类缺省 WriteNull）
  fn not_found_null(_args: &[&[u8]], output: &mut Vec<u8>) {
    output.extend_from_slice(b"$-1\r\n");
  }

  // ---- R.SETBIT ----

  /// 建对象前先行校验参数（C# RSetBit 类 NeedInitialUpdate 钩子）：
  /// 畸形 R.SETBIT 不得留下空对象（防空墓碑）；
  /// 只走参数副本，Updater 仍从 offset 0 重新解析
  fn set_bit_need_initial_update(args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    let offset_arg = args.first().copied().unwrap_or(b"");
    let bit_arg = args.get(1).copied().unwrap_or(b"");
    if Self::try_parse_uint32(offset_arg).is_none() {
      write_error_raw(output, ERR_OFFSET);
      return false;
    }
    if Self::try_parse_bit(bit_arg).is_none() {
      write_error_raw(output, ERR_VALUE);
      return false;
    }
    true
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RSetBit
  ///
  /// 命令主执行体（C# RSetBit.Updater 钩子）
  fn set_bit_updater(payload: &mut Vec<u8>, args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    let offset_arg = args.first().copied().unwrap_or(b"");
    let bit_arg = args.get(1).copied().unwrap_or(b"");
    let Some(bit_offset) = Self::try_parse_uint32(offset_arg) else {
      write_error_raw(output, ERR_OFFSET);
      return false;
    };
    let Some(bit) = Self::try_parse_bit(bit_arg) else {
      write_error_raw(output, ERR_VALUE);
      return false;
    };
    let Some(mut obj) = Self::decode(payload) else {
      write_error_raw(output, ERR_DECODE);
      return false;
    };
    let previous = obj.set_bit(bit_offset, bit);
    if Self::encode(&obj, payload).is_none() {
      write_error_raw(output, ERR_DECODE);
      return false;
    }
    output.write_resp_int(previous as i64);
    true
  }

  // ---- R.GETBIT ----

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RGetBit
  ///
  /// 命令主执行体（C# RGetBit.Reader 钩子）
  fn get_bit_reader(payload: &[u8], args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    let offset_arg = args.first().copied().unwrap_or(b"");
    let Some(bit_offset) = Self::try_parse_uint32(offset_arg) else {
      write_error_raw(output, ERR_OFFSET);
      return false;
    };
    let Some(obj) = Self::decode(payload) else {
      write_error_raw(output, ERR_DECODE);
      return false;
    };
    output.write_resp_int(obj.get_bit(bit_offset) as i64);
    true
  }

  /// 缺键分支（C# RGetBit.NotFound）：校验 offset 后应答缺席位 0（读不建键）
  fn get_bit_not_found(args: &[&[u8]], output: &mut Vec<u8>) {
    let offset_arg = args.first().copied().unwrap_or(b"");
    if Self::try_parse_uint32(offset_arg).is_none() {
      write_error_raw(output, ERR_OFFSET);
    } else {
      output.write_resp_int(0);
    }
  }

  // ---- R.BITCOUNT ----

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RBitCount
  ///
  /// 命令主执行体（C# RBitCount.Reader 钩子）
  fn bit_count_reader(payload: &[u8], _args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    let Some(obj) = Self::decode(payload) else {
      write_error_raw(output, ERR_DECODE);
      return false;
    };
    output.write_resp_int(obj.bit_count());
    true
  }

  /// 缺键分支（C# RBitCount.NotFound）：应答计数 0
  fn bit_count_not_found(_args: &[&[u8]], output: &mut Vec<u8>) {
    output.write_resp_int(0);
  }

  // ---- R.BITPOS ----

  /// R.BITPOS 参数解析（C# RBitPos.TryParseArgs）：bit 与可选 from
  fn bit_pos_parse_args(args: &[&[u8]], output: &mut Vec<u8>) -> Option<(bool, u32)> {
    let bit_arg = args.first().copied().unwrap_or(b"");
    let bit = Self::try_parse_bit(bit_arg).or_else(|| {
      write_error_raw(output, ERR_BIT);
      None
    })?;
    let from = match args.get(1) {
      Some(from_arg) => Self::try_parse_uint32(from_arg).or_else(|| {
        write_error_raw(output, ERR_FROM);
        None
      })?,
      None => 0,
    };
    Some((bit, from))
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RBitPos
  ///
  /// 命令主执行体（C# RBitPos.Reader 钩子）
  fn bit_pos_reader(payload: &[u8], args: &[&[u8]], output: &mut Vec<u8>) -> bool {
    let Some((bit, from)) = Self::bit_pos_parse_args(args, output) else {
      return false;
    };
    let Some(obj) = Self::decode(payload) else {
      write_error_raw(output, ERR_DECODE);
      return false;
    };
    output.write_resp_int(obj.bit_pos(bit, from));
    true
  }

  /// 缺键分支（C# RBitPos.NotFound）：bit==1 → -1；bit==0 → 首个未置位即 from（缺省 0）
  fn bit_pos_not_found(args: &[&[u8]], output: &mut Vec<u8>) {
    let Some((bit, from)) = Self::bit_pos_parse_args(args, output) else {
      return;
    };
    output.write_resp_int(if bit { -1 } else { i64::from(from) });
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 非空载荷上空对象判定不成立；置位/清空后随动
  #[test]
  fn empty_object_detection() {
    let mut payload = Vec::new();
    assert!(RoaringBitmapCommands::payload_is_empty(&payload));
    let mut out = Vec::new();
    assert!(RoaringBitmapCommands::set_bit_updater(
      &mut payload,
      &[b"42", b"1"],
      &mut out
    ));
    assert_eq!(out, b":0\r\n");
    assert!(!RoaringBitmapCommands::payload_is_empty(&payload));

    out.clear();
    assert!(RoaringBitmapCommands::set_bit_updater(
      &mut payload,
      &[b"42", b"0"],
      &mut out
    ));
    assert_eq!(out, b":1\r\n");
    assert!(RoaringBitmapCommands::payload_is_empty(&payload));
  }

  /// 防空墓碑：畸形参数在 NeedInitialUpdate 即拒绝，不触碰载荷
  #[test]
  fn need_initial_update_rejects_bad_args() {
    let mut out = Vec::new();
    assert!(!RoaringBitmapCommands::set_bit_need_initial_update(
      &[b"notanumber", b"1"],
      &mut out
    ));
    assert_eq!(
      out,
      b"-ERR bit offset is not an unsigned 32-bit integer\r\n"
    );

    out.clear();
    assert!(!RoaringBitmapCommands::set_bit_need_initial_update(
      &[b"-5", b"1"],
      &mut out
    ));
    assert_eq!(
      out,
      b"-ERR bit offset is not an unsigned 32-bit integer\r\n"
    );

    out.clear();
    assert!(!RoaringBitmapCommands::set_bit_need_initial_update(
      &[b"5", b"2"],
      &mut out
    ));
    assert_eq!(out, b"-ERR bit value must be 0 or 1\r\n");

    out.clear();
    assert!(RoaringBitmapCommands::set_bit_need_initial_update(
      &[b"5", b"0"],
      &mut out
    ));
    assert!(out.is_empty());
  }

  /// 读命令 NotFound 语义（读不建键的应答面）
  #[test]
  fn not_found_semantics() {
    let mut out = Vec::new();
    RoaringBitmapCommands::get_bit_not_found(&[b"12345"], &mut out);
    assert_eq!(out, b":0\r\n");

    out.clear();
    RoaringBitmapCommands::bit_count_not_found(&[], &mut out);
    assert_eq!(out, b":0\r\n");

    out.clear();
    RoaringBitmapCommands::bit_pos_not_found(&[b"1"], &mut out);
    assert_eq!(out, b":-1\r\n");

    out.clear();
    RoaringBitmapCommands::bit_pos_not_found(&[b"0"], &mut out);
    assert_eq!(out, b":0\r\n");

    out.clear();
    RoaringBitmapCommands::bit_pos_not_found(&[b"0", b"100"], &mut out);
    assert_eq!(out, b":100\r\n");

    out.clear();
    RoaringBitmapCommands::bit_pos_not_found(&[b"2"], &mut out);
    assert_eq!(out, b"-ERR bit must be 0 or 1\r\n");
  }

  /// Reader 命中路径：setbit 置位后 getbit/bitcount/bitpos 读值
  #[test]
  fn reader_paths_after_updates() {
    let mut payload = Vec::new();
    let mut out = Vec::new();
    assert!(RoaringBitmapCommands::set_bit_updater(
      &mut payload,
      &[b"100", b"1"],
      &mut out
    ));
    assert_eq!(out, b":0\r\n");

    out.clear();
    assert!(RoaringBitmapCommands::get_bit_reader(
      &payload,
      &[b"100"],
      &mut out
    ));
    assert_eq!(out, b":1\r\n");

    out.clear();
    assert!(RoaringBitmapCommands::get_bit_reader(
      &payload,
      &[b"99"],
      &mut out
    ));
    assert_eq!(out, b":0\r\n");

    out.clear();
    assert!(RoaringBitmapCommands::bit_count_reader(
      &payload,
      &[],
      &mut out
    ));
    assert_eq!(out, b":1\r\n");

    out.clear();
    assert!(RoaringBitmapCommands::bit_pos_reader(
      &payload,
      &[b"1"],
      &mut out
    ));
    assert_eq!(out, b":100\r\n");
  }
}
