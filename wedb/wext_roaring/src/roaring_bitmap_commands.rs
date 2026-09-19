//! R.* 四条命令静态执行面（对标 modules/RoaringBitmap/RoaringBitmapCommands.cs
//! 与 RoaringBitmapModule.cs:OnLoad 的编译期化承接）
//!
//! C# 经 MODULE LOADCS 运行时注册类型与命令；rust 按转写规范以静态枚举
//! [`RoaringCommand`] + const 清单承接：`match_command` 零锁零分配完成
//! 命令名 → 执行体/元数据/信封标签的一次性解析，命令名单源自
//! [`RoaringCommand::name`]（全集 [`RoaringCommand::ALL`]），server 层
//! 分发/校验一律经 wnode 编译期清单 [`RoaringCommand::OBJECT_ENTRY`]
//! 单点命中，不再按特性手写比对臂。
//!
//! C# 每条命令为独立 `CustomObjectFunctions` 子类；Rust 以
//! [`wcustom::CustomObjectFns`] 函数指针集承接同一四接口形态：
//! - RSetBit：NeedInitialUpdate（防空墓碑校验）+ Updater（读改写）
//! - RGetBit / RBitCount / RBitPos：Reader（命中只读）+ NotFound（读不建键）
//!
//! 载荷域为对象信封载荷字节：空载荷 = 键缺失新建初值
//!（C# 工厂 `RoaringBitmapFactory.Create` 的零载荷承接）。

use core::str;

use wcustom::{CommandType, CustomObjectFns};
use wresp::{
  cmd_strings::{RESP_ERR_COMMAND_READ_ONLY, RESP_ERR_COMMAND_WRITE_ONLY, write_error_raw},
  ext::RespVecExt,
};
use wval::CustomObjectType;

use crate::roaring_bitmap_object::RoaringBitmapObject;

/// R.* 静态命令枚举（编译期分发；对齐 C# 每命令独立 CustomObjectFunctions
/// 子类的注册语义）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoaringCommand {
  /// R.SETBIT key offset value（ReadModifyWrite）
  SetBit,
  /// R.GETBIT key offset（Read）
  GetBit,
  /// R.BITCOUNT key（Read）
  BitCount,
  /// R.BITPOS key bit [from]（Read）
  BitPos,
}

/// 命令静态清单表项（C# RespCommandsInfo 最小承接；COMMAND 目录 / ACL
/// 注册名校验的单一数据源）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoaringCommandInfo {
  /// 命令名（大写规范形）
  pub name: &'static str,
  /// 元数（负值 = 至少 -arity-1 个参数）
  pub arity: i32,
  /// ACL 类别
  pub acl_categories: &'static [&'static str],
  /// 摘要
  pub summary: &'static str,
}

/// 编译期命令清单（对标 RoaringBitmapModule.cs:OnLoad 的四条 RegisterCommand；
/// 名与元数单源自 [`RoaringCommand`] const 取值，acl 类别/摘要为清单独有面）
pub const COMMAND_INFOS: &[RoaringCommandInfo] = &[
  RoaringCommandInfo {
    name: RoaringCommand::SetBit.name(),
    arity: RoaringCommand::SetBit.arity(),
    acl_categories: &["bitmap"],
    summary: "Set or clear the bit at offset",
  },
  RoaringCommandInfo {
    name: RoaringCommand::GetBit.name(),
    arity: RoaringCommand::GetBit.arity(),
    acl_categories: &["bitmap"],
    summary: "Return the bit value at offset",
  },
  RoaringCommandInfo {
    name: RoaringCommand::BitCount.name(),
    arity: RoaringCommand::BitCount.arity(),
    acl_categories: &["bitmap"],
    summary: "Count the set bits in the bitmap",
  },
  RoaringCommandInfo {
    name: RoaringCommand::BitPos.name(),
    arity: RoaringCommand::BitPos.arity(),
    acl_categories: &["bitmap"],
    summary: "Find the first bit with the given value",
  },
];

/// 自定义命令注册名校验（大小写不敏感；对标
/// libs/server/Custom/CustomCommandManager.cs:IsCustomCommandRegistered，
/// ACL SETUSER 按名规则的未知名拒绝路径）
///
/// 名单单源：本函数是 [`RoaringCommand::match_command`] 的布尔投影（命令名
/// 唯一权威为枚举 [`RoaringCommand::name`]），server 层 ACL 门已改走 wnode
/// 编译期清单单点，此处只承扩展 crate 自身的注册名查询面
pub fn is_command_registered(name: &str) -> bool {
  RoaringCommand::match_command(name.as_bytes()).is_some()
}

impl RoaringCommand {
  /// 命令全集（枚举单源权威表：按名解析与 [`COMMAND_INFOS`] 目录共用的
  /// 唯一名单，新增一条命令只在此加一项）
  pub const ALL: &[Self] = &[Self::SetBit, Self::GetBit, Self::BitCount, Self::BitPos];

  /// 信封内层类型标签（wval::CustomObjectType::Roaring 分配单点投影；
  /// 全仓严禁 BASE + n 裸偏移）
  pub const OBJECT_TAG: u8 = CustomObjectType::Roaring.as_u8();

  /// Redis TYPE 应答类型串（C# modules 注册名，对标
  /// RoaringBitmapModule.cs:18 `context.Initialize("GarnetRoaringBitmap", 1)`；
  /// C# HandleType 对 custom object 无 default 臂输出零字节 quirk，rust
  /// 修复为回注册名，TYPE 与 EXISTS 存活口径一致）
  pub const OBJECT_TYPE_NAME: &str = "GarnetRoaringBitmap";

  /// 扩展对象静态描述清单项（wnode 编译期清单单条；标签、TYPE 注册名、
  /// 按名解析入口一处描述，对标 CustomObjectFactory 工厂集中持有形态）
  pub const OBJECT_ENTRY: wcustom::CustomObjectEntry = wcustom::CustomObjectEntry {
    tag: CustomObjectType::Roaring,
    type_name: Self::OBJECT_TYPE_NAME,
    match_command: Self::match_command_meta,
  };

  /// 按名匹配（大小写不敏感；零锁零分配的编译期清单直比，命令名唯一权威
  /// 为 [`Self::name`]，对标 RoaringBitmapModule.cs:OnLoad 四个 RegisterCommand 名）
  pub fn match_command(name: &[u8]) -> Option<Self> {
    Self::ALL
      .iter()
      .copied()
      .find(|cmd| cmd.name().as_bytes().eq_ignore_ascii_case(name))
  }

  /// 按名解析 → 描述清单命令元数据（标签由清单项 [`Self::OBJECT_ENTRY`]
  /// 单点附带，命令元数据只承载执行面）
  fn match_command_meta(name: &[u8]) -> Option<wcustom::CustomCommandMeta> {
    Self::match_command(name).map(|cmd| wcustom::CustomCommandMeta {
      name: cmd.name(),
      command_type: cmd.command_type(),
      arity: cmd.arity(),
      fns: cmd.fns(),
    })
  }

  /// 命令名（静态清单规范形）
  pub const fn name(self) -> &'static str {
    match self {
      Self::SetBit => "R.SETBIT",
      Self::GetBit => "R.GETBIT",
      Self::BitCount => "R.BITCOUNT",
      Self::BitPos => "R.BITPOS",
    }
  }

  /// 元数
  pub const fn arity(self) -> i32 {
    match self {
      Self::SetBit => 4,
      Self::GetBit => 3,
      Self::BitCount => 2,
      Self::BitPos => -3,
    }
  }

  /// 命令类型
  pub const fn command_type(self) -> CommandType {
    match self {
      Self::SetBit => CommandType::ReadModifyWrite,
      Self::GetBit | Self::BitCount | Self::BitPos => CommandType::Read,
    }
  }

  /// 静态执行体（编译期函数指针集，对标各命令类的四接口实现）
  pub const fn fns(self) -> CustomObjectFns {
    match self {
      Self::SetBit => RoaringBitmapCommands::R_SET_BIT,
      Self::GetBit => RoaringBitmapCommands::R_GET_BIT,
      Self::BitCount => RoaringBitmapCommands::R_BIT_COUNT,
      Self::BitPos => RoaringBitmapCommands::R_BIT_POS,
    }
  }
}

/// 校验失败文案（逐字节对标 C# 各命令类私有 ReadOnlySpan 错误串）
const ERR_OFFSET: &str = "ERR bit offset is not an unsigned 32-bit integer";
const ERR_VALUE: &str = "ERR bit value must be 0 or 1";
const ERR_BIT: &str = "ERR bit must be 0 or 1";
const ERR_FROM: &str = "ERR from offset is not an unsigned 32-bit integer";
/// 载荷解码失败文案（C# Deserialize 异常经框架折算的通用错误承接）
const ERR_DECODE: &str = "ERR RoaringBitmap object decode failed";

/// 共享参数解析辅助（C# RoaringBitmapArgs 静态类）+ 四命令执行体
pub struct RoaringBitmapCommands;

impl RoaringBitmapCommands {
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseUInt32
  ///
  /// 对齐 C# `Utf8Parser.TryParse(long)` 默认 TryParseInt64D 路径：可选
  /// +/- 符号（符号后必须至少一位数字）、前导零不计溢出（"0000000000042"
  /// 合法）；随后 signed ∈ [0, u32.MaxValue] 值域过滤 ⇒ 除 -0 外负值拒绝，
  /// long 溢出拒绝（u32 checked 累计提前短路，接受集与拒绝集等价）
  pub fn try_parse_uint32(raw: &[u8]) -> Option<u32> {
    let (negative, digits) = match raw {
      [b'+', rest @ ..] => (false, rest),
      [b'-', rest @ ..] => (true, rest),
      _ => (false, raw),
    };
    if digits.is_empty() {
      return None;
    }
    let mut val: u32 = 0;
    for &b in digits {
      if !b.is_ascii_digit() {
        return None;
      }
      val = val.checked_mul(10)?.checked_add(u32::from(b - b'0'))?;
    }
    if negative && val != 0 {
      return None;
    }
    Some(val)
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
  #[inline]
  fn payload_is_empty(payload: &[u8]) -> bool {
    payload.is_empty()
  }

  /// 不可达执行槽兜底（对标 C# 基类虚方法 NotImplementedException；
  /// 路由层按 CommandType 分流后不可达，以明确错误帧替代 panic）
  fn reject_write_only(
    _payload: &[u8],
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    write_error_raw(output, RESP_ERR_COMMAND_WRITE_ONLY);
    false
  }

  fn reject_read_only_initial(_args: &[&[u8]], output: &mut Vec<u8>, _resp_version: u8) -> bool {
    write_error_raw(output, RESP_ERR_COMMAND_READ_ONLY);
    false
  }

  fn reject_read_only_update(
    _payload: &mut Vec<u8>,
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    write_error_raw(output, RESP_ERR_COMMAND_READ_ONLY);
    false
  }

  /// RMW 侧 NotFound 不可达兜底（C# 基类缺省 WriteNull：FunctionsState.cs:nilResp）
  ///
  /// 版本二选一单源在 wresp::ext::RespVecExt::write_resp_null_ver
  fn not_found_null(_args: &[&[u8]], output: &mut Vec<u8>, resp_version: u8) {
    output.write_resp_null_ver(resp_version);
  }

  // ---- R.SETBIT ----

  /// 建对象前先行校验参数（C# RSetBit 类 NeedInitialUpdate 钩子）：
  /// 畸形 R.SETBIT 不得留下空对象（防空墓碑）；
  /// 只走参数副本，Updater 仍从 offset 0 重新解析
  fn set_bit_need_initial_update(args: &[&[u8]], output: &mut Vec<u8>, _resp_version: u8) -> bool {
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
  fn set_bit_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
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
    if obj.is_empty() {
      payload.clear();
    } else if previous != bit && Self::encode(&obj, payload).is_none() {
      write_error_raw(output, ERR_DECODE);
      return false;
    }
    output.write_resp_int(i64::from(previous));
    true
  }

  // ---- R.GETBIT ----

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RGetBit
  ///
  /// 命令主执行体（C# RGetBit.Reader 钩子）
  fn get_bit_reader(
    payload: &[u8],
    args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
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
  fn get_bit_not_found(args: &[&[u8]], output: &mut Vec<u8>, _resp_version: u8) {
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
  fn bit_count_reader(
    payload: &[u8],
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    let Some(obj) = Self::decode(payload) else {
      write_error_raw(output, ERR_DECODE);
      return false;
    };
    output.write_resp_int(obj.bit_count());
    true
  }

  /// 缺键分支（C# RBitCount.NotFound）：应答计数 0
  fn bit_count_not_found(_args: &[&[u8]], output: &mut Vec<u8>, _resp_version: u8) {
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
  fn bit_pos_reader(
    payload: &[u8],
    args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
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
  fn bit_pos_not_found(args: &[&[u8]], output: &mut Vec<u8>, _resp_version: u8) {
    let Some((bit, from)) = Self::bit_pos_parse_args(args, output) else {
      return;
    };
    output.write_resp_int(if bit { -1 } else { i64::from(from) });
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 静态清单按名匹配（大小写不敏感）与注册名校验同径
  #[test]
  fn static_match_and_registration() {
    assert_eq!(
      RoaringCommand::match_command(b"r.setbit"),
      Some(RoaringCommand::SetBit)
    );
    assert_eq!(
      RoaringCommand::match_command(b"R.BITPOS"),
      Some(RoaringCommand::BitPos)
    );
    assert_eq!(RoaringCommand::match_command(b"R.NOPE"), None);
    assert_eq!(
      RoaringCommand::match_command(b"R.SETBIT").map(|c| c.arity()),
      Some(4)
    );
    assert!(is_command_registered("r.getbit"));
    assert!(!is_command_registered(""));
    assert!(!is_command_registered("SET"));
  }

  /// 命令目录 [`COMMAND_INFOS`] 与按名解析 [`RoaringCommand::ALL`] 同一名单
  /// 源（枚举 [`RoaringCommand::name`]）：新增命令漏登记任一侧由本用例挡住，
  /// 杜绝目录与解析判定漂移
  #[test]
  fn directory_and_match_share_one_name_source() {
    assert_eq!(
      COMMAND_INFOS.len(),
      RoaringCommand::ALL.len(),
      "命令目录与枚举全集条目数漂移"
    );
    for cmd in RoaringCommand::ALL.iter().copied() {
      let name = cmd.name();
      assert!(
        COMMAND_INFOS.iter().any(|info| info.name == name),
        "命令目录缺项 {name}"
      );
      assert_eq!(RoaringCommand::match_command(name.as_bytes()), Some(cmd));
      assert!(is_command_registered(name));
    }
  }

  /// 非空载荷上空对象判定不成立；置位/清空后随动
  #[test]
  fn empty_object_detection() {
    let mut payload = Vec::new();
    assert!(RoaringBitmapCommands::payload_is_empty(&payload));
    let mut out = Vec::new();
    assert!(RoaringBitmapCommands::set_bit_updater(
      &mut payload,
      &[b"42", b"1"],
      &mut out,
      2
    ));
    assert_eq!(out, b":0\r\n");
    assert!(!RoaringBitmapCommands::payload_is_empty(&payload));

    // 幂等重复置 1：previous 为 1，载荷未改动且非空（跳过重编码）
    out.clear();
    let old_payload = payload.clone();
    assert!(RoaringBitmapCommands::set_bit_updater(
      &mut payload,
      &[b"42", b"1"],
      &mut out,
      2
    ));
    assert_eq!(out, b":1\r\n");
    assert_eq!(payload, old_payload);
    assert!(!RoaringBitmapCommands::payload_is_empty(&payload));

    out.clear();
    assert!(RoaringBitmapCommands::set_bit_updater(
      &mut payload,
      &[b"42", b"0"],
      &mut out,
      2
    ));
    assert_eq!(out, b":1\r\n");
    assert!(RoaringBitmapCommands::payload_is_empty(&payload));

    // 空载荷置 0：previous 为 0，维持空载荷
    out.clear();
    assert!(RoaringBitmapCommands::set_bit_updater(
      &mut payload,
      &[b"42", b"0"],
      &mut out,
      2
    ));
    assert_eq!(out, b":0\r\n");
    assert!(RoaringBitmapCommands::payload_is_empty(&payload));
  }

  /// 防空墓碑：畸形参数在 NeedInitialUpdate 即拒绝，不触碰载荷
  #[test]
  fn need_initial_update_rejects_bad_args() {
    let mut out = Vec::new();
    assert!(!RoaringBitmapCommands::set_bit_need_initial_update(
      &[b"notanumber", b"1"],
      &mut out,
      2
    ));
    assert_eq!(
      out,
      b"-ERR bit offset is not an unsigned 32-bit integer\r\n"
    );

    out.clear();
    assert!(!RoaringBitmapCommands::set_bit_need_initial_update(
      &[b"-5", b"1"],
      &mut out,
      2
    ));
    assert_eq!(
      out,
      b"-ERR bit offset is not an unsigned 32-bit integer\r\n"
    );

    out.clear();
    assert!(!RoaringBitmapCommands::set_bit_need_initial_update(
      &[b"5", b"2"],
      &mut out,
      2
    ));
    assert_eq!(out, b"-ERR bit value must be 0 or 1\r\n");

    out.clear();
    assert!(RoaringBitmapCommands::set_bit_need_initial_update(
      &[b"5", b"0"],
      &mut out,
      2
    ));
    assert!(out.is_empty());
  }

  /// try_parse_uint32 对标 C# Utf8Parser 默认整数路径：
  /// 符号（仅 -0 类负值合法）、任意长前导零、u32 边界与溢出
  #[test]
  fn try_parse_uint32_aligns_utf8_parser() {
    // 常规十进制
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"0"), Some(0));
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"42"), Some(42));
    assert_eq!(
      RoaringBitmapCommands::try_parse_uint32(b"4294967295"),
      Some(u32::MAX)
    );
    // 值域上界溢出（C# long 解析成功后 signed <= uint.MaxValue 过滤拒绝）
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"4294967296"), None);
    assert_eq!(
      RoaringBitmapCommands::try_parse_uint32(b"99999999999999999999"),
      None
    );
    // 可选符号：+N 合法；仅 -0 类经值域过滤后合法；残缺符号非法
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"+42"), Some(42));
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"+0"), Some(0));
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"-0"), Some(0));
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"-00"), Some(0));
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"-5"), None);
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"+"), None);
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"-"), None);
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"+-1"), None);
    // 前导零不计溢出（TryParseInt64D 零串吞并，长度无上限）
    assert_eq!(
      RoaringBitmapCommands::try_parse_uint32(b"0000000000042"),
      Some(42)
    );
    assert_eq!(
      RoaringBitmapCommands::try_parse_uint32(b"00000000000000000000005"),
      Some(5)
    );
    assert_eq!(
      RoaringBitmapCommands::try_parse_uint32(b"004294967295"),
      Some(u32::MAX)
    );
    assert_eq!(
      RoaringBitmapCommands::try_parse_uint32(b"004294967296"),
      None
    );
    assert_eq!(
      RoaringBitmapCommands::try_parse_uint32(b"-0000000000000005"),
      None
    );
    // 非法字符与空串（C# 整体消费校验等价拒绝）
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b""), None);
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"12x"), None);
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b" 1"), None);
    assert_eq!(RoaringBitmapCommands::try_parse_uint32(b"+1 "), None);

    // 接受面贯穿命令层：+5 / 长前导零偏移可正常置位读回
    let mut payload = Vec::new();
    let mut out = Vec::new();
    assert!(RoaringBitmapCommands::set_bit_updater(
      &mut payload,
      &[b"+5", b"1"],
      &mut out,
      2
    ));
    assert_eq!(out, b":0\r\n");
    out.clear();
    assert!(RoaringBitmapCommands::get_bit_reader(
      &payload,
      &[b"0000000000005"],
      &mut out,
      2
    ));
    assert_eq!(out, b":1\r\n");
  }

  /// 读命令 NotFound 语义（读不建键的应答面）
  #[test]
  fn not_found_semantics() {
    let mut out = Vec::new();
    RoaringBitmapCommands::get_bit_not_found(&[b"12345"], &mut out, 2);
    assert_eq!(out, b":0\r\n");

    out.clear();
    RoaringBitmapCommands::bit_count_not_found(&[], &mut out, 2);
    assert_eq!(out, b":0\r\n");

    out.clear();
    RoaringBitmapCommands::bit_pos_not_found(&[b"1"], &mut out, 2);
    assert_eq!(out, b":-1\r\n");

    out.clear();
    RoaringBitmapCommands::bit_pos_not_found(&[b"0"], &mut out, 2);
    assert_eq!(out, b":0\r\n");

    out.clear();
    RoaringBitmapCommands::bit_pos_not_found(&[b"0", b"100"], &mut out, 2);
    assert_eq!(out, b":100\r\n");

    out.clear();
    RoaringBitmapCommands::bit_pos_not_found(&[b"2"], &mut out, 2);
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
      &mut out,
      2
    ));
    assert_eq!(out, b":0\r\n");

    out.clear();
    assert!(RoaringBitmapCommands::get_bit_reader(
      &payload,
      &[b"100"],
      &mut out,
      2
    ));
    assert_eq!(out, b":1\r\n");

    out.clear();
    assert!(RoaringBitmapCommands::get_bit_reader(
      &payload,
      &[b"99"],
      &mut out,
      2
    ));
    assert_eq!(out, b":0\r\n");

    out.clear();
    assert!(RoaringBitmapCommands::bit_count_reader(
      &payload,
      &[],
      &mut out,
      2
    ));
    assert_eq!(out, b":1\r\n");

    out.clear();
    assert!(RoaringBitmapCommands::bit_pos_reader(
      &payload,
      &[b"1"],
      &mut out,
      2
    ));
    assert_eq!(out, b":100\r\n");
  }
}
