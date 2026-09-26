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
//!
//! 自研依据: Roaring 命令分发（C# 对应 test/standalone/Garnet.test/RespRoaringBitmapTests.cs + RoaringBitmapDataTests.cs）

use core::str;

use wcustom::{CommandType, CustomObjectFns, KeyScope};
use wresp::{
  cmd_strings::{RESP_ERR_COMMAND_READ_ONLY, RESP_ERR_COMMAND_WRITE_ONLY, write_error_raw},
  ext::RespVecExt,
};
use wval::CustomObjectType;

use crate::roaring_bitmap_object::RoaringBitmapObject;

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

macro_rules! define_roaring_commands {
  ($(
    $variant:ident => {
      name: $name:literal,
      suffix: $suffix:literal,
      arity: $arity:expr,
      cmd_type: $cmd_type:ident,
      acl: $acl:expr,
      summary: $summary:literal,
      fns: $fns:expr $(,)?
    },
  )*) => {
    /// R.* 静态命令枚举（编译期分发；对齐 C# 每命令独立 CustomObjectFunctions
    /// 子类的注册语义）
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RoaringCommand {
      $($variant,)*
    }

    /// 编译期命令清单（对标 RoaringBitmapModule.cs:OnLoad 的四条 RegisterCommand；
    /// 名与元数单源自 [`RoaringCommand`] const 取值，acl 类别/摘要为清单独有面）
    pub const COMMAND_INFOS: &[RoaringCommandInfo] = &[
      $(
        RoaringCommandInfo {
          name: $name,
          arity: $arity,
          acl_categories: $acl,
          summary: $summary,
        },
      )*
    ];

    impl RoaringCommand {
      /// 命令全集（枚举单源权威表：按名解析与 [`COMMAND_INFOS`] 目录共用的
      /// 唯一名单，新增一条命令只在此加一项）
      pub const ALL: &[Self] = &[$(Self::$variant,)*];

      /// 命令名（静态清单规范形）
      pub const fn name(self) -> &'static str {
        match self {
          $(Self::$variant => $name,)*
        }
      }

      /// 元数
      pub const fn arity(self) -> i32 {
        match self {
          $(Self::$variant => $arity,)*
        }
      }

      /// 命令类型
      pub const fn command_type(self) -> CommandType {
        match self {
          $(Self::$variant => CommandType::$cmd_type,)*
        }
      }

      /// 静态执行体（编译期函数指针集，对标各命令类的四接口实现）
      pub const fn fns(self) -> CustomObjectFns {
        match self {
          $(Self::$variant => $fns,)*
        }
      }

      /// 按名匹配（大小写不敏感；前缀 O(1) 预筛 + 后缀比对）
      pub fn match_command(name: &[u8]) -> Option<Self> {
        let suffix = match name {
          [b'r' | b'R', b'.', rest @ ..] => rest,
          _ => return None,
        };
        $(
          if suffix.eq_ignore_ascii_case($suffix) {
            return Some(Self::$variant);
          }
        )*
        None
      }
    }
  };
}

define_roaring_commands! {
  SetBit => {
    name: "R.SETBIT",
    suffix: b"SETBIT",
    arity: 4,
    cmd_type: ReadModifyWrite,
    acl: &["bitmap"],
    summary: "Set or clear the bit at offset",
    fns: RoaringBitmapCommands::R_SET_BIT,
  },
  GetBit => {
    name: "R.GETBIT",
    suffix: b"GETBIT",
    arity: 3,
    cmd_type: Read,
    acl: &["bitmap"],
    summary: "Return the bit value at offset",
    fns: RoaringBitmapCommands::R_GET_BIT,
  },
  BitCount => {
    name: "R.BITCOUNT",
    suffix: b"BITCOUNT",
    arity: 2,
    cmd_type: Read,
    acl: &["bitmap"],
    summary: "Count the set bits in the bitmap",
    fns: RoaringBitmapCommands::R_BIT_COUNT,
  },
  BitPos => {
    name: "R.BITPOS",
    suffix: b"BITPOS",
    arity: -3,
    cmd_type: Read,
    acl: &["bitmap"],
    summary: "Find the first bit with the given value",
    fns: RoaringBitmapCommands::R_BIT_POS,
  },
}

/// 自定义命令注册名校验（大小写不敏感；单元测试专用面）
#[cfg(test)]
pub fn is_command_registered(name: &str) -> bool {
  RoaringCommand::match_command(name.as_bytes()).is_some()
}

impl RoaringCommand {
  /// Redis TYPE 应答类型串（C# modules 注册名，对标
  /// RoaringBitmapModule.cs:18 `context.Initialize("GarnetRoaringBitmap", 1)`；
  /// C# HandleType 对 custom object 无 default 臂输出零字节 quirk，rust
  /// 修复为回注册名，TYPE 与 EXISTS 存活口径一致）
  pub const OBJECT_TYPE_NAME: &str = "GarnetRoaringBitmap";

  /// 扩展对象静态描述清单项（wnode 编译期清单单条；标签、TYPE 注册名、
  /// 按名解析入口、堆估算入口一处描述，对标 CustomObjectFactory 工厂集中
  /// 持有形态与 RoaringBitmapObject 的 IHeapObject.HeapMemorySize 记账）
  pub const OBJECT_ENTRY: wcustom::CustomObjectEntry = wcustom::CustomObjectEntry {
    tag: CustomObjectType::Roaring,
    type_name: Self::OBJECT_TYPE_NAME,
    match_command: Self::match_command_meta,
    heap_estimate: super::roaring_bitmap_object::heap_estimate,
    scan_members: super::roaring_bitmap_object::scan_members,
  };

  /// 按名解析 → 描述清单命令元数据（标签由清单项 [`Self::OBJECT_ENTRY`]
  /// 单点附带，命令元数据只承载执行面）
  fn match_command_meta(name: &[u8]) -> Option<wcustom::CustomCommandMeta> {
    Self::match_command(name).map(|cmd| wcustom::CustomCommandMeta {
      name: cmd.name(),
      command_type: cmd.command_type(),
      // 位图命令全单键（对标 RoaringBitmapModule.cs 四条 RegisterCommand 的
      // 单键形态），多键读只与 JSON.MGET 并置
      key_scope: KeyScope::Single,
      arity: cmd.arity(),
      fns: cmd.fns(),
    })
  }
}

/// 校验失败文案（逐字节对标 C# 各命令类私有 ReadOnlySpan 错误串）
const ERR_OFFSET: &str = "ERR bit offset is not an unsigned 32-bit integer";
const ERR_VALUE: &str = "ERR bit value must be 0 or 1";
const ERR_BIT: &str = "ERR bit must be 0 or 1";
const ERR_FROM: &str = "ERR from offset is not an unsigned 32-bit integer";
/// 载荷解码失败文案（C# Deserialize 异常经框架折算的通用错误承接）
const ERR_DECODE: &str = "ERR RoaringBitmap object decode failed";

type ReaderFn = fn(&[u8], &[&[u8]], &mut Vec<u8>, u8) -> bool;
type NotFoundFn = fn(&[&[u8]], &mut Vec<u8>, u8);

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
  pub const fn try_parse_bit(raw: &[u8]) -> Option<bool> {
    match raw {
      b"0" => Some(false),
      b"1" => Some(true),
      _ => None,
    }
  }

  /// 取第 i 个参数（缺失按空串，交解析臂统一拒绝）
  fn arg<'a>(args: &[&'a [u8]], i: usize) -> &'a [u8] {
    args.get(i).copied().unwrap_or(b"")
  }

  /// 解析失败统一短路：写错误帧并回 None（参数臂与载荷解码臂共用）
  fn checked<T>(parsed: Option<T>, err: &str, output: &mut Vec<u8>) -> Option<T> {
    parsed.or_else(|| {
      write_error_raw(output, err);
      None
    })
  }

  /// 第 i 位 u32 参数解析（失败写 err 帧）
  fn uint_arg(args: &[&[u8]], i: usize, err: &str, output: &mut Vec<u8>) -> Option<u32> {
    Self::checked(Self::try_parse_uint32(Self::arg(args, i)), err, output)
  }

  /// 第 i 位 0/1 参数解析（失败写 err 帧）
  fn bit_arg(args: &[&[u8]], i: usize, err: &str, output: &mut Vec<u8>) -> Option<bool> {
    Self::checked(Self::try_parse_bit(Self::arg(args, i)), err, output)
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

  /// 读命令静态函数指针集构建（消除只读命令重复模板）
  const fn read_fns(reader: ReaderFn, not_found: NotFoundFn) -> CustomObjectFns {
    CustomObjectFns {
      need_initial_update: Self::reject_read_only_initial,
      updater: Self::reject_read_only_update,
      reader,
      not_found,
      is_empty: Self::payload_is_empty,
    }
  }

  /// R.GETBIT key offset（Read，arity 3）
  ///
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RGetBit
  pub const R_GET_BIT: CustomObjectFns =
    Self::read_fns(Self::get_bit_reader, Self::get_bit_not_found);

  /// R.BITCOUNT key（Read，arity 2）
  ///
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RBitCount
  pub const R_BIT_COUNT: CustomObjectFns =
    Self::read_fns(Self::bit_count_reader, Self::bit_count_not_found);

  /// R.BITPOS key bit [from]（Read，arity -3）
  ///
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RBitPos
  pub const R_BIT_POS: CustomObjectFns =
    Self::read_fns(Self::bit_pos_reader, Self::bit_pos_not_found);

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
  const fn payload_is_empty(payload: &[u8]) -> bool {
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

  /// 提取 offset 参数并校验（失败直接写出错误帧）
  fn parse_offset_arg(args: &[&[u8]], output: &mut Vec<u8>) -> Option<u32> {
    Self::uint_arg(args, 0, ERR_OFFSET, output)
  }

  /// 提取并校验 R.SETBIT 的 (offset, bit) 参数对
  fn parse_set_bit_args(args: &[&[u8]], output: &mut Vec<u8>) -> Option<(u32, bool)> {
    let bit_offset = Self::parse_offset_arg(args, output)?;
    Some((bit_offset, Self::bit_arg(args, 1, ERR_VALUE, output)?))
  }

  /// 建对象前先行校验参数（C# RSetBit 类 NeedInitialUpdate 钩子）：
  /// 畸形 R.SETBIT 不得留下空对象（防空墓碑）；
  /// 只走参数副本，Updater 仍从 offset 0 重新解析
  fn set_bit_need_initial_update(args: &[&[u8]], output: &mut Vec<u8>, _resp_version: u8) -> bool {
    Self::parse_set_bit_args(args, output).is_some()
  }

  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RSetBit
  ///
  /// 命令主执行体（C# RSetBit.Updater 钩子）
  ///
  /// modules/RoaringBitmap/RoaringBitmapCommands.cs:Updater
  fn set_bit_updater(
    payload: &mut Vec<u8>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    let Some((bit_offset, bit)) = Self::parse_set_bit_args(args, output) else {
      return false;
    };
    let Some(mut obj) = Self::checked(Self::decode(payload), ERR_DECODE, output) else {
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
  ///
  /// modules/RoaringBitmap/RoaringBitmapCommands.cs:Reader
  fn get_bit_reader(
    payload: &[u8],
    args: &[&[u8]],
    output: &mut Vec<u8>,
    _resp_version: u8,
  ) -> bool {
    let Some(bit_offset) = Self::parse_offset_arg(args, output) else {
      return false;
    };
    let Some(obj) = Self::checked(Self::decode(payload), ERR_DECODE, output) else {
      return false;
    };
    output.write_resp_int(i64::from(obj.get_bit(bit_offset)));
    true
  }

  /// 缺键分支（C# RGetBit.NotFound）：校验 offset 后应答缺席位 0（读不建键）
  ///
  /// modules/RoaringBitmap/RoaringBitmapCommands.cs:NotFound
  fn get_bit_not_found(args: &[&[u8]], output: &mut Vec<u8>, _resp_version: u8) {
    if Self::parse_offset_arg(args, output).is_some() {
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
    let Some(obj) = Self::checked(Self::decode(payload), ERR_DECODE, output) else {
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
  ///
  /// modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseArgs
  fn bit_pos_parse_args(args: &[&[u8]], output: &mut Vec<u8>) -> Option<(bool, u32)> {
    let bit = Self::bit_arg(args, 0, ERR_BIT, output)?;
    let from = if args.len() > 1 {
      Self::uint_arg(args, 1, ERR_FROM, output)?
    } else {
      0
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
    let Some(obj) = Self::checked(Self::decode(payload), ERR_DECODE, output) else {
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

  /// RESP 版本（各用例统一 RESP2）
  const VER: u8 = 2;

  /// R.SETBIT Updater 臂按编译期清单派发：断言成功，应答帧留在 out
  fn set(payload: &mut Vec<u8>, args: &[&[u8]], out: &mut Vec<u8>) {
    out.clear();
    let up = RoaringCommand::SetBit.fns().updater;
    assert!(up(payload, args, out, VER));
  }

  /// Reader 臂按编译期清单派发：断言成功，应答帧留在 out
  fn read(cmd: RoaringCommand, payload: &[u8], args: &[&[u8]], out: &mut Vec<u8>) {
    out.clear();
    let rd = cmd.fns().reader;
    assert!(rd(payload, args, out, VER));
  }

  /// NotFound 臂按编译期清单派发：应答帧写入 out
  fn nf(cmd: RoaringCommand, args: &[&[u8]], out: &mut Vec<u8>) {
    out.clear();
    (cmd.fns().not_found)(args, out, VER);
  }

  /// 静态清单按名匹配（大小写不敏感）与注册名校验同径
  #[test]
  fn static_match_and_registration() {
    let mc = RoaringCommand::match_command;
    assert_eq!(mc(b"r.setbit"), Some(RoaringCommand::SetBit));
    assert_eq!(mc(b"R.BITPOS"), Some(RoaringCommand::BitPos));
    assert_eq!(mc(b"R.SETBIT").map(RoaringCommand::arity), Some(4));
    assert_eq!(mc(b"R.NOPE"), None);
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
    let mut out = Vec::new();
    let empty = RoaringBitmapCommands::payload_is_empty;
    assert!(empty(&payload));

    set(&mut payload, &[b"42", b"1"], &mut out);
    assert_eq!(out, b":0\r\n");
    assert!(!empty(&payload));

    // 幂等重复置 1：previous 为 1，载荷未改动且非空（跳过重编码）
    let old_payload = payload.clone();
    set(&mut payload, &[b"42", b"1"], &mut out);
    assert_eq!(out, b":1\r\n");
    assert_eq!(payload, old_payload);
    assert!(!empty(&payload));

    set(&mut payload, &[b"42", b"0"], &mut out);
    assert_eq!(out, b":1\r\n");
    assert!(empty(&payload));

    // 空载荷置 0：previous 为 0，维持空载荷
    set(&mut payload, &[b"42", b"0"], &mut out);
    assert_eq!(out, b":0\r\n");
    assert!(empty(&payload));
  }

  /// 防空墓碑：畸形参数在 NeedInitialUpdate 即拒绝，不触碰载荷
  #[test]
  fn need_initial_update_rejects_bad_args() {
    let niu = RoaringBitmapCommands::set_bit_need_initial_update;
    let mut out = Vec::new();
    assert!(!niu(&[b"notanumber", b"1"], &mut out, VER));
    assert_eq!(
      out,
      b"-ERR bit offset is not an unsigned 32-bit integer\r\n"
    );
    out.clear();
    assert!(!niu(&[b"-5", b"1"], &mut out, VER));
    assert_eq!(
      out,
      b"-ERR bit offset is not an unsigned 32-bit integer\r\n"
    );
    out.clear();
    assert!(!niu(&[b"5", b"2"], &mut out, VER));
    assert_eq!(out, b"-ERR bit value must be 0 or 1\r\n");
    out.clear();
    assert!(niu(&[b"5", b"0"], &mut out, VER));
    assert!(out.is_empty());
  }

  /// try_parse_uint32 对标 C# Utf8Parser 默认整数路径：
  /// 符号（仅 -0 类负值合法）、任意长前导零、u32 边界与溢出
  #[test]
  fn try_parse_uint32_aligns_utf8_parser() {
    let parse = RoaringBitmapCommands::try_parse_uint32;
    // 常规十进制
    assert_eq!(parse(b"0"), Some(0));
    assert_eq!(parse(b"42"), Some(42));
    assert_eq!(parse(b"4294967295"), Some(u32::MAX));
    // 值域上界溢出（C# long 解析成功后 signed <= uint.MaxValue 过滤拒绝）
    assert_eq!(parse(b"4294967296"), None);
    assert_eq!(parse(b"99999999999999999999"), None);
    // 可选符号：+N 合法；仅 -0 类经值域过滤后合法；残缺符号非法
    assert_eq!(parse(b"+42"), Some(42));
    assert_eq!(parse(b"+0"), Some(0));
    assert_eq!(parse(b"-0"), Some(0));
    assert_eq!(parse(b"-00"), Some(0));
    assert_eq!(parse(b"-5"), None);
    assert_eq!(parse(b"+"), None);
    assert_eq!(parse(b"-"), None);
    assert_eq!(parse(b"+-1"), None);
    // 前导零不计溢出（TryParseInt64D 零串吞并，长度无上限）
    assert_eq!(parse(b"0000000000042"), Some(42));
    assert_eq!(parse(b"00000000000000000000005"), Some(5));
    assert_eq!(parse(b"004294967295"), Some(u32::MAX));
    assert_eq!(parse(b"004294967296"), None);
    assert_eq!(parse(b"-0000000000000005"), None);
    // 非法字符与空串（C# 整体消费校验等价拒绝）
    assert_eq!(parse(b""), None);
    assert_eq!(parse(b"12x"), None);
    assert_eq!(parse(b" 1"), None);
    assert_eq!(parse(b"+1 "), None);

    // 接受面贯穿命令层：+5 / 长前导零偏移可正常置位读回
    let mut payload = Vec::new();
    let mut out = Vec::new();
    set(&mut payload, &[b"+5", b"1"], &mut out);
    assert_eq!(out, b":0\r\n");
    read(
      RoaringCommand::GetBit,
      &payload,
      &[b"0000000000005"],
      &mut out,
    );
    assert_eq!(out, b":1\r\n");
  }

  /// 读命令 NotFound 语义（读不建键的应答面）
  #[test]
  fn not_found_semantics() {
    let mut out = Vec::new();
    nf(RoaringCommand::GetBit, &[b"12345"], &mut out);
    assert_eq!(out, b":0\r\n");
    nf(RoaringCommand::BitCount, &[], &mut out);
    assert_eq!(out, b":0\r\n");
    nf(RoaringCommand::BitPos, &[b"1"], &mut out);
    assert_eq!(out, b":-1\r\n");
    nf(RoaringCommand::BitPos, &[b"0"], &mut out);
    assert_eq!(out, b":0\r\n");
    nf(RoaringCommand::BitPos, &[b"0", b"100"], &mut out);
    assert_eq!(out, b":100\r\n");
    nf(RoaringCommand::BitPos, &[b"2"], &mut out);
    assert_eq!(out, b"-ERR bit must be 0 or 1\r\n");
  }

  /// Reader 命中路径：setbit 置位后 getbit/bitcount/bitpos 读值
  #[test]
  fn reader_paths_after_updates() {
    let mut payload = Vec::new();
    let mut out = Vec::new();
    set(&mut payload, &[b"100", b"1"], &mut out);
    assert_eq!(out, b":0\r\n");
    read(RoaringCommand::GetBit, &payload, &[b"100"], &mut out);
    assert_eq!(out, b":1\r\n");
    read(RoaringCommand::GetBit, &payload, &[b"99"], &mut out);
    assert_eq!(out, b":0\r\n");
    read(RoaringCommand::BitCount, &payload, &[], &mut out);
    assert_eq!(out, b":1\r\n");
    read(RoaringCommand::BitPos, &payload, &[b"1"], &mut out);
    assert_eq!(out, b":100\r\n");
  }
}
