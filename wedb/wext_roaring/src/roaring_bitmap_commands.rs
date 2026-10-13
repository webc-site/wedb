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

    /// 自省展示项清单（与 [`COMMAND_INFOS`]、[`RoaringCommand::ALL`] 同一次
    /// 宏展开产出，值只取 `$name`/`$arity` 字面量，杜绝第二名单；wnode
    /// 编译期清单经 [`RoaringCommand::OBJECT_ENTRY`] 的 `command_display`
    /// 字段消费，COMMAND 自省面帧的名与 arity 唯此一处真源）
    pub const COMMAND_DISPLAY: &[wcustom::CustomCommandDisplay] = &[
      $(
        wcustom::CustomCommandDisplay {
          name: $name,
          arity: $arity,
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
    command_display: COMMAND_DISPLAY,
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
  const R_SET_BIT: CustomObjectFns = CustomObjectFns {
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
  const R_GET_BIT: CustomObjectFns = Self::read_fns(Self::get_bit_reader, Self::get_bit_not_found);

  /// R.BITCOUNT key（Read，arity 2）
  ///
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RBitCount
  const R_BIT_COUNT: CustomObjectFns =
    Self::read_fns(Self::bit_count_reader, Self::bit_count_not_found);

  /// R.BITPOS key bit [from]（Read，arity -3）
  ///
  /// 在 garnet 中的相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:RBitPos
  const R_BIT_POS: CustomObjectFns = Self::read_fns(Self::bit_pos_reader, Self::bit_pos_not_found);

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
