//! RESP 对象命令层：同步信封读写辅助 + 信封堆估算合流 + 命令中止工具
//!
//! 信封存储：记录挂 `KeyTag::ObjectEnvelope` 物理键（带外类型通道，对标 C#
//! LogRecord.DataHeader.ValueIsObject），值为 [1 字节类型标签][bitcode 载荷]；
//! 编码由 `wcol::object_payload` 一处定义，杜绝跨层 WRONGTYPE 误判与裸载荷覆盖。
//! 信封标签消费面（TYPE 类型串、MEMORY USAGE 堆估算）在本层合流：`wcol` 与
//! `libs/server/Objects` 同层，不引用扩展 crate（对标 Garnet.server 工程不引用
//! modules），扩展段统一派生自 [`crate::resp::custom_objects`] 编译期静态单表，
//! 扩展标签只有 server 层可见。
//!
//! RMW 执行域（同步/异步泛型骨架、分层感知收尾状态机、慢路径调度壳）已拆至
//! [`super::rmw_helpers`]，其符号经本模块 `pub use` 按原路径转发，消费面零改动。
//!
//! 降级约定与命令层一致：磁盘候选 / 环形页翻转须异步裁决时读侧返回 `Ok(None)`、
//! 写侧返回 `Ok(false)`，由调用方整体转异步重放。
//!
//! 中止工具对标 libs/server/Resp/Objects/ObjectStoreUtils.cs（C# 为 RespServerSession
//! partial）：`AbortWithWrongNumberOfArgumentsOrUnknownSubcommand` 已由
//! resp::admin_commands 在 RespServerSession 上实现（同映射注释），此处不再重复定义。

use core::str;

use wbase::{
  convert::{
    expire_after_ms_to_ticks, expire_after_to_ticks, expire_at_milliseconds_to_ticks,
    expire_at_seconds_to_ticks,
  },
  num::{strict_i32, strict_i64},
  time::now_ticks,
};
pub(crate) use wcol::object_payload::{
  GarnetObjectPayload, NO_EXPIRY_WATERMARK, ObjLoad, count_of_blob, expiry_watermark_of_blob,
};
use wcol::{
  object_payload::{
    obj_decode_custom, obj_encode_custom_into, obj_encode_into, object_heap_estimate,
  },
  types::garnet_object::IGarnetObject,
};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{
    self as cs, RESP_ERR_WRONG_TYPE, abort_with_wrong_number_of_arguments, write_error_raw,
  },
  ext::RespVecExt,
  options::{ExpirationWithOption, ExpireOption, try_get_expire_option},
};
use wval::{GarnetObjectType, KeyTag, MetaValue};

pub use super::rmw_helpers::{RespRmwDone, SyncRmwCmd, SyncRmwHandlers, run_sync_rmw};
pub(crate) use super::rmw_helpers::{
  SealedLoad, load_typed_sealed, obj_writeback_tiered, retire_tiered_dest, run_async_rmw,
  slow_load_eval, try_tiered_arm, write_rmw_reply,
};
use crate::{
  resp::{custom_objects::CUSTOM_OBJECT_ENTRIES, resp_server_session::RespServerSession},
  storage::session::{
    common::{
      TagRead,
      ttl_sync::{probe_alive_domain_with_prefix, rmw_ttl_rebuild_sync},
    },
    storage_session::StorageSession,
  },
};

/// 集合元素头部类型（FIELDS 或 MEMBERS）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElementHeaderKind {
  Fields,
  Members,
}

impl ElementHeaderKind {
  /// 元素头词元：对标 C# 各命令调用点直传给 GenericErrMandatoryMissing 的
  /// `"FIELDS"` / `"MEMBERS"` 字面量（libs/server/Resp/Objects/HashCommands.cs
  /// 的 HashExpire 族与 SortedSetCommands.cs 的 SortedSetExpire 族同形）
  #[inline]
  pub const fn token(self) -> &'static str {
    match self {
      Self::Fields => "FIELDS",
      Self::Members => "MEMBERS",
    }
  }

  /// 元素计数参数名：对标 C# 直传给 GenericParamShouldBeGreaterThanZero 与
  /// GenericErrMustMatchNoOfArgs 的 `{0}` 实参（同上传入点）
  #[inline]
  pub const fn num_param(self) -> &'static str {
    match self {
      Self::Fields => "numFields",
      Self::Members => "numMembers",
    }
  }

  #[inline]
  pub const fn mandatory_missing_err(self) -> &'static str {
    match self {
      Self::Fields => "Mandatory argument FIELDS is missing or not at the right position",
      Self::Members => "Mandatory argument MEMBERS is missing or not at the right position",
    }
  }

  #[inline]
  pub const fn greater_than_zero_err(self) -> &'static str {
    match self {
      Self::Fields => "ERR Parameter `numFields` should be greater than 0",
      Self::Members => "ERR Parameter `numMembers` should be greater than 0",
    }
  }

  #[inline]
  pub const fn must_match_args_err(self) -> &'static str {
    match self {
      Self::Fields => "The `numFields` parameter must match the number of arguments",
      Self::Members => "The `numMembers` parameter must match the number of arguments",
    }
  }
}

/// 解析 `FIELDS/MEMBERS count elem [elem...]` 公共头部，返回 `(elements_start_idx, num_elements)`
///
/// 成功路径零堆分配、O(1) 状态校验（失败帧文案编译期静态常量化，零运行时堆分配，
/// 对标 C# `AbortWithErrorMessage(template, token)`）。三段与各命令内联段逐段对位，
/// C# 无值域门（num 为 0 且计数吻合时正常执行，逐元素循环零次回 `*0`）：
/// 1. 关键字 FIELDS/MEMBERS → GenericErrMandatoryMissing
/// 2. TryGetInt 解析失败 → GenericParamShouldBeGreaterThanZero（上游文案命名误导，
///    仅出现在「压根不是整数」臂）
/// 3. 实参个数匹配 `Count != currIdx + num` → GenericErrMustMatchNoOfArgs
///
/// 本函数是 C# 内联六段重复头校验的单点合流：
/// libs/server/Resp/Objects/HashCommands.cs（HashExpire、HashTimeToLive、HashPersist）与
/// libs/server/Resp/Objects/SortedSetCommands.cs（SortedSetExpire、SortedSetTimeToLive、
/// SortedSetPersist）各命令体中的 FIELDS/MEMBERS 三段校验；命令级 C# 锚点登记在
/// hash_commands.rs 与 sorted_set_commands/write.rs 的对应命令函数上，此处不复挂。
///
/// 在 garnet 中的相对路径:libs/server/Resp/Objects/HashCommands.cs
pub fn parse_elements_header(
  parse_state: &[&[u8]],
  curr_idx: usize,
  kind: ElementHeaderKind,
  output: &mut Vec<u8>,
) -> Option<(usize, usize)> {
  if curr_idx >= parse_state.len()
    || !parse_state[curr_idx].eq_ignore_ascii_case(kind.token().as_bytes())
  {
    cs::abort_with_error_message(output, kind.mandatory_missing_err());
    return None;
  }

  let num_idx = curr_idx + 1;
  let Some(num_elements) = parse_state.get(num_idx).and_then(|raw| strict_i32(raw)) else {
    cs::abort_with_error_message(output, kind.greater_than_zero_err());
    return None;
  };

  // 计数比对在带符号域进行（对位 C# `parseState.Count != currIdx + numFields` 的
  // int 域比较），先比后转：不等（含负数）即 must match 早退，杜绝 usize 下溢 panic；
  // 比对通过则 num 必非负（len ≥ elements_start），转域 100% 安全
  let elements_start = num_idx + 1;
  if parse_state.len() as i64 != elements_start as i64 + i64::from(num_elements) {
    cs::abort_with_error_message(output, kind.must_match_args_err());
    return None;
  }

  Some((elements_start, num_elements as usize))
}

/// 过期时长 → 绝对过期 .NET Ticks（乘加/钳制公式统一委托 [`wbase::convert`]
/// 单点：相对时长 saturating，绝对时间戳负值夹 0、超界钳到最大可表示 ticks；
/// is_timestamp 对 UNIX_EPOCH，否则对当前时刻；供 hash 与 sorted_set 跨模块复用）
#[inline]
pub fn compute_expiration_ticks(expiration: i64, is_milliseconds: bool, is_timestamp: bool) -> i64 {
  if is_timestamp {
    if is_milliseconds {
      expire_at_milliseconds_to_ticks(expiration)
    } else {
      expire_at_seconds_to_ticks(expiration)
    }
  } else if is_milliseconds {
    expire_after_ms_to_ticks(now_ticks(), expiration)
  } else {
    expire_after_to_ticks(now_ticks(), expiration)
  }
}

// ============ 对象族参数推导单源（快慢路径共用） ============
// 对标 basic_commands 的 parse_*_args 单源范式：推导体为无 IO 纯解析 +
// 失败帧直写 output，快慢两侧各接各的 IO 原语与出帧通道，同输入应答逐字节
// 一致；慢分派不再对快路径已校验参数做第二份推导（SINTERCARD 半开区间
// 漂移即双写不同步的实证），RESP_ERR_ASYNC_REQUIRED 仅保留在真正未接线
// 的命令兜底臂。

/// HEXPIRE / ZEXPIRE 族参数推导结果
#[derive(Debug)]
pub struct ExpireElementsArgs<'a> {
  /// 目标键
  pub key: &'a [u8],
  /// FIELDS/MEMBERS 头之后的元素切片
  pub elements: &'a [&'a [u8]],
  /// ExpirationWithOption word 高低 32 位对（ticks 换算与打包单源）
  pub args12: (i32, i32),
}

/// HEXPIRE / ZEXPIRE 族参数推导单源（快慢路径共用；解析失败时已写出错误
/// 应答并返回 None），按 `kind` 参数化 FIELDS/MEMBERS 使两族共用一份
///
/// 判定序对标 C#（HashCommands.cs 的 HashExpire 与 SortedSetCommands.cs 的
/// SortedSetExpire 同构）：arity ≥ 5 → expiration i64（TryGetLong 口径）→
/// 负时刻拒 → NX|XX|GT|LT 词元 → 元素头（见 [`parse_elements_header`]）→
/// ticks 换算与 word 打包
pub fn parse_expire_elements_args<'a>(
  cmd_name: &'static str,
  parse_state: &'a [&'a [u8]],
  kind: ElementHeaderKind,
  is_milliseconds: bool,
  is_timestamp: bool,
  output: &mut Vec<u8>,
) -> Option<ExpireElementsArgs<'a>> {
  check_arg_count!(parse_state, 5.., output, cmd_name, return None);

  let key = parse_state[0];
  let Some(expiration) = strict_i64(parse_state[1]) else {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
    return None;
  };
  if expiration < 0 {
    cs::abort_with_error_message(output, cs::RESP_ERR_INVALID_EXPIRE_TIME);
    return None;
  }

  let mut curr_idx = 2;
  let mut expire_option = ExpireOption::NONE;
  if let Some(opt) = try_get_expire_option(parse_state[curr_idx]) {
    expire_option = opt;
    curr_idx += 1;
  }

  let (elements_start, _) = parse_elements_header(parse_state, curr_idx, kind, output)?;

  let e = ExpirationWithOption::new(
    compute_expiration_ticks(expiration, is_milliseconds, is_timestamp),
    expire_option,
  );
  Some(ExpireElementsArgs {
    key,
    elements: &parse_state[elements_start..],
    args12: ((e.word() >> 32) as i32, e.word() as i32),
  })
}

/// HTTL / HPERSIST / ZTTL / ZPERSIST 族参数推导单源（快慢路径共用；解析
/// 失败时已写出错误应答并返回 None）：arity ≥ 4 → key → 元素头（`kind`
/// 参数化 FIELDS/MEMBERS），返回 (key, 元素切片)
///
/// 判定序对标 C#（HashCommands.cs 的 HashTimeToLive/HashPersist 与
/// SortedSetCommands.cs 的 SortedSetTimeToLive/SortedSetPersist 同构）
pub fn parse_elements_only_args<'a>(
  cmd_name: &'static str,
  parse_state: &'a [&'a [u8]],
  kind: ElementHeaderKind,
  output: &mut Vec<u8>,
) -> Option<(&'a [u8], &'a [&'a [u8]])> {
  check_arg_count!(parse_state, 4.., output, cmd_name, return None);
  let (elements_start, _) = parse_elements_header(parse_state, 1, kind, output)?;
  Some((parse_state[0], &parse_state[elements_start..]))
}

/// 随机成员族（HRANDFIELD / ZRANDMEMBER）count 打包结果
#[derive(Debug)]
pub struct RandomMemberArgs {
  /// 打包 (count << 1 | included_count) << 1 | with_flag（对位 C# ObjectInput.arg1）
  pub arg1: i32,
  /// 钳至有符号 30 位后的 count
  pub param_count: i32,
  /// 是否显式给了 count（缺省 1）
  pub included_count: bool,
}

/// 随机成员族参数推导单源（快慢路径共用；解析失败时已写出错误应答并返回
/// None），`with_token` 参数化 WITHVALUES/WITHSCORES 大小写门
///
/// 判定序对标 C#（HashCommands.cs 的 HashRandomField 与 SortedSetCommands.cs
/// 的 SortedSetRandomMember 同构）：arity 1..=3 → count i32（TryGetInt 口径，
/// 负数合法表示从尾部取）→ 第三词元大小写门（仅 len==3 校验，语法错误帧）
/// → count 钳至有符号 30 位（预留 2 位元数据位，C# Math.Min 同款）
pub fn parse_random_member_args(
  cmd_name: &'static str,
  parse_state: &[&[u8]],
  with_token: &[u8],
  output: &mut Vec<u8>,
) -> Option<RandomMemberArgs> {
  check_arg_count!(parse_state, 1..=3, output, cmd_name, return None);

  let (param_count, included_count, with_flag) = match parse_state.get(1) {
    None => (1_i32, false, false),
    Some(raw) => {
      // C# TryGetInt（int32）：越界即 VALUE_IS_NOT_INTEGER
      let Some(v) = strict_i32(raw) else {
        cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return None;
      };
      // 第三词元大小写门（仅 len==3 校验，COUNT>3 不经此臂）
      let with_flag = match parse_state.get(2) {
        Some(token) if token.eq_ignore_ascii_case(with_token) => true,
        Some(_) => {
          cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return None;
        }
        None => false,
      };
      (v, true, with_flag)
    }
  };

  // 预留 2 位元数据位，count 钳至剩余有符号 30 位（C# Math.Min 同款）
  let param_count = param_count.min(i32::MAX >> 2);
  Some(RandomMemberArgs {
    arg1: ((param_count << 1) | i32::from(included_count)) << 1 | i32::from(with_flag),
    param_count,
    included_count,
  })
}

/// 随机成员族缺失态 / count=0 短路应答单源（C# NOTFOUND 臂与「count 为 0
/// 不触达后端」短路的同形应答：带 count → 空数组，否则 null 版本分派）
pub fn write_random_member_missing(output: &mut Vec<u8>, included_count: bool, resp_version: u8) {
  if included_count {
    // 空数组头经版本感知写帧单点（*0 双版本同形，对位 C# WriteDirect
    // RESP_EMPTYLIST）
    output.write_resp_array_len(0);
  } else {
    output.write_resp_null_ver(resp_version);
  }
}

/// 交集基数族（SINTERCARD / ZINTERCARD）命令形态
///
/// 两族判定序同构、错误帧不同源（C# 两份独立实现），帧选择随形态分发：
/// set 族 numkeys < 1 与键段短参同报 numkeys 帧；zset 族分别报
/// at-least-one-key 帧与 syntax error 帧
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntersectCardKind {
  Set,
  SortedSet,
}

impl IntersectCardKind {
  /// 命令名（arity 帧内嵌）
  pub const fn cmd_name(self) -> &'static str {
    match self {
      Self::Set => "SINTERCARD",
      Self::SortedSet => "ZINTERCARD",
    }
  }

  /// numkeys < 1 帧（set 族报 numkeys 文案常量；zset 族走 at-least-one-key
  /// 模板单点回填命令名，对标 C# SortedSetCommands.cs:1191 的
  /// `AbortWithErrorMessage(CmdStrings.GenericErrAtLeastOneKey, nameof(RespCommand.ZINTERCARD))`）
  fn write_numkeys_error(self, output: &mut Vec<u8>) {
    match self {
      Self::Set => cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_NUMKEYS),
      Self::SortedSet => cs::abort_with_at_least_one_key(output, self.cmd_name()),
    }
  }

  /// 键段短参帧（实参数不足以容纳 numkeys 个键）
  const fn err_short_keys(self) -> &'static str {
    match self {
      Self::Set => cs::RESP_ERR_GENERIC_NUMKEYS,
      Self::SortedSet => cs::RESP_ERR_GENERIC_SYNTAX_ERROR,
    }
  }
}

/// 交集基数族参数推导结果
#[derive(Debug)]
pub struct IntersectCardArgs<'a> {
  /// 参与交集的键切片
  pub keys: &'a [&'a [u8]],
  /// LIMIT 上限（None = 未给；0 与 None 同效，仅正值参与钳制）
  pub limit: Option<i32>,
}

/// SINTERCARD / ZINTERCARD 参数推导单源（快慢路径共用；解析失败时已写出
/// 错误应答并返回 None）
///
/// 判定序对标 C#（SetCommands.cs 的 SetIntersectLength 与
/// SortedSetCommands.cs 的 SortedSetIntersectLength 同构）：arity ≥ 2 →
/// numkeys i32（TryGetInt 口径，超值域视为非整数）→ numkeys ≥ 1 → 键段
/// 足量 → LIMIT 形态恰多 2 参（token 大小写门）→ limit i32 → 负值拒
pub fn parse_intersect_card_args<'a>(
  kind: IntersectCardKind,
  parse_state: &'a [&'a [u8]],
  output: &mut Vec<u8>,
) -> Option<IntersectCardArgs<'a>> {
  check_arg_count!(parse_state, 2.., output, kind.cmd_name(), return None);

  // C# TryGetInt（int32）：超 i32 值域（含负溢出）视为非整数
  let Some(num_keys) = strict_i32(parse_state[0]) else {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
    return None;
  };
  if num_keys < 1 {
    kind.write_numkeys_error(output);
    return None;
  }
  if parse_state.len().saturating_sub(1) < num_keys as usize {
    cs::abort_with_error_message(output, kind.err_short_keys());
    return None;
  }

  let idx = num_keys as usize + 1;
  let mut limit = None;
  if parse_state.len() > idx {
    if !parse_state[idx].eq_ignore_ascii_case(cs::LIMIT) || parse_state.len() != idx + 2 {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return None;
    }
    let Some(v) = strict_i32(parse_state[idx + 1]) else {
      cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return None;
    };
    if v < 0 {
      cs::abort_with_error_message(output, cs::RESP_ERR_LIMIT_CANT_BE_NEGATIVE);
      return None;
    }
    limit = Some(v);
  }

  Some(IntersectCardArgs {
    keys: &parse_state[1..=num_keys as usize],
    limit,
  })
}

impl RespServerSession {
  /// 参数数量错误中止（始终消费完整命令，返回 true）
  ///
  /// libs/server/Resp/Objects/ObjectStoreUtils.cs:AbortWithWrongNumberOfArguments
  pub fn abort_with_wrong_number_of_arguments(
    &mut self,
    cmd_name: &str,
    output: &mut Vec<u8>,
  ) -> bool {
    self.command_error_written = true;
    abort_with_wrong_number_of_arguments(output, cmd_name);
    true
  }

  /// 以给定错误信息中止
  ///
  /// libs/server/Resp/Objects/ObjectStoreUtils.cs:AbortWithErrorMessage
  /// （C# 置 commandErrorWritten 后经 RespWriteUtils 写错误帧）
  pub fn abort_with_error_message(&mut self, error_message: &[u8], output: &mut Vec<u8>) -> bool {
    self.command_error_written = true;
    let msg_str = str::from_utf8(error_message).unwrap_or("");
    write_error_raw(output, msg_str);
    true
  }
}

/// 信封解码错误分类（标签不符 vs 载荷畸形）
pub(super) enum EnvDecodeErr {
  /// 类型标签与请求类型不符（WRONGTYPE）
  WrongType,
  /// 标签相符但载荷畸形（fail-fast 显式失败，严禁静默回退空对象）
  Corrupt,
}

/// 载荷畸形错误文案（对标 C# GarnetObjectSerializer.DeserializeInternal
/// 抛 GarnetException 的可见失败语义：宁可回错，不可丢数据）
const RESP_ERR_CORRUPT_PAYLOAD: &str = "ERR Corrupted object payload";

/// 载荷畸形装载统一漏斗：带键与标签落错误日志 + 写错误帧，借 WrongType 装载态
/// 返回（其调用侧语义即「键存在但不可操作、错误已写出、命令中止」——不回写、
/// 不删除、不按 Missing 新建覆盖，保守保住原键原载荷）
fn corrupt_payload_reject<T>(output: &mut Vec<u8>, key: &[u8], tag: u8) -> ObjLoad<T> {
  log::error!(
    "obj_load: corrupted object envelope payload, key='{}' tag={tag:#04x}",
    String::from_utf8_lossy(key)
  );
  write_error_raw(output, RESP_ERR_CORRUPT_PAYLOAD);
  ObjLoad::WrongType
}

// ============ 对象探测决策核（sync/async 孪生共用） ============
// C# 侧同步与异步对象读只有一份实现：`where TObjectContext : ITsavoriteContext<…>`
// 把会话类型参数化（libs/server/Storage/Session/ObjectStore/Common.cs
// :ReadObjectStoreOperation / :RMWObjectStoreOperation）。rust 无法在同一函数体内
// 既 `await` 又同步执行（异步闭包上下文须装箱，热路径凭空多一次分配），故把 C# 的
// 「上下文泛型」换轴为「决策与 I/O 组合子分层」：
// - 纯决策（meta 闸、信封解码分类、到期水位门、WRONGTYPE 落地、String 域兜底次序）
//   在本段单点定义，签名里不含任何会话类型与 await；
// - 孪生函数只保留「发起读 → 归一化为 TagRead → 递进决策核」三步，不含任何裁决。

/// Meta 分层判定结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetaGate {
  /// Meta 不存在或非存活
  Absent,
  /// 命中且存活但类型相符需走降级（分层态慢路径 / 磁盘候选 / 存储故障）
  Degrade,
  /// 水位快路径直读短路返回 size
  SizeShortCircuit(usize),
  /// 类型不符
  WrongType,
}

/// 归一化后的 Meta 域读结果 → 分层判定核（三态折叠与类型/水位门唯一判定源）
#[inline]
fn meta_gate(read: TagRead<Option<MetaValue>>, tag: u8, want_size: bool) -> MetaGate {
  let Some(meta) = (match read {
    // 磁盘候选与存储故障：同步侧唯一正确出路是整体转异步重放
    TagRead::Deferred => return MetaGate::Degrade,
    TagRead::Missing => None,
    TagRead::Hit(meta) => meta,
  }) else {
    return MetaGate::Absent;
  };
  if !meta.is_live() {
    return MetaGate::Absent;
  }
  if meta.collection_type as u8 != tag {
    return MetaGate::WrongType;
  }
  if want_size && now_ticks() <= meta.next_expiry {
    return MetaGate::SizeShortCircuit(meta.size as usize);
  }
  MetaGate::Degrade
}

/// Meta 闸的未终局续步（装载族与长度族共用的探测树步进）
enum MetaStep<T> {
  /// 终局（错误帧按需已写出）
  Done(ObjLoad<T>),
  /// 放行：须继续探 ObjectEnvelope 内存信封
  Envelope,
}

/// 信封步的未终局续步
pub(super) enum EnvStep<T> {
  /// 终局（错误帧按需已写出）
  Done(ObjLoad<T>),
  /// 信封域确认缺失：须反探 String 域
  StringDomain,
}

/// 装载族 Meta 闸（sync/async 共用）：分层存活键一律转降级慢路径取真实对象
#[inline]
fn step_load_meta<T>(
  read: TagRead<Option<MetaValue>>,
  tag: u8,
  output: &mut Vec<u8>,
) -> MetaStep<T> {
  match meta_gate(read, tag, false) {
    MetaGate::Absent => MetaStep::Envelope,
    MetaGate::Degrade => MetaStep::Done(ObjLoad::Degrade),
    MetaGate::WrongType => {
      write_wrong_type(output);
      MetaStep::Done(ObjLoad::WrongType)
    }
    // want_size=false 恒不产出；即便产出，装载仍须真实对象而非计数
    MetaGate::SizeShortCircuit(_) => MetaStep::Envelope,
  }
}

/// 长度族 Meta 闸（sync/async 共用）：到期水位内直读 `MetaValue.size` 短路，
/// O(1) 零反序列化（RI.COUNT 非全区间臂与 HLEN/SCARD/ZCARD/LLEN 快路径同源）
#[inline]
fn step_len_meta(
  read: TagRead<Option<MetaValue>>,
  tag: u8,
  output: &mut Vec<u8>,
) -> MetaStep<usize> {
  match meta_gate(read, tag, true) {
    MetaGate::SizeShortCircuit(size) => MetaStep::Done(ObjLoad::Present(size)),
    MetaGate::Absent => MetaStep::Envelope,
    MetaGate::Degrade => MetaStep::Done(ObjLoad::Degrade),
    MetaGate::WrongType => {
      write_wrong_type(output);
      MetaStep::Done(ObjLoad::WrongType)
    }
  }
}

/// Meta 原始字节探针闭包（装载族与长度族 sync/async 四处共用）
#[inline]
fn meta_probe(raw: &[u8]) -> Option<MetaValue> {
  if raw.len() >= wval::META_VALUE_SIZE {
    MetaValue::from_slice(raw).ok()
  } else {
    None
  }
}

/// 信封装载解码闭包单点（装载族与自定义对象命令族 sync/async 四处共用）：
/// 剥壳后零拷贝直喂反序列化器，标签不符判 WRONGTYPE、载荷畸形判 Corrupt
#[inline]
pub(super) fn env_decode_object<T>(
  raw: &[u8],
  tag: u8,
  deserialize: impl FnOnce(&[u8]) -> Option<T>,
) -> Result<T, EnvDecodeErr> {
  let payload = obj_decode_custom(raw, tag).ok_or(EnvDecodeErr::WrongType)?;
  deserialize(payload).ok_or(EnvDecodeErr::Corrupt)
}

/// 信封头部 (计数, 到期水位) 解码单点（长度族 sync/async 两处共用）
#[inline]
fn env_decode_head(raw: &[u8], tag: GarnetObjectType) -> Result<ObjHead, EnvDecodeErr> {
  let payload = obj_decode_custom(raw, tag as u8).ok_or(EnvDecodeErr::WrongType)?;
  Ok(ObjHead {
    count: count_of_blob(payload).unwrap_or(0),
    watermark: expiry_watermark_of_blob(tag, payload).unwrap_or(NO_EXPIRY_WATERMARK),
  })
}

/// 信封头部计数与到期水位（4B 计数 + 8B 水位直读，零反序列化）
struct ObjHead {
  count: usize,
  watermark: i64,
}

/// 信封态到期水位门（长度族 sync/async 两处共用，与分层态 `meta_gate` 同构）：
/// 水位内直读头部计数恒精确；越线即字段级 TTL 已有成员到期、头部计数失真，
/// 转调用方既有物化矫正臂（Hlen/Zcard purge + mutated_by_ttl 升格写回一次矫正
/// 并触发删空自愈），矫正后水位前移回归 O(1)，每到期纪元至多一次
#[inline]
fn gate_head_length(head: ObjHead) -> ObjLoad<usize> {
  if now_ticks() <= head.watermark {
    ObjLoad::Present(head.count)
  } else {
    ObjLoad::Degrade
  }
}

/// 信封步决策（装载族与长度族 sync/async 四处共用）：命中值交 `present` 映射为
/// 族终局，标签不符落 WRONGTYPE、载荷畸形显式拒绝、磁盘候选降级、域内确认缺失
/// 转 String 域反探
#[inline]
pub(super) fn step_envelope<T, V>(
  read: TagRead<Result<V, EnvDecodeErr>>,
  key: &[u8],
  tag: u8,
  output: &mut Vec<u8>,
  present: impl FnOnce(V) -> ObjLoad<T>,
) -> EnvStep<T> {
  match read {
    TagRead::Hit(Ok(hit)) => EnvStep::Done(present(hit)),
    TagRead::Hit(Err(EnvDecodeErr::WrongType)) => {
      write_wrong_type(output);
      EnvStep::Done(ObjLoad::WrongType)
    }
    TagRead::Hit(Err(EnvDecodeErr::Corrupt)) => {
      EnvStep::Done(corrupt_payload_reject(output, key, tag))
    }
    TagRead::Deferred => EnvStep::Done(ObjLoad::Degrade),
    TagRead::Missing => EnvStep::StringDomain,
  }
}

/// String 域反探终局（装载族、长度族与自定义对象命令族 sync/async 六处共用）：
/// 命中即用户字符串键（WRONGTYPE），两域皆缺为键缺失，磁盘候选转异步
#[inline]
fn step_string_domain<T>(read: TagRead<Option<u8>>, output: &mut Vec<u8>) -> ObjLoad<T> {
  match read {
    TagRead::Hit(_) => {
      write_wrong_type(output);
      ObjLoad::WrongType
    }
    TagRead::Missing => ObjLoad::Missing,
    TagRead::Deferred => ObjLoad::Degrade,
  }
}

/// 同步标签读 + 归一化（[`wkv::BatchStoreSession::try_read_tag_sync`] 三态折叠）
///
/// 存储故障与磁盘候选同归 `Deferred`：同步侧对二者的唯一正确出路都是整体转异步
/// 重放（异步侧重发同一读并如实上抛错误），折叠方向单一、不吞错
#[inline]
pub(super) fn probe_tag_sync<T, D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: KeyTag,
  f: impl FnOnce(&[u8]) -> T,
) -> TagRead<T> {
  TagRead::from_result(store.try_read_tag_sync(key, tag, f)).unwrap_or(TagRead::Deferred)
}

/// 异步标签读 + 归一化（[`StorageSession::read_tag_with`] 的 `Option` 折叠）
///
/// 磁盘候选已在存储层异步读内核内闭环，故恒不产出 `Deferred`；IO 错误原样上抛，
/// 由调用方落慢路径存储错误帧
pub(super) async fn probe_tag_async<T, D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: KeyTag,
  f: impl Fn(&[u8]) -> T,
) -> wkv::Result<TagRead<T>> {
  Ok(
    storage
      .read_tag_with(key, tag, f)
      .await?
      .map_or(TagRead::Missing, TagRead::Hit),
  )
}

/// 同步发起 String 域读并归一化（探测树末步，裁决单源 [`step_string_domain`]）
#[inline]
pub(super) fn read_string_domain_sync<T, D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> ObjLoad<T> {
  step_string_domain(
    probe_tag_sync(store, key, KeyTag::String, |raw| raw.first().copied()),
    output,
  )
}

/// 异步发起 String 域读并归一化（探测树末步，裁决单源 [`step_string_domain`]）
#[inline]
pub(super) async fn read_string_domain_async<T, D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> wkv::Result<ObjLoad<T>> {
  Ok(step_string_domain(
    probe_tag_async(storage, key, KeyTag::String, |raw| raw.first().copied()).await?,
    output,
  ))
}

/// 通用同步对象装载
///
/// 读取走 KeyTag::ObjectEnvelope 物理域，信封剥壳后零拷贝直喂反序列化器；
/// 信封域未命中时反探 Meta 域与 String 域——命中即用户字符串键（WRONGTYPE），两域
/// 皆缺为键缺失；载荷畸形显式回错（`deserialize` 返回 `None` 即触发）
pub fn obj_load_typed_sync<T, D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  output: &mut Vec<u8>,
  deserialize: impl FnOnce(&[u8]) -> Option<T>,
) -> ObjLoad<T> {
  obj_load_custom_sync(store, key, tag as u8, output, deserialize)
}

/// 键同步装载是否不可出件（活跃分层键或磁盘候选）——阻塞族命令 park 前预探
///
/// 复用 [`obj_load_typed_sync`] 判定单源：返回 `Degrade` 即经纪取件源
/// （`CollectionItemSource` 经同一函数装载判型）同步恒不可出件，观察者
/// 将挂起至超时；此类键整体路由异步慢路径臂（物化降级 / 分层原生臂）闭环。
/// WRONGTYPE 不路由——经纪首试即据实回错，对位 C# GetCollectionItemAsync
/// 的 storageSession.GET 判型形态（C# 无分层与同步磁盘降级概念）
pub fn obj_load_sync_degrades<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
) -> bool {
  let mut scratch = Vec::new();
  matches!(
    obj_load_typed_sync(store, key, tag, &mut scratch, |_| Some(())),
    ObjLoad::Degrade
  )
}

/// 写入 WRONGTYPE 错误帧（全模块单点）
#[inline]
fn write_wrong_type(output: &mut Vec<u8>) {
  write_error_raw(output, RESP_ERR_WRONG_TYPE);
}

/// 自定义对象同步装载（支持 u8 扩展标签）
///
/// 三步探测树（Meta 分层闸 → ObjectEnvelope 信封解码 → String 域反探）的裁决
/// 全在共享决策核（[`step_load_meta`] / [`step_envelope`] / [`step_string_domain`]），
/// 本函数与 [`obj_load_custom_async`] 各自只发起读并归一化为 [`TagRead`]
pub fn obj_load_custom_sync<T, D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  output: &mut Vec<u8>,
  deserialize: impl FnOnce(&[u8]) -> Option<T>,
) -> ObjLoad<T> {
  // 1. Meta 分层闸
  if let MetaStep::Done(load) = step_load_meta(
    probe_tag_sync(store, key, KeyTag::Meta, meta_probe),
    tag,
    output,
  ) {
    return load;
  }
  // 2. ObjectEnvelope 内存信封（剥壳零拷贝直喂反序列化器）
  if let EnvStep::Done(load) = step_envelope(
    probe_tag_sync(store, key, KeyTag::ObjectEnvelope, |raw| {
      env_decode_object(raw, tag, deserialize)
    }),
    key,
    tag,
    output,
    ObjLoad::Present,
  ) {
    return load;
  }
  // 3. String 域反探（命中即用户字符串键）
  read_string_domain_sync(store, key, output)
}

/// 通用异步对象装载
///
/// 读取走 KeyTag::ObjectEnvelope 物理域，信封剥壳后反序列化；
/// 信封域未命中时反探 Meta 域与 String 域——命中即用户字符串键（WRONGTYPE），两域
/// 皆缺为键缺失。
/// 对齐 obj_load_typed_sync 语义与 WRONGTYPE 判定口径
pub async fn obj_load_typed_async<T, D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  output: &mut Vec<u8>,
  deserialize: impl Fn(&[u8]) -> Option<T>,
) -> wkv::Result<ObjLoad<T>> {
  obj_load_custom_async(storage, key, tag as u8, output, deserialize).await
}

/// 自定义对象异步装载（支持 u8 扩展标签）—— [`obj_load_custom_sync`] 的异步对位，
/// 决策核共用，差异仅在读原语（磁盘候选在存储层异步内核内闭环，故无降级态）
/// 与 IO 错误如实上抛
pub async fn obj_load_custom_async<T, D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: u8,
  output: &mut Vec<u8>,
  deserialize: impl Fn(&[u8]) -> Option<T>,
) -> wkv::Result<ObjLoad<T>> {
  // 1. Meta 分层闸
  if let MetaStep::Done(load) = step_load_meta(
    probe_tag_async(storage, key, KeyTag::Meta, meta_probe).await?,
    tag,
    output,
  ) {
    return Ok(load);
  }
  // 2. ObjectEnvelope 内存信封（剥壳零拷贝直喂反序列化器）
  if let EnvStep::Done(load) = step_envelope(
    probe_tag_async(storage, key, KeyTag::ObjectEnvelope, |raw| {
      env_decode_object(raw, tag, &deserialize)
    })
    .await?,
    key,
    tag,
    output,
    ObjLoad::Present,
  ) {
    return Ok(load);
  }
  // 3. String 域反探（命中即用户字符串键）
  read_string_domain_async(storage, key, output).await
}

/// 同步直读集合对象元素数量（信封头计数 + 到期水位门 / 分层态 MetaValue.size
/// 直读短路，O(1) 零反序列化与零扫页）
///
/// 探测树与裁决单源（[`step_len_meta`] 承接分层水位快路径，
/// [`gate_head_length`] 承接信封态水位门：越线即字段级 TTL 已有成员到期、头部
/// 计数失真，转调用方既有物化矫正臂，每到期纪元至多一次）
#[inline]
pub fn obj_length_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  output: &mut Vec<u8>,
) -> ObjLoad<usize> {
  // 1. Meta 分层闸（水位内直读 MetaValue.size 短路）
  if let MetaStep::Done(load) = step_len_meta(
    probe_tag_sync(store, key, KeyTag::Meta, meta_probe),
    tag as u8,
    output,
  ) {
    return load;
  }
  // 2. ObjectEnvelope 内存信封（4B 计数 + 8B 到期水位直读）
  if let EnvStep::Done(load) = step_envelope(
    probe_tag_sync(store, key, KeyTag::ObjectEnvelope, |raw| {
      env_decode_head(raw, tag)
    }),
    key,
    tag as u8,
    output,
    gate_head_length,
  ) {
    return load;
  }
  // 3. String 域反探（命中即用户字符串键）
  read_string_domain_sync(store, key, output)
}

/// 异步直读集合对象元素数量—— [`obj_length_sync`] 的异步对位，决策核共用
#[inline]
pub async fn obj_length_async<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  output: &mut Vec<u8>,
) -> wkv::Result<ObjLoad<usize>> {
  // 1. Meta 分层闸（水位内直读 MetaValue.size 短路）
  if let MetaStep::Done(load) = step_len_meta(
    probe_tag_async(storage, key, KeyTag::Meta, meta_probe).await?,
    tag as u8,
    output,
  ) {
    return Ok(load);
  }
  // 2. ObjectEnvelope 内存信封（4B 计数 + 8B 到期水位直读）
  if let EnvStep::Done(load) = step_envelope(
    probe_tag_async(storage, key, KeyTag::ObjectEnvelope, |raw| {
      env_decode_head(raw, tag)
    })
    .await?,
    key,
    tag as u8,
    output,
    gate_head_length,
  ) {
    return Ok(load);
  }
  // 3. String 域反探（命中即用户字符串键）
  read_string_domain_async(storage, key, output).await
}

/// 对象信封内层堆内存估算（MEMORY USAGE 消费点，标准段 + 自定义扩展段单点合流）
///
/// 对标 libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs:HandleMemoryUsage
/// （:117 `srcLogRecord.ValueObject.HeapMemorySize`）：C# 由 `IHeapObject`
/// （libs/storage/Tsavorite/cs/src/core/Allocator/IHeapObject.cs:12）多态取对象
/// 自身记账，估算实现落在 libs/server/Objects 各对象与 modules 各对象内，
/// Garnet.server 工程不引用 modules；rust 对象以 `[1B 标签][载荷]` 信封落库，
/// 标签分派须由持有扩展 crate 编译期接线（Cargo feature）的 server 层承担——
/// 标准段委托 [`wcol::object_payload::object_heap_estimate`]；扩展段走
/// [`crate::resp::custom_objects::CUSTOM_OBJECT_ENTRIES`] 编译期静态单表
/// （各清单项 `heap_estimate` 自持对象级记账，对标 modules/RoaringBitmap/
/// RoaringBitmapObject.cs 真实记账与 GarnetJsonObject.cs 恒定 DictionaryOverhead
/// 两种 IHeapObject.HeapMemorySize 语义），编译期定长比对链，零运行时查表、
/// 零锁；未启用相应特性或未知标签按 0 计（信封记录物理尺寸已含序列化载荷
/// 本体，不重复计）
#[inline]
pub(crate) fn envelope_heap_estimate(raw: &[u8]) -> i64 {
  if let Some(size) = object_heap_estimate(raw) {
    return size;
  }
  // 扩展段（标签自 wval::CustomObjectType 分配单点，标准段外）：编译期单表派生
  match raw.split_first() {
    Some((&tag, payload)) => CUSTOM_OBJECT_ENTRIES
      .iter()
      .find(|entry| entry.tag.as_u8() == tag)
      .map_or(0, |entry| (entry.heap_estimate)(payload)),
    None => 0,
  }
}

/// 同步写对象信封（记录挂 KeyTag::ObjectEnvelope 物理域）
///
/// 入口前置 [`rmw_ttl_rebuild_sync`] 过期残留清退（读侧把过期键判缺失后，
/// RMW 重建写回若保留残留 TTL，新值写完立即可判过期——对标 C#
/// ObjectStore/RMWMethods.cs 的 CheckExpiry → ExpireAndResume 后转
/// InitialUpdater，初始记录无 Expiration）。
/// `Ok(true)` 已闭环；`Ok(false)` 须降级异步（含 TTL 记录磁盘候选降级，
/// 异步臂经 wkv `read_tag_with` 的 probe_alive 惰性清除自愈）；`Err` 存储层错误
///
/// 本入口只判 TTL 残留、不复验域归属：RMW 窗口持有人（[`super::rmw_helpers::run_sync_rmw`]
/// 及其异步对位）落笔前必须先过 [`obj_save_recheck_sync`]，杜绝窗口期内已 ACK 的
/// 并发 DEL/SET 被尾段盲写顶掉
pub(super) fn obj_save_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  payload: &[u8],
) -> wkv::Result<bool> {
  if !rmw_ttl_rebuild_sync(store, key)? {
    return Ok(false);
  }
  let mut val = Vec::with_capacity(payload.len() + 1);
  obj_encode_into(tag, payload, &mut val);
  Ok(
    store
      .try_upsert_tag_sync(key, KeyTag::ObjectEnvelope, &val)?
      .is_ok(),
  )
}

/// 写回复验判定核（同步档与异步档共用唯一裁决）：装载时刻存活域 == 落笔时刻存活域
#[inline(always)]
fn obj_domain_unchanged(loaded: Option<KeyTag>, current: Option<KeyTag>) -> bool {
  loaded == current
}

/// 对象信封写回前终态复验·同步档（RMW 窗口持有人在落笔前调用）
///
/// 缺陷背景（票 r6-del-rmw-writeback-revalidate）：[`super::rmw_helpers::run_sync_rmw`]
/// 与 [`super::rmw_helpers::run_async_rmw`] 的 [`wkv::RmwWindow`] 桶排他闩只覆盖 RMW 方，
/// 对面删除者与覆写者按**物理记录键**取记录桶闩（wkv 的
/// `try_delete_sync_unprotected_with_prefix` / `try_upsert_tag_sync_unprotected_with_prefix`），
/// 与本窗口的**用户键**桶是两个不同基（garnet 单份哈希表故同键同桶，wedb 同键的
/// TTL / 字符串 / 信封 / 元记录各自成键故各落一桶），窗口期内 DEL 可自由墓碑信封、
/// SET 可自由清退信封并写字符串；信封写回臂此前只经 [`rmw_ttl_rebuild_sync`] 判 TTL
/// 残留，不复验域归属，尾段落笔即令已 ACK 删除的键复活，或令同键 String 与信封
/// 双物理域并存（打破「用户键同一时刻至多驻留一个物理域」不变式，对象族与字符串族
/// 按各自探测序给出类型面发散答案）。
///
/// C# 无此形态：对象求值与写回全在记录 X 锁内（ObjectStore/RMWMethods.cs 的
/// CopyUpdater / InPlaceUpdaterWorker），同键覆写与删除互斥于同一条记录锁
/// （MainStore/UpsertMethods.cs:InPlaceWriter、MainStore/DeleteMethods.cs:InitialDeleter），
/// 「装载 → 写回」间隙根本不存在。rust 不扩锁面（把 DEL/SET 纳入本桶闩须先对信封
/// 写回自身嵌套的 `try_delete_sync` / `try_upsert_tag_sync` 做重入折算，且令全仓最热
/// 两条写路径锁面翻倍），改为落笔前复验域归属。
///
/// 判据与装载同向单源：装载臂 `Present` 只可能来自 ObjectEnvelope 步、`Missing` 即
/// 三域存活判缺（同一 TTL 门裁决），故期望域传 [`KeyTag::ObjectEnvelope`]（既存装载）
/// 或 `None`（缺失新建），现域取 [`probe_alive_domain_with_prefix`] 三域存活探针，
/// 二者相等即窗口期内域归属未被触碰；探针磁盘候选（同步段不可裁决）一律判不复通过，
/// 绝不盲写。本复验只判**域归属**，不判 TTL——过期残留清退仍由 [`rmw_ttl_rebuild_sync`]
/// 单点承担，杜绝第二套过期判定；wkv 两个同步写删入口照旧不取 rmw 窗口（双锁序
/// 死锁面），由本复验承接交错裁决
pub fn obj_save_recheck_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  loaded: Option<KeyTag>,
) -> wkv::Result<bool> {
  let prefix = store.session_prefix();
  Ok(
    probe_alive_domain_with_prefix(store, prefix.as_slice(), key)?
      .is_some_and(|current| obj_domain_unchanged(loaded, current)),
  )
}

/// [`obj_save_recheck_sync`] 的异步档（RMW 窗口让核等待臂专用，同一判定核）：
/// 同步档先行（暖态零额外 I/O），三域有磁盘候选时交
/// [`StorageSession::probe_alive_domain_async_with_prefix`] 闭环终态（过期记录在异步读内核内惰性清退后视同缺失，无降级态）
pub async fn obj_save_recheck_async<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  loaded: Option<KeyTag>,
) -> wkv::Result<bool> {
  let prefix = storage.batch.session_prefix();
  let current = match probe_alive_domain_with_prefix(&storage.batch, prefix.as_slice(), key)? {
    Some(current) => return Ok(obj_domain_unchanged(loaded, current)),
    None => {
      storage
        .probe_alive_domain_async_with_prefix(prefix.as_slice(), key)
        .await?
    }
  };
  Ok(obj_domain_unchanged(loaded, current))
}

/// 信封整值写回 + 入账：写成功后同栈触发信封整值写通知（ObjectStoreUpsert
/// 全量条目，对标 C# WriteLogUpsert：libs/server/Storage/Functions/ObjectStore/
/// PrivateMethods.cs:WriteLogUpsert）
///
/// resp 层同步快路径散点（HCOLLECT/ZCOLLECT 单键、GEOADD/ZUNIONSTORE、
/// SPOP/LPOP 族、集合项经纪取件、自定义对象命令同步臂）统一收口——这些点
/// 携带的命令上下文各异，增量条目（ObjectStoreRMW）无法在此层一处合成，
/// 以全量收敛等价闭环；增量条目仅由 [`run_sync_rmw`] 经
/// [`notify_object_rmw_raw`] + 显式 ObjectStoreRMW 通知单独承接，不重复入账
pub(super) fn obj_save_notified<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  payload: &[u8],
) -> wkv::Result<bool> {
  obj_save_custom_notified(store, key, tag as u8, payload)
}

/// 自定义对象信封整值写回 + 入账（支持 u8 扩展标签）
///
/// 入口前置 [`rmw_ttl_rebuild_sync`] 过期残留清退（语义同 [`obj_save_sync`]）
pub(super) fn obj_save_custom_notified<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: u8,
  payload: &[u8],
) -> wkv::Result<bool> {
  if !rmw_ttl_rebuild_sync(store, key)? {
    return Ok(false);
  }
  let mut val = Vec::with_capacity(payload.len() + 1);
  obj_encode_custom_into(tag, payload, &mut val);
  let saved = store
    .try_upsert_tag_sync(key, KeyTag::ObjectEnvelope, &val)?
    .is_ok();
  if saved {
    let raw_key = store.session_tag_key(KeyTag::ObjectEnvelope, key);
    // AOF 入队失败不回滚已生效的信封写（主存先行语义），告警可见
    if let Err(e) = store
      .store
      .notify_envelope_upsert(raw_key.as_slice(), val.as_slice())
    {
      log::error!("对象信封写 AOF 入队失败: {e}");
    }
  }
  Ok(saved)
}

/// 移除族收尾（无入账内核）：对象被删空时整键回收，否则写回新载荷
///
/// 供 [`run_sync_rmw`] 复用（其增量条目另行显式通知，杜绝双份入账）。
/// 删除经 wkv 双域删除内核（信封墓碑由写监听入队 StoreDelete）。
pub(super) fn obj_save_or_gc_raw<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  payload: &[u8],
  now_empty: bool,
) -> wkv::Result<bool> {
  if now_empty {
    return Ok(store.try_delete_sync(key)?.is_ok());
  }
  obj_save_sync(store, key, tag, payload)
}

/// 移除族收尾：对象被删空时整键回收，否则写回新载荷并自动入账
///
/// 对齐 storage 层 `finalize_removal`（对象为空即回收键，不留空对象信封）。
/// 返回约定同 [`obj_save_sync`]；非空写回成功后经 [`obj_save_notified`]
/// 同栈入账（删空臂的信封墓碑由写监听 StoreDelete 承接）
pub(super) fn obj_save_or_gc_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  payload: &[u8],
  now_empty: bool,
) -> wkv::Result<bool> {
  if now_empty {
    return Ok(store.try_delete_sync(key)?.is_ok());
  }
  obj_save_notified(store, key, tag, payload)
}

/// 通用对象变更写回或 GC（空集合整键回收，否则更新信封载荷；自动入账）
///
/// 升阶判定下沉单点（doc/zh/collection.md 3.2 全臂就地升阶）：非空写回前若
/// `obj.should_promote()` 命中或信封记录超页（[`envelope_overflow`]），则不写
/// 信封、不入账，直接返回 `Ok(false)`——与
/// [`run_sync_rmw`] 写回前判同款 `Degrade → 上层统一转异步重放 →
/// [`apply_rmw_post_operate`] 闭环 `promote_collection_to_bftree` 就地升阶`。
/// 判据沿用 `wcol` 单源（garnet_object.rs `should_promote`，不新造第二套门限）；
/// 漏斗外的手写写回臂（SMOVE/LMOVE/GEOADD/BZPOPMIN 族/HCOLLECT/Lua 面等）经此
/// 单点统一收口，禁在各臂逐个补判。同步入账通道（本函数 [`obj_save_notified`]
/// 全量条目）与异步升阶重灌通道互斥：命中降级时于任何写入前返回，不残留信封
/// 写与全量入账，无双份 AOF
pub fn obj_save_or_gc<T: IGarnetObject, D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  obj: &T,
  is_empty: bool,
  serialize: impl FnOnce(&T) -> Vec<u8>,
) -> wkv::Result<bool> {
  if !is_empty && obj.should_promote() {
    return Ok(false);
  }
  let payload = if is_empty { Vec::new() } else { serialize(obj) };
  if !is_empty && envelope_overflow(store, key, &payload) {
    return Ok(false);
  }
  obj_save_or_gc_sync(store, key, tag, &payload, is_empty)
}

/// 信封记录超页判定（升阶容量门，一处定义）
///
/// 物理记录 = 信封物理键 + [1 字节类型标签 + 序列化载荷]（`obj_encode_into`
/// 前缀，见文件头信封存储格式），与 whlog 写侧 `RecordTooLarge` 拒绝共用
/// `record_size` 单一公式（经 `WedbStore::record_fits_page` 转发）。
///
/// 动机：记录不可跨页是两侧共口径（rust whlog `validate_append_args` ⇄ C#
/// AllocatorBase.cs:TryAllocate "Entry does not fit on page" 硬抛），但 C#
/// 默认 16m 页（ServerOptions.cs PageSize）且无升阶机制，信封恒可长到页容量；
/// rust 页容量随预算收缩（[64KB,16MB]），页容量低于升阶内存阈
/// （wcol::TIERED_PROMOTE_BYTES = 4MB）的配置下，中间信封落在
/// (页容量, 升阶阈) 死区：obj_save 被 RecordTooLarge 拒、should_promote 又
/// 未达阈且失败写不推进状态 → 键永久卡死（实测异值 1000×600B 起第二块即拒；
/// 同值载荷因 bitcode 对重复内容的折叠变小侥幸绕过，非契约）。调用方据此把
/// 「信封装不下」视同超升阶阈改走既有升阶臂（wbftree 树按成员逐条入表，
/// 无单记录上限），异值批与同值批同命
pub(crate) fn envelope_overflow<D: Device>(
  store: &BatchStoreSession<'_, D>,
  user_key: &[u8],
  payload: &[u8],
) -> bool {
  let phys = store.session_tag_key(KeyTag::ObjectEnvelope, user_key);
  !store.store.record_fits_page(phys.len(), payload.len() + 1)
}

#[cfg(test)]
mod abort_tests {
  use wbase::convert::{TICKS_PER_MILLISECOND, TICKS_PER_SECOND, UNIX_EPOCH_TICKS};

  use super::*;

  #[test]
  fn abort_frames_match_csharp_text() {
    let mut sess = RespServerSession::default();
    let mut out = Vec::new();
    assert!(sess.abort_with_wrong_number_of_arguments("ZADD", &mut out));
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ZADD' command\r\n"
    );

    out.clear();
    assert!(sess.abort_with_error_message(b"ERR custom", &mut out));
    assert_eq!(out, b"-ERR custom\r\n");
  }

  #[test]
  fn test_parse_elements_header_fields_and_members() {
    let mut out = Vec::new();

    // 正常 FIELDS
    let args: &[&[u8]] = &[b"key", b"FIELDS", b"2", b"f1", b"f2"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
    assert_eq!(res, Some((3, 2)));
    assert!(out.is_empty());

    // 正常 MEMBERS
    let args: &[&[u8]] = &[b"key", b"MEMBERS", b"1", b"m1"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
    assert_eq!(res, Some((3, 1)));
    assert!(out.is_empty());

    // 缺失 FIELDS
    out.clear();
    let args: &[&[u8]] = &[b"key", b"WRONG", b"1", b"m1"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-Mandatory argument FIELDS is missing or not at the right position\r\n"
    );

    // 缺失 MEMBERS
    out.clear();
    let args: &[&[u8]] = &[b"key", b"WRONG", b"1", b"m1"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-Mandatory argument MEMBERS is missing or not at the right position\r\n"
    );

    // num 为 0 且计数吻合：C# 无值域门，放行零元素（HashCommands.cs:HashExpire 同臂）
    out.clear();
    let args: &[&[u8]] = &[b"key", b"MEMBERS", b"0"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
    assert_eq!(res, Some((3, 0)));
    assert!(out.is_empty());

    // num 为 0 但计数不吻合 → must match（非 greater-than-0）
    out.clear();
    let args: &[&[u8]] = &[b"key", b"FIELDS", b"0", b"x"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-The `numFields` parameter must match the number of arguments\r\n"
    );

    // 负数：C# TryGetInt 接受负值后落 Count 比对臂 → must match
    out.clear();
    let args: &[&[u8]] = &[b"key", b"FIELDS", b"-1"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-The `numFields` parameter must match the number of arguments\r\n"
    );

    // i32 极值：usize 转域溢出回归护栏（带符号域比对，debug 构建不 panic）
    out.clear();
    let args: &[&[u8]] = &[b"key", b"MEMBERS", b"-2147483648"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-The `numMembers` parameter must match the number of arguments\r\n"
    );

    out.clear();
    let args: &[&[u8]] = &[b"key", b"FIELDS", b"2147483647"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-The `numFields` parameter must match the number of arguments\r\n"
    );

    // num 不是数字
    out.clear();
    let args: &[&[u8]] = &[b"key", b"FIELDS", b"abc"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Fields, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-ERR Parameter `numFields` should be greater than 0\r\n"
    );

    // 参数数量不匹配
    out.clear();
    let args: &[&[u8]] = &[b"key", b"MEMBERS", b"2", b"m1"];
    let res = parse_elements_header(args, 1, ElementHeaderKind::Members, &mut out);
    assert_eq!(res, None);
    assert_eq!(
      out,
      b"-The `numMembers` parameter must match the number of arguments\r\n"
    );
  }

  #[test]
  fn test_compute_expiration_ticks() {
    let now_before = now_ticks();
    let ticks_sec = compute_expiration_ticks(10, false, false);
    let now_after = now_ticks();
    assert!(ticks_sec >= now_before + 10 * TICKS_PER_SECOND);
    assert!(ticks_sec <= now_after + 10 * TICKS_PER_SECOND);

    let ticks_ms = compute_expiration_ticks(500, true, false);
    assert!(ticks_ms >= now_before + 500 * TICKS_PER_MILLISECOND);

    let ts_sec = compute_expiration_ticks(1_700_000_000, false, true);
    assert_eq!(ts_sec, UNIX_EPOCH_TICKS + 1_700_000_000 * TICKS_PER_SECOND);

    let ts_ms = compute_expiration_ticks(1_700_000_000_000, true, true);
    assert_eq!(
      ts_ms,
      UNIX_EPOCH_TICKS + 1_700_000_000_000 * TICKS_PER_MILLISECOND
    );

    // 溢出防护（saturating）
    let max_ticks = compute_expiration_ticks(i64::MAX, false, false);
    assert_eq!(max_ticks, i64::MAX);
  }
}
