//! SCAN 参数过滤解析（快路径校验段与慢路径执行段共用）
//!
//! 对标 libs/server/Resp/ArrayCommands.cs:NetworkSCAN 参数校验段与
//! libs/server/Storage/Session/ArrayKeyIterationFunctions.cs:DbScan 的
//! TYPE 比对表；应答出帧与扫描执行见 [`super`] 与
//! [`crate::resp::garnet_api::slow::scan`]。

use std::borrow::Cow;

use wbase::num::strict_i64;
use wresp::cmd_strings::{
  RESP_ERR_GENERIC_INVALIDCURSOR, RESP_ERR_GENERIC_SYNTAX_ERROR,
  RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
};
use wval::GarnetObjectType;

use crate::storage::session::common::array_key_iteration_functions::ScanTypeFilter;

/// SCAN 未指定 COUNT 时的默认单页计数（Redis/garnet 同口径的 COUNT 10）
const SCAN_DEFAULT_COUNT: usize = 10;

/// SCAN 过滤参数（C# NetworkSCAN 局部变量组的结构化承接）
///
/// 快路径校验段与慢路径执行段共用同一解析单源 [`parse_scan_filter`]

#[derive(Debug)]
pub struct ScanFilter {
  /// 游标（C# cursorFromInput；地址游标口径，0 = 从头）
  pub cursor: i64,
  /// 匹配模式（默认 `*` 静态借用零分配；显式 MATCH 臂才落堆；C# patternArgSlice）
  pub pattern: Cow<'static, [u8]>,
  /// 模式为 `*` 时全量放行（C# allKeys）
  pub all_keys: bool,
  /// 单页计数（默认 10；负/零钳 0——C# countValue 直传扫描层
  /// `acceptedCount >= count`，首条匹配后即停）
  pub count: usize,
  /// 已知 TYPE 参数映射的过滤类型（C# matchType）
  pub type_filter: Option<ScanTypeFilter>,
  /// TYPE 参数出现过（此时单页无上限，C# `long.MaxValue` 同口径）
  pub type_given: bool,
  /// TYPE 值不属于五类已知类型（C# DbScan 对非空未知 typeObject 直接
  /// 回空列表 + 游标 0，ArrayKeyIterationFunctions.cs:82-84）。末值覆盖
  /// 语义：对位 C# typeParameterValue 局部直赋（ArrayCommands.cs:305-311），
  /// 每个 TYPE 词元解析前清位重判，前次非法值可被后次合法 TYPE 覆清；
  /// 空串 TYPE 归本标志系 §20 b 在册刻意偏离（见解析臂注释），边界不变
  pub type_unknown: bool,
}

/// SCAN 已知选项种类（三选项一律携一元组参数，取参与缺参语法错共用骨架）
#[derive(Clone)]
enum ScanOpt {
  Match,
  Count,
  Type,
}

/// [`ScanTypeFilter::Object`] 简构（保持比对表表项单行）
const fn obj(t: GarnetObjectType) -> ScanTypeFilter {
  ScanTypeFilter::Object(t)
}

/// SCAN TYPE 取值双形态精确比对表（C# DbScan 全大写/全小写各一，
/// ArrayKeyIterationFunctions.cs:57-76；string 的 C# 常量名 stringt 但值为 "string"）。
/// 混合大小写及表外值（含空串，§20 b 在册偏离）归未知臂回空
const SCAN_TYPES: &[(&[u8], &[u8], ScanTypeFilter)] = &[
  (b"string", b"STRING", ScanTypeFilter::String),
  (b"list", b"LIST", obj(GarnetObjectType::List)),
  (b"set", b"SET", obj(GarnetObjectType::Set)),
  (b"hash", b"HASH", obj(GarnetObjectType::Hash)),
  (b"zset", b"ZSET", obj(GarnetObjectType::SortedSet)),
];
/// 编译期自检：五类已知类型各双形态，表项数恒 5（新增类型须同步扩表）
const _: () = assert!(SCAN_TYPES.len() == 5);

/// 解析 SCAN 参数（cursor + MATCH/COUNT/TYPE 选项）
///
/// 校验口径 1:1 对标 C# NetworkSCAN：cursor 非法/负值、选项缺参、COUNT
/// 非整数均返回完整 RESP 错误行；未知选项静默跳过（C# if/else-if 链无
/// else 分支）；TYPE 取值按 C# DbScan 双形态精确比对（全大写/全小写各一，
/// ArrayKeyIterationFunctions.cs:57-76），混合大小写及其它未知值归未知类型，
/// 由慢路径直接回空结果（C# DbScan 提前返回同口径）。选项名本身（MATCH/COUNT/
/// TYPE）大小写不敏感（C# EqualsUpperCaseSpanIgnoringCase 同口径）。
/// `RespServerSession::network_scan` 的共享解析单源（快路径校验段与
/// 慢路径执行段同一入口，单次实现）
pub fn parse_scan_filter(args: &[&[u8]]) -> Result<ScanFilter, &'static str> {
  // C# TryGetLong 失败（非整数）与负值同回 invalid cursor，双臂折叠单帧校验
  let Some(cursor) = strict_i64(args.first().copied().unwrap_or(b"")).filter(|c| *c >= 0) else {
    return Err(RESP_ERR_GENERIC_INVALIDCURSOR);
  };

  let mut filter = ScanFilter {
    cursor,
    pattern: Cow::Borrowed(b"*"),
    all_keys: true,
    count: SCAN_DEFAULT_COUNT,
    type_filter: None,
    type_given: false,
    type_unknown: false,
  };
  let mut token_idx = 1;
  while token_idx < args.len() {
    let param = args[token_idx];
    token_idx += 1;
    // 选项名本身大小写不敏感（C# EqualsUpperCaseSpanIgnoringCase 同口径）；
    // 未知选项静默跳过（C# if/else-if 链无 else，仅消费参数名本身）
    let opt = if param.eq_ignore_ascii_case(b"MATCH") {
      ScanOpt::Match
    } else if param.eq_ignore_ascii_case(b"COUNT") {
      ScanOpt::Count
    } else if param.eq_ignore_ascii_case(b"TYPE") {
      ScanOpt::Type
    } else {
      continue;
    };
    // 已知选项一律携一元组参数：共享取参骨架，缺参回语法错
    let Some(&value) = args.get(token_idx) else {
      return Err(RESP_ERR_GENERIC_SYNTAX_ERROR);
    };
    token_idx += 1;
    match opt {
      ScanOpt::Match => {
        filter.pattern = Cow::Owned(value.to_vec());
        filter.all_keys = value == b"*";
      }
      ScanOpt::Count => {
        // 刻意偏离：C# TryGetLong 仅校验整数性，若 count<=0 直传，
        // 在底层扫描由于 acceptedCount(0) >= count(<=0) 立即返回 true，
        // 导致未扫描即中断；外层若无键匹配（keys.Count == 0）会硬写游标 0，
        // 此处修复性规整，负/零钳 0，并在扫描层 max(1) 保证至少扫描 1 条。
        // 已登记 doc/zh/deviations.md §188，勿按 C# 改回。
        let Some(n) = strict_i64(value) else {
          return Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        };
        filter.count = n.max(0) as usize;
      }
      ScanOpt::Type => {
        filter.type_given = true;
        // 末值覆盖（对位 C# ArrayCommands.cs:305-311：typeParameterValue 为
        // 局部 ReadOnlySpan 直赋，每个 TYPE 词元整体覆盖前值，不留标志位）：
        // 解析本词元类型值前先清未知标志，前次非法 TYPE 不粘滞，判型只取
        // 末次词元（末值合法覆清、末值非法由下方未知臂照常置位）
        filter.type_unknown = false;
        // 双形态精确比对走 const 表 [`SCAN_TYPES`]；未知类型（含混合大小写、
        // stream、空串等表外值）落 None：C# DbScan 对非空未知 typeObject 回空
        // 列表 + 游标 0。刻意偏离：C# 空串 TYPE 会透传并忽略类型过滤，此处统一
        // 视为空集。已登记 doc/zh/deviations.md 第 20 条 b，勿按 C# 改回。
        filter.type_filter = SCAN_TYPES
          .iter()
          .find(|(lo, up, _)| value == *lo || value == *up)
          .map(|(_, _, f)| *f);
        filter.type_unknown = filter.type_filter.is_none();
      }
    }
  }
  Ok(filter)
}
