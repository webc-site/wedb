//! Vector Set 命令的网络层（对标 libs/server/Resp/Vector/RespServerSessionVectors.cs）
//!
//! C# 为 RespServerSession 的 partial，直接消费 parseState / storageApi /
//! networkSender；Rust 侧 RespServerSession 属并行域，此处以
//! `&[&[u8]]` 参数面 + [`VectorReply`] 应答面承接同一命令语义
//! （选项解析、重复项报错、默认值、RESP2/RESP3 应答布局）。
//!
//! 数值解析复用 [`wbase::num`] 的 `strict_i32`/`strict_f32`（逐项对齐 C#
//! `parseState.TryGetInt`/`TryGetFloat` 的严格语义），错误文案与命令应答对齐
//! C# 字面量。

use core::fmt;
use std::{borrow::Cow, ops::RangeBounds, sync::Arc};

use wbase::num::{strict_f32, strict_i32};
use wdev::Device;
use wresp::{
  cmd_strings as cs,
  cmd_strings::{RESP_ERR_SLOW_PATH_STORAGE, write_error_raw},
  command::RespCommand,
  ext::is_resp3,
  options::equals_ignore_case,
  resp_memory_writer::{Resp2, Resp3, RespProtocol as _, RespWriter},
  wrong_num_args,
};
use wval::KeyTag;
use wvector::{VectorDistanceMetricType, VectorQuantType, VectorValueType, unpack_length_prefixed};

use super::{
  ERR_VECTOR_SET_DISABLED,
  vector_manager::{
    ERR_VECTOR_SERVICE_RESPONSE, MAX_EXPLORATION_FACTOR, MAX_FILTERING_SCALE_FACTOR,
    MAX_RETRIEVE_COUNT, MAX_VECTOR_DIMENSIONS, VectorAddArgs, VectorManager, VectorManagerResult,
    VectorSearchOptions,
  },
  vector_manager_locking::CreateIndexParams,
};

/// VADD 的 M 取值边界（libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD 的 MinM/MaxM）。
const MIN_M: i32 = 4;
const MAX_M: i32 = 4_096;

/// VSIM 默认结果数（NetworkVSIM 的 DefaultResultSetSize 语义：count ??= 10）。
const DEFAULT_VSIM_COUNT: i32 = 10;
/// VSIM 默认搜索探索因子（count ??= 10 / searchExplorationFactor ??= 100）。
const DEFAULT_VSIM_EF: i32 = 100;
/// VSIM 默认过滤过取放大（maxFilteringEffort ??= 16）。
const DEFAULT_VSIM_FILTER_EF: i32 = 16;
/// VSIM 默认距离截断上限（对齐 C# RespServerSessionVectors.cs:860 epsilon ?? 2f）。
const DEFAULT_VSIM_EPSILON: f32 = 2.0;

// ── 错误文案常量（逐字节对齐 C# 字面量；自带 RESP 前缀原样写出） ──

/// AbortVectorSetWrongType：对齐 Redis 行为、不指名具体类型。
/// 注意：本条为无句点版，C# 在
/// libs/server/Resp/Vector/RespServerSessionVectors.cs:AbortVectorSetWrongType
/// 就地书写同款无句点字面量，与 CmdStrings.RESP_ERR_WRONG_TYPE 的带句点版
/// 是两条不同文案，不合并（与已合并的 wresp::cmd_strings 单点无副本关系）。
const ERR_VECTOR_SET_WRONG_TYPE: &[u8] =
  b"WRONGTYPE Operation against a key holding the wrong kind of value";
const ERR_INVALID_VECTOR_SPEC: &[u8] = b"ERR invalid vector specification";
const ERR_REDUCE_MUST_BE_POSITIVE: &[u8] = b"REDUCE dimension must be > 0";
const ERR_REDUCE_EXCEEDS_DIMS: &[u8] = b"ERR REDUCE dimension must be <= vector dimensions";
const ERR_INVALID_OPTION_AFTER_ELEMENT: &[u8] = b"ERR invalid option after element";
const ERR_QUANT_SPECIFIED_TWICE: &[u8] = b"Quantization specified multiple times";
const ERR_EF_RANGE: &[u8] = b"ERR EF must be an integer between 1 and 1000000";
const ERR_M_RANGE: &[u8] = b"ERR M must be an integer between 4 and 4096";
const ERR_INVALID_DISTANCE_METRIC: &[u8] = b"ERR invalid XDISTANCE_METRIC";
const ERR_EMPTY_VECTOR_SET_KEY: &[u8] = b"ERR Vector Set key cannot be empty";
const ERR_QUANT_MISMATCH: &[u8] = super::vector_manager::ERR_QUANTIZATION_MISMATCH;
const ERR_FP32_MULTIPLE_OF_4: &[u8] = b"FP32 values must be multiple of 4-bytes in size";
const ERR_VALUES_COUNT_MUST_BE_POSITIVE: &[u8] = b"VALUES count must > 0";
const ERR_VALUES_MUST_BE_FLOAT: &[u8] = b"VALUES value must be valid float";
const ERR_VSIM_EXPECTED_KIND: &[u8] = b"VSIM expected ELE, FP32, or VALUES";
const ERR_COUNT_RANGE: &[u8] = b"ERR COUNT must be an integer between 0 and 100000000";
const ERR_EPSILON_MUST_BE_POSITIVE: &[u8] = b"EPSILON must be float > 0";
const ERR_FILTER_EF_RANGE: &[u8] = b"ERR FILTER-EF must be an integer between 4 and 256";
const ERR_UNKNOWN_OPTION: &[u8] = b"Unknown option";
/// 元素不在集合中（刻意偏差见 `doc/zh/deviations.md` §79：激活 C# 会话层死分支文案，严禁回改）。
pub(crate) const ERR_ELEMENT_NOT_IN_SET: &[u8] = b"Element not in Vector Set";
const ERR_VEMB_UNEXPECTED_OPTION: &[u8] = b"Unexpected option to VEMB";
const ERR_KEY_NOT_FOUND: &[u8] = b"ERR Key not found";
const ERR_VLINKS_UNEXPECTED_OPTION: &[u8] = b"ERR Unexpected option";
const ERR_EXPECTED_INTEGER_COUNT: &[u8] = b"ERR expected integer count";
/// 超出服务端上下文配额（C# 为 GarnetException；Rust 防御性转为错误应答）。
const ERR_MAX_ALLOCATIONS_EXCEEDED: &[u8] =
  b"ERR Maximum Vector Set allocations exceeded, cannot issue new context";

/// 选项重复文案（对齐 C# `"<OPT> specified multiple times"` 字面量）。
macro_rules! err_dup {
  ($opt:literal) => {
    VectorReply::err(concat!($opt, " specified multiple times").as_bytes())
  };
}

/// 开关型选项消费骨架（各 NetworkV* 选项循环共用）：重复置位即回
/// [`err_dup!`] 编译期文案，否则置位并跳过关键字参。
macro_rules! dup_flag {
  ($cur:expr, $seen:expr, $opt:literal) => {
    if *$seen {
      return Err(err_dup!($opt));
    }
    *$seen = true;
    $cur.skip();
  };
}

/// 命令入口 arity 守卫骨架（各 NetworkV* 入口共用）：预览未启用 / 参数
/// 个数不在 `range` 区间即就地应答返回（文案 [`wrong_num_args!`] 单点）。
macro_rules! wna_entry {
  ($sess:expr, $args:expr, $range:expr, $cmd:literal) => {
    if let Some(reply) = $sess.entry($args, $range, wrong_num_args!($cmd).as_bytes()) {
      return reply;
    }
  };
}

/// 命令应答（RESP 数据模型）。
///
/// 静态文案（错误/简单字符串）以 `&'static [u8]` 借用承载，动态载荷
/// （存储读出值、非常量错误）经 `Cow<'static, [u8]>` 落堆：
/// 检索命中 id/属性的源缓冲（`SimilarityOutput`）为函数局部量，应答归还
/// 后即释放，故借用上限为 'static，非静态载荷一律 Owned。
/// 整型标量不入 `Bulk`：一律走 [`VectorReply::BulkInt`]，帧字节由 wresp 单点在
/// 编码期以 itoa 栈上缓冲产出，构造期零堆分配。
#[derive(Debug, Clone, PartialEq)]
pub enum VectorReply {
  /// 简单字符串（+...，载荷恒为编译期常量文案）。
  Simple(&'static [u8]),
  /// 错误（-...，含完整前缀）。
  Error(Cow<'static, [u8]>),
  /// 整数。
  Integer(i64),
  /// 批量字符串（None = NULL）。
  Bulk(Option<Cow<'static, [u8]>>),
  /// 整数值批量字符串（`$<len>\r\n<digits>\r\n`）。
  ///
  /// 对标 C# RespServerSessionVectors.cs:1608-1616 `WriteInt32AsBulkString` /
  /// `WriteInt64AsBulkString`：应答面把整型标量按 bulk 串交付的命令（VINFO 的
  /// 维度/参数/基数）一律用本变体，不得再 `to_string().into_bytes()` 造临时堆物。
  BulkInt(i64),
  /// 数组。
  Array(Vec<VectorReply>),
  /// 空（NULL）数组：RESP2 `*-1\r\n`、RESP3 `_\r\n`（帧型由 wresp Resp2/Resp3 单点承载）。
  NullArray,
  /// RESP3 映射（RESP2 退化为键值交错的扁平数组）。
  Map(Vec<(VectorReply, VectorReply)>),
  /// RESP3 双精度浮点。
  Double(f64),
  /// 布尔（RESP3）。
  Boolean(bool),
}

impl VectorReply {
  /// 静态错误文案应答（借用零分配）。
  #[inline]
  fn err(msg: &'static [u8]) -> Self {
    Self::Error(Cow::Borrowed(msg))
  }

  /// 编码为 RESP2 字节（Double 退化为 bulk 字符串、Map 退化为双倍长度数组）。
  ///
  /// 各臂帧型一律转调 wresp 单点（RespWriter / Resp2 协议面），本枚举仅作
  /// 「数据模型 → 单点」薄壳，不持有第二套帧字节。
  pub fn encode_resp2(&self, out: &mut Vec<u8>) {
    let mut w = RespWriter::new_ref(out);
    match self {
      VectorReply::Simple(s) => w.write_simple_string_bytes(s),
      VectorReply::Error(e) => w.write_error_bytes(e),
      VectorReply::Integer(i) => w.write_int64(*i),
      VectorReply::Bulk(v) => match v {
        Some(v) => w.write_bulk_string(v),
        None => Resp2::write_null(w.buf_mut()),
      },
      // 整型 bulk 臂：帧字节由 wresp 单点（RespWriteUtils.cs:542,565 对位）产出，
      // RESP3 同型（该臂在 encode_resp3 落 `other => encode_resp2` 兜底，无第二份帧）
      VectorReply::BulkInt(i) => w.write_int64_as_bulk_string(*i),
      VectorReply::NullArray => Resp2::write_null_array(w.buf_mut()),
      // RESP2 口径 map 头即双倍长度数组（C# TryWriteMapLength resp2 分支）
      VectorReply::Map(pairs) => {
        w.write_map_length(pairs.len());
        let out = w.buf_mut();
        for (k, v) in pairs {
          k.encode_resp2(out);
          v.encode_resp2(out);
        }
      }
      VectorReply::Double(d) => w.write_double_bulk_string(*d),
      VectorReply::Boolean(b) => {
        // RESP2 面批量串布尔（生产调用点在构造期即分派 Integer 臂，本臂只余
        // 嵌套数组形态）；帧字节由 bulk 单点产出，与 RESP3 的 `#t/#f` 同处枚举分派
        w.write_bulk_string(if *b { b"1" } else { b"0" });
      }
      VectorReply::Array(items) => {
        w.write_array_length(items.len());
        let out = w.buf_mut();
        for item in items {
          item.encode_resp2(out);
        }
      }
    }
  }

  /// 编码为 RESP3 字节（Double 为 `,`、Boolean 为 `#`、Map 为 `%`、
  /// null 族为 `_\r\n`；其余帧与 RESP2 同型，转调 [`Self::encode_resp2`]）。
  pub fn encode_resp3(&self, out: &mut Vec<u8>) {
    let mut w = RespWriter::<_, Resp3>::new_ref_p(out);
    match self {
      VectorReply::Double(d) => w.write_double_numeric(*d),
      VectorReply::Boolean(b) => Resp3::write_bool(w.buf_mut(), *b),
      VectorReply::Map(pairs) => {
        w.write_map_length(pairs.len());
        let out = w.buf_mut();
        for (k, v) in pairs {
          k.encode_resp3(out);
          v.encode_resp3(out);
        }
      }
      VectorReply::Array(items) => {
        w.write_array_length(items.len());
        let out = w.buf_mut();
        for item in items {
          item.encode_resp3(out);
        }
      }
      VectorReply::Bulk(None) => Resp3::write_null(w.buf_mut()),
      VectorReply::NullArray => Resp3::write_null_array(w.buf_mut()),
      other => other.encode_resp2(w.buf_mut()),
    }
  }

  /// 统一按协议版本编码为 RESP 字节
  #[inline]
  pub fn encode_resp(&self, out: &mut Vec<u8>, resp3: bool) {
    if resp3 {
      self.encode_resp3(out);
    } else {
      self.encode_resp2(out);
    }
  }
}

// ======================== 命令入口解析骨架 ========================

/// VADD 参数个数不足文案（`wrong_num_args!` 编译期展开；入口与取参共用单点）。
const WNA_VADD: &[u8] = wrong_num_args!("VADD").as_bytes();
/// VSIM 参数个数不足文案（同上）。
const WNA_VSIM: &[u8] = wrong_num_args!("VSIM").as_bytes();

/// VADD 默认建索引探索因子（对齐 C# buildExplorationFactor ??= 200）。
const DEFAULT_VADD_BUILD_EF: i32 = 200;
/// VADD 默认每层链数（对齐 C# numLinks ??= 16）。
const DEFAULT_VADD_NUM_LINKS: i32 = 16;

/// 向量取参形态（[`VALUE_KINDS`] 表载荷；ELE 由 VSIM 单独识别，不入表）。
#[derive(Clone, Copy, PartialEq)]
enum ValueKind {
  /// FP32 原始字节
  Fp32,
  /// VALUES num v1..vn 文本浮点
  Values,
  /// XU8 原始字节
  Xu8,
  /// XI8 原始字节
  Xi8,
}

/// 格式关键字 → 取参形态（表序即 VADD 原 if-chain 识别序；关键字互斥故顺序无关）。
const VALUE_KINDS: &[(&[u8], ValueKind)] = &[
  (b"FP32", ValueKind::Fp32),
  (b"VALUES", ValueKind::Values),
  (b"XU8", ValueKind::Xu8),
  // XB8 为 XU8 的向后兼容别名，推荐 XU8
  (b"XB8", ValueKind::Xu8),
  (b"XI8", ValueKind::Xi8),
];

/// 量化器选项关键字 → 量化类型（表序即原识别序；重复指定判据由调用点承接）。
const QUANT_OPTS: &[(&[u8], VectorQuantType)] = &[
  (b"NOQUANT", VectorQuantType::NoQuant),
  (b"Q8", VectorQuantType::Q8),
  (b"BIN", VectorQuantType::Bin),
  (b"XNOQUANT_U8", VectorQuantType::XnoQuantU8),
  // XPREQ8 为 XNOQUANT_U8 的向后兼容别名，推荐 XNOQUANT_U8
  (b"XPREQ8", VectorQuantType::XnoQuantU8),
  (b"XNOQUANT_I8", VectorQuantType::XnoQuantI8),
  (b"XBIN_I8", VectorQuantType::XbinI8),
  (b"XBIN_U8", VectorQuantType::XbinU8),
];

/// 距离度量关键字 → 度量类型（VADD XDISTANCE_METRIC 值参）。
const METRIC_OPTS: &[(&[u8], VectorDistanceMetricType)] = &[
  (b"L2", VectorDistanceMetricType::L2),
  (b"COSINE", VectorDistanceMetricType::Cosine),
  (b"IP", VectorDistanceMetricType::InnerProduct),
  (
    b"XCOSINE_NORMALIZED",
    VectorDistanceMetricType::XCosineNormalized,
  ),
];

/// 编译期选项表命中：`arg` 大小写无关命中表内关键字即取其载荷。
#[inline]
fn lookup<T: Copy>(table: &[(&[u8], T)], arg: &[u8]) -> Option<T> {
  table
    .iter()
    .find(|(kw, _)| equals_ignore_case(arg, kw))
    .map(|(_, v)| *v)
}

/// 值格式每维字节数（FP32 4 字节，XU8/XI8 单字节）。
#[inline]
const fn dim_bytes(value_type: VectorValueType) -> usize {
  match value_type {
    VectorValueType::FP32 => 4,
    _ => 1,
  }
}

/// X 系量化器（与 REDUCE 互斥，对齐 C# 调 storageApi 前的 BadParams 判定）。
#[inline]
const fn is_x_quant(quant: VectorQuantType) -> bool {
  matches!(
    quant,
    VectorQuantType::XbinU8
      | VectorQuantType::XbinI8
      | VectorQuantType::XnoQuantU8
      | VectorQuantType::XnoQuantI8
  )
}

/// 真值应答：RESP3 布尔 / RESP2 整数 1·0（对齐 C# 各命令的 resp3 分派）。
#[inline]
fn bool_reply(value: bool, resp3: bool) -> VectorReply {
  if resp3 {
    VectorReply::Boolean(value)
  } else {
    VectorReply::Integer(i64::from(value))
  }
}

/// 检索输出上限：有位图时 popcount，否则全部命中，再与 count 取小（C# 同款）。
#[inline]
fn output_limit(total_found: usize, filter_bitmap: &[u8], count: usize) -> usize {
  if filter_bitmap.is_empty() {
    return total_found.min(count);
  }
  filter_bitmap
    .iter()
    .map(|b| b.count_ones() as usize)
    .sum::<usize>()
    .min(count)
}

/// 过滤通过项下标序列（RESP2/RESP3 输出共用骨架）：零压实契约下按原结果
/// 下标剔除未过项、至多产出 `limit` 项（与逐位 `break`/`continue` 手写循环同序同集）。
fn passed_indices(
  total_found: usize,
  filter_bitmap: &[u8],
  limit: usize,
) -> impl Iterator<Item = usize> {
  let has_filter = !filter_bitmap.is_empty();
  (0..total_found)
    .filter(move |&i| !has_filter || (filter_bitmap[i >> 3] >> (i & 7)) & 1 != 0)
    .take(limit)
}

/// 向量取参的非法文案集（VADD/VSIM 各成一表，逐字保持原命令文案）。
struct OperandErrs {
  /// 关键字后取尽缺参
  missing: &'static [u8],
  /// FP32 字节数非每维字节数倍数
  align: &'static [u8],
  /// VALUES 数量非法
  count: &'static [u8],
  /// VALUES 单值非法
  float: &'static [u8],
  /// 格式关键字未知
  kind: &'static [u8],
}

/// VADD 向量取参文案（缺参走 wrong-num-args，余下四项原文案统一 invalid spec）。
const VADD_OPERAND: OperandErrs = OperandErrs {
  missing: WNA_VADD,
  align: ERR_INVALID_VECTOR_SPEC,
  count: ERR_INVALID_VECTOR_SPEC,
  float: ERR_INVALID_VECTOR_SPEC,
  kind: ERR_INVALID_VECTOR_SPEC,
};

/// VSIM 向量取参文案（各色非法文案逐一区别于 VADD，禁合并）。
const VSIM_OPERAND: OperandErrs = OperandErrs {
  missing: WNA_VSIM,
  align: ERR_FP32_MULTIPLE_OF_4,
  count: ERR_VALUES_COUNT_MUST_BE_POSITIVE,
  float: ERR_VALUES_MUST_BE_FLOAT,
  kind: ERR_VSIM_EXPECTED_KIND,
};

/// 向量取参产出（VADD/VSIM 共用形态）。
struct Operand<'a> {
  /// 值格式（ELE 形态为 Invalid）
  value_type: VectorValueType,
  /// 向量字节（原始字节借参数缓冲、VALUES 文本浮点落堆；ELE 为空）
  values: Cow<'a, [u8]>,
  /// 维度（值格式步长折算）
  dims: usize,
  /// ELE 形态的查询元素（仅 VSIM 产出）
  element: Option<&'a [u8]>,
}

/// 参数游标：命令入口「识别关键字 → 取值 → 校验 → 前进」骨架的承接。
/// 缺失/非法应答文案一律由调用点供给，逐字保持各命令原文案。
struct Cur<'a> {
  args: &'a [&'a [u8]],
  ix: usize,
}

impl<'a> Cur<'a> {
  #[inline]
  fn new(args: &'a [&'a [u8]], ix: usize) -> Self {
    Self { args, ix }
  }

  /// 当前位置参数（取尽为空切片，永不命中表内关键字）。
  #[inline]
  fn peek(&self) -> &'a [u8] {
    self.args.get(self.ix).copied().unwrap_or_default()
  }

  /// 剩余参数未取尽（选项循环的续跑判据）。
  #[inline]
  fn more(&self) -> bool {
    self.ix < self.args.len()
  }

  /// 当前位置是否命中关键字 `kw`（不前进）。
  #[inline]
  fn at(&self, kw: &[u8]) -> bool {
    equals_ignore_case(self.peek(), kw)
  }

  /// 跳过当前位置（无值参的开关型选项）。
  #[inline]
  fn skip(&mut self) {
    self.ix += 1;
  }

  /// 取当前参并前进；取尽回空串（C# 对缺参不做显式校验的形态）。
  #[inline]
  fn next_or_empty(&mut self) -> &'a [u8] {
    let arg = self.peek();
    self.ix += 1;
    arg
  }

  /// 取当前参并前进；取尽即回 `missing` 文案应答。
  fn next(&mut self, missing: &'static [u8]) -> Result<&'a [u8], VectorReply> {
    let arg = self
      .args
      .get(self.ix)
      .copied()
      .ok_or_else(|| VectorReply::err(missing))?;
    self.ix += 1;
    Ok(arg)
  }

  /// 取当前关键字后的值参并前进两位；缺失回 `missing`。
  fn value(&mut self, missing: &'static [u8]) -> Result<&'a [u8], VectorReply> {
    self.skip();
    self.next(missing)
  }

  /// 当前关键字后跟严格 i32 值参：缺失回 `missing`、非法或不满足 `ok` 回
  /// `bad`（`strict_i32` 对齐 C# `parseState.TryGetInt`，前导零拒收系 rust
  /// 严格收口，见 doc/zh/deviations.md §32）。
  #[inline]
  fn i32_value(
    &mut self,
    missing: &'static [u8],
    ok: impl Fn(i32) -> bool,
    bad: &'static [u8],
  ) -> Result<i32, VectorReply> {
    strict_i32(self.value(missing)?)
      .filter(|&v| ok(v))
      .ok_or_else(|| VectorReply::err(bad))
  }

  /// 当前关键字后跟严格 f32 值参（应答口径同 [`Self::i32_value`]）。
  #[inline]
  fn f32_value(
    &mut self,
    missing: &'static [u8],
    ok: impl Fn(f32) -> bool,
    bad: &'static [u8],
  ) -> Result<f32, VectorReply> {
    strict_f32(self.value(missing)?, true)
      .filter(|&v| ok(v))
      .ok_or_else(|| VectorReply::err(bad))
  }
}

/// VADD 同步解析段的产出（C# NetworkVADD 校验完毕、调 storageApi 前的
/// 参数快照）：键/元素/属性借参数缓冲，VALUES 文本浮点落堆（Cow），标量
/// 集为合成默认值后的终值。执行段 [`RespServerSessionVectors::network_vadd_slow`]
/// 以此取齐 [`CreateIndexParams`] / [`VectorAddArgs`]。
struct VaddPlan<'a> {
  /// 集合键（借参数缓冲）
  key: &'a [u8],
  /// 元素键（借参数缓冲）
  element: &'a [u8],
  /// 向量格式
  value_type: VectorValueType,
  /// 向量字节（FP32/XU8/XI8 借参数零拷贝，VALUES 合成落堆）
  values: Cow<'a, [u8]>,
  /// 属性（缺省空串）
  attributes: &'a [u8],
  /// REDUCE 降维（0 = 无）
  reduce_dims: u32,
  /// 量化器（默认 Q8）
  quant: VectorQuantType,
  /// 建索探索因子（默认 200）
  build_ef: u32,
  /// 每层链数（默认 16）
  num_links: u32,
  /// 距离度量（默认 L2）
  distance_metric: VectorDistanceMetricType,
  /// 会话库级定槽（doc/zh/db.md 4.1）
  slot: u16,
  /// 向量维度（value_type 步长推导，供 manager 校验）
  dims: u32,
}

/// VSIM 同步解析段的产出（C# NetworkVSIM 校验完毕、调 storageApi 前的参数
/// 快照）：键/元素/过滤借参数缓冲，VALUES 文本浮点落堆（Cow），检索标量
/// 为合成默认值后的终值。执行段 [`RespServerSessionVectors::network_vsim`]
/// 以此取齐 [`VectorSearchOptions`] 与检索中心。
struct VsimPlan<'a> {
  /// 集合键（借参数缓冲）
  key: &'a [u8],
  /// ELE 形态的查询元素（Some 即以元素为中心，忽略 `values`）
  element: Option<&'a [u8]>,
  /// 查询向量格式
  value_type: VectorValueType,
  /// 查询向量字节
  values: Cow<'a, [u8]>,
  /// 检索参数（默认值合成后的终值；`count` 即应答上限）
  search: VectorSearchOptions<'a>,
  /// WITHSCORES
  with_scores: bool,
}

use wvector::store::StoreCallbacks;

use crate::{
  resp::vector::vector_store_callbacks::WedbVectorStoreCallbacks,
  storage::session::{
    common::{
      TagRead, read_tag_sync,
      ttl_sync::{probe_alive_domain, purge_expired_residue_sync},
    },
    storage_session::StorageSession,
  },
};

/// 向量集命令前置守卫裁决（快路径同步探针三态，[`RespServerSessionVectors::vector_key_guard`] 产出）
pub enum VectorGuardVerdict {
  /// 键驻留非向量值域：回 WRONGTYPE（对齐 C# res==WRONGTYPE 分支）
  Reject(VectorReply),
  /// 值域双缺：放行走登记表命令面
  Allow,
  /// 磁盘候选待裁决 / 存储错误：同步段读不准。只读命令由 exec 转慢路径
  /// [`Self::network_vector_read_slow`] 真读裁决；写命令保守拒（取舍登记
  /// doc/zh/deviations.md §22）
  Degrade,
}

/// Vector Set 命令处理（会话承接层）。
pub struct RespServerSessionVectors<
  S: StoreCallbacks = WedbVectorStoreCallbacks<wdev::SegmentedDevice>,
> {
  /// 向量集合管理器。
  pub manager: Arc<VectorManager<S>>,
}

impl<S: StoreCallbacks> RespServerSessionVectors<S> {
  /// 创建命令处理层。
  pub fn new(manager: Arc<VectorManager<S>>) -> Self {
    Self { manager }
  }

  /// 向量集命令前置守卫判据（exec 向量分支单点；C# 无对位函数——C# 各
  /// NetworkV* 经 VectorManager.Locking.cs:ReadVectorIndexCore 的
  /// Read_MainStore 真读落盘裁决后 res 三态就地分派，rust 快路径以同步探针
  /// 承接同一判据、读不准态转 [`Self::network_vector_read_slow`] 闭环）
  ///
  /// 键驻留 wkv 值域（string / 对象信封，或 RangeIndex 专属的 Meta 元记录
  /// 域）即 Reject，杜绝与既有非向量键并行建向量集产生双域键（C# 判定为主
  /// 存记录 RecordType 非向量集；rust 索引记录驻留域内登记表，wkv 域命中即
  /// 非向量键）。磁盘候选待裁决与存储错误同步段读不准交 Degrade——写命令由
  /// exec 按保守拒处置（误拒可 DEL 后重试，双域键一经写即成幽灵，取舍登记
  /// doc/zh/deviations.md §22），只读命令降级异步真读
  pub fn vector_key_guard<'a, D: Device>(
    &self,
    key: &[u8],
    store: &wkv::BatchStoreSession<'a, D>,
  ) -> VectorGuardVerdict {
    match probe_alive_domain(store, key) {
      // 存活命中非向量值域：错型事实确证
      Ok(Some(Some(_))) => VectorGuardVerdict::Reject(self.wrong_type_reply()),
      // 值域双缺：再探 Meta 元记录域（RangeIndex 单树专属物理域）
      Ok(Some(None)) => match read_tag_sync(store, key, KeyTag::Meta, |_| ()) {
        Ok(TagRead::Missing) => VectorGuardVerdict::Allow,
        // Meta 内存命中 = RI / 升阶记录在驻，非向量事实确证
        Ok(TagRead::Hit(())) => VectorGuardVerdict::Reject(self.wrong_type_reply()),
        // Meta 磁盘候选 / 存储错误：与首层同款读不准降级
        Ok(TagRead::Deferred) | Err(_) => VectorGuardVerdict::Degrade,
      },
      // 磁盘候选待裁决 / 存储错误：读不准降级
      Ok(None) | Err(_) => VectorGuardVerdict::Degrade,
    }
  }

  /// 守卫 WRONGTYPE 应答帧（文案单点：C# RespServerSessionVectors.cs:
  /// AbortVectorSetWrongType 的无句点字面量，与带句点的
  /// CmdStrings.RESP_ERR_WRONG_TYPE 是两条不同文案）
  pub(crate) fn wrong_type_reply(&self) -> VectorReply {
    VectorReply::err(ERR_VECTOR_SET_WRONG_TYPE)
  }

  /// 只读向量命令冷态降级裁决与应答（慢路径承接；对标 C# 各 NetworkV* 的
  /// res 三态分派——Read_MainStore 真读落盘裁决后 WRONGTYPE / NOTFOUND 族
  /// 就地应答，RespServerSessionVectors.cs:905-911/1529-1533/1830 等）
  ///
  /// 快路径守卫（[`Self::vector_key_guard`]）报 Degrade（磁盘候选待裁决 /
  /// 存储错误）时由 exec 转投本面：异步三域真读后——存活非向量键回
  /// WRONGTYPE（res==WRONGTYPE 分支），缺失键（冷态墓碑 / 不存在）放行登记
  /// 表 NOTFOUND 族应答（res==NOTFOUND 分支：VSIM/VEMB 空数组、VREM 0、
  /// VGETATTR null、VDIM "ERR Key not found" 等），存储错误回慢路径统一错误帧
  pub async fn network_vector_read_slow<D: Device>(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    args: &[&[u8]],
    resp_version: u8,
    output: &mut Vec<u8>,
  ) {
    let key = args.first().copied().unwrap_or(&[]);
    let resp3 = is_resp3(resp_version);
    match storage.probe_alive_domain(key).await {
      // 真读存活：非向量记录，对齐 C# res==WRONGTYPE
      Ok(Some(_)) => self.wrong_type_reply().encode_resp(output, resp3),
      // 真读缺失：登记表 NOTFOUND 族应答（对齐 C# res==NOTFOUND）
      Ok(None) => {
        // 循环前缀外提（快路径同款）：本执行域 (ns, db) 会话域内寻址登记表
        let prefix = storage.batch.session_prefix();
        let prefix = prefix.as_slice();
        let reply = match cmd {
          RespCommand::Vsim => self.network_vsim(prefix, args, resp3).await,
          RespCommand::Vemb => self.network_vemb(prefix, args).await,
          RespCommand::Vcard => self.network_vcard(prefix, args).await,
          RespCommand::Vdim => self.network_vdim(prefix, args).await,
          RespCommand::Vgetattr => self.network_vgetattr(prefix, args).await,
          RespCommand::Vinfo => self.network_vinfo(prefix, args).await,
          RespCommand::Vismember => self.network_vismember(prefix, args, resp3).await,
          RespCommand::Vlinks => self.network_vlinks(prefix, args).await,
          RespCommand::Vrandmember => self.network_vrandmember(prefix, args).await,
          _ => unreachable!("is_vector_read_command 钉住慢路径分派集"),
        };
        reply.encode_resp(output, resp3);
      }
      // 存储错误：慢路径统一错误帧
      Err(_) => write_error_raw(output, RESP_ERR_SLOW_PATH_STORAGE),
    }
  }

  /// 向量写族（VADD / VSETATTR）冷态降级裁决与应答（慢路径承接；与
  /// [`Self::network_vector_read_slow`] 同型，对标 C# 各 NetworkV* 的
  /// res 三态分派——Read_MainStore 真读落盘裁决后 WRONGTYPE 就地应答）
  ///
  /// 快路径守卫（[`Self::vector_key_guard`]）报 Allow 后命令体挂起
  /// SlowWait（同步段 inline_wait 收割移除，插入/属性写链为 compio 存储
  /// 异步操作），由 exec_slow 转投本面：执行前以异步真读复判键域——挂起
  /// 窗口内键可被并发 SET 写入 wkv 值域，真读存活非向量记录即回 WRONGTYPE，
  /// 杜绝挂起拉长竞态窗口后落双域键（取舍方向同 doc/zh/deviations.md §22）。
  /// 注意真读裁决先于参数解析（原同步形态解析错误优先）——仅挂起期键被
  /// 并发覆写的竞态窗口内二者同现时序可见，字节面各分支同源不变。
  ///
  /// `args` 为参数快照（VADD 尾参携 2 字节 LE 库级定槽，对标 INFO 库数上限 /
  /// MSETNX 续跑标记的快照尾参先例；exec_slow 无会话可达面，槽位随调度点
  /// 快照带入），末尾定槽尾参剥除后交 [`Self::network_vadd`]。
  pub async fn network_vector_write_slow<D: Device>(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    args: &[&[u8]],
    resp_version: u8,
    output: &mut Vec<u8>,
  ) {
    let key = args.first().copied().unwrap_or(&[]);
    let resp3 = is_resp3(resp_version);
    match storage.probe_alive_domain(key).await {
      // 真读存活：非向量记录（挂起期并发覆写），对齐 C# res==WRONGTYPE
      Ok(Some(_)) => self.wrong_type_reply().encode_resp(output, resp3),
      // 真读缺失：值域双缺维持 Allow 裁决，写族命令体闭环
      Ok(None) => {
        // 循环前缀外提（快/只读慢路径同款）：本执行域 (ns, db) 会话域内
        // 寻址登记表
        let prefix = storage.batch.session_prefix();
        let prefix = prefix.as_slice();
        let reply = match cmd {
          RespCommand::Vadd => {
            // VADD 登记创建位过期残留清退·异步承接（案二 zcode-r151c-exwatch，
            // 快臂见 garnet_api exec Allow 分支）：同源 helper 重跑（exec 段
            // 已闭环形回 Pass 零写，幂等）；失闩/磁盘候选/页翻转等不可闭环
            // 形在**建籍态**（登记表 miss）经统一 delete 级联窗内闭环——残留
            // TTL 与判死值记录物理清退、bump 与 VADD 自身合并同命令内，恒先
            // 于任何后续 WATCH 登记；已建籍键按 §75（登记表无过期刻度）无
            // 残留复合态，降级仅系桶闩交叠，零副作用放行，绝不触缺席清退
            // 钩子（delete_vector_set 整集删除，误触即毁集）
            if !purge_expired_residue_sync(&storage.batch, key).unwrap_or(false)
              && self.manager.read_stored_index(prefix, key).is_none()
              && storage.batch.delete(key).await.is_err()
            {
              write_error_raw(output, RESP_ERR_SLOW_PATH_STORAGE);
              return;
            }
            // 尾参剥除定槽（兜底 0 = 根域单库语义，分派集钉住 VADD 快照
            // 至少 4 参 + 尾槽，正常路径恒走 and_then 命中）
            let slot = args
              .last()
              .and_then(|a| <[u8; 2]>::try_from(*a).ok())
              .map(u16::from_le_bytes)
              .unwrap_or(0);
            self
              .network_vadd(prefix, &args[..args.len() - 1], slot, resp3)
              .await
          }
          RespCommand::Vsetattr => self.network_vsetattr(prefix, args, resp3).await,
          // VREM 挂起化（元素删除为存储异步回调，登记写透 async 化后快
          // 路径不再承接）归本臂真读复判后闭环
          RespCommand::Vrem => self.network_vrem(prefix, args).await,
          _ => unreachable!("is_vector_set_command 钉住写族慢路径分派集"),
        };
        reply.encode_resp(output, resp3);
      }
      // 存储错误：慢路径统一错误帧
      Err(_) => write_error_raw(output, RESP_ERR_SLOW_PATH_STORAGE),
    }
  }

  /// Vector Set 预览未启用的统一拒绝。
  fn abort_disabled(&self) -> VectorReply {
    VectorReply::err(ERR_VECTOR_SET_DISABLED)
  }

  /// 命令入口骨架守卫：预览未启用 → 统一拒绝；参数个数不在 `len` 区间 →
  /// `bad` 文案。返回 Some(应答) 即入口拒止（各命令合法参数个数区间逐一对
  /// 齐 C# 各 NetworkV*）。
  #[inline]
  fn entry(
    &self,
    args: &[&[u8]],
    len: impl RangeBounds<usize>,
    bad: &'static [u8],
  ) -> Option<VectorReply> {
    if !self.manager.is_enabled() {
      return Some(self.abort_disabled());
    }
    (!len.contains(&args.len())).then(|| VectorReply::err(bad))
  }

  /// OK 后合成写注入 AOF 的统一失败口径（对标 C# 各 VectorStoreOps 的
  /// ReplicateVectorSet* 失败臂：记日志并回服务错误帧）。
  #[inline]
  fn aof_failed<E: fmt::Display>(&self, op: &str, res: Result<(), E>) -> Option<VectorReply> {
    res
      .map_err(|e| {
        log::error!("network_{op}: 向量 AOF 合成写入队失败: {e}");
        VectorReply::err(ERR_VECTOR_SERVICE_RESPONSE)
      })
      .err()
  }

  /// 向量取参骨架（VADD/VSIM 共用）：格式关键字命中 [`VALUE_KINDS`] 后按
  /// 形态取参——原始字节（FP32/XU8/XI8）零拷贝借接收缓冲参数、VALUES 文本
  /// 浮点落堆；非法文案一律由 `errs` 供给，逐字保持各命令原文案。
  /// 维度上限以 usize 比对（上限远小于 isize::MAX，与原 i32/usize 两种
  /// 写法在全部可达输入上同判）。
  fn vector_operand<'a>(
    &self,
    cur: &mut Cur<'a>,
    errs: &OperandErrs,
  ) -> Result<Operand<'a>, VectorReply> {
    let Some(kind) = lookup(VALUE_KINDS, cur.peek()) else {
      return Err(VectorReply::err(errs.kind));
    };
    if kind == ValueKind::Values {
      let n = strict_i32(cur.value(errs.missing)?)
        .filter(|n| *n > 0)
        .ok_or_else(|| VectorReply::err(errs.count))?;
      if n as usize > MAX_VECTOR_DIMENSIONS as usize {
        return Err(self.abort_too_many_dimensions());
      }
      if cur.ix + n as usize > cur.args.len() {
        return Err(VectorReply::err(errs.missing));
      }
      let mut floats = Vec::with_capacity(n as usize * 4);
      for _ in 0..n {
        let f =
          strict_f32(cur.next(errs.float)?, true).ok_or_else(|| VectorReply::err(errs.float))?;
        floats.extend_from_slice(&f.to_le_bytes());
      }
      return Ok(Operand {
        value_type: VectorValueType::FP32,
        values: Cow::Owned(floats),
        dims: n as usize,
        element: None,
      });
    }
    // 原始字节形态：每维字节数由格式定（FP32 另校验 4 字节对齐）
    let value_type = match kind {
      ValueKind::Fp32 => VectorValueType::FP32,
      ValueKind::Xu8 => VectorValueType::XU8,
      ValueKind::Xi8 => VectorValueType::XI8,
      ValueKind::Values => VectorValueType::Invalid,
    };
    let bytes = cur.value(errs.missing)?;
    let step = dim_bytes(value_type);
    if step > 1 && bytes.len() % step != 0 {
      return Err(VectorReply::err(errs.align));
    }
    let dims = bytes.len() / step;
    if dims > MAX_VECTOR_DIMENSIONS as usize {
      return Err(self.abort_too_many_dimensions());
    }
    Ok(Operand {
      value_type,
      values: Cow::Borrowed(bytes),
      dims,
      element: None,
    })
  }

  // ======================== VADD ========================

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD
  ///
  /// `VADD key [REDUCE dim] (FP32 | XU8 | XI8 | VALUES num) vector element
  ///   [CAS] [NOQUANT | Q8 | BIN | XNOQUANT_U8 | XPREQ8 | XNOQUANT_I8 | XBIN_I8 | XBIN_U8]
  ///   [EF build-exploration-factor] [SETATTR attributes] [M numlinks]
  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD
  ///
  /// 插入链为存储异步操作（compio 写盘跨 await），本函数 async 化闭环
  ///（对标 cluster 链 pending_slow 挂起先例与 C# 同步栈 NetworkVADD 的
  /// rust 快慢分臂对偶）：生产 RESP 臂经 [`Self::network_vector_write_slow`]
  /// 挂起驱动，直调方（测试 / 复制回放）自持运行时 await。
  ///
  /// 库级定槽（doc/zh/db.md 4.1）：`slot` 为调用会话库级槽位
  ///（`RespServerSession::active_db_slot`），索引登记的槽位随会话所属库，
  /// 键内容不参与定槽；`resp3` 选择成功/重复应答的布尔或整数形态。
  pub async fn network_vadd(
    &self,
    prefix: &[u8],
    args: &[&[u8]],
    slot: u16,
    resp3: bool,
  ) -> VectorReply {
    match self.parse_vadd(args, slot) {
      Ok(plan) => self.network_vadd_slow(prefix, plan, resp3).await,
      Err(reply) => reply,
    }
  }

  /// VADD 参数解析段（C# NetworkVADD 选项循环的纯解析投影）：零存储触达，
  /// 校验/默认值合成/维度推导产出 [`VaddPlan`]，错误应答就地返回。
  ///
  /// 与执行段 [`Self::network_vadd_slow`] 的拆分线对齐 C# 原序：C# 在调
  /// storageApi 前完成全部参数校验（X 系量化互斥判定注释「before calling
  /// storageApi」），拆分后解析期错误不经挂起面，语义与字节面同源。
  fn parse_vadd<'a>(&self, args: &'a [&'a [u8]], slot: u16) -> Result<VaddPlan<'a>, VectorReply> {
    if let Some(reply) = self.entry(args, 4.., WNA_VADD) {
      return Err(reply);
    }

    let key = args[0];
    let mut cur = Cur::new(args, 1);

    // REDUCE dim（C# TryGetInt 严格 i32，溢出同非法；缺失/非法/非正统一报
    // REDUCE 文案；前导零拒收系 rust 严格收口，见 doc/zh/deviations.md §32）
    let mut reduce_dims = 0u32;
    if cur.at(b"REDUCE") {
      reduce_dims = cur.i32_value(
        ERR_REDUCE_MUST_BE_POSITIVE,
        |v| v > 0,
        ERR_REDUCE_MUST_BE_POSITIVE,
      )? as u32;
    }

    // 向量格式分派：FP32 / VALUES num / XU8|XB8 / XI8（ELE 归入非法格式文案）
    let vector = self.vector_operand(&mut cur, &VADD_OPERAND)?;
    let value_type = vector.value_type;
    let values = vector.values;
    if usize::try_from(reduce_dims).unwrap_or(vector.dims + 1) > vector.dims {
      return Err(VectorReply::err(ERR_REDUCE_EXCEEDS_DIMS));
    }

    // 元素键
    let element = cur.next(WNA_VADD)?;

    // 选项循环（C#：元素后顺序未指定，逐一识别）
    let mut cas_seen = false;
    let mut quant: Option<VectorQuantType> = None;
    let mut build_ef: Option<i32> = None;
    let mut attributes: Option<&[u8]> = None;
    let mut num_links: Option<i32> = None;
    let mut distance_metric: Option<VectorDistanceMetricType> = None;

    while cur.more() {
      // REDUCE 在元素之后无论何种写法均非法
      if cur.at(b"REDUCE") {
        return Err(VectorReply::err(ERR_INVALID_OPTION_AFTER_ELEMENT));
      }
      // 量化器选项（含 XPREQ8 别名）单表识别，表序即原链式识别序
      if let Some(quant_type) = lookup(QUANT_OPTS, cur.peek()) {
        if quant.is_some() {
          return Err(VectorReply::err(ERR_QUANT_SPECIFIED_TWICE));
        }
        quant = Some(quant_type);
        cur.skip();
      } else if cur.at(b"CAS") {
        // CAS 仅识别不处理
        dup_flag!(cur, &mut cas_seen, "CAS");
      } else if cur.at(b"EF") {
        if build_ef.is_some() {
          return Err(err_dup!("EF"));
        }
        build_ef = Some(cur.i32_value(
          ERR_INVALID_OPTION_AFTER_ELEMENT,
          |v| v > 0 && v <= MAX_EXPLORATION_FACTOR as i32,
          ERR_EF_RANGE,
        )?);
      } else if cur.at(b"SETATTR") {
        if attributes.is_some() {
          return Err(err_dup!("SETATTR"));
        }
        attributes = Some(cur.value(ERR_INVALID_OPTION_AFTER_ELEMENT)?);
      } else if cur.at(b"M") {
        if num_links.is_some() {
          return Err(err_dup!("M"));
        }
        num_links = Some(cur.i32_value(
          ERR_INVALID_OPTION_AFTER_ELEMENT,
          |v| (MIN_M..=MAX_M).contains(&v),
          ERR_M_RANGE,
        )?);
      } else if cur.at(b"XDISTANCE_METRIC") {
        if distance_metric.is_some() {
          return Err(err_dup!("XDISTANCE_METRIC"));
        }
        let metric = cur.value(ERR_INVALID_OPTION_AFTER_ELEMENT)?;
        distance_metric = Some(
          lookup(METRIC_OPTS, metric)
            .ok_or_else(|| VectorReply::err(ERR_INVALID_DISTANCE_METRIC))?,
        );
      } else {
        return Err(VectorReply::err(ERR_INVALID_OPTION_AFTER_ELEMENT));
      }
    }

    if key.is_empty() {
      return Err(VectorReply::err(ERR_EMPTY_VECTOR_SET_KEY));
    }

    // 默认值（对齐 C#：Q8 / 200 / 16 / L2）
    let quant = quant.unwrap_or(VectorQuantType::Q8);
    let build_ef = build_ef.unwrap_or(DEFAULT_VADD_BUILD_EF) as u32;
    let num_links = num_links.unwrap_or(DEFAULT_VADD_NUM_LINKS) as u32;
    let distance_metric = distance_metric.unwrap_or(VectorDistanceMetricType::L2);

    // X 系量化器与 REDUCE 互斥：C# 在调storageApi 前以此判 BadParams（自定义
    // 文案为空 → 回落 quantization mismatch 文案）
    if is_x_quant(quant) && reduce_dims != 0 {
      return Err(VectorReply::err(ERR_QUANT_MISMATCH));
    }

    Ok(VaddPlan {
      key,
      element,
      value_type,
      values,
      attributes: attributes.unwrap_or(b""),
      reduce_dims,
      quant,
      build_ef,
      num_links,
      distance_metric,
      slot,
      // 向量维度（供 manager 校验；上限校验已收口于取参骨架）
      dims: vector.dims as u32,
    })
  }

  /// VADD 执行段（C# ReadOrCreateVectorIndex → TryAdd → ReplicateVectorSetAdd
  /// 锁链的投影）。
  ///
  /// 共享索引锁在 [`Self::parse_vadd`] 之后的 `read_or_create_vector_index`
  /// 取得，覆盖 `try_add` 全程（manager 契约「假定索引已锁定」，防并发
  /// DEL/UNLINK/FLUSHDB 摘除 context）：guard 随本 async 栈帧跨 await 存活。
  /// 与线程槽守卫「绝不跨 await」纪律分属两轴——每键数据锁的跨 await 由
  /// 慢路径 [`crate::resp::slow_path::SlowFuture`] 的 Send 承诺承担（compio
  /// thread-per-core 下 poll 恒在属主任务线程，guard 永不跨线程 move/drop；
  /// 对标 C# ReadOrCreateVectorIndex 返回锁对象持续至 TryAdd 完成的同一
  /// 语义），ActiveVectorSessionGuard 则由慢路径 SlowPollSessionBound 包装
  /// 在每次 poll 边界重绑——同步段收割 inline_wait 移除后，本函数不再内联
  /// 重入 tick。
  ///
  /// 守卫自取得起存活至本函数返回（同 C# VectorStoreOps.cs:192 using 罩
  /// TryAdd 与 OK 后 ReplicateVectorSetAdd 全程）：并发 DEL/UNLINK/FLUSHDB
  /// 的排他删除锁（ReadForDeleteVectorIndex）被排挡至写体与 AOF 注入完成，
  /// 杜绝 service.insert miss 折 Duplicate 伪应答与 Arc 保活孤儿写。
  async fn network_vadd_slow(&self, prefix: &[u8], plan: VaddPlan<'_>, resp3: bool) -> VectorReply {
    let VaddPlan {
      key,
      element,
      value_type,
      values,
      attributes,
      reduce_dims,
      quant,
      build_ef,
      num_links,
      distance_metric,
      slot,
      dims,
    } = plan;

    // 读或创建索引记录（缺失或需重建时按选项建原生索引，对齐 C# ReadOrCreateVectorIndex）
    let params = CreateIndexParams {
      hash_slot: slot,
      dims,
      reduce_dims,
      quant,
      build_exploration_factor: build_ef,
      num_links,
      distance_metric,
    };
    let (index, _lock) = match self
      .manager
      .read_or_create_vector_index(prefix, key, Some(&params))
      .await
    {
      Ok(acquired) => acquired,
      Err(_) => return VectorReply::err(ERR_MAX_ALLOCATIONS_EXCEEDED),
    };

    // 经 manager 执行插入（重复/参数不匹配校验在 try_add 内）
    let stored = index.to_bytes();
    let add_args = VectorAddArgs {
      element,
      value_type,
      values: values.as_ref(),
      attributes,
      reduce_dims,
      quant_type: quant,
      num_links,
      distance_metric,
    };
    match self.manager.try_add(prefix, key, &stored, &add_args).await {
      Ok(VectorManagerResult::OK) => {
        // 成功后合成写注入 AOF（对标 C# VectorStoreOps.VectorSetAdd 的
        // OK-后 ReplicateVectorSetAdd；重复添加幂等跳过，不入日志）
        if let Some(reply) = self.aof_failed(
          "vadd",
          self
            .manager
            .replicate_vector_set_add(prefix, key, dims, build_ef, &add_args),
        ) {
          return reply;
        }
        // 对齐 C#：成功 → RESP3 布尔真 / RESP2 整数 1，重复 → 布尔假 / 整数 0
        bool_reply(true, resp3)
      }
      Ok(VectorManagerResult::Duplicate) => bool_reply(false, resp3),
      Ok(other) => VectorReply::err(other.error_msg()),
      Err(e) => VectorReply::Error(e.message.into()),
    }
  }

  /// 维度上限错误。
  fn abort_too_many_dimensions(&self) -> VectorReply {
    VectorReply::Error(
      format!("ERR vector exceeds maximum of {MAX_VECTOR_DIMENSIONS} dimensions")
        .into_bytes()
        .into(),
    )
  }

  // ======================== VSIM ========================

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSIM
  ///
  /// `VSIM key (ELE | FP32 | XU8 | XI8 | VALUES num) (vector | element)
  ///   [WITHSCORES] [WITHATTRIBS] [COUNT num] [EPSILON delta] [EF factor]
  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSIM
  ///
  /// `resp3` 选择应答协议版本。
  pub async fn network_vsim(&self, prefix: &[u8], args: &[&[u8]], resp3: bool) -> VectorReply {
    let VsimPlan {
      key,
      element,
      value_type,
      values,
      search,
      with_scores,
    } = match self.parse_vsim(args) {
      Ok(plan) => plan,
      Err(reply) => return reply,
    };

    // 键不存在：对齐 C# NOTFOUND → 空数组（非错误）。命中走读+重建锁协议
    //（C# ReadVectorIndex + RecreateIndex）：登记记录 ptr=0 时在独占锁内
    // 重载原生索引——恢复回建后首次检索即由此装载，非裸读登记表。
    // 重建臂含登记写透 `.await`（真异步，无内联收割），读路径 async 化
    let (stored, _index_guard) = match self.manager.read_vector_index(prefix, key).await {
      (Some(index), guard) => (index.to_bytes(), guard),
      (None, _) => return VectorReply::Array(Vec::new()),
    };

    let result = match element {
      Some(elem) => {
        self
          .manager
          .element_similarity(&stored, elem, &search)
          .await
      }
      None => {
        self
          .manager
          .value_similarity(&stored, value_type, values.as_ref(), &search)
          .await
      }
    };

    let output = match result {
      Ok(out) => out,
      Err(e) => return VectorReply::Error(e.message.into()),
    };

    // 拆包命中
    let ids: Vec<&[u8]> = unpack_length_prefixed(&output.output_ids);
    let attrs: Option<Vec<&[u8]>> = search
      .include_attributes
      .then(|| unpack_length_prefixed(&output.output_attributes));

    if resp3 {
      RespServerSessionVectors::write_resp3_result(
        search.count,
        &ids,
        &output.output_distances,
        &output.filter_bitmap,
        attrs.as_deref(),
        with_scores,
      )
    } else {
      RespServerSessionVectors::write_resp2_result(
        search.count,
        &ids,
        &output.output_distances,
        &output.filter_bitmap,
        attrs.as_deref(),
        with_scores,
      )
    }
  }

  /// VSIM 参数解析段（C# NetworkVSIM 校验完毕、调 storageApi 前的纯解析
  /// 投影）：查询向量取参与选项循环骨架复用 VADD 同款〔
  /// [`Self::vector_operand`]／[`Cur`]〕，各档非法文案逐字保持原文案。
  fn parse_vsim<'a>(&self, args: &'a [&'a [u8]]) -> Result<VsimPlan<'a>, VectorReply> {
    if let Some(reply) = self.entry(args, 3.., WNA_VSIM) {
      return Err(reply);
    }

    let key = args[0];
    let mut cur = Cur::new(args, 1);

    // 查询向量（ELE 形态以既有元素为中心；C# 对缺失的元素参数不做显式
    // 校验，空切片语义）
    let vector = if cur.at(b"ELE") {
      cur.skip();
      Operand {
        value_type: VectorValueType::Invalid,
        values: Cow::Borrowed(&[]),
        dims: 0,
        element: Some(cur.next_or_empty()),
      }
    } else {
      self.vector_operand(&mut cur, &VSIM_OPERAND)?
    };

    // 选项（默认值对齐 C#：count=10 / delta=2 / EF=100 / FILTER-EF=16）
    let mut with_scores = false;
    let mut with_attribs = false;
    let mut count: Option<i32> = None;
    let mut epsilon: Option<f32> = None;
    let mut ef: Option<i32> = None;
    let mut filter: Option<&[u8]> = None;
    let mut filter_ef: Option<i32> = None;
    // 对标 C# 仅做选项语法识别，当前执行流未启用真值比较与单线程模式
    let mut truth_seen = false;
    let mut no_thread_seen = false;

    while cur.more() {
      if cur.at(cs::WITHSCORES) {
        dup_flag!(cur, &mut with_scores, "WITHSCORES");
      } else if cur.at(b"WITHATTRIBS") {
        dup_flag!(cur, &mut with_attribs, "WITHATTRIBS");
      } else if cur.at(cs::COUNT) {
        if count.is_some() {
          return Err(err_dup!("COUNT"));
        }
        count = Some(cur.i32_value(
          WNA_VSIM,
          |v| v >= 0 && v <= MAX_RETRIEVE_COUNT as i32,
          ERR_COUNT_RANGE,
        )?);
      } else if cur.at(b"EPSILON") {
        if epsilon.is_some() {
          return Err(err_dup!("EPSILON"));
        }
        epsilon = Some(cur.f32_value(WNA_VSIM, |v| v > 0.0, ERR_EPSILON_MUST_BE_POSITIVE)?);
      } else if cur.at(b"EF") {
        if ef.is_some() {
          return Err(err_dup!("EF"));
        }
        ef = Some(cur.i32_value(
          WNA_VSIM,
          |v| v > 0 && v <= MAX_EXPLORATION_FACTOR as i32,
          ERR_EF_RANGE,
        )?);
      } else if cur.at(b"FILTER") {
        if filter.is_some() {
          return Err(err_dup!("FILTER"));
        }
        filter = Some(cur.value(WNA_VSIM)?);
      } else if cur.at(b"FILTER-EF") {
        if filter_ef.is_some() {
          return Err(err_dup!("FILTER-EF"));
        }
        filter_ef = Some(cur.i32_value(
          WNA_VSIM,
          |v| v >= 4 && v <= MAX_FILTERING_SCALE_FACTOR as i32,
          ERR_FILTER_EF_RANGE,
        )?);
      } else if cur.at(b"TRUTH") {
        // TODO 语义与 C# 一致：仅识别
        dup_flag!(cur, &mut truth_seen, "TRUTH");
      } else if cur.at(b"NOTHREAD") {
        // C# 忽略 NOTHREAD
        dup_flag!(cur, &mut no_thread_seen, "NOTHREAD");
      } else {
        return Err(VectorReply::err(ERR_UNKNOWN_OPTION));
      }
    }

    // EPSILON / FILTER-EF 参与检索（对齐 C# 传参语义：maxFilteringEffort ??= 16
    // 放大过滤候选队列；delta 截断最大距离 —— 缺省对齐 Garnet 2.0f32）
    Ok(VsimPlan {
      key,
      element: vector.element,
      value_type: vector.value_type,
      values: vector.values,
      search: VectorSearchOptions {
        // 结果数/EF 均经范围校验（>=0），max(0) 为原写法保留
        count: count.unwrap_or(DEFAULT_VSIM_COUNT).max(0) as usize,
        search_exploration_factor: ef.unwrap_or(DEFAULT_VSIM_EF).max(0) as usize,
        filter: filter.unwrap_or(b""),
        max_filtering_effort: filter_ef.unwrap_or(DEFAULT_VSIM_FILTER_EF).max(0) as usize,
        delta: epsilon.unwrap_or(DEFAULT_VSIM_EPSILON),
        include_attributes: with_attribs,
      },
      with_scores,
    })
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVEMB
  ///
  /// `VEMB key element [RAW]` → 嵌入向量数组；RAW 时输出
  /// [量化器名, 原始量化字节, 范数, (Q8 量化范围)]。
  pub async fn network_vemb(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 2..=3, "VEMB");
    // RAW 形态：第三参须为 RAW 关键字，其后按 raw 分支取原始量化数据
    if args.len() == 3 && !equals_ignore_case(args[2], b"RAW") {
      return VectorReply::err(ERR_VEMB_UNEXPECTED_OPTION);
    }
    let raw = args.len() == 3;

    // C#：键/元素缺失统一写空数组。命中走读+重建锁协议（恢复回建后
    // 首次嵌入读取由此装载原生索引，同 VSIM 口径）
    let (stored, _index_guard) = match self.manager.read_vector_index(prefix, args[0]).await {
      (Some(index), guard) => (index.to_bytes(), guard),
      (None, _) => return VectorReply::Array(Vec::new()),
    };

    if raw {
      return match self.manager.try_get_raw_embedding(&stored, args[1]).await {
        Some((bytes, quant, norm, range)) => {
          // 量化器名映射：BIN/XBIN_* → bin；Q8/XNOQUANT_* → q8；NOQUANT → fp32
          let quant_name: &[u8] = match quant {
            VectorQuantType::Bin | VectorQuantType::XbinI8 | VectorQuantType::XbinU8 => b"bin",
            VectorQuantType::Q8 | VectorQuantType::XnoQuantU8 | VectorQuantType::XnoQuantI8 => {
              b"q8"
            }
            VectorQuantType::NoQuant => b"fp32",
            VectorQuantType::Invalid => b"fp32",
          };
          let mut items = vec![
            VectorReply::Simple(quant_name),
            VectorReply::Bulk(Some(bytes.into())),
            VectorReply::Double(norm),
          ];
          // 仅 Q8 追加量化范围
          if quant == VectorQuantType::Q8 {
            items.push(VectorReply::Double(range.unwrap_or(0.0)));
          }
          VectorReply::Array(items)
        }
        None => VectorReply::Array(Vec::new()),
      };
    }

    match self.manager.try_get_embedding(&stored, args[1]).await {
      Some(embedding) => VectorReply::Array(
        embedding
          .into_iter()
          .map(|v| VectorReply::Double(f64::from(v)))
          .collect(),
      ),
      None => VectorReply::Array(Vec::new()),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVCARD
  ///
  /// C# 对位 VectorStoreOps.cs:VectorSetCardinality 锁点 :459——
  /// `using (ReadVectorIndex)` 共享读锁全程罩住基数读取体；rust 同款：
  /// 守卫随本 async 栈帧跨 card 读存活，ptr=0 冷记录经独占重建后降级共享
  /// 命中（懒回建窗静默零答结构性消失）。
  pub async fn network_vcard(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 1..=1, "VCARD");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      return VectorReply::Integer(0);
    };
    VectorReply::Integer(self.manager.service.card(index.context) as i64)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVDIM
  ///
  /// C# 对位 VectorStoreOps.cs:VectorSetDimensions 锁点 :394（using 锁全程
  /// 持读优化共享锁），rust 同款锁定读面（防删锁与族形欠账一并收口）。
  pub async fn network_vdim(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 1..=1, "VDIM");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      // 对齐 C# NOTFOUND → "ERR Key not found"
      return VectorReply::err(ERR_KEY_NOT_FOUND);
    };
    VectorReply::Integer(i64::from(index.dimensions))
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVGETATTR
  ///
  /// C# 对位 VectorStoreOps.cs:VectorSetGetAttribute 锁点 :565（using 锁
  /// 全程罩住属性读取体），rust 同款：守卫随本 async 栈帧跨属性读 await
  /// 存活。
  pub async fn network_vgetattr(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 2..=2, "VGETATTR");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      // 对齐 C# NOTFOUND → null
      return VectorReply::Bulk(None);
    };
    match self
      .manager
      .fetch_single_vector_element_attributes(&index.to_bytes(), args[1])
      .await
    {
      Some(attr) => VectorReply::Bulk(Some(attr.into())),
      None => VectorReply::Bulk(None),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVINFO
  ///
  /// `VINFO key` → 14 项元信息（quant-type/distance-metric/input-vector-dimensions/
  /// reduced-dimensions/build-exploration-factor/num-links/size）。
  ///
  /// C# 对位 VectorStoreOps.cs:VectorSetInfo 锁点 :428（using 锁全程罩住
  /// 元信息与 size 读取体）；rust 同款：size 臂走 service.card 需原生索引
  /// 在位，锁定读面使 ptr=0 冷记录先重建后应答（懒回建窗 size=0 静默零答
  /// 结构性消失）。
  pub async fn network_vinfo(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 1..=1, "VINFO");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      // 对齐 C# NOTFOUND → null 数组
      return VectorReply::NullArray;
    };
    // 对齐 C# 小写枚举名（Invalid 在 C# 侧抛异常；此处防御性报错）
    let quant: &[u8] = match index.quant_type {
      VectorQuantType::NoQuant => b"f32",
      VectorQuantType::Bin => b"bin",
      VectorQuantType::Q8 => b"q8",
      VectorQuantType::XnoQuantU8 => b"xnoquant_u8",
      VectorQuantType::XnoQuantI8 => b"xnoquant_i8",
      VectorQuantType::XbinI8 => b"xbin_i8",
      VectorQuantType::XbinU8 => b"xbin_u8",
      VectorQuantType::Invalid => return VectorReply::err(b"ERR Invalid VectorQuantType"),
    };
    let metric: &[u8] = match index.distance_metric {
      VectorDistanceMetricType::Cosine => b"cosine",
      VectorDistanceMetricType::InnerProduct => b"inner-product",
      VectorDistanceMetricType::L2 => b"l2",
      VectorDistanceMetricType::XCosineNormalized => b"cosine-normalized",
    };
    let bulk_int = |v: u32| VectorReply::BulkInt(i64::from(v));
    VectorReply::Array(vec![
      VectorReply::Simple(b"quant-type"),
      VectorReply::Simple(quant),
      VectorReply::Simple(b"distance-metric"),
      VectorReply::Simple(metric),
      VectorReply::Simple(b"input-vector-dimensions"),
      bulk_int(index.dimensions),
      VectorReply::Simple(b"reduced-dimensions"),
      bulk_int(index.reduce_dims),
      VectorReply::Simple(b"build-exploration-factor"),
      bulk_int(index.build_exploration_factor),
      VectorReply::Simple(b"num-links"),
      bulk_int(index.num_links),
      VectorReply::Simple(b"size"),
      // C# :1616 WriteInt64AsBulkString(size)；基数为 u64 计数，转 i64 与本文件
      // VCARD（:980 `card(..) as i64`）同口径
      VectorReply::BulkInt(self.manager.service.card(index.context) as i64),
    ])
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVISMEMBER
  ///
  /// `VISMEMBER key element`（RESP3 以布尔应答）。C# 对位
  /// VectorStoreOps.cs:VectorSetIsMember 锁点 :484——守卫随本 async 栈帧
  /// 跨成员读 await 存活。
  pub async fn network_vismember(&self, prefix: &[u8], args: &[&[u8]], resp3: bool) -> VectorReply {
    wna_entry!(self, args, 2..=2, "VISMEMBER");
    let member = match self.manager.read_vector_index(prefix, args[0]).await {
      (Some(index), _guard) => self.manager.is_member(&index.to_bytes(), args[1]).await,
      (None, _) => false,
    };
    bool_reply(member, resp3)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVLINKS
  ///
  /// `VLINKS key element [WITHSCORES]`。C# 侧输出为 TODO（恒 +OK）；
  /// 此处返回层 0 邻接的实际元素（超集语义），键/元素缺失写 null。
  /// C# 对位 VectorStoreOps.cs:VectorSetLinks 锁点 :512——守卫随本 async
  /// 栈帧跨邻接读 await 存活。
  pub async fn network_vlinks(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 2..=3, "VLINKS");
    if args.len() == 3 && !equals_ignore_case(args[2], cs::WITHSCORES) {
      return VectorReply::err(ERR_VLINKS_UNEXPECTED_OPTION);
    }
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      return VectorReply::Bulk(None);
    };
    match self.manager.service.links_of(index.context, args[1]).await {
      Some(links) => VectorReply::Array(
        links
          .into_iter()
          .map(|l| VectorReply::Bulk(Some(l.into())))
          .collect(),
      ),
      None => VectorReply::Bulk(None),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVRANDMEMBER
  ///
  /// C# 侧输出为 TODO（恒 +OK）；此处返回实际取样元素（超集语义）。
  /// C# 对位 VectorStoreOps.cs:VectorSetRandomMembers 锁点 :541——守卫随
  /// 本 async 栈帧跨取样 await 存活。
  pub async fn network_vrandmember(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 1..=2, "VRANDMEMBER");
    let count = match args.get(1) {
      Some(raw) => {
        let Some(v) = strict_i32(raw) else {
          return VectorReply::err(ERR_EXPECTED_INTEGER_COUNT);
        };
        v
      }
      None => 1,
    };
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      // 对齐 C# NOTFOUND：指定 count → 空数组；未指定 → null
      return if args.len() == 2 {
        VectorReply::Array(Vec::new())
      } else {
        VectorReply::Bulk(None)
      };
    };
    let samples = self
      .manager
      .service
      .sample(index.context, count.max(0) as usize)
      .await;
    if count == 1 && args.len() == 1 {
      return match samples.into_iter().next() {
        Some(s) => VectorReply::Bulk(Some(s.into())),
        None => VectorReply::Bulk(None),
      };
    }
    VectorReply::Array(
      samples
        .into_iter()
        .map(|v| VectorReply::Bulk(Some(v.into())))
        .collect(),
    )
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVREM
  ///
  /// C# 对位 VectorStoreOps.cs:VectorSetRemove 锁点 :225，锁内 :227-234
  /// 原文自陈 "After a successful read we remove the vector while holding a
  /// shared lock / That lock prevents deletion, but everything else can
  /// proceed in parallel"——共享守卫全程罩住 TryRemove 写体与 OK 后合成
  /// 复制注入；rust 同款：守卫随本 async 栈帧跨 `try_remove` / AOF 注入
  /// 存活，DEL 独占臂被排挡至写体完成，杜绝「remove 落已弃上下文 + AOF
  /// 注入穿透删除锁」的主从发散竞态（manager 契约：try_remove 假定调用方
  /// 已持共享读守卫）。
  pub async fn network_vrem(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    wna_entry!(self, args, 2..=2, "VREM");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      return VectorReply::Integer(0);
    };
    let removed = self
      .manager
      .try_remove(prefix, args[0], &index.to_bytes(), args[1])
      .await;
    match removed {
      VectorManagerResult::OK => {
        // 成功后合成写注入 AOF（对标 C# VectorStoreOps.VectorSetRemove 的
        // OK-后 ReplicateVectorSetRemove）
        if let Some(reply) = self.aof_failed(
          "vrem",
          self
            .manager
            .replicate_vector_set_remove(prefix, args[0], args[1]),
        ) {
          return reply;
        }
        VectorReply::Integer(1)
      }
      _ => VectorReply::Integer(0),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSETATTR
  ///
  /// `VSETATTR key element attributes`（RESP3 以布尔应答；缺失元素 → 假 / 0，非错误）。
  ///
  /// 属性写为存储异步操作（compio 写盘跨 await），本函数 async 化闭环
  ///（对标 cluster 链 pending_slow 挂起先例）：生产 RESP 臂经
  /// [`Self::network_vector_write_slow`] 挂起驱动，直调方（测试 / 复制回放）
  /// 自持运行时 await。C# 对位 VectorStoreOps.cs:VectorSetSetAttribute
  /// 锁点 :262——`using (ReadVectorIndex)` 共享锁全程罩住 TrySetAttribute
  /// 写体（该臂无自陈注释，锁域即证据）；rust 同款：守卫随本 async 栈帧
  /// 跨 `try_set_attribute` / AOF 注入存活，与 VREM 臂共享读防删排挡归一
  ///（manager 契约：try_set_attribute 假定调用方已持共享读守卫）。
  pub async fn network_vsetattr(&self, prefix: &[u8], args: &[&[u8]], resp3: bool) -> VectorReply {
    wna_entry!(self, args, 3..=3, "VSETATTR");
    let (Some(index), _guard) = self.manager.read_vector_index(prefix, args[0]).await else {
      return bool_reply(false, resp3);
    };
    let ok = self
      .manager
      .try_set_attribute(prefix, args[0], &index.to_bytes(), args[1], args[2])
      .await;
    if ok {
      // 成功后合成写注入 AOF（对标 C# VectorStoreOps.VectorSetSetAttribute
      // 的成功后 ReplicateVectorSetSetAttribute）
      if let Some(reply) = self.aof_failed(
        "vsetattr",
        self
          .manager
          .replicate_vector_set_set_attribute(prefix, args[0], args[1], args[2]),
      ) {
        return reply;
      }
    }
    bool_reply(ok, resp3)
  }
}

impl RespServerSessionVectors {
  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:WriteRESP3Result
  ///
  /// RESP3：无 score/attr 时为普通数组；否则为 map（id → score/attr/[score, attr]），
  /// 过滤未过项剔除，空属性写 NULL。
  pub fn write_resp3_result(
    count: usize,
    ids: &[&[u8]],
    distances: &[f32],
    filter_bitmap: &[u8],
    attributes: Option<&[&[u8]]>,
    with_scores: bool,
  ) -> VectorReply {
    let with_attribs = attributes.is_some();
    // C#：有位图时输出上限 = popcount(bitmap)，否则全部命中；再与 count 取小
    let total_found = ids.len();
    let output_count = output_limit(total_found, filter_bitmap, count);
    // 扁平数组与 map 两形态互斥（`!flat` 即原 `with_scores || with_attribs`），
    // 任一时刻仅一路产出：预容量按该路布尔折算（另一路恒 0 容量、不分配）
    let flat = !with_scores && !with_attribs;
    let mut plain = Vec::with_capacity(usize::from(flat) * output_count);
    let mut map = Vec::with_capacity(usize::from(!flat) * output_count);
    for result_index in passed_indices(total_found, filter_bitmap, output_count) {
      let id = ids[result_index];
      let score = VectorReply::Double(f64::from(
        distances.get(result_index).copied().unwrap_or(0.0),
      ));
      let attr_reply = match attributes
        .and_then(|attrs| attrs.get(result_index))
        .copied()
      {
        // 命中属性源为函数局部检索缓冲，应答需拥有数据（仅此处落堆）
        Some(a) if !a.is_empty() => VectorReply::Bulk(Some(a.to_vec().into())),
        // RESP3：空属性写 NULL
        _ => VectorReply::Bulk(None),
      };
      if flat {
        plain.push(VectorReply::Bulk(Some(id.to_vec().into())));
      } else {
        // 分数与属性齐备时以二元素数组为 map 值（顺序：score → attr）
        let value = if with_scores && with_attribs {
          VectorReply::Array(vec![score, attr_reply])
        } else if with_scores {
          score
        } else {
          attr_reply
        };
        map.push((VectorReply::Bulk(Some(id.to_vec().into())), value));
      }
    }
    if flat {
      VectorReply::Array(plain)
    } else {
      VectorReply::Map(map)
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:WriteRESP2Result
  ///
  /// RESP2：扁平数组（WITHSCORES 时 id/score 成对；WITHATTRIBS 附加属性；
  /// 二者齐备时长度为三倍），空属性写空 bulk 字符串。
  pub fn write_resp2_result(
    count: usize,
    ids: &[&[u8]],
    distances: &[f32],
    filter_bitmap: &[u8],
    attributes: Option<&[&[u8]]>,
    with_scores: bool,
  ) -> VectorReply {
    let with_attribs = attributes.is_some();
    let total_found = ids.len();
    let output_count = output_limit(total_found, filter_bitmap, count);

    let multiplier = 1 + usize::from(with_scores) + usize::from(with_attribs);
    let mut items = Vec::with_capacity(output_count * multiplier);
    for result_index in passed_indices(total_found, filter_bitmap, output_count) {
      let id = ids[result_index];
      // 命中 id/属性源为函数局部检索缓冲，应答需拥有数据（仅此处落堆）
      items.push(VectorReply::Bulk(Some(id.to_vec().into())));
      if with_scores {
        items.push(VectorReply::Double(f64::from(
          distances.get(result_index).copied().unwrap_or(0.0),
        )));
      }
      if with_attribs && let Some(attr) = attributes.and_then(|attrs| attrs.get(result_index)) {
        items.push(VectorReply::Bulk(Some(attr.to_vec().into())));
      }
    }
    VectorReply::Array(items)
  }
}
