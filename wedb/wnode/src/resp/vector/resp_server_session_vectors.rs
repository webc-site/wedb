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

use std::{borrow::Cow, sync::Arc};

use wbase::num::{strict_f32, strict_i32};
use wresp::{
  cmd_strings as cs,
  resp_memory_writer::{Resp2, Resp3, RespProtocol as _, RespWriter},
  wrong_num_args,
};
use wval::KeyTag;
use wvector::{VectorDistanceMetricType, VectorQuantType, VectorValueType, unpack_length_prefixed};

use super::{
  ERR_VECTOR_SET_DISABLED,
  vector_manager::{
    MAX_EXPLORATION_FACTOR, MAX_FILTERING_SCALE_FACTOR, MAX_RETRIEVE_COUNT, MAX_VECTOR_DIMENSIONS,
    VectorAddArgs, VectorManager, VectorManagerResult, VectorSearchOptions,
  },
  vector_manager_index::Index,
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
/// 元素不在集合中（C# VectorSetElementSimilarity MissingElement 出参文案）。
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

/// 命令应答（RESP 数据模型）。
///
/// 静态文案（错误/简单字符串）以 `&'static [u8]` 借用承载，动态载荷
/// （存储读出值、数值格式化、非常量错误）经 `Cow<'static, [u8]>` 落堆：
/// 检索命中 id/属性的源缓冲（`SimilarityOutput`）为函数局部量，应答归还
/// 后即释放，故借用上限为 'static，非静态载荷一律 Owned。
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

/// 大小写不敏感比较（对齐 EqualsUpperCaseSpanIgnoringCase）。
fn eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
  a.eq_ignore_ascii_case(b)
}

use wvector::store::StoreCallbacks;

use crate::{
  resp::vector::vector_store_callbacks::WedbVectorStoreCallbacks,
  storage::session::common::{TagRead, read_tag_sync, ttl_sync::probe_alive_domain},
};

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

  /// 键 → 索引记录读取。
  pub fn read_index(&self, prefix: &[u8], key: &[u8]) -> Option<Index> {
    let stored = self.manager.read_stored_index(prefix, key)?;
    Index::from_bytes(&stored)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:AbortVectorSetWrongType
  ///
  /// VADD/VREM/VSETATTR 写入守卫：键已驻留 wkv（string / 对象信封值域，
  /// 或 RangeIndex 专属的 Meta 元记录域）即回 WRONGTYPE，杜绝与既有非向量
  /// 键并行建向量集产生双域键（C# 判定为主存记录 RecordType 非向量集；
  /// rust 索引记录驻留域内登记表，wkv 域命中即非向量键）。磁盘候选待裁决
  /// 与存储错误按存在保守拒写——误拒可 DEL 后重试，双域键一经写即成幽灵
  ///（KEYS 迁移只迁 string，源端残留幽灵上下文）
  pub fn abort_vector_set_wrong_type<'a, D: wdev::Device>(
    &self,
    key: &[u8],
    store: &wkv::BatchStoreSession<'a, D>,
  ) -> Option<VectorReply> {
    let wrong_type = || Some(VectorReply::err(ERR_VECTOR_SET_WRONG_TYPE));
    match probe_alive_domain(store, key) {
      // 存活命中 / 磁盘候选待裁决 / 存储错误：一律 WRONGTYPE
      Ok(Some(Some(_)) | None) | Err(_) => wrong_type(),
      // 值域双缺：再探 Meta 元记录域（RangeIndex 单树专属物理域）
      Ok(Some(None)) => match read_tag_sync(store, key, KeyTag::Meta, |_| ()) {
        Ok(TagRead::Missing) => None,
        Ok(TagRead::Hit(()) | TagRead::Deferred) | Err(_) => wrong_type(),
      },
    }
  }

  /// Vector Set 预览未启用的统一拒绝。
  fn abort_disabled(&self) -> VectorReply {
    VectorReply::err(ERR_VECTOR_SET_DISABLED)
  }

  // ======================== VADD ========================

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD
  ///
  /// `VADD key [REDUCE dim] (FP32 | XU8 | XI8 | VALUES num) vector element
  ///   [CAS] [NOQUANT | Q8 | BIN | XNOQUANT_U8 | XPREQ8 | XNOQUANT_I8 | XBIN_I8 | XBIN_U8]
  ///   [EF build-exploration-factor] [SETATTR attributes] [M numlinks]
  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD
  #[inline]
  pub fn network_vadd(&self, prefix: &[u8], args: &[&[u8]], slot: u16) -> VectorReply {
    self.network_vadd_impl(prefix, args, slot, false)
  }

  /// VADD 内部实现（`resp3` 选择成功/重复应答的布尔或整数形态）。
  ///
  /// 库级定槽（doc/zh/db.md 4.1）：`slot` 为调用会话库级槽位
  ///（`RespServerSession::active_db_slot`），索引登记的槽位随会话所属库，
  /// 键内容不参与定槽（C# CreateIndexParams.HashSlot = HashSlotUtils.HashSlot
  /// 的键级推导随键级哈希废除）
  pub fn network_vadd_impl(
    &self,
    prefix: &[u8],
    args: &[&[u8]],
    slot: u16,
    resp3: bool,
  ) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() < 4 {
      return VectorReply::err(wrong_num_args!("VADD").as_bytes());
    }

    let key = args[0];
    let mut cur_ix = 1usize;

    // REDUCE dim
    let mut reduce_dims = 0u32;
    if eq_ignore_case(args[cur_ix], b"REDUCE") {
      cur_ix += 1;
      // C# TryGetInt（严格 i32）；缺失/非法/非正统一报 REDUCE 文案
      let v = args.get(cur_ix).and_then(|a| strict_i32(a));
      let Some(v) = v.filter(|v| *v > 0) else {
        return VectorReply::err(ERR_REDUCE_MUST_BE_POSITIVE);
      };
      reduce_dims = v as u32;
      cur_ix += 1;
    }

    // 向量格式分派：FP32 / VALUES num / XU8|XB8 / XI8
    // 查询向量零拷贝：FP32/XU8/XI8 借用接收缓冲参数，仅 VALUES 文本浮点落缓冲
    let Some(&kind) = args.get(cur_ix) else {
      return VectorReply::err(wrong_num_args!("VADD").as_bytes());
    };
    let value_type;
    let values: Cow<'_, [u8]>;
    let vector_dims: i32;
    if eq_ignore_case(kind, b"FP32") {
      cur_ix += 1;
      let Some(as_bytes) = args.get(cur_ix) else {
        return VectorReply::err(wrong_num_args!("VADD").as_bytes());
      };
      if as_bytes.len() % 4 != 0 {
        return VectorReply::err(ERR_INVALID_VECTOR_SPEC);
      }
      vector_dims = (as_bytes.len() / 4) as i32;
      if vector_dims > MAX_VECTOR_DIMENSIONS as i32 {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::FP32;
      values = Cow::Borrowed(*as_bytes);
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"VALUES") {
      cur_ix += 1;
      let Some(count_raw) = args.get(cur_ix) else {
        return VectorReply::err(wrong_num_args!("VADD").as_bytes());
      };
      let Some(n) = strict_i32(count_raw).filter(|n| *n > 0) else {
        return VectorReply::err(ERR_INVALID_VECTOR_SPEC);
      };
      cur_ix += 1;
      if n > MAX_VECTOR_DIMENSIONS as i32 {
        return self.abort_too_many_dimensions();
      }
      if cur_ix + n as usize > args.len() {
        return VectorReply::err(wrong_num_args!("VADD").as_bytes());
      }
      value_type = VectorValueType::FP32;
      let mut floats = Vec::with_capacity(n as usize * 4);
      for _ in 0..n {
        let Some(f) = args.get(cur_ix).and_then(|a| strict_f32(a, true)) else {
          return VectorReply::err(ERR_INVALID_VECTOR_SPEC);
        };
        floats.extend_from_slice(&f.to_le_bytes());
        cur_ix += 1;
      }
      vector_dims = n;
      values = Cow::Owned(floats);
    } else if eq_ignore_case(kind, b"XU8") || eq_ignore_case(kind, b"XB8") {
      // XB8 为向后兼容别名，推荐 XU8
      cur_ix += 1;
      let Some(as_bytes) = args.get(cur_ix) else {
        return VectorReply::err(wrong_num_args!("VADD").as_bytes());
      };
      vector_dims = as_bytes.len() as i32;
      if vector_dims > MAX_VECTOR_DIMENSIONS as i32 {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::XU8;
      values = Cow::Borrowed(*as_bytes);
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"XI8") {
      cur_ix += 1;
      let Some(as_bytes) = args.get(cur_ix) else {
        return VectorReply::err(wrong_num_args!("VADD").as_bytes());
      };
      vector_dims = as_bytes.len() as i32;
      if vector_dims > MAX_VECTOR_DIMENSIONS as i32 {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::XI8;
      values = Cow::Borrowed(*as_bytes);
      cur_ix += 1;
    } else {
      return VectorReply::err(ERR_INVALID_VECTOR_SPEC);
    }

    if reduce_dims as i32 > vector_dims {
      return VectorReply::err(ERR_REDUCE_EXCEEDS_DIMS);
    }

    // 元素键
    let Some(&element) = args.get(cur_ix) else {
      return VectorReply::err(wrong_num_args!("VADD").as_bytes());
    };
    cur_ix += 1;

    // 选项循环（C#：元素后顺序未指定，逐一识别）
    let mut cas_seen = false;
    let mut quant: Option<VectorQuantType> = None;
    let mut build_ef: Option<i32> = None;
    let mut attributes: Option<&[u8]> = None;
    let mut num_links: Option<i32> = None;
    let mut distance_metric: Option<VectorDistanceMetricType> = None;

    while cur_ix < args.len() {
      let opt = args[cur_ix];
      // REDUCE 在元素之后无论何种写法均非法
      if eq_ignore_case(opt, b"REDUCE") {
        return VectorReply::err(ERR_INVALID_OPTION_AFTER_ELEMENT);
      }
      if eq_ignore_case(opt, b"CAS") {
        if cas_seen {
          return err_dup!("CAS");
        }
        // CAS 仅识别不处理
        cas_seen = true;
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"NOQUANT") {
        if quant.is_some() {
          return VectorReply::err(ERR_QUANT_SPECIFIED_TWICE);
        }
        quant = Some(VectorQuantType::NoQuant);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"Q8") {
        if quant.is_some() {
          return VectorReply::err(ERR_QUANT_SPECIFIED_TWICE);
        }
        quant = Some(VectorQuantType::Q8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"BIN") {
        if quant.is_some() {
          return VectorReply::err(ERR_QUANT_SPECIFIED_TWICE);
        }
        quant = Some(VectorQuantType::Bin);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XNOQUANT_U8") || eq_ignore_case(opt, b"XPREQ8") {
        // XPREQ8 为向后兼容别名，推荐 XNOQUANT_U8
        if quant.is_some() {
          return VectorReply::err(ERR_QUANT_SPECIFIED_TWICE);
        }
        quant = Some(VectorQuantType::XnoQuantU8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XNOQUANT_I8") {
        if quant.is_some() {
          return VectorReply::err(ERR_QUANT_SPECIFIED_TWICE);
        }
        quant = Some(VectorQuantType::XnoQuantI8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XBIN_I8") {
        if quant.is_some() {
          return VectorReply::err(ERR_QUANT_SPECIFIED_TWICE);
        }
        quant = Some(VectorQuantType::XbinI8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XBIN_U8") {
        if quant.is_some() {
          return VectorReply::err(ERR_QUANT_SPECIFIED_TWICE);
        }
        quant = Some(VectorQuantType::XbinU8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"EF") {
        if build_ef.is_some() {
          return err_dup!("EF");
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return VectorReply::err(ERR_INVALID_OPTION_AFTER_ELEMENT);
        };
        let Some(v) = strict_i32(v).filter(|v| *v > 0 && *v <= MAX_EXPLORATION_FACTOR as i32)
        else {
          return Self::abort_ef_range();
        };
        build_ef = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"SETATTR") {
        if attributes.is_some() {
          return err_dup!("SETATTR");
        }
        cur_ix += 1;
        let Some(attr) = args.get(cur_ix) else {
          return VectorReply::err(ERR_INVALID_OPTION_AFTER_ELEMENT);
        };
        attributes = Some(attr);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"M") {
        if num_links.is_some() {
          return err_dup!("M");
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return VectorReply::err(ERR_INVALID_OPTION_AFTER_ELEMENT);
        };
        let Some(v) = strict_i32(v).filter(|v| (MIN_M..=MAX_M).contains(v)) else {
          return Self::abort_m_range();
        };
        num_links = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XDISTANCE_METRIC") {
        if distance_metric.is_some() {
          return err_dup!("XDISTANCE_METRIC");
        }
        cur_ix += 1;
        let Some(metric) = args.get(cur_ix) else {
          return VectorReply::err(ERR_INVALID_OPTION_AFTER_ELEMENT);
        };
        distance_metric = Some(if eq_ignore_case(metric, b"L2") {
          VectorDistanceMetricType::L2
        } else if eq_ignore_case(metric, b"COSINE") {
          VectorDistanceMetricType::Cosine
        } else if eq_ignore_case(metric, b"IP") {
          VectorDistanceMetricType::InnerProduct
        } else if eq_ignore_case(metric, b"XCOSINE_NORMALIZED") {
          VectorDistanceMetricType::XCosineNormalized
        } else {
          return VectorReply::err(ERR_INVALID_DISTANCE_METRIC);
        });
        cur_ix += 1;
      } else {
        return VectorReply::err(ERR_INVALID_OPTION_AFTER_ELEMENT);
      }
    }

    if key.is_empty() {
      return VectorReply::err(ERR_EMPTY_VECTOR_SET_KEY);
    }

    // 默认值（对齐 C#：Q8 / 200 / 16 / L2）
    let quant = quant.unwrap_or(VectorQuantType::Q8);
    let build_ef = build_ef.unwrap_or(200) as u32;
    let num_links = num_links.unwrap_or(16) as u32;
    let distance_metric = distance_metric.unwrap_or(VectorDistanceMetricType::L2);

    // X 系量化器与 REDUCE 互斥：C# 在调storageApi 前以此判 BadParams（自定义
    // 文案为空 → 回落 quantization mismatch 文案）
    if matches!(
      quant,
      VectorQuantType::XbinU8
        | VectorQuantType::XbinI8
        | VectorQuantType::XnoQuantU8
        | VectorQuantType::XnoQuantI8
    ) && reduce_dims != 0
    {
      return VectorReply::err(ERR_QUANT_MISMATCH);
    }

    // 向量维度（供 manager 校验）
    let dims = values.len() as u32
      / match value_type {
        VectorValueType::FP32 => 4,
        _ => 1,
      };

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
      attributes: attributes.unwrap_or(b""),
      reduce_dims,
      quant_type: quant,
      num_links,
      distance_metric,
    };
    match self.manager.try_add(prefix, key, &stored, &add_args) {
      Ok(VectorManagerResult::OK) => {
        // 成功后合成写注入 AOF（对标 C# VectorStoreOps.VectorSetAdd 的
        // OK-后 ReplicateVectorSetAdd；重复添加幂等跳过，不入日志）
        self
          .manager
          .replicate_vector_set_add(prefix, key, dims, build_ef, &add_args);
        // 对齐 C#：成功 → RESP3 布尔真 / RESP2 整数 1
        if resp3 {
          VectorReply::Boolean(true)
        } else {
          VectorReply::Integer(1)
        }
      }
      Ok(VectorManagerResult::Duplicate) => {
        // 对齐 C#：重复 → RESP3 布尔假 / RESP2 整数 0
        if resp3 {
          VectorReply::Boolean(false)
        } else {
          VectorReply::Integer(0)
        }
      }
      Ok(VectorManagerResult::BadParams) => {
        VectorReply::err(VectorManagerResult::BadParams.error_msg())
      }
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

  /// EF 范围错误。
  fn abort_ef_range() -> VectorReply {
    VectorReply::err(ERR_EF_RANGE)
  }

  /// M 范围错误。
  fn abort_m_range() -> VectorReply {
    VectorReply::err(ERR_M_RANGE)
  }

  /// FILTER-EF 范围错误。
  fn abort_filter_ef_range() -> VectorReply {
    VectorReply::err(ERR_FILTER_EF_RANGE)
  }

  // ======================== VSIM ========================

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSIM
  ///
  /// `VSIM key (ELE | FP32 | XU8 | XI8 | VALUES num) (vector | element)
  ///   [WITHSCORES] [WITHATTRIBS] [COUNT num] [EPSILON delta] [EF factor]
  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSIM
  #[inline]
  pub fn network_vsim(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    self.network_vsim_impl(prefix, args, false)
  }

  /// VSIM 内部实现（`resp3` 选择应答协议版本）。
  pub fn network_vsim_impl(&self, prefix: &[u8], args: &[&[u8]], resp3: bool) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() < 3 {
      return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
    }

    let key = args[0];
    let kind = args[1];
    let mut cur_ix = 2usize;

    let mut element: Option<&[u8]> = None;
    let mut value_type = VectorValueType::Invalid;
    // 查询向量零拷贝：ELE/FP32/XU8/XI8 借用接收缓冲参数，仅 VALUES 文本浮点落缓冲
    let values: Cow<'_, [u8]>;

    if eq_ignore_case(kind, b"ELE") {
      // C# 对缺失的元素参数不做显式校验（空切片语义），此处以空串承接
      element = Some(args.get(cur_ix).copied().unwrap_or(b""));
      cur_ix += 1;
      values = Cow::Borrowed(&[]);
    } else if eq_ignore_case(kind, b"FP32") {
      let Some(as_bytes) = args.get(cur_ix) else {
        return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
      };
      if as_bytes.len() % 4 != 0 {
        return VectorReply::err(ERR_FP32_MULTIPLE_OF_4);
      }
      if as_bytes.len() / 4 > MAX_VECTOR_DIMENSIONS as usize {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::FP32;
      values = Cow::Borrowed(*as_bytes);
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"XU8") || eq_ignore_case(kind, b"XB8") {
      let Some(as_bytes) = args.get(cur_ix) else {
        return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
      };
      if as_bytes.len() > MAX_VECTOR_DIMENSIONS as usize {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::XU8;
      values = Cow::Borrowed(*as_bytes);
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"XI8") {
      let Some(as_bytes) = args.get(cur_ix) else {
        return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
      };
      if as_bytes.len() > MAX_VECTOR_DIMENSIONS as usize {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::XI8;
      values = Cow::Borrowed(*as_bytes);
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"VALUES") {
      let Some(count_raw) = args.get(cur_ix) else {
        return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
      };
      let Some(n) = strict_i32(count_raw).filter(|n| *n > 0) else {
        return VectorReply::err(ERR_VALUES_COUNT_MUST_BE_POSITIVE);
      };
      if n > MAX_VECTOR_DIMENSIONS as i32 {
        return self.abort_too_many_dimensions();
      }
      cur_ix += 1;
      if cur_ix + n as usize > args.len() {
        return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
      }
      value_type = VectorValueType::FP32;
      let mut floats = Vec::with_capacity(n as usize * 4);
      for _ in 0..n {
        let Some(f) = args.get(cur_ix).and_then(|a| strict_f32(a, true)) else {
          return VectorReply::err(ERR_VALUES_MUST_BE_FLOAT);
        };
        floats.extend_from_slice(&f.to_le_bytes());
        cur_ix += 1;
      }
      values = Cow::Owned(floats);
    } else {
      return VectorReply::err(ERR_VSIM_EXPECTED_KIND);
    }

    // 选项（默认值对齐 C#：count=10 / delta=2 / EF=100 / FILTER-EF=16）
    let mut with_scores = false;
    let mut with_attribs = false;
    let mut count: Option<i32> = None;
    let mut epsilon: Option<f32> = None;
    let mut ef: Option<i32> = None;
    let mut filter: Option<&[u8]> = None;
    let mut filter_ef: Option<i32> = None;
    // 对标 C# 仅做选项语法识别，当前执行流未启用真值比较与单线程模式
    let mut _truth = false;
    let mut _no_thread = false;

    while cur_ix < args.len() {
      let opt = args[cur_ix];
      if eq_ignore_case(opt, cs::WITHSCORES) {
        if with_scores {
          return err_dup!("WITHSCORES");
        }
        with_scores = true;
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"WITHATTRIBS") {
        if with_attribs {
          return err_dup!("WITHATTRIBS");
        }
        with_attribs = true;
        cur_ix += 1;
      } else if eq_ignore_case(opt, cs::COUNT) {
        if count.is_some() {
          return err_dup!("COUNT");
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
        };
        let Some(v) = strict_i32(v).filter(|v| *v >= 0 && *v <= MAX_RETRIEVE_COUNT as i32) else {
          return Self::abort_count_range();
        };
        count = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"EPSILON") {
        if epsilon.is_some() {
          return err_dup!("EPSILON");
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
        };
        let Some(v) = strict_f32(v, true).filter(|v| *v > 0.0) else {
          return VectorReply::err(ERR_EPSILON_MUST_BE_POSITIVE);
        };
        epsilon = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"EF") {
        if ef.is_some() {
          return err_dup!("EF");
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
        };
        let Some(v) = strict_i32(v).filter(|v| *v > 0 && *v <= MAX_EXPLORATION_FACTOR as i32)
        else {
          return Self::abort_ef_range();
        };
        ef = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"FILTER") {
        if filter.is_some() {
          return err_dup!("FILTER");
        }
        cur_ix += 1;
        let Some(f) = args.get(cur_ix) else {
          return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
        };
        filter = Some(f);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"FILTER-EF") {
        if filter_ef.is_some() {
          return err_dup!("FILTER-EF");
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return VectorReply::err(wrong_num_args!("VSIM").as_bytes());
        };
        let Some(v) = strict_i32(v).filter(|v| *v >= 4 && *v <= MAX_FILTERING_SCALE_FACTOR as i32)
        else {
          return Self::abort_filter_ef_range();
        };
        filter_ef = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"TRUTH") {
        if _truth {
          return err_dup!("TRUTH");
        }
        // TODO 语义与 C# 一致：仅识别
        _truth = true;
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"NOTHREAD") {
        if _no_thread {
          return err_dup!("NOTHREAD");
        }
        // C# 忽略 NOTHREAD
        _no_thread = true;
        cur_ix += 1;
      } else {
        return VectorReply::err(ERR_UNKNOWN_OPTION);
      }
    }
    // EPSILON / FILTER-EF 参与检索（对齐 C# 传参语义：maxFilteringEffort ??= 16
    // 放大过滤候选队列；delta 截断最大距离 —— 缺省对齐 Garnet 2.0f32）
    let delta = epsilon.unwrap_or(DEFAULT_VSIM_EPSILON);
    let filter_effort = filter_ef.unwrap_or(DEFAULT_VSIM_FILTER_EF).max(0) as usize;

    let count = count.unwrap_or(DEFAULT_VSIM_COUNT);

    // 键不存在：对齐 C# NOTFOUND → 空数组（非错误）
    let Some(stored) = self.manager.read_stored_index(prefix, key) else {
      return VectorReply::Array(Vec::new());
    };

    let search_opts = VectorSearchOptions {
      count: count.max(0) as usize,
      search_exploration_factor: ef.unwrap_or(DEFAULT_VSIM_EF).max(0) as usize,
      filter: filter.unwrap_or(b""),
      max_filtering_effort: filter_effort,
      delta,
      include_attributes: with_attribs,
    };

    let result = match element {
      Some(elem) => self.manager.element_similarity(&stored, elem, &search_opts),
      None => self
        .manager
        .value_similarity(&stored, value_type, values.as_ref(), &search_opts),
    };

    let output = match result {
      Ok(out) => out,
      Err(e) => return VectorReply::Error(e.message.into()),
    };

    // 拆包命中
    let ids: Vec<&[u8]> = unpack_length_prefixed(&output.output_ids);
    let attrs: Option<Vec<&[u8]>> =
      with_attribs.then(|| unpack_length_prefixed(&output.output_attributes));

    if resp3 {
      RespServerSessionVectors::write_resp3_result(
        count as usize,
        &ids,
        &output.output_distances,
        &output.filter_bitmap,
        attrs.as_deref(),
        with_scores,
      )
    } else {
      RespServerSessionVectors::write_resp2_result(
        count as usize,
        &ids,
        &output.output_distances,
        &output.filter_bitmap,
        attrs.as_deref(),
        with_scores,
      )
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVEMB
  ///
  /// `VEMB key element [RAW]` → 嵌入向量数组；RAW 时输出
  /// [量化器名, 原始量化字节, 范数, (Q8 量化范围)]。
  pub fn network_vemb(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() < 2 || args.len() > 3 {
      return VectorReply::err(wrong_num_args!("VEMB").as_bytes());
    }
    let raw = if args.len() == 3 {
      if !eq_ignore_case(args[2], b"RAW") {
        return VectorReply::err(ERR_VEMB_UNEXPECTED_OPTION);
      }
      true
    } else {
      false
    };

    // C#：键/元素缺失统一写空数组
    let Some(stored) = self.manager.read_stored_index(prefix, args[0]) else {
      return VectorReply::Array(Vec::new());
    };

    if raw {
      return match self.manager.try_get_raw_embedding(&stored, args[1]) {
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

    match self.manager.try_get_embedding(&stored, args[1]) {
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
  pub fn network_vcard(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() != 1 {
      return VectorReply::err(wrong_num_args!("VCARD").as_bytes());
    }
    let Some(index) = self.read_index(prefix, args[0]) else {
      return VectorReply::Integer(0);
    };
    VectorReply::Integer(self.manager.service.card(index.context) as i64)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVDIM
  pub fn network_vdim(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() != 1 {
      return VectorReply::err(wrong_num_args!("VDIM").as_bytes());
    }
    let Some(index) = self.read_index(prefix, args[0]) else {
      // 对齐 C# NOTFOUND → "ERR Key not found"
      return VectorReply::err(ERR_KEY_NOT_FOUND);
    };
    VectorReply::Integer(i64::from(index.dimensions))
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVGETATTR
  pub fn network_vgetattr(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() != 2 {
      return VectorReply::err(wrong_num_args!("VGETATTR").as_bytes());
    }
    let Some(index) = self.read_index(prefix, args[0]) else {
      // 对齐 C# NOTFOUND → null
      return VectorReply::Bulk(None);
    };
    match self.manager.service.get_attribute(index.context, args[1]) {
      Some(attr) => VectorReply::Bulk(Some(attr.into())),
      None => VectorReply::Bulk(None),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVINFO
  ///
  /// `VINFO key` → 14 项元信息（quant-type/distance-metric/input-vector-dimensions/
  /// reduced-dimensions/build-exploration-factor/num-links/size）。
  pub fn network_vinfo(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() != 1 {
      return VectorReply::err(wrong_num_args!("VINFO").as_bytes());
    }
    let Some(index) = self.read_index(prefix, args[0]) else {
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
    let bulk_u32 = |v: u32| VectorReply::Bulk(Some(v.to_string().into_bytes().into()));
    VectorReply::Array(vec![
      VectorReply::Simple(b"quant-type"),
      VectorReply::Simple(quant),
      VectorReply::Simple(b"distance-metric"),
      VectorReply::Simple(metric),
      VectorReply::Simple(b"input-vector-dimensions"),
      bulk_u32(index.dimensions),
      VectorReply::Simple(b"reduced-dimensions"),
      bulk_u32(index.reduce_dims),
      VectorReply::Simple(b"build-exploration-factor"),
      bulk_u32(index.build_exploration_factor),
      VectorReply::Simple(b"num-links"),
      bulk_u32(index.num_links),
      VectorReply::Simple(b"size"),
      VectorReply::Bulk(Some(
        self
          .manager
          .service
          .card(index.context)
          .to_string()
          .into_bytes()
          .into(),
      )),
    ])
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVISMEMBER
  #[inline]
  pub fn network_vismember(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    self.network_vismember_impl(prefix, args, false)
  }

  /// VISMEMBER 内部实现（RESP3 以布尔应答）。
  pub fn network_vismember_impl(&self, prefix: &[u8], args: &[&[u8]], resp3: bool) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() != 2 {
      return VectorReply::err(wrong_num_args!("VISMEMBER").as_bytes());
    }
    let member = self
      .read_index(prefix, args[0])
      .is_some_and(|index| self.manager.is_member(&index.to_bytes(), args[1]));
    match (member, resp3) {
      (true, true) => VectorReply::Boolean(true),
      (false, true) => VectorReply::Boolean(false),
      (true, false) => VectorReply::Integer(1),
      (false, false) => VectorReply::Integer(0),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVLINKS
  ///
  /// `VLINKS key element [WITHSCORES]`。C# 侧输出为 TODO（恒 +OK）；
  /// 此处返回层 0 邻接的实际元素（超集语义），键/元素缺失写 null。
  pub fn network_vlinks(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() != 2 && args.len() != 3 {
      return VectorReply::err(wrong_num_args!("VLINKS").as_bytes());
    }
    if args.len() == 3 && !eq_ignore_case(args[2], cs::WITHSCORES) {
      return VectorReply::err(ERR_VLINKS_UNEXPECTED_OPTION);
    }
    let Some(index) = self.read_index(prefix, args[0]) else {
      return VectorReply::Bulk(None);
    };
    match self.manager.service.links_of(index.context, args[1]) {
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
  pub fn network_vrandmember(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.is_empty() || args.len() > 2 {
      return VectorReply::err(wrong_num_args!("VRANDMEMBER").as_bytes());
    }
    let count = match args.get(1) {
      Some(raw) => {
        let Some(v) = strict_i32(raw) else {
          return VectorReply::err(ERR_EXPECTED_INTEGER_COUNT);
        };
        v
      }
      None => 1,
    };
    let Some(index) = self.read_index(prefix, args[0]) else {
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
      .sample(index.context, count.max(0) as usize);
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
  pub fn network_vrem(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() != 2 {
      return VectorReply::err(wrong_num_args!("VREM").as_bytes());
    }
    let Some(index) = self.read_index(prefix, args[0]) else {
      return VectorReply::Integer(0);
    };
    let removed = self.manager.try_remove(&index.to_bytes(), args[1]);
    match removed {
      VectorManagerResult::OK => {
        // 成功后合成写注入 AOF（对标 C# VectorStoreOps.VectorSetRemove 的
        // OK-后 ReplicateVectorSetRemove）
        self
          .manager
          .replicate_vector_set_remove(prefix, args[0], args[1]);
        VectorReply::Integer(1)
      }
      _ => VectorReply::Integer(0),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSETATTR
  #[inline]
  pub fn network_vsetattr(&self, prefix: &[u8], args: &[&[u8]]) -> VectorReply {
    self.network_vsetattr_impl(prefix, args, false)
  }

  /// VSETATTR 内部实现（RESP3 以布尔应答；缺失元素 → 假 / 0，非错误）。
  pub fn network_vsetattr_impl(&self, prefix: &[u8], args: &[&[u8]], resp3: bool) -> VectorReply {
    if !self.manager.is_enabled() {
      return self.abort_disabled();
    }
    if args.len() != 3 {
      return VectorReply::err(wrong_num_args!("VSETATTR").as_bytes());
    }
    let found = self.read_index(prefix, args[0]).is_some_and(|index| {
      let ok = self
        .manager
        .try_set_attribute(&index.to_bytes(), args[1], args[2]);
      if ok {
        // 成功后合成写注入 AOF（对标 C# VectorStoreOps.VectorSetSetAttribute
        // 的成功后 ReplicateVectorSetSetAttribute）
        self
          .manager
          .replicate_vector_set_set_attribute(prefix, args[0], args[1], args[2]);
      }
      ok
    });
    match (found, resp3) {
      (true, true) => VectorReply::Boolean(true),
      (false, true) => VectorReply::Boolean(false),
      (true, false) => VectorReply::Integer(1),
      (false, false) => VectorReply::Integer(0),
    }
  }
}

/// COUNT 范围错误。
impl<S: StoreCallbacks> RespServerSessionVectors<S> {
  fn abort_count_range() -> VectorReply {
    VectorReply::err(ERR_COUNT_RANGE)
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
    let has_filter = !filter_bitmap.is_empty();
    let with_attribs = attributes.is_some();
    // C#：有位图时输出上限 = popcount(bitmap)，否则全部命中；再与 count 取小
    let total_found = ids.len();
    let output_count = if has_filter {
      filter_bitmap
        .iter()
        .map(|b| b.count_ones() as usize)
        .sum::<usize>()
        .min(count)
    } else {
      total_found.min(count)
    };

    let mut plain = if !with_scores && !with_attribs {
      Vec::with_capacity(output_count)
    } else {
      Vec::new()
    };
    let mut map = if with_scores || with_attribs {
      Vec::with_capacity(output_count)
    } else {
      Vec::new()
    };
    let mut written = 0usize;
    for (result_index, id) in ids.iter().enumerate().take(total_found) {
      if written >= output_count {
        break;
      }
      // 过滤位图：未通过项跳过
      if has_filter && (filter_bitmap[result_index >> 3] >> (result_index & 7)) & 1 == 0 {
        continue;
      }
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
      if !with_scores && !with_attribs {
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
      written += 1;
    }
    if !with_scores && !with_attribs {
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
    let has_filter = !filter_bitmap.is_empty();
    let with_attribs = attributes.is_some();
    let total_found = ids.len();
    let output_count = if has_filter {
      filter_bitmap
        .iter()
        .map(|b| b.count_ones() as usize)
        .sum::<usize>()
        .min(count)
    } else {
      total_found.min(count)
    };

    let multiplier = 1 + usize::from(with_scores) + usize::from(with_attribs);
    let mut items = Vec::with_capacity(output_count * multiplier);
    let mut written = 0usize;
    for (result_index, id) in ids.iter().enumerate().take(total_found) {
      if written >= output_count {
        break;
      }
      if has_filter && (filter_bitmap[result_index >> 3] >> (result_index & 7)) & 1 == 0 {
        continue;
      }
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
      written += 1;
    }
    VectorReply::Array(items)
  }
}
